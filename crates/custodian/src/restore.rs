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
//! ## 4. Upload sessions come back live
//!
//! A resurrected `Open` or `Completing` upload could be completed over reclaimed bytes, so, last,
//! this pass **fences** every such session (0016 D-B; see [`reconcile_after_restore`]), and keeps
//! a record of its own progress, the **restore-fence generation**
//! ([`wyrd_core::multipart::FenceGeneration`]): written not complete before the pass's first
//! write, and complete after its last, only once no upload session needs a human.
//!
//! # The safety gate, unchanged
//!
//! Marking is the front half of a deletion, so the invariant [`crate::gc`] is built around
//! holds here identically and is enforced twice: **a fragment referenced by a committed chunk
//! map is never marked** (and, even if it somehow were, GC's own gate would still refuse to
//! reclaim it). A chunk with a *malformed* placement is treated as fully referenced — fail
//! safe — exactly as GC treats it.
//!
//! **Nor is a staged fragment ever marked** — one a multipart upload's committed part or in-flight
//! owned staging entry names ([`crate::gc::StagedSet`], proposal 0016 decision 2). The pass reads
//! that class through the reader GC uses, before either reading of the committed namespace, and
//! gates on it by the same rules: a staged record it cannot read withholds every mark and is named
//! in the report, one it cannot trust holds its chunk and is named there too, and a store fault
//! under one of its reads fails the pass; what the class keeps is counted. The protection covers
//! the upload records already durable when the pass read them; a write or an upload that starts
//! while the pass runs is not yet covered (#805).
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

use wyrd_core::metadata::{
    self, parse_seg_key, seg_range_prefix, ChunkMapError, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord,
};
use wyrd_core::multipart::{
    decode_fence_generation, decode_part_record, decode_retire_obligation, decode_session_record,
    parse_mpu_key, parse_part_key, part_range, retire_key, FenceGeneration, RetireMode,
    RetireObligation, RetireToken, SessionRecord, SessionState, UploadId, MPUFENCE_KEY, MPU_PREFIX,
};
use wyrd_traits::{
    ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::gc::{
    marked_among, object_name, orphan_key, parse_pending_chunk, referenced_fragments,
    staged_fragments, staged_page, GcContext, ReferenceSet, StagedSet,
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
    /// Unreferenced fragments that already carried an `orphan:` record — their own key, whatever
    /// its value holds. Left untouched — re-stamping would reset the grace clock and delay their
    /// reclamation, or overwrite a value GC cannot read that a human still has to repair.
    pub already_marked: usize,
    /// Unreferenced fragments left alone because their chunk still holds a `pending:`
    /// lease — an in-flight write, whose lease TTL is already its grace. GC owns them.
    pub pending_skipped: usize,
    /// Fragments left unmarked because the **staged protection class** keeps them
    /// ([`crate::gc::StagedSet`], `0016:823`): a multipart upload's own record places them, or
    /// names their chunk untrustworthily ([`RestoreReport::staged_untrusted`]), or a staged record
    /// could not be read at all — which keeps every fragment the committed readings do not, and is
    /// named in [`RestoreReport::unresolvable`].
    ///
    /// Each kept fragment is counted once, under the FIRST protection that keeps it in the pass's
    /// order: the committed readings (uncounted), the staged class, the displaced check, the
    /// pending lease. So one a committed map protects is never counted here, and one counted here
    /// is not counted again under `displaced_kept` or `pending_skipped`.
    pub staged_skipped: usize,
    /// `pending:` entries this pass could **not read as an ordinary lease** — torn, malformed,
    /// or an owned staging entry filed under the wrong key — named by key as
    /// [`RestoreReport::unresolvable`] names a record. Their chunks are **held** exactly as a
    /// live lease's are (a fragment of theirs counts under [`RestoreReport::pending_skipped`]),
    /// because an entry the pass cannot read may be protecting an in-flight write; and they are
    /// a human's, because nothing else will ever clear them — GC's lease path refuses the same
    /// value, so the entry and the fragments it holds would otherwise stay forever without a
    /// signal anywhere (PR #793 review). Not folded into `unresolvable`: an unreadable lease does
    /// not make the reading of the **committed** namespace partial, so it withholds no mark and
    /// does not take "complete" off the summary line.
    pub pending_unreadable: Vec<String>,
    /// Staged multipart records this pass read but could **not trust** about where a chunk's
    /// fragments are — a placement of the wrong length, or an owned `sidx:` value that will not
    /// decode under a key that still names its chunk ([`crate::gc::StagedSet::held`]). Named by
    /// key as [`RestoreReport::unresolvable`] names a record, once per record, in key order.
    ///
    /// The pass held every fragment of each such chunk and marked none; that is all it claims. It
    /// does not claim the staged bytes survived: it judges missing bytes for committed chunks
    /// only. The run is then not [clean](RestoreReport::is_clean), and does **not** need a human
    /// ([`RestoreReport::needs_human`] says why). Not in `unresolvable`: the record was read, so
    /// the reading is not partial.
    pub staged_untrusted: Vec<String>,
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
    /// Records this pass could **not read**, named by key as the store spells it, escaped rather
    /// than rendered lossily so two damaged records never arrive under one name, and ordered by
    /// that key. Two kinds:
    ///
    /// - **committed objects** whose chunk map could not be read — a segmented generation whose
    ///   `seg:` records are incomplete, or a record that will not decode — by `inode:` key
    ///   ([`crate::gc::ReferenceSet::unresolvable`]); and
    /// - **staged multipart records** that could not be read — a session key naming no upload, a
    ///   part key or value that will not parse or decode, an owned staging key naming no chunk —
    ///   by `mpu:`, `part:` or `sidx:` key ([`crate::gc::StagedSet::unresolvable`]).
    ///
    /// The pass keeps going past them and marks **nothing** on their account: while any record
    /// is unresolvable the reading is incomplete, so every fragment in the fleet is held
    /// off-limits and [`RestoreReport::stranded_marked`] stays 0.
    ///
    /// They are reported because every *other* verdict here — dangling, misplaced,
    /// under-replicated — is drawn over the objects the pass COULD read: a clean report with a
    /// non-empty list here is a clean report about **part** of the store, and an operator
    /// reading it as a clean bill would decommission on it.
    pub unresolvable: Vec<String>,
    /// `Open` and `Completing` sessions **fenced** to `Aborting`, each counted once its commit
    /// landed (`0016:823`).
    pub sessions_fenced: usize,
    /// Sessions the fence could **not** fence, by `mpu:` key and why, in key order: each left as
    /// read, with no obligation — a human's (also `action=session-unsettled` on the audit seam).
    pub sessions_unsettled: Vec<UnsettledSession>,
    /// Sessions fenced from `Completing` (by this pass or an earlier one) that still need a human
    /// ([`UnaccountedSegments`]), in key order (also `action=session-segments-unaccounted`).
    pub segments_unaccounted: Vec<UnaccountedSegments>,
    /// This pass's **restore-fence generation** as it last wrote it under [`MPUFENCE_KEY`]
    /// (`0016:723-728`, X17b): `Some` on every report the pass returns, and complete exactly when
    /// [`RestoreReport::sessions_settled`] holds — so a run can need a human over a dangling chunk
    /// and still complete it. `None` only on a report no pass returned (a default).
    ///
    /// It says this pass finished its fence. It does not say this restore's pass ran: a restore
    /// brings back whatever generation record its image held, `complete` included (see
    /// [`FenceGeneration`]).
    pub fence_generation: Option<FenceGeneration>,
}

/// A fenced `Completing` session whose attempt's `seg:` range holds a record nothing accounts for
/// (or whose records obligation is missing, or not that range's deleter): that obligation deletes
/// the range without marking a byte (`0016:2347-2350`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaccountedSegments {
    /// Its `mpu:` key, escaped as every name in the report is ([`crate::gc::object_name`]).
    pub session: String,
    /// The first record at fault, by key, escaped: a `seg:` record, or the `retire:records:` one.
    pub record: String,
    /// What is wrong with it.
    pub fault: SegmentFault,
}

/// What is wrong with the record [`UnaccountedSegments`] names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentFault {
    /// It names a chunk no `part:` record of the session (that this pass could read) holds.
    ChunkInNoPart {
        /// The chunk.
        chunk: ChunkId,
    },
    /// It will not decode (ADR-0045): a segment, naming chunks no one can read; or the records
    /// obligation, deleting a range no one can read.
    Undecodable {
        /// The decoder's rejection.
        fault: String,
    },
    /// Its key is not a segment key of the attempt's group, yet it sits in the range deleted.
    KeyNotOfGroup,
    /// The records obligation decodes but is not the fence's `{seg: <own group>}`: it owes another
    /// group (nothing deletes the attempt's records, X57), or `part:` records as well (X104).
    NotOfAttempt,
    /// A segment record of the attempt, with no records obligation there at all to delete it.
    NoDeleter,
}

impl std::fmt::Display for SegmentFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChunkInNoPart { chunk } => {
                let chunk = wyrd_traits::chunk_hex(*chunk);
                write!(f, "it names chunk {chunk}, held by no readable part")
            }
            Self::Undecodable { fault } => write!(f, "it will not decode: {fault}"),
            Self::KeyNotOfGroup => write!(f, "its key is not a segment key of the attempt"),
            Self::NotOfAttempt => write!(f, "it does not owe only the attempt's own segments"),
            Self::NoDeleter => write!(f, "no `retire:records:` obligation is there to delete it"),
        }
    }
}

/// One session the post-restore fence could not fence, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsettledSession {
    /// Its `mpu:` key, escaped as every name in the report is ([`crate::gc::object_name`]).
    pub session: String,
    /// Why the fence left it as read.
    pub cause: SessionUnsettled,
}

/// Why the post-restore fence left a session unfenced — byte-identical, with no obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionUnsettled {
    /// Its key names no upload (the staged read names it in [`RestoreReport::unresolvable`]).
    KeyNamesNoUpload,
    /// Its value will not decode (ADR-0045); the staged class still protects it by key.
    ValueUndecodable {
        /// The decoder's rejection.
        fault: String,
    },
    /// `Open` or `Completing` at `u64::MAX`: every fence bumps the epoch, and there is no next.
    EpochExhausted,
    /// Its record changed between the pass's read and the fence's commit.
    ChangedUnderPass,
    /// An obligation key was already taken (`require_absent`, `0016:369-373`): never retried.
    ObligationKeyTaken {
        /// The taken key, escaped.
        key: String,
    },
    /// The commit lost a conflict whose cause a re-read no longer finds.
    LostConflict,
}

impl std::fmt::Display for SessionUnsettled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNamesNoUpload => write!(f, "its key names no upload"),
            Self::ValueUndecodable { fault } => write!(f, "its value will not decode: {fault}"),
            Self::EpochExhausted => write!(f, "it is at the last epoch, with no next"),
            Self::ChangedUnderPass => write!(f, "it changed after the pass read it"),
            Self::ObligationKeyTaken { key } => write!(f, "its obligation key {key} is taken"),
            Self::LostConflict => write!(f, "its commit lost a conflict a re-read cannot explain"),
        }
    }
}

impl RestoreReport {
    /// Did the pass find anything an operator must act on or absorb — **and** did its reading
    /// finish?
    ///
    /// The strict superset of [`Self::needs_human`]: it also counts the work this pass DID
    /// (fragments marked collectable), the work the repair loop will do (under-replicated
    /// chunks), and the [untrusted staged records](RestoreReport::staged_untrusted) it held chunks
    /// over, none of which is a human's. Written **in terms of** that predicate rather than
    /// beside it, so the two cannot drift as fields are added.
    /// A [fenced session](RestoreReport::sessions_fenced) is work this pass DID, too.
    ///
    /// An [unresolvable object](RestoreReport::unresolvable) counts: "clean" is a claim about a
    /// reading that FINISHED, and this one did not — so a store the pass could only partly read
    /// is never certified clean (`docs/principles.md` §5 C-1). An untrusted staged record counts
    /// on the same ground: the pass could not tell where that chunk's fragments are.
    pub fn is_clean(&self) -> bool {
        self.stranded_marked == 0
            && self.sessions_fenced == 0
            && self.under_replicated.is_empty()
            && self.staged_untrusted.is_empty()
            && !self.needs_human()
    }

    /// Does this run need a **human** — the question `wyrd custodian --reconcile-after-restore`
    /// turns into its exit status (`crates/server/src/cli.rs`'s `restore_verdict`)?
    ///
    /// The findings no loop resolves on its own: chunks that can no longer be read at all, chunks
    /// whose bytes are somewhere the restored map does not look, records — committed objects or
    /// staged multipart records — this pass could not read, and pending-ledger entries it could
    /// not read as a lease. Marks and under-replication are deliberately **not** here — the
    /// first is this pass doing its job and the second is the reconstruction loop's, so failing
    /// a restore script on either would train an operator to ignore the status. It lives on the
    /// report rather than in the command because a caller that never prints the summary still
    /// needs the same verdict, and would otherwise re-derive it slightly differently.
    ///
    /// A session the fence could not fence counts — nothing stops it publishing — and one it
    /// fenced does not: that is this pass's job; unless its segment records need a human
    /// ([`RestoreReport::segments_unaccounted`]).
    ///
    /// **A deliberate exception to that rule:** an untrusted staged record
    /// ([`RestoreReport::staged_untrusted`]) is left out, although no loop removes one yet
    /// (whether the retire drain does is #659's call). It rests on the human's decision at #664's
    /// plan revision (2026-09-18), not on the rule above: once the session is fenced its staged
    /// bytes are garbage whatever the record says, so what is left is cleanup — automatic work,
    /// not a judgement — and keep-on-doubt protects user data, not system residue (#811). Its
    /// condition is that fence (#841, #842); until then the session is still `Open`, a window no
    /// production client reaches, since none can create a session before #508, which lands after
    /// it. The record is still named, and the run is not [clean](Self::is_clean): a damaged staged
    /// record points at a bug or corruption and blocks every drain in the cluster, so the operator
    /// should hear of it at restore time rather than when a drain stalls.
    pub fn needs_human(&self) -> bool {
        !self.dangling.is_empty()
            || !self.misplaced.is_empty()
            || !self.unresolvable.is_empty()
            || !self.pending_unreadable.is_empty()
            || !self.sessions_unsettled.is_empty()
            || !self.segments_unaccounted.is_empty()
    }

    /// Did the fence leave **no** upload session for a human — the condition this pass's
    /// [restore-fence generation](RestoreReport::fence_generation) completes on?
    ///
    /// Session findings only (#810): a session the fence could not read or could not fence
    /// ([`RestoreReport::sessions_unsettled`] — a key naming no upload, a value that will not
    /// decode, a commit it could not land), and a fenced one that still needs a human
    /// ([`RestoreReport::segments_unaccounted`]), whether this pass fenced it or an earlier one
    /// did: both are judged afresh from the store on every run. Findings about committed objects
    /// (dangling, misplaced, an unreadable `inode:` record), the pending ledger, and staged
    /// `part:` / `sidx:` records (an untrusted one included) are [`Self::needs_human`]'s and do not
    /// withhold it: none of them lets a session publish. So a generation left not complete on a
    /// returned report always comes with a NEEDS-HUMAN finding, and the reverse does not hold.
    pub fn sessions_settled(&self) -> bool {
        self.sessions_unsettled.is_empty() && self.segments_unaccounted.is_empty()
    }
}

/// Why the post-restore pass could not open or close its restore-fence generation
/// ([`FenceGeneration`], under [`MPUFENCE_KEY`]). Each ends the pass with an `Err`: one met while
/// opening it, before the pass writes anything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceGenerationFault {
    /// The record will not decode (ADR-0045), so the pass cannot tell which generation comes
    /// next. Repair it, or remove it (absent reads as "no pass has run"), and re-run.
    Unreadable {
        /// The decoder's rejection.
        fault: String,
    },
    /// Generation `u64::MAX` has already run: there is no next.
    Exhausted,
    /// The record changed between this pass's read and its write: another pass ran meanwhile.
    ChangedUnderPass {
        /// The generation this pass was writing.
        generation: u64,
    },
}

impl std::fmt::Display for FenceGenerationFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = object_name(MPUFENCE_KEY);
        match self {
            Self::Unreadable { fault } => write!(
                f,
                "the restore-fence generation record {key} will not decode ({fault}); this pass \
                 wrote nothing. Repair or remove it (absent reads as no pass has run), then re-run"
            ),
            Self::Exhausted => write!(
                f,
                "the restore-fence generation record {key} is at generation {}, with no next; \
                 this pass wrote nothing",
                u64::MAX
            ),
            Self::ChangedUnderPass { generation } => write!(
                f,
                "the restore-fence generation record {key} changed while this pass wrote \
                 generation {generation}: another pass ran meanwhile, so this one's generation is \
                 not complete. Let the other pass finish, then re-run"
            ),
        }
    }
}

impl std::error::Error for FenceGenerationFault {}

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
/// Then, last, every session the image holds `Open` or `Completing` is fenced (below).
///
/// Deletes nothing: it writes marks, session fences and its generation record. Run it with
/// **writers stopped**, after a restore.
///
/// # Staged bytes are never marked
///
/// A fragment a multipart upload's committed part (`part:`) or in-flight owned staging entry
/// (`sidx:`) names is referenced by no committed chunk map yet, and marking it would hand GC a
/// live upload's bytes. So the pass first reads the staged protection class
/// ([`staged_fragments`], the reader GC uses), for every session listed under `mpu:` whatever its
/// state, and never marks a fragment it protects. It is read before either reading of the
/// committed namespace, so a publication that moves a chunk from its part record to a committed
/// inode while the pass runs leaves it protected by one reading or the other. A staged record the
/// pass cannot read is named in [`RestoreReport::unresolvable`] and withholds every mark, as an
/// unreadable committed object does; one it can read but not trust holds its chunk and is named on
/// the audit seam and in [`RestoreReport::staged_untrusted`]; a store fault under a staged read
/// fails the pass with an `Err` naming the read. What the class keeps is counted
/// ([`RestoreReport::staged_skipped`]).
///
/// What this covers is the upload records **already durable when the pass read them**. A write or
/// an upload that starts after that read is not protected by it — the runbook's writers-stopped
/// rule is what covers that window today (#805).
///
/// # Every session the image held `Open` or `Completing` is fenced — last
///
/// A resurrected session could be completed over reclaimed bytes (0016 D-B, `0016:717-728`). So
/// after Pass 3 the pass re-lists `mpu:` in bounded pages and moves each `Open@E` session to
/// `Aborting@E+1` in **one** commit with its `{session, all}` obligation
/// ([`wyrd_core::multipart::SessionRecord::open_teardown`]), requiring the bytes it read and the
/// obligation key absent (`0016:369-373`). What it cannot fence is named, classified once and
/// never retried ([`RestoreReport::sessions_unsettled`]). A failed commit — an unknown outcome
/// included, never read as a `Conflict` — ends the pass with that `Err` after the summary records
/// every count as INCOMPLETE, never clean; the re-run is idempotent.
///
/// A `Completing@E` session is fenced the same way, its commit also installing its attempt's
/// `retire:records:{seg}` ([`SessionRecord::completing_teardown`]), that key required absent too;
/// the attempt's range is then judged on every run ([`RestoreReport::segments_unaccounted`]).
///
/// # Its restore-fence generation: opened first, completed last
///
/// Before it writes anything else the pass opens its **restore-fence generation** under
/// [`MPUFENCE_KEY`] — `{N+1, complete: false}` over the `{N, ..}` it read, generation 1 over an
/// absent record — and runs no mark and no fence until that commit is acknowledged
/// (`0016:723-728`, X17b). After its last write it records that generation complete, conditioned
/// on the not-complete bytes it wrote, only if every write it made was acknowledged and
/// [`RestoreReport::sessions_settled`] holds. A fenced session's residue is never carried in
/// memory or on the generation record: every run re-reads each `Aborting` session's records and
/// names it again while it needs a human, and an undecodable session is named again by the fence,
/// so no later pass completes a generation over residue an earlier one could not repair — however
/// the earlier one ended.
///
/// The record says this pass finished its fence; it cannot say this restore's pass ran, because a
/// restore rewinds it with everything else ([`FenceGeneration`]). Until a gateway reads it beside
/// a restore-scoped signal (#508), what keeps multipart off a restored image is the runbook: this
/// pass runs with writers stopped, before any gateway is re-enabled (`0016:3017-3021`).
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
    // FIRST OF ALL, this pass's restore-fence generation, NOT complete — acknowledged before the
    // pass writes anything else, so no mark or fence it makes runs under a `complete` reading
    // (a restored image may carry one), and "complete" is observable only after its last write
    // (`close_generation`, below).
    let generation = open_generation(ctx.meta).await?;
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
    //
    // FIRST, the staged protection class — the fragments multipart uploads' own records name — by
    // the reader GC uses (`gc::staged_fragments`), and BEFORE either reading of the committed
    // namespace below: a publication moves a chunk's protection from its `part:` record to a
    // committed inode, so this order sees it in at least one of them (`0016:793-800`). A store
    // fault under it fails the pass here, before anything is marked.
    //
    // deferred: #805 — this read protects the upload records already durable when it ran. A write
    // or an upload that starts after it is not in it; until #805 that window is the runbook's
    // writers-stopped rule, not this pass's.
    let staged = staged_fragments(ctx.meta).await?;
    // ATTRIBUTED THE INSTANT IT IS KNOWN, per record, before the next store read — the placement
    // `gc::reconcile` uses for the same set (`gc.rs`, its `unresolvable` loop sits between the
    // reference build and the fleet walk). Batching these names behind the reads below would
    // mean a genuine, unrelated store fault in any of them — one `?` away — ends the pass with
    // an `Err` carrying nothing, and the record the operator must repair, ALREADY KNOWN by
    // then, never reaches them at all. Attribution that a later transient fault can swallow is
    // not attribution.
    let mut unreadable = BTreeSet::new();
    let staged_untrusted = attribute_staged(&staged, &mut unreadable);
    let referenced = referenced_fragments(ctx.meta).await?;
    attribute_unresolvable(&referenced.unresolvable, &mut unreadable);
    let PendingLedger {
        held: pending,
        unreadable: pending_unreadable,
    } = pending_chunks(ctx.meta).await?;
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
        // deduplicated and in the store's own key order, whichever read met each of them. A
        // staged record the staged read could not read is in it too: the mark half gates on
        // that class as well, so its hole is this run's hole.
        unresolvable: unreadable.iter().map(|key| object_name(key)).collect(),
        pending_unreadable,
        staged_untrusted,
        fence_generation: Some(generation),
        ..Default::default()
    };
    // ONE READING, ONE CONCLUSION. `gc::ReferenceSet::protects` already withholds every fragment
    // in the fleet while the reference BUILD found a hole; this extends the same withholding to
    // a hole the verdict read found, so the pass can never both mark a fragment and report a
    // record it could not read. Whichever read met the damage, the answer is the same one.
    let incomplete = !report.unresolvable.is_empty();
    // The COMMITTED half of that hole: a record either reading of the committed namespace could
    // not read. The rest of `incomplete` is the staged class's own, and the gate below keeps it
    // under that class (and counts it there), not as a committed protection.
    let committed_incomplete =
        !referenced.unresolvable.is_empty() || !committed.unresolvable.is_empty();
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

    // Pass 2 — decide, with the full picture: first which fragments may be marked at all, then
    // (below) which of those already are.
    let mut candidates: Vec<(DServerId, FragmentId)> = Vec::new();
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
        if committed_incomplete
            || referenced.protects(dserver, frag)
            || appeared.protects(dserver, frag)
        {
            continue;
        }

        // And never a fragment the staged class protects: a multipart upload's committed part or
        // owned staging entry names it, or names its chunk untrustworthily, or a staged record
        // could not be read — which keeps EVERY fragment the committed readings left (`incomplete`
        // past `committed_incomplete` is that record's hole alone; `StagedSet::protects` says the
        // same, and the gate does not lean on it to). COUNTED, once, by the FIRST protection that
        // keeps it: after the committed readings, before the displaced check and the pending lease
        // below (`RestoreReport::staged_skipped`, `0016:823`).
        if incomplete || staged.protects(dserver, frag) {
            report.staged_skipped += 1;
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

        candidates.push((dserver, frag));
    }

    // Which candidates ALREADY carry their own `orphan:` record — judged over the WHOLE ledger,
    // walked in bounded pages (`gc::marked_among`), never one `scan` (which fails whole past
    // `SCAN_CAP`) and never part of it: the put below re-stamps anything this judgement missed,
    // restarting that mark's grace clock. Any value counts, readable or not — a mark whose value
    // does not read as an instant is still a mark, and re-stamping it would erase the only
    // evidence a human has to repair. Read only once the candidates are known, so a pass with
    // nothing it may mark reads none of the ledger.
    let already = marked_among(ctx.meta, &candidates).await?;
    for (dserver, frag) in candidates {
        if already.contains(&(dserver, frag)) {
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
            commit_marks(ctx.meta, std::mem::take(&mut marks)).await?;
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
        commit_marks(ctx.meta, std::mem::take(&mut marks)).await?;
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

    // THE SESSION FENCE — after Pass 3, so a fence fault never takes a verdict with it (#651's
    // class): the summary is emitted first, INCOMPLETE, and is where the counts survive an `Err`.
    // Then, LAST, the generation's completion — reached only when every write above was
    // acknowledged, since every path where one was not has already returned its `Err`.
    let (finished, cut) = match fence_open_sessions(ctx.meta, &mut report).await {
        Err(fault) => (Err(fault), Some(Cut::Fence)),
        Ok(()) => match close_generation(ctx.meta, generation, &mut report).await {
            Ok(()) => (Ok(()), None),
            Err(fault) => (Err(fault), Some(Cut::Generation)),
        },
    };
    emit_summary(&report, cut);
    finished?;
    Ok(report)
}

/// Open this pass's restore-fence generation: write `{N+1, complete: false}` over the `{N, ..}`
/// read there (generation 1 over an absent record), conditioned on the bytes read, and return it
/// only once that commit is **acknowledged** — the pass's first write, ahead of every mark and
/// fence.
///
/// Anything else ends the pass here, having written nothing else: a record that will not decode
/// or has no next generation ([`FenceGenerationFault`]), a `Conflict` (it changed under the
/// pass), and an `Err`. An unknown outcome is that `Err` whether or not it applied
/// (`crates/traits/src/lib.rs:204-247`): until the pass knows its not-complete record landed, a
/// fence it ran could run under the `complete` a restored image carried. Its read's and commit's
/// awaits are bounded by the `MetadataStore` implementation (#508/#636), as the fence's are.
async fn open_generation(meta: &dyn MetadataStore) -> Result<FenceGeneration> {
    let read = meta.get(MPUFENCE_KEY).await?;
    let prior = match read.as_deref().map(decode_fence_generation).transpose() {
        Ok(prior) => prior,
        Err(fault) => {
            let fault = fault.to_string();
            return Err(generation_fault(FenceGenerationFault::Unreadable { fault }));
        }
    };
    let Some(opened) = FenceGeneration::next(prior.as_ref()) else {
        return Err(generation_fault(FenceGenerationFault::Exhausted));
    };
    let batch = match read {
        Some(bytes) => WriteBatch::new().require(MPUFENCE_KEY, bytes),
        None => WriteBatch::new().require_absent(MPUFENCE_KEY),
    };
    let batch = batch.put(MPUFENCE_KEY, metadata::encode(&opened));
    match meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            emit_generation(&opened);
            Ok(opened)
        }
        Ok(CommitOutcome::Conflict) => {
            let generation = opened.generation();
            let fault = FenceGenerationFault::ChangedUnderPass { generation };
            Err(generation_fault(fault))
        }
        Err(fault) => {
            emit_generation_write_failed(&opened, &fault.to_string());
            Err(fault)
        }
    }
}

/// Close this pass's generation: write it complete, conditioned on the not-complete bytes the pass
/// opened it with — only when no session needs a human ([`RestoreReport::sessions_settled`]), and
/// only once every earlier write was acknowledged (the caller reaches here on no other path).
///
/// The precondition is what keeps a late landing honest: a completion whose commit answered an
/// unknown outcome and lands after a newer pass opened its own generation finds that newer record,
/// not the bytes it requires, and writes nothing — a stale `complete` never masks a newer pass.
/// An `Err` here is the pass's `Err`: an unknown outcome is never read as a clean finish
/// (`AGENTS.md`), though if it applied, the record it wrote is true — every earlier write was
/// acknowledged — and if it did not, the next pass opens and completes its own generation. Its
/// commit's await is bounded by the `MetadataStore` implementation (#508/#636).
async fn close_generation(
    meta: &dyn MetadataStore,
    opened: FenceGeneration,
    report: &mut RestoreReport,
) -> Result<()> {
    if !report.sessions_settled() {
        emit_generation_left_open(&opened, report);
        return Ok(());
    }
    let closed = opened.completed();
    let batch = WriteBatch::new()
        .require(MPUFENCE_KEY, metadata::encode(&opened))
        .put(MPUFENCE_KEY, metadata::encode(&closed));
    match meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            emit_generation(&closed);
            report.fence_generation = Some(closed);
            Ok(())
        }
        Ok(CommitOutcome::Conflict) => {
            let generation = opened.generation();
            let fault = FenceGenerationFault::ChangedUnderPass { generation };
            Err(generation_fault(fault))
        }
        Err(fault) => {
            emit_generation_write_failed(&closed, &fault.to_string());
            Err(fault)
        }
    }
}

/// Name a [`FenceGenerationFault`] on the audit seam and box it as the pass's `Err`.
fn generation_fault(fault: FenceGenerationFault) -> wyrd_traits::BoxError {
    emit_generation_fault(&fault);
    fault.into()
}

/// Commit one batch of orphan marks: acknowledged, or the pass's `Err`. The batch carries no
/// precondition, so a `Conflict` is the store answering that none of it was written — never
/// counted and audited as marks GC will act on, and never a write the pass's generation may be
/// completed over.
async fn commit_marks(meta: &dyn MetadataStore, marks: WriteBatch) -> Result<()> {
    match meta.commit(marks).await? {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::Conflict => Err("post-restore: a batch of orphan marks, which carries no \
                                        precondition, answered Conflict — none of it was \
                                        written; re-run the pass"
            .into()),
    }
}

/// The session fence: re-list `mpu:` in bounded pages ([`staged_page`]) and fence each session.
/// Every read's and commit's await is bounded by the `MetadataStore` implementation (#508/#636).
// deferred: #843 — seeded Tier-0 DST coverage of this fence (809.5).
async fn fence_open_sessions(meta: &dyn MetadataStore, report: &mut RestoreReport) -> Result<()> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (sessions, next) = staged_page(meta, MPU_PREFIX, after.as_deref()).await?;
        for (key, value) in &sessions {
            fence_session(meta, key, value, report).await?;
        }
        match (next, sessions.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(()),
        }
    }
}

/// Fence one listed session, or name why not ([`SessionUnsettled`]).
async fn fence_session(
    meta: &dyn MetadataStore,
    key: &[u8],
    read: &[u8],
    report: &mut RestoreReport,
) -> Result<()> {
    let session = object_name(key);
    let fence = match plan_fence(key, read) {
        Ok(Plan::Fence(fence)) => *fence,
        Ok(Plan::Fenced(upload, group)) => {
            return recheck_fenced(meta, &session, &upload, &group, report).await;
        }
        Ok(Plan::Settled) => return Ok(()),
        Err(cause) => {
            unsettled(report, session, cause);
            return Ok(());
        }
    };
    let keys: Vec<Vec<u8>> = fence
        .obligations
        .iter()
        .map(RetireObligation::key)
        .collect();
    let mut batch = WriteBatch::new().require(key.to_vec(), read.to_vec());
    for obligation in &keys {
        batch = batch.require_absent(obligation.clone());
    }
    batch = batch.put(key.to_vec(), metadata::encode(&fence.session));
    for (obligation, at) in fence.obligations.iter().zip(&keys) {
        batch = batch.put(at.clone(), metadata::encode(obligation.payload()));
    }
    match meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            let named: Vec<String> = keys.iter().map(|at| object_name(at)).collect();
            emit_session_fenced(&session, fence.session.epoch(), &named.join(" "));
            report.sessions_fenced += 1;
            if let Some((upload, group)) = &fence.attempt {
                check_attempt(meta, &session, upload, group, report).await?;
            }
        }
        Ok(CommitOutcome::Conflict) => {
            // Which precondition lost is read afresh, once: a token is minted once
            // (`0016:358-373`), so a taken key is damage a retry cannot settle.
            let cause = if meta.get(key).await?.as_deref() != Some(read) {
                SessionUnsettled::ChangedUnderPass
            } else {
                let mut cause = SessionUnsettled::LostConflict;
                for taken in &keys {
                    if meta.get(taken).await?.is_some() {
                        let key = object_name(taken);
                        cause = SessionUnsettled::ObligationKeyTaken { key };
                        break;
                    }
                }
                cause
            };
            unsettled(report, session, cause);
        }
        // Never read as a `Conflict`: the outcome may be unknown (`CommitUnknownResult`).
        Err(fault) => {
            emit_session_fence_failed(&session, &fault.to_string());
            return Err(fault);
        }
    }
    Ok(())
}

/// What the fence does with one listed session ([`plan_fence`]).
enum Plan {
    /// Commit this fence.
    Fence(Box<Fence>),
    /// `Aborting@E'`: nothing to write, but see [`recheck_fenced`] for its own group at `E'-1`.
    Fenced(UploadId, SegmentGroup),
    /// `Completed`, or `Aborting@0`: it can no longer publish, and no attempt is owed.
    Settled,
}

/// One session's teardown, and for a `Completing` one the attempt to check once it lands.
struct Fence {
    session: SessionRecord,
    obligations: Vec<RetireObligation>,
    attempt: Option<(UploadId, SegmentGroup)>,
}

/// What to do with the session read as (`key`, `read`), or why it cannot be fenced.
fn plan_fence(key: &[u8], read: &[u8]) -> std::result::Result<Plan, SessionUnsettled> {
    let upload = parse_mpu_key(key).map_err(|_| SessionUnsettled::KeyNamesNoUpload)?;
    let record = decode_session_record(read).map_err(|fault| {
        let fault = fault.to_string();
        SessionUnsettled::ValueUndecodable { fault }
    })?;
    let fence = match record.state() {
        SessionState::Open {} => record.open_teardown(&upload).map(|teardown| Fence {
            session: teardown.session().clone(),
            obligations: vec![teardown.obligation().clone()],
            attempt: None,
        }),
        SessionState::Completing { .. } => {
            let group = record.attempt_segment_group();
            let teardown = record.completing_teardown(&upload);
            teardown.zip(group).map(|(teardown, group)| Fence {
                session: teardown.session().clone(),
                obligations: vec![teardown.bytes().clone(), teardown.records().clone()],
                attempt: Some((upload, group)),
            })
        }
        SessionState::Aborting {} => {
            let Some(attempt) = record.epoch().checked_sub(1) else {
                return Ok(Plan::Settled);
            };
            let group = SegmentGroup::from_nonce(record.segment_nonce().clone(), attempt);
            return Ok(Plan::Fenced(upload, group));
        }
        SessionState::Completed { .. } => return Ok(Plan::Settled),
    };
    let fence = fence.ok_or(SessionUnsettled::EpochExhausted)?;
    Ok(Plan::Fence(Box::new(fence)))
}

/// Name a session the fence could not fence, on the audit seam and in the report.
fn unsettled(report: &mut RestoreReport, session: String, cause: SessionUnsettled) {
    emit_session_unsettled(&session, &cause);
    report
        .sessions_unsettled
        .push(UnsettledSession { session, cause });
}

/// Re-check the attempt (`group`, the session's own at `E'-1`) of an `Aborting@E'` session, so a
/// re-run names it again. Its deleter is `retire:records:s:<id>:<E'-1>`, filed by a `Completing`
/// fence, and trusted only if it owes `group` and nothing else: any other is the first record at
/// fault. Its absence proves nothing (a damaged or hand-repaired store): the range must be empty.
// deferred: #659 — a retire drain may delete part of this range, or the parts it is checked
// against, between two runs; a half-drained range can then read as chunks no part holds, or,
// its obligation dropped first, as records with no deleter.
async fn recheck_fenced(
    meta: &dyn MetadataStore,
    session: &str,
    upload: &UploadId,
    group: &SegmentGroup,
    report: &mut RestoreReport,
) -> Result<()> {
    let (upload_id, epoch, part) = (upload.clone(), group.epoch(), None);
    let token = RetireToken::Session {
        upload_id,
        epoch,
        part,
    };
    let key = retire_key(RetireMode::Records, &token);
    let Some(value) = meta.get(&key).await? else {
        let (left, _) = staged_page(meta, &seg_range_prefix(group), None).await?;
        if let Some((first, _)) = left.first() {
            unaccounted(report, session, object_name(first), SegmentFault::NoDeleter);
        }
        return Ok(());
    };
    let fault = match decode_retire_obligation(&key, &value) {
        Ok((_, _, owed)) if owed.segments() == Some(group) && owed.parts().is_none() => {
            return check_attempt(meta, session, upload, group, report).await;
        }
        Ok(_) => SegmentFault::NotOfAttempt,
        Err(fault) => SegmentFault::Undecodable {
            fault: fault.to_string(),
        },
    };
    unaccounted(report, session, object_name(&key), fault);
    Ok(())
}

/// Read a fenced attempt's `seg:` range (frozen: a segment write requires `Completing@E`) in bounded
/// pages ([`staged_page`]), and name the session at its first faulty record.
async fn check_attempt(
    meta: &dyn MetadataStore,
    session: &str,
    upload: &UploadId,
    group: &SegmentGroup,
    report: &mut RestoreReport,
) -> Result<()> {
    let range = seg_range_prefix(group);
    let (mut page, mut next) = staged_page(meta, &range, None).await?;
    if page.is_empty() {
        return Ok(());
    }
    let held = part_chunks(meta, upload).await?;
    loop {
        for (key, value) in &page {
            if let Some(fault) = segment_fault(key, value, &held) {
                unaccounted(report, session, object_name(key), fault);
                return Ok(());
            }
        }
        let after = match (next, page.last()) {
            (Some(_), Some((last, _))) => last.clone(),
            _ => return Ok(()),
        };
        (page, next) = staged_page(meta, &range, Some(&after)).await?;
    }
}

/// What is wrong with one record under the attempt's range, if anything. The range is the group's
/// own `seg:<nonce>:<E>:`, so a key there that parses is one of the group's.
fn segment_fault(key: &[u8], value: &[u8], held: &HashSet<ChunkId>) -> Option<SegmentFault> {
    if parse_seg_key(key).is_err() {
        return Some(SegmentFault::KeyNotOfGroup);
    }
    match metadata::decode::<SegmentRecord>(value) {
        Ok(segment) => segment
            .chunks()
            .iter()
            .find(|chunk| !held.contains(&chunk.id))
            .map(|chunk| SegmentFault::ChunkInNoPart { chunk: chunk.id }),
        Err(fault) => Some(SegmentFault::Undecodable {
            fault: fault.to_string(),
        }),
    }
}

/// Every chunk a `part:` record of `upload` holds. One that will not decode is skipped (the staged
/// read names it): it can only make a chunk look held by no part, naming the session.
async fn part_chunks(meta: &dyn MetadataStore, upload: &UploadId) -> Result<HashSet<ChunkId>> {
    let (range, mut held) = (part_range(upload), HashSet::new());
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = staged_page(meta, &range, after.as_deref()).await?;
        for (key, value) in &page {
            if let Ok(part) = parse_part_key(key).and_then(|_| decode_part_record(value)) {
                held.extend(part.chunks().iter().map(|chunk| chunk.id));
            }
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(held),
        }
    }
}

/// Name a fenced session whose attempt needs a human, on the audit seam and in the report.
fn unaccounted(report: &mut RestoreReport, session: &str, record: String, fault: SegmentFault) {
    let session = session.to_owned();
    let found = UnaccountedSegments {
        session,
        record,
        fault,
    };
    emit_segments_unaccounted(&found);
    report.segments_unaccounted.push(found);
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

/// Name what the staged read could not read or could not trust on the durability seam, the moment
/// that read returns — [`attribute_unresolvable`]'s placement and reasons, for the staged class.
///
/// An unreadable staged record joins `named`, so it is reported in
/// [`RestoreReport::unresolvable`] beside the committed objects and withholds every mark. A record
/// it read but cannot trust holds its chunk (the mark gate skips every fragment bearing its id), is
/// named on the audit seam, and is returned, once per record, for
/// [`RestoreReport::staged_untrusted`].
fn attribute_staged(staged: &StagedSet, named: &mut BTreeSet<Vec<u8>>) -> Vec<String> {
    for (record, fault) in &staged.unresolvable {
        if named.insert(record.clone()) {
            emit_unresolvable_staged(&object_name(record), fault);
        }
    }
    // Named in the report too (not clean, and no human: `RestoreReport::needs_human` says why).
    // By record, not chunk: one part record may hold several chunks.
    let mut untrusted = BTreeSet::new();
    for (&chunk, records) in &staged.held {
        for (record, fault) in records {
            emit_untrusted_staged(&object_name(record), chunk, fault);
            untrusted.insert(record.as_slice());
        }
    }
    untrusted.into_iter().map(object_name).collect()
}

/// How many fragments must survive for this chunk to be rebuildable: `k` under
/// Reed-Solomon, and 1 under `EcScheme::None` (the lone fragment *is* the data).
fn reconstruction_threshold(chunk: &wyrd_core::metadata::ChunkRef) -> u16 {
    match chunk.scheme {
        wyrd_core::metadata::EcScheme::None => 1,
        wyrd_core::metadata::EcScheme::ReedSolomon { k, .. } => u16::from(k),
    }
}

/// What the `pending:` scan found: the chunks it holds, and the entries it could not read.
struct PendingLedger {
    /// Chunk ids under a `pending:` key — a live lease's, **and** an unreadable entry's (held on
    /// the same terms; see [`RestoreReport::pending_unreadable`]).
    held: HashSet<ChunkId>,
    /// The entries [`wyrd_core::metadata::decode_pending_entry`] refused, by escaped key, in
    /// the store's own key order.
    unreadable: Vec<String>,
}

/// Chunk ids that still hold a `pending:` lease — an in-flight write, GC's business — read
/// through the namespace's one decode entry point, as every other `pending:` reader is.
///
/// A value that entry point refuses is not a lease this pass may reason from, and it is not
/// nothing either: the key still names a chunk, and the entry may be a misfiled owned staging
/// record protecting a write in flight. So its chunk is held as a live lease's would be, and the
/// entry is named on the audit seam and in the report — never read as a valid lease on the
/// strength of its key alone.
async fn pending_chunks(meta: &dyn MetadataStore) -> Result<PendingLedger> {
    let mut ledger = PendingLedger {
        held: HashSet::new(),
        unreadable: Vec::new(),
    };
    for (key, value) in meta.scan(b"pending:").await? {
        if let Err(fault) = wyrd_core::metadata::decode_pending_entry(&value) {
            let entry = object_name(&key);
            emit_unreadable_pending(&entry, &fault.to_string());
            ledger.unreadable.push(entry);
        }
        if let Some(chunk) = parse_pending_chunk(&key) {
            ledger.held.insert(chunk);
        }
    }
    Ok(ledger)
}

/// Emit a `pending:` entry this pass could **not read as an ordinary lease** on the
/// durability-plane seam (ADR-0011 / ADR-0012): its chunk is held, nothing of it is marked, and
/// the entry stays until a human repairs or refiles it — GC's expired-lease scan
/// (`gc::emit_unreadable_pending`) says the same of the same value.
fn emit_unreadable_pending(entry: &str, fault: &str) {
    tracing::warn!(monotonic_counter.restore_unreadable_pending_entries = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "unreadable-pending-entry",
        entry = %entry,
        fault = %fault,
        "post-restore: could not read a pending-ledger entry as an ordinary lease; its chunk is held unmarked and the entry is left in place — operator signal",
    );
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

/// A staged multipart record this pass could **not read** — a session key naming no upload, a part
/// key or value that will not parse or decode, an owned staging key naming no chunk — named on the
/// durability-plane seam (ADR-0011 / ADR-0012) as GC names the same record on its own
/// (`gc::emit_unresolvable_staged`). Which chunks it protects is unknown, so nothing was marked
/// anywhere in the fleet on its account, and the report names it in
/// [`RestoreReport::unresolvable`].
fn emit_unresolvable_staged(record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.restore_unresolvable_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "unresolvable-staged-record",
        record = %record,
        fault = %fault,
        "post-restore: a staged multipart record could not be read; nothing was marked anywhere in the fleet on its account — this run is NOT a clean bill for the store until this record is repaired",
    );
}

/// A staged multipart record this pass read but could **not trust** about where `chunk`'s
/// fragments are — a placement of the wrong length, or a value that will not decode under a key
/// that still names the chunk — named on the durability-plane seam as GC names it
/// (`gc::emit_untrusted_staged`). Every fragment of the chunk was held unmarked.
fn emit_untrusted_staged(record: &str, chunk: ChunkId, fault: &str) {
    tracing::warn!(monotonic_counter.restore_untrusted_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "untrusted-staged-record",
        record = %record,
        chunk = %wyrd_traits::chunk_hex(chunk),
        fault = %fault,
        "post-restore: a staged multipart record names this chunk but cannot be trusted about where its fragments are; every fragment of the chunk was held unmarked — operator signal",
    );
}

/// A session fenced to `Aborting@epoch` with its obligation, once that commit landed.
fn emit_session_fenced(session: &str, epoch: u64, obligation: &str) {
    tracing::info!(monotonic_counter.restore_sessions_fenced = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "session-fenced",
        session = %session,
        epoch,
        obligation = %obligation,
        "post-restore: an Open or Completing upload session is fenced to Aborting with the \
         obligations owing its records; it can no longer be completed",
    );
}

/// A fenced session whose attempt's segment records need a human. Operator signal.
fn emit_segments_unaccounted(found: &UnaccountedSegments) {
    tracing::warn!(monotonic_counter.restore_segments_unaccounted = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "session-segments-unaccounted",
        session = %found.session,
        record = %found.record,
        fault = %found.fault,
        "post-restore: a fenced upload session's segment records, or the retirement obligation \
         owing them, include a record nothing accounts for — NEEDS-HUMAN",
    );
}

/// A session the fence could **not** fence, and why — left as read. Operator signal.
fn emit_session_unsettled(session: &str, cause: &SessionUnsettled) {
    tracing::warn!(monotonic_counter.restore_sessions_unsettled = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "session-unsettled",
        session = %session,
        cause = %cause,
        "post-restore: could not fence an upload session; it is left as read — NEEDS-HUMAN",
    );
}

/// A fence commit that answered `Err` (its outcome possibly unknown): nothing is claimed for it.
fn emit_session_fence_failed(session: &str, fault: &str) {
    tracing::error!(
        target: "wyrd.custodian.restore.audit",
        action = "session-fence-failed",
        session = %session,
        fault = %fault,
        "post-restore: a session's fence commit failed, landed or not; the pass stops — re-run it",
    );
}

/// The restore-fence generation this pass just wrote: opened not complete, or completed.
fn emit_generation(generation: &FenceGeneration) {
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "fence-generation",
        generation = generation.generation(),
        complete = generation.is_complete(),
        "post-restore: the restore-fence generation record now names this pass's generation{}",
        if generation.is_complete() {
            ", complete: every upload session the store held is fenced or settled"
        } else {
            ", not complete"
        },
    );
}

/// The generation left not complete because a session needs a human. Operator signal.
fn emit_generation_left_open(generation: &FenceGeneration, report: &RestoreReport) {
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "fence-generation-not-complete",
        generation = generation.generation(),
        sessions_unsettled = report.sessions_unsettled.len(),
        segments_unaccounted = report.segments_unaccounted.len(),
        "post-restore: the restore-fence generation is left NOT complete — an upload session \
         needs a human; keep multipart uploads off this store, repair it and re-run — NEEDS-HUMAN",
    );
}

/// A generation write that answered `Err` (its outcome possibly unknown): nothing is claimed.
fn emit_generation_write_failed(generation: &FenceGeneration, fault: &str) {
    tracing::error!(
        target: "wyrd.custodian.restore.audit",
        action = "fence-generation-write-failed",
        generation = generation.generation(),
        complete = generation.is_complete(),
        fault = %fault,
        "post-restore: a restore-fence generation write failed, landed or not; the pass stops — \
         re-run it",
    );
}

/// The generation record could not be opened or closed ([`FenceGenerationFault`]).
fn emit_generation_fault(fault: &FenceGenerationFault) {
    tracing::error!(
        target: "wyrd.custodian.restore.audit",
        action = "fence-generation-fault",
        fault = %fault,
        "post-restore: the restore-fence generation record could not be written; the pass stops",
    );
}

/// Where a pass stopped short of finishing.
#[derive(Clone, Copy)]
enum Cut {
    /// A session fence commit failed.
    Fence,
    /// The fence finished, but the generation's completion write did not land as acknowledged.
    Generation,
}

impl Cut {
    fn text(self) -> &'static str {
        match self {
            Self::Fence => {
                "the session fence did not finish, and the pass returns an error — re-run it"
            }
            Self::Generation => {
                "the restore-fence generation's completion write did not land as acknowledged, \
                 and the pass returns an error — re-run it"
            }
        }
    }
}

/// The pass's own verdict, so a restore's true cost lands in one line an operator can read.
///
/// It says **complete** only when the reading finished and the pass did (`cut` is `None`). Over a
/// store with an unreadable committed or staged record in it the same line would otherwise be the
/// certification the rest of this pass refuses to give, in the one place an operator greps for
/// it. It is emitted even when a fence commit failed: it is the only record of some counts. The
/// restore-fence generation is reported beside it, as its own two fields.
fn emit_summary(report: &RestoreReport, cut: Option<Cut>) {
    const PARTIAL_READ: &str = "every count above covers only the records this pass could read";
    let finished = cut.is_none();
    let generation = report.fence_generation;
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "summary",
        stranded_marked = report.stranded_marked,
        already_marked = report.already_marked,
        pending_skipped = report.pending_skipped,
        staged_skipped = report.staged_skipped,
        pending_unreadable = report.pending_unreadable.len(),
        staged_untrusted = report.staged_untrusted.len(),
        displaced_kept = report.displaced_kept,
        dangling = report.dangling.len(),
        misplaced = report.misplaced.len(),
        under_replicated = report.under_replicated.len(),
        sessions_fenced = report.sessions_fenced,
        sessions_unsettled = report.sessions_unsettled.len(),
        segments_unaccounted = report.segments_unaccounted.len(),
        fence_finished = !matches!(cut, Some(Cut::Fence)),
        // The generation this pass opened, and whether it completed (0: none was opened).
        fence_generation = generation.map_or(0, |at| at.generation()),
        fence_generation_complete = generation.is_some_and(|at| at.is_complete()),
        // The qualifier on every count above: they are drawn over the records this pass could
        // read, and this is how many it could not.
        unresolvable = report.unresolvable.len(),
        // The pass's own two-word verdict, so the predicate the report offers its callers is the
        // one its audit trail states rather than a third rendering of the same fields — except
        // that a pass cut short may have left an `Open` session live, which no partial report
        // can call clean and only a human's re-run settles.
        clean = finished && report.is_clean(),
        needs_human = !finished || report.needs_human(),
        "post-restore reconciliation {}",
        match (report.unresolvable.is_empty(), cut) {
            (true, None) => "complete".to_owned(),
            (false, None) => format!("INCOMPLETE — {PARTIAL_READ}"),
            (true, Some(cut)) => format!("INCOMPLETE — {}", cut.text()),
            (false, Some(cut)) => format!("INCOMPLETE — {PARTIAL_READ}; and {}", cut.text()),
        },
    );
}
