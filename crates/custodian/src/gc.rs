//! The **GC custodian loop** (proposal 0005 §"The four custodian loops" / GC,
//! `0005:288-295`; the GC step of the reconstruction pipeline `0005:279`; the
//! correctness argument Q3 `0005:394-397`; the graduation invariant `0005:486-488`;
//! PR-sequence slice 4 `0005:524-527`).
//!
//! GC promotes the test-invoked stand-in (`core::sweep_expired_leases`,
//! `crates/core/src/write.rs:332`, which removed only the `pending:` ledger entry and
//! explicitly **deferred the fragment-byte reclaim**, `write.rs:330-331`) into a
//! running reconciliation loop dispatched from the fenced control point
//! ([`crate::reconcile_step`]). It reclaims the **two** GC inputs (`0005:288-291`):
//!
//! 1. the **bytes behind an expired pending-ledger lease** — the leased garbage a
//!    crashed write/repair fan-out leaves (`0005:289-290`); and
//! 2. an **orphaned fragment** — present in a D server's
//!    [`ChunkStore::list_fragments`] but referenced by **no** committed chunk map
//!    (from deletes and completed reconstructions, `0005:290-291`).
//!
//! Bytes are reclaimed via [`ChunkStore::delete_fragment`] **only after a reader-safe
//! grace window** — long enough that an in-flight reader holding the prior version is
//! never torn (`0005:291-294`; the pending-ledger sweep pattern of architecture §5).
//!
//! The `orphan:` ledger is **walked, never read whole** (proposal 0016, `0016:1392-1408`):
//! each pass reads one window of at most [`ORPHAN_WINDOW`] entries, in cursor-keyed
//! [`MetadataStore::scan_page`] pages, from where the previous pass stopped, and returns to
//! the head of the ledger at its end ([`OrphanWindow`]). What a pass concludes from its
//! window is only what that window can show: a fragment whose mark lies outside it is left
//! for a later pass, never reclaimed on other evidence meanwhile. Its own writes commit in
//! batches of at most [`CLEANUP_BATCH`].
//!
//! The loop's load-bearing invariant, whose violation is **silent corruption**:
//! **never reclaim a referenced fragment** — a fragment a committed chunk map's
//! placement record points at is **never** passed to `delete_fragment`
//! (`0005:294-295`, Q3 `0005:394-397`, graduation invariant `0005:488`). The
//! **reference set itself** is built through the one shared resolver (proposal 0016
//! decision 7(e), [`referenced_fragments`]), so a **segmented** object's chunks are in
//! it too; a committed object the build cannot read makes the set **incomplete**
//! ([`ReferenceSet::unresolvable`]), which reclaims nothing fleet-wide and certifies
//! nothing rather than guess (`docs/principles.md` §5 C-1). The one object's fault is
//! contained: it is attributed, and the walk — and every other object's protection —
//! continues.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this loop stays over the
//! `traits` / `core` seams plus `tracing` — **no** concrete backend.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// The orphan-ledger key protocol lives in `core::metadata` (beside `pending_key`) so the
// delete path that WRITES a grace record and this GC loop that READS it share one
// definition and can never key-format-drift (issue #364). Re-exported `pub(crate)` here so
// the other orphaning loops (`reconstruction.rs`, `rebalance.rs`) keep calling
// `crate::gc::orphan_key` unchanged.
pub(crate) use wyrd_core::metadata::orphan_key;
use wyrd_core::metadata::{
    self, parse_orphan_key, ChunkMapError, EcScheme, InodeRecord, InodeState, MalformedPlacement,
    ORPHAN_PREFIX,
};
use wyrd_traits::{
    BoxError, ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result, ScanPage,
    WriteBatch, SCAN_CAP,
};

use crate::reconciliation::Reconciled;

/// **B** — how many `orphan:` ledger entries one GC pass reads: its *window* of the ledger.
///
/// The ledger is never read by one `scan`. `scan` fails whole past [`SCAN_CAP`] and returns no
/// partial result, and one maximum segmented-object retirement installs ~1.78 M marks against a
/// cap of 1,048,576 (`0016:1392-1398`) — so a single large delete would fail GC on every pass
/// from then on, the pass that should shrink the ledger being the one that cannot start
/// (`0016:1396-1398`). A pass instead reads at most `B` entries, counted across however many
/// [`MetadataStore::scan_page`] pages the store's own page cap splits them into, resuming where
/// the previous pass stopped ([`OrphanWindow`]).
///
/// Derivation: `SCAN_CAP / 16` = 65,536. A pass already holds up to `SCAN_CAP` entries of each
/// of two other namespaces (its `inode:` and `pending:` scans), so its window of the ledger adds
/// at most a sixteenth of one scan's heap bound again, whatever the ledger's size. At that rate a
/// ledger holding the largest single retirement 0016 sizes drains in ⌈1,780,000 / 65,536⌉ = 28
/// passes.
pub(crate) const ORPHAN_WINDOW: usize = SCAN_CAP / 16;

/// **W** — the most writes one commit of a GC pass's own ledger cleanup carries.
///
/// A pass deletes the `orphan:` marks it consumed and the `pending:` entries it retired in
/// commits of at most `W` deletes each, never in one batch sized by the pass: that batch grew
/// with the ledger, and one past the backend's transaction envelope failed AFTER the fragments
/// were already deleted, leaving their marks behind. `W` is restore's `MARK_BATCH` precedent
/// (`restore.rs`), the existing bounded commit against the same envelope.
///
/// **Bytes: bounded.** Every key a cleanup commit deletes is an
/// `orphan:<dserver>:<chunk>:<index>` key (at most 73 bytes) or a `pending:<chunk>` key (at most
/// 47), so `W` of them come to under 73 KB — far inside the 10 MB per transaction of the
/// inherited envelope (`MetadataStore`, "Operational envelope").
///
/// **Operations: NOT calibrated.** The envelope's other half is 5 s per transaction, and a
/// backend applies a batch's mutations one after another inside it — TiKV takes a lock round
/// trip per delete — so whether `W` deletes finish inside 5 s depends on the deployment's round
/// trip time, and a batch of ~1,000 small marks can exceed it (`0016:630-636`). Proposal 0016
/// bounds every such batch by an operation-count knob `B_ops` calibrated against the slowest
/// supported backend (`0016:640-643`); `W` is not that calibration. What it does guarantee is
/// that a cleanup commit's size is a constant, independent of the ledger and of the pass.
pub(crate) const CLEANUP_BATCH: usize = 1_000;

/// Where the `orphan:` ledger walk resumes: the one persisted record of [`OrphanWindow`].
///
/// Its value is the last ledger key a pass read — the exclusive cursor the next pass starts
/// after — or empty to start at the head of the ledger (as does an absent record, before the
/// first lap has ended, or a value that is not an `orphan:` key at all). It is a position, never
/// evidence: whatever it holds, the walk only reads from there, so a stale or damaged value
/// costs at most a lap, never a mark.
///
/// It lives in the metadata store because nothing else survives between passes: the deployed
/// loop builds a fresh [`GcContext`] every pass (`crates/server/src/custodian.rs`), and shared
/// mutable globals are ruled out (ADR-0035). Being in the store also carries the walk across a
/// leader change. Outside `orphan:`, so the walk never reads its own bookmark.
pub(crate) const ORPHAN_CURSOR_KEY: &[u8] = b"gc:orphan-cursor";

pub(crate) fn parse_pending_chunk(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}

/// What the GC reconciler reads and reclaims over: the authoritative metadata store
/// (committed chunk maps + the pending / orphan ledgers) and the **fleet** of D
/// servers, each a [`ChunkStore`] keyed by its stable [`DServerId`]. The
/// `grace_window_millis` is the reader-safe window an **orphaned** fragment must
/// outlive before reclamation — **derived** from reader version-hold / lease
/// semantics by the caller, not a magic constant baked into GC (`0005:585-586`).
///
/// This is the input the running control point hands GC; it is **not** a deployed
/// custodian process (Option A, `0005:524-527`) — standing up the host that drives
/// the loop against live stores is a later slice. The loop is correct over these
/// abstractions and reachable through the real [`crate::reconcile_step`].
pub struct GcContext<'a> {
    /// The authoritative metadata store.
    pub meta: &'a dyn MetadataStore,
    /// The fleet of D servers to sweep, each addressed by its stable id.
    pub fleet: &'a [(DServerId, &'a dyn ChunkStore)],
    /// The reader-safe grace window (logical millis) an orphan must outlive.
    pub grace_window_millis: u64,
    /// Whether input (1) — expired pending-lease garbage — may be reclaimed this
    /// pass. See [`ExpiredPendingPolicy`] for why a deployed caller must defer it.
    pub expired_pending: ExpiredPendingPolicy,
}

/// Policy for GC input (1): the bytes a crashed write fan-out left under an expired
/// `pending:` lease.
///
/// "Expired" is only as trustworthy as the lease **stamp**. GC classifies with the
/// caller's `now_millis`, so reclaiming on an expired lease is sound ONLY when every
/// producer that stamps `pending:` leases shares that clock. The CLI write path does
/// not: it stamps leases from a fixed logical clock (`cli.rs` `NOW_MILLIS = 0`, so
/// `lease_expiry = 60_000` — one minute past the Unix epoch), which a wall-clocked
/// deployed pass reads as expired **while the write is still in flight**. Sweeping it
/// deletes the mid-flight fan-out and lets the writer commit a chunk map over missing
/// bytes — silent data loss on a shared write-taking backend (#557). Until every
/// producer stamps live leases (the #490 lease-liveness work), a deployed pass must
/// [`Self::Defer`]; [`Self::Reclaim`] is for callers that control every lease stamp
/// (the in-process test/DST wiring) or a backend attested to be taking no writes.
///
/// Input (2) — orphaned fragments — is unaffected: an orphan record is written by the
/// delete/repair path only AFTER the referencing commit is gone, so its fragment is
/// unreferenced no matter whose clock stamped it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiredPendingPolicy {
    /// Reclaim expired-lease garbage. Sound only when every `pending:` stamp shares
    /// the reconciler's clock — or the backend is attested write-free.
    Reclaim,
    /// Keep every `pending:` entry and the fragments under it untouched this pass —
    /// deferred, never mistaken for collected. A later pass under a live-lease regime
    /// reclaims them.
    Defer,
}

/// Record that `frag` on `dserver` became **orphaned** at `orphaned_at_millis` — the
/// grace-record an orphaning operation (delete / completed reconstruction, later
/// slices) writes so GC can honour the reader-safe window before reclaiming the
/// bytes. Idempotent at the metadata layer (a plain put).
pub async fn mark_orphaned(
    meta: &impl MetadataStore,
    dserver: DServerId,
    frag: FragmentId,
    orphaned_at_millis: u64,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(
        orphan_key(dserver, frag),
        orphaned_at_millis.to_string().into_bytes(),
    ))
    .await?;
    Ok(())
}

/// One GC reconciliation pass over `ctx` at logical time `now_millis`. Dispatched
/// only from [`crate::reconcile_step`] (the fenced control point) — never a parallel
/// entry.
///
/// Returns [`Reconciled::Blocked`] if the reference set is **incomplete** — at least one
/// committed object's chunk map could not be read ([`ReferenceSet::unresolvable`]), so
/// [`ReferenceSet::protects`] withheld every fragment in the fleet and this pass may not
/// report the store converged — [`Reconciled::Changed`] if any fragment bytes were
/// reclaimed, and [`Reconciled::Satisfied`] otherwise. Scrub answers the identical
/// condition the identical way ([`crate::scrub::reconcile`]): one incomplete set, one
/// rule, read twice. A pass whose window of the `orphan:` ledger ([`OrphanWindow`]) stopped
/// short of the ledger's end and reclaimed nothing answers [`Reconciled::Partial`], never
/// `Satisfied`: `Satisfied` certifies that reality matched, and a caller driving the loop to
/// satisfaction would stop on it with eligible marks still unvisited in the windows ahead (PR
/// #802 review). `Satisfied` is therefore "this pass read the ledger to its end and reclaimed
/// nothing".
pub(crate) async fn reconcile(ctx: &GcContext<'_>, now_millis: u64) -> Result<Reconciled> {
    // The reference set is the safety gate: every fragment a *committed* chunk map's
    // placement record points at. A fragment in this set is NEVER reclaimed
    // (`0005:294-295`, Q3 `0005:394-397`) — its violation is silent corruption.
    let referenced = referenced_fragments(ctx.meta).await?;
    // Malformed committed placement (ADR-0040 decision 4, "strict maintenance"): a
    // non-empty, wrong-length vector can only be truncation/corruption. GC FAILS SAFE —
    // the chunk is treated as fully referenced below (none of its fragments is ever
    // reclaimed) — and surfaces each one as an operator signal on the durability seam,
    // instead of silently identity-filling the missing tail into the reference set.
    for (&chunk, m) in &referenced.malformed {
        emit_malformed(chunk, m.expected, m.actual);
    }
    // **An INCOMPLETE reference set authorizes no reclamation.** A committed object whose
    // chunk map this pass could not read contributes no fragments to the set, so no
    // fragment in the fleet can be shown NOT to be one of its (unknown) chunks —
    // [`ReferenceSet::protects`] therefore withholds every one of them, and the outcome
    // below refuses to certify. Attributed here, per object, and BEFORE the fleet walk, so
    // a transient store fault later in the pass cannot cost the operator the name of the
    // record to repair. Emitted by the CONSUMER, not by the shared builder: the same set
    // is read by scrub, restore and the drain-status surface, and a GC counter incremented
    // from inside the builder would tick for passes GC never ran.
    for (object, fault) in &referenced.unresolvable {
        emit_unresolvable(&object_name(object), fault);
    }
    // Input (1): chunks whose pending lease has expired — their fan-out garbage is
    // collectable (the lease TTL already encodes the crashed-write grace). GATED on the
    // caller's policy: a deployed pass cannot trust "expired" while any producer stamps
    // logical-clock leases (#557 / #490 — see [`ExpiredPendingPolicy`]), so under `Defer`
    // this input is empty and every `pending:` entry and its fragments survive untouched.
    let expired_pending = match ctx.expired_pending {
        ExpiredPendingPolicy::Reclaim => expired_pending_chunks(ctx.meta, now_millis).await?,
        ExpiredPendingPolicy::Defer => HashSet::new(),
    };
    // Input (2): ONE WINDOW of the orphan ledger — at most `ORPHAN_WINDOW` entries, paged from
    // where the previous pass stopped. Never the whole prefix in one `scan`: past `SCAN_CAP`
    // that read fails whole on every pass, and the pass that should shrink the ledger would be
    // the one that cannot start (`0016:1392-1408`).
    let window = OrphanWindow::read(ctx.meta).await?;
    // Where the NEXT pass resumes is recorded before this one acts on its window, so a fault
    // later in the pass cannot pin the walk here for good: the window's marks are all still in
    // the ledger, and the walk reads them again on its next lap.
    window.record_resume_point(ctx.meta).await?;

    let mut changed = false;
    let mut cleanup = Cleanup::new(ctx.meta);
    // For the expired-lease input, per chunk: did this pass reclaim any of its bytes, and does
    // any unprotected fragment of it survive the pass? Its `pending:` entry is the one record
    // naming every such survivor, so it is retired only when the first holds and the second
    // does not (below).
    let mut reclaimed_expired: BTreeSet<ChunkId> = BTreeSet::new();
    let mut still_held: HashSet<ChunkId> = HashSet::new();

    for &(dserver, store) in ctx.fleet {
        for frag in store.list_fragments().await? {
            // SAFETY GATE — never reclaim a referenced fragment. A fragment of a
            // malformed-placement chunk is protected the same way (fail safe): its true
            // placement cannot be trusted, so every fragment bearing its id is off-limits;
            // so is every fragment at all while the set is incomplete. The set itself says
            // WHICH rule held, so the audit trail never files an unrelated orphan under
            // `referenced` when what actually saved it was a blanket containment.
            if let Some(reason) = referenced.protection(dserver, frag) {
                emit_skip(dserver, frag, reason);
                continue;
            }

            let mark = window.mark_of(dserver, frag);
            let reason = if let Some(ReadMark::Stamped(since)) = mark {
                // Orphan input: reclaim ONLY after the reader-safe grace window.
                if now_millis >= since.saturating_add(ctx.grace_window_millis) {
                    Some("orphan")
                } else {
                    emit_skip(dserver, frag, "within-grace");
                    None
                }
            } else if mark.is_none()
                && expired_pending.contains(&frag.chunk)
                && window.covers_mark_of(dserver, frag)
            {
                // Expired pending-lease input: the lease TTL is its grace — for a fragment this
                // window shows has NO mark, and for no other. A mark outranks the lease: it may
                // still be inside its own grace window, which a reader holding the prior version
                // is relying on. So the arm needs the window to have covered the key position
                // the fragment's mark would occupy and found nothing there. A mark this pass did
                // not read — outside its window — is unknown, not absent; one it read and could
                // not decode is a mark all the same (`ReadMark::Unreadable`). Both fall to the
                // conservative arm below and wait for a pass that can see them.
                //
                // "Covered and absent" is sound although the page that covered the position may
                // have been read a moment ago: every in-tree writer of a mark that dereferences
                // (unlink, supersede, repoint, evacuation) writes it in the SAME commit that
                // dereferences the fragment, and the reference set this pass gates on was read
                // before this window was — a fragment dereferenced after that read is still
                // protected this pass. The one writer that marks without dereferencing, the
                // post-restore pass, never marks a fragment whose chunk holds a `pending:` entry.
                Some("expired-lease")
            } else {
                // No evidence the grace window elapsed — conservatively keep it
                // (reader-safe: a fragment is never reclaimed without a deadline).
                None
            };

            if let Some(reason) = reason {
                store.delete_fragment(frag).await?;
                emit_reclaim(dserver, frag, reason);
                if reason == "orphan" {
                    // Consume the mark this pass read and judged — the one key it holds a
                    // licence to delete. An expired-lease reclaim deletes no ledger key at all:
                    // its window found none at this position, and a key it did not read is not
                    // one to destroy.
                    cleanup.delete(orphan_key(dserver, frag)).await?;
                }
                if expired_pending.contains(&frag.chunk) {
                    reclaimed_expired.insert(frag.chunk);
                }
                changed = true;
            } else if expired_pending.contains(&frag.chunk) {
                // An unprotected fragment of an expired-lease chunk survives this pass — a mark
                // in its grace window, one outside this window, or none that can be seen yet.
                still_held.insert(frag.chunk);
            }
        }
    }

    // Retire the swept pending-ledger entries (the byte reclaim the stand-in deferred,
    // `write.rs:330-331`) — but an entry is CHUNK-WIDE evidence, and under a paged walk one pass
    // may reclaim some of a chunk's fragments while others wait for a later window. Deleting the
    // entry then would leave those with neither the entry nor a mark: evidence-free bytes GC
    // keeps forever. So an entry goes only once this pass has reclaimed bytes of its chunk and no
    // unprotected fragment of it is left for the entry to account for.
    for chunk in reclaimed_expired {
        if !still_held.contains(&chunk) {
            cleanup.delete(metadata::pending_key(chunk)).await?;
        }
    }
    cleanup.finish().await?;

    Ok(if !referenced.unresolvable.is_empty() {
        // Refuse to certify — whatever this pass reclaimed above is durable either way (a
        // reclaim never depended on the object it could not read, `ReferenceSet::protects`
        // withheld everything). What answering `Changed` / `Satisfied` would destroy is the
        // only signal that this pass could not see every committed object's chunks: an
        // operator reading `Satisfied` is being told the store converged, and would act on
        // it — decommission the server, close the ticket (`docs/principles.md` §5 C-1).
        Reconciled::Blocked
    } else if changed {
        Reconciled::Changed
    } else if window.is_partial() {
        // Nothing reclaimed in THIS window, and more of the ledger lies beyond it: not a
        // certification. The next pass resumes where this one stopped.
        Reconciled::Partial
    } else {
        Reconciled::Satisfied
    })
}

/// The **committed reference set** GC and scrub gate on: every fragment a *valid*
/// committed chunk map places, keyed by its placed D server, **plus** the chunk ids
/// whose committed placement is **malformed** (ADR-0040 decision 4). A pending
/// (uncommitted) inode's provisional map is excluded — only a committed reference
/// protects bytes.
///
/// A malformed committed placement (non-empty, `len != fragment_count()`) is deliberately
/// **not** expanded into `placed`: its identity-filled tail would be fabricated, so the
/// chunk is recorded in `malformed` and treated as **fully referenced** instead — every
/// fragment bearing its id is protected (fail safe), because its true placement cannot be
/// trusted (truncation / corruption).
///
/// A committed object whose map could not be **read at all** (`unresolvable`) is the same
/// rule one level up: its chunk ids are not merely untrustworthy, they are *unknown*, so
/// the set as a whole is **incomplete** — it authorizes no reclamation ([`Self::protects`])
/// and certifies nothing (each reading loop's outcome).
pub(crate) struct ReferenceSet {
    /// `(dserver, fragment)` a valid committed chunk map references.
    pub placed: HashSet<(DServerId, FragmentId)>,
    /// Chunk ids whose committed placement is malformed, each with its classification.
    pub malformed: HashMap<ChunkId, MalformedPlacement>,
    /// The committed [`EcScheme`] of each validly-placed chunk, so a consumer verifying a
    /// referenced fragment against the chunk map can check its header's FULL identity —
    /// `ec_fragment_index` and the EC tuple, not the `chunk_id` alone
    /// (`wyrd_core::repair::header_matches_identity`, the scrub/verify contract
    /// `0005:262-267`).
    pub schemes: HashMap<ChunkId, EcScheme>,
    /// Committed objects whose chunk map this build could **not** read, keyed by the
    /// `inode:` key exactly as the store spells it — **the raw bytes** — and valued by the
    /// fault that stopped it: attribution, so the blocker is repairable rather than merely
    /// known to be somewhere.
    ///
    /// Keyed by bytes rather than by a rendered name, because the key is what identifies
    /// the record and a rendering need not be injective: `String::from_utf8_lossy` maps
    /// every distinct invalid byte onto the same replacement character, so two damaged
    /// records could collapse into one entry here and one of them would go unreported —
    /// the silent skip this whole rule exists to prevent. Named for the operator at the
    /// point of emission instead ([`object_name`], which escapes rather than replaces).
    /// Not parsed, either: a key that would not parse is still a record a human has to go
    /// and find. Ordered (a `BTreeMap`, in the store's own byte order), so the audit trail
    /// two consumers emit over one set is in the same order.
    ///
    /// While this is non-empty the set is **incomplete**, and *every* consumer of it must
    /// say so in its own answer — see [`Self::protects`] for the reclamation side and each
    /// loop's outcome for the certification side.
    pub unresolvable: BTreeMap<Vec<u8>, String>,
}

impl ReferenceSet {
    /// **Why** `frag` on `dserver` is protected from reclamation — the audit reason — or
    /// `None` when nothing protects it and it may be judged on its own evidence.
    ///
    /// The reason is returned rather than left to each caller to re-derive, so a skip is
    /// never filed under a rule that did not actually hold: while the set is incomplete
    /// EVERY fragment is withheld, including orphans and expired-lease garbage that no
    /// chunk map references, and recording those as `referenced` would tell an operator
    /// the store is healthier than it is.
    pub fn protection(&self, dserver: DServerId, frag: FragmentId) -> Option<&'static str> {
        if self.placed.contains(&(dserver, frag)) {
            Some("referenced")
        } else if self.malformed.contains_key(&frag.chunk) {
            Some("malformed-placement")
        } else if !self.unresolvable.is_empty() {
            Some("incomplete-reference-set")
        } else {
            None
        }
    }

    /// Whether `frag` on `dserver` is protected from reclamation — a valid placed
    /// reference, *any* fragment of a malformed (fully-referenced) chunk, or **anything at
    /// all** while the set is incomplete.
    ///
    /// That last clause is the containment rule for an object whose map could not be read
    /// (0016 decision 7(e)). Unlike a malformed placement — where the chunk id is known and
    /// only its placement is not — an unreadable map hides *which chunks the object owns*,
    /// so no fragment in the fleet can be shown not to be one of them: a partial reference
    /// set authorizes nothing (`docs/principles.md` §5 C-1). It is enforced HERE rather
    /// than left to each caller to remember, because every deletion-capable pass already
    /// gates on this one predicate (`gc.rs`'s safety gate, `restore.rs`'s mark gate) — so
    /// the containment holds for all of them or for none. The cost is a leak until the
    /// object is repaired; the alternative is deleting a live object's bytes.
    pub fn protects(&self, dserver: DServerId, frag: FragmentId) -> bool {
        self.protection(dserver, frag).is_some()
    }
}

/// Build the [`ReferenceSet`] over every **committed** chunk map, resolving each one
/// through the ONE resolver every consumer shares ([`metadata::resolve_chunk_map`],
/// proposal 0016 decision 7(e)) before classifying its committed placement (ADR-0040
/// decision 4). A flat map is borrowed and costs no extra read; a **segmented** one is read
/// from its own bounded `seg:<nonce>:<epoch>:` range — which is what puts a segmented
/// object's chunks in this set at all, instead of leaving every fragment it owns looking
/// unreferenced to the pass that deletes.
///
/// **One damaged object does not end the walk.** A record that will not decode, or a
/// generation the resolver cannot read on a root that still names it
/// ([`wyrd_core::metadata::ChunkMapError`]), is recorded in [`ReferenceSet::unresolvable`]
/// and the walk goes on: the set is then *incomplete*, which [`ReferenceSet::protects`]
/// turns into "reclaim nothing" and each reading loop turns into "certify nothing". That is
/// the containment shape this repo already uses for a record it cannot trust
/// (`ReconciliationStatus::PendingMalformed`, `crates/custodian/src/desired_state.rs`:
/// attribute the blocker, name it, keep answering). Ending the walk instead would cost
/// every *healthy* object in the store its protection and its verification, and would blank
/// the drain-status surface fleet-wide, over one damaged record.
///
/// A fault that is **not** this object's own — a store access failing underneath the
/// resolver, which the resolver itself does not describe as a chunk-map anomaly — still
/// propagates (`?`): a walk that cannot read the metadata store has no reference set at
/// all, incomplete or otherwise, and containing that as "one object is unreadable" would be
/// the wrong answer for every object in it.
pub(crate) async fn referenced_fragments(meta: &dyn MetadataStore) -> Result<ReferenceSet> {
    let mut placed = HashSet::new();
    let mut malformed = HashMap::new();
    let mut schemes = HashMap::new();
    let mut unresolvable = BTreeMap::new();
    for (key, value) in meta.scan(b"inode:").await? {
        // The record's own bytes are already in hand, so a decode failure is THIS object's
        // fault and no store's: a structurally invalid map (a `segment_count` disagreeing
        // with its table, say) is rejected at decode — structural invariants surface as
        // errors, never as values (ADR-0045) — and never reaches the resolver below. It is
        // contained exactly as an unreadable generation is, for the same reason.
        //
        // Conservatively, WITHOUT first asking whether the record was committed: reading
        // `state` out of bytes that will not decode needs a lenient peek, and this loop
        // holds the ADR-0010 boundary of `traits` / `core` / `tracing` (module docs above)
        // — it owns no decoder of its own to do it with. So an unreadable record blocks
        // until it is repaired, which is fail-closed; the alternative direction (assume it
        // was uncommitted, and reclaim on) is the silent-corruption one.
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
        // Resolve through the shared resolver, restarting onto the live root if this scan's
        // snapshot was superseded mid-resolve (decision 7(h)). `Ok(None)` means no live
        // committed generation is left under this key (deleted or retired since the scan
        // read it) — there is nothing left to reference, so it is skipped exactly as an
        // already-uncommitted record is above.
        //
        // The network bound on this await is the `MetadataStore` IMPLEMENTATION's, not this
        // caller's (#508/#636, three times over) — the same rule the `meta.scan(b"inode:")`
        // above has always followed, and no await in any of the four custodian loops carries
        // a caller-side timeout. Wrapping this one would mean a production `tokio` dependency
        // in a crate whose seam boundary is `traits` / `core` / `tracing` (ADR-0010, module
        // docs above), and would bound one read of a pass built from many. It is fail-closed
        // either way: an error here either propagates or contains the object — it is never
        // read as "this object owns no bytes".
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
                // Not a chunk-map anomaly: a store fault under the read. Not this object's
                // fault, so it is not folded into "this object is unreadable".
                Err(err) => return Err(err),
            },
        };
        for chunk in resolved.chunks.iter() {
            // Classify the committed placement BEFORE expanding it via the shared strict
            // companion (`ChunkRef::checked_fragments`, `metadata.rs`, ADR-0040 decision
            // 4). A valid (empty / full-length) vector resolves through the same
            // authoritative identity-fallback the read path and reconstruction use — a
            // pre-M3 / mixed-era chunk with an empty `placement` (decoded via
            // `#[serde(default)]`, `metadata.rs:93`) resolves fragment `i` to D-server
            // `i`, closing the pre-M3 silent-loss gap (issue #287). A MALFORMED vector is
            // NOT identity-filled into `placed`; the chunk is recorded as fully referenced
            // instead, so GC never reclaims any of its fragments.
            match chunk.checked_fragments() {
                Ok(frags) => {
                    for (index, dserver) in frags {
                        placed.insert((
                            dserver,
                            FragmentId {
                                chunk: chunk.id,
                                index,
                            },
                        ));
                    }
                    // Record the committed scheme so scrub can verify each referenced
                    // fragment's full identity (index + EC tuple) against the chunk map,
                    // not its `chunk_id` alone.
                    schemes.insert(chunk.id, chunk.scheme);
                }
                Err(m) => {
                    malformed.insert(chunk.id, m);
                }
            }
        }
    }
    Ok(ReferenceSet {
        placed,
        malformed,
        schemes,
        unresolvable,
    })
}

/// How a blocker is named to an operator: the `inode:` key as the store spells it, with
/// every byte outside printable ASCII escaped as `\xNN` (and `\` itself doubled).
/// Attribution, never a parse — a key whose bytes are not UTF-8 is still a record a human
/// has to go and find, so it gets a name rather than being dropped.
///
/// **Escaped rather than rendered lossily**, because this name is how one blocker is told
/// from another. `String::from_utf8_lossy` is not injective — every invalid byte becomes
/// the same `U+FFFD` — so `inode:\xfe` and `inode:\xff` would arrive at the operator (and
/// at the drain-status answer, `crate::desired_state::ReconciliationStatus`) under one
/// name, and a repair guided by it would fix one record and leave the other blocking the
/// fleet with nothing left pointing at it. Escaping is injective: `\` is the only
/// character an escape can start with and it is itself escaped, so distinct keys always
/// have distinct names. An ordinary `inode:1` is unchanged by it.
pub(crate) fn object_name(key: &[u8]) -> String {
    let mut name = String::with_capacity(key.len());
    for &byte in key {
        match byte {
            b'\\' => name.push_str(r"\\"),
            0x20..=0x7e => name.push(byte as char),
            _ => name.push_str(&format!("\\x{byte:02x}")),
        }
    }
    name
}

/// The chunk ids whose pending-ledger lease has expired as of `now_millis`.
///
/// A `pending:` value that does not read as an **ordinary** lease — an owned multipart staging
/// entry filed under the wrong key, a torn or malformed value — is **classified and skipped**:
/// its chunk never enters the expired set, so none of its fragments is reclaimed as expired-lease
/// garbage and its entry is never deleted, and the scan goes on for every other entry. That is
/// ADR-0045 decision 3 for a GC sweep — classify, skip and emit NEEDS-HUMAN, and fail safe rather
/// than reclaim on doubt — with the containment the `malformed-placement` skip gives a corrupt
/// committed placement: each skipped entry is named to an operator on the durability seam
/// ([`emit_unreadable_pending`], as [`emit_malformed`] names that chunk), so the skip is never
/// silent. It is not an error: a `?` here would fail the whole pass, and every other reclaim in
/// it, over one record — the stall decision 3 rules out. Nor is a repair record written for it:
/// the unreadable entry stays where it is, and every pass that reads this input names it again
/// until a human repairs or refiles it. Only a pass under [`ExpiredPendingPolicy::Reclaim`]
/// reads it: under `Defer`, a deployment's default, nothing here runs and the entry is simply
/// left in place, unnamed.
async fn expired_pending_chunks(
    meta: &dyn MetadataStore,
    now_millis: u64,
) -> Result<HashSet<ChunkId>> {
    let mut set = HashSet::new();
    for (key, value) in meta.scan(b"pending:").await? {
        let entry = match metadata::decode_pending_entry(&value) {
            Ok(entry) => entry,
            Err(fault) => {
                emit_unreadable_pending(&object_name(&key), &fault.to_string());
                continue;
            }
        };
        if entry.lease_expiry_millis <= now_millis {
            if let Some(chunk) = parse_pending_chunk(&key) {
                set.insert(chunk);
            }
        }
    }
    Ok(set)
}

/// What one pass's [`OrphanWindow`] read of a fragment's own mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadMark {
    /// The mark, stamped with the instant its fragment was orphaned.
    Stamped(u64),
    /// The mark, holding a value that does not read as an instant. Still a mark: it is evidence
    /// that something stranded this fragment, and the instant it would have given is unknown,
    /// so it can neither start nor end a grace window — its fragment is kept, on no other
    /// evidence, until a human repairs the value ([`emit_unreadable_mark`]).
    Unreadable,
}

/// One GC pass's **window** of the `orphan:` ledger: at most [`ORPHAN_WINDOW`] entries, read
/// in [`MetadataStore::scan_page`] pages from where the previous pass stopped, and the key range
/// they span.
///
/// The walk resumes from the cursor in [`ORPHAN_CURSOR_KEY`] and returns to the head of the
/// ledger once a pass reaches its end, so every entry present for a whole lap is read once per
/// lap (the no-skip-for-stable-keys clause of `scan_page`). An entry written behind the walk
/// after its page was read may be missed until the next lap; nothing is concluded from that miss
/// that could not be concluded from absence (see [`OrphanWindow::covers_mark_of`]).
///
/// Only a key a writer could have written is read as a mark: its fragment's own key, exactly as
/// [`orphan_key`] spells it. [`parse_orphan_key`] reads each field as a plain integer, so
/// `orphan:5:01:0` decodes to the position `orphan:5:1:0` names — a spelling no writer produces,
/// sorting elsewhere in the ledger, possibly in a different window from the mark itself. Such a
/// key, or one that does not parse at all, is classified, skipped and named on the audit seam
/// ([`emit_malformed_orphan_key`]), never taken for the mark of the position it parses to.
pub(crate) struct OrphanWindow {
    /// The exclusive lower end of the key range read: the cursor this pass resumed from, or
    /// `None` when it started at the head of the ledger.
    after: Option<Vec<u8>>,
    /// The inclusive upper end: the last key read, or `None` when the pass reached the end of the
    /// ledger (the range then runs to the end of the prefix). Also where the next pass resumes.
    through: Option<Vec<u8>>,
    /// The marks read, keyed by the fragment each is its own key for.
    marks: HashMap<(DServerId, FragmentId), ReadMark>,
    /// The cursor record's value as this pass found it, so it is rewritten only when it moves.
    stored: Option<Vec<u8>>,
}

impl OrphanWindow {
    /// Read this pass's window: resume from the persisted cursor and page until [`ORPHAN_WINDOW`]
    /// entries are in hand or the ledger ends, whichever comes first.
    async fn read(meta: &dyn MetadataStore) -> Result<Self> {
        let stored = meta
            .get(ORPHAN_CURSOR_KEY)
            .await?
            .map(|value| value.to_vec());
        // Only a key the ledger could hold is a place to resume: under the prefix AND no longer
        // than the longest key a writer can spell. The empty value is the head, as
        // `record_resume_point` writes it. Anything else — a torn, damaged or oversized value a
        // restore or a fault left here — is named and the walk restarts from the head. Handed
        // to `scan_page` as-is, an oversized cursor would be refused by the backend as a key
        // too large BEFORE `record_resume_point` could move it, and every later pass would
        // reread the same value and fail the same way — reclamation stopped for good on one
        // record (PR #802 review). `record_resume_point` rewrites it below, since the head this
        // pass resumes from differs from what is stored.
        let after = match stored.as_deref() {
            None => None,
            Some([]) => None,
            Some(cursor) if is_resumable_cursor(cursor) => Some(cursor.to_vec()),
            Some(cursor) => {
                emit_unusable_cursor(&object_name(cursor), cursor.len());
                None
            }
        };

        let mut marks = HashMap::new();
        let mut position = after.clone();
        let mut read = 0;
        // Each page asks for exactly what is left of the budget, and `ledger_page` refuses a page
        // longer than it asked for, so `read` lands on `ORPHAN_WINDOW` exactly and never past it.
        let through = loop {
            let (page, next) = ledger_page(meta, position.as_deref(), ORPHAN_WINDOW - read).await?;
            read += page.len();
            for (key, value) in page {
                classify_ledger_entry(&mut marks, &key, &value);
                position = Some(key);
            }
            if next.is_none() {
                // The end of the ledger: the range read runs to the end of the prefix, and the
                // next pass starts again at its head.
                break None;
            }
            if read == ORPHAN_WINDOW {
                break position;
            }
        };
        Ok(Self {
            after,
            through,
            marks,
            stored,
        })
    }

    /// Persist where the next pass resumes — after the last key this one read, or the head of
    /// the ledger once it reached the end — writing the cursor record only when it moves. A
    /// small ledger read whole every pass therefore writes nothing.
    ///
    /// A blind put, like every other write GC makes: whatever value lands, the walk only reads
    /// from there, so no precondition protects anything a racing writer (a deposed custodian
    /// finishing its pass) could harm.
    async fn record_resume_point(&self, meta: &dyn MetadataStore) -> Result<()> {
        let resume: &[u8] = self.through.as_deref().unwrap_or_default();
        let unchanged = match &self.stored {
            None => resume.is_empty(),
            Some(stored) => stored.as_slice() == resume,
        };
        if !unchanged {
            meta.commit(WriteBatch::new().put(ORPHAN_CURSOR_KEY, resume.to_vec()))
                .await?;
        }
        Ok(())
    }

    /// Whether this window stopped short of the ledger's end — so the pass has not seen the
    /// whole ledger and may not answer [`Reconciled::Satisfied`] over it.
    fn is_partial(&self) -> bool {
        self.through.is_some()
    }

    /// The fragment's own mark, if this window read it.
    fn mark_of(&self, dserver: DServerId, frag: FragmentId) -> Option<ReadMark> {
        self.marks.get(&(dserver, frag)).copied()
    }

    /// Whether this window read the key range the fragment's own mark would occupy — so that
    /// [`Self::mark_of`] answering `None` means the fragment has **no** mark, rather than that
    /// this pass did not look.
    fn covers_mark_of(&self, dserver: DServerId, frag: FragmentId) -> bool {
        let key = orphan_key(dserver, frag);
        self.after
            .as_deref()
            .is_none_or(|after| key.as_slice() > after)
            && self
                .through
                .as_deref()
                .is_none_or(|through| key.as_slice() <= through)
    }
}

/// The longest key [`orphan_key`] can spell: the prefix, then a `u64` D-server id, a `u128`
/// chunk id and a `u16` index in decimal, colon-separated — the bound a persisted cursor is
/// held to before it is handed to a backend as a key.
const ORPHAN_KEY_MAX_LEN: usize = ORPHAN_PREFIX.len() + 20 + 1 + 39 + 1 + 5;

/// Whether a persisted cursor is a place the walk may resume from: a key under the ledger's
/// prefix that a backend will accept as a key. It need not parse — a malformed key already IN
/// the ledger can legitimately end a window and become the cursor, and restarting from the
/// head on it would pin the walk to that window for good — only be bounded.
fn is_resumable_cursor(cursor: &[u8]) -> bool {
    cursor.starts_with(ORPHAN_PREFIX) && cursor.len() <= ORPHAN_KEY_MAX_LEN
}

/// Read one `orphan:` ledger entry into `marks` — or, for a key no writer spells, classify it,
/// name it and skip it (ADR-0045 decision 3: a record that does not decode is never acted on).
///
/// A value that does not read as an instant is decoded exactly as the ledger always has been
/// (the bare decimal [`mark_orphaned`] writes); it still counts as its fragment's mark
/// ([`ReadMark::Unreadable`]) and is named on the audit seam.
fn classify_ledger_entry(
    marks: &mut HashMap<(DServerId, FragmentId), ReadMark>,
    key: &[u8],
    value: &[u8],
) {
    let Some(slot) =
        parse_orphan_key(key).filter(|&(dserver, frag)| orphan_key(dserver, frag) == key)
    else {
        emit_malformed_orphan_key(&object_name(key));
        return;
    };
    let mark = match std::str::from_utf8(value).ok().and_then(|s| s.parse().ok()) {
        Some(at) => ReadMark::Stamped(at),
        None => {
            emit_unreadable_mark(&object_name(key));
            ReadMark::Unreadable
        }
    };
    marks.insert(slot, mark);
}

/// One page of the `orphan:` ledger strictly after `after`, at most `limit` entries — refused
/// if it is longer than that, or does not move the walk forward.
///
/// `scan_page`'s contract already promises both (a page holds at most `limit` entries, starts
/// strictly after its cursor, and is empty only at the end), but a walk that trusted it blindly
/// would read past its budget on a store that broke the bound — the pass would no longer be
/// bounded — and loop forever on one that broke the cursor. So a walker checks the two things its
/// bound and its termination rest on: the page is no longer than it asked for, and it either
/// ends the walk or carries it past `after`. Anything else is an error, never a page accepted
/// silently.
async fn ledger_page(
    meta: &dyn MetadataStore,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<ScanPage> {
    let (page, next) = meta.scan_page(ORPHAN_PREFIX, after, limit).await?;
    if page.len() > limit {
        return Err(BoxError::from(format!(
            "orphan-ledger scan_page after {:?} returned {} entries for a limit of {limit} — \
             refused rather than read past the walk's budget",
            after.map(object_name),
            page.len(),
        )));
    }
    let advances = match (page.last(), after) {
        (Some((last, _)), Some(after)) => last.as_slice() > after,
        (Some(_), None) => true,
        (None, _) => next.is_none(),
    };
    if !advances {
        return Err(BoxError::from(format!(
            "orphan-ledger scan_page after {:?} returned a page that does not advance the walk \
             ({} entries, next {:?}) — refused rather than walked forever",
            after.map(object_name),
            page.len(),
            next.as_deref().map(object_name),
        )));
    }
    Ok((page, next))
}

/// Which of `candidates` carry their own `orphan:` mark — their key exactly as [`orphan_key`]
/// spells it, whatever its value holds.
///
/// The post-restore pass's "already marked" judgement, read over the **whole** ledger in pages
/// of at most [`ORPHAN_WINDOW`] entries, never with one `scan`. It must see every mark: the pass
/// writes a fresh stamp for any candidate it did not find marked, and a stamp written over a
/// mark it failed to read restarts that mark's grace clock — or, for a value that does not read
/// as an instant, erases the only evidence a human has to repair. So it walks to the end, and
/// what it holds at once is one page plus the candidates' own keys, never the ledger. Existence
/// is all it asks, so no value is decoded here.
pub(crate) async fn marked_among(
    meta: &dyn MetadataStore,
    candidates: &[(DServerId, FragmentId)],
) -> Result<HashSet<(DServerId, FragmentId)>> {
    let mut marked = HashSet::new();
    if candidates.is_empty() {
        return Ok(marked);
    }
    let wanted: HashMap<Vec<u8>, (DServerId, FragmentId)> = candidates
        .iter()
        .map(|&(dserver, frag)| (orphan_key(dserver, frag), (dserver, frag)))
        .collect();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = ledger_page(meta, after.as_deref(), ORPHAN_WINDOW).await?;
        for (key, _value) in &page {
            if let Some(&slot) = wanted.get(key) {
                marked.insert(slot);
            }
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(marked),
        }
    }
}

/// A GC pass's own ledger writes, committed in batches of at most [`CLEANUP_BATCH`] deletes.
///
/// Blind deletes, like the single batch this replaces: no precondition, so the commit can never
/// come back `Conflict` (`CommitOutcome`, clause 3), and nothing here reads anything into one.
struct Cleanup<'a> {
    meta: &'a dyn MetadataStore,
    batch: WriteBatch,
}

impl<'a> Cleanup<'a> {
    fn new(meta: &'a dyn MetadataStore) -> Self {
        Self {
            meta,
            batch: WriteBatch::new(),
        }
    }

    /// Queue `key` for deletion, committing the batch the moment it holds [`CLEANUP_BATCH`].
    async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.batch = std::mem::take(&mut self.batch).delete(key);
        if self.batch.deletes.len() >= CLEANUP_BATCH {
            self.commit_queued().await?;
        }
        Ok(())
    }

    /// Commit whatever is still queued.
    async fn finish(mut self) -> Result<()> {
        self.commit_queued().await
    }

    async fn commit_queued(&mut self) -> Result<()> {
        if !self.batch.deletes.is_empty() {
            self.meta.commit(std::mem::take(&mut self.batch)).await?;
        }
        Ok(())
    }
}

/// Emit a reclamation on the durability-plane seam (ADR-0011 / ADR-0012): a metric
/// the `DurabilityTelemetry` `tracing`→OTel bridge counts, plus an append-only audit
/// event (`0005:336-340`).
fn emit_reclaim(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_fragments_reclaimed = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "reclaim",
        reason,
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "gc reclaimed collectable fragment bytes after the grace window",
    );
}

/// Emit a **malformed committed placement** signal on the durability-plane seam
/// (ADR-0011 / ADR-0012, ADR-0040 decision 4): a committed chunk whose `placement` vector
/// is non-empty but of the wrong length — truncation / corruption. GC fails safe (the
/// chunk is treated as fully referenced, never reclaimed); this is the operator signal
/// that a corrupt placement was masked no longer.
fn emit_malformed(chunk: ChunkId, expected: u16, actual: usize) {
    tracing::warn!(monotonic_counter.gc_malformed_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "malformed-placement",
        chunk = %wyrd_traits::chunk_hex(chunk),
        expected,
        actual,
        "gc found a committed placement of the wrong length (truncation/corruption); chunk treated as fully referenced, NEVER reclaimed — operator signal",
    );
}

/// Emit a committed object whose chunk map GC could **not read** on the durability-plane
/// seam (ADR-0011 / ADR-0012): the reference set is incomplete because of it, so this pass
/// reclaims nothing fleet-wide and certifies nothing until that record is repaired.
///
/// Emitted from the GC loop, never from the shared builder: the same
/// [`referenced_fragments`] call backs scrub, restore and the drain-status surface, and a
/// `gc_` counter ticked inside it would report a blocked GC pass for a scrub or a status
/// query that GC never ran at all.
///
/// The counterpart of `crate::scrub`'s own emitter on the reclaim side of the same
/// incomplete set — both NAME the object, so the gap is repairable rather than merely known
/// to be somewhere.
fn emit_unresolvable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.gc_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "gc could not read a committed object's chunk map; its reference set is incomplete, so gc reclaims NOTHING and certifies NOTHING until this record is repaired — operator signal",
    );
}

/// Emit a `pending:` entry GC could **not read as an ordinary lease** on the durability-plane
/// seam (ADR-0011 / ADR-0012): the expired-lease input skipped it, so no fragment is reclaimed on
/// its lease and its entry is left in place until a human repairs or refiles it. Named by key —
/// escaped as [`object_name`] escapes an `inode:` key — for the reason [`emit_unresolvable`]
/// names its record.
fn emit_unreadable_pending(entry: &str, fault: &str) {
    tracing::warn!(monotonic_counter.gc_unreadable_pending_entries = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unreadable-pending-entry",
        entry = %entry,
        fault = %fault,
        "gc could not read a pending-ledger entry as an ordinary lease; it reclaims nothing on that lease and leaves the entry in place — operator signal",
    );
}

/// Emit an `orphan:` mark whose value GC could **not read as an instant** on the durability-plane
/// seam (ADR-0011 / ADR-0012): the mark still counts as its fragment's mark, so the fragment is
/// kept on no other evidence — neither its grace window nor an expired lease can reclaim it —
/// and the mark is left in place, until a human repairs the value. Named by key, escaped as
/// [`object_name`] escapes an `inode:` key, as [`emit_unreadable_pending`] names its entry.
fn emit_unreadable_mark(mark: &str) {
    tracing::warn!(monotonic_counter.gc_unreadable_orphan_marks = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unreadable-orphan-mark",
        mark = %mark,
        "gc could not read an orphan-ledger mark's value as an instant; it still counts as a mark, so its fragment is kept and the mark left in place — operator signal",
    );
}

/// Emit a persisted walk cursor the pass could **not resume from** — not a bounded key under the
/// ledger's prefix — on the durability-plane seam (ADR-0011 / ADR-0012): the walk restarts from
/// the head and the record is rewritten, so no pass fails on it, but a value got there that no
/// pass wrote, and an operator should know.
fn emit_unusable_cursor(cursor: &str, len: usize) {
    tracing::warn!(monotonic_counter.gc_unusable_orphan_cursors = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unusable-orphan-cursor",
        cursor = %cursor,
        len,
        "gc could not resume the orphan-ledger walk from the persisted cursor (not a bounded ledger key); restarting from the head and rewriting it — operator signal",
    );
}

/// Emit a key under `orphan:` that **no writer spells** on the durability-plane seam (ADR-0011 /
/// ADR-0012): it does not parse, or it parses to a position [`orphan_key`] spells differently.
/// It licenses nothing — no reclaim, no delete — and is left in place for a human, named by key
/// as [`emit_unreadable_mark`] names a mark.
fn emit_malformed_orphan_key(key: &str) {
    tracing::warn!(monotonic_counter.gc_malformed_orphan_keys = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "malformed-orphan-key",
        key = %key,
        "gc found an orphan-ledger key no writer spells; it licenses nothing and is left in place — operator signal",
    );
}

/// Emit a skip (a still-referenced or within-grace fragment) on the same seam — the
/// observable record that GC *considered* and *declined* a fragment.
fn emit_skip(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_fragments_skipped = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "skip",
        reason,
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "gc declined a fragment (still referenced, or within its grace window)",
    );
}
