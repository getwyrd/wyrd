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
//! **A marked fragment's reclamation is recorded before its bytes are destroyed** (proposal
//! 0016, `0016:1312-1338`). A mark is read in any of its three shapes through the one codec
//! beside its key ([`metadata::decode_orphan_mark`]); a value that is none of them is kept,
//! with its fragment, and named. A mark past its grace window is first moved to its terminal
//! `reclaiming` shape — an exact-value compare-and-swap from the bytes the pass read, up to
//! [`CLEANUP_BATCH`] of them per commit — and only a mark whose swap committed has its fragment
//! deleted, and then its key. So any commit still preconditioned on the mark's earlier bytes (a
//! mover adopting a pre-marked position) fails from the instant reclamation begins, instead of
//! landing between the fragment's deletion and the key's and publishing a placement over bytes
//! that are gone. A mark that changed after the pass read it loses its swap and keeps its
//! fragment; a mark already `reclaiming` — a pass that died between its swap and its deletes —
//! is finished with no second grace test; and a mark whose event names a retirement still
//! draining (`retire:bytes:<event>`, one keyed read) is not reclaimed until that drain is done
//! (`0016:1226-1247`). The bytes behind an expired `pending:` lease carry no mark, and are
//! reclaimed as before.
//!
//! **A mark with no fragment beneath it is swept once no fragment can still land under it**
//! (proposal 0016, `0016:1359-1408`, X87, X91, X96). The walk above reaches a mark only through a
//! fragment some D server lists, so a mark whose position holds none — the old position of a
//! repaired missing fragment, a `reclaiming` mark whose fragment is already gone — would stay in
//! the ledger for good. A pass therefore also deletes each mark of its window whose position no
//! listing of **this** pass reported, once the mark is at least the late-write deadline
//! [`LATE_WRITE_DEADLINE_MILLIS`] old ([`Sweep::sweep_fragment_less_marks`]): the listing was then
//! taken after the last instant a fragment could land under that mark, so the position's
//! emptiness is an observation, never an inference from the mark's age. That holds for the marks
//! written **after** their fragment — a legacy mark, and GC's own `reclaiming` mark; a structured
//! mark carrying an unreference event may have been written ahead of its fragment (a repoint's
//! pre-mark, a teardown's planned placement), and a write whose effect the store could not verify
//! may still land under it, so it is left to the slice that introduces its writer. A mark on a
//! D server this pass did not list, at a position the reference set or the staged class protects,
//! or whose value is none of the three shapes is left in place. Each delete is an exact-value
//! compare-and-swap from the bytes the window read, in commits of at most [`CONDITIONAL_BATCH`]
//! marks, so a mark rewritten after the pass read it survives; and each is audited and counted
//! only once its commit has landed. A commit whose result the backend could not report is judged
//! as the one atomic commit it was, from a fresh read of every key in it: a mark still holding
//! what the pass read proves it did not land, and a key found gone is recorded as gone but
//! claimed by nobody — this pass's delete or another writer's, the read cannot tell.
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
//! **Staged bytes are never reclaimed either**, and they are protected as a class of their own
//! ([`StagedSet`], proposal 0016 decision 2, `0016:765-893`): the fragments a multipart upload's
//! committed parts (`part:`) and in-flight owned staging entries (`sidx:`) name, for every
//! session listed under `mpu:`, before any committed chunk map names them. A pass reads that
//! class ([`staged_fragments`]) through each session's own bounded ranges — never one read of a
//! whole namespace — and BEFORE the committed reference set: owned entries, then committed parts,
//! then committed inodes, the order that leaves a chunk in at least one class whenever a part
//! commit or a publication lands mid-read (`0016:782-800`). A staged record the pass cannot read
//! makes that class incomplete, which reclaims nothing and certifies nothing exactly as an
//! unreadable committed map does; a store fault under one of its reads fails the pass. The
//! drain-status surface and reconstruction read the same class; scrub reads only its committed
//! `part:` half, through a reader of its own ([`staged_committed_parts`]).
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
    self, decode_orphan_mark, encode_orphan_mark, parse_orphan_key, ChunkMapError, ChunkRef,
    EcScheme, InodeRecord, InodeState, MalformedPlacement, OrphanMark, ORPHAN_PREFIX,
};
use wyrd_core::multipart::{
    decode_owned_entry, decode_part_record, parse_mpu_key, parse_part_key, parse_sidx_key,
    part_range, retire_key, sidx_range, RetireMode, MAX_BATCH_OPS, MAX_PART_CHUNKS, MPU_PREFIX,
    U_REF,
};
use wyrd_traits::{
    BoxError, ChunkId, ChunkStore, CommitOutcome, CommitUnknownResult, DServerId, FragmentId,
    MetadataStore, Result, ScanPage, WriteBatch, SCAN_CAP,
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
/// The same bound caps each commit that records reclaim intents ([`Intent`]): at most `W` marks
/// moved to `reclaiming` per commit — one precondition and one put each — so the commit count a
/// pass spends on them is `⌈n / W⌉`, not one per mark.
///
/// **Bytes: bounded.** Every key a cleanup commit deletes is an
/// `orphan:<dserver>:<chunk>:<index>` key (at most 73 bytes) or a `pending:<chunk>` key (at most
/// 47), so `W` of them come to under 73 KB — far inside the 10 MB per transaction of the
/// inherited envelope (`MetadataStore`, "Operational envelope"). An intent commit carries each
/// mark's key twice and its value twice (the bytes read, then `reclaiming`); a decodable mark is
/// at most 328 bytes ([`metadata::MAX_ORPHAN_EVENT_LEN`]), so `W` intents come to under 800 KB.
///
/// **Operations: NOT calibrated.** The envelope's other half is 5 s per transaction, and a
/// backend applies a batch's mutations one after another inside it — TiKV takes a lock round
/// trip per delete — so whether `W` deletes finish inside 5 s depends on the deployment's round
/// trip time, and a batch of ~1,000 small marks can exceed it (`0016:630-636`). Proposal 0016
/// bounds every such batch by an operation-count knob `B_ops` calibrated against the slowest
/// supported backend (`0016:640-643`); `W` is not that calibration. What it does guarantee is
/// that a cleanup commit's size is a constant, independent of the ledger and of the pass.
///
/// The same bound caps each commit of the fragment-less sweep ([`Sweepable`]): at most `W` marks
/// deleted per commit, each conditioned on the value the pass read — one precondition and one
/// delete, so a commit carries each key twice and each value once: at most 73 + 328 + 73 bytes a
/// mark, under 480 KB for `W` of them.
pub(crate) const CLEANUP_BATCH: usize = 1_000;

/// The most marks one **conditional** commit of a pass carries — a reclaim intent's swap
/// ([`Intent::record`]) or a fragment-less sweep's delete ([`Sweepable::delete`]), each a
/// precondition plus a mutation, two sequential operations on the networked backends.
///
/// [`CLEANUP_BATCH`] bounds bytes; it does not bound the transaction's **time**. Both networked
/// backends run a batch's preconditions and mutations one after another inside the transaction,
/// so `CLEANUP_BATCH` conditional deletes are 2,000 sequential operations — past the five-second
/// envelope at the latency proposal 0016 assumes, and a batch that always times out is not slow,
/// it is stuck: every later pass would retry the same leading batch and the ledger would never
/// shrink (PR #821 review). So a conditional commit carries at most half of
/// [`MAX_BATCH_OPS`] marks — `B_ops`, the operation budget 0016 derives for the slowest
/// supported backend (`0016:640-648`), spent two operations a mark. The blind cleanup batch and
/// the restore pass's `MARK_BATCH` are one operation a key and keep the byte bound until
/// `B_ops` is calibrated against a measured backend (the obligation on `MAX_BATCH_OPS`).
pub(crate) const CONDITIONAL_BATCH: usize = (MAX_BATCH_OPS / 2) as usize;
const _: () = assert!(CONDITIONAL_BATCH >= 1 && CONDITIONAL_BATCH <= CLEANUP_BATCH);

/// **`W_write`** — the fragment-write deadline (proposal 0016 decision 5, `0016:1551-1576`): a
/// fragment write lands within `W_write` of the instant its writer authorized it, or not at all.
/// Both ends hold the bound: the writer's own await gives up at the deadline, and the D server
/// refuses a write whose deadline has passed (the `deadline_millis` of
/// [`ChunkStore::put_fragment`], refused as [`wyrd_traits::WriteDeadlineExpired`], #638) — a
/// caller-side timeout alone bounds how long the writer waits, never when an accepted write takes
/// effect (`0016:1557-1564`).
///
/// Derivation: 30 s, the D server's own ceiling on one request (`DEFAULT_REQUEST_TIMEOUT`,
/// `crates/server/src/dserver.rs`), at which it cuts the request with a deadline status. A write
/// still running past that is one the server has stopped serving, so a longer `W_write` would
/// promise a window no write can use; a shorter one would refuse writes the server still serves.
///
/// A bound on writers, not on GC: it holds only for a write that carries it. See
/// [`LATE_WRITE_DEADLINE_MILLIS`] for the writers that must.
pub const W_WRITE_MILLIS: u64 = 30_000;

/// **`W_repoint`** — the pre-mark deadline (proposal 0016 decision 4.2, `0016:1339-1349`, X88): a
/// mover that marks a position before it writes a fragment there must authorize that write within
/// `W_repoint` of its pre-mark's stamp; once the pre-mark is older it MUST NOT authorize the write
/// at all, and restarts from a fresh pre-mark instead. A caller-side deadline gating a caller-side
/// action — whether this mover issues the write — which is the job such a deadline can do
/// soundly (`0016:1350-1356`).
///
/// Derivation: 10 s, two metadata transaction envelopes of 5 s (`E_TX_MILLIS`,
/// `crates/core/src/multipart.rs`, the envelope every `MetadataStore` commit is sized against):
/// the pre-mark's own commit, which may use its whole envelope before the mover learns it landed,
/// and one more read or commit before the mover authorizes the write. A mover slower than that has
/// stalled, and a fresh pre-mark is the restart 0016 already asks of it.
///
/// A bound on writers, not on GC, exactly as [`W_WRITE_MILLIS`] is.
pub const W_REPOINT_MILLIS: u64 = 10_000;

/// **`δ_clock`** — the most two clocks that judge one mark's deadline may disagree by: the
/// writer's, which stamps the mark and authorizes a write under it; the D server's, which refuses
/// a late write; and the custodian's, which ages the mark (`0016:1566-1569`). All three read the
/// one deployment wall clock, on different hosts (the clock-lifecycle table, `0016:2508-2509`).
///
/// Derivation: 1 s, a stated budget rather than a measurement. ADR-0024 (Proposed) makes one
/// clock-skew budget the bound every time-dependent check shares, with hosts held to it by
/// authenticated time sync, and sets no value yet; this is the budget the sweep assumes, and a
/// fleet whose hosts drift further apart than this has already broken the lease and grace checks
/// that share it. It is a thousand ticks of the millisecond clock every stamp is read in, so the
/// strict margin 0016 needs — at least one tick (`0016:1570-1575`) — holds with room to spare.
pub const DELTA_CLOCK_MILLIS: u64 = 1_000;

/// **`D`** — the late-write deadline (proposal 0016, `0016:1369-1391`, X91): the longest after a
/// mark's `orphaned_at` that a fragment may still land at its position, `W_repoint + W_write +
/// δ_clock`. A repoint's pre-mark is legitimately fragment-less for up to [`W_REPOINT_MILLIS`],
/// the write it then authorizes lands within [`W_WRITE_MILLIS`], and the clocks that stamped the
/// mark, refused a late write and now age the mark may disagree by [`DELTA_CLOCK_MILLIS`]. So a
/// listing taken at or after `orphaned_at + D` that shows the position empty shows it empty for
/// good, and that listing is what licenses the fragment-less sweep to delete the mark
/// ([`Sweep::sweep_fragment_less_marks`]) — never the mark's age alone (X96, `0016:2625`).
///
/// 41 s, strictly inside the orphan grace the deployed pass honours, as 0016 requires
/// (`G_orphan > W_repoint + W_write + δ_clock`, `0016:1386-1388`): a fragment written as late as
/// its writer may write it lands while its mark's grace is still running, so the reclaim never
/// removes evidence a late write needs. The build holds that against the deployed grace itself,
/// beside it (`GC_GRACE_WINDOW_MILLIS`, `crates/server/src/custodian.rs`). The uniform bound is
/// the safe default; 0016 allows tightening it per event kind (`0016:1388-1390`), which this does
/// not do.
///
/// **The writers' obligation.** `D` bounds only a writer that enforces its parts, and no change to
/// GC can make it sound alone (`0016:1339-1349`, `:1551-1576`). Every writer that marks a position
/// before its fragment lands there — the staged re-place (#814, split from #663), flat pre-marking
/// (#723) and multipart teardown — MUST refuse to authorize the write once its mark is older than
/// [`W_REPOINT_MILLIS`], and MUST write with a deadline [`W_WRITE_MILLIS`] after that
/// authorization, which the D server enforces. No writer on `main` marks ahead of its fragment, so
/// the sweep is sound against today's tree: unlink marks the placed positions of a committed map,
/// and a map commits only after every fragment it names is acknowledged; reconstruction and
/// rebalance write first, and mark only the positions they vacate, in the repoint commit.
///
/// **`D` bounds acceptance, not effect, and the sweep relies on it only for marks written after
/// their fragment.** A D server judges the deadline before it publishes, but the publishing step
/// itself can straddle it, and the store then answers [`wyrd_traits::WriteEffect::Unknown`] with
/// the bytes possibly on disk (`ChunkStore::put_fragment`'s contract; `FsChunkStore` re-reads its
/// clock after `rename`). So a writer that marks a position **ahead of** its fragment cannot take
/// a listing at `orphaned_at + D` as proof its position stays empty. Until the slice that
/// introduces such a writer settles its marks — on `Unknown`, re-read the position; if the bytes
/// landed and the adoption cannot proceed, re-mark them so they stay evidenced; and only then add
/// its event to the sweep's set — the sweep leaves every structured, non-`reclaiming` mark in
/// place (`event-may-await-write`, [`Sweep::sweep_fragment_less_marks`]; PR #821 review).
pub const LATE_WRITE_DEADLINE_MILLIS: u64 = W_REPOINT_MILLIS + W_WRITE_MILLIS + DELTA_CLOCK_MILLIS;

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

/// **P** — the most records one page of a staged read holds ([`staged_fragments`]).
///
/// Every staged read is paged rather than one `scan`: `scan` is complete-or-fail-loud, so a range
/// larger than the store's configured cap would fail whole on every pass, and GC and the
/// post-restore pass with it. A page is bounded instead.
///
/// Derivation: 512. One `part:` value names at most [`MAX_PART_CHUNKS`] (158) chunk references,
/// so one page of committed parts holds at most 512 × 158 = 80,896 — within [`U_REF`] (85,952),
/// one admitted session's worst-case staged footprint (`0016:1469`), the unit the per-pass
/// reference budget `W_ref` already charges every session. Reading one page therefore never holds
/// more references than that budget allows a single session, and its raw bytes come to at most
/// 512 × [`metadata::MAX_VALUE_BYTES`] = 51.2 MB. A session at `MAX_PARTS_PER_SESSION` (10,000
/// parts) is read in 20 pages; its owned range, at most `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` =
/// 2,528 entries, in 5.
const STAGED_PAGE: usize = 512;

// The derivation above, held at compile time: a retuned page or part cap that breaks it fails the
// build here.
const _: () = assert!(STAGED_PAGE as u64 * MAX_PART_CHUNKS as u64 <= U_REF);

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
/// bytes. Idempotent at the metadata layer (a plain put). The value is the legacy
/// [`OrphanMark`] shape — the bare decimal instant — spelled by the shared codec.
pub async fn mark_orphaned(
    meta: &impl MetadataStore,
    dserver: DServerId,
    frag: FragmentId,
    orphaned_at_millis: u64,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(
        orphan_key(dserver, frag),
        encode_orphan_mark(&OrphanMark::legacy(orphaned_at_millis)),
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
/// reclaimed or any fragment-less mark swept, and [`Reconciled::Satisfied`] otherwise. Scrub
/// answers the identical condition the identical way ([`crate::scrub::reconcile`]): one
/// incomplete set, one rule, read twice. A staged multipart record this pass could not read
/// ([`StagedSet::unresolvable`]) is answered the same way, by GC, by reconstruction and by the
/// drain-status query ([`crate::desired_state::reconciliation_status`]) — scrub answers the
/// identical condition too, but over its own narrower, PART-ONLY staged read
/// ([`StagedPartSet::unresolvable`], `crate::scrub`): it never reads an owned `sidx:` entry
/// (0016 decision 2, `0016:776-781`). A pass whose window of the `orphan:` ledger
/// ([`OrphanWindow`]) stopped short of the ledger's end and reclaimed nothing answers
/// [`Reconciled::Partial`], never
/// `Satisfied`: `Satisfied` certifies that reality matched, and a caller driving the loop to
/// satisfaction would stop on it with eligible marks still unvisited in the windows ahead (PR
/// #802 review). A pass that lost a reclaim intent — a mark it judged changed before its
/// `reclaiming` swap committed — and reclaimed nothing answers `Partial` for the same reason:
/// the mark's new value is one this pass never read. So does a pass whose sweep of a
/// fragment-less mark lost its precondition to a mark a fresh read then found still present.
/// `Satisfied` is therefore "this pass read the ledger to its end, lost no intent and no sweep
/// to a mark still there, and reclaimed and swept nothing".
///
/// A store fault ends the pass with its error, after the key deletes it had queued for
/// fragments already deleted are committed, best effort ([`Cleanup::finish_after_fault`]).
pub(crate) async fn reconcile(ctx: &GcContext<'_>, now_millis: u64) -> Result<Reconciled> {
    // The staged protection class, read FIRST: every fragment a multipart upload's committed
    // parts and in-flight owned staging entries name. Never reclaimed either — and read before the
    // committed reference set below because a publication moves a chunk's protection from its
    // `part:` record to a committed inode, so reading the inodes first could miss a flip and then
    // miss the part record its drain deleted, seeing the chunk in neither (`0016:793-800`, X67).
    // Its own reads run owned entries before committed parts, for a part commit's move
    // (`0016:782-790`). A store fault under any of them fails the pass here, before anything is
    // reclaimed.
    let staged = staged_fragments(ctx.meta).await?;
    // Attributed the moment the reading returns, before the next store read, on the placement the
    // committed set's attribution below explains: a fault a `?` later must not take the names of
    // records a human has to repair with it.
    for (record, fault) in &staged.unresolvable {
        emit_unresolvable_staged(&object_name(record), fault);
    }
    for (&chunk, records) in &staged.held {
        for (record, fault) in records {
            emit_untrusted_staged(&object_name(record), chunk, fault);
        }
    }
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

    let mut cleanup = Cleanup::new(ctx.meta);
    let mut sweep = Sweep {
        ctx,
        now_millis,
        referenced: &referenced,
        staged: &staged,
        window: &window,
        expired_pending: &expired_pending,
        intents: Vec::new(),
        changed: false,
        lost_intent: false,
        reclaimed_expired: BTreeSet::new(),
        still_held: HashSet::new(),
        listed: HashSet::new(),
        marks_swept: false,
        lost_sweep: false,
    };
    if let Err(fault) = sweep.run(&mut cleanup).await {
        // The sweep may already have deleted fragments whose key deletes are still queued: commit
        // those before the fault goes up, so it costs the pass its remaining work and never
        // strands a consumed mark over bytes that are gone.
        cleanup.finish_after_fault(&fault).await;
        return Err(fault);
    }

    // Retire the swept pending-ledger entries (the byte reclaim the stand-in deferred,
    // `write.rs:330-331`) — but an entry is CHUNK-WIDE evidence, and under a paged walk one pass
    // may reclaim some of a chunk's fragments while others wait for a later window. Deleting the
    // entry then would leave those with neither the entry nor a mark: evidence-free bytes GC
    // keeps forever. So an entry goes only once this pass has reclaimed bytes of its chunk and no
    // unprotected fragment of it is left for the entry to account for.
    for &chunk in &sweep.reclaimed_expired {
        if !sweep.still_held.contains(&chunk) {
            cleanup.delete(metadata::pending_key(chunk)).await?;
        }
    }
    cleanup.finish().await?;

    Ok(
        if !referenced.unresolvable.is_empty() || !staged.unresolvable.is_empty() {
            // Refuse to certify — whatever this pass reclaimed above is durable either way (a
            // reclaim never depended on the object it could not read, `ReferenceSet::protects`
            // withheld everything). What answering `Changed` / `Satisfied` would destroy is the
            // only signal that this pass could not see every committed object's chunks: an
            // operator reading `Satisfied` is being told the store converged, and would act on
            // it — decommission the server, close the ticket (`docs/principles.md` §5 C-1). An
            // unreadable staged record is the same hole in the other class: `StagedSet::protects`
            // withheld everything, and the answer says so.
            Reconciled::Blocked
        } else if sweep.changed || sweep.marks_swept {
            Reconciled::Changed
        } else if window.is_partial() || sweep.lost_intent || sweep.lost_sweep {
            // Nothing reclaimed or swept, and something this pass did not read: more of the ledger
            // beyond its window, or a mark that changed after its window read it. Not a
            // certification; the next pass reads both.
            Reconciled::Partial
        } else {
            Reconciled::Satisfied
        },
    )
}

/// One pass's walk of the fleet against its window: each unprotected fragment judged on its own
/// evidence, and the reclaims it licenses recorded before any is carried out — and then the sweep
/// of the window's marks that no listing of the pass reported.
struct Sweep<'p, 'a> {
    ctx: &'p GcContext<'a>,
    now_millis: u64,
    referenced: &'p ReferenceSet,
    staged: &'p StagedSet,
    window: &'p OrphanWindow,
    expired_pending: &'p HashSet<ChunkId>,
    /// Marks judged past their grace window whose `reclaiming` swap has not been committed yet —
    /// never more than [`CONDITIONAL_BATCH`]. Nothing is deleted for them until it has.
    intents: Vec<Intent<'a>>,
    /// Whether this pass deleted any fragment bytes.
    changed: bool,
    /// Whether an intent lost its precondition to a mark that changed after the window read it.
    lost_intent: bool,
    /// For the expired-lease input, per chunk: did this pass reclaim any of its bytes, and does
    /// any unprotected fragment of it survive the pass? Its `pending:` entry is the one record
    /// naming every such survivor, so it is retired only when the first holds and the second does
    /// not ([`reconcile`]).
    reclaimed_expired: BTreeSet<ChunkId>,
    still_held: HashSet<ChunkId>,
    /// The positions of this window's marks that a listing of THIS pass reported — each keyed by
    /// the fragment its own mark names, never by a raw ledger key. A position here holds a
    /// fragment, which the walk judged; the fragment-less sweep never touches it. Bounded by the
    /// window, not by the fleet's fragment count: only a listed fragment that has a mark in the
    /// window is recorded.
    listed: HashSet<(DServerId, FragmentId)>,
    /// Whether this pass deleted any fragment-less mark (a sweep whose commit landed).
    marks_swept: bool,
    /// Whether a sweep lost its precondition to a mark a fresh read then found still present.
    lost_sweep: bool,
}

/// A reclaim a pass has judged and not yet recorded: the fragment, the store holding it, and its
/// mark exactly as the pass's window read it.
struct Intent<'a> {
    dserver: DServerId,
    store: &'a dyn ChunkStore,
    frag: FragmentId,
    mark: OrphanMark,
}

impl Intent<'_> {
    /// Add this intent's `reclaiming` swap to `batch`: the mark must still hold the bytes the pass
    /// read, and becomes `reclaiming` with its stamp and event unchanged. Re-encoding the decoded
    /// mark reproduces those bytes exactly — the codec decodes only what it would itself encode —
    /// so the precondition is the value read, not a paraphrase of it.
    fn record(&self, batch: WriteBatch) -> WriteBatch {
        let key = orphan_key(self.dserver, self.frag);
        batch
            .require(key.clone(), encode_orphan_mark(&self.mark))
            .put(
                key,
                encode_orphan_mark(&self.mark.clone().into_reclaiming()),
            )
    }
}

/// A fragment-less mark the sweep judged deletable: its key, the position it names, and its value
/// exactly as the pass's window read it, borrowed from the window.
struct Sweepable<'w> {
    key: Vec<u8>,
    dserver: DServerId,
    frag: FragmentId,
    mark: &'w OrphanMark,
}

impl Sweepable<'_> {
    /// Add this mark's delete to `batch`, conditioned on the mark still holding the bytes the pass
    /// read — the decoded mark re-encoded, which reproduces those bytes exactly, as a reclaim
    /// intent's precondition does ([`Intent::record`]). A mark rewritten since, deleted since, or
    /// deleted and written again with another value fails it.
    fn delete(&self, batch: WriteBatch) -> WriteBatch {
        batch
            .require(self.key.clone(), encode_orphan_mark(self.mark))
            .delete(self.key.clone())
    }
}

impl<'p, 'a> Sweep<'p, 'a> {
    /// Walk every D server's fragments, record whatever intents are still pending, and then sweep
    /// the window's marks that no listing of this pass reported.
    async fn run(&mut self, cleanup: &mut Cleanup<'_>) -> Result<()> {
        for &(dserver, store) in self.ctx.fleet {
            for frag in store.list_fragments().await? {
                // Recorded before the fragment is judged, whatever the judgement: a listed position
                // is the walk's, protected or not. Per listing, so a fleet naming one server twice
                // still has the position listed if either of its listings reports it.
                if self.window.mark_of(dserver, frag).is_some() {
                    self.listed.insert((dserver, frag));
                }
                self.judge(dserver, store, frag, cleanup).await?;
            }
        }
        self.record_intents(cleanup).await?;
        self.sweep_fragment_less_marks().await
    }

    async fn judge(
        &mut self,
        dserver: DServerId,
        store: &'a dyn ChunkStore,
        frag: FragmentId,
        cleanup: &mut Cleanup<'_>,
    ) -> Result<()> {
        // SAFETY GATE — never reclaim a referenced fragment. A fragment of a malformed-placement
        // chunk is protected the same way (fail safe): its true placement cannot be trusted, so
        // every fragment bearing its id is off-limits; so is every fragment at all while the set
        // is incomplete. The set itself says WHICH rule held, so the audit trail never files an
        // unrelated orphan under `referenced` when what actually saved it was a blanket
        // containment. The staged class gates the same way, by its own rules and under its own
        // reasons. It outranks every mark — a `reclaiming` one included: that decision was taken
        // against an earlier reading, and this pass's reading says the bytes are named.
        if let Some(reason) = self
            .referenced
            .protection(dserver, frag)
            .or_else(|| self.staged.protection(dserver, frag))
        {
            emit_skip(dserver, frag, reason);
            return Ok(());
        }

        let window = self.window;
        match window.mark_of(dserver, frag) {
            Some(ReadMark::Reclaiming(_)) => {
                // Reclamation already decided and recorded by an earlier pass that did not finish
                // it: resume, with no second grace test — the window elapsed once, and the swap
                // did not move the stamp it was measured from (`0016:1321-1333`). Deleting the
                // fragment again is idempotent if the earlier attempt landed.
                self.destroy(dserver, store, frag, "resumed", cleanup)
                    .await?;
            }
            Some(ReadMark::Stamped(mark)) => {
                // Orphan input: reclaim ONLY after the reader-safe grace window.
                if self.now_millis
                    < mark
                        .orphaned_at_millis()
                        .saturating_add(self.ctx.grace_window_millis)
                {
                    emit_skip(dserver, frag, "within-grace");
                    self.held(frag.chunk);
                } else if self.retirement_draining(mark).await? {
                    emit_skip(dserver, frag, "draining-retirement");
                    self.held(frag.chunk);
                } else {
                    self.intents.push(Intent {
                        dserver,
                        store,
                        frag,
                        mark: mark.clone(),
                    });
                    if self.intents.len() >= CONDITIONAL_BATCH {
                        self.record_intents(cleanup).await?;
                    }
                }
            }
            // A mark whose value is none of the three shapes: evidence that something stranded
            // the fragment, with no instant a grace window could run from. Kept, on no other
            // evidence, until a human repairs it (named when the window read it).
            Some(ReadMark::Unreadable) => self.held(frag.chunk),
            None if self.expired_pending.contains(&frag.chunk)
                && window.covers_mark_of(dserver, frag) =>
            {
                // Expired pending-lease input: the lease TTL is its grace — for a fragment this
                // window shows has NO mark, and for no other. A mark outranks the lease: it may
                // still be inside its own grace window, which a reader holding the prior version
                // is relying on. So the arm needs the window to have covered the key position the
                // fragment's mark would occupy and found nothing there. A mark this pass did not
                // read — outside its window — is unknown, not absent; one it read and could not
                // decode is a mark all the same (`ReadMark::Unreadable`). Both fall to the
                // conservative arm below and wait for a pass that can see them.
                //
                // "Covered and absent" is sound although the page that covered the position may
                // have been read a moment ago: every in-tree writer of a mark that dereferences
                // (unlink, supersede, repoint, evacuation) writes it in the SAME commit that
                // dereferences the fragment, and the reference set this pass gates on was read
                // before this window was — a fragment dereferenced after that read is still
                // protected this pass. The one writer that marks without dereferencing, the
                // post-restore pass, never marks a fragment whose chunk holds a `pending:` entry.
                //
                // No mark, so nothing to record first: the bytes go as they always have (#557,
                // #490 own this arm's order), and no ledger key is deleted — the window found none
                // at this position, and a key it did not read is not one to destroy.
                store.delete_fragment(frag).await?;
                emit_reclaim(dserver, frag, "expired-lease");
                self.reclaimed(frag.chunk);
            }
            // No evidence the grace window elapsed — conservatively keep it (reader-safe: a
            // fragment is never reclaimed without a deadline).
            None => self.held(frag.chunk),
        }
        Ok(())
    }

    /// Whether `mark` names a retirement that is still draining: its event spells a retirement
    /// token and that token's `retire:bytes:` obligation is present (`0016:1226-1247`, X97
    /// `0016:2626`).
    ///
    /// One keyed read per candidate, never a range read of `retire:` — that namespace is
    /// deliberately not bounded by cardinality, and expanding it would make a pass's memory
    /// unbounded. A drain installs its obligation before it writes a mark naming it, and a token
    /// is never reused, so an obligation found absent here has drained for good; a mark that
    /// changes to name another event after this read loses its `reclaiming` swap instead. The
    /// read's await is bounded as every other custodian read is, by the `MetadataStore`
    /// implementation's own network bound (#508/#636), and a fault fails the pass before it
    /// records anything for this mark. The obligation's value is not decoded: its presence is the
    /// whole answer, so one that will not decode protects all the same.
    async fn retirement_draining(&self, mark: &OrphanMark) -> Result<bool> {
        let Some(token) = mark.retire_token() else {
            return Ok(false);
        };
        let obligation = retire_key(RetireMode::Bytes, &token);
        Ok(self.ctx.meta.get(&obligation).await?.is_some())
    }

    /// Record the pending intents — each mark's exact-value swap to `reclaiming`, all of them in
    /// one commit — and only then delete the fragments whose swap committed, and queue their keys.
    ///
    /// A `Conflict` says at least one mark changed after the window read it. A lost precondition
    /// costs only its own intent: each intent is then recorded alone, and one that loses keeps
    /// its fragment and its mark's new value, both untouched by this pass. An `Err` is propagated
    /// before anything of this batch is deleted: whether its swaps landed is unknown, and a mark
    /// left `reclaiming` over a present fragment is simply resumed by the next pass.
    async fn record_intents(&mut self, cleanup: &mut Cleanup<'_>) -> Result<()> {
        if self.intents.is_empty() {
            return Ok(());
        }
        let intents = std::mem::take(&mut self.intents);
        let batch = intents
            .iter()
            .fold(WriteBatch::new(), |batch, intent| intent.record(batch));
        if self.ctx.meta.commit(batch).await? == CommitOutcome::Committed {
            for intent in &intents {
                self.destroy(intent.dserver, intent.store, intent.frag, "orphan", cleanup)
                    .await?;
            }
            return Ok(());
        }
        for intent in &intents {
            let alone = intent.record(WriteBatch::new());
            match self.ctx.meta.commit(alone).await? {
                CommitOutcome::Committed => {
                    self.destroy(intent.dserver, intent.store, intent.frag, "orphan", cleanup)
                        .await?;
                }
                CommitOutcome::Conflict => {
                    emit_skip(intent.dserver, intent.frag, "mark-changed");
                    self.lost_intent = true;
                    self.held(intent.frag.chunk);
                }
            }
        }
        Ok(())
    }

    /// Carry out a recorded reclaim: delete the fragment's bytes, then queue its mark's key for
    /// deletion — the one key this pass holds a licence to delete. Never called before the mark
    /// is `reclaiming` in the store.
    ///
    /// A pass that dies after the fragment is deleted and before the queued key delete commits
    /// leaves a `reclaiming` mark over bytes that are gone. It is safe — nothing preconditioned on
    /// the mark's earlier bytes can commit, and no writer overwrites it — and this walk, driven by
    /// `list_fragments()`, never visits the position again; the fragment-less sweep deletes the
    /// key instead ([`Self::sweep_fragment_less_marks`]).
    async fn destroy(
        &mut self,
        dserver: DServerId,
        store: &dyn ChunkStore,
        frag: FragmentId,
        reason: &str,
        cleanup: &mut Cleanup<'_>,
    ) -> Result<()> {
        store.delete_fragment(frag).await?;
        emit_reclaim(dserver, frag, reason);
        self.reclaimed(frag.chunk);
        cleanup.delete(orphan_key(dserver, frag)).await
    }

    /// **Sweep the window's fragment-less marks** (proposal 0016, `0016:1359-1408`, X87, X91,
    /// X96): delete each mark this pass's window read whose position no listing of this pass
    /// reported, once the mark is at least [`LATE_WRITE_DEADLINE_MILLIS`] old — so every `orphan:`
    /// mark has a deleter whether or not a fragment ever lands under it (`0016:1406-1408`).
    ///
    /// **An observation licenses the delete, never the mark's age** (X96, `0016:2625`). The pass's
    /// clock was read before the pass began — the deployed loop reads it as the pass's argument
    /// (`crates/server/src/custodian.rs`) — and every listing is taken during the pass, so no
    /// listing predates `now_millis`. A mark at least `D` old at `now_millis` was therefore listed
    /// empty at or after `orphaned_at + D`, past the last instant a fragment may land under it,
    /// and its position stays empty. Only this pass's own listing counts: nothing of a listing
    /// outlives its pass (the deployed loop builds a fresh [`GcContext`] every pass, and nothing
    /// here is written to the store), so a listing an earlier pass took — before a fragment
    /// landed — never licenses a delete. One clock: the mark is aged on `now_millis`, the reading
    /// the grace test uses, so the mark's whole lifecycle is judged on it (ADR-0009).
    ///
    /// A mark is left in place when:
    ///
    /// - its position is listed: a fragment is there, and the walk judged it;
    /// - its D server is not in this pass's fleet: nothing listed that server, so no absence was
    ///   observed;
    /// - the reference set or the staged class protects its position — the reclaim's own safety
    ///   gate, read the same way, so an incomplete set sweeps nothing as it reclaims nothing;
    /// - its value is none of the three shapes: there is no stamp to age, so it stays
    ///   byte-identical, as it was named when the window read it (ADR-0045 decision 3);
    /// - it is younger than `D`;
    /// - it is a structured mark that is not `reclaiming` — one carrying an unreference event
    ///   (`event-may-await-write`). Such a mark may have been written **ahead of** its fragment
    ///   (a repoint's pre-mark, a teardown's planned placement), and `D` bounds when a write is
    ///   *accepted*, not when it takes effect: a publication that straddles the deadline is
    ///   reported as [`wyrd_traits::WriteEffect::Unknown`] with the bytes possibly landed, so a
    ///   listing at `orphaned_at + D` does not prove the position stays empty (PR #821 review).
    ///   No writer on `main` writes such a mark yet; the slice that introduces one settles how
    ///   its mark is retired — see [`LATE_WRITE_DEADLINE_MILLIS`] — and adds its event to the
    ///   swept set then. A legacy mark is written after its fragment (unlink, a vacated source),
    ///   and a `reclaiming` mark is GC's own decision over a position nothing may write under.
    ///
    /// Only a key a writer spells is a mark here. The window files a mark under the position it
    /// names only when [`orphan_key`] spells that position as the very key read, and records a
    /// listing only by that position, so a differently spelled key is never swept or rewritten,
    /// and never lends the mark of its position its stamp or its listing — it was named when the
    /// window read it ([`classify_ledger_entry`]).
    ///
    /// The marks are judged in position order, and the deletes go in that order, at most
    /// [`CONDITIONAL_BATCH`] to a commit ([`Self::commit_sweep`]) — so the commits a pass makes are
    /// the same on every run.
    async fn sweep_fragment_less_marks(&mut self) -> Result<()> {
        let window: &'p OrphanWindow = self.window;
        let fleet: HashSet<DServerId> =
            self.ctx.fleet.iter().map(|&(dserver, _)| dserver).collect();
        let mut judged = Vec::new();
        for (dserver, frag, read) in window.marks_in_position_order() {
            let mark = match read {
                ReadMark::Stamped(mark) | ReadMark::Reclaiming(mark) => mark,
                ReadMark::Unreadable => continue,
            };
            if self.listed.contains(&(dserver, frag)) {
                continue;
            }
            let kept = if !fleet.contains(&dserver) {
                Some("server-not-in-fleet")
            } else if let Some(reason) = self
                .referenced
                .protection(dserver, frag)
                .or_else(|| self.staged.protection(dserver, frag))
            {
                Some(reason)
            } else if self.now_millis
                < mark
                    .orphaned_at_millis()
                    .saturating_add(LATE_WRITE_DEADLINE_MILLIS)
            {
                Some("within-late-write-deadline")
            } else if !mark.is_reclaiming() && mark.event().is_some() {
                // Possibly written ahead of its fragment, and a straddling write's effect is
                // not bounded by `D` — left to the writer's own slice (see the doc above).
                Some("event-may-await-write")
            } else {
                None
            };
            match kept {
                Some(reason) => emit_mark_skip(dserver, frag, reason),
                // Its key is the one the window read: the window files a mark only under the
                // position `orphan_key` spells as that key.
                None => judged.push(Sweepable {
                    key: orphan_key(dserver, frag),
                    dserver,
                    frag,
                    mark,
                }),
            }
        }
        for batch in judged.chunks(CONDITIONAL_BATCH) {
            self.commit_sweep(batch).await?;
        }
        Ok(())
    }

    /// Commit one batch of the sweep's deletes, and claim each — audit it, count it — only once
    /// its commit has landed: evidence is claimed only once it is durable, as the post-restore
    /// pass claims its marks (`crate::restore`).
    ///
    /// The batch goes in one commit while every mark in it still holds the bytes the pass read. A
    /// `Conflict` says only that some precondition lost (`CommitOutcome`, clause 2) — not which,
    /// and not that any of the marks still exists — so each delete is then committed alone, and
    /// one that loses again is judged on a **fresh read** of its key, never on the `Conflict`: a
    /// mark found holding other bytes was rewritten after the pass read it, and keeps its new
    /// value for a pass that reads it; one found holding the same bytes lost to something else
    /// (a concurrent commit on the key) and waits for the next pass; one found absent was deleted
    /// by another writer, and this pass claims nothing about it — neither a sweep it did not make
    /// nor a mark that is not there. The fresh read's await is bounded as every other custodian
    /// read is, by the `MetadataStore` implementation's own network bound (#508/#636).
    ///
    /// An `Err` ends the pass with nothing of its own commit claimed — whether that commit landed
    /// is unknown — while every commit before it was claimed the moment it landed. So a fault
    /// partway through the sweep leaves the audit trail and the count saying exactly which
    /// deletes were durable when it struck. The one `Err` that is **settled first** is a
    /// [`CommitUnknownResult`] the backend says is no longer in flight
    /// ([`Self::settle_unknown_sweep`]): a delete that landed under it has taken the key with it,
    /// so a pass that gave up there would leave that delete unaudited and uncounted with nothing
    /// a later pass could read to reconstruct it (PR #821 review). The settle judges the batch as
    /// the one commit it was and claims no sweep from it: a key found gone is recorded on the
    /// audit seam as gone and attributed to nobody, since another writer's delete reads exactly
    /// as this pass's would (PR #823 review).
    async fn commit_sweep(&mut self, batch: &[Sweepable<'p>]) -> Result<()> {
        let whole = batch
            .iter()
            .fold(WriteBatch::new(), |acc, mark| mark.delete(acc));
        match self.ctx.meta.commit(whole).await {
            Ok(CommitOutcome::Committed) => {
                for mark in batch {
                    self.claim_sweep(mark);
                }
                return Ok(());
            }
            Ok(CommitOutcome::Conflict) => {}
            Err(err) => return self.settle_unknown_sweep(batch, err).await,
        }
        for mark in batch {
            let alone = match self.ctx.meta.commit(mark.delete(WriteBatch::new())).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    return self
                        .settle_unknown_sweep(std::slice::from_ref(mark), err)
                        .await
                }
            };
            match alone {
                CommitOutcome::Committed => self.claim_sweep(mark),
                CommitOutcome::Conflict => {
                    let reason = match self.ctx.meta.get(&mark.key).await? {
                        // Deleted by another writer: nothing about it for this pass to claim.
                        None => "mark-gone",
                        // A mark is at the key and this pass did not delete it: the next pass
                        // reads it, so this one certifies nothing.
                        Some(now) => {
                            self.lost_sweep = true;
                            if now == encode_orphan_mark(mark.mark) {
                                "mark-unchanged"
                            } else {
                                "mark-changed"
                            }
                        }
                    };
                    emit_mark_skip(mark.dserver, mark.frag, reason);
                }
            }
        }
        Ok(())
    }

    /// A sweep commit answered `Err`. If it is a [`CommitUnknownResult`] the backend says is out
    /// of flight, judge the batch **as the one atomic commit it was**, from a fresh read of every
    /// key in it. The commit deleted all of its marks or none, so a key still holding the bytes
    /// the pass read proves the batch did **not** land — its precondition on that key would have
    /// taken the key with it — and every key of the batch found absent was then deleted by
    /// another writer (`mark-gone`, as the re-read after a lost precondition judges it), not by
    /// this pass. With no such survivor nothing proves the batch landed either: a key found
    /// absent may have been taken by this pass's delete or by another writer's, and one found
    /// rewritten may have been rewritten over this pass's delete or instead of it. An absent key
    /// is then named `sweep-unattributed` on the audit seam — gone, after a commit whose result
    /// the store could not report — and claimed by nobody: not audited as this pass's sweep, not
    /// counted, and the pass answers `Partial` for it, never `Changed` or `Satisfied`. A key still
    /// holding a mark is left for the next pass in every case, as a lost precondition's is.
    ///
    /// The reads are sequential and a writer may land between them; the judgement does not
    /// depend on their order. The batch was out of flight before the first read, so a survivor
    /// holding the read bytes disproves the landing at whatever instant it is read, and an
    /// absent key is never attributed, whenever it is read. What the judgement rests on is the
    /// commit's atomicity, which the `MetadataStore` contract guarantees (PR #823 review).
    ///
    /// If the batch **may still commit** no re-read can judge it: each mark is named as unsettled
    /// and the fault ends the pass, as every other `Err` does — a later pass reads whichever way
    /// it went. Any other error is returned untouched.
    async fn settle_unknown_sweep(&mut self, marks: &[Sweepable<'p>], err: BoxError) -> Result<()> {
        let Some(unknown) = err.downcast_ref::<CommitUnknownResult>() else {
            return Err(err);
        };
        if unknown.may_still_commit {
            for mark in marks {
                emit_mark_unsettled(mark.dserver, mark.frag, &unknown.detail);
            }
            self.lost_sweep = true;
            return Err(err);
        }
        // Every key first, then the judgement: it is one commit that is being judged, and a
        // survivor anywhere in it speaks for every key of it.
        let mut reads = Vec::with_capacity(marks.len());
        for mark in marks {
            reads.push(self.ctx.meta.get(&mark.key).await?);
        }
        let landing_disproved = marks.iter().zip(&reads).any(|(mark, now)| {
            now.as_ref()
                .is_some_and(|now| *now == encode_orphan_mark(mark.mark))
        });
        for (mark, now) in marks.iter().zip(&reads) {
            match now {
                // The batch did not land, so this pass did not delete it: another writer did.
                None if landing_disproved => emit_mark_skip(mark.dserver, mark.frag, "mark-gone"),
                // Gone, and nothing says by whose hand.
                None => {
                    self.lost_sweep = true;
                    emit_mark_gone_unattributed(mark.dserver, mark.frag, &unknown.detail);
                }
                Some(now) => {
                    self.lost_sweep = true;
                    let reason = if *now == encode_orphan_mark(mark.mark) {
                        "mark-unchanged"
                    } else {
                        "mark-changed"
                    };
                    emit_mark_skip(mark.dserver, mark.frag, reason);
                }
            }
        }
        Ok(())
    }

    /// Claim one sweep whose commit has landed: its audit event, its count, and the pass's answer.
    fn claim_sweep(&mut self, mark: &Sweepable<'_>) {
        emit_mark_swept(mark.dserver, mark.frag);
        self.marks_swept = true;
    }

    fn reclaimed(&mut self, chunk: ChunkId) {
        self.changed = true;
        if self.expired_pending.contains(&chunk) {
            self.reclaimed_expired.insert(chunk);
        }
    }

    /// An unprotected fragment of `chunk` survives this pass — a mark in its grace window, one
    /// outside this window or not readable, a draining retirement, a lost intent, or no evidence
    /// that can be seen yet.
    fn held(&mut self, chunk: ChunkId) {
        if self.expired_pending.contains(&chunk) {
            self.still_held.insert(chunk);
        }
    }
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

/// The **staged protection class** (proposal 0016 decision 2, `0016:765-893`): every fragment a
/// multipart upload's own records name — the chunks of its committed parts (`part:`) and the
/// planned placement of its in-flight owned staging entries (`sidx:`) — for **every** session
/// listed under `mpu:`, whatever its state. 0016 counts fewer (`0016:770-775`); covering more only
/// keeps more, and a session's state is not needed to find its records, only its upload id.
///
/// A class of its own, **disjoint** from the committed [`ReferenceSet`] rather than merged into
/// it, so each consumer decides for itself what staged bytes mean to it (`0016:767-782`, `:881`),
/// and built by a reader of its own ([`staged_fragments`]) rather than inside
/// [`referenced_fragments`]. Four passes read it: the two that delete or mark — GC's reclaim
/// ([`reconcile`]) and the post-restore mark gate ([`crate::restore`]) — the operator's
/// drain-status query ([`crate::desired_state::reconciliation_status`]), which counts a staged
/// fragment as held so a drain is never certified over a live upload's bytes (`0016:826-827`),
/// and reconstruction (`crate::reconstruction`), which never drains an obligation for a chunk
/// this class still names. Scrub reads a narrower slice of the same protocol instead — every
/// session's committed `part:` records, never its `sidx:` entries, through a reader of its own
/// ([`crate::scrub`]) — so an upload's owned-entry damage, a store fault reading one, or the
/// cost of reading them cannot reach scrub's answer.
///
/// Its rules mirror the committed set's, one level each:
///
/// - a record whose placement **cannot be trusted** — a staged placement whose length is not its
///   scheme's fragment count, or an owned value that will not decode under a key that still names
///   its chunk — holds that chunk **whole** ([`Self::held`]), as a malformed committed placement
///   does (0016 X65, `0016:2594`; ADR-0045 decision 3). A staged placement is held to the exact
///   length: the identity fallback a committed map's empty placement gets exists for records
///   written before placements were, and every staged record is born with a full one
///   (`0016:828`);
/// - a record that **cannot be read at all** — a session key naming no upload, a part key the
///   parser rejects or a part value that will not decode, an owned key naming no chunk — hides
///   which chunks it protects, so the class is **incomplete** ([`Self::unresolvable`]) and
///   protects every fragment in the fleet, exactly as an unreadable committed map does.
///
/// deferred: #814 — rebuilding or re-placing a staged chunk. #663's other half is discharged:
/// scrub now checks a session's committed `part:` fragments (`crate::scrub`) and reconstruction
/// now keeps rather than drains an obligation this class still names
/// (`crate::reconstruction`); neither one repoints or re-places over a staged record, which
/// stays #814's. (#664's half — drain status and rebalance — was already discharged: the
/// drain-status query above reads this class, and rebalance is disjoint from it by
/// construction, `crate::rebalance::plan_evacuations`.)
#[derive(Default)]
pub(crate) struct StagedSet {
    /// `(dserver, fragment)` a staged record places: each chunk of a committed part at its recorded
    /// placement, each owned entry's chunk at its planned one.
    pub placed: HashSet<(DServerId, FragmentId)>,
    /// Chunks held **whole**, each with the staged record(s) naming it that could not be trusted —
    /// by key as the store spells it, and why — for attribution. Every fragment bearing one of
    /// these ids is protected.
    pub held: BTreeMap<ChunkId, Vec<(Vec<u8>, String)>>,
    /// Staged records that could not be read at all, keyed by their raw key bytes and valued by the
    /// fault — [`ReferenceSet::unresolvable`]'s shape, for its reasons (a rendered name is not
    /// injective, and a key that will not parse is still a record a human has to go and find).
    /// While this is non-empty the class is **incomplete**.
    pub unresolvable: BTreeMap<Vec<u8>, String>,
}

impl StagedSet {
    /// **Why** `frag` on `dserver` is protected by the staged class — the audit reason, one per
    /// rule, never a committed-set reason — or `None` when this class does not protect it.
    pub fn protection(&self, dserver: DServerId, frag: FragmentId) -> Option<&'static str> {
        if self.placed.contains(&(dserver, frag)) {
            Some("staged")
        } else if self.held.contains_key(&frag.chunk) {
            Some("untrusted-staged-record")
        } else if !self.unresolvable.is_empty() {
            Some("incomplete-staged-set")
        } else {
            None
        }
    }

    /// Whether the staged class protects `frag` on `dserver`: a staged placement names it, a staged
    /// record that cannot be trusted names its chunk, or **anything at all** while a staged record
    /// could not be read — [`ReferenceSet::protects`]' containment, for the other class.
    pub fn protects(&self, dserver: DServerId, frag: FragmentId) -> bool {
        self.protection(dserver, frag).is_some()
    }

    /// Classify one `sidx:` entry through the namespace's one decode entry point
    /// ([`decode_owned_entry`]), which checks key and value together.
    fn read_owned_entry(&mut self, key: &[u8], value: &[u8]) {
        match decode_owned_entry(key, value) {
            Ok((_part, chunk, entry)) => {
                let staged = entry.staged();
                // The planned placement in the shape both placement rules are stated over. An
                // owned entry records no logical length, and neither rule reads one.
                let planned = ChunkRef {
                    id: chunk,
                    scheme: staged.scheme(),
                    len: 0,
                    placement: staged.placement().to_vec(),
                };
                self.place(key, &planned);
            }
            // The decode failed; whether the damage is confined to one chunk is the key's to say.
            Err(fault) => match parse_sidx_key(key) {
                Ok((_upload, _part, chunk)) => self.hold(chunk, key, fault.to_string()),
                Err(_) => {
                    self.unresolvable.insert(key.to_vec(), fault.to_string());
                }
            },
        }
    }

    /// Classify one `part:` record. Its key and its value are validated separately
    /// ([`parse_part_key`], [`decode_part_record`]), and a record either refuses is one this pass
    /// cannot read: a value naming chunks under a key no writer spells is not a part anyone can
    /// publish or retire.
    fn read_part(&mut self, key: &[u8], value: &[u8]) {
        match parse_part_key(key).and_then(|_| decode_part_record(value)) {
            Ok(part) => {
                for chunk in part.chunks() {
                    self.place(key, chunk);
                }
            }
            Err(fault) => {
                self.unresolvable.insert(key.to_vec(), fault.to_string());
            }
        }
    }

    /// Place `chunk`, which the staged record under `key` names — or hold it whole when its
    /// placement is not exactly one D server per fragment ([`staged_placement`], the one rule
    /// both staged readers resolve a staged placement through).
    fn place(&mut self, key: &[u8], chunk: &ChunkRef) {
        match staged_placement(chunk) {
            Ok(frags) => {
                for (index, dserver) in frags {
                    self.placed.insert((
                        dserver,
                        FragmentId {
                            chunk: chunk.id,
                            index,
                        },
                    ));
                }
            }
            Err(m) => self.hold(
                chunk.id,
                key,
                format!(
                    "staged placement names {} D server(s) for a scheme of {} fragment(s)",
                    m.actual, m.expected
                ),
            ),
        }
    }

    fn hold(&mut self, chunk: ChunkId, key: &[u8], fault: String) {
        self.held
            .entry(chunk)
            .or_default()
            .push((key.to_vec(), fault));
    }
}

/// Resolve the placement a **staged** record (`sidx:` or `part:`) gives a chunk — the one rule
/// every staged reader in this module shares ([`StagedSet::place`] for the protection class,
/// [`read_staged_part`] for the scrub-checked one), so neither can drift from the other about
/// which staged placements are trustworthy.
///
/// **Exact length, no identity fallback.** A staged placement is valid *iff* it names exactly one
/// D server per fragment of its own scheme; any other length — the EMPTY vector included — is
/// [`MalformedPlacement`]. This is deliberately STRICTER than the committed rule
/// ([`wyrd_core::metadata::ChunkRef::checked_fragments`], ADR-0040 decisions 3–4), which admits
/// an empty vector as the pre-M3 identity fallback: that exemption exists for committed records
/// written before placements were, and **every** staged record is born with a full placement
/// (`0016:828`). So an empty staged placement can only be damage, and identity-filling it would
/// hand a reader D-server locations no record ever named — protecting fragments at fabricated
/// positions on GC's side, and on scrub's fetching and then enqueueing *phantom* repair
/// obligations against them (`0016` X65, `0016:2594`; ADR-0045 decision 3). Damage is contained
/// instead: the caller holds or reports the chunk, and fabricates nothing.
fn staged_placement(
    chunk: &ChunkRef,
) -> std::result::Result<impl Iterator<Item = (u16, DServerId)> + '_, MalformedPlacement> {
    let expected = chunk.fragment_count();
    if chunk.placement.len() == usize::from(expected) {
        Ok(chunk.fragments())
    } else {
        Err(MalformedPlacement {
            expected,
            actual: chunk.placement.len(),
        })
    }
}

/// Read the [`StagedSet`]: list the sessions under `mpu:`, then read each one's owned staging
/// entries (`sidx:<id>:`) and **then** its committed parts (`part:<id>:`).
///
/// **The order is the protection** (`0016:782-800`). A part commit deletes a chunk's owned entry
/// and writes the part record naming it in one batch, so reading parts before owned entries could
/// see the chunk in neither; reading the source first sees it in one or both. Publication moves the
/// same protection on to a committed inode, so the caller reads this class before the committed
/// reference set ([`referenced_fragments`]) for the same reason. Reads within a range may be paged
/// without weakening either: a whole source range is read before its destination is begun, so a
/// move that lands mid-walk is still seen on one side of it.
///
/// **Bounded reads, never a namespace scan** (`0016:890`). Every range is walked in pages of at
/// most [`STAGED_PAGE`], through the same checked page read the orphan-ledger walk uses: the
/// listing, then two ranges per session. No read covers another session's records, so none grows
/// with the fleet's upload population beyond the session count admission bounds. A `part:` or
/// `sidx:` record whose session is no longer listed is therefore not read: no per-session range
/// can reach it. Each read's await is bounded as every other custodian read is, by the
/// `MetadataStore` implementation's own network bound (#508/#636); a read that fails fails the
/// pass closed.
///
/// **What it cannot read or trust, it contains** (ADR-0045 decision 3): see [`StagedSet`]. A
/// session record's **value** is never decoded: protection does not depend on the state, and a
/// damaged value still names its ranges through its key. A **store fault** is not contained: the
/// reading is then missing an unknown part of some session's records, so it propagates as a
/// [`StagedReadFault`] naming the range that failed, and the pass fails before it deletes or marks
/// anything.
pub(crate) async fn staged_fragments(meta: &dyn MetadataStore) -> Result<StagedSet> {
    // deferred: #806 — the `mpuctl` budget-profile preflight (`0016:348`, X99 `0016:2628`): read
    // the admission record and fail closed with an alarm, before this build, when its stored
    // profile differs from the custodian's own, so a rolling profile change cannot grow this
    // reading past the `W_ref` the custodian was sized for. Until then the reading's total size is
    // bounded only by the profile the gateways admitted sessions under, which this pass does not
    // check; each page of it stays bounded by `STAGED_PAGE`.
    let mut set = StagedSet::default();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (sessions, next) = staged_page(meta, MPU_PREFIX, after.as_deref()).await?;
        for (key, _session) in &sessions {
            let upload = match parse_mpu_key(key) {
                Ok(upload) => upload,
                Err(fault) => {
                    set.unresolvable.insert(key.clone(), fault.to_string());
                    continue;
                }
            };
            walk_staged_range(meta, &sidx_range(&upload), |key, value| {
                set.read_owned_entry(key, value)
            })
            .await?;
            walk_staged_range(meta, &part_range(&upload), |key, value| {
                set.read_part(key, value)
            })
            .await?;
        }
        match (next, sessions.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(set),
        }
    }
}

/// The **scrub-checked staged class** (0016 decision 2, `0016:824-825`; split from #663): every
/// fragment a session's COMMITTED `part:` record places, with the [`EcScheme`] that record's
/// own [`ChunkRef`] carries — never an in-flight `sidx:` entry. Checking a fragment needs the
/// COMMITTED scheme a part record carries; an owned entry's is only planned, not yet committed
/// (`0016:776-781`), so scrub — which only ever verifies committed state — leaves an in-flight
/// chunk alone by construction (leg B, `crates/custodian/tests/staged_scrub.rs`).
///
/// A reader of its own, not [`StagedSet`]: `StagedSet` folds `sidx:` and `part:` placements
/// into one set because GC's and restore's question is "is this fragment protected", never
/// "which record placed it, and under what scheme" — the question this one answers instead.
/// Widening `StagedSet`'s own shape for one consumer would cost every other one a field it
/// never reads; a second, PART-ONLY walk keeps the boundary explicit.
pub(crate) struct StagedPartSet {
    /// `(dserver, fragment)` a committed part record places, each with the scheme its own
    /// `ChunkRef` carries — mirrors [`ReferenceSet::schemes`], so a consumer verifies a
    /// fragment's FULL identity (index + EC tuple) against the part record, not the chunk id
    /// alone (`wyrd_core::repair::fragment_intact`, the scrub/verify contract `0005:262-267`).
    pub placed: HashMap<(DServerId, FragmentId), EcScheme>,
    /// Chunks whose committed part placement is malformed — not exactly one D server per
    /// fragment, the EMPTY vector included ([`staged_placement`], the same rule
    /// [`StagedSet::place`] holds a chunk by) — held out of `placed` rather than
    /// identity-filled, as [`ReferenceSet::malformed`] treats a malformed COMMITTED placement
    /// (ADR-0040 decision 4): the chunk's true placement cannot be trusted, so none of it is
    /// fabricated and no fragment is fetched, checked or enqueued at a position no record
    /// named. A committed chunk map's empty placement IS valid (the pre-M3 identity fallback);
    /// a staged record's never is (`0016:828`). Each chunk keeps the `part:` key(s) naming it
    /// with the damage, as [`StagedSet::held`] does, so an operator is sent to the damaged
    /// RECORD — there may be no committed object to look for at all.
    pub malformed: BTreeMap<ChunkId, Vec<(Vec<u8>, MalformedPlacement)>>,
    /// Committed part records that could not be read at all — a session key `parse_mpu_key`
    /// rejects, or a part key/value [`parse_part_key`]/[`decode_part_record`] rejects — keyed
    /// by the raw key bytes and valued by the fault, [`ReferenceSet::unresolvable`]'s shape for
    /// the same reasons. While non-empty this class is INCOMPLETE: scrub cannot say one word
    /// about the chunks the unreadable record would have named, so it refuses to certify the
    /// store rather than certify only the part it could read.
    pub unresolvable: BTreeMap<Vec<u8>, String>,
}

/// Read the [`StagedPartSet`]: list the sessions under `mpu:` (reusing the same paged listing
/// [`staged_fragments`] makes), then read each one's committed parts (`part:<id>:`) — never its
/// owned staging range. Bounded exactly as [`staged_fragments`] is: every range is walked in
/// pages of at most [`STAGED_PAGE`] ([`walk_staged_range`]), never one `scan` of a whole
/// namespace (`0016:890`).
///
/// **The caller reads this class BEFORE the committed reference set** ([`referenced_fragments`]),
/// for the reason [`staged_fragments`]'s own caller does (`0016:782-800`): a publication writes
/// the committed inode and only LATER deletes the `part:` record it replaces, so a reading that
/// took the destination first could see a chunk in neither class — and for scrub that means never
/// fetching its fragments while certifying the store (`crate::scrub::reconcile`).
///
/// **What it cannot read or trust, it contains** (ADR-0045 decision 3), [`StagedSet`]'s own
/// rule for the same two record shapes: an unparsable session or part key, or an undecodable
/// part value, names that record in [`StagedPartSet::unresolvable`] and the walk goes on over
/// every OTHER record — one damaged part record does not stop scrub from checking the rest of
/// the store. A **store** fault is not contained: it propagates as the same [`StagedReadFault`]
/// [`staged_fragments`] raises, naming the range that failed, before scrub enqueues or
/// certifies anything.
pub(crate) async fn staged_committed_parts(meta: &dyn MetadataStore) -> Result<StagedPartSet> {
    let mut set = StagedPartSet {
        placed: HashMap::new(),
        malformed: BTreeMap::new(),
        unresolvable: BTreeMap::new(),
    };
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (sessions, next) = staged_page(meta, MPU_PREFIX, after.as_deref()).await?;
        for (key, _session) in &sessions {
            let upload = match parse_mpu_key(key) {
                Ok(upload) => upload,
                Err(fault) => {
                    set.unresolvable.insert(key.clone(), fault.to_string());
                    continue;
                }
            };
            walk_staged_range(meta, &part_range(&upload), |key, value| {
                read_staged_part(&mut set, key, value)
            })
            .await?;
        }
        match (next, sessions.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(set),
        }
    }
}

/// Classify one `part:` record for [`staged_committed_parts`] — [`StagedSet::read_part`]'s
/// twin, scheme-tagged for scrub's verify against the chunk's committed EC tuple instead of
/// expanded into a reclaim-protection set.
fn read_staged_part(set: &mut StagedPartSet, key: &[u8], value: &[u8]) {
    match parse_part_key(key).and_then(|_| decode_part_record(value)) {
        Ok(part) => {
            for chunk in part.chunks() {
                // Classify the committed placement BEFORE expanding it (ADR-0040 decision 4),
                // exactly as `referenced_fragments` does for a committed chunk map — but
                // through the STAGED rule ([`staged_placement`], the one
                // [`StagedSet::place`] applies), not the committed one: a part record's
                // placement is never identity-filled, the empty vector included, because
                // every staged record is born with a full one (`0016:828`). Filling one
                // would have scrub fetch — and enqueue phantom repairs against — D servers
                // no record ever named.
                match staged_placement(chunk) {
                    Ok(frags) => {
                        for (index, dserver) in frags {
                            set.placed.insert(
                                (
                                    dserver,
                                    FragmentId {
                                        chunk: chunk.id,
                                        index,
                                    },
                                ),
                                chunk.scheme,
                            );
                        }
                    }
                    Err(m) => {
                        let records = set.malformed.entry(chunk.id).or_default();
                        records.push((key.to_vec(), m));
                    }
                }
            }
        }
        Err(fault) => {
            set.unresolvable.insert(key.to_vec(), fault.to_string());
        }
    }
}

/// Walk every record under `range`, page by page, handing each to `read`.
async fn walk_staged_range(
    meta: &dyn MetadataStore,
    range: &[u8],
    mut read: impl FnMut(&[u8], &[u8]),
) -> Result<()> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = staged_page(meta, range, after.as_deref()).await?;
        for (key, value) in &page {
            read(key, value);
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(()),
        }
    }
}

/// One checked page of a staged read, its failure named by the range it was reading.
async fn staged_page(
    meta: &dyn MetadataStore,
    range: &[u8],
    after: Option<&[u8]>,
) -> Result<ScanPage> {
    checked_page(meta, range, "staged-record", after, STAGED_PAGE)
        .await
        .map_err(|source| {
            BoxError::from(StagedReadFault {
                range: object_name(range),
                source,
            })
        })
}

/// A read of the staged protection class that failed, naming the key range it was reading.
///
/// The store's own error stays reachable through [`source`](std::error::Error::source), so a
/// chain-walking classifier (`wyrd_traits::classify`) still finds its class.
#[derive(Debug)]
struct StagedReadFault {
    /// The key range being read, escaped as [`object_name`] escapes a key.
    range: String,
    source: BoxError,
}

impl std::fmt::Display for StagedReadFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "staged protection: reading the key range {} failed: {}",
            self.range, self.source
        )
    }
}

impl std::error::Error for StagedReadFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
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
#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadMark {
    /// A legacy or structured mark: the instant its fragment was orphaned and, for a structured
    /// one, the unreference event that wrote it — decoded from exactly the bytes read, which is
    /// what the reclaim intent's precondition re-encodes.
    Stamped(OrphanMark),
    /// A mark GC has already moved to `reclaiming`: the reclamation is decided, and what is left
    /// is to finish it. Its stamp and bytes are kept for the fragment-less sweep, which ages it
    /// and deletes it on the exact value read once its fragment is gone.
    Reclaiming(OrphanMark),
    /// The mark, holding a value that is none of the three shapes. Still a mark: it is evidence
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
    fn mark_of(&self, dserver: DServerId, frag: FragmentId) -> Option<&ReadMark> {
        self.marks.get(&(dserver, frag))
    }

    /// Every mark this window read — the position it names and what the window read of its
    /// value — ordered by position (D server, chunk, index), so whatever a pass does over them it
    /// does in the same order on every run.
    fn marks_in_position_order(&self) -> Vec<(DServerId, FragmentId, &ReadMark)> {
        let mut marks: Vec<_> = self
            .marks
            .iter()
            .map(|(&(dserver, frag), read)| (dserver, frag, read))
            .collect();
        marks.sort_unstable_by_key(|&(dserver, frag, _)| (dserver, frag.chunk, frag.index));
        marks
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
/// The value is decoded through the one codec every mark writer and reader shares
/// ([`decode_orphan_mark`]): legacy and structured marks carry their stamp, a `reclaiming` mark
/// its recorded decision. A value that is none of the three shapes still counts as its
/// fragment's mark ([`ReadMark::Unreadable`]) and is named on the audit seam, and nothing this
/// pass writes touches it.
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
    let mark = match decode_orphan_mark(value) {
        Ok(mark) if mark.is_reclaiming() => ReadMark::Reclaiming(mark),
        Ok(mark) => ReadMark::Stamped(mark),
        Err(fault) => {
            emit_unreadable_mark(&object_name(key), &fault.to_string());
            ReadMark::Unreadable
        }
    };
    marks.insert(slot, mark);
}

/// One page of the `orphan:` ledger strictly after `after`, at most `limit` entries — refused
/// if it is longer than that, or does not move the walk forward ([`checked_page`]).
async fn ledger_page(
    meta: &dyn MetadataStore,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<ScanPage> {
    checked_page(meta, ORPHAN_PREFIX, "orphan-ledger", after, limit).await
}

/// One page of the keys under `prefix` strictly after `after`, at most `limit` entries — refused
/// if it is longer than that, or does not move the walk forward. `walk` names the walk in the
/// refusal.
///
/// `scan_page`'s contract already promises both (a page holds at most `limit` entries, starts
/// strictly after its cursor, and is empty only at the end), but a walk that trusted it blindly
/// would read past its budget on a store that broke the bound — the pass would no longer be
/// bounded — and loop forever on one that broke the cursor. So a walker checks the two things its
/// bound and its termination rest on: the page is no longer than it asked for, and it either
/// ends the walk or carries it past `after`. Anything else is an error, never a page accepted
/// silently. Shared by every paged walk in this module — the `orphan:` ledger's and the staged
/// protection class's — so they cannot disagree about what a usable page is.
async fn checked_page(
    meta: &dyn MetadataStore,
    prefix: &[u8],
    walk: &str,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<ScanPage> {
    let (page, next) = meta.scan_page(prefix, after, limit).await?;
    if page.len() > limit {
        return Err(BoxError::from(format!(
            "{walk} scan_page after {:?} returned {} entries for a limit of {limit} — \
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
            "{walk} scan_page after {:?} returned a page that does not advance the walk \
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

    /// Commit whatever is still queued after `fault` ended the pass — best effort.
    ///
    /// Every key queued by then is the mark of a fragment the pass has already deleted, so
    /// dropping the batch with the fault would leave each of those marks `reclaiming` over bytes
    /// that are gone, a position no `list_fragments()`-driven walk visits again — only the
    /// fragment-less sweep, a late-write deadline later. A failure here is
    /// named on the audit seam and dropped: `fault` is what the pass reports. Only deletes never
    /// attempted are still queued — a batch whose own commit failed was taken when it was tried,
    /// and is not retried, because its result may be unknown and a blind delete applied after a
    /// first attempt that did land could take a mark a writer wrote in between.
    async fn finish_after_fault(self, fault: &BoxError) {
        if let Err(also) = self.finish().await {
            emit_cleanup_lost(&fault.to_string(), &also.to_string());
        }
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
/// event (`0005:336-340`). `reason` is `orphan` (a mark this pass moved to `reclaiming`),
/// `resumed` (a mark an earlier pass left `reclaiming`) or `expired-lease`.
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

/// Emit a fragment-less mark the sweep deleted on the durability-plane seam (ADR-0011 /
/// ADR-0012): a metric the `DurabilityTelemetry` bridge counts, plus an append-only audit event —
/// [`emit_reclaim`]'s pair, for a mark rather than for fragment bytes. Emitted only once the
/// commit that deleted the mark has landed ([`Sweep::commit_sweep`]).
fn emit_mark_swept(dserver: DServerId, frag: FragmentId) {
    tracing::info!(monotonic_counter.gc_orphan_marks_swept = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "sweep-mark",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "gc deleted an orphan mark with no fragment beneath it: this pass's own listing, taken past the mark's late-write deadline, showed its position empty",
    );
}

/// Emit a sweep delete whose commit answered an out-of-flight [`CommitUnknownResult`] and whose
/// key a fresh read then found absent, with nothing in the batch proving the commit did not land
/// ([`Sweep::settle_unknown_sweep`]): the mark is gone, and whether this pass's delete or another
/// writer's took it cannot be told, so the pass claims neither — not [`emit_mark_swept`]'s action
/// and not its counter. A counter of its own, so an operator can see how much of the trail is
/// deletion attributed to nobody, and `detail` carrying the backend's account of the commit.
fn emit_mark_gone_unattributed(dserver: DServerId, frag: FragmentId, detail: &str) {
    tracing::warn!(monotonic_counter.gc_orphan_mark_sweeps_unattributed = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "sweep-unattributed",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        detail = %detail,
        "an orphan mark is gone after a sweep delete whose commit result the store could not report: taken by this pass's delete or by another writer's, and claimed by neither; the pass answers Partial",
    );
}

/// Emit a sweep delete whose commit answered a [`CommitUnknownResult`] that **may still commit**
/// — nothing this pass can read settles it — on the durability-plane seam: the pass ends on the
/// fault, and the next pass reads whichever way it went.
fn emit_mark_unsettled(dserver: DServerId, frag: FragmentId, detail: &str) {
    tracing::warn!(monotonic_counter.gc_orphan_mark_sweeps_unsettled = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "sweep-unsettled",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        detail = %detail,
        "gc could not tell whether its delete of an orphan mark landed (the commit may still be applied); the pass ends and the next one reads the outcome",
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

/// Emit a staged multipart record GC could **not read** on the durability-plane seam (ADR-0011 /
/// ADR-0012): the staged class is incomplete because of it, so this pass reclaims nothing
/// fleet-wide and certifies nothing until that record is repaired — [`emit_unresolvable`]'s
/// signal, for the other class. Named by key, escaped as [`object_name`] escapes an `inode:` key.
/// Emitted from the GC loop, never from the reader both GC and the post-restore pass share.
fn emit_unresolvable_staged(record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.gc_unresolvable_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unresolvable-staged-record",
        record = %record,
        fault = %fault,
        "gc could not read a staged multipart record; its staged set is incomplete, so gc reclaims NOTHING and certifies NOTHING until this record is repaired — operator signal",
    );
}

/// Emit a staged multipart record GC could read but **not trust** on the durability-plane seam
/// (ADR-0011 / ADR-0012): its placement is of the wrong length, or its value will not decode under
/// a key that still names `chunk`. Every fragment of that chunk is held, never reclaimed, until
/// the record is repaired — [`emit_malformed`]'s containment, attributed to the record.
fn emit_untrusted_staged(record: &str, chunk: ChunkId, fault: &str) {
    tracing::warn!(monotonic_counter.gc_untrusted_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "untrusted-staged-record",
        record = %record,
        chunk = %wyrd_traits::chunk_hex(chunk),
        fault = %fault,
        "gc found a staged multipart record it cannot trust about where its chunk's fragments are; every fragment of that chunk is held, NEVER reclaimed — operator signal",
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

/// Emit an `orphan:` mark whose value is **none of the three mark shapes** on the durability-plane
/// seam (ADR-0011 / ADR-0012): the mark still counts as its fragment's mark, so the fragment is
/// kept on no other evidence — neither a grace window nor an expired lease can reclaim it — and
/// the mark is left in place, byte for byte, until a human repairs the value. Named by key,
/// escaped as [`object_name`] escapes an `inode:` key, as [`emit_unreadable_pending`] names its
/// entry, with the codec's reason.
fn emit_unreadable_mark(mark: &str, fault: &str) {
    tracing::warn!(monotonic_counter.gc_unreadable_orphan_marks = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "unreadable-orphan-mark",
        mark = %mark,
        fault = %fault,
        "gc could not read an orphan-ledger mark's value as any mark shape; it still counts as a mark, so its fragment is kept and the mark left in place — operator signal",
    );
}

/// Emit a pass whose best-effort commit of its queued key deletes failed after a store fault had
/// already ended it ([`Cleanup::finish_after_fault`]) on the durability-plane seam (ADR-0011 /
/// ADR-0012): the marks of fragments it deleted may remain, `reclaiming`, over bytes that are
/// gone. Safe — nothing preconditioned on their earlier bytes can commit — and a later pass's
/// fragment-less sweep deletes them ([`Sweep::sweep_fragment_less_marks`]), but an operator
/// should know.
fn emit_cleanup_lost(fault: &str, cleanup: &str) {
    tracing::warn!(monotonic_counter.gc_cleanup_lost_after_fault = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.gc.audit",
        action = "cleanup-lost-after-fault",
        fault = %fault,
        cleanup = %cleanup,
        "gc could not commit the key deletes of fragments it had already reclaimed when a store fault ended its pass; those marks stay `reclaiming` over deleted bytes — operator signal",
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

/// Emit a skip on the same seam — the observable record that GC *considered* and *declined* a
/// fragment: still protected, within its grace window, named by a retirement still draining, or
/// its mark changed before the pass could record its reclaim (`mark-changed`).
fn emit_skip(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_fragments_skipped = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "skip",
        reason,
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "gc declined a fragment (still protected, within its grace window, under a draining retirement, or its mark changed)",
    );
}

/// Emit a fragment-less mark the sweep considered and did **not** delete on the same seam —
/// [`emit_skip`]'s record, for a mark rather than a fragment: its D server is not in this pass's
/// fleet (`server-not-in-fleet`), a protection class covers its position (the class's own reason,
/// as for a fragment), it is inside its late-write deadline (`within-late-write-deadline`), it is a
/// structured mark whose writer may still have a write in flight (`event-may-await-write`), or its
/// delete lost and a fresh read found it rewritten (`mark-changed`), as it was
/// (`mark-unchanged`), or deleted by another writer (`mark-gone`).
fn emit_mark_skip(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_orphan_marks_skipped = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "skip-mark",
        reason,
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "gc did not delete an orphan mark with no fragment beneath it (its server was not listed, its position is protected, it is inside its late-write deadline, or its delete lost)",
    );
}
