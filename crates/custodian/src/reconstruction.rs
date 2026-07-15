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
//! no backend and no on-disk-format knowledge of its own.

use std::collections::HashMap;

use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain, Topology};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

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
    inode_id: InodeId,
    prior: InodeRecord,
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

/// One reconstruction reconciliation pass over `ctx` at logical time `now_millis`.
/// Dispatched only from [`crate::reconcile_step`] (the fenced control point) — never a
/// parallel entry. Returns [`Reconciled::Changed`] if any chunk's placement record was
/// repointed, [`Reconciled::Satisfied`] otherwise.
pub(crate) async fn reconcile(
    ctx: &ReconstructionContext<'_>,
    now_millis: u64,
) -> Result<Reconciled> {
    let stores: HashMap<DServerId, &dyn ChunkStore> = ctx.fleet.iter().copied().collect();

    // Drain the shared repair queue: the obligations scrub / the read path produced.
    let queue = repair::queued_repairs(ctx.meta).await?;
    emit_queue_depth(queue.len());

    // Assess each obligation (resolve the chunk, gather + verify survivors) so the
    // drain can be ordered by repair priority before any rebuild commits.
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
    for chunk in queue {
        match assess(ctx, &stores, chunk).await? {
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
    // recorded on `reconstruction_conflict`, and one that cannot proceed (the selector
    // chose a server outside the fleet view, so nothing is committed) on
    // `reconstruction_aborted` — so successful repairs are
    // `reconstruction_repaired − conflict − aborted`. Both offsets leave the obligation
    // queued, to be re-assessed next pass.
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

    let mut changed = false;
    for plan in &plans {
        match repair_chunk(ctx, &stores, plan, now_millis).await? {
            RepairOutcome::Committed => changed = true,
            RepairOutcome::Conflict => emit_conflict(plan.chunk_id),
            RepairOutcome::Aborted => emit_aborted(plan.chunk_id),
        }
    }

    // Drain the no-op obligations in one commit (best-effort; not the binding repoint).
    if !drain_only.is_empty() {
        let mut batch = WriteBatch::new();
        for chunk in drain_only {
            batch = batch.delete(repair::repair_key(chunk));
        }
        ctx.meta.commit(batch).await?;
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
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
}

/// Resolve `chunk` to its committed chunk map, then gather and **verify** its surviving
/// fragments — classifying it into an [`Assessment`].
async fn assess(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    chunk: ChunkId,
) -> Result<Assessment> {
    let Some((inode_id, prior, chunk_index)) = find_chunk(ctx.meta, chunk).await? else {
        // The chunk is referenced by no committed chunk map — it was deleted out from
        // under the obligation. Nothing to repair.
        return Ok(Assessment::Drain);
    };
    let chunk_ref = prior.chunk_map[chunk_index].clone();

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

    let mut survivors = Vec::new();
    let mut survivor_domains = Vec::new();
    let mut missing = Vec::new();
    // How many missing fragments are missing ONLY because their placed D server is
    // configured-but-unreachable this pass (`ctx.unreachable`) — transiently unavailable, not
    // confirmed lost. Used below to distinguish a recoverable degraded state from real data
    // loss (iteration-7 MUST-FIX). Zero in the M3 library contexts (empty `unreachable`), so a
    // missing fragment is always `confirmed gone` there — prior semantics unchanged.
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
        // a checksum-failing, misplaced, or mis-encoded fragment is excluded (never
        // decoded) and treated as missing (`0005:275`). `repair::intact_shard` is the
        // shared verify, so `custodian` recovers the shard without a chunk-format
        // dependency.
        match bytes
            .as_deref()
            .and_then(|b| repair::intact_shard(b, frag, chunk_ref.scheme))
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

    if missing.is_empty() {
        // Already at full redundancy: a stale / duplicate obligation. Drain it.
        return Ok(Assessment::Drain);
    }
    if survivors.len() < k {
        // Below the scheme's tolerance. Distinguish a REACHABILITY-driven shortfall from a
        // CONFIRMED one: fragments missing only because their D server is transiently
        // unreachable this pass return with the server, so counting them as present may reach
        // `k`. Only when even then the chunk is still below `k` is data confirmably LOST
        // (iteration-7 MUST-FIX) — otherwise a rolling restart / partition would falsely page
        // as permanent data loss on physically-intact fragments.
        if survivors.len() + transient_missing >= k {
            return Ok(Assessment::Unreachable);
        }
        return Ok(Assessment::Unrepairable);
    }
    // Repairable in principle (survivors ≥ `k`). But the rebuild must PLACE each missing shard
    // in a failure domain distinct from every survivor's; if no free distinct domain remains
    // (a minimal cluster at exactly `n` with a domain down), the repair cannot proceed this
    // pass. Route it to the distinct `Blocked` signal rather than the repairable-backlog gauge
    // — a never-completable repair counted there would floor the day-one "returns to zero"
    // gauge at ≥1 forever (iteration-7 MUST-FIX). It becomes repairable again when a free
    // distinct domain (capacity) returns. This mirrors the selector `repair_chunk` runs, so a
    // chunk that WOULD abort in the repair loop is diverted before it inflates the backlog.
    if select_distinct_domains_excluding(ctx.topology, missing.len() as u16, &survivor_domains)
        .is_err()
    {
        return Ok(Assessment::Blocked);
    }

    Ok(Assessment::Repairable(Box::new(RepairPlan {
        inode_id,
        prior,
        chunk_index,
        chunk_id: chunk,
        k,
        m,
        survivors,
        survivor_domains,
        missing,
        placement,
        len: chunk_ref.len as usize,
    })))
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
    /// aborted plan is not mistaken for a success (see [`emit_aborted`]).
    Aborted,
}

/// Rebuild `plan`'s missing fragment(s), re-place them in distinct failure domains, and
/// repoint the chunk's placement record with **one version-conditional commit**.
async fn repair_chunk(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
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

    // Write the rebuilt fragments to their new D servers FIRST — before the commit, so a
    // crash here leaves only collectable garbage, never a torn chunk (`0005:277`).
    let mut new_placement = plan.placement.clone();
    let mut displaced = Vec::new();
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
        target_store.put_fragment(frag, frag_bytes).await?;

        let old = plan.placement[index];
        if old != target {
            displaced.push((old, frag));
        }
        new_placement[index] = target;
    }

    // THE binding commit: ONE version-conditional mutation that atomically repoints the
    // placement record, drains the obligation, and orphans the displaced fragments. The
    // CAS on the prior inode record is the second fence (`0005:200-203`, ADR-0015) — a
    // racing writer / superseded custodian loses here rather than corrupting the record.
    let mut next_chunk_map = plan.prior.chunk_map.clone();
    next_chunk_map[plan.chunk_index].placement = new_placement;
    let next = InodeRecord {
        size: plan.prior.size,
        chunk_map: next_chunk_map,
        state: InodeState::Committed,
        version: plan.prior.version + 1,
    };
    let inode_key = metadata::inode_key(plan.inode_id);
    let mut batch = WriteBatch::new()
        .require(inode_key.clone(), metadata::encode(&plan.prior))
        .put(inode_key, metadata::encode(&next))
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

/// Find the committed inode whose chunk map references `chunk`, returning its id, the
/// full prior record (for the CAS), and the chunk's index within the map.
async fn find_chunk(
    meta: &dyn MetadataStore,
    chunk: ChunkId,
) -> Result<Option<(InodeId, InodeRecord, usize)>> {
    for (key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        if let Some(index) = record
            .chunk_map
            .iter()
            .position(|c: &ChunkRef| c.id == chunk)
        {
            if let Some(inode_id) = parse_inode_key(&key) {
                return Ok(Some((inode_id, record, index)));
            }
        }
    }
    Ok(None)
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
/// [`emit_conflict`], and one that cannot proceed (no commit) on [`emit_aborted`], so
/// successful repairs are `reconstruction_repaired − conflict − aborted`.
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

/// Emit a lost-CAS conflict on the same seam: the repoint raced another writer and the
/// rebuilt fragments are now collectable garbage.
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
/// (the selector chose a server outside the fleet view), so nothing was committed. Like
/// [`emit_conflict`], this offsets the up-front [`emit_repaired`] increment — the
/// obligation stays queued and the durability-plane success identity stays
/// `reconstruction_repaired − conflict − aborted`, so an aborted plan never inflates the
/// successful-repair count.
fn emit_aborted(chunk: ChunkId) {
    tracing::info!(monotonic_counter.reconstruction_aborted = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "aborted",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction could not place the rebuilt shard(s); nothing was committed and the obligation stays queued",
    );
}
