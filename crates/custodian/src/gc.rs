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
//! The loop's load-bearing invariant, whose violation is **silent corruption**:
//! **never reclaim a referenced fragment** — a fragment a committed chunk map's
//! placement record points at is **never** passed to `delete_fragment`
//! (`0005:294-295`, Q3 `0005:394-397`, graduation invariant `0005:488`).
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this loop stays over the
//! `traits` / `core` seams plus `tracing` — **no** concrete backend.

use std::collections::{BTreeSet, HashMap, HashSet};

// The orphan-ledger key protocol lives in `core::metadata` (beside `pending_key`) so the
// delete path that WRITES a grace record and this GC loop that READS it share one
// definition and can never key-format-drift (issue #364). Re-exported `pub(crate)` here so
// the other orphaning loops (`reconstruction.rs`, `rebalance.rs`) keep calling
// `crate::gc::orphan_key` unchanged.
pub(crate) use wyrd_core::metadata::orphan_key;
use wyrd_core::metadata::{
    self, parse_orphan_key, EcScheme, InodeRecord, InodeState, MalformedPlacement, PendingEntry,
    ORPHAN_PREFIX,
};
use wyrd_traits::{ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result, WriteBatch};

use crate::reconciliation::Reconciled;

fn parse_pending_chunk(key: &[u8]) -> Option<ChunkId> {
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
/// entry. Returns [`Reconciled::Changed`] if any fragment bytes were reclaimed,
/// [`Reconciled::Satisfied`] otherwise.
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
    // Input (1): chunks whose pending lease has expired — their fan-out garbage is
    // collectable (the lease TTL already encodes the crashed-write grace). GATED on the
    // caller's policy: a deployed pass cannot trust "expired" while any producer stamps
    // logical-clock leases (#557 / #490 — see [`ExpiredPendingPolicy`]), so under `Defer`
    // this input is empty and every `pending:` entry and its fragments survive untouched.
    let expired_pending = match ctx.expired_pending {
        ExpiredPendingPolicy::Reclaim => expired_pending_chunks(ctx.meta, now_millis).await?,
        ExpiredPendingPolicy::Defer => HashSet::new(),
    };
    // Input (2): orphaned fragments and the instant each was stranded.
    let orphaned_at = orphan_leases(ctx.meta).await?;

    let mut changed = false;
    let mut cleanup = WriteBatch::new();
    let mut swept_pending: HashSet<ChunkId> = HashSet::new();

    for &(dserver, store) in ctx.fleet {
        for frag in store.list_fragments().await? {
            // SAFETY GATE — never reclaim a referenced fragment. A fragment of a
            // malformed-placement chunk is protected the same way (fail safe): its true
            // placement cannot be trusted, so every fragment bearing its id is off-limits.
            if referenced.protects(dserver, frag) {
                let reason = if referenced.malformed.contains_key(&frag.chunk) {
                    "malformed-placement"
                } else {
                    "referenced"
                };
                emit_skip(dserver, frag, reason);
                continue;
            }

            let reason = if let Some(&since) = orphaned_at.get(&(dserver, frag)) {
                // Orphan input: reclaim ONLY after the reader-safe grace window.
                if now_millis >= since.saturating_add(ctx.grace_window_millis) {
                    Some("orphan")
                } else {
                    emit_skip(dserver, frag, "within-grace");
                    None
                }
            } else if expired_pending.contains(&frag.chunk) {
                // Expired pending-lease input: the lease TTL is its grace.
                Some("expired-lease")
            } else {
                // No evidence the grace window elapsed — conservatively keep it
                // (reader-safe: a fragment is never reclaimed without a deadline).
                None
            };

            if let Some(reason) = reason {
                store.delete_fragment(frag).await?;
                emit_reclaim(dserver, frag, reason);
                cleanup = cleanup.delete(orphan_key(dserver, frag));
                if reason == "expired-lease" {
                    swept_pending.insert(frag.chunk);
                }
                changed = true;
            }
        }
    }

    // Retire the swept pending-ledger entries (the byte reclaim the stand-in
    // deferred, `write.rs:330-331`) and the consumed orphan grace records.
    for chunk in swept_pending {
        cleanup = cleanup.delete(metadata::pending_key(chunk));
    }
    if changed {
        ctx.meta.commit(cleanup).await?;
    }

    Ok(if changed {
        Reconciled::Changed
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
/// A committed object whose map cannot be **resolved at all** — a segmented root whose
/// generation is incomplete — is the same rule one level up, and it is recorded in
/// `unresolvable`: its chunk ids are not merely untrustworthy, they are *unknown*, so the
/// set as a whole is incomplete and cannot authorize any reclamation
/// ([`ReferenceSet::protects`]).
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
    /// Committed objects whose chunk map could not be resolved, by inode key as the store
    /// spells it (`inode:<id>`) — the blockers this set is **incomplete** because of.
    ///
    /// Rendered rather than parsed: this is attribution, it is what the audit seam emits,
    /// and a key that did not parse would otherwise drop a blocker on the floor.
    pub unresolvable: BTreeSet<String>,
}

impl ReferenceSet {
    /// Whether `frag` on `dserver` is protected from reclamation — either a valid placed
    /// reference, *any* fragment of a malformed (fully-referenced) chunk, or **anything at
    /// all** while the set is incomplete.
    ///
    /// That last clause is the containment rule for an object whose map cannot be resolved
    /// (proposal 0016 decision 7(e); the brief's failure-containment table). Unlike a
    /// malformed placement — where the chunk id is known and only its placement is not —
    /// an unresolvable map hides *which chunks the object owns*, so no fragment in the
    /// fleet can be shown not to be one of them. An incomplete reference set may not
    /// authorize any reclamation, and this is where that is enforced rather than left to
    /// each caller to remember: every deletion-capable pass already gates on this one
    /// predicate (`gc.rs:162`, `restore.rs:239`), so the containment holds for all of them
    /// or for none. The cost is a leak until the object is repaired; the alternative is
    /// deleting a live object's bytes, which is the recorded #508-attempt-4 failure.
    pub fn protects(&self, dserver: DServerId, frag: FragmentId) -> bool {
        !self.unresolvable.is_empty()
            || self.placed.contains(&(dserver, frag))
            || self.malformed.contains_key(&frag.chunk)
    }
}

/// Build the [`ReferenceSet`] over every **committed** chunk map, classifying each
/// chunk's committed placement **before** expanding it (ADR-0040 decision 4).
///
/// **One object's unresolvable map does not end the walk.** A segmented generation the
/// store cannot resolve ([`wyrd_core::metadata::ChunkMapError`]) is recorded in
/// [`ReferenceSet::unresolvable`] and attributed on the audit seam, and the walk goes on:
/// the set is then *incomplete*, which `protects` turns into "reclaim nothing" — refuse to
/// certify, attribute the blocker, keep going, the containment shape this repo already uses
/// for a record it cannot trust (`ReconciliationStatus::PendingMalformed`,
/// `crates/custodian/src/desired_state.rs:88-101`,`:196-208`).
///
/// Propagating instead would be safe for GC — it deletes nothing on an `Err` — and is the
/// wrong answer for the callers that only *read* this set: `reconciliation_status` would
/// return `Err` for **every** D server in the store, so one damaged object would blank the
/// drain-status surface fleet-wide. Anything that is **not** the object's own fault (a
/// store error, an undecodable record) still propagates: a walk that cannot read the
/// metadata store has no reference set at all, incomplete or otherwise.
pub(crate) async fn referenced_fragments(meta: &dyn MetadataStore) -> Result<ReferenceSet> {
    let mut placed = HashSet::new();
    let mut malformed = HashMap::new();
    let mut schemes = HashMap::new();
    let mut unresolvable = BTreeSet::new();
    for (key, value) in meta.scan(b"inode:").await? {
        // The root's own decode is contained by the same rule as the resolve below, and
        // for the same reason: a segmented root whose structure is corrupt fails HERE,
        // before any resolver is reached, and propagating it would end the walk for every
        // healthy object in the store. `metadata::decode` is what makes the two
        // indistinguishable at this seam — it classifies a structural chunk-map fault as
        // `ChunkMapError` whichever record class carried it (`crates/core/src/metadata.rs:1673`),
        // so this arm cannot be the one a new failure spelling slips past.
        let record: InodeRecord = match crate::resolve::classify_root(&key, &value)? {
            crate::resolve::Root::Decoded(record) => record,
            // **The state check comes first, even here** — and it is taken in
            // `classify_root` rather than restated, because this set covers *committed*
            // maps only (the skip below), so an uncommitted record's map is not in it
            // whether or not it can be read, and recording one as a blocker would make an
            // object that authorizes nothing stop every reclamation in the fleet
            // ([`ReferenceSet::protects`]) until a human repaired it.
            crate::resolve::Root::UncommittedUnreadable(fault) => {
                fault.attribute_uncommitted(crate::resolve::GC);
                continue;
            }
            crate::resolve::Root::Unresolvable(fault) => {
                unresolvable.insert(fault.attribute(crate::resolve::GC));
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        // Resolve the map through the ONE resolver every consumer shares (proposal 0016
        // decision 7(e)): a flat map is borrowed unchanged, a SEGMENTED one is read from
        // its bounded `seg:<nonce>:<epoch>:` range. This is the first and most
        // safety-critical consumer — a segmented object whose chunks never enter this set
        // is an object GC believes is unreferenced.
        let live = match crate::resolve::contain(
            &key,
            crate::resolve::chunks_of(meta, &key, &record).await,
        )? {
            crate::resolve::Contained::Resolved(live) => live,
            crate::resolve::Contained::Unresolvable(fault) => {
                unresolvable.insert(fault.attribute(crate::resolve::GC));
                continue;
            }
        };
        let Some(live) = live else {
            continue;
        };
        for chunk in live.chunks.iter() {
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

/// The chunk ids whose pending-ledger lease has expired as of `now_millis`.
async fn expired_pending_chunks(
    meta: &dyn MetadataStore,
    now_millis: u64,
) -> Result<HashSet<ChunkId>> {
    let mut set = HashSet::new();
    for (key, value) in meta.scan(b"pending:").await? {
        let entry: PendingEntry = metadata::decode(&value)?;
        if entry.lease_expiry_millis <= now_millis {
            if let Some(chunk) = parse_pending_chunk(&key) {
                set.insert(chunk);
            }
        }
    }
    Ok(set)
}

/// The orphan ledger: each stranded `(dserver, fragment)` and the instant it became
/// orphaned.
pub(crate) async fn orphan_leases(
    meta: &dyn MetadataStore,
) -> Result<HashMap<(DServerId, FragmentId), u64>> {
    let mut map = HashMap::new();
    for (key, value) in meta.scan(ORPHAN_PREFIX).await? {
        if let Some(slot) = parse_orphan_key(&key) {
            if let Some(at) = std::str::from_utf8(&value)
                .ok()
                .and_then(|s| s.parse().ok())
            {
                map.insert(slot, at);
            }
        }
    }
    Ok(map)
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
