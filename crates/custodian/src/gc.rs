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

use std::collections::{BTreeMap, HashMap, HashSet};

// The orphan-ledger key protocol lives in `core::metadata` (beside `pending_key`) so the
// delete path that WRITES a grace record and this GC loop that READS it share one
// definition and can never key-format-drift (issue #364). Re-exported `pub(crate)` here so
// the other orphaning loops (`reconstruction.rs`, `rebalance.rs`) keep calling
// `crate::gc::orphan_key` unchanged.
pub(crate) use wyrd_core::metadata::orphan_key;
use wyrd_core::metadata::{
    self, parse_orphan_key, ChunkMapError, EcScheme, InodeRecord, InodeState, MalformedPlacement,
    PendingEntry, ORPHAN_PREFIX,
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
/// entry.
///
/// Returns [`Reconciled::Blocked`] if the reference set is **incomplete** — at least one
/// committed object's chunk map could not be read ([`ReferenceSet::unresolvable`]), so
/// [`ReferenceSet::protects`] withheld every fragment in the fleet and this pass may not
/// report the store converged — [`Reconciled::Changed`] if any fragment bytes were
/// reclaimed, and [`Reconciled::Satisfied`] otherwise. Scrub answers the identical
/// condition the identical way ([`crate::scrub::reconcile`]): one incomplete set, one
/// rule, read twice.
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
    // Input (2): orphaned fragments and the instant each was stranded.
    let orphaned_at = orphan_leases(ctx.meta).await?;

    let mut changed = false;
    let mut cleanup = WriteBatch::new();
    let mut swept_pending: HashSet<ChunkId> = HashSet::new();

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
