//! The **declarative management hook** (proposal 0005 §"Declarative management hook",
//! `0005:346-356`; ADR-0011 rule 2: declarative, self-reconciling management;
//! architecture §8.4).
//!
//! Management is **declarative**: the operator writes **desired state** (drain /
//! decommission a D server) and the custodian's rebalance loop reconciles reality
//! toward it — the Kubernetes control-loop pattern on the substrate already present.
//! M3 builds only the **hook** — the desired-state read/write + the
//! reconciliation-status surface — single-zone: desired state **folds into the local
//! metadata** (`0005:353-354`). The full API-first management surface and its CLI are
//! ADR-0013, deferred (`0005:355-356`).
//!
//! The load-bearing contract is that **"policy changed"** (desired state recorded) and
//! **"policy satisfied"** (reality matches — the drained server holds no byte that can
//! still become referenced: none a committed chunk map places there, and none a
//! multipart upload has staged there) are **distinct, observable moments**
//! (`0005:351-352`, proposal 0016 decision 2 `0016:826-827`). The concrete
//! desired-state encoding (a `desired:dserver:<id>` ledger entry) and the
//! reconciliation-status shape ([`ReconciliationStatus`]) are ILLUSTRATIVE; the two
//! observable moments are BINDING.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this stays over the `traits` seam —
//! the desired state is a plain metadata-ledger entry, mirroring the `pending:` /
//! `orphan:` / `repair:` ledger pattern, so the hook gains no backend of its own.

use std::collections::BTreeMap;

use wyrd_traits::{ChunkId, DServerId, MetadataStore, Result, WriteBatch};

use crate::gc::{object_name, referenced_fragments, staged_fragments};

/// Key prefix for the **desired-state** ledger — a D server the operator has marked
/// draining / decommissioning. Mirrors the `pending:` / `orphan:` / `repair:` ledger
/// pattern (architecture §5); the value records which lifecycle was requested.
const DESIRED_PREFIX: &[u8] = b"desired:dserver:";

/// Key for one D server's desired-state record: `desired:dserver:<id>`.
pub fn desired_key(dserver: DServerId) -> Vec<u8> {
    format!("desired:dserver:{dserver}").into_bytes()
}

fn parse_desired_key(key: &[u8]) -> Option<DServerId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("desired:dserver:")?
        .parse()
        .ok()
}

/// The operator-requested lifecycle of a D server (`0005:349`). Both are evacuation
/// targets for the rebalance loop — fragments are moved **off** the server; the
/// distinction (drain = temporary, decommission = permanent removal) is recorded for
/// the audit trail and a later policy, not the M3 evacuation mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DServerLifecycle {
    /// Temporarily evacuate the server (e.g. maintenance); it may return to service.
    Draining,
    /// Permanently evacuate the server ahead of removal from the fleet.
    Decommissioning,
}

impl DServerLifecycle {
    /// The on-ledger label for this lifecycle.
    pub fn label(self) -> &'static str {
        match self {
            DServerLifecycle::Draining => "draining",
            DServerLifecycle::Decommissioning => "decommissioning",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "draining" => Some(DServerLifecycle::Draining),
            "decommissioning" => Some(DServerLifecycle::Decommissioning),
            _ => None,
        }
    }
}

/// The reconciliation status of a D server's drain/decommission desired state — the
/// observable surface that makes **"policy changed"** and **"policy satisfied"**
/// distinct moments (`0005:351-352`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationStatus {
    /// No drain/decommission desired state is recorded for this server.
    NotRequested,
    /// Desired state is recorded (**policy changed**) but reality has not yet
    /// converged — the server still holds at least one fragment that can still become
    /// referenced: one a **committed** chunk map places on it, or one a multipart
    /// upload's own records stage there (`crate::gc::StagedSet`, proposal 0016 decision
    /// 2 `0016:826-827`). Either way the byte is live, so the drain is not finished.
    Pending,
    /// Desired state is recorded and the server holds no *valid* referenced fragment,
    /// yet the drain **cannot** be certified satisfied: one or more records carry a
    /// placement that **cannot be trusted** — a committed chunk map's **malformed**
    /// placement (ADR-0040 decision 4) that rebalance refuses to evacuate (skip +
    /// NEEDS-HUMAN), or a staged multipart record held whole by the same rule one level
    /// down (`crate::gc::StagedSet::held`) — so a corrupt record, which cannot be trusted
    /// to *not* name this server, might still reference it. The drain stays blocked
    /// **cluster-wide** (fail safe: the block is deliberately *not* scoped to servers the
    /// malformed vector happens to name, since trusting its contents is exactly what
    /// ADR-0040 forbids), and the blocking chunk ids are surfaced **in the answer itself**
    /// so an operator can attribute the stall to specific corruption and resolve it,
    /// rather than see an unexplained `Pending`. Chunk ids are sorted (stable order).
    PendingMalformed {
        /// The chunk ids whose placement is blocking every drain — a committed map's
        /// malformed placement, or a staged record held whole. Sorted and deduplicated,
        /// so one chunk both classes distrust is named once.
        chunks: Vec<ChunkId>,
    },
    /// Desired state is recorded and the server holds no *valid* referenced fragment, yet the
    /// drain **cannot** be certified satisfied: one or more records could not be read **at
    /// all** — a committed object's chunk map (an incomplete segmented generation, a record
    /// that will not decode — `crate::gc::ReferenceSet::unresolvable`) or a multipart upload's
    /// staged record (`crate::gc::StagedSet::unresolvable`) — so the set this answer is
    /// computed from is **incomplete** and cannot be shown *not* to name this server.
    ///
    /// [`Self::PendingMalformed`] one level up — a malformed placement hides *where* one
    /// chunk's fragments are, an unreadable map hides *which chunks the object owns* — and the
    /// same containment: refuse to certify, **attribute** the blocking objects in the answer,
    /// keep answering. Distinct from [`Self::Pending`] because the two are not the same
    /// operator instruction: `Pending` says an evacuation is running and will finish, this says
    /// nothing will finish until a named record is repaired, since rebalance cannot evacuate
    /// fragments of a map it cannot read. An unattributed wait is a stall nothing exits
    /// (`docs/principles.md` §5 C-1); `Err` instead would take the whole fleet's drain-status
    /// surface down for one damaged object, and `Satisfied` would certify a decommission over
    /// bytes an object may still own.
    PendingUnresolvable {
        /// The blocking records, by key as the store spells it, escaped so two damaged
        /// records never arrive under one name (`crate::gc::object_name`). Committed
        /// `inode:` objects first, then staged multipart records, each group in the
        /// store's own key order — so a staged blocker appearing never reorders the
        /// committed names an operator already read.
        objects: Vec<String>,
    },
    /// Desired state is recorded **and** reality matches (**policy satisfied**) — the
    /// server holds no byte that can still become referenced: no committed chunk map
    /// places a fragment there, and no multipart upload has staged one there; its
    /// leftover bytes are GC-eligible orphans.
    Satisfied,
}

/// **Operator write** — record that `dserver` should be drained / decommissioned. This
/// is the **"policy changed"** moment (`0005:351`). Idempotent at the metadata layer (a
/// plain put), single-zone (folds into the local metadata, `0005:353-354`).
pub async fn set_lifecycle(
    meta: &dyn MetadataStore,
    dserver: DServerId,
    lifecycle: DServerLifecycle,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(desired_key(dserver), lifecycle.label().as_bytes().to_vec()))
        .await?;
    Ok(())
}

/// Clear `dserver`'s desired state — it returns to active service (a drain cancelled).
pub async fn clear_lifecycle(meta: &dyn MetadataStore, dserver: DServerId) -> Result<()> {
    meta.commit(WriteBatch::new().delete(desired_key(dserver)))
        .await?;
    Ok(())
}

/// Every D server the operator has marked draining / decommissioning, with its
/// requested lifecycle — the desired state the rebalance loop reconciles against.
pub async fn draining_servers(
    meta: &dyn MetadataStore,
) -> Result<BTreeMap<DServerId, DServerLifecycle>> {
    let mut map = BTreeMap::new();
    for (key, value) in meta.scan(DESIRED_PREFIX).await? {
        if let Some(id) = parse_desired_key(&key) {
            if let Some(lifecycle) = std::str::from_utf8(&value)
                .ok()
                .and_then(DServerLifecycle::parse)
            {
                map.insert(id, lifecycle);
            }
        }
    }
    Ok(map)
}

/// The [`ReconciliationStatus`] of `dserver`'s desired state — the observable
/// "changed" vs "satisfied" surface (`0005:351-352`). A drain is **satisfied** only once no
/// byte that can still become referenced lives on `dserver`: neither one a **committed**
/// chunk map places there (the reference set GC / scrub gate on) nor one a multipart
/// upload's own records stage there — a committed part's chunks (`part:`) or an in-flight
/// owned staging entry's planned placement (`sidx:`), the class
/// `crate::gc::staged_fragments` builds (proposal 0016 decision 2, `0016:765-893`).
///
/// **Both classes, because a certification over one of them is a certification over an
/// incomplete picture** (`docs/principles.md` §5 C-1). A staged fragment is durable and
/// live long before it is published — the part commit that names it, and the publication
/// that turns it into a committed reference, are still to come — so a `Satisfied` computed
/// from committed placements alone tells an operator a box is safe to wipe while a live
/// upload's bytes are on it (`0016:826-827`). The sharper case is an in-flight part whose
/// `part:` record does not exist yet (`0016:827`): nothing committed names it at all. The
/// two classes are counted **separately**, not merged, because they are separately damaged
/// and separately attributed (`0016:767-782`); an implementation that counted only one of
/// them would answer this query right for half the uploads in the store (`0016:883`).
///
/// The staged class is read **first**, before the committed one, for the reason every other
/// consumer reads it first (`0016:793-800`): a publication moves a chunk's protection from
/// its `part:` record onto a committed inode, so reading the inodes first could miss the
/// flip and then miss the part record it deleted, seeing the chunk in neither. It costs this
/// query the staged reading — the session listing plus two bounded ranges per session
/// (`0016:890`), the same bounded reading GC already does every pass — which is what being
/// right about a live upload's bytes costs.
///
/// Every non-satisfied answer says **why**, because "not yet" and "not ever, until you repair
/// X" are different operator instructions: still-held ([`ReconciliationStatus::Pending`]),
/// blocked by a placement that cannot be trusted ([`ReconciliationStatus::PendingMalformed`],
/// with the chunk ids), or blocked by a record that could not be read at all
/// ([`ReconciliationStatus::PendingUnresolvable`], with the record names). One damaged record
/// never turns this query into an `Err`: this surface is read per D server, and blanking the
/// fleet's drain status over one record is the outage the containment rule exists to prevent.
/// A **store fault** under either reading is not that case and still propagates: a query that
/// could not read the store has no answer to contain.
pub async fn reconciliation_status(
    meta: &dyn MetadataStore,
    dserver: DServerId,
) -> Result<ReconciliationStatus> {
    if meta.get(&desired_key(dserver)).await?.is_none() {
        return Ok(ReconciliationStatus::NotRequested);
    }
    // The staged class FIRST, then the committed one — the order above, `0016:793-800`. Its
    // reads are bounded ranges, never a namespace scan (`0016:890`), and the network bound on
    // this await is the `MetadataStore` IMPLEMENTATION's, not this caller's — the rule every
    // other custodian read follows (#508/#636), including the committed read on the next line.
    let staged = staged_fragments(meta).await?;
    let referenced = referenced_fragments(meta).await?;
    // A genuine, trustworthy hold: a *valid* placement — committed, or staged by a record
    // this pass could read and trust — that resolves a fragment onto `dserver`. While one
    // exists the drain is honestly `Pending`, whichever class named it: the rebalance loop
    // evacuates the committed ones and the upload itself retires the staged ones, and in
    // both cases the answer an operator needs is "a live byte is still on this box".
    let genuinely_holds = referenced
        .placed
        .iter()
        .chain(staged.placed.iter())
        .any(|(server, _)| *server == dserver);
    if genuinely_holds {
        return Ok(ReconciliationStatus::Pending);
    }
    // ...and a set that could not be fully BUILT cannot certify either. A committed object
    // whose chunk map this build could not read (`gc::ReferenceSet::unresolvable`) contributes
    // no fragments at all, so `placed` above is silent about it and nothing here can show that
    // the bytes on `dserver` are not its. Answering `Satisfied` would be the reclamation
    // decision in report form — "you may decommission this box" — over exactly the incomplete
    // set GC refuses to reclaim a byte on (`gc::ReferenceSet::protects`), and that is the
    // permanent, data-losing outcome C-1 forbids (`docs/principles.md` §5).
    //
    // ATTRIBUTED, as `PendingMalformed` names chunk ids: an operator watching a drain stall
    // needs the record to REPAIR, and a bare `Pending` — the answer a server that genuinely
    // still holds referenced fragments gets — tells them to keep waiting for an evacuation that
    // can never finish, because rebalance cannot move fragments of a map it cannot read. A wait
    // with nothing to act on is a state nothing exits, which is the same permanence C-1
    // forbids, reached through the report instead of through a deletion. Named on the audit
    // seam as well as in the answer, so a collector that only watches the durability plane sees
    // the blocker too — the shape `gc::emit_unresolvable` / `scrub::emit_unscrubbable` already
    // use for the same record.
    //
    // Blocked cluster-wide (fail safe), and deliberately NOT scoped to servers the unreadable
    // object might name: which chunks it owns is exactly what could not be read.
    //
    // Ranked BELOW the genuine reference above, exactly as `PendingMalformed` is: while valid
    // committed placements still name this server the drain is honestly not converged and the
    // rebalance loop is moving them, so "wait" is both true and actionable. This answer takes
    // over the moment that wait would otherwise become unbounded — when nothing valid names the
    // server any more and the only thing left between it and `Satisfied` is a record a human
    // has to repair.
    //
    // A staged record this build could not read (`gc::StagedSet::unresolvable`) is the same
    // hole in the other class, and blocks the same way: it hides which chunks its upload
    // owns, so no fragment on `dserver` can be shown not to be one of them — the containment
    // `StagedSet::protects` already applies on the reclamation side, read here on the
    // certification side.
    if !referenced.unresolvable.is_empty() || !staged.unresolvable.is_empty() {
        let mut objects =
            Vec::with_capacity(referenced.unresolvable.len() + staged.unresolvable.len());
        for (key, fault) in &referenced.unresolvable {
            let object = object_name(key);
            emit_unresolvable(dserver, &object, fault);
            objects.push(object);
        }
        for (key, fault) in &staged.unresolvable {
            let record = object_name(key);
            emit_unresolvable_staged(dserver, &record, fault);
            objects.push(record);
        }
        return Ok(ReconciliationStatus::PendingUnresolvable { objects });
    }
    // No valid placement of either class names `dserver`. But a malformed committed placement
    // (ADR-0040 decision 4) cannot be trusted to *not* name it, and rebalance refuses to
    // evacuate it (skip + NEEDS-HUMAN), so the drain genuinely cannot complete while one
    // exists. A staged record held whole (`gc::StagedSet::held`) is the same rule for the other
    // class, by the same reasoning one level down: its placement is of the wrong length, or its
    // value will not decode under a key that still names its chunk, so where that chunk's
    // fragments actually sit is exactly what is unknown. Stay blocked **cluster-wide** (fail
    // safe — deliberately not scoped to servers the corrupt vector names, since trusting its
    // contents is what ADR-0040 forbids), but ATTRIBUTE the stall: surface the blocking chunk
    // ids in the answer so `Pending` is never unexplained. Only once neither class distrusts a
    // record is the drain `Satisfied`.
    if referenced.malformed.is_empty() && staged.held.is_empty() {
        return Ok(ReconciliationStatus::Satisfied);
    }
    let mut chunks: Vec<ChunkId> = referenced
        .malformed
        .keys()
        .chain(staged.held.keys())
        .copied()
        .collect();
    chunks.sort_unstable();
    // One chunk both classes distrust is one blocker to repair, not two lines in the answer.
    chunks.dedup();
    Ok(ReconciliationStatus::PendingMalformed { chunks })
}

/// Emit the committed object that is blocking `dserver`'s drain on the durability-plane seam
/// (ADR-0011 / ADR-0012), naming it exactly as GC and scrub name the same record on theirs
/// (`gc::emit_unresolvable`, `scrub::emit_unscrubbable`): the answer this query returns carries
/// the blocker too, and this is the same attribution for a collector watching the seam rather
/// than polling the status.
///
/// The counter counts **observations**, one per blocking record per status read — a status read
/// is the operator's poll, not a pass, so it is a rate of *asking while blocked*, not a census
/// of damaged records. `unresolvable-chunk-map` is the shared action, so one query selects
/// every unreadable-record signal across all the surfaces that read this set.
fn emit_unresolvable(dserver: DServerId, object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.drain_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.drain.audit",
        action = "unresolvable-chunk-map",
        dserver,
        inode = %object,
        fault = %fault,
        "a committed object's chunk map could not be read, so this drain cannot be shown to hold none of its fragments; the drain stays blocked cluster-wide and will NOT converge until this record is repaired — operator signal",
    );
}

/// Emit the **staged multipart record** that is blocking `dserver`'s drain on the
/// durability-plane seam — [`emit_unresolvable`]'s placement, reasons and counter shape, for the
/// other protection class, and naming the record exactly as GC names the same one on its seam
/// (`gc::emit_unresolvable_staged`).
///
/// A separate action from `unresolvable-chunk-map` because it is a different record for a human
/// to go and find — an upload's `mpu:` / `part:` / `sidx:` record, not a committed object — and a
/// separate counter for the same reason: an operator watching this stall needs to know which
/// namespace to repair. Like its committed twin it counts **observations**, one per blocking
/// record per status read.
fn emit_unresolvable_staged(dserver: DServerId, record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.drain_unresolvable_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.drain.audit",
        action = "unresolvable-staged-record",
        dserver,
        record = %record,
        fault = %fault,
        "a multipart upload's staged record could not be read, so this drain cannot be shown to hold none of its staged fragments; the drain stays blocked cluster-wide and will NOT converge until this record is repaired — operator signal",
    );
}
