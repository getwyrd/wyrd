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
//! **"policy satisfied"** (reality matches — the drained server holds no referenced
//! fragment) are **distinct, observable moments** (`0005:351-352`). The concrete
//! desired-state encoding (a `desired:dserver:<id>` ledger entry) and the
//! reconciliation-status shape ([`ReconciliationStatus`]) are ILLUSTRATIVE; the two
//! observable moments are BINDING.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this stays over the `traits` seam —
//! the desired state is a plain metadata-ledger entry, mirroring the `pending:` /
//! `orphan:` / `repair:` ledger pattern, so the hook gains no backend of its own.

use std::collections::BTreeMap;

use wyrd_traits::{ChunkId, DServerId, MetadataStore, Result, WriteBatch};

use crate::gc::{object_name, referenced_fragments};

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
    /// converged — the server still holds at least one **referenced** fragment, so the
    /// evacuation the rebalance loop is running has not finished.
    Pending,
    /// Desired state is recorded and the server holds no *valid* referenced fragment,
    /// yet the drain **cannot** be certified satisfied: one or more committed chunk maps
    /// carry a **malformed** placement (ADR-0040 decision 4) that rebalance refuses to
    /// evacuate (skip + NEEDS-HUMAN), so a corrupt record — which cannot be trusted to
    /// *not* name this server — might still reference it. The drain stays blocked
    /// **cluster-wide** (fail safe: the block is deliberately *not* scoped to servers the
    /// malformed vector happens to name, since trusting its contents is exactly what
    /// ADR-0040 forbids), and the blocking chunk ids are surfaced **in the answer itself**
    /// so an operator can attribute the stall to specific corruption and resolve it,
    /// rather than see an unexplained `Pending`. Chunk ids are sorted (stable order).
    PendingMalformed {
        /// The committed chunk ids whose malformed placement is blocking every drain.
        chunks: Vec<ChunkId>,
    },
    /// Desired state is recorded and the server holds no *valid* referenced fragment, yet the
    /// drain **cannot** be certified satisfied: one or more committed objects have a chunk map
    /// that could not be read **at all** (an incomplete segmented generation, a record that
    /// will not decode — `crate::gc::ReferenceSet::unresolvable`), so the reference set this
    /// answer is computed from is **incomplete** and cannot be shown *not* to name this server.
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
        /// The blocking objects, by `inode:` key as the store spells it, escaped so two
        /// damaged records never arrive under one name (`crate::gc::object_name`). Ordered by
        /// that key.
        objects: Vec<String>,
    },
    /// Desired state is recorded **and** reality matches (**policy satisfied**) — the
    /// server holds no referenced fragment; its leftover bytes are GC-eligible orphans.
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
/// "changed" vs "satisfied" surface (`0005:351-352`). "Satisfied" is computed from the
/// **committed** placement records (the same reference set GC / scrub gate on): a drain
/// is satisfied once no committed chunk map's placement record points at `dserver`.
///
/// Every non-satisfied answer says **why**, because "not yet" and "not ever, until you repair
/// X" are different operator instructions: still-referenced ([`ReconciliationStatus::Pending`]),
/// blocked by a malformed placement ([`ReconciliationStatus::PendingMalformed`], with the chunk
/// ids), or blocked by a committed object that could not be read at all
/// ([`ReconciliationStatus::PendingUnresolvable`], with the record names). One damaged object
/// never turns this query into an `Err`: this surface is read per D server, and blanking the
/// fleet's drain status over one record is the outage the containment rule exists to prevent.
pub async fn reconciliation_status(
    meta: &dyn MetadataStore,
    dserver: DServerId,
) -> Result<ReconciliationStatus> {
    if meta.get(&desired_key(dserver)).await?.is_none() {
        return Ok(ReconciliationStatus::NotRequested);
    }
    let referenced = referenced_fragments(meta).await?;
    // A genuine, trustworthy reference: a *valid* committed placement that resolves a
    // fragment onto `dserver`. While one exists the drain is honestly `Pending`.
    let genuinely_holds = referenced
        .placed
        .iter()
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
    if !referenced.unresolvable.is_empty() {
        let mut objects = Vec::with_capacity(referenced.unresolvable.len());
        for (key, fault) in &referenced.unresolvable {
            let object = object_name(key);
            emit_unresolvable(dserver, &object, fault);
            objects.push(object);
        }
        return Ok(ReconciliationStatus::PendingUnresolvable { objects });
    }
    // No valid reference names `dserver`. But a malformed committed placement (ADR-0040
    // decision 4) cannot be trusted to *not* name it, and rebalance refuses to evacuate
    // it (skip + NEEDS-HUMAN), so the drain genuinely cannot complete while one exists.
    // Stay blocked **cluster-wide** (fail safe — deliberately not scoped to servers the
    // corrupt vector names, since trusting its contents is what ADR-0040 forbids), but
    // ATTRIBUTE the stall: surface the blocking chunk ids in the answer so `Pending` is
    // never unexplained. Only once no malformed placement remains is the drain `Satisfied`.
    if referenced.malformed.is_empty() {
        return Ok(ReconciliationStatus::Satisfied);
    }
    let mut chunks: Vec<ChunkId> = referenced.malformed.keys().copied().collect();
    chunks.sort_unstable();
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
