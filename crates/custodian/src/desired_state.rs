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

use crate::gc::referenced_fragments;

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
    /// converged — the server still holds at least one **referenced** fragment.
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
