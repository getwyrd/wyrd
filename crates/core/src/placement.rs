//! The zone-local **failure-domain-aware placement selector** (proposal 0005,
//! §"Failure-domain-aware placement", `0005:235-245`; invariant `0005:491`).
//!
//! M2 spread a chunk's `n` fragments across *distinct endpoints* and **claimed no
//! durability math** (`chunkstore-grpc/src/fanout.rs:5-10`): `index % n` is
//! domain-blind, so two of a chunk's fragments can land in the same rack / power /
//! switch and a single failure-domain loss can take both. M3 introduces a
//! **zone-local failure-domain model**: each D server carries an **opaque**
//! failure-domain label (rack / power / switch, architecture §7.3) surfaced through
//! its registration, and this selector enforces the **distinctness invariant** —
//! a chunk's `n` fragments occupy `n` **distinct** domains wherever the topology
//! offers ≥ `n` of them, and the selector **refuses** (errors) otherwise. That is
//! the first point at which RS(6,3)'s durability math is *claimable* within a
//! single zone (`0005:243-245`).
//!
//! The abstraction is kept deliberately **thin** (`0005:251`): an opaque domain id
//! per D server, the distinctness invariant, and per-domain utilization for
//! balancing — no placement *policy* (per-tenant scheme, global capacity, cross-zone
//! placement), which is L2 / M6 (`0005:247-253`). The selection *order* below is
//! ILLUSTRATIVE (least-utilized domain first, lowest id within a domain); the
//! BINDING contract is only that the returned ids are `n` and span `n` distinct
//! domains, or the call errors.
//!
//! The selector is **shared by the write fan-out** (`write::WritePlan::place`) and,
//! later, custodian re-placement, so the invariant holds on both write and repair
//! (`0005:241-242`).

use std::collections::BTreeMap;

use wyrd_traits::DServerId;

/// An **opaque** failure-domain label (rack / power / switch — architecture §7.3).
///
/// Deliberately opaque (`0005:251`): the selector **compares** labels for
/// distinctness, it never interprets their structure. A `String` is the M3
/// encoding — a D server reports it from config through its registration; richer
/// hierarchical domains (rack ⊂ row ⊂ hall) are an M6 policy concern, not this
/// thin zone-local model.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FailureDomain(pub String);

impl FailureDomain {
    /// Wrap a label string as an opaque failure domain.
    pub fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl<T: Into<String>> From<T> for FailureDomain {
    fn from(label: T) -> Self {
        Self(label.into())
    }
}

/// One D server's placement-relevant facts: its stable [`DServerId`] and the
/// opaque failure domain it occupies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DServerTopology {
    /// The stable D-server id (the placement record keys on this, never the URL).
    pub id: DServerId,
    /// The opaque failure domain the server occupies.
    pub domain: FailureDomain,
}

/// The zone-local view the selector places against: the known D servers, their
/// failure domains, and per-server utilization for balancing.
///
/// Built from D-server registration/discovery (each registration carries the
/// `{ id, endpoint, failure-domain label }`, `0005:194-196`). Kept thin on purpose
/// (`0005:251`).
#[derive(Debug, Clone, Default)]
pub struct Topology {
    servers: Vec<DServerTopology>,
    /// Per-server utilization (e.g. fragment count / bytes used); drives
    /// least-loaded selection. Absent entries are treated as zero.
    utilization: BTreeMap<DServerId, u64>,
}

impl Topology {
    /// Build a topology from `servers`.
    pub fn new(servers: Vec<DServerTopology>) -> Self {
        Self {
            servers,
            utilization: BTreeMap::new(),
        }
    }

    /// Register one D server's `id` and failure-domain `domain`.
    pub fn register(&mut self, id: DServerId, domain: impl Into<FailureDomain>) -> &mut Self {
        self.servers.push(DServerTopology {
            id,
            domain: domain.into(),
        });
        self
    }

    /// Record per-server utilization used to break ties toward the least-loaded
    /// server (and least-loaded domain). Optional; absent ⇒ zero.
    pub fn set_utilization(&mut self, id: DServerId, used: u64) -> &mut Self {
        self.utilization.insert(id, used);
        self
    }

    /// The number of **distinct** failure domains the topology spans — the ceiling
    /// on how wide a distinct-domain placement can be.
    pub fn distinct_domains(&self) -> usize {
        let mut domains: Vec<&FailureDomain> = self.servers.iter().map(|s| &s.domain).collect();
        domains.sort();
        domains.dedup();
        domains.len()
    }

    fn util(&self, id: DServerId) -> u64 {
        self.utilization.get(&id).copied().unwrap_or(0)
    }
}

/// Why a distinct-domain placement could not be produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorError {
    /// The topology offers fewer than `need` distinct failure domains, so the
    /// distinctness invariant cannot be met — the selector **refuses** rather than
    /// silently colliding domains (durability is gate-zero, `0005:303`).
    InsufficientDomains {
        /// Distinct domains actually available.
        have: usize,
        /// Distinct domains required (one per fragment, `n`).
        need: u16,
    },
}

impl std::fmt::Display for SelectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectorError::InsufficientDomains { have, need } => write!(
                f,
                "failure-domain placement refused: topology offers {have} distinct \
                 domain(s) but {need} fragments need {need} distinct domains"
            ),
        }
    }
}

impl std::error::Error for SelectorError {}

/// Select the stable D-server ids for a chunk's `0..n` fragments so they occupy
/// **`n` distinct failure domains** (the BINDING invariant, `0005:491`).
///
/// Returns the placement vector in fragment-index order (`result[i]` holds fragment
/// `i`), or [`SelectorError::InsufficientDomains`] when the topology offers fewer
/// than `n` distinct domains — the selector **refuses** rather than place two
/// fragments in one domain.
///
/// The selection *algorithm* is ILLUSTRATIVE (`0005:235`, the selector's internal
/// algorithm): it walks domains least-utilized-first and picks the least-utilized
/// server within each, so repeated placements spread load. Only the
/// distinct-domain guarantee is contractual.
pub fn select_distinct_domains(
    topo: &Topology,
    n: u16,
) -> std::result::Result<Vec<DServerId>, SelectorError> {
    let need = n as usize;

    // Group servers by their opaque domain.
    let mut by_domain: BTreeMap<&FailureDomain, Vec<&DServerTopology>> = BTreeMap::new();
    for server in &topo.servers {
        by_domain.entry(&server.domain).or_default().push(server);
    }

    if by_domain.len() < need {
        return Err(SelectorError::InsufficientDomains {
            have: by_domain.len(),
            need: n,
        });
    }

    // Order domains by their least-loaded member (least-utilized domain first),
    // then by the domain label for a deterministic tie-break.
    let mut domains: Vec<(&FailureDomain, Vec<&DServerTopology>)> = by_domain.into_iter().collect();
    domains.sort_by(|(la, a), (lb, b)| {
        let min_a = a.iter().map(|s| topo.util(s.id)).min().unwrap_or(0);
        let min_b = b.iter().map(|s| topo.util(s.id)).min().unwrap_or(0);
        min_a.cmp(&min_b).then_with(|| la.cmp(lb))
    });

    // Take one server — the least-utilized, lowest-id — from each of the first `n`
    // distinct domains. Distinctness is guaranteed by construction: one pick per
    // domain, `need` domains.
    let placement = domains
        .into_iter()
        .take(need)
        .map(|(_, mut members)| {
            members.sort_by_key(|s| (topo.util(s.id), s.id));
            members[0].id
        })
        .collect();
    Ok(placement)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topo() -> Topology {
        // Two servers share domain "A"; the rest are singletons B..K. Identity
        // placement (ids 0..n) would collide in A and span too few domains.
        let mut t = Topology::default();
        t.register(0, "A").register(1, "A");
        for (i, d) in (2u64..).zip(["B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]) {
            t.register(i, d);
        }
        t
    }

    #[test]
    fn places_across_n_distinct_domains() {
        let t = topo();
        let placement = select_distinct_domains(&t, 9).unwrap();
        assert_eq!(placement.len(), 9);
        // BINDING: the chosen servers occupy 9 distinct domains.
        let domains: std::collections::HashSet<_> = placement
            .iter()
            .map(|id| {
                t.servers
                    .iter()
                    .find(|s| s.id == *id)
                    .map(|s| s.domain.clone())
                    .unwrap()
            })
            .collect();
        assert_eq!(domains.len(), 9, "n fragments on n distinct domains");
    }

    #[test]
    fn refuses_when_domains_below_n() {
        // Only three distinct domains; a 9-wide placement must be refused.
        let mut t = Topology::default();
        t.register(0, "A").register(1, "B").register(2, "C");
        let err = select_distinct_domains(&t, 9).unwrap_err();
        assert_eq!(err, SelectorError::InsufficientDomains { have: 3, need: 9 });
    }
}
