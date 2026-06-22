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

use std::collections::{BTreeMap, BTreeSet};

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

    /// The opaque failure domain `id` occupies, if the topology knows the server.
    /// The custodian uses this on reconstruction to read off the **surviving**
    /// fragments' domains, so the rebuilt fragment(s) can be re-placed in domains
    /// **distinct** from them (`0005:276`, the distinct-domain leg of the repair
    /// pipeline).
    pub fn domain_of(&self, id: DServerId) -> Option<&FailureDomain> {
        self.servers.iter().find(|s| s.id == id).map(|s| &s.domain)
    }

    fn util(&self, id: DServerId) -> u64 {
        self.utilization.get(&id).copied().unwrap_or(0)
    }

    /// The **per-failure-domain utilization** — the capacity-plane signal the
    /// custodian publishes (proposal 0005 §"The durability plane", `0005:341-343`;
    /// architecture §8.3): each opaque failure domain mapped to the **sum** of its
    /// member servers' recorded utilization (a server with no recorded utilization
    /// contributes zero). It is the by-product of the thin domain model the durability
    /// plane emits per domain; the selector keeps no policy of its own (`0005:251`).
    pub fn domain_utilization(&self) -> BTreeMap<FailureDomain, u64> {
        let mut by_domain: BTreeMap<FailureDomain, u64> = BTreeMap::new();
        for server in &self.servers {
            let entry = by_domain.entry(server.domain.clone()).or_insert(0);
            *entry = entry.saturating_add(self.util(server.id));
        }
        by_domain
    }

    /// A view of this topology with the `exclude`d servers removed — the re-placement
    /// pool a **drain / decommission** evacuation selects against, so an evacuated
    /// fragment is never re-placed back onto a draining server (proposal 0005
    /// §"Rebalance", `0005:297-303`: move fragments *off* draining servers). The
    /// retained servers' utilization is carried over so least-loaded selection still
    /// holds on the filtered view.
    pub fn excluding(&self, exclude: &BTreeSet<DServerId>) -> Topology {
        Topology {
            servers: self
                .servers
                .iter()
                .filter(|s| !exclude.contains(&s.id))
                .cloned()
                .collect(),
            utilization: self
                .utilization
                .iter()
                .filter(|(id, _)| !exclude.contains(id))
                .map(|(id, used)| (*id, *used))
                .collect(),
        }
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

/// Select stable D-server ids for `count` **rebuilt** fragments so they occupy
/// `count` failure domains that are distinct from each other **and** from every
/// domain in `occupied` — the custodian re-placement step of reconstruction
/// (`0005:276`, `0005:241-242`: the selector is shared by the write fan-out *and*
/// custodian re-placement).
///
/// `occupied` is the set of domains the chunk's **surviving** fragments already
/// hold; excluding them is what keeps the post-repair chunk on `n` distinct
/// domains (the BINDING durability invariant, `0005:491`). Returns
/// [`SelectorError::InsufficientDomains`] when fewer than `count` *free* distinct
/// domains remain — the selector **refuses** rather than collide a rebuilt
/// fragment into a domain a survivor already occupies (durability is gate-zero,
/// `0005:303`).
///
/// The selection *algorithm* is ILLUSTRATIVE and identical to
/// [`select_distinct_domains`] (least-utilized free domain first, least-utilized
/// lowest-id server within it); only the distinct-and-disjoint guarantee is
/// contractual.
pub fn select_distinct_domains_excluding(
    topo: &Topology,
    count: u16,
    occupied: &[FailureDomain],
) -> std::result::Result<Vec<DServerId>, SelectorError> {
    let need = count as usize;
    let occupied: std::collections::BTreeSet<&FailureDomain> = occupied.iter().collect();

    // Group only the servers whose domain is still free (not held by a survivor).
    let mut by_domain: BTreeMap<&FailureDomain, Vec<&DServerTopology>> = BTreeMap::new();
    for server in &topo.servers {
        if occupied.contains(&server.domain) {
            continue;
        }
        by_domain.entry(&server.domain).or_default().push(server);
    }

    if by_domain.len() < need {
        return Err(SelectorError::InsufficientDomains {
            have: by_domain.len(),
            need: count,
        });
    }

    let mut domains: Vec<(&FailureDomain, Vec<&DServerTopology>)> = by_domain.into_iter().collect();
    domains.sort_by(|(la, a), (lb, b)| {
        let min_a = a.iter().map(|s| topo.util(s.id)).min().unwrap_or(0);
        let min_b = b.iter().map(|s| topo.util(s.id)).min().unwrap_or(0);
        min_a.cmp(&min_b).then_with(|| la.cmp(lb))
    });

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

    #[test]
    fn replacement_avoids_occupied_domains() {
        // Four domains A..D. Survivors hold A and C; a single rebuilt fragment must
        // land in a domain distinct from both — B or D, never A/C.
        let mut t = Topology::default();
        t.register(0, "A")
            .register(1, "B")
            .register(2, "C")
            .register(3, "D");
        let occupied = [FailureDomain::new("A"), FailureDomain::new("C")];
        let picked = select_distinct_domains_excluding(&t, 1, &occupied).unwrap();
        assert_eq!(picked.len(), 1);
        let domain = t.domain_of(picked[0]).unwrap().clone();
        assert!(
            domain != FailureDomain::new("A") && domain != FailureDomain::new("C"),
            "rebuilt fragment must not collide with a surviving fragment's domain"
        );
    }

    #[test]
    fn domain_utilization_sums_per_domain() {
        // Two servers in domain A (10 + 5), one in B (7), one in C (no recorded util).
        let mut t = Topology::default();
        t.register(0, "A").register(1, "A").register(2, "B");
        t.register(3, "C");
        t.set_utilization(0, 10)
            .set_utilization(1, 5)
            .set_utilization(2, 7);
        let util = t.domain_utilization();
        assert_eq!(util.get(&FailureDomain::new("A")), Some(&15), "A sums 10+5");
        assert_eq!(util.get(&FailureDomain::new("B")), Some(&7));
        assert_eq!(
            util.get(&FailureDomain::new("C")),
            Some(&0),
            "a domain with no recorded utilization maps to zero"
        );
    }

    #[test]
    fn excluding_drops_servers_and_their_utilization() {
        let mut t = Topology::default();
        t.register(0, "A").register(1, "B").register(2, "C");
        t.set_utilization(1, 99);
        // Exclude server 1 (the draining server): it must not be selectable.
        let drained: std::collections::BTreeSet<DServerId> = [1].into_iter().collect();
        let filtered = t.excluding(&drained);
        assert_eq!(filtered.distinct_domains(), 2, "B is gone with server 1");
        // A 2-wide distinct placement on the filtered view never lands on server 1.
        let placement = select_distinct_domains(&filtered, 2).unwrap();
        assert!(
            !placement.contains(&1),
            "an evacuation never re-places onto the draining server"
        );
    }

    #[test]
    fn replacement_refuses_when_no_free_domain_remains() {
        // Two domains, both occupied by survivors: no free domain for a rebuild.
        let mut t = Topology::default();
        t.register(0, "A").register(1, "B");
        let occupied = [FailureDomain::new("A"), FailureDomain::new("B")];
        let err = select_distinct_domains_excluding(&t, 1, &occupied).unwrap_err();
        assert_eq!(err, SelectorError::InsufficientDomains { have: 0, need: 1 });
    }
}
