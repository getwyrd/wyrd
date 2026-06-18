---
created: 18.06.2026 19:20
type: adr
status: Proposed
tags:
  - adr
  - control-plane
  - metadata
  - storage
  - pluggability
---
# 0020. Global namespace store: NamespaceStore trait and TiDB default

## Context

The L2 global control plane (section 5) holds the system's globally-authoritative truth — directory tree, file→home-zone mapping, ACLs, sharing, quotas, the zone registry, the replica catalog. It is the one heavy external dependency the architecture names but never *decides*: ADR-0008 covers the **zonal** `MetadataStore` (L4 inode/dirent/chunk-map, linearizable within one zone), and nothing covers L2. The de-Cockroaching note in section 5 even attributed L2's seam to `MetadataStore` (ADR-0008) — a tell that the real seam was missing.

L2's demanded capability is specific, strong, and different from L4's: namespace operations must be **linearizable globally** (section 8.1 — a create / rename / delete / share is visible in every zone once acknowledged; no "lost just-created" or "resurrected deleted" file). That needs a geo-distributed, externally-consistent transactional store with multi-key transactions and a monotonic global commit version. It is also *tiny* (metadata about metadata — kilobytes per file) and off the data path, so the operational weight of such a store is affordable (section 10).

Leaving L2 as a bare product name violates the replaceability goal (section 1.3, goal 5) and the dependency rule (ADR-0010): every backend sits behind a narrow trait so only `server` knows the concrete. L2 was the lone exception. Which store fills it is governed by the dependency-selection tests (ADR-0003) — license, governance, control-resilience — not by vendor.

## Decision

1. **Define a narrow `NamespaceStore` trait** — the L2 analog of `MetadataStore`. Its contract is a *capability*, not a product: globally-linearizable multi-key transactions over the namespace keyspace (path resolution, atomic inode+dirent create, atomic rename, delete, list), plus a **monotonic global commit version** the consistency fence reads (ADR-0015). The architecture depends only on this trait; `server` wires the concrete (ADR-0010).

2. **Default backend: TiDB** — the SQL surface over the TiKV stack already chosen for L4 (ADR-0008). It is Apache-2.0 and CNCF-graduated (passes ADR-0003); it reuses the L4 storage substrate, so a provider operates *one* engine family rather than two; it delivers geo-distributed external consistency, multi-key transactions, and a global commit timestamp suited to the version fence; and its SQL surface fits the relational namespace (tree, ACLs, sharing, quotas) ergonomically.

3. **Alternatives behind the trait:** **YugabyteDB** (Apache-2.0, PostgreSQL-compatible) as a second fully-open option; **PostgreSQL** for single-region / small / dev (not natively geo-distributed-strong, so not a fleet default). **CockroachDB** fits the trait but is **not** a default — its 2024 move to a source-available (non-OSI) license fails the ADR-0003 license test.

4. **L2 and L4 are distinct deployments, not one store.** They may share the TiKV family but differ in *scope of consistency*: L4 is linearizable **within one zone** (the commit point); L2 is linearizable **globally across regions**. The two traits stay separate because conflating them would either burden L4 with global consensus or under-constrain L2.

5. **Below the multi-zone tier there is no separate L2.** In a single-zone deployment the namespace *is* the zonal store: `NamespaceStore` is backed by the same embedded redb as L4 in the single-binary and small profiles, and becomes a distinct geo-distributed TiDB deployment only at the provider-fleet tier. The global control plane is therefore a composition choice — consistent with the build order placing L2/L3 last (section 9).

## Consequences

- The replaceability principle now covers the architecture's largest external dependency; "swap the namespace store" is a composition change in `server`, not a refactor.
- One storage-engine family (TiKV) spans L2 and L4 in production — less operational surface, shared tooling and skills — while the trait preserves the exit.
- The single-binary / dev profile gains a real L2 story (folded into redb) instead of an unstated gap; what is tested locally still exercises the namespace path.
- The contract names the one capability L2 truly needs — globally-linearizable multi-key transactions plus a global version — keeping a non-SQL backend (FoundationDB-class) open if SQL ergonomics ever stop earning their operational weight.
- A second trait to keep honest under DST (ADR-0009): the global-consistency semantics must be simulatable and pinned by at least two implementations before any embedded-global backend is trusted — the rule already applied to coordination (openraft) and metadata.
- **[OPEN]** the exact `NamespaceStore` method set, and whether placement / zone-registry / replica-catalog are records inside it or dedicated services over it.
- **[OPEN]** whether cross-zone sharing (a dirent pointing at a remote inode) resolves through L2 alone or needs a cross-zone transaction.
