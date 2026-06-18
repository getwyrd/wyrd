---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - deployment
---
# 7. Deployment view

> Living document.

## 7.1 The three deployment profiles

The same codebase, composed differently. The composition lives in the `server` crate; switching profiles swaps backends behind the `ChunkStore`, `MetadataStore`, `NamespaceStore`, and `Coordination` traits.

| Profile | Coordination | Metadata (L4) | Global plane (L2/L3) | Chunk store | Durability | Status |
|---------|--------------|---------------|----------------------|-------------|------------|--------|
| Single binary (dev) | in-memory | redb (embedded) | none — single-zone | filesystem | none / replication(1) | Dev & eval only (ADR-0014) |
| Small multi-node | 3-node etcd | TiKV (small) / redb | none — single-zone | local-disk D servers | replication(n) or rs(k,m) | Production |
| Provider fleet | dedicated etcd per zone | TiKV | TiDB + L3 replication (multi-region) | local-disk D servers | rs(k,m) | Production |

The single-binary profile collapses all components into one process: gateway, embedded metadata, one logical D server, custodians, in-memory coordination. It exists for development and evaluation and carries **no production durability promise** — a single chassis cannot deliver independent failure domains.

**There is no separate L2/L3 below the multi-zone tier.** The single-binary and small-multi-node profiles are *single-zone*: there is one home zone, so file→home-zone is trivial and the global namespace folds into the zonal store — `NamespaceStore` is backed by the same redb instance as `MetadataStore`, and cross-zone replication (L3) does not exist. A distinct, geo-distributed **L2** (TiDB behind `NamespaceStore`, ADR-0020) and the **L3** replication layer appear only at the **provider-fleet** profile — the first genuinely multi-region tier. This is why the build order (section 9) puts L2/L3 last.

## 7.2 Deployment substrate (pluggable)

Kubernetes is *available but never required* (ADR-0010):

- **Stateless components** (gateways, replication workers, leader-elected custodians) are Kubernetes-friendly: a Deployment behind a Service, or systemd units, or compose.
- **Stateful components** (D servers; self-hosted metadata/etcd) want node affinity to their disks. On Kubernetes: StatefulSets with local persistent volumes and strict anti-rescheduling, managed by an operator that understands the custodians own placement. Off Kubernetes: systemd on the storage hosts.

What is shipped, in priority order: a single static binary (primary), an OCI image (same binary), a docker-compose (small multi-node), and later a Helm chart and operator (`deploy/`). **No code knows it is on Kubernetes** — peers are discovered through L5, never through orchestrator APIs. The system must come up identically whether launched by systemd, compose, or a Kubernetes pod.

## 7.3 Failure domains

Durability math depends on fragments landing in independent failure domains (rack, power, switch). The placement service (L2) and custodians (L4) enforce domain spread for the configured EC scheme. A leading-indicator capacity metric is per-failure-domain utilization: running out of room *in a specific domain* blocks new EC writes before total capacity is exhausted.
