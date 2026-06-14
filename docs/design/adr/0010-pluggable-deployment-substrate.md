# 0010. Pluggable deployment substrate; Kubernetes optional

Date: design phase
Status: Accepted

## Context

The single-binary/NAS profile forbids requiring Kubernetes. Separately,
Kubernetes' scheduler is built for fungible, relocatable, stateless workloads,
while D servers are welded to specific physical disks holding specific fragments
— their identity *is* their data. Placement and durability authority must live in
the custodians, not in an orchestrator's scheduler.

## Decision

The deployment substrate is pluggable; Kubernetes is **available but never
required**. Stateless components (gateways, replication workers, leader-elected
custodians) are orchestrator-friendly. Stateful components (D servers,
self-hosted metadata/etcd) want node affinity to disks: StatefulSets with local
persistent volumes and an operator that understands the custodians own placement,
or systemd on the storage hosts. **No code couples to orchestrator APIs**; peers
are discovered through L5. The system must come up identically whether launched
by systemd, docker-compose, or a Kubernetes pod.

Shipped, in priority order: single static binary (primary), OCI image,
docker-compose, then later a Helm chart and operator (`deploy/`, outside the Rust
workspace).

## Consequences

- The Synology profile and the provider-fleet profile stay one codebase.
- The custodians retain undivided authority over placement.
- `deploy/` being outside the workspace makes it structurally hard for
  orchestrator coupling to sneak into a component.
