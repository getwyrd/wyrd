---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - deployment
  - scope
---
# 0014. Single-binary profile is dev/eval only

## Context

The single-binary / NAS-class profile (in-memory coordination, embedded redb, filesystem chunk store) is essential for development velocity and evaluation. The question is whether it is also a *supported production tier* — e.g. a homelab user's family Drive on one or two NAS boxes. A single chassis cannot deliver independent failure domains, so single-box durability promises would be dishonest.

## Decision

The single-binary / NAS-class profile is **for development and evaluation only**, explicitly not a supported production tier. The embedded backends carry no production durability promise, and the docs say so plainly. Production durability begins at the real multi-node backends with proper failure-domain separation.

## Consequences

- Less test surface and fewer guarantees to maintain for the small profile.
- The "runs on a Synology" message stays honest — it does not imply "your only copy on one box is safe."
- If a supported small-production tier is ever wanted, it would require a stated durability floor (e.g. replication across independent machines) and the corresponding test surface; that is a future decision, not this one.
