---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - runtime
  - commit-protocol
---
# 6. Runtime view

> Living document. The scenarios that most define the system's behavior. See `diagrams/` for sequence diagrams.

## 6.1 Write path (the commit point)

The headline scenario. Detailed step-by-step in section 5 (L4 write protocol) and normatively in the commit-protocol spec. The essential property: the file does not exist until step 3's single atomic metadata mutation, and fully exists after it. Bulk data (steps 2) flows directly from client to D servers, crossing no shared component — this is the basis of the throughput-scaling claim.

## 6.2 Read path

1. Client fetches file metadata (chunk list, fragment locations, EC scheme) from the metadata store (cached after first read).
2. Client reads fragments directly from D servers. Because it holds the EC logic, it needs only *k* of *n* fragments and reconstructs from whichever *k* arrive first — turning erasure coding into a tail-latency *advantage*: a slow or dead disk does not slow the read.
3. Client verifies checksums on every fragment, catching bit rot at read time; re-reads elsewhere on failure.

## 6.3 Repair (self-healing, intra-zone)

Driven by custodians (L4):

1. **Scrub** — continuously read fragments, verify checksums against metadata, detect bit rot *before* the data is needed.
2. **Reconstruct** — on D-server loss or failed checksum, read any *k* surviving fragments, recompute the missing ones, place them on healthy servers in correct failure domains, and update the chunk's location via a single atomic metadata mutation — the same commit-point pattern as a write.
3. **Rebalance** — proactively move data off draining/hot servers, preserving failure-domain invariants.

Every recovery action is itself commit-point-atomic. A crashed repair job leaves garbage (collected later), never corruption.

The metric that matters: **time-to-repair vs. failure rate**. Durability ("the nines") is essentially the probability that more than *m* fragments fail within one repair window. Fast, parallel repair matters more than wide encoding. This is why repair-queue depth and time-to-repair are first-class telemetry (ADR-0011), not vanity metrics.

## 6.4 Cross-zone replication and zone-loss recovery

- **Replication** (L3): after a home-zone commit, replication workers copy chunks to other zones per policy. The remote replica becomes readable only when its record commits in L2 — never mid-copy.
- **Zone loss** (L3 global custodians): detect the dead zone, find every file now below its policy replica count, re-replicate from survivors. Effective geographic durability is bounded by cross-region bandwidth and rebuild parallelism — the same repair-time principle at planetary scale.

## 6.5 Disaster recovery ordering

After a real disaster, restore in dependency order:

1. **L5 coordination** first — so components can find each other and elect leaders.
2. **L2 / L4 metadata** next — so the map to the bytes exists. Restored from out-of-band backups that do **not** depend on the system being restored.
3. **L3 replica verification and re-replication** — so the bytes are protected again.

Restoring bytes before the map is useless; restoring the map before coordination cannot even start. This ordering is a runbook section and must be written and drilled before it is needed.

## 6.6 Consistency at runtime

See section 8 (the consistency contract). In brief: the namespace is globally strongly consistent; a file's writes are linearizable at its home zone; per-session read-your-writes and monotonic reads are provided (v1: by routing consistency-sensitive reads to the home zone); stale-tolerant reads may be served from a lagging replica; cross-session visibility of a remote write is eventually consistent, bounded by replication lag.
