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

The headline scenario; see `diagrams/write-path-sequence.mermaid`. Detailed step-by-step in section 5 (L4 write protocol) and normatively in the commit-protocol spec.

**Step 0 — resolve the home zone (L2).** Before any data moves, the access layer resolves the target against the global namespace (`NamespaceStore`, ADR-0020): an existing object returns its **home zone** (cached after first use); a new object is assigned a home zone and replica set by the **placement service** from policy — residency, replication factor, capacity. Steps 1–4 then run entirely *within that home zone*.

**Steps 1–4 — stage and commit (home zone).** As detailed in section 5: the client registers chunk IDs in the pending ledger, erasure-codes and writes fragments directly to the home zone's D servers — bulk data crosses no shared component, the basis of the throughput-scaling claim — then issues the single atomic metadata mutation that *is* the commit point. The file does not exist until that mutation and fully exists after it.

**Two linearization points, not one.** Introducing or moving a *name* (create, rename) is linearized globally by L2, so it is visible in every zone and never lost or resurrected (section 8.1); a file's *content version* is linearized at its home zone by the commit point. For a new object the global name is published only once its first content version has committed, so no reader anywhere sees a name without content. The exact interleaving is owned by the commit-protocol spec.

## 6.2 Read path

See `diagrams/read-path-sequence.mermaid`.

1. **Resolve the home zone (L2).** Resolve the path/key against the global namespace → file identity, **home zone**, and an ACL check (cached after first use). Consistency-sensitive reads route to the home zone (section 6.6); stale-tolerant reads may target a nearer replica.
2. **Fetch the chunk map (L4).** From the home zone's metadata store — or a version-fenced replica — get the chunk list, fragment locations, and EC scheme (cached after first read). In steady state both metadata hops are cached, so a hot read goes straight to the data path.
3. **Read fragments directly from D servers.** Holding the EC logic, the client needs only *k* of *n* fragments and reconstructs from whichever *k* arrive first — turning erasure coding into a tail-latency *advantage*: a slow or dead disk does not slow the read.
4. **Verify checksums** on every fragment, catching bit rot at read time; re-read elsewhere on failure.

## 6.3 Repair (self-healing, intra-zone)

Driven by custodians (L4):

1. **Scrub** — continuously read fragments, verify checksums against metadata, detect bit rot *before* the data is needed.
2. **Reconstruct** — on D-server loss or failed checksum, read any *k* surviving fragments, recompute the missing ones, place them on healthy servers in correct failure domains, and update the chunk's location via a single atomic metadata mutation — the same commit-point pattern as a write.
3. **Rebalance** — proactively move data off draining/hot servers, preserving failure-domain invariants.

Every recovery action is itself commit-point-atomic. A crashed repair job leaves garbage (collected later), never corruption.

The metric that matters: **time-to-repair vs. failure rate**. Durability ("the nines") is essentially the probability that more than *m* fragments fail within one repair window. Fast, parallel repair matters more than wide encoding. This is why repair-queue depth and time-to-repair are first-class telemetry (ADR-0011), not vanity metrics.

**Repair vs. serve.** A D-server loss makes both clients (read reconstruction, section 6.2) and custodians (repair reconstruction) read the surviving fragments, so they contend. Repair reads are throttled **below** foreground reads to protect read latency — but repair priority **rises as redundancy falls**, so a chunk near its durability floor (close to losing its *m*-th fragment) preempts foreground work. Durability is gate-zero (goal 1); latency yields to it only when redundancy is genuinely threatened. This dynamic priority is part of the admission/backpressure model (section 8.9).

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

## 6.7 Delete and space reclamation

Delete is a commit-point operation too, and its cost is paid lazily, off the critical path.

1. **Unlink (the commit point).** Removing the dirent — and tombstoning the inode/version — is a single atomic metadata mutation, linearized globally by L2 for the *name* so no zone ever resurrects a deleted file (section 8.1). The delete acknowledges here; the object is now invisible to every consistency-sensitive reader and its chunks are unreferenced.
2. **Reclaim (custodian GC, L4).** The orphaned fragments are reclaimed by a GC custodian after a grace period — long enough that an in-flight reader holding the old version is never torn — exactly the pending-ledger sweep pattern (section 5). Reclamation is background work, not on the delete's latency path.
3. **Propagate across zones (L3).** If the object had replicas, the deletion propagates through the replica catalog: each holding zone drops its catalog record and its GC reclaims the local fragments. Until propagation completes, a *stale-tolerant* read in a lagging zone may still see the old object, bounded by replication lag (section 8.1); consistency-sensitive reads route to the home zone and never do.
4. **Prove it (audit + crypto-erase).** The deletion is recorded in the append-only audit/event log — the operator's compliance and GDPR-deletion story (section 8.3). Where per-tenant envelope encryption is enabled, dropping the object/tenant key crypto-shreds the data immediately, making it unrecoverable independent of when GC runs (section 8.5).
