---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - consistency
  - commit-protocol
---
# 0015. Consistency contract: home-zone authority, version-fence reserved

## Context

The namespace is globally strongly consistent (L2 is synchronously consensus-replicated) and a file's writes are linearizable at its home zone (the commit point). The open question was cross-zone read semantics when a reader is served by a non-home zone holding an asynchronously-replicated copy. Three options: (A) home-zone authority — route consistency-sensitive reads to the home zone; (B) nearest-replica with bounded staleness — fast but no cross-zone read-your-writes; (C) version-fenced reads — carry a version high-water mark, serve from a replica only if caught up, giving per-session read-your-writes and monotonic reads while allowing fast local reads.

## Decision

Adopt **Option A (home-zone authority) for v1**, with the **Option C version-fence reserved** in the protocol as a non-breaking future strengthening. The mechanism for C — a per-file version authoritative in L2, usable as a read fence — is the existing `meta:version` counter and costs nothing to reserve now. Adding C later only ever strengthens locality; it never relaxes a guarantee.

The contract (full text in architecture section 8.1):

1. Namespace operations are linearizable globally.
2. A file's writes are linearizable at its home zone.
3. Per-session read-your-writes and monotonic reads (v1: route to home zone; later: version-fence replica reads).
4. Stale-tolerant reads may lag by up to the replication lag; cross-session visibility of a remote write is eventually consistent, bounded by lag.
5. Synchronous N-zone replication is a per-tenant opt-in.

## Consequences

- A contract that can be stated in a few sentences, tested exhaustively, and never walked back.
- Failover preserves monotonic reads via the same version high-water mark.
- The collaborative-editor future is served: the home zone is the per-document serialization point (point 2).
- v1 readers far from a file's home pay WAN latency on consistency-sensitive reads — accepted, because the owner is usually near the home region, and C removes this later without a contract change.
