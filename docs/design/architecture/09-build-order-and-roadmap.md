---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - roadmap
  - build-order
---
# 9. Build order and roadmap

> Living document. For an open-source project the build order *is* the contribution roadmap — it is how newcomers know where to start.

The strategy (ADR-0009, section 4.2): **not bottom-up**. A vertical slice through the layers that matter for one operation, then widen by risk. Trait boundaries exist from day one even where crate boundaries do not yet; traits are cheap to define and expensive to retrofit, crate splits are the reverse.

## Milestone 0 — the walking skeleton

One atomic write and read, end to end, in a single process. Proves the commit protocol — the entire differentiator.

- S3 PUT/GET (minimal L1) → client library (chunk + commit) → embedded metadata store (redb) → filesystem chunk store → in-memory coordination.
- No EC yet (`replication(1)` or `none`), no custodians, no second zone, no global plane.
- DST harness (`testkit`) and the commit-protocol property tests attach here and grow with the system. Jepsen-style fault injection begins as soon as there is a networked path.
- Definition of done: a file written and read back, with the commit proven atomic under fault injection in simulation.

## Widening, in risk order

1. **Erasure coding** — real Reed-Solomon in the client (the hottest, riskiest loop), validated against the working slice. Benchmarks in CI from here.
2. **Networked D servers** — replace the in-process filesystem store with the gRPC `ChunkStore`, proving the direct-write data path.
3. **Custodians** — GC, scrub, repair, rebalance (the second home of correctness risk), now with real data to maintain. Durability telemetry emitted from their first commit.
4. **Production metadata backend** — swap redb for TiKV behind the `MetadataStore` trait, proving pluggability.
5. **Cross-zone layers** — L3 replication and L2 global namespace, last, because multi-zone is meaningless until single-zone is solid and the requirements (single-provider, single useful zone) give the most slack here.

Each step has a natural definition of done and a place to attach tests. A single zone of this design is already a useful product — a self-hostable, EC-efficient, atomically-consistent object store — which earns adoption and contributors long before the global federation exists.

## Deferred-with-reserved-seats

These are not built early, but their *hooks* must exist from the relevant milestone because they are expensive to retrofit:

- Append / CAS / watch storage primitives (ADR-0007) — the commit protocol and metadata schema must accommodate them from the start.
- The version-fence for Option C consistency (ADR-0015) — the `meta:version` counter is reserved now.
- Observability hooks (metric emission points, audit event stream, desired-state API) — emitted from when custodians exist; dashboards are cheap to add later (ADR-0011).
- openraft embedded coordination backend (ADR-0006).
- Web management UI — API-first now (ADR-0013), UI deferred.
