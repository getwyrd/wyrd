---
created: 22.06.2026 19:30
type: adr
status: Proposed
tags:
  - adr
  - replication
  - cross-zone
  - transport
  - pluggability
  - sovereignty
---
# 0027. Cross-zone replication transport: the ReplicationQueue trait and NATS JetStream default

## Context

L3 (section 5) turns independent single-datacenter zones into one geographically distributed
system by copying committed chunks between zones per policy, off the foreground write path. The
building-block view names its transport — "NATS JetStream queue" — and ADR-0003 keeps NATS on
sovereignty grounds (Apache-2.0, CNCF; the 2025 Synadia attempt to relicense it to BSL is the
table's live cautionary example). But no ADR ever recorded *why* this transport, the way ADR-0006
did for coordination, ADR-0008 for zonal metadata, and ADR-0020 for the namespace store. The L3
transport is the last instance of a dependency the architecture **names but never decides** — the
exact gap ADR-0020 was written to close for L2, and one the replaceability goal (section 1.3,
goal 5) and the dependency rule (ADR-0010) do not permit to stand.

What L3 actually demands, derived from the architecture:

- **A durable work queue, not a data bus.** The transport carries replication *tasks* — which
  chunk, source and target zones, policy — kilobytes each; the replication *workers* copy the bulk
  bytes directly zone-to-zone (section 5). This keeps the transport off the data path, the same
  "the control plane decides and records; bulk flows directly" principle that governs L2.
- **Durable, at-least-once delivery.** A lost task is a chunk left silently under-replicated — a
  durability hole, not a dropped message. Delivery must survive a worker or broker crash.
- **At-least-once is sufficient because the operation is idempotent.** A replica is real only once
  its record commits to the replica catalog (section 5), so a redelivered copy re-runs harmlessly;
  exactly-once is neither required nor relied upon. Correctness lives in the catalog commit, not in
  the transport.
- **Per-policy, per-zone-pair routing** — "sync within a region, async across continents"
  (section 5), per-tenant replication factor and residency (section 8.8). The transport must route
  by zone-pair and policy class.
- **Consumer-paced backpressure** — cross-region bandwidth is the binding limit (scenario Q4,
  "bounded by cross-region bandwidth"), so consumers must pull at their own rate.
- **A geo-distributed broker topology** spanning the same regions the fleet does.
- **Sovereign and forkable** (ADR-0003), and single-binary-light to match the rest of the stack.

The rationale below is reconstructed from these requirements and the standing ADR-0003 keep-
decision; it is recorded here for confirmation by the decision owner in review, not asserted as a
previously-minuted choice.

## Decision

1. **Define a narrow `ReplicationQueue` trait** — the L3 analog of the other backend seams. Its
   contract is a *capability*, not a product: durable, at-least-once task streams; subject/topic
   routing keyed by zone-pair and policy class; consumer-paced pull with acknowledgement and
   redelivery. The architecture depends only on the trait; `server` wires the concrete (ADR-0010).

2. **Default backend: NATS JetStream.** Apache-2.0 and CNCF (passes ADR-0003). Its persistent
   streams with acks and redelivery give the durable at-least-once semantics; subject-based routing
   maps cleanly onto zone-pair × policy; pull consumers with flow control fit the bandwidth-bound
   consumer; and NATS **superclusters** (gateway-connected clusters with leaf nodes) are built for
   exactly the multi-region topology L3 spans. It is a single, light Go binary, matching the stack's
   single-binary-to-fleet operational gradient.

3. **At-least-once, made safe by catalog idempotence.** The transport guarantees at-least-once; the
   replica catalog (section 5), committing a replica exactly once, is what makes a redelivered copy
   safe. The system MUST NOT depend on exactly-once delivery from the transport.

4. **Tasks, not payload.** The queue carries replication tasks (kilobytes); workers copy the bulk
   bytes directly. The transport never becomes a data-proportional dependency — the same discipline
   that keeps L5 small (section 11, ADR-0006).

5. **Alternatives behind the trait, and what is rejected.** **Apache Pulsar** (Apache-2.0, CNCF,
   native geo-replication) is the open alternative for an operator who wants it, at the cost of a
   heavier deployment (BookKeeper + ZooKeeper). **Apache Kafka** is Apache-2.0 but JVM-heavy and
   less geo-native. **Redpanda is rejected** — its Community Edition is BSL, failing the ADR-0003
   licence test (a second live cautionary case beside Vault and CockroachDB). The replication
   backlog does **not** belong in **etcd** (it is data-proportional; "nothing data-proportional in
   L5", section 11), and a **hand-rolled queue over the metadata store** is rejected on the
   novelty-budget argument of ADR-0006 — acks, redelivery, flow control, and consumer groups are
   exactly the parts that are subtle to get right in a component durability trusts.

6. **No L3 below the multi-zone tier.** A single-zone deployment has nothing to replicate, so the
   `ReplicationQueue` trait is backed by an in-process queue in the dev / single-binary profile —
   consistent with ADR-0020 §5 (L2 folds into redb below the fleet) and the build order placing
   L2/L3 last (section 9).

7. **Licence is a standing selection gate (ADR-0003).** NATS's own 2025 near-miss is *why* this
   sits behind a trait: the keep-decision is re-confirmed, not assumed, and the seam preserves the
   exit should NATS ever be successfully pulled from its open governance.

## Consequences

- The architecture's last named-but-undecided dependency now sits behind a trait; "swap the
  replication transport" is a composition change in `server`, not a refactor (ADR-0010) — and given
  NATS's own governance scare, that exit is not hypothetical.
- At-least-once delivery plus catalog idempotence keeps the transport simple and the correctness in
  the metadata tier, where it is already proven under DST — rather than demanding exactly-once from
  the broker.
- The queue stays small (tasks, not bytes), so it never turns into a data-path or data-proportional
  dependency.
- Cost: operating a region-spanning NATS supercluster is real operational surface at the fleet tier;
  mitigated by L3 being last in the build order and absent below the multi-zone profile.
- A backend to keep honest under DST (ADR-0009): the at-least-once and redelivery semantics, and the
  worker's idempotent re-copy, must be simulatable and pinned by the harness like the other seams.
- **[OPEN]** Whether **synchronous N-zone replication** — the per-tenant, no-data-loss-window opt-in
  (section 8.1, point 5) — rides this transport at all. A synchronous replica must block the commit
  acknowledgement until N zones confirm, which an async fire-and-forget queue cannot express; it
  likely needs a separate synchronous path (or the trait must model both modes). This interacts with
  the consistency contract (ADR-0015) and is the sharpest open question here.
- **[OPEN]** The exact `ReplicationQueue` method set and the subject taxonomy for zone-pair × policy.
- **Reconstructed rationale** — confirm with the decision owner before this moves to Accepted.
- Refines section 5 (L3); applies ADR-0003 and ADR-0010; depends on ADR-0015 for the synchronous-
  replication interaction.
