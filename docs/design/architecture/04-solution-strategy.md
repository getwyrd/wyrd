---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - strategy
  - quality-goals
---
# 4. Solution strategy

> Living document. This section is the bridge between the quality goals (section 1.3) and the detailed building-block view (section 5).

| Quality goal | Strategy | Where |
|--------------|----------|-------|
| Correctness / durability | Commit-point atomicity: a single linearizable metadata mutation makes a write visible. Data fragments are staged invisibly first. | ADR-0001, commit-protocol spec |
| | Deterministic simulation testing of the whole zone from day one; Jepsen-style fault injection. | ADR-0009 |
| | Erasure coding for data durability within a zone; consensus replication (never EC) for metadata. | section 6 |
| Horizontal scalability | Bulk data never crosses a shared component: the client writes erasure-coded fragments directly to storage servers; the control plane handles only metadata. | section 5, L4 |
| | Metadata store is itself horizontally sharded; data and metadata tiers scale independently. | section 5, L4 |
| Operability | Durability-plane telemetry is a first-class output of the maintenance services. | ADR-0011 |
| | Declarative, self-reconciling management: operators change desired state; background services reconcile reality toward it. | ADR-0011, ADR-0013 |
| Scale range | Every backend (storage, metadata, coordination) is pluggable behind a narrow trait, with an embedded single-process implementation and a distributed one. | ADR-0006, ADR-0008 |
| Replaceability | Components communicate through versioned protobuf and depend only on trait definitions, never on concrete backends — only the final binary wires concretes together. | section 5, ADR-0010 |

## 4.1 The shape in one paragraph

Storage servers store bytes and verify checksums; they are deliberately dumb and commodity. The metadata store records truth — where the bytes are, what the file is, where it lives in the namespace. The control plane decides where things should live. The client library is the only component that understands what a *file* is: it chunks data, erasure-codes it, writes fragments directly to storage servers in parallel, and then issues the single atomic metadata commit that makes the file exist. Background maintenance services (custodians) keep the system healthy — garbage collection, scrubbing, repair, rebalancing — and emit the durability telemetry that makes the guarantee observable.

## 4.2 Build strategy

Not bottom-up. A **vertical slice** through the layers that matter for one real operation, then widen by risk. See section 9 for the milestone plan. The trait boundaries that enable pluggability are also exactly what let each layer start with a trivial implementation and gain a real one later — the architecture's pluggability and its build order are the same decision viewed twice.
