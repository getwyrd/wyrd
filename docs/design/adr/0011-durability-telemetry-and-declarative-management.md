---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - durability
  - operability
  - telemetry
---
# 0011. Durability telemetry and declarative management

## Context

A storage system's most important signals are not request metrics but durability metrics — and those are invisible unless designed in. A system silently below its redundancy floor reports all-green on the request plane. Separately, the dangerous operations (draining, upgrades, policy changes, recovery) are routine and stateful, and are most safely expressed as desired-state changes reconciled by background services rather than imperative commands issued in the right order.

## Decision

Two architectural rules:

1. **Custodians emit durability-plane telemetry as a first-class output** — under-replicated chunk count, repair-queue depth, time-to-repair distribution, replication lag per zone pair, scrub coverage and corruption rate. Plus an append-only audit/event log of significant state transitions. Three planes: request, durability, capacity (section 8.3).
2. **Management is declarative and self-reconciling** — operators change desired state (in L2); custodians (L3/L4) reconcile reality toward it, the same control-loop pattern as Kubernetes, on the substrate already present.

The hooks (metric emission, audit stream, desired-state API) exist from when custodians exist; polished dashboards/alerting are later and cheap to add once hooks emit.

### Reconstruction repair-accounting counters

The §1 set above answers *how much is at risk and how fast it heals*. The reconstruction custodian (the time-to-repair loop) additionally emits three `monotonic_counter.*` counters on the same `tracing`→OpenTelemetry / Prometheus seam (ADR-0012) that answer a distinct operator question — *is the repair loop actually converging, or churning?* — by separating an **attempted** repair from a **successful** one:

| Counter | Incremented when | Source |
|---|---|---|
| `reconstruction_repaired` | a repair is **dispatched** — the pass decided to rebuild this chunk's missing shard(s) and repoint its placement | `reconstruction::emit_repaired` |
| `reconstruction_conflict` | the version-conditional repoint **lost the CAS race** (a racing writer or superseded custodian moved the placement first); the rebuilt fragments become collectable garbage | `reconstruction::emit_conflict` |
| `reconstruction_aborted` | the repair **could not place** the rebuilt shard (the failure-domain selector chose a server outside the fleet view), so nothing was committed | `reconstruction::emit_aborted` |

`reconstruction_repaired` is incremented **up front**, at the assessment frame, because the rebuild runs a heavy erasure-decode plus a version-conditional commit and a metric emitted *after* that section is unreliable on the `tracing`→OTel bridge under load. Each non-success is therefore offset on its own counter, giving the **success-netting identity**:

> **successful repairs = `reconstruction_repaired` − `reconstruction_conflict` − `reconstruction_aborted`**

A conflict or an abort leaves the repair obligation **queued**, so it is re-assessed on the next pass — it is a retry, not a loss. These three counters **complement, not replace,** the five higher-level durability metrics of §1: a non-zero `reconstruction_conflict` / `reconstruction_aborted` rate while `under-replicated count` is not falling is the signal that the loop is churning rather than converging. They are emitted from the custodians' first commit; the source of truth for their exact emission is `crates/custodian/src/reconstruction.rs` (`emit_repaired` / `emit_conflict` / `emit_aborted`), and they sit alongside the proposal 0005 §"durability metrics" set.

## Consequences

- The durability guarantee is observable, not merely asserted.
- The management surface is small, safe, and auditable: read/write desired state
  + observe reconciliation.
- "Policy changed" (recorded) and "policy satisfied" (replicas exist) are distinct, observable moments.
