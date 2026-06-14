# 0011. Durability telemetry and declarative management

Date: design phase
Status: Accepted

## Context

A storage system's most important signals are not request metrics but durability
metrics — and those are invisible unless designed in. A system silently below its
redundancy floor reports all-green on the request plane. Separately, the
dangerous operations (draining, upgrades, policy changes, recovery) are routine
and stateful, and are most safely expressed as desired-state changes reconciled
by background services rather than imperative commands issued in the right order.

## Decision

Two architectural rules:

1. **Custodians emit durability-plane telemetry as a first-class output** —
   under-replicated chunk count, repair-queue depth, time-to-repair distribution,
   replication lag per zone pair, scrub coverage and corruption rate. Plus an
   append-only audit/event log of significant state transitions. Three planes:
   request, durability, capacity (section 8.3).
2. **Management is declarative and self-reconciling** — operators change desired
   state (in L2); custodians (L3/L4) reconcile reality toward it, the same
   control-loop pattern as Kubernetes, on the substrate already present.

The hooks (metric emission, audit stream, desired-state API) exist from when
custodians exist; polished dashboards/alerting are later and cheap to add once
hooks emit.

## Consequences

- The durability guarantee is observable, not merely asserted.
- The management surface is small, safe, and auditable: read/write desired state
  + observe reconciliation.
- "Policy changed" (recorded) and "policy satisfied" (replicas exist) are
  distinct, observable moments.
