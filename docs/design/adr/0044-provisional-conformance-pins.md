---
created: 07.07.2026 19:30
type: adr
status: Proposed
tags:
  - adr
  - process
  - testing
  - conformance
  - governance
---
# 0044. Provisional pins in the shared conformance suite: mark, don't gate

## Context

The shared `metadata-conformance` suite is an **executable, multi-backend
contract**: every `MetadataStore` implementation (redb today; TiKV and
FoundationDB behind the trait per ADR-0008/0042) must pass the identical suite,
which is also how the trait is pinned by more than one implementation (ADR-0006).
Adding a property to that suite therefore does not test one backend — it
promotes a requirement that *every* backend, present and future, must satisfy.

That power created a governance gap. #419 landed `contract_read_after_commit`
and `contract_scan_is_consistent_cut` into the shared suite, encoding the
read-consistency semantics decided in #261 — but #261 was still open and
unratified: its decision lived only in a maintainer comment, and the (now frozen)
M4 plan proposal 0015 still listed it under §Open questions. So a not-yet-governed
decision became an executable, cross-backend contract that TiKV (#254) and FDB
(#438) inherit unchanged. The Act review (2026-07-04) surfaced this and asked for
a standing rule.

The genuine tension: **ratify-first** protects governance but is too rigid —
writing the pin is often exactly what clarifies and de-risks the decision, so
forbidding a pin until its decision is ratified stalls the work that would ratify
it (and would retroactively cast the reasonable #419 landing as a violation). At
the other pole, allowing **unmarked** early pins lets ungoverned requirements
accrete silently into the contract every backend must honour — precisely the
failure #426 names.

## Decision

**We adopt the provisional-marker rule.** A conformance property MAY land in the
shared suite before the decision it encodes is ratified, provided it is tagged
with an explicit `PROVISIONAL(<decision-id>)` marker.

- The pin **MUST** carry a `PROVISIONAL(<decision-id>)` marker naming the
  governing decision (the issue and/or ADR that will ratify it), adjacent to the
  property in the shared conformance suite — today
  `crates/metadata-redb/tests/conformance.rs`; the `crates/metadata-conformance`
  crate once a second backend lifts it there.
- When that decision ratifies, the marker **MUST** be removed in the same change
  that records the ratification — the pin then stands unconditional.
- A provisional pin is **still binding**: every backend must pass it. The marker
  records that its *governance* is pending, not that the requirement is optional
  or a backend may skip it.

We **reject "ratify-first"** (option 1 of #426) as too rigid — it blocks the
pin-leads-decision workflow that the #419/#261 case shows is normal and useful.
We **reject the mechanical Check/CI gate** (option 3) *for now* — a greppable
marker plus the ratify-sweep discipline is the lightweight pole; if provisional
markers are observed to rot un-swept, escalating to a mechanical gate that
requires every shared-suite property to reference a ratified decision id is the
reserved next step, and this ADR is what it would supersede.

**Immediate follow-through (the live instance).** #419's
`contract_read_after_commit` / `contract_scan_is_consistent_cut` are confirmed to
encode #261's intended fresh-snapshot / one-consistent-cut-per-scan semantics that
the paged TiKV/FDB scans must preserve. #261 is resolved, but its ratification is
**not yet recorded in a landed accepted artifact** — proposal 0015 is frozen with
the item still under §Open questions, and this ADR is itself only `Proposed`. So,
by this rule, those pins **carry `PROVISIONAL(#261)`** when the marker convention
is introduced to the suite, and the marker is cleared in the same landed change
that records the ratification (this ADR's acceptance). The marker convention lands
with the M4 backend code, since the shared suite is on the integration branch
today — there is nothing to sweep on `main` yet, but the rule binds the moment
there is.

## Consequences

Momentum is preserved: a pin may lead its decision, which is when a pin is most
valuable. Governance debt becomes **visible and self-clearing** — a
`grep -r PROVISIONAL` over the conformance suite enumerates every requirement
whose decision is still in flight, and each marker names what must ratify to clear
it.
The shared suite stays honest across backends: a new backend (FDB, #438) can read
the suite and know which contracts are settled and which are provisional.

The cost is that the sweep is a discipline, not yet an enforced invariant — a
marker could be forgotten on ratification, leaving a stale `PROVISIONAL` tag on a
now-governed pin. That failure is benign (the pin is still binding and correct;
only its bookkeeping lies) and greppable, and it is exactly the trigger to adopt
the reserved mechanical gate. This commits the project to a simple standing rule:
**every property in the shared conformance suite either references a ratified
decision or carries a `PROVISIONAL(<decision-id>)` marker** — nothing ungoverned
enters the contract silently. Reversing toward stricter governance (ratify-first
or the mechanical gate) is cheap; this is the least-machinery pole by design.
Refs #426, #419, #261, #254, #438; ADR-0006 (two-implementation trait pinning).
