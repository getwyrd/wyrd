---
created: 22.06.2026 18:50
type: adr
status: Proposed
tags:
  - adr
  - time
  - clock
  - coordination
  - verification
---
# 0024. Clock and time-source trust

## Context

Several load-bearing mechanisms trust "a clock". Coordination leases expire against a clock
(`MemCoordination` reads `Clock::now_millis()` to stamp and check `expiry_millis`; the default
lease TTL is 30 s), and the custodian's first GC input is "bytes behind an *expired* lease" —
so the moment a fragment becomes collectable is a clock reading. The atomicity story (ADR-0015)
and the "never reclaim a referenced fragment" invariant (the custodian, scenario Q3) both rest
on lease expiry meaning what it says.

Time's *integrity* has so far been an unstated assumption, and the production seam quietly
fabricates a value when it fails: `SystemClock::now_millis()` does `…unwrap_or(0)`, so a clock
that cannot be read returns the Unix epoch rather than an error. A fabricated `now` of 0 makes
every lease look unexpired (`expiry_millis > 0`), so a node that loses its clock would hold
leases forever and starve GC — a silent, fail-*open* failure in exactly the path durability
depends on. Conversely, a clock that jumps *forward* expires live leases early. The DST harness
can already drive a `ManualClock`, so skew and jumps are reproducible to test against — the gap
is a *decision* about what correct behaviour is, not the means to verify it.

This decision is timely now (M2 introduces real, independently-clocked nodes) but the seam and
the invariant exist today, so the rule can be enforced from M0.

## Decision

1. **No fabricated time.** A clock read that fails is a fault, not a `0`. The production
   `Clock` MUST surface the failure (the `unwrap_or(0)` is replaced) so callers never act on an
   invented timestamp. A node that cannot read a plausible time MUST fail closed for the
   operations that depend on it (lease renewal, GC reclamation, expiry checks) rather than
   proceed against a bad clock.

2. **Bounded skew tolerance.** Lease and expiry comparisons accept a small, configured
   clock-skew window; beyond it the operation fails closed. The window is a single stated budget
   — kept small because every second of slack is either attacker slack or a wider window for a
   torn lease — shared by every time-dependent check so it is set once.

3. **A trusted time source.** In a multi-node deployment, nodes synchronize via **authenticated
   time** (NTS, RFC 8915, or the platform's trusted time source), not unauthenticated NTP that a
   network attacker on the internal fabric can spoof. Time-source integrity is an operational
   requirement of the deployment view (section 7), not optional.

4. **Implausible-clock detection.** A node whose clock is grossly wrong (failed sync, large
   drift, time moving backward across reads) drains itself rather than issuing leases or running
   GC against a corrupt clock — a corrupt clock is a correctness fault, treated like one.

This refines the coordination contract (ADR-0006: a lease is now "expires against a *trusted*
clock within a skew budget") and the consistency contract's failover fence (ADR-0015), and it
is verified under DST (ADR-0009) by injecting skew, jumps, and backward motion through the
`ManualClock` seam.

## Consequences

- Clock failure can no longer silently fabricate a time and starve GC, and a forward jump can no
  longer reclaim a referenced fragment early; the trusted-clock assumption becomes explicit,
  bounded, and enforced in exactly the durability-critical path.
- The skew budget is shared across lease expiry, GC, and any later freshness check, so it is set
  once and small — and it becomes a knob the operator can reason about.
- Time-source integrity becomes a documented deployment requirement (authenticated sync,
  alerting on daemon failure and stratum drift), and a node with a bad clock removes itself —
  consistent with the fail-closed admission posture (section 8.9).
- Cost: the `Clock` trait and its callers carry a failure path they did not before (a clock read
  becomes fallible at the seam), and the skew budget is one more parameter to set and test. The
  alternative — trusting the wall clock implicitly — is what this ADR rejects.
- Refines ADR-0006 and ADR-0015.
