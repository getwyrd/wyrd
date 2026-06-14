# 0009. Deterministic simulation testing as first-class

Date: design phase
Status: Accepted

## Context

The project's pitch is a provably-atomic commit protocol. The credibility engine
for that claim is the FoundationDB-style methodology: run the entire zone —
metadata, D servers, custodians, faults, clock skew — in a single-threaded
simulated world where every bug reproduces from a seed. The Rust ecosystem
(madsim, turmoil) supports this well. The constraint it imposes — components
written against abstract time/network/disk traits — is nearly impossible to
retrofit.

## Decision

Treat deterministic simulation testing (DST) as a first-class architectural
principle, not a test-time afterthought. Components are written against abstract
time/network/disk abstractions in a `testkit` crate that the others test
*against*. The DST harness and the commit-protocol property tests attach at
Milestone 0 and grow with the system. Jepsen-style fault injection begins as
soon as there is a networked path.

## Consequences

- The atomicity claim is backed by reproducible, seed-driven fault tests.
- Interface shapes are constrained from day one (must be simulatable) — accepted,
  because retrofitting is impractical.
- `testkit` is a real dependency, not a helper, so DST does not rot as code
  grows.
