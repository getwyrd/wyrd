---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - rust
  - structure
---
# 0016. Monorepo and evolving crate structure

## Context

The crates evolve together, conformance vectors must sit beside the spec they verify, and docs must be reviewable in the same PR as the code they describe. Multi-repo's only real win — independent release cadence across teams — is not a need for a small team and imposes a coordination tax. Separately, premature fine-grained crate splitting has a real cost (Cargo plumbing, cross-crate visibility friction), while trait boundaries are cheap to define and expensive to retrofit.

## Decision

A **monorepo** (Cargo workspace). The mature structure is the `crates/` + `docs/` + `deploy/` layout in the architecture overview, with the dependency rule: implementations and consumers depend on the `traits` crate, never on each other's concretes; only the `server` binary wires concretes together. `chunk-format` and `proto` are dependency-light foundation crates; `testkit` is first-class (ADR-0009).

The structure is allowed to **evolve**: start coarser (e.g. `chunk-format`, `proto`, `traits`, a combined `core`, and `server`) and split as boundaries firm up and compile times demand. **Trait boundaries exist from day one even where crate boundaries do not yet.** Commit `Cargo.lock` (this is an application). Automation lives in an `xtask` crate (codegen, release, conformance run).

## Consequences

- Conformance vectors, docs, and code stay synchronized in single PRs.
- The trait keystone makes backend swaps composition changes, not refactors.
- Coarse-to-fine evolution avoids premature plumbing while preserving the important (trait) boundaries.
- Contributors on any OS run `cargo xtask <thing>` without a working `make`.
