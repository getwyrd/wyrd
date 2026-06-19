---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - testing
  - correctness
  - ci
---
# 0009. Testing strategy

## Context

The project's pitch is a provably-atomic commit protocol. The credibility engine for that claim is the FoundationDB-style methodology: run the entire zone — metadata, D servers, custodians, faults, clock skew — in a single-threaded simulated world where every bug reproduces from a seed. The Rust ecosystem (madsim, turmoil) supports this well. The constraint it imposes — components written against abstract time/network/disk traits — is nearly impossible to retrofit, so it must be a day-one commitment. And a testing strategy is only as good as its enforcement: how the tests run in CI is part of the same decision.

## Decision

**Deterministic simulation testing (DST) is the spine, from Milestone 0.** Components are written against abstract time/network/disk abstractions in a `testkit` crate that the rest test *against*. The DST harness and the commit-protocol property tests attach at M0 and grow with the system. Jepsen-style fault injection begins as soon as there is a networked path.

**Tooling: madsim is the primary runtime.** madsim simulates time, scheduling, network, and randomness — the whole runtime — and is therefore the constraint that shapes every async pattern in the code; turmoil may supplement network-specific scenarios. The DST tier runs in-process and is **never containerised**: a container reintroduces the real clock, scheduler, and network that DST exists to remove, breaking seed-reproducibility. It runs as `cargo test` / `cargo xtask`, on a laptop and in CI.

**Two tiers, kept distinct.** (1) *DST correctness* — in-process, deterministic, seed-reproducible, from M0, no containers. (2) *Integration, Jepsen, and performance* — real networked backends (gRPC D servers, TiKV, etcd) under containers/VMs, non-deterministic, from the first networked milestone (M2). Tier 2 validates that the abstractions Tier 1 simulates match reality; it complements DST, it does not replace it.

**CI runs and enforces the strategy — GitHub Actions, `xtask`-driven.** CI logic lives in `cargo xtask` (ADR-0016), not YAML, so the same checks run on a laptop; the existing `adr-immutability` and `docs` workflows already follow this shape. Every pull request gates on `cargo fmt --check` / `clippy -D warnings` / `build` / `test` (the DST property tests run here), `cargo xtask conformance` (the format reader against `specs/conformance/`, ADR-0002), **DCO** sign-off (ADR-0003 §1), and **`cargo-deny`** — a permissive-license allowlist denying AGPL/BSL/SSPL, plus `cargo-audit` (RUSTSEC advisories) — which makes the automatable part of the dependency-selection license test a machine-checked wall (ADR-0003 §2). The deterministic tier runs on GitHub-hosted runners every PR; Tier 2 runs from M2 on nightly/heavier runners. `Cargo.lock` is committed for reproducibility (ADR-0016), and **a DST seed that finds a bug is committed as a permanent regression test** (the FoundationDB / TigerBeetle pattern).

## Consequences

- The atomicity claim is backed by reproducible, seed-driven fault tests, and the rules around it — DCO, the dependency-license wall, format conformance, ADR immutability — are machine-enforced in CI rather than honour-system; a contribution that breaks one fails the build.
- Interface shapes are constrained from day one (must be simulatable) and async patterns are constrained to the madsim runtime — accepted, because retrofitting determinism is impractical.
- `testkit` is a real dependency, not a helper, so DST does not rot as code grows.
- The container question is settled: the correctness tier never uses containers; containers/VMs belong only to the non-deterministic integration tier (M2+).
- CI logic in `cargo xtask` is runnable locally and not locked to GitHub; the `cargo-deny` license allowlist is a small list to maintain, accepted as the mechanism that makes ADR-0003's license test real.
