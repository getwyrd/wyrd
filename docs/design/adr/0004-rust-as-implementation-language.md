---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - rust
  - language
---
# 0004. Rust as the implementation language

## Context

The realistic choice was Go or Rust; both have shipped systems of exactly this class (Go: SeaweedFS, MinIO, CubeFS, CockroachDB's Pebble; Rust: TiKV, Garage). Go offers a larger infrastructure contributor pool, faster ramp-up, excellent operational legibility for the Kubernetes-shaped target audience, and faster iteration for a small team. Rust offers no GC tail latencies in the data path, compile-time guarantees in exactly the code where bugs are silent corruption rather than crashes, io_uring headroom, TiKV-ecosystem affinity, and access to the deterministic-simulation-testing culture.

The project's stated differentiator is correctness: a provably-atomic commit protocol with a conformance suite.

## Decision

Implement in **Rust**, for all components, for v1. Language choices fail on people, not benchmarks; the founding contributors' fluency and the correctness-as-differentiator positioning point to Rust.

## Consequences

- Memory safety in the client library and custodians, where bugs are silent corruption.
- Access to deterministic simulation testing (ADR-0009) — a credibility engine for the atomicity claim, where the Rust ecosystem leads.
- Smaller drive-by-contributor pool and steeper on-ramp — mitigated by excellent CONTRIBUTING docs and good-first-issues in non-hot-path crates.
- Slower compile cycles on a large workspace — mitigated by a disciplined crate split (ADR-0016).
- Lost conveniences, each re-solved: no pure-Go klauspost/reedsolomon (use `reed-solomon-simd`); no embedded etcd (use an in-memory coordination backend for the dev profile — ADR-0006).
- The D server sits behind a gRPC `ChunkStore` contract, so a future Rust-vs- performance question is settled by the CI benchmark suite, and any component could in principle be reimplemented behind its protobuf contract.
