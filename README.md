# Wyrd

> **What is written, stays written.**

Wyrd is a globally scalable, atomically consistent distributed file storage
foundation. It keeps metadata and bulk data architecturally separate, yet every
write commits as a single, indivisible act: from the caller's point of view, a
write either happened in full or not at all. It scales from one static binary on
a laptop to a multi-region fleet in production — the same system, configured
differently. Written in Rust, licensed Apache-2.0.

**Status:** early implementation. The single-process slice works end to end —
Milestone 0 (atomic commit, proven under deterministic simulation) and
Milestone 1 (real Reed-Solomon erasure coding in the data path) are complete;
networked multi-process storage (Milestone 2) is next. Wyrd is **not yet
deployable** and carries no durability or stability promise at this stage.

## Why it exists

Many systems do part of this; no open-source one combines all of it: a rigorous
atomic commit point, a globally consistent namespace, erasure coding within a
zone and geo-replication across zones, pluggable metadata and chunk backends
behind narrow interfaces, an on-disk format specified so the data outlives the
software that wrote it — and correctness treated as the headline feature,
verified with deterministic simulation testing from day one.

| Property | What it means |
|----------|---------------|
| **atomic** | Commit-point atomicity. A write is linearized at a single point — no torn state, no half-written files, ever observable. |
| **global** | One strongly consistent namespace across many zones; per-file writes linearize at a home zone. |
| **durable** | Configurable per zone: replication or Reed-Solomon erasure coding within a zone, whole-copy geo-replication across them. |
| **pluggable** | Narrow backends — an embedded store for development, a distributed store for production — behind the same interface. |
| **provable** | Correctness as a feature: deterministic simulation testing, where every bug reproduces from a seed. |

The model follows the Colossus-class lineage — a global control plane over
per-zone storage, with bulk data flowing directly between client and storage
servers so throughput scales with the fleet rather than through a bottleneck.

## The name

In Norse and Old English myth, *wyrd* is fate — not a fixed script, but the
woven web of what has happened, what is happening, and what is yet owed. A
storage system is, in the end, a keeper of wyrd: it holds what was written,
weaves in what is being written, and carries the debts of what it still owes.
The components are named for the Norns who tend it — **Urth** (what has become),
**Verdandi** (what is becoming), and **Skuld** (what is owed). It is also,
cheerfully, a homophone of *weird* — the correct word for what happens to
distributed storage at 3 a.m. See [`docs/NAME.md`](docs/NAME.md).

## Documentation

This repository is the single source of truth for Wyrd's documentation,
authored in Markdown and published to [getwyrd.dev](https://getwyrd.dev).

- [`docs/`](docs/README.md) — where everything lives, and how it's organized.
- [`docs/design/`](docs/design/README.md) — **start here**: architecture,
  specifications, decision records (ADRs), and proposals.

## Repository layout

Wyrd is a Cargo workspace. Following [ADR-0016], it starts coarse — foundation
crates plus a combined `core` — and splits as boundaries firm up. The same ADR
sets the dependency rule: implementations and consumers depend on `traits`,
never on each other's concretes; only the `server` binary wires concretes
together.

| Crate | Role |
|-------|------|
| [`crates/traits`](crates/traits) | The narrow interfaces everything depends on. |
| [`crates/chunk-format`](crates/chunk-format) | The on-disk chunk/fragment format codec and conformance vectors. |
| [`crates/proto`](crates/proto) | Protocol definitions. |
| [`crates/core`](crates/core) | Combined core logic (split as boundaries firm up). |
| [`crates/server`](crates/server) | The binary that wires concrete implementations together. |
| [`crates/testkit`](crates/testkit) | Shared test scaffolding and fixtures. |
| [`xtask`](xtask) | Workspace automation tasks. |

Build and test the workspace with the standard Cargo flow:

```sh
cargo build
cargo test
```

## Security

Wyrd is pre-release software and carries no security promise yet, but we still
want to hear about vulnerabilities early. Please report them **privately** — see
[SECURITY.md](SECURITY.md) for how.

## Contributing & governance

Contributions are welcome. Please read the
[Code of Conduct](docs/governance/CODE_OF_CONDUCT.md) and
[Governance](docs/governance/GOVERNANCE.md) documents before getting involved.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE)
for attribution.

[ADR-0016]: docs/design/adr/0016-monorepo-and-crate-structure.md
