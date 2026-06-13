# Architecture Decision Records

Numbered, immutable records of significant decisions and *why* they were made.
An ADR is never edited after acceptance; it is superseded by a later ADR that
references it. Format follows the lightweight Nygard template (see ADR-0001).

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-spec-first-on-disk-format-only.md) | Spec-first for the on-disk format only | Accepted |
| [0003](0003-apache-2-license-and-dco.md) | Apache 2.0 license and DCO sign-off | Accepted |
| [0004](0004-rust-as-implementation-language.md) | Rust as the implementation language | Accepted |
| [0005](0005-single-provider-closed-federation.md) | Single-provider, closed federation | Accepted |
| [0006](0006-etcd-for-coordination.md) | etcd for coordination; openraft reserved | Accepted |
| [0007](0007-reserve-append-cas-watch.md) | Reserve append / CAS / watch primitives | Accepted |
| [0008](0008-tikv-metadata-and-pluggable-backends.md) | TiKV metadata store and pluggable backends | Accepted |
| [0009](0009-deterministic-simulation-testing.md) | Deterministic simulation testing as first-class | Accepted |
| [0010](0010-pluggable-deployment-substrate.md) | Pluggable deployment substrate; Kubernetes optional | Accepted |
| [0011](0011-durability-telemetry-and-declarative-management.md) | Durability telemetry and declarative management | Accepted |
| [0012](0012-opentelemetry-instrumentation.md) | OpenTelemetry instrumentation; storage/viz agnostic | Accepted |
| [0013](0013-api-first-management.md) | API-first management surface | Accepted |
| [0014](0014-single-binary-dev-only.md) | Single-binary profile is dev/eval only | Accepted |
| [0015](0015-consistency-contract.md) | Consistency contract: home-zone authority, version-fence reserved | Accepted |
| [0016](0016-monorepo-and-crate-structure.md) | Monorepo and evolving crate structure | Accepted |
| [0017](0017-project-name-and-norn-scheme.md) | Project name (Wyrd) and the Norn component scheme | Accepted |

## Why not ... ?

ADR-0002 and the architecture overview's section 11 cover the major "why not"
questions (why not fork SeaweedFS, why not Ceph, why not MinIO). A dedicated
comparison ADR can be added when the project's public positioning needs it.
