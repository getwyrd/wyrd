---
created: 13.06.2026 11:57
type: index
tags:
  - adr
---
# Architecture Decision Records

Numbered, immutable records of significant decisions and *why* they were made. An ADR is never edited after acceptance; it is superseded by a later ADR that references it. Format follows the lightweight Nygard template (see ADR-0001).

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-spec-first-on-disk-format-only.md) | Spec-first for the on-disk format only | Accepted |
| [0003](0003-apache-2-license-and-dco.md) | Licensing, and dependency-selection criteria | Accepted |
| [0004](0004-rust-as-implementation-language.md) | Rust as the implementation language | Accepted |
| [0005](0005-single-provider-closed-federation.md) | Single-provider, closed federation | Proposed |
| [0006](0006-etcd-for-coordination.md) | etcd for coordination; openraft reserved | Accepted |
| [0007](0007-reserve-append-cas-watch.md) | Reserve append / CAS / watch primitives | Accepted |
| [0008](0008-tikv-metadata-and-pluggable-backends.md) | TiKV metadata store and pluggable backends | Accepted |
| [0009](0009-deterministic-simulation-testing.md) | Testing strategy | Accepted |
| [0010](0010-pluggable-deployment-substrate.md) | Pluggable deployment substrate; Kubernetes optional | Accepted |
| [0011](0011-durability-telemetry-and-declarative-management.md) | Durability telemetry and declarative management | Accepted |
| [0012](0012-opentelemetry-instrumentation.md) | OpenTelemetry instrumentation; storage/viz agnostic | Accepted |
| [0013](0013-api-first-management.md) | API-first management surface | Proposed |
| [0014](0014-single-binary-dev-only.md) | Single-binary profile is dev/eval only | Accepted |
| [0015](0015-consistency-contract.md) | Consistency contract: home-zone authority, version-fence reserved | Accepted |
| [0016](0016-monorepo-and-crate-structure.md) | Monorepo and evolving crate structure | Accepted |
| [0017](0017-project-name-and-norn-scheme.md) | Project name (Wyrd) and the Norn component scheme | Proposed |
| [0018](0018-reserve-hooks-for-hyperscale-identity-consumer.md) | Reserve hooks for a hyperscale identity consumer | Proposed |
| [0019](0019-chunk-format-layout.md) | Chunk/fragment on-disk format layout | Accepted |
| [0020](0020-global-namespace-store.md) | Global namespace store (L2) and the NamespaceStore trait | Proposed |
| [0021](0021-encryption-at-rest-and-key-management.md) | Encryption at rest and key management | Proposed |
| [0022](0022-multi-tenancy-model.md) | Multi-tenancy model | Proposed |
| [0023](0023-act-beat-process-improvement-loop.md) | Adopt the Act beat: a process-improvement loop | Proposed |
| [0024](0024-clock-and-time-source-trust.md) | Clock and time-source trust | Proposed |
| [0025](0025-internal-service-to-service-trust.md) | Internal service-to-service trust | Proposed |
| [0026](0026-key-service-and-kms-backend-selection.md) | Key management: the KeyService contract and KMS backend selection | Proposed |
| [0027](0027-cross-zone-replication-transport.md) | Cross-zone replication transport: the ReplicationQueue trait and NATS JetStream default | Proposed |
| [0028](0028-erasure-versus-retention-precedence.md) | Erasure-versus-retention precedence | Proposed |
| [0029](0029-key-compromise-emergency-response.md) | Key-compromise emergency response | Proposed |
| [0030](0030-build-and-release-integrity.md) | Build and release integrity | Proposed |
| [0031](0031-watch-and-change-feed-contract.md) | Watch / change-feed contract | Proposed |
| [0032](0032-d-server-on-disk-fragment-layout.md) | D-server on-disk fragment layout (FsChunkStore) | Proposed |
| [0033](0033-fragment-durability-via-redundancy.md) | Fragment durability: redundancy + crash-atomic rename, not per-write fsync | Proposed |
| [0034](0034-d-server-disk-model.md) | D server disk model: one-per-disk now, multi-disk reserved | Proposed |
| [0035](0035-no-dst-reachable-global-mutable-state.md) | No DST-reachable shared mutable global state (seed-determinism discipline) | Proposed |
| [0036](0036-internal-ca-step-ca-spire.md) | Internal CA and identity fabric: step-ca now, SPIRE reserved | Proposed |
| [0037](0037-proposal-and-spec-process.md) | Proposal and specification process, lifecycle, and immutability | Accepted (supersession-marker clause refined by 0038) |
| [0038](0038-supersession-recorded-in-the-index.md) | Supersession is recorded in the index, not on the frozen file | Proposed |
| [0039](0039-tier1-consistency-in-repo-scenario.md) | Tier-1 consistency-over-repair as an in-repo Rust scenario; literal Jepsen deferred (refines proposal 0005 §13.2) | Accepted |
| [0040](0040-mixed-era-placement-expansion.md) | Mixed-era placement expansion: one identity-fallback rule, liberal read / strict maintenance (refines proposal 0005 §placement) | Accepted |
| [0041](0041-consistency-checker-substrate.md) | Consistency-checker substrate: model the mutable metadata register, not the immutable data path (refines the ADR-0039 deferral; unblocks #329) | Proposed |

## Why not ... ?

ADR-0002 and the architecture overview's section 11 cover the major "why not" questions (why not fork SeaweedFS, why not Ceph, why not MinIO). A dedicated comparison ADR can be added when the project's public positioning needs it.
