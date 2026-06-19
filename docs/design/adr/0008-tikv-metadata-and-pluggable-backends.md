---
created: 13.06.2026 11:57
type: adr
status: Accepted
tags:
  - adr
  - metadata
  - storage
  - pluggability
---
# 0008. TiKV metadata store and pluggable backends

## Context

The metadata model is hierarchical (inode + dirent) to make rename a single atomic mutation and to express cross-zone sharing (section 5). File creation must atomically write both an inode and its dirent — a multi-key operation. HBase offers only single-row atomicity, which would force either denormalization (re-introducing a rename/scale problem) or a write-ahead intent log — the exact contortion Tectonic's authors describe regretting. TiKV offers native multi-key transactions, Raft replication, region-aware placement, and a first-class Rust client. Separately, the single-binary profile cannot run TiKV's multi-node minimum on NAS-class hardware.

## Decision

Define a narrow `MetadataStore` trait (`Get`/`Put`/`Scan`/`AtomicCommit` with multi-key conditions). Ship two backends: **redb** (pure-Rust embedded, single-process) for dev/small and the Synology profile; **TiKV** for production. Make durability scheme configurable per zone (`none` / `replication(n)` / `rs(k,m)`), recorded per chunk in the on-disk format so a zone can grow from replication into EC with mixed-era data.

## Consequences

- Atomic multi-key directory operations without intent-log gymnastics.
- The trait keeps the metadata layer honest about which KV features it actually depends on, preserving the option to swap stores.
- redb avoids a C++ build dependency (RocksDB) for the embedded profile, easing cross-compilation for NAS targets; RocksDB remains a fallback behind the trait.
- Backend choice becomes a composition concern in `server`, not a refactor.
