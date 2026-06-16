---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - building-blocks
  - layers
---
# 5. Building block view

> Living document. This is the layer model (L1–L5) and the components within each layer. See `diagrams/c4-container.mermaid` for the container diagram and `diagrams/layer-stack.mermaid` for the layer stack.

The system is organized into five layers. L4 and L5 operate within a single zone (one datacenter) and are unaware that other zones exist. L1, L2, and L3 are the layers that span zones. The decisive property throughout: **the layer with global scope (L2) holds only small, slow-changing, high-value truth, so it can afford global consensus without becoming a bottleneck**, while bulk data flows directly between client and storage servers, crossing no shared component.

```
L1  Access layer            S3 / FUSE / WebDAV / SDK gateways      (stateless)
L2  Global control plane     namespace, placement, ACLs, registry   (global consensus)
L3  Cross-zone replication    async chunk copy, global custodians    (off the data path)
L4  Zonal file system         metadata store, D servers, custodians, client library
L5  Bootstrap & coordination  service discovery, leader election, locks, config
```

---

## L1 — Access layer

Stateless protocol translators. They hold no durable state, scale horizontally, authenticate the caller, and embed the client library (L4) to do the actual storage work. All consistency logic lives below them.

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| S3 gateway | Rust, `gateway-s3` crate | Primary integration surface; object semantics over the file system |
| FUSE / NFS | `fuser` crate; NFS-Ganesha | POSIX-ish access; deliberately second-class |
| WebDAV / Drive | Rust | The consumer Drive surface: sync, sharing links, web UI |
| Native SDK | gRPC + prost | The real interface; streaming reads, atomic commit, append, CAS |

---

## L2 — Global control plane

The brain of the multi-zone system: the one component with a global, authoritative view of what exists and where it should live. It controls by *deciding and recording*; it distributes by *routing actors to the right zones*, never by carrying the payload itself.

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Global namespace DB | CockroachDB or TiDB (Spanner-class) | Directory tree, file→home-zone mapping, ACLs, sharing, quotas. Synchronously consensus-replicated. |
| Placement service | Rust, policy engine | Decides home zone and replica set per file from policy (residency, replication factor, cost tier, capacity) |
| Zone registry | in the namespace DB | Every zone: region, capacity, utilization, health, capabilities |
| Identity & auth | OIDC for users; internal mTLS PKI for services | Trust fabric (single-provider, so internal PKI not cross-org) |

Holds *metadata about metadata* — kilobytes per file. Consulted on namespace and placement operations (create, rename, share, delete, "where do I read this"), which are rare relative to data operations and ideally cached by clients.

---

## L3 — Cross-zone replication

Turns independent single-datacenter systems into one geographically distributed system, without ever sitting on the foreground write path.

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Replication workers | Rust; NATS JetStream queue | Copy committed chunks between zones per policy; sync within a region, async across continents |
| Replica catalog | in L2's DB | Tracks which zones hold valid replicas; a replica exists only once its record commits |
| Global custodians | Rust | Disaster recovery: detect zone loss, find under-replicated files, schedule re-replication from survivors |

Default replication is **async** (home-zone commit acknowledges immediately). **Sync N-zone** replication is a per-tenant opt-in that eliminates the data-loss window at the cost of cross-region write latency.

---

## L4 — Zonal file system (the Colossus analog, one per datacenter)

Where atomicity lives and where the bulk of correctness risk concentrates.

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Metadata store | `redb` (embedded) / TiKV (prod), behind `MetadataStore` trait | Inodes, dirents, chunk maps, the pending-chunk GC ledger, version counters. Hosts the single atomic mutation that *is* the commit point. |
| D servers | Rust, `dserver` crate; `ChunkStore` trait (local / filesystem / S3) | Store/retrieve EC fragments by chunk ID, verify checksums, report health. Deliberately dumb — no placement logic, no metadata. |
| Custodians | Rust, `custodian` crate | GC (expired pending chunks, orphans), scrubbing (checksum verification → bit-rot detection), reconstruction (rebuild lost fragments from survivors), rebalancing. Emit durability telemetry. |
| Client library | Rust, `client` crate; `reed-solomon-simd` | Chunk → erasure-code → direct parallel fragment writes → atomic commit. Small-file inlining. Append/CAS. Read reconstruction from any *k* of *n* fragments. |

A zone is the unit of atomicity and the unit of (intra-provider) federation.

### The metadata model

Hierarchical: **inode + dirent**, not path-as-key.

- `inode:<id>` → attributes, chunk map (or inline data for small files), state, version.
- `dirent:<parent_id>/<name>` → child inode id.

This makes rename a single dirent mutation (atomic under the same mechanism as a write) instead of a mass key rewrite, and makes cross-zone sharing expressible (a dirent pointing at an inode owned elsewhere). It is the strongest concrete driver of the TiKV-over-HBase choice, because file creation must atomically write both the inode and its dirent — a multi-key transaction. See section 6 and ADR-0008.

### The write protocol (the commit point)

1. **Intent** — client registers chunk IDs in the pending-chunk ledger with a lease. The chunks exist nowhere in the namespace.
2. **Data path** — client erasure-codes and writes fragments directly to D servers, which verify checksums. Failures here are harmless garbage.
3. **Commit** — one atomic metadata mutation writes the chunk map + sets state `COMMITTED` + bumps the version, conditional on the prior version. **This is the atomicity.** Concurrent writers conflict here; exactly one wins.
4. **Release** — delete the ledger entries. Crash between 3 and 4 leaves the ledger to a custodian sweep; crash before 3 leaves leased garbage for GC.

Readers never consult the ledger; they see the old version or the new one, never a hybrid.

---

## L5 — Bootstrap and coordination

The smallest layer by data volume and the one everything else depends on. It answers the questions a distributed system must answer before it can do any real work. Stores kilobytes; never on the data path.

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Coordination service | etcd (prod) / in-memory (dev), behind `Coordination` trait | Service discovery (leased registration), leader election (single active custodian), distributed locks with fencing tokens, zone-wide config with change notification |
| Deployment substrate | systemd / docker-compose / Kubernetes operator | Process lifecycle. Pluggable; the binary never couples to orchestrator APIs (ADR-0010). |

The Colossus-style bootstrap recursion bottoms out here: etcd depends on nothing above it. Losing L5 loses no data; established connections keep working from cached state. What is lost is the ability to *react* — failovers, elections, topology changes — until coordination returns.

**openraft** is reserved as a future embedded coordination backend (a no-external-dependency production mode) behind the same trait, validated by the existing DST harness once the trait semantics are pinned by two implementations. Not built for v1 (ADR-0006).

---

## Cross-cutting components

| Concern | Technology | Crate / location |
|---------|------------|------------------|
| Wire contracts | protobuf + prost | `proto` |
| On-disk format | versioned, spec-first | `chunk-format` + `specs/chunk-format/` |
| Pluggability seams | trait definitions, no impls | `traits` (`ChunkStore`, `MetadataStore`, `Coordination`) |
| Telemetry | OpenTelemetry (OTLP) | `telemetry` (ADR-0012) |
| Simulation testing | madsim / turmoil | `testkit` (ADR-0009) |
| Composition / the binary | wires all concretes | `server` |

The dependency rule (ADR-0010): implementations and consumers depend on `traits`, never on each other's concretes. Only `server` knows the concrete backends, which is what makes "swap redb for TiKV" or "in-memory for etcd" a composition change rather than a refactor.
