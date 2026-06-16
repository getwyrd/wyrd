---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - crosscutting
  - consistency
  - erasure-coding
---
# 8. Cross-cutting concepts

> Living document.

## 8.1 The consistency contract

The most-cited paragraph of the commit-protocol spec. v1 adopts the simplest strong contract (Option A, home-zone authority) with the mechanism for the locality optimization (Option C, version-fenced reads) reserved so it can be added later without weakening any guarantee. See ADR-0015.

1. **Namespace operations are linearizable globally.** Once acknowledged, visible in all zones. Create / rename / delete / share / ACL change never produce a "lost just-created file" or "resurrected deleted file".
2. **A file's writes are linearizable at its home zone.** The commit point totally orders its versions.
3. **Per-session read-your-writes and monotonic reads.** v1: by routing consistency-sensitive reads to the home zone. Forward-compatible path: version-fencing replica reads against the strongly-consistent namespace version (the existing `meta:version` counter — reserved, costs nothing now).
4. **Stale-tolerant reads** may be served from a replica and may lag the latest by up to the replication lag. Cross-session visibility of a remote write is eventually consistent, bounded by replication lag.
5. **Synchronous N-zone replication** is a per-tenant opt-in: no data-loss window, near-zero staleness, at the cost of cross-region write latency.

The version high-water mark also handles failover: if the home zone dies mid-session and the client is routed to a replica, the fence refuses any replica behind the last version seen — monotonic reads survive disaster.

The collaborative-editor future is served by this, not despite it: an OT/CRDT engine needs a single serialization point per document, and "the document's home zone linearizes its operation log" (point 2) is exactly that. The storage layer provides the strong primitive; the merge logic stays in the application.

## 8.2 Redundancy and recovery model

Durability is handled independently per layer, each using the mechanism suited to its data shape. This asymmetry is deliberate.

| Data | Mechanism | Why |
|------|-----------|-----|
| Chunk data (D servers) | Reed-Solomon erasure coding, e.g. RS(6,3) at ~1.5× overhead | Large volume; EC is far cheaper than replication for the same durability |
| Chunk data, across zones | Whole-copy async replication (L3) | WAN latency forbids cross-zone EC; replicate copies, EC within each |
| Zonal metadata (L4) | Consensus replication (Raft), never EC | Small but precious; losing it orphans all chunks |
| Global namespace (L2) | Synchronous consensus replication, geo-distributed | Must never lag or diverge; can afford it because it is tiny |
| Everything | Out-of-band backup to independent storage | Replication faithfully replicates logical disasters (bad migration, errant delete) |

The bootstrap rule: **backups must not depend on the system they back up.** The namespace cannot be backed up into the file system whose namespace it is; the recursion bottoms out in boring, independent storage.

Configurable durability per zone (ADR-0008): `none` (dev), `replication(n)` (small), `rs(k,m)` (production). The chunk format records the scheme per chunk, so a zone that grows from replication into EC carries mixed-era data correctly.

## 8.3 Observability — three planes

ADR-0011. Instrument with OpenTelemetry (ADR-0012); the planes are *what* to instrument:

- **Request plane** — RED metrics (rate, errors, duration) per layer; traces following a write through chunk → EC → commit. The easy, standard part.
- **Durability plane** — the part that must be designed in. Under-replicated chunk count, repair-queue depth, time-to-repair distribution, replication lag per zone pair, scrub coverage and scrub-detected corruption rate. These come from instrumenting the custodians, which must emit them as a first-class output. This plane is the differentiator: a storage system silently below its redundancy floor reports all-green on the request plane.
- **Capacity plane** — per-server, per-zone, per-failure-domain utilization and growth rate, as a leading indicator.

Plus an append-only **audit/event log** of significant state transitions (placement, repairs, admissions, policy changes, deletions) — operational debugging *and* the provider's compliance story (GDPR deletion proof).

## 8.4 Manageability — declarative reconciliation

ADR-0011, ADR-0013. The management model is declarative, not imperative: operators change desired state (zone draining, tenant replication factor, server decommissioned) and the custodians continuously reconcile reality toward it — the same control-loop pattern as Kubernetes, on the substrate already present (L2 holds desired state, L3/L4 custodians reconcile). The management API is therefore mostly "read/write desired state + observe reconciliation progress": a small, safe, auditable surface.

The operations that must be first-class, safe, resumable, and observable: adding and *draining* capacity (the operation that separates real storage systems from toys), rolling upgrades (version skew is the normal state), policy changes as managed rollouts ("changed" in L2 vs. "satisfied" by actual replicas are different moments), and backup/restore.

## 8.5 Security and trust

Single-provider, closed federation (ADR-0005): zones authenticate with mTLS under a provider-operated CA; zone health is self-reported and trusted; no proof-of-storage protocols. etcd's own auth is defense-in-depth, never the primary boundary — the coordination service is network-isolated to the zone's control components and fronted by the mTLS fabric. Per-tenant envelope encryption is an optional feature (it also means D servers hold only ciphertext, relaxing the trust needed in storage hardware).

## 8.6 The thick client and the conformance suite

The client library embeds chunking, EC, the commit protocol, and failover — so a second-language client (e.g. a Python SDK for an application team) cannot be a thin shim; it must re-implement all of it identically or it will write data the reference client cannot read, or commit non-atomically. This is why the on-disk format is a real spec with conformance vectors (`specs/`), and why a longer-term option is to expose the thick logic via a Rust core with FFI bindings rather than inviting risky reimplementation.
