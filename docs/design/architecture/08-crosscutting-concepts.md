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

The trust model follows from single-provider, closed federation (ADR-0005): one operator, an internal PKI, no cross-org or untrusted-operator trust to negotiate. That shrinks the security surface to four questions — who is calling, what they may touch, who can read the bytes at rest, and how tenants are kept apart. The adversarial view — assets, trust boundaries, the D-server-compromise blast radius, and the storage attack catalog with each attack mapped to its mitigating decision — is the threat model (section 14).

**Authentication — two planes.** *Users and applications* authenticate at the access layer (L1): OIDC / OAuth2 bearer tokens for the Drive / WebDAV / SDK surfaces, S3 Signature V4 (HMAC over per-tenant access keys) for the S3 surface, and OIDC + mTLS for the management API. The gateway is the authentication boundary; nothing below L1 re-authenticates the external principal. *Services* authenticate to each other with **mTLS under the provider CA** (ADR-0005) — zones, D servers, custodians, coordination; identity is the certificate, and there are no shared service secrets on the wire. etcd's own auth is defense-in-depth, never the primary boundary — coordination is network-isolated to the zone's control components and fronted by the mTLS fabric.

**Authorization — enforced at the gateway, recorded in L2.** The authoritative ACLs and sharing grants live in the global namespace (L2), globally linearizable (section 8.1), so a decision is never made against a stale grant. Enforcement is at the **access layer**: the gateway resolves the caller, fetches the cached, version-fenced ACL, and authorizes *before* issuing any storage operation. The metadata store and D servers are **dumb about identity** — they trust that a request which reached them was already authorized (defense in depth: they are network-isolated to the zone, and with encryption on they hold only ciphertext anyway). Fine-grained, relationship-based authorization (Zanzibar-class) is **not** v1; it is a reserved future *consumer* of the system, with its consistency-token hooks already noted (ADR-0018). v1 is POSIX-ish ACLs plus bucket / prefix policy.

**Encryption.** In transit, TLS everywhere — the external APIs and the internal mTLS fabric. At rest, optional **per-tenant envelope encryption** (ADR-0021): the client library encrypts before erasure coding and decrypts after reconstruction, so the metadata store and D servers hold only ciphertext; a provider KMS holds the keys behind a `KeyService` trait, and dropping a key is a **crypto-erase** (the section 6.7 delete fast-path).

**Tenant boundary.** The namespace, quotas, and placement policy are per-tenant (section 5, L2); envelope encryption makes the boundary cryptographic, not merely logical — one tenant's ciphertext is unreadable without that tenant's key. The fuller multi-tenancy model (isolation classes, noisy-neighbour control, quota enforcement points) is scoped separately.

## 8.6 The thick client and the conformance suite

The client library embeds chunking, EC, the commit protocol, and failover — so a second-language client (e.g. a Python SDK for an application team) cannot be a thin shim; it must re-implement all of it identically or it will write data the reference client cannot read, or commit non-atomically. This is why the on-disk format is a real spec with conformance vectors (`specs/`), and why a longer-term option is to expose the thick logic via a Rust core with FFI bindings rather than inviting risky reimplementation.

## 8.7 Compatibility and version skew

A half-upgraded fleet is the normal state during a rolling upgrade (constraint 2.1, scenario Q8), so version skew is designed for, not treated as an incident.

**Two compatibility axes.** *Wire*: every inter-component contract is versioned protobuf (`proto`), evolved by addition — neighbours interoperate across at least a one-version gap, so fields are never repurposed and removals lag deprecation by a release. *On-disk*: the chunk/fragment format carries its own version and EC-scheme id (ADR-0002, ADR-0019); a reader accepts every format version it claims to support, because data outlives the software that wrote it — old-format data is read, never rejected.

**Tested, not hoped.** Skew is exercised under the deterministic-simulation harness (ADR-0009): a simulated zone runs mixed-version nodes from a seed and asserts the commit protocol and read path stay correct across the gap. On-disk compatibility is pinned by conformance vectors per format version (`specs/conformance/`) that every reader must accept, and a mixed-version matrix in CI gates the one-version-gap guarantee (Q8) — the structural complement to the load/fault scenarios in section 10.

## 8.8 Multi-tenancy

The primary deployment is a provider serving many tenants on shared infrastructure (section 1.4), so a **tenant** is a first-class unit — a namespace partition plus an identity domain, carrying its own policy (residency, replication factor, encryption, quotas, rate limits). See ADR-0022.

Multi-tenancy is **logical, not physical** (single-provider, ADR-0005): tenants share zones, D servers, and the metadata tiers. Isolation is the composition of four boundaries, each enforced at a definite point:

- **Namespace** — the L2 global namespace is partitioned per tenant; cross-tenant naming or traversal is impossible by construction (ADR-0020).
- **Data** — per-tenant envelope encryption makes the boundary cryptographic; D servers hold opaque, mixed-tenant ciphertext (ADR-0021).
- **Capacity** — per-tenant quotas checked at admission in the gateway against the tenant's L2 record (section 8.9).
- **Performance** — per-tenant rate limits at the access layer, and placement spreads a tenant across failure domains, containing noisy neighbours.

Enforcement lives at L1 (authentication, rate) and L2 (authorization, quota), never below — D servers and the metadata store stay tenant-oblivious, trusting an admitted request. The storage tier stays dumb; the policy tier stays centralized.

## 8.9 Admission control and backpressure

The system **fails closed** under pressure: a write is admitted only when identity, quota, capacity, and failure-domain room all allow it, and is refused with a clear, retryable signal otherwise — never a silent half-write or a durability corner cut.

- **Quota / rate** — the gateway checks the tenant's quota and rate limit (section 8.8) at admission; over a hard limit it rejects (429-style), backpressuring the client.
- **Zone full** — per-failure-domain utilization is the binding capacity signal (section 7.3): when a domain has no room for the configured EC scheme, placement (L2) redirects new writes to a zone with capacity, or rejects if residency policy forbids the redirect. Running out of room *in one domain* blocks EC writes before total capacity is exhausted.
- **Metadata tier saturated** — backpressure propagates to clients; the metadata tier is shardable (goal 2), so sustained pressure is a scaling signal surfaced by telemetry, not a failure.
- **Repair vs. serve** — see section 6.3: repair reads are throttled below foreground reads, but their priority rises as redundancy falls, so a chunk near its durability floor preempts. Durability (goal 1) outranks latency.

The principle throughout: shed or slow load predictably, surface it on the capacity and durability planes (section 8.3), and never trade correctness for admission.
