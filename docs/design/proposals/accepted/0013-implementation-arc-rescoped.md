---
created: 27.06.2026
type: proposal
status: accepted
supersedes: 0002-implementation-arc.md
author: Eduard Ralph
tracking-issue: TBD
tags:
  - proposal
  - roadmap
  - build-order
---
# Proposal: The implementation arc (rescoped) — ordering of when to build what

> **Supersedes [proposal 0002](0002-implementation-arc.md)** (2026-06-27), the
> original implementation arc, which is retained unchanged as a frozen record.
> This document carries the **current** arc; where the two differ, this one
> governs. The change is a **rescope of Step 2**, not a reordering of what was
> already built (M0–M4 are untouched). See "Changes from proposal 0002" below for
> the old→new milestone mapping a reader needs to reconcile this arc with the
> earlier per-milestone proposals (0001/0003/0004/0005), which are frozen and use
> proposal 0002's original numbering.

> This proposal sits one level above [the Milestone-0 walking-skeleton
> proposal](0001-milestone-0-walking-skeleton.md). 0001 details the *first*
> vertical slice; this document gives the *whole arc* that slice begins —
> the three large steps, the milestones between them, why they are in this
> order, what each retires in risk, and which of them are natural stopping or
> release points. It records *direction and ordering*, not dates: this is an
> open-source project, so the sequence is the commitment and the timing is not.
>
> The *why* of the architecture lives in the ADRs and the architecture
> overview; this document only orders the building of it. It is the contribution
> roadmap (a newcomer reads it to learn where to start) and the artifact an
> operational partner reads to understand what exists at each boundary.

### Changes from proposal 0002

This arc **rescopes Step 2**. The original arc treated the internal trust fabric,
encryption at rest, and the operator surface as *features that attach* to the
core rather than milestones; this arc promotes them, because a single zone is not
*production-usable* without them — only feature-complete. Concretely:

- **Step 2 extended** with three new milestones: the internal trust fabric
  (**M5**, step-ca / ADR-0036/0025), encryption at rest (**M6**, KeyService /
  ADR-0026/0021), and the operator surface (**M8**, CLI + portal / ADR-0013).
- **The Step-2 ★ release point moved from M4 to M8** ("usable within a single
  datacenter"). M4 is now a soft stopping point — a feature-complete *data plane*,
  not yet a deployable *product*.
- **Failover split.** The original M7 ("failover & DR, drilled") drilled
  *cross-zone* zone loss. This arc separates it: **M7** now drills
  *single-datacenter* failure recovery (Step 2), and the *cross-zone* zone-loss DR
  drill becomes **M11** (the Step-3 ★).

The old→new milestone mapping (for reconciling the frozen proposals 0001/0003/0004/0005,
which use the original numbers):

| Proposal 0002 (original) | This arc |
|--------------------------|----------|
| M0–M4 | unchanged (M0–M4) |
| — | **M5** internal CA (step-ca) — new |
| — | **M6** encryption at rest (KeyService/KMS) — new |
| M7 failover & DR (cross-zone) | **M7** failover & DR (single-datacenter), with the cross-zone part → M11 |
| — | **M8** manageability (CLI + portal) — new, Step-2 ★ |
| M5 cross-zone replication | **M9** |
| M6 global control plane | **M10** |
| M7 failover & DR (cross-zone part) | **M11**, Step-3 ★ |

## Motivation

A from-scratch storage foundation can be built in many orders, most of them
wrong. Bottom-up (all the storage servers and erasure coding first) defers the
one thing that justifies the project — the provably-atomic commit protocol —
until last, and risks discovering late that the central claim is hard. This
proposal commits to the opposite: a **vertical slice that proves the
differentiator first, then widens by risk** (architecture §4.2, §9; ADR-0009).

The ordering principle throughout is **risk retired, not features delivered**.
Each milestone is placed by what uncertainty it removes, and earlier milestones
de-risk the project's load-bearing claims before later breadth is built on top
of them. A second principle makes the arc safe to pursue incrementally: **every
milestone is a coherent stopping point, and two are release points** — so the
project can pause, publish, or seek an operational owner at a real boundary
rather than being all-or-nothing.

## Design

### The three steps

| Step | Goal | Ends with |
|------|------|-----------|
| **1. Prove the differentiator** | One atomic write and read, end to end, in one process, shown atomic under fault injection in simulation. | The central claim is no longer a claim. |
| **2. A real single-zone system** | Widen the proven slice, in risk order, into a self-hostable, erasure-coded, atomically-consistent — and then *secured, encrypted, and operable* — object store within one datacenter. | **Release point:** the first genuinely useful product, usable within a single datacenter. |
| **3. A multi-region sovereign substrate** | Add the cross-zone layers that turn independent single-datacenter systems into one geographically distributed foundation. | **Release point:** the mission artifact a provider builds a product on. |

Step 3 is the longest and most trust-bound, and should only be pursued in full
with a concrete operational owner committed to adopting and hardening it (the
success pattern; the speculative-build-and-hope pattern is the one to avoid).

### The milestones

Each milestone below carries: **proves** (what becomes true), **retires** (what
risk it removes), **needs** (what must exist first), **done when** (definition of
done), and whether it is a **stopping/release point**.

#### Prerequisite — On-disk format, specified

- **Proves:** the one contract that must outlive software is fixed before code is
  written against it (ADR-0002).
- **Retires:** the risk of building the `chunk-format` crate against an
  unspecified layout and having to rewrite readers.
- **Needs:** nothing; it is the first work item.
- **Done when:** ✅ **the byte layout is specified and decided** —
  `specs/chunk-format/v1.md` is complete (no `[TO BE SPECIFIED]`), with the
  trade-offs recorded in ADR-0019. What remains is mechanical and lands with the
  `chunk-format` crate at M0: at least one conformance vector in
  `specs/conformance/`. The format stays **v0/unstable** until validated (the
  spec's own rule); `v1` is stamped only after a second independent reader or a
  sustained fault-injection run.
- **Stopping point:** no — it is a prerequisite, not a deliverable on its own.

This is the only artifact detailed spec-first up front. Everything else is
implementation-first behind versioned protobuf (ADR-0002); see "What this
proposal deliberately does not order" below.

#### M0 — Walking skeleton *(Step 1)*

- **Proves:** the commit protocol — the entire differentiator — works end to end.
- **Retires:** the project's single largest risk: that atomic commit across a
  separate metadata and data path is hard to get right.
- **Needs:** the format spec (above); the workspace scaffold and `testkit`.
- **Done when:** a file written via S3 PUT reads back byte-identical via GET, and
  the commit is proven atomic under fault injection in simulation, reproducible
  from a seed; commit-protocol property tests green. (Full detail:
  [proposal 0001](0001-milestone-0-walking-skeleton.md).)
- **Stopping point:** yes — a credible proof-of-concept and a publishable
  artifact (the idea, demonstrated and verified) even if nothing follows.

Scope is exactly the layers one real operation touches: S3 PUT/GET → client
(chunk + commit) → redb metadata → filesystem chunk store → in-memory
coordination, plus the DST harness. No EC, no networked servers, no custodians,
no second zone.

#### M1 — Erasure coding *(Step 1 → Step 2)*

- **Proves:** real Reed-Solomon encode/decode and reconstruction-from-any-*k*
  works in the client data path.
- **Retires:** the correctness risk in the hottest, most failure-critical loop in
  the system, validated against the already-working slice rather than in the
  abstract.
- **Needs:** M0 (a correct commit path to encode into and read back from).
- **Done when:** files are stored as RS(k,m) fragments and read back by
  reconstructing from any *k*; a fragment loss is survived without read error;
  EC benchmarks enter CI.
- **Stopping point:** soft — the system is now space-efficient but still
  single-process; not yet a deployable product.

#### M2 — Networked D servers *(Step 2)*

- **Proves:** the direct client→storage-server data path works over the network
  (the basis of the throughput-scaling claim).
- **Retires:** the risk that the in-process abstraction hid a problem the real
  gRPC `ChunkStore` will expose; proves the trait seam is real.
- **Needs:** M1 (fragments to write); the `proto` shapes from M0.
- **Done when:** the in-process filesystem store is replaced by networked gRPC
  D servers behind the unchanged `ChunkStore` trait; the client writes fragments
  directly to them in parallel; end-to-end tests pass over the network.
- **Stopping point:** soft.

#### M3 — Custodians *(Step 2)*

- **Proves:** the system maintains its own durability — GC, scrub, reconstruction,
  rebalance — and reports it.
- **Retires:** the *second* home of correctness risk (the background repair
  loops, where bugs are silent corruption), now with real networked data to
  maintain. Also retires the "durability is asserted, not observable" risk.
- **Needs:** M2 (real distributed data to maintain and repair).
- **Done when:** a lost D server's fragments are reconstructed onto healthy
  servers in correct failure domains via the commit-point-atomic pattern;
  scrubbing detects injected bit-rot; durability-plane telemetry is emitted from
  the custodians' first commit (ADR-0011).
- **Stopping point:** soft — but this is the milestone that makes single-zone
  *trustworthy*, so it gates the Step-2 release.

#### M4 — Production metadata backend *(Step 2)*

- **Proves:** pluggability is real — the `MetadataStore` trait survives swapping
  embedded redb for distributed TiKV under load.
- **Retires:** the risk that the trait abstraction leaked and a real distributed
  backend needs a refactor rather than a composition change.
- **Needs:** M3 (a complete single-zone system to run on the new backend).
- **Done when:** the same system runs on TiKV behind the unchanged trait;
  multi-key atomic directory operations hold under the distributed store; the
  swap is a `server`-crate composition change, not a refactor.
- **Stopping point:** soft. M4 completes the single-zone **data plane** —
  feature-complete and pluggable — but it is **not yet production-deployable**:
  it has no internal trust fabric, no encryption at rest, no drilled local
  failure recovery, and no operator surface. Those land in M5–M8, and the
  Step-2 ★ release point moves to **M8**, where the single-zone product becomes
  genuinely usable within a datacenter.

#### M5 — Internal CA (step-ca) *(Step 2)*

- **Proves:** the service fabric authenticates *itself* — every internal RPC is
  mutually authenticated under a provider-operated CA, and peers are authorized
  by a first-class, SPIFFE-shaped identity rather than by network position.
- **Retires:** the internal-trust enforcement risk (a wire promoted to mTLS that
  still lets any authenticated component do anything; §14.9), and the risk that
  the reserved SPIRE upgrade silently closes if authorization matches raw
  certificate fields.
- **Needs:** M2 (a networked fabric to authenticate); the slice can begin against
  the tail of M4.
- **Done when:** step-ca issues short-lived, auto-rotated certificates behind a
  `CertificateAuthority`/`IdentityProvider` seam; mTLS is required and
  fail-closed (no plaintext fallback); authorization is keyed on an abstract
  `PeerIdentity { role, zone, instance }` with a standing guard test; least
  authority holds per component (a valid identity is still denied an out-of-role
  operation). The dev profile uses a self-signed CA behind the same seam (ADR-0036,
  ADR-0025).
- **Stopping point:** soft.

#### M6 — Encryption at rest (KeyService / KMS) *(Step 2)*

- **Proves:** envelope encryption-at-rest works behind the `KeyService` trait
  against a real KMS, with KEK material never leaving the KMS.
- **Retires:** the KMS-as-a-new-failure-domain risk (a KMS outage or a lost KEK is
  catastrophic; §11) and the risk that the `KeyService` seam leaks a vendor API
  rather than the envelope contract.
- **Needs:** M5 (the trust fabric the KMS authenticates against) and the format's
  already-reserved key-version hooks (from M0).
- **Done when:** per-object DEKs are wrapped by per-tenant KEKs in OpenBao
  (Transit); the read/write paths encrypt and decrypt with bounded DEK caching
  and fail-closed on KMS unavailability; KEK custody is residency-pinned;
  crypto-erase by KEK destruction is gated on retention/hold state; encryption is
  off by default in dev (ADR-0026, ADR-0021).
- **Stopping point:** soft.

#### M7 — Failover and disaster recovery, single-datacenter *(Step 2)*

- **Proves:** the system survives and recovers from node / disk / rack loss
  *within one datacenter*, drilled rather than asserted.
- **Retires:** the risk that single-zone durability holds in steady state but not
  through local failure and recovery.
- **Needs:** M3 (the custodian reconstruction this extends) and M6 (so recovery
  operates on the secured, encrypted store).
- **Done when:** node / disk / rack loss is detected and under-replicated data
  reconstructed from survivors in correct failure domains; the local failover and
  recovery ordering is written as a runbook and exercised in a drill.
- **Stopping point:** soft. (The cross-zone, zone-loss DR drill — home-zone
  failover via the version high-water mark, ADR-0015 — is a separate Step-3
  milestone, **M11**.)

#### M8 — Manageability: CLI + portal *(Step 2)*

- **Proves:** the single-zone system is *operable* — an operator can run day-2
  operations, administer tenants, and observe the durability / capacity / request
  planes through a management CLI and web portal over the API-first management
  plane.
- **Retires:** the "built but not operable" risk — a correct substrate that no
  operator can actually run in production.
- **Needs:** M5 (the management plane reuses the CA / identity fabric for OIDC +
  mTLS auth), M7 (failure recovery is an operator workflow), and M3 (the telemetry
  to surface).
- **Done when:** a management CLI and web portal drive desired state +
  reconciliation status over gRPC/REST (ADR-0013); drain/decommission, rolling
  upgrade, backup/restore, and per-tenant policy administration work; the three
  observability planes and the append-only audit log are surfaced (ADR-0011,
  ADR-0012). First real implementation of ADR-0011/0012/0013 (proposal 0008).
- ★ **Release point:** end of Step 2. The result — a self-hostable, EC-efficient,
  atomically-consistent, **secured, encrypted, and operable** single-zone object
  store — is the first genuinely useful product, **usable within a single
  datacenter**, worth announcing and deploying even if Step 3 never follows. A
  single zone of this design already earns adoption and contributors.

#### M9 — Cross-zone replication, L3 *(Step 3)*

- **Proves:** committed chunks replicate between zones off the foreground write
  path, copy-then-commit-the-replica-record, with no half-copied replica visible.
- **Retires:** the risk in async geo-replication and the replica-catalog
  commit-ordering.
- **Needs:** M8 (a complete, secured, operable single zone — multi-zone is
  meaningless until single-zone is solid and usable).
- **Done when:** a committed file's chunks are asynchronously replicated to a
  second zone and become readable only once their catalog record commits;
  sync-N-zone is available as a per-tenant opt-in.
- **Stopping point:** soft.

#### M10 — Global control plane, L2 *(Step 3)*

- **Proves:** the geo-distributed namespace and placement (ADR-0020), and the
  home-zone authority consistency contract, operate across zones.
- **Retires:** the risk in the global, strongly-consistent namespace and in the
  per-file home-zone routing.
- **Needs:** M9 (replicated data for the namespace to point across zones to).
- **Done when:** the namespace is globally consistent across zones; placement
  assigns home zones and replica sets by policy; per-session read-your-writes and
  monotonic reads hold per the contract (ADR-0015).
- **Stopping point:** soft.

#### M11 — Cross-zone failover and disaster recovery, drilled *(Step 3)*

- **Proves:** the consistency contract survives real zone loss, and the recovery
  ordering works in practice, not just on paper.
- **Retires:** the final and least-forgiving risk — that the guarantees hold in
  the steady state but not through disaster. (Distinct from M7, which drilled
  *single-datacenter* failure recovery; this is the *cross-zone* zone-loss case.)
- **Needs:** M10 (a working multi-zone system to fail and recover).
- **Done when:** zone loss is detected and under-replicated files re-replicated
  from survivors; monotonic reads survive home-zone failover via the version
  high-water mark; the DR ordering (L5 → L2/L4 → L3) is written as a runbook and
  exercised in a drill.
- ★ **Release point:** end of Step 3. The result is the mission artifact: a
  multi-region sovereign storage substrate a provider can build a product on.

### Dependency graph

The arc is mostly linear by construction (each milestone needs the previous
one's result to validate against), with one item off to the side:

```
[format spec] ─► M0 ─► M1 ─► M2 ─► M3 ─► M4 ─► M5 ─► M6 ─► M7 ─► M8 ★─► M9 ─► M10 ─► M11 ★
                 │                                                │
                 └─ testkit/DST harness ───────────────────────────┴─ grows with every step
```

The two ★ are the release points: **M8** ends Step 2 (a usable single-datacenter
product) and **M11** ends Step 3 (the multi-region mission artifact). M5–M8
(trust fabric, encryption, single-DC failover, manageability) are what turn M4's
feature-complete *data plane* into a deployable *product*.

- The format spec is the only true *prerequisite* that is not itself a runtime
  deliverable.
- The DST harness (`testkit`) attaches at M0 and is extended at every subsequent
  milestone — it is not a one-time build but a growing dependency (ADR-0009).
- Linearity is deliberate at this stage (small team; each step validates the
  next). Genuine parallelism appears only once there are multiple contributors —
  at which point M2 (networked D servers) and M3 (custodians) have the most
  independent surface and are the natural first split.

### Specification prerequisites per step

What must be *specified* (not merely built) before each step, respecting the
spec-first-only-for-the-format rule (ADR-0002):

| Step / milestone | Specification needed first |
|------------------|----------------------------|
| Format spec → M0 | The on-disk chunk/fragment format, finalized in `specs/chunk-format/v1.md` (the one normative artifact). |
| M0 | The commit-protocol *contract* (its invariants) stated in `architecture/` — not a separate spec; it firms up through M0 and is verified by DST. |
| M1 | EC-scheme identifiers recorded in the format spec (already required by the format prerequisite). |
| M2–M4 | None up front — wire/RPC surfaces and the metadata schema are implementation-first behind versioned protobuf, by decision (ADR-0002). |
| M5–M8 | None up front — the trust, key-management, and management contracts already exist as ADRs (ADR-0036/0025 trust, ADR-0026/0021 keys, ADR-0013 management); the wire and trait surfaces are implementation-first behind those contracts. |
| M9–M11 | None up front — the cross-zone protocols are implementation-first; the consistency *contract* they honor already exists (ADR-0015). |

### Open-questions triage

The `[OPEN]` items in the docs split into two kinds: those to **resolve now by
reasoning** (which then become ADRs), and those **deliberately deferred to
measurement** (which stay open, with the reason recorded, because deciding them
from an armchair would be false precision).

| Open question | Disposition | Reason |
|---------------|-------------|--------|
| Checksum algorithm for the fragment header (crc32c vs. blake3), field widths, endianness | **✅ Resolved → ADR-0019** | A reasoned decision (integrity-only vs. cryptographic; performance): crc32c default, blake3 reserved, little-endian, fixed-width — folded into the format spec. |
| EC-scheme identifier encoding in the format | **✅ Resolved → ADR-0019** | Fixed before any fragment is written: `ec_scheme_type` / `ec_k` / `ec_m` / `ec_fragment_index` in the header. |
| Small-file **inline threshold** (the byte size below which data is inlined in metadata) | **Defer to measurement** | Genuinely empirical — the right threshold depends on the metadata tier's behavior under a real small-file workload (M3–M4). Picking a number now is false precision. Record *why* deferred. |
| Chunk / stripe size | **Defer to measurement** | Same: a throughput/overhead trade best set against M1 benchmarks. |
| Whether minimal S3 lives in `server` or warrants a `gateway-s3` crate at M0 | **Resolve now → trivial, into 0001** | A crate-boundary call, cheap to decide; combined `server` for M0, split later (ADR-0016 evolution rule). |
| redb key encoding / dirent name normalization | **Resolve during M0** | An implementation detail of the first metadata backend; settle while building, record in-code and in the architecture doc. |

Each "resolve now" row that is a genuine architectural decision becomes an ADR in
the 0018+ range; the format-internal ones fold directly into
`specs/chunk-format/v1.md` rather than getting their own ADR.

### What this proposal deliberately does not order

The arc orders the **risk-retiring core** (M0–M11). Several *decided* capabilities are deliberately **not** milestones, because they attach to the core as features rather than gating it as uncertainties:

- **Multi-tenancy** (ADR-0022) — namespace / quota / rate isolation. Its enforcement points (the L1 gateway, L2) arrive with those layers (M2+, M10); the model layers on, it does not reorder the arc.
- **The hyperscale identity consumer** (ADR-0018) — a *consumer* of the substrate, reserved-only; built, if ever, on top of a finished Step 3.
- **Wire / RPC surfaces and metadata schemas** — implementation-first behind versioned protobuf (ADR-0002), discovered correctly only by building them; never specified up front.

The rule: if a capability does not *retire a load-bearing risk*, it attaches to a milestone — it is not a milestone of its own.

**Promoted to milestones (relative to proposal 0002).** Two capabilities the
original arc (proposal 0002) listed here as "attach, not order" are now milestones, because the
single-datacenter *product* (not just the data plane) cannot be trusted or run
without them:

- **Encryption at rest** (ADR-0021/0026) → **M6**. The format hooks (`flags`,
  `encryption_scheme`, the header extension, key-version) are still reserved from
  M0, but the `KeyService` contract and a real KMS are a load-bearing failure
  domain (§11) that a production single zone must retire before release, not an
  optional add-on — so it is ordered.
- **The management surface** (ADR-0013) → **M8**. The durability-plane *telemetry*
  still emits from M3, but the operator CLI + portal that make the zone *runnable*
  in production gate the Step-2 release; "operable" is a risk to retire, not a
  later convenience. (The polished web UI / dashboards beyond the floor remain
  deferred.) Internal service-to-service trust (ADR-0036/0025) is likewise now
  **M5**, for the same reason: a networked single zone is not production-safe
  while internal dials are plaintext (§14.9).

## Alternatives considered

- **Bottom-up build** (storage servers + EC first, commit protocol last):
  rejected (§4.2, ADR-0009). It defers the differentiator and the largest risk to
  the end and gives no early proof of the central claim.
- **Specify everything up front** (full spec-first): rejected (ADR-0002). Weeks of
  documents before runnable code, and most interfaces are discovered correctly
  only by building them; spec-first is confined to the on-disk format, which is
  the only contract that must outlive software.
- **Date-driven roadmap:** rejected. An open-source project cannot honestly commit
  dates this early, and a slipped public timeline misleads the contributors and
  partners it is meant to attract. The arc commits to *order*, not schedule.

## Graduation criteria

This proposal is **accepted** (moves `draft/ → accepted/`) when the three-step
shape and the milestone ordering are agreed and no milestone's *needs* edge is
disputed. It is **implemented** in the sense that matters for a roadmap when each
milestone has a corresponding tracking issue and definition of done; it is
superseded by a revised arc if the ordering is found wrong in practice (a
milestone that does not retire the risk it claims is the signal to re-order).

## Backward compatibility

- **On-disk format:** the format spec is fixed before M0 but stays v0/unstable;
  no data exists to migrate until it is stamped `v1`.
- **Deferred-with-reserved-seats** are honored from the milestone where retrofit
  is expensive, not when the feature ships: append/CAS/watch hooks from M0
  (ADR-0007), the `meta:version` consistency fence reserved from M0 (ADR-0015),
  trait seams for TiKV/etcd/openraft from M0.
- **Public API / deployments:** none until the Step-2 release point (M8); nothing
  to stay compatible with before then. (M4 completes the data plane but is a soft
  stopping point, not a public-API commitment.)

## Open questions

- Does Step 3 begin only once a concrete operational owner is committed, or is a
  reference multi-zone implementation pursued speculatively to demonstrate the
  full design? (A governance/strategy question, not a technical one — recorded
  here because it gates the largest investment in the arc.)
- At which milestone does the commit-protocol *contract* graduate from
  descriptive architecture to a normative `specs/commit-protocol/` document, if
  ever — i.e. when a second-language client must honor it as a frozen contract
  (architecture §8.6 flags this as a future possibility, not a current need)?
