---
created: 17.06.2026 19:53
type: proposal
status: superseded
superseded-by: 0013-implementation-arc-rescoped.md
author: Eduard Ralph
tracking-issue: TBD
tags:
  - proposal
  - roadmap
  - build-order
---
# Proposal: The implementation arc — ordering of when to build what

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
| **2. A real single-zone system** | Widen the proven slice, in risk order, into a self-hostable, erasure-coded, atomically-consistent object store within one datacenter. | **Release point:** the first genuinely useful product. |
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
- ★ **Release point:** end of Step 2. The result — a self-hostable, EC-efficient,
  atomically-consistent single-zone object store — is the first genuinely useful
  product, worth announcing and deploying even if Step 3 never follows. A single
  zone of this design already earns adoption and contributors.

#### M5 — Cross-zone replication, L3 *(Step 3)*

- **Proves:** committed chunks replicate between zones off the foreground write
  path, copy-then-commit-the-replica-record, with no half-copied replica visible.
- **Retires:** the risk in async geo-replication and the replica-catalog
  commit-ordering.
- **Needs:** M4 (a solid, production-backed single zone — multi-zone is
  meaningless until single-zone is solid).
- **Done when:** a committed file's chunks are asynchronously replicated to a
  second zone and become readable only once their catalog record commits;
  sync-N-zone is available as a per-tenant opt-in.
- **Stopping point:** soft.

#### M6 — Global control plane, L2 *(Step 3)*

- **Proves:** the geo-distributed namespace and placement (ADR-0020), and the
  home-zone authority consistency contract, operate across zones.
- **Retires:** the risk in the global, strongly-consistent namespace and in the
  per-file home-zone routing.
- **Needs:** M5 (replicated data for the namespace to point across zones to).
- **Done when:** the namespace is globally consistent across zones; placement
  assigns home zones and replica sets by policy; per-session read-your-writes and
  monotonic reads hold per the contract (ADR-0015).
- **Stopping point:** soft.

#### M7 — Failover and disaster recovery, drilled *(Step 3)*

- **Proves:** the consistency contract survives real zone loss, and the recovery
  ordering works in practice, not just on paper.
- **Retires:** the final and least-forgiving risk — that the guarantees hold in
  the steady state but not through disaster.
- **Needs:** M6 (a working multi-zone system to fail and recover).
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
[format spec] ──► M0 ──► M1 ──► M2 ──► M3 ──► M4 ★──► M5 ──► M6 ──► M7 ★
                   │                                  │
                   └─ testkit/DST harness ────────────┴─ grows with every step
```

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
| M5–M7 | None up front — the cross-zone protocols are implementation-first; the consistency *contract* they honor already exists (ADR-0015). |

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

The arc orders the **risk-retiring core** (M0–M7). Several *decided* capabilities are deliberately **not** milestones, because they attach to the core as features rather than gating it as uncertainties:

- **Encryption at rest** (ADR-0021) — optional, per-tenant. Its format hooks (`flags`, `encryption_scheme`, the header extension) are reserved from M0; the feature itself is built when a tenant needs it, never on the critical path.
- **Multi-tenancy** (ADR-0022) — namespace / quota / rate isolation. Its enforcement points (the L1 gateway, L2) arrive with those layers (M2+, M6); the model layers on, it does not reorder the arc.
- **The hyperscale identity consumer** (ADR-0018) — a *consumer* of the substrate, reserved-only; built, if ever, on top of a finished Step 3.
- **Observability dashboards and the management UI** — the durability-plane *telemetry* is emitted from M3 (the custodians, ADR-0011); dashboards and the web UI (ADR-0013) are cheap to add later and carry no ordering risk.
- **Wire / RPC surfaces and metadata schemas** — implementation-first behind versioned protobuf (ADR-0002), discovered correctly only by building them; never specified up front.

The rule: if a capability does not *retire a load-bearing risk*, it attaches to a milestone — it is not a milestone of its own.

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
- **Public API / deployments:** none until the Step-2 release point (M4); nothing
  to stay compatible with before then.

## Open questions

- Does Step 3 begin only once a concrete operational owner is committed, or is a
  reference multi-zone implementation pursued speculatively to demonstrate the
  full design? (A governance/strategy question, not a technical one — recorded
  here because it gates the largest investment in the arc.)
- At which milestone does the commit-protocol *contract* graduate from
  descriptive architecture to a normative `specs/commit-protocol/` document, if
  ever — i.e. when a second-language client must honor it as a frozen contract
  (architecture §8.6 flags this as a future possibility, not a current need)?
