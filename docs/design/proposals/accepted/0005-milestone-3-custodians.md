---
created: 21.06.2026 02:35
type: proposal
status: accepted
author: Eduard Ralph
tracking-issue: "#147"
tags:
  - proposal
  - milestone-3
  - implementation-plan
  - custodians
---
# Proposal: Milestone 3 — custodians (implementation plan)

> The implementation plan for the third *widening* step of the [implementation
> arc][p2] (proposal 0002). [Proposal 0004][p4] built (in plan) the networked
> data path — the client writes a chunk's *n* fragments directly and in parallel
> to distinct gRPC D servers and reads back any *k* that arrive first — but the
> system written that way **cannot yet keep what it stores**: a D server that
> dies after a clean commit silently lowers a chunk's redundancy, and nothing
> notices, rebuilds, or reports it. M3 adds the **custodians** — the background
> maintenance plane ([§4.1][s4]; [§5][s5] L4) that **garbage-collects, scrubs,
> reconstructs, and rebalances**, and **emits the durability telemetry that makes
> the guarantee observable instead of merely asserted** ([ADR-0011][a11]). It
> records *how* M3 is built; the *why* of the durability design lives in the
> architecture and the ADRs it references ([§6.3][s6], [§8.3][s8], [§8.9][s8],
> [ADR-0011][a11], [ADR-0012][a12]). M3 is implementation-first behind versioned
> protobuf and the existing traits ([ADR-0002][a2]): the maintenance RPCs and the
> placement record are wire/metadata contracts discovered by building them — **no
> new spec** is required (spec-first effort stays reserved for the on-disk format).

## Motivation

M3 proves that **the system maintains its own durability — GC, scrub,
reconstruction, rebalance — and reports it** ([§9][s9]; arc M3). M0 retired
atomic-commit-across-paths; M1 retired the coding math; M2 retired the networking.
Each of those lives on the **request plane** — the path a `PUT`/`GET` takes. M3
opens the **second home of correctness risk**: the **background repair loops**,
where a bug is not a failed request but **silent corruption or silent data
loss** ([§11][s10] risk table; [§13.1][s10]). It is de-risked **against the
already-working M2 slice** — there is now real, distributed, erasure-coded data
to lose and to rebuild — so a failure isolates to the maintenance plane and not to
the surrounding request path.

The ordering principle of the arc is **risk retired, not features delivered**.
M3's risk is two-fold and both halves are load-bearing:

- **Repair correctness.** A reconstruction that is *not* atomic, or a GC that
  reclaims a *referenced* fragment, corrupts data that the request plane reports
  as healthy. Every recovery action must be **commit-point-atomic** — recompute
  and re-place the fragment, then **one** atomic metadata mutation flips the
  chunk's location; only *after* it commits is the old fragment collectable. A
  crashed repair leaves **garbage (collected later), never corruption**
  ([§6.3][s6]).
- **Observability.** "A storage system silently below its redundancy floor
  reports all-green on the request plane" ([§8.3][s8]). Durability is invisible
  unless designed in; M3 retires the **"durability is asserted, not observable"**
  risk by making the custodians emit the durability plane as a **first-class
  output** from their first commit ([ADR-0011][a11]).

M3 is a **soft stopping point** but **the milestone that makes a single zone
*trustworthy***, so it **gates the Step-2 release** (arc): M4's production
metadata backend is worth deploying only on a zone that can already keep, repair,
and report on its own data. It is also — with M2 — the part of the arc with **the
most independent surface**, the natural first **parallel split** for more than one
contributor ([§9][s9]; arc dependency graph): once the foundation slices (M3.1–M3.3)
land, GC, scrub, reconstruction, and rebalance are largely independent loops.

## Design

### Scope boundary

**In scope** — exactly what retires the durability-maintenance risk:

- A new **`custodian` crate** (L4, [§5][s5]) — the home of the four maintenance
  loops, leader-elected to a **single active custodian per zone** via the existing
  `Coordination::elect_leader` seam, fenced by its leadership token.
- **Finalizing the placement record** (M2's deferred open question, [0004][p4]
  Open questions): the committed chunk map records, per fragment, the **stable
  D-server** that holds it; the write path records it at commit, the read path
  consumes it (retiring M2's stateless `index % n`), and the custodians read and
  rewrite it.
- **`ChunkStore` enumerate + delete** — the two affordances M1/M2 deliberately
  left out ([0003][p3]: "a `list`/`has` affordance may be added when M2's
  networked discovery needs it"): `list_fragments` (scrub must walk what a D
  server holds) and `delete_fragment` (GC must reclaim orphaned bytes — none
  exists today).
- A **zone-local failure-domain-aware selector** (rack/power/switch labels,
  [§7.3][s7]) shared by the write fan-out and custodian re-placement, so a chunk's
  *n* fragments occupy *n* **distinct failure domains** — the first point at which
  the **RS(6,3) durability math is claimable** within a single zone.
- The **four custodian loops**: **GC** (expired pending-ledger leases + orphaned
  fragments, after a reader-safe grace window), **scrub** (checksum verification →
  bit-rot detection), **reconstruction** (any-*k* → recompute → place in correct
  failure domains → commit-point-atomic location update), and **rebalance**
  (drain/decommission evacuation preserving the failure-domain invariant).
- **Repair-vs-serve dynamic priority** ([§8.9][s8]): repair reads throttled below
  foreground reads, but priority **rising as redundancy falls** so a chunk one
  fragment from its *m*-th loss preempts serving — attached to the **read-retry
  reserved seat** M2 left in the parallel read path ([0004][p4]), not a redesign.
- The **durability plane** ([ADR-0011][a11], [ADR-0012][a12]): the custodians emit
  **under-replicated chunk count, repair-queue depth, time-to-repair distribution,
  scrub coverage, scrub-detected corruption rate**, plus an **append-only
  audit/event log** of significant transitions, instrumented with **OpenTelemetry**
  (`tracing` + `tracing-opentelemetry`) over **both** a Prometheus-scrapeable
  endpoint **and** OTLP push.
- The **declarative reconciliation hook** ([ADR-0011][a11] rule 2): operators
  set desired state (drain / decommission a D server) and the custodians reconcile
  toward it, surfacing "changed" vs "satisfied" as distinct observable moments.
- **Tier-0 DST** (the correctness authority) extended with custodian properties,
  **Tier-1** disk-fault injection (dm-flakey / dm-error) for the scrub/checksum
  path and **Jepsen** consistency over repair, and **Tier-2** single-node
  kill-and-reconstruct ([§13][s10]).

**Out of scope** — deferred to the milestone that actually retires its risk, its
hooks present where retrofit is expensive:

- **Degraded-write tolerance** (commit with < *n*, custodians backfill; re-place a
  dead endpoint mid-write). M2's [0004][p4] hand-off note lists this "→ M3", but
  the **architecture body does not**: the write protocol commits the **full
  fragment set** ([§6.1][s6]) and the admission model **fails closed** — "never a
  silent half-write" ([§8.9][s8]). M3 therefore **keeps the write path fail-closed**
  and has the custodians repair only **post-commit** degradation (a fragment lost
  *after* a clean commit) — which **is** the durability-maintenance risk M3 exists
  to retire, and which the architecture **does** specify. Commit-below-*n* would
  reverse a load-bearing invariant for no risk M3 owns; it is left to a future
  slice **gated by its own ADR**, if ever. (See *Alternatives* and *Open questions*.)
- **The L2 placement-*policy* service** — per-tenant scheme selection, the global
  capacity view, cross-zone placement → **L2 / M6** ([§7.3][s7]). M3 owns the
  **custodian half**: enforcing domain spread on *write and repair* using
  zone-local labels. The abstraction is kept deliberately thin (an opaque domain
  id per D server + a distinctness invariant) so M6's policy service does not
  inherit a throwaway model.
- **Cross-zone replication and its telemetry** → **M5** (L3). "Replication lag per
  zone pair" is a named [ADR-0011][a11] durability metric, but **no zone pair
  exists at single-zone M3** — it is explicitly deferred to M5, not silently
  dropped.
- **Production metadata backend (TiKV)** → M4. M3 runs on the embedded redb store;
  the custodian's atomic mutations go through the **unchanged** `MetadataStore`
  trait, so the M4 swap stays a composition change.
- **Polished management surface** — dashboards (`deploy/grafana/`), alerting, the
  web UI ([ADR-0013][a13]), and any management CLI beyond the reconciliation hook.
  [ADR-0011][a11]/[§9][s9]: the **hooks** exist from when custodians exist;
  dashboards are "cheap to add later and carry no ordering risk" ([p2][p2]).
- **Small-file inlining and the chunk/stripe-size decision** → still
  deferred-to-measurement ([p2][p2]); M3–M4 is merely the window in which a real
  workload makes them measurable, not where the mechanism is built.
- **Tier-3 multi-region hardware** → M5+. M3 must **not** stand up the rented rig
  ([§13.4][s10]).

### What carries over from M0–M2, unchanged

M3 adds a **maintenance plane** *beside* the request plane; it does **not** touch
the commit guarantee or the data path that M0–M2 proved:

- The four-phase write/commit protocol ([§5][s5]): intent → data path → commit →
  release. The commit point is still **one `MetadataStore::commit` of a
  version-conditional `WriteBatch`** (`crates/traits/src/lib.rs:96-179`);
  reconstruction **reuses this exact mechanism** for its atomic location update, so
  there is no new commit machinery — a repair is "a write of a fragment that
  already has bytes."
- The redb metadata model — `inode:` / `dirent:` / `pending:<chunk_id>` /
  per-inode `version` CAS (`crates/core/src/metadata.rs:23-93`). The reserved
  `meta:version` global fence ([§ADR-0015][a15]) stays reserved; the per-inode
  version carries the reconstruction CAS, exactly as it carries a write's.
- The EC coding loop (`reed-solomon-simd`, encode → *n* shards, reconstruct from
  any *k*, [0003][p3]) is the **engine the reconstruction loop calls** — M3 adds no
  new coding math; it adds the loop that *invokes* the decoder on a survivor set
  and writes the rebuilt fragment back.
- The gRPC `ChunkStore` trait and transport ([0004][p4]) are unchanged in
  *contract*; M3 **extends** the service additively (`list_fragments`,
  `delete_fragment`) behind versioned protobuf ([ADR-0002][a2]), the same
  fields-never-repurposed rule M2 followed ([§8.7][s8]).
- The any-*k* parallel read with re-read-on-failure ([0004][p4]); M3 attaches
  repair-throttle priority to its **existing** re-read seat, and feeds its
  read-time checksum failures into the same reconstruction queue scrub feeds.

### The placement record (the first load-bearing change)

M2 routes a fragment **statelessly**: `FanoutChunkStore::route(index) = stores[index
% n]` (`crates/chunkstore-grpc/src/fanout.rs:51-53`), and its own docstring records
the debt — "the read resolves a fragment back to where the write put it without a
placement record (**the recorded-placement question is settled at M3**)"
(`fanout.rs:9-12`). The committed chunk map carries no location: `ChunkRef { id,
scheme, len }` (`crates/core/src/metadata.rs:73-80`). The moment a custodian *moves*
a fragment, `index % n` is wrong — so M3 must record placement, and this proposal
**resolves M2's open question in the affirmative: placement is recorded at commit.**

- **Shape.** The chunk map records, per fragment index, the **stable D-server id**
  that holds it (a per-`ChunkRef` placement vector, length *n*). Recorded at the
  write commit, consumed by the read path (replacing `index % n`), and read +
  rewritten by the custodians.
- **Stable D-server identity.** A D server is referenced by a **stable id**, not
  its endpoint URL — URLs change (rebind / NAT) and a placement record keyed on a
  URL would rot. Registration through `Coordination` ([§5][s5] L5) carries `{ id,
  endpoint, failure-domain label }`; discovery resolves `id → current endpoint`.
- **The atomic location update.** Reconstruction stages the rebuilt fragment, then
  issues **one `commit(WriteBatch)`** that `require`s the prior inode version and
  `put`s the inode with the placement entry repointed and the version bumped — the
  **same commit point as a write** ([§6.3][s6]). Readers in flight hold the old
  version and finish against the old fragment; readers after the commit see the
  new one; **never a hybrid**. A `Conflict` (a concurrent writer won the CAS) is a
  retry, not an error.

This is a composition-local change to `core` (metadata model + write/read paths)
and `chunkstore-grpc` (the fan-out stops being the location authority); the
`MetadataStore` and `ChunkStore` *traits* are untouched by it.

### `ChunkStore`: enumerate + delete (the second load-bearing change)

The trait is `put_fragment` / `get_fragment` / `health` only
(`crates/traits/src/lib.rs:72-84`) — a store cannot be *walked* and a fragment
cannot be *deleted*. The two maintenance loops need exactly those:

```rust
// crates/traits — additive to ChunkStore (fields-never-repurposed, ADR-0002 wire rule)
async fn list_fragments(&self) -> Result<Vec<FragmentId>>;   // scrub walks the store
async fn delete_fragment(&self, id: FragmentId) -> Result<()>; // GC reclaims orphan bytes
```

- **`list_fragments`** lets scrub enumerate what a D server actually holds and
  diff it against the chunk map (orphans the GC should reclaim; absences the
  reconstruction should rebuild). For the networked store it is a new
  `ChunkStore` gRPC rpc; for `chunkstore-fs` it is a directory walk.
- **`delete_fragment`** is what makes GC able to reclaim bytes — today the
  test-invoked ledger sweep (`core`, `sweep_expired_leases`) deletes *ledger
  entries* but **no fragment bytes**, because the affordance does not exist. The
  networked store gains the matching rpc; the D server, staying **deliberately
  dumb** ([§8.5][s8]), simply removes the bytes it is told to.

Both land additively in `proto` (the `ChunkStore` service), `chunkstore-grpc`
(client + D-server service), and `chunkstore-fs` — a one-version-gap-compatible
service evolution ([§8.7][s8]), not a `format_version` or trait-contract break.

### Failure-domain-aware placement (the M3 custodian half)

M2 spread fragments across **distinct endpoints**, explicitly **not** independent
failure domains, and **claimed no durability math** ([0004][p4]). M3 introduces a
**zone-local failure-domain model**: each D server carries an opaque **failure-domain
label** (rack / power / switch, [§7.3][s7]) from config, surfaced through its
registration. A small **domain-aware selector** — shared by the write fan-out and
by custodian re-placement — enforces the invariant that a chunk's *n* fragments
occupy *n* distinct domains where the topology allows. This is the first point at
which RS(6,3)'s durability math is **claimable within a single zone** with ≥ *n*
domains.

The boundary with **L2 / M6** is deliberate and stated: M3 owns **enforcement on
write and repair** with zone-local labels; the **placement-*policy* service** —
which scheme a tenant gets, the global capacity-and-growth view, cross-zone
placement — is M6 ([§7.3][s7]: "placement service (L2) **and** custodians (L4)
enforce domain spread"). The M3 abstraction is kept thin (an opaque domain id +
the distinctness invariant + per-domain utilization) precisely so M6's policy
service layers on rather than replaces it.

### The four custodian loops

All four are continuous reconciliation loops on the single active custodian,
reading authoritative state (the chunk maps, the pending ledger, D-server health
and `list_fragments`) and converging reality toward the recorded intent. Every
loop that mutates a location does so **commit-point-atomically**.

**Scrub** — continuously `list_fragments` each D server, fetch and **verify each
fragment's checksum against the chunk map**, and on a mismatch treat the fragment
as lost and enqueue the chunk for reconstruction ([§6.3][s6] step 1). It catches
**bit rot before the data is needed**, the mirror of the read path's read-time
checksum verification ([§6.2][s6]). It emits **scrub coverage** and
**scrub-detected corruption rate**.

**Reconstruction** — the heart of M3. On a trigger (a D-server health report of
loss, a scrub corruption finding, or a read-path checksum failure), for each
affected chunk:

```
detect:  D-server loss (health) ∪ failed checksum (scrub / read)  ──►  under-replicated chunk
repair:  gather any k surviving fragments ──[verify checksums]──► [reconstruct missing shard(s)]
         ──► place rebuilt fragment(s) on healthy D servers in DISTINCT failure domains
         ──[ONE version-conditional MetadataStore::commit: repoint the placement record]──►
            readers flip atomically to the new location
gc:      the displaced / orphaned fragment ──[after a reader-safe grace window]──► delete_fragment
```

Reconstruction reads the **per-chunk** EC scheme (`EcScheme::{None,
ReedSolomon{k,m}}`, `crates/core/src/metadata.rs:55-67`) — *k*/*m* vary per chunk
(mixed-era), so reconstruction is scheme-driven, never a zone-global constant. With
encryption on ([ADR-0021][a21]) the client encrypts *below* EC, so the custodian
**reconstructs ciphertext fragments and never needs tenant keys** ([§8.5][s8]).

**GC** — promote the test-invoked stand-in into a running loop with **two inputs**
([§6.7][s6]): expired **pending-ledger leases** (crashed writes/repairs — the
leased garbage M1/M2 already produce on failed/partial fan-out) and **orphaned
fragments** (from deletes and from completed reconstructions). Bytes are reclaimed
via the new `delete_fragment` only **after a reader-safe grace window** — long
enough that an in-flight reader holding the prior version is **never torn**
([§6.7][s6]; the pending-ledger sweep pattern of [§5][s5]). GC's invariant is the
one whose violation is silent corruption: **never reclaim a referenced fragment**.

**Rebalance** — proactively move fragments off **draining / decommissioning** D
servers, **preserving the failure-domain invariant** ([§6.3][s6] step 3). Each move
is the same commit-point-atomic re-place as a reconstruction. It is driven by
operator desired state (drain this server) reconciled by the custodian
([§8.4][s8]); hot-spot rebalance is the lighter, measurement-driven part and may
trail the drain/decommission path. Where rebalance and durability spread conflict,
**spread wins** (durability is gate-zero).

### Repair-vs-serve: dynamic priority, not a static throttle

Repair reads are **throttled below foreground reads** to protect read latency —
**but** repair priority **rises as redundancy falls**, so a chunk one fragment from
its *m*-th loss (its durability floor) **preempts** foreground work ([§6.3][s6]
line 45; [§8.9][s8]). Durability is gate-zero; latency yields to it only when
redundancy is genuinely threatened. M3 implements this as a priority function over
the repair queue, attached to the **read-retry reserved seat** M2 left in the
parallel read path ([0004][p4]) — *not* a redesign of the read path. The objective
the priority serves is **minimizing time-to-repair** (durability is the probability
that more than *m* fragments fail within one repair window, [§6.3][s6]); the full
global admission/backpressure model ([§8.9][s8]) lands incrementally — M3 builds
the seat and the priority function, not a fleet-wide scheduler.

### The durability plane (telemetry + audit log)

This is the half of M3 that retires the **observability** risk, and it is a
**graduation criterion**, not a nicety: the metrics are emitted **from the
custodians' first commit** ([ADR-0011][a11]). M3 emits the **five single-zone
durability metrics**:

1. **under-replicated chunk count** — chunks below their scheme's fragment count,
   materialized from the chunk maps + D-server health (the metric whose silent
   non-zero value is the failure the request plane hides);
2. **repair-queue depth**;
3. **time-to-repair distribution**;
4. **scrub coverage**;
5. **scrub-detected corruption rate**.

(The sixth ADR-0011 metric, **replication lag per zone pair**, is **deferred to
M5** — single-zone M3 has no zone pair to measure.) Alongside the metrics, an
**append-only audit/event log** of significant transitions — placement, repairs,
admissions, deletions ([§8.3][s8]) — the operational-debugging *and* GDPR-deletion-proof
record. Instrumentation is **OpenTelemetry** via `tracing` + `tracing-opentelemetry`,
exposing **both** a Prometheus-scrapeable endpoint (zero-dependency, ideal for the
dev profile) **and** OTLP push (production), hardcoding no backend ([ADR-0012][a12]).
The custodians are also the natural producer of the **capacity plane's**
per-failure-domain utilization ([§8.3][s8], [§8.9][s8]); M3 emits it as a
by-product of the domain model. **Dashboards, alerting, and the UI stay deferred**
([ADR-0013][a13]).

### Declarative management hook

Management is **declarative and self-reconciling** ([ADR-0011][a11] rule 2): the
operator writes **desired state** (drain / decommission a D server) and the
custodian reconciles reality toward it — the Kubernetes control-loop pattern on the
substrate already present. "Policy changed" (recorded) and "policy satisfied"
(reality matches) are **distinct, observable moments** ([§8.4][s8]). M3 builds only
the **hook**: the desired-state read/write + reconciliation-status surface the
custodian needs, single-zone (desired state folds into the local metadata /
coordination config). The full API-first management surface and its thin CLI are
[ADR-0013][a13], deferred.

### Single active custodian, fenced

The custodian runs as **one active leader per zone** ([§5][s5] L5), elected via the
**existing** `Coordination::elect_leader`, which returns a fenced `Leadership`
token (`crates/traits/src/lib.rs:209-211, 39-41`). Two guards make a
deposed-but-still-running custodian safe: the **fencing token** rejects its
coordination actions, and — decisively — its location-update `commit` is
**version-conditional**, so a stale custodian's mutation loses the CAS and lands
nothing. Single-leader is the M3 choice; **sharded** scrub/repair (for throughput
against the time-to-repair budget) is an Open question, not M3 scope.

### DST and tests (the heart of M3)

[ADR-0009][a9] remains the correctness authority: the custodians run **inside the
deterministic simulator** ([§13.1][s10]: "metadata, D servers, custodians, faults,
clock skew… single-threaded"), and **every real-world discovery is promoted back
into DST** as a permanent seeded regression (the M0/[ADR-0009][a9] rule). M3's tier
mapping is **Tier 0–2; Jepsen consistency; disk-fault injection for scrub/repair**
([§13.4][s10]) — **no Tier 3** (that is M5).

**Tier-0 — deterministic simulation** (the bar). The properties, against the
quality scenarios [§10][s10] Q1–Q3:

1. **Reconstruct-to-full-redundancy (Q1)** — kill a D server (drop its fragments);
   the custodian rebuilds every affected chunk onto healthy servers in **distinct
   failure domains** within the repair budget, and **reads never error during
   repair** (any-*k* still satisfiable from survivors throughout).
2. **Commit-point-atomic repair under crash** — crash the custodian at every step
   of the reconstruction pipeline; the chunk is **always** either fully at its old
   location or fully at its new one — **never a hybrid** — and any fragment a
   crashed repair placed-but-did-not-commit is **collectable garbage, not
   corruption**.
3. **Scrub detects bit-rot then reconstructs (Q2)** — inject a bit-flip into a
   stored fragment; scrub's checksum verification **excludes** it, flags
   corruption, and reconstruction restores full redundancy; a checksum-failing
   shard is **never** fed to the decoder.
4. **GC reclaims only true orphans (Q3)** — a write/repair interrupted before
   commit leaves leased garbage that GC reclaims after the grace window; GC
   **never** deletes a fragment a committed chunk map references, and **never**
   tears an in-flight reader holding the prior version.
5. **Fenced stale leader** — a deposed custodian that keeps running lands **no**
   location update (version CAS + fencing token), even racing the new leader.
6. **Durability-plane emission** — after an injected loss, **under-replicated
   count** rises then returns to zero as repair completes; **repair-queue depth**
   and **time-to-repair** are emitted and correct — telemetry validated by
   **assertion**, not by a dashboard.

**Tier-1 — software-defined faults** ([§13.2][s10]): **disk-fault injection**
(device-mapper **dm-flakey / dm-error**, FUSE injectors) drives the scrub and
checksum-verification paths against **real block-layer misbehaviour** — the
real-hardware complement to DST's modeled bit-rot — and **Jepsen** consistency runs
over the repair path. **Tier-2** ([§13.2][s10]): on a single real node, kill a real
D server and watch real reconstruction over real NVMe/fsync. A seed that finds a
bug is committed as a permanent regression.

### Crate touch-points

Building on the workspace as it stands after M2 (`chunk-format`, `traits`, `proto`,
`core`, `chunkstore-fs`, `chunkstore-grpc`, `coordination-mem`, `metadata-redb`,
`testkit`, `server`, `dst`, `xtask`):

- **`custodian`** (**new**, L4) — the four loops, the reconciliation control loop,
  the failure-domain selector, single-active leadership, durability-plane emission.
  Deps `traits`, `core`, `proto`, `tracing`/`tracing-opentelemetry`; **never** a
  concrete backend ([ADR-0010][a10]).
- **`traits`** — `ChunkStore` gains `list_fragments` + `delete_fragment`.
- **`proto`** — additive `ChunkStore` service rpcs (`ListFragments`,
  `DeleteFragment`); a stable D-server-id + failure-domain field on registration.
- **`core`** — placement recorded in the chunk map; read path consumes it
  (retire `index % n`); the version-conditional location-update used by repair;
  the GC sweep promoted from a test helper toward the running loop.
- **`chunkstore-fs` / `chunkstore-grpc`** — implement `list_fragments` /
  `delete_fragment`; the fan-out stops being the location authority.
- **`server`** — a **`custodian` subcommand/role** ([ADR-0014][a14]/[ADR-0016][a16],
  coarse-then-split); D-server failure-domain labels from config; the OTLP/Prometheus
  exporter wiring; the desired-state (drain/decommission) surface.
- **`testkit`** — a **bit-rot / fragment-loss fault seam** and a D-server-kill seam
  alongside the existing `Clock`/`Disk`/`Network` seams.
- **`dst`** — the custodian property campaign under `--cfg madsim`; new seeds.
- **`xtask`** — Tier-1 disk-fault (dm-flakey/dm-error) + Jepsen runners; the
  Tier-2 kill-and-reconstruct integration.
- **deps** — `tracing-opentelemetry` / `opentelemetry-otlp` (+ a Prometheus
  exporter); confirm under the `cargo-deny` allowlist ([ADR-0003][a3]).

## Alternatives considered

- **Degraded-write tolerance in M3** (commit with < *n*, custodians backfill;
  re-place a dead endpoint mid-write): **rejected for M3.** The architecture commits
  the full fragment set and **fails closed** ([§6.1][s6], [§8.9][s8]: "never a
  silent half-write"); degraded-write appears only in M2's hand-off note, not the
  architecture body. M3's risk is the **maintenance loops on post-commit
  degradation**, which reconstruction already retires; commit-below-*n* reverses a
  load-bearing invariant for a risk M3 does not own, and would entangle the pending
  ledger and version fence with a backfill completing *after* commit. It is left to
  a future slice **gated by its own ADR**. (Recorded as an Open question because it
  reverses a prior proposal's stated hand-off.)
- **Stateless per-read placement selection** (re-discover and probe a sufficient
  set, no recorded placement): **rejected.** It cannot survive a custodian *moving*
  a fragment — the moment placement changes, a probe-the-default scheme returns the
  wrong server — and it gives the custodians no authoritative map to scrub and
  reconstruct against. Recording at commit is the shape M2 already leaned toward.
- **Placement in a separate `placement:<chunk>` keyspace** rather than embedded in
  the chunk map: **kept as an open alternative.** Embedding makes the location
  update a single inode CAS (clean commit-point reuse) at the cost of inode-record
  size; a separate keyspace decouples placement churn from inode version but adds a
  second key to the atomic batch. Lean: embed (the `WriteBatch` is already
  multi-key, so either is one atomic `commit`).
- **A multi-active / sharded custodian** at M3: **deferred.** Single active leader
  (the [§5][s5] model) is correct and simplest; sharding for repair throughput is an
  optimization to make once time-to-repair telemetry shows it is needed.
- **A dedicated custodian/telemetry ADR minted by M3:** not minted for the loops
  themselves — they are implementation-first behind the architecture and
  [ADR-0011][a11]/[ADR-0012][a12]. But those two ADRs are **status: Proposed**, and
  M3 is where their contract is first *implemented*; M3 should **ratify ADR-0011 and
  ADR-0012 (Proposed → Accepted)** as part of the milestone (Open questions). The
  `ChunkStore` enumerate/delete + placement-record evolution may also force the
  fragment-addressing ADR M1 left open.
- **A separately-published custodian binary:** deferred. Single-binary-dev
  ([ADR-0014][a14]) and coarse-then-split ([ADR-0016][a16]) bless a `custodian`
  **role/subcommand**; the binary split waits for the production role-split.

## Graduation criteria (definition of done)

- A **lost D server's fragments are reconstructed onto healthy servers in correct
  failure domains via the commit-point-atomic pattern** — proven in DST and on a
  Tier-2 single node — with **no read errors during repair**.
- **Scrubbing detects injected bit-rot** and drives reconstruction; a corrupt
  fragment is excluded by its checksum and never decoded.
- **Reconstruction, rebalance, and GC are each commit-point-atomic**: a crashed
  maintenance job leaves **collectable garbage, never corruption or a torn read**;
  GC **never** reclaims a referenced fragment.
- **Placement is recorded at commit** (stable D-server id) and consumed by the read
  path; the M2 `index % n` stateless routing is retired.
- A chunk's *n* fragments are placed across *n* **distinct failure domains** on both
  write and repair where the topology allows; the **RS(6,3) durability math is
  claimable** for the single zone.
- **Durability-plane telemetry is emitted from the custodians' first commit**:
  under-replicated count, repair-queue depth, time-to-repair, scrub coverage,
  scrub-detected corruption rate, plus the append-only audit/event log — via OTLP +
  Prometheus, validated by assertion. ("Replication lag per zone pair" is deferred
  to M5.)
- The write path **remains fail-closed**; M3 introduces **no** degraded-write.
- Tier-0 DST custodian suite **green and seed-reproducible** (seeds committed);
  Tier-1 dm-flakey/dm-error scrub + Jepsen consistency green; Tier-2 single-node
  kill-and-reconstruct green in CI.
- `fmt`/`clippy` clean; `Cargo.lock` updated; `cargo-deny` passes with the new
  OpenTelemetry deps.

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M3** milestone (branch
`feat/m3.<n>-<slug>`, commit subject `feat(<crate>): … (M3.<n>, #<issue>)`):

1. **Placement record + stable D-server id** — record per-fragment placement in the
   chunk map at commit; read path consumes it (retire `index % n`); registration
   carries `{ id, endpoint, failure-domain label }`. *DoD:* an `rs(6,3)` write
   records placement; the read resolves fragments from the record after a restart;
   M0–M2 suites still green.
2. **`ChunkStore` enumerate + delete** — `list_fragments` + `delete_fragment` on the
   trait, the gRPC service, and both backends. *DoD:* a store can be walked and a
   fragment deleted over real tonic and in-process.
3. **`custodian` crate skeleton** — single-active leadership (fenced), the
   reconciliation loop scaffold, the failure-domain selector (shared with the write
   fan-out), and the OpenTelemetry seam wired (Prometheus + OTLP). *DoD:* a leader
   is elected and fenced; a domain-distinct placement is selectable; the exporter
   emits a first metric.
4. **GC custodian** — running loop over expired pending leases + orphaned fragments;
   reclaim bytes via `delete_fragment` after a reader-safe grace window; GC
   telemetry + audit events. *DoD:* leased garbage and orphans are reclaimed; a
   referenced fragment never is; an in-flight reader is never torn.
5. **Scrub custodian** — walk via `list_fragments`, verify checksums, detect bit-rot,
   enqueue repair; emit scrub coverage + corruption rate. *DoD:* an injected
   bit-flip is detected and excluded; coverage/corruption metrics emitted.
6. **Reconstruction custodian + repair-vs-serve priority** — detect under-replication;
   any-*k* → recompute → place in distinct failure domains → version-conditional
   atomic location update; dynamic repair priority on the read-retry seat; emit
   under-replicated count, repair-queue depth, time-to-repair. *DoD:* a killed D
   server's chunks return to full redundancy with no read errors; repair is
   commit-point-atomic under injected crash.
7. **Rebalance + declarative drain/decommission + capacity telemetry** — drain/
   decommission evacuation preserving the failure-domain invariant; the desired-state
   reconciliation hook; per-failure-domain utilization. *DoD:* a drained server is
   evacuated preserving spread; "changed" vs "satisfied" are observable.
8. **DST campaign + Tier-1/Tier-2 fault injection** — the Tier-0 custodian property
   suite (Q1–Q3, atomicity-under-crash, fenced-stale-leader, telemetry emission);
   Tier-1 dm-flakey/dm-error scrub + Jepsen; Tier-2 single-node kill-and-reconstruct.
   *DoD:* all green in the seed sweep, seeds committed; the container/Tier-2 job
   green in CI.

(M3 is larger than M1/M2's seven slices — it is the milestone that makes a single
zone trustworthy and gates the Step-2 release. Steps 1–3 are the foundation; after
them, GC, scrub, reconstruction, and rebalance are largely independent and are the
natural parallel split for more than one contributor, [§9][s9].)

## Backward compatibility

- **On-disk format** — **unchanged.** M3 adds a *metadata* placement record and new
  *maintenance* RPCs; it does not touch the fragment layout or `format_version`. The
  format stays **v0/unstable**; no production data exists to migrate. (M3's sustained
  fault-injection on maintained data is, however, a candidate **v1-stamping trigger**
  — see Open questions.)
- **Metadata model** — the chunk-map placement field is **additive** to a
  never-yet-deployed schema; M0–M2 chunks (single-store) read through the same path,
  their "placement" being the single store they were written to.
- **Wire contract** — the `ChunkStore` service grows `ListFragments` /
  `DeleteFragment` **by addition** ([§8.7][s8], fields never repurposed); a D server
  and client interoperate across a one-version gap.
- **Trait / internal API** — `ChunkStore` gains two methods (pre-1.0, no published
  API); `MetadataStore` and `Coordination` are **unchanged** (the location CAS and
  leader election already exist). Consumers in `core` see only the traits.
- **Public API / deployments** — none yet (the first deployable product is M4);
  nothing to stay compatible with.

## Open questions

- **Degraded-write tolerance** — this proposal **defers** it (keeps the write path
  fail-closed), against M2's [0004][p4] hand-off note that listed it "→ M3". If a
  real workload shows fail-closed writes are too brittle under partial fan-out, a
  follow-on slice may introduce commit-below-*n* + backfill — but **only with its
  own ADR** reconciling it against [§8.9][s8]'s fail-closed admission. Flagged
  prominently because it reverses a prior stated hand-off.
- **ADR-0011 / ADR-0012 status** — both are **Proposed**. M3 is the first
  *implementation* of their contract; they should be ratified (Proposed → Accepted)
  as part of (or just ahead of) M3, rather than shipping a graduation criterion that
  rests on an unratified ADR.
- **Placement-record home** — embedded in the chunk map (inode CAS, lean) vs a
  separate `placement:` keyspace (decoupled churn). Confirm in M3.1.
- **GC grace-window length** — must be tied to reader version-hold / lease
  semantics, not a magic constant; the exact value is a measurement question.
- **Repair priority function** — the concrete shape of "priority rises as redundancy
  falls" (and how much of the global [§8.9][s8] admission model is M3 vs later) is an
  M3 design point, observable via repair-queue-depth telemetry.
- **Single vs sharded custodian** — single active leader is the M3 choice; whether
  scrub/repair must shard for the time-to-repair budget is left to telemetry.
- **`v1`-stamping trigger** — the format prerequisite ([p2][p2]) stamps `v1` after
  "a second independent reader **or** a sustained fault-injection run." M3's Tier-0/1
  campaign on continuously-maintained data is a candidate trigger; recorded here, not
  decided (consistent with [0003][p3]).

[p1]: 0001-milestone-0-walking-skeleton.md
[p2]: 0002-implementation-arc.md
[p3]: 0003-milestone-1-erasure-coding.md
[p4]: 0004-milestone-2-networked-d-servers.md
[s4]: ../../architecture/04-solution-strategy.md
[s5]: ../../architecture/05-building-block-view.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s9]: ../../architecture/09-build-order-and-roadmap.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a12]: ../../adr/0012-opentelemetry-instrumentation.md
[a13]: ../../adr/0013-api-first-management.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a15]: ../../adr/0015-consistency-contract.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a21]: ../../adr/0021-encryption-at-rest-and-key-management.md
