---
created: 24.06.2026
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#266"
tags:
  - proposal
  - observability
  - telemetry
  - operability
  - milestone-4
  - diagnostics
---
# Proposal: The observability floor — making the first deployment diagnosable

> Draft. The **minimum observability that must be wired into a runnable binary
> before M4 real-world testing begins**, so that when something goes wrong in the
> first deployment, the three operator questions — *why is it slow? which node is
> down? why did this request fail?* — have answers. It is deliberately a **floor,
> not the operator surface**: the full management plane, auth/RBAC, day-2 ops,
> audit log, and polished dashboards are [proposal 0008][p8]; the d-server
> performance program is [proposal 0009][p9]; the TiKV backend swap is M4
> ([proposal 0007][p7]). This proposal pulls the *telemetry-emission-and-wiring*
> subset forward, decoupled from 0008's API, because the [M4 first-deployment
> blueprint][bp] already **instructs the first user** to run `wyrd custodian
> --otlp-endpoint …` and to *"watch the durability plane from minute one"* — and
> none of that is wired into the binary today. The point of this milestone is to
> make the blueprint's observability assumptions **true**.

## Motivation

**Operability is quality goal #3** — *"the durability state of the system must be
observable, and the dangerous-but-routine operations … must be safe, resumable, and
observable"* ([§1.3][s1]). The [M4 first-deployment blueprint][bp] turns that into a
concrete day-one checklist: verify placement spread "the custodian/telemetry should
expose this"; **"watch the durability plane from minute one"** (the five M3 metrics
over Prometheus/OTLP); kill a D server and **watch under-replicated count rise and
return to zero** ([blueprint §Day-one operations][bp]). The bring-up script runs
`wyrd custodian … --otlp-endpoint <your-collector>` ([blueprint §B.4][bp]).

**None of that is reachable from a deployable binary today.** Verified against the
working tree:

- The durability telemetry seam is **library-only**. `DurabilityTelemetry::new` and
  `ExporterConfig` (`crates/custodian/src/telemetry.rs`) have **no non-test caller**;
  they are constructed only in DST and unit tests.
- The custodian — which owns both the maintenance loops **and** the telemetry — is a
  dependency of the **`dst` test harness only** (`crates/dst/Cargo.toml:35`). The
  `server` binary does not depend on it and runs no custodian loop
  (`crates/server/src/cli.rs:47`: "The CLI runs no custodian sweep"). There is no
  `wyrd custodian` subcommand; `crates/server/src/main.rs` is a thin dispatch to the
  M0 `put`/`get`/`demo` frontend.
- **No `tracing` subscriber is installed at any binary entry** — no
  `tracing-subscriber`/`RUST_LOG`/`.init()` outside tests. So even the durability
  metrics the custodians *do* emit (`tracing::info!(monotonic_counter.… )`) would
  surface nowhere: a metric event with no subscriber + exporter is dropped on the
  floor. Operational logging on the access path was explicitly deferred at M0
  (`crates/server/src/cli.rs:8-9`).
- The **request and capacity planes are not instrumented** ([§8.3][s8] names them;
  [proposal 0008][p8] designs them; neither is built). The d-server's admission
  control sheds load **silently** (`crates/server/src/dserver.rs`) — visible only as
  a client-side gRPC `RESOURCE_EXHAUSTED`.
- **Errors are opaque.** `BoxError` everywhere except the one typed `IntegrityFault`
  (`crates/traits/src/lib.rs:59,62`), so "why did it fail" cannot distinguish a
  network drop from a store error from a timeout.
- **`health()` is implemented** (`crates/chunkstore-fs/src/lib.rs:320`) but is exposed
  through **no readiness/liveness probe** — no `tonic_health`/health service exists —
  so an orchestrator cannot tell which node is down.

The result: a `wyrd` process you deploy for the M4 campaign emits **no logs, no
metrics, and runs none of the loops** the blueprint tells you to watch. The
observability *design* is comprehensive and already exists ([0008][p8], [0009][p9]);
the gap is that it is **designed-but-unbuilt-and-unwired**, sequenced after M4, and
M4 itself ([0007][p7]) scopes none of it. This proposal closes exactly that gap, and
no more.

**Why now:** real-world testing is the point at which "designed" stops being enough.
A first deployment with no diagnostics doesn't just slow debugging — it silently
fails goal #3's own acceptance test (kill a node, watch the plane), because there is
no plane to watch.

## Design

### Scope boundary

The organizing rule is **"can I diagnose *at all*" — not "can I diagnose
*elegantly*."** Every item below is either a carve-out of [proposal 0008][p8]'s
telemetry plane (pulled forward and decoupled from its management API) or one of the
four small gaps 0008/0009 do not own. The "elegant" tier — management API, auth,
audit log, polished Grafana, full distributed tracing — stays in 0008.

**In scope** (the floor — gates the M4 first-deployment campaign):

1. **The wiring keystone — a telemetry handle + `tracing` subscriber installed at
   every role entry.** This is the single highest-leverage change and a prerequisite
   for *every other* item, including the durability plane that is already written.
   Construct the telemetry handle and install a `tracing-subscriber` registry with
   the `MetricsLayer` (`telemetry.rs:116`) **and** a fmt/log layer, plus the chosen
   exporter (`ExporterConfig::{Prometheus,Otlp,Both}`), at each binary role's entry.
   Without this, nothing emits no matter how much is instrumented.
2. **The custodian as a runnable role.** Make `wyrd custodian` real (the blueprint's
   `§B.4` command): `server` gains a `custodian` subcommand that depends on the
   `custodian` crate (today a `dst`-only dep), runs the leader-elected loop, and
   installs the telemetry handle from item 1. This is what turns the **already-built,
   DST-verified durability plane** from simulation-only into something a process
   emits — the blueprint's "watch the durability plane from minute one."
3. **Operational logging** — `--log-level` / `RUST_LOG` (an `EnvFilter`), structured
   stderr logs for startup/shutdown/leadership-change/major decisions, lifting the
   `cli.rs:8-9` M0 deferral. Falls out of item 1; preserves the stdout-is-payload
   discipline already documented there.
4. **Request-plane RED, minimal** ([§8.3][s8]): per-operation latency histograms and
   error counters on the write/read path (`crates/core`), the error counter **keyed
   by the typed failure class** from item 6. Adequate, not elegant — RED counters,
   not full traces.
5. **Capacity-plane signals, minimal** ([§8.3][s8], [§8.9][s8]): emit the
   admission-control **events** the d-server currently swallows
   (`dserver.rs` — admitted / shed / timed-out), in-flight-request and
   concurrent-stream gauges, and per-failure-domain utilization (the custodian
   already has the domain model from M3).
6. **Typed errors at the trait seam** — a richer error enum extending the
   `IntegrityFault` precedent (`crates/traits/src/lib.rs:64-100`) that distinguishes
   **transient** (unreachable / timed-out / busy) from **terminal** classes, so the
   request-plane error counter and the operator both get a *class*, not an opaque
   string. **This is the one item that can collide with M4 — see Sequencing.**
7. **Health as an orchestrator probe** — expose the existing `health()`
   (`chunkstore-fs/src/lib.rs:320`, the `Health` enum at `traits/src/lib.rs:120`)
   over a standard gRPC health/readiness surface (`tonic-health`) so "which node is
   down" is answerable by a liveness/readiness check, not inferred.

**Out of scope** — owned elsewhere, deliberately *not* pulled forward:

- The **operator management API, thin CLI, OIDC+mTLS auth, RBAC, day-2 operations,
  multi-tenancy administration, and the append-only audit log** → [proposal 0008][p8].
  This floor emits signals; 0008 builds the surface that governs and secures them.
- **Polished Grafana dashboards** (`deploy/grafana/`) and alerting → [0008][p8]
  ("a gift, not a gate", [ADR-0012][a12]). Early testing queries Prometheus directly;
  this proposal ships only the **metric/log inventory**, not curated dashboards.
- **Full distributed tracing** — OTel spans propagated across the gateway→d-server
  gRPC seam, Tempo, and the **cross-plane request↔durability correlation** (joining a
  write to the repair its fragment triggers minutes later, in a different process).
  That correlation is the architecturally novel, diagnostically decisive piece and is
  **not owned by 0008** — it warrants its **own ADR** (next free number 0036). The
  floor ships a **per-request correlation id in the logs** (cheap, no span plumbing);
  the full span graph + cross-plane join is a fast-follow (see Open questions).
- **The d-server performance program** (PUT/GET p50/p99/p999 harness, mTLS, copy
  elimination) → [proposal 0009][p9]. This floor tells you *that* the d-server is
  slow; 0009 tells you *why* and makes it fast.
- **The TiKV backend and the `wyrd gateway`/`d-server` production roles' backend
  composition** → M4 ([0007][p7]). This proposal wires telemetry into whatever roles
  exist; it does not introduce the metadata backend.

### What carries over, unchanged

This is **purely additive instrumentation + wiring**. It touches **no** commit
protocol, **no** consistency contract ([ADR-0015][a15]), and **no** on-disk format
([ADR-0002][a2], [ADR-0019][a19]) — the three highest-bar surfaces. The
`MetadataStore`/`ChunkStore`/`Coordination` trait *contracts* are unchanged except
for the additive typed-error enum (item 6) and the already-present `health()`. The
custodian loops, the EC path, and the gRPC data path are untouched in behaviour; they
gain emission points, not new logic. The durability telemetry seam
(`telemetry.rs`) is **reused, not rebuilt** — the floor wires it in and generalizes
it to a second and third consumer.

### The telemetry seam: reuse, then generalize (the one structural question)

The floor wires `DurabilityTelemetry` into a running binary and then needs the **same
dual-export, no-backend-hardcoded** seam ([ADR-0012][a12]) for the gateway (request
plane) and d-server (capacity plane). Two consumers beyond the custodian now exist, so
[ADR-0016][a16]'s evolution rule ("extract when the second consumer appears") applies:
**lean — extract a shared `crates/telemetry` crate** holding the `ExporterConfig`,
the provider/registry construction, the subscriber installation, and the
`gather_prometheus` read-back, leaving `custodian` to own only its *durability
meters*. This keeps one instrumentation path (the ADR-0012 discipline) rather than
three divergent ones. Final call deferred to implementation (Open questions); either
way no concrete backend leaks into a leaf crate ([ADR-0010][a10]).

### Crate touch-points

Building on the workspace after M3 (and independent of M4's TiKV work except item 6):

- **`server`** — install the subscriber + exporter at each role entry
  (`main.rs`/`cli.rs`); add the **`custodian` runnable role** (depend on the
  `custodian` crate, run the loop); add `--log-level`/`RUST_LOG`; wire the
  `tonic-health` service on the d-server (and other) roles; instrument the
  write/read gateway path with request-plane RED.
- **`telemetry`** (**new**, or kept in `custodian` — Open question) — the shared
  dual-export handle + subscriber installer, lifted from
  `custodian/src/telemetry.rs`.
- **`custodian`** — becomes a `server` dependency; expose a runnable loop entry;
  emit per-failure-domain capacity utilization (it has the domain model). No change
  to its durability meters.
- **`core`** — request-plane latency/error emission around `read`/`write`; map
  backend errors to the new typed classes at the boundary.
- **`traits`** — the additive typed-error enum extending `IntegrityFault`;
  `is_integrity_fault`-style classifiers gain transient/terminal siblings. `Health`
  already present.
- **`chunkstore-grpc` / `server::dserver`** — emit admission-control events +
  in-flight/stream gauges; surface the typed error classes over gRPC status codes
  (the `DATA_LOSS`-for-`IntegrityFault` precedent, reconstructed client-side).
- **`proto`** — reuse the standard gRPC health proto via `tonic-health` (no bespoke
  service); any error-class detail rides existing status conventions.
- **`dst`** — assertions that each new emission point fires (the `gather_prometheus`
  read-back pattern), that typed errors survive the gRPC seam, and that the
  subscriber/exporter wiring installs in a simulated role.
- **deps** — `tracing-subscriber` (EnvFilter + fmt) and `tonic-health`; the
  OpenTelemetry stack is already vendored ([ADR-0003][a3] `cargo-deny` allowlist
  check).

### DST and tests

[ADR-0009][a9] stays the correctness authority, and **DST never measures
performance** ([proposal 0009][p9]; [§13][s10]) — this floor is about *emission
correctness*, not latency. Properties:

- **Emission-wired** — in a simulated role with the subscriber + Prometheus surface
  installed, each plane's metrics are **read back in-process**
  (`DurabilityTelemetry::gather_prometheus`, `telemetry.rs:135-142`) and asserted
  present and correctly labelled — the M3 C4-verify pattern extended to the request
  and capacity planes.
- **Durability plane lights up in a process** — the M3 property "after an injected
  loss, under-replicated count rises then returns to zero" now asserted **through the
  wired custodian role**, not only the library — closing the sim-only gap.
- **Typed errors survive the wire** — a transient fault and a terminal fault each
  reconstruct to the correct class client-side across the gRPC seam (mirrors the
  `IntegrityFault`-over-`DATA_LOSS` test).
- **Health probe** — a store made unhealthy flips the readiness probe to not-ready.
- A live Prometheus scrape / OTLP collector run is **supplementary off-Check
  evidence** ([ADR-0012][a12]), exercised once on a Tier-2 single node against the
  blueprint's day-one checklist.

## The milestone — "observability floor", a release-gate dependency of M4 testing

This is **not a new arc milestone** (it retires no new correctness risk; it is
operability work layered on M3, like [0008][p8]/[0009][p9]). It is a focused
implementation milestone whose **definition of done is: the [M4 first-deployment
blueprint][bp]'s day-one checklist is executable as written** — every `watch`/`verify`
step resolves to a real metric, log, or probe. It **gates the M4 real-world testing
campaign** and runs **in parallel with M4 implementation** ([0007][p7]) — except item
6 (typed errors), which must be sequenced against M4 (below).

### Suggested PR sequence (each its own definition of done)

Branch `feat/obs-floor.<n>-<slug>`. Ordered so the keystone lands first and value
accrues incrementally:

1. **Telemetry seam wired + subscriber installed** (items 1, 3; optionally the
   `telemetry` crate extraction). *DoD:* a running role installs a subscriber, exposes
   a Prometheus endpoint / pushes OTLP, logs startup at `RUST_LOG=info`, and a first
   metric is scrapeable.
2. **`wyrd custodian` runnable role** (item 2). *DoD:* the blueprint's `§B.4`
   custodian command starts, elects leadership, runs the loops, and emits the five M3
   durability metrics to a live collector; the kill-a-D-server day-one test shows
   under-replicated count rise and return to zero against a real exporter.
3. **Typed errors at the trait seam** (item 6) — **sequenced per the note below**.
   *DoD:* errors classify transient vs terminal at the trait boundary and across gRPC;
   `IntegrityFault` remains a distinct terminal class.
4. **Request-plane RED** (item 4). *DoD:* per-op latency + error-by-class counters
   emit from the write/read path; asserted via in-process read-back.
5. **Capacity-plane signals** (item 5). *DoD:* admission admitted/shed/timed-out
   events and per-failure-domain utilization emit; a forced load-shed is observable as
   an event, not just a client status code.
6. **Health/readiness probe** (item 7). *DoD:* a `tonic-health` readiness check
   reflects `health()`; an unhealthy store reads not-ready.
7. **Metric/log inventory doc + blueprint cross-check** — a short reference listing
   every emitted metric/log/probe, and a pass confirming each blueprint day-one step
   maps to one. *DoD:* the three operator questions each resolve to a named signal.

### Sequencing note — the one M4 collision

The **typed-error enum is at the trait seam (`crates/traits/src/lib.rs`); M4 swaps the
metadata backend *behind that same trait*** ([0007][p7]). A richer enum can ripple
into M4's TiKV `MetadataStore` implementation (it must produce the new variants).
**Before doing PR 3 and M4 in parallel, confirm whether they churn the same trait
definitions.** If they do, sequence deliberately — land the enum first so M4's backend
targets the final shape, or land it after M4's backend and adapt. Cheap to decide now,
expensive to discover at merge. PRs 1–2 and 4–7 are disjoint from M4's metadata
surface and parallelize freely.

## Alternatives considered

- **Fold this into [proposal 0008][p8] and build it all together.** Rejected for
  *timing*, not scope: 0008 is the full operator surface (API + auth + RBAC + day-2 +
  multi-tenancy + audit + dashboards) and is a large body of work. Real-world testing
  is weeks out; serializing the whole management plane in front of the first
  deployment means testing with no diagnostics. The floor is the strict subset that
  unblocks testing; 0008 subsumes it later. (0008's telemetry section should be
  rebased onto whatever this floor wires.)
- **Author a fresh, full observability proposal.** Rejected — it would duplicate
  [0008][p8]'s three-plane design and contradict its ADR-0011/0012/0013 ratification.
  This proposal references that design and pulls a slice forward rather than restating
  it.
- **Skip the custodian-role wiring; just expose a metrics endpoint.** Rejected — the
  durability plane *is* the custodian's output; with no loop running there is nothing
  to expose. Wiring the role (item 2) is what makes the already-built plane real.
- **Defer typed errors to a later cleanup.** Rejected — "why did it fail" is one of
  the three questions the campaign must answer, and the enum is small; the only cost
  is the M4 sequencing, which is a decision, not a blocker.
- **Build full distributed tracing now.** Rejected for the floor — spans + Tempo +
  cross-plane correlation are the heaviest lift and the elegant tier; good per-plane
  metrics + logs + a correlation id answer the day-one questions. The hard
  cross-plane join gets its own ADR when the floor is in.

## Graduation criteria (definition of done)

- A deployed `wyrd` process (per the [blueprint][bp]) **emits logs and metrics**: a
  `tracing` subscriber + exporter is installed at role entry; `RUST_LOG`/`--log-level`
  works; the Prometheus endpoint scrapes / OTLP pushes.
- **`wyrd custodian` runs as a role** and the **durability plane emits in-process**:
  the blueprint's kill-a-D-server test shows under-replicated count rise and return to
  zero against a **live** exporter, not a test read-back.
- The **request plane** (per-op latency + error-by-class) and **capacity plane**
  (admission events + per-failure-domain utilization) emit over the dual
  Prometheus/OTLP seam with **no backend hardcoded** ([ADR-0012][a12]).
- **Errors classify** transient vs terminal at the trait seam and across gRPC;
  `IntegrityFault` stays distinct.
- **`health()` is reachable as a readiness probe**; an unhealthy store reads not-ready.
- **The three operator questions resolve to named signals**, demonstrated by the
  inventory doc and a blueprint day-one cross-check.
- DST emission properties **green and seed-reproducible** ([ADR-0009][a9]); `fmt`/
  `clippy` clean; `Cargo.lock` updated; `cargo-deny` passes with `tracing-subscriber`
  + `tonic-health`.
- The typed-errors/M4 sequencing decision is **recorded** before the two run in
  parallel.

## Backward compatibility

- **On-disk format** — unchanged; this touches no fragment layout or `format_version`.
- **Consistency contract / commit protocol** — unchanged ([ADR-0015][a15]); emission
  points only.
- **Wire** — additive: the `tonic-health` service is new and standard; error classes
  ride existing gRPC status conventions (the `IntegrityFault`/`DATA_LOSS` precedent);
  the `ChunkStore`/`MetadataStore` data-path RPCs are untouched. One-version-gap
  evolve-by-addition holds ([ADR-0002][a2]).
- **Trait / internal API** — the typed-error enum is additive on a pre-1.0 seam (no
  published API). Consumers in `core` see richer classes; existing `BoxError` callers
  keep compiling through a `From`/boxing path during migration.
- **Deployments** — none in production yet (M4 is the first deployable product); the
  dev/eval single-binary profile ([ADR-0014][a14]) gains logs/metrics it can ignore.
  The custodian role is new and opt-in.

## Open questions

- **Shared `telemetry` crate vs keep in `custodian`** — lean: extract (the
  second/third consumer now exists, [ADR-0016][a16]); confirm at PR 1.
- **Typed-error/M4 sequencing** — the gating decision above; resolve before
  parallelizing PR 3 with M4.
- **Cross-plane request↔durability correlation** — the genuinely novel design
  question this floor *defers*: joining a request to the repair its fragment triggers
  later, in a different process, with no live span to carry — requires durability
  events to carry enough **provenance** (chunk, placement decision) to be joined back.
  Owned by neither [0008][p8] nor this floor; should become **ADR-0036** (or a 0008
  addendum) when the floor is in. The floor ships only a log-level correlation id.
- **ADR ratification** — this floor is arguably the *first real implementation of the
  [ADR-0012][a12] OpenTelemetry-instrumentation contract* (the first to wire OTel into
  a binary). By the established preference it leaves the **Proposed → Accepted** flip
  of ADR-0011/0012/0013 to [proposal 0008][p8] as the completing management
  implementation; flag for the architecture board whether wiring-first warrants moving
  ADR-0012 here instead. (No flip is performed by this proposal.)
- **Log/metric cardinality** — per-tenant / per-D-server label cardinality bounds
  (a real cost at fleet scale) are a tuning question, not a floor blocker; note limits
  in the inventory doc.

## Relationship to existing decisions

- **Implements (first wiring of)** [ADR-0011][a11] (durability telemetry) and
  [ADR-0012][a12] (OpenTelemetry, dual-export, no hardcoded backend) **in a runnable
  binary** — but **defers their ratification** to [proposal 0008][p8] (see Open
  questions).
- **Precursor to / subsumed by** [proposal 0008][p8]: this is the diagnostic floor;
  0008 is the operator surface (API/auth/RBAC/day-2/audit/dashboards) that builds on
  the signals wired here. 0008's three-plane telemetry section should rebase onto this.
- **Complements** [proposal 0009][p9]: the floor surfaces *that* the d-server is slow;
  0009 is the performance program that explains and fixes it.
- **Gates** the [M4 first-deployment blueprint][bp] day-one checklist; **parallel
  with** M4 ([proposal 0007][p7]) except the typed-errors trait-seam item.
- **Builds on** the M3 custodian work ([proposal 0005][p5]: `telemetry.rs`, the loops,
  the failure-domain model), [ADR-0009][a9] (DST authority), [ADR-0010][a10] (no
  concrete backend in leaf crates), [ADR-0014][a14] (single-binary dev/eval),
  [ADR-0016][a16] (coarse-then-split / extract-on-second-consumer).
- **Forward-references** ADR-0036 (cross-plane correlation), unwritten, for the
  elegant tier.

[p5]: ../accepted/0005-milestone-3-custodians.md
[p7]: ../accepted/0007-milestone-4-production-metadata-backend.md
[p8]: 0008-management-and-administration.md
[p9]: 0009-d-server-performance.md
[bp]: ../../architecture/m4-first-deployment-blueprint.md
[s1]: ../../architecture/01-introduction-and-goals.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a12]: ../../adr/0012-opentelemetry-instrumentation.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a15]: ../../adr/0015-consistency-contract.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a19]: ../../adr/0019-chunk-format-layout.md
