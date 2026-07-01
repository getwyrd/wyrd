---
created: 24.06.2026
type: review
author: Eduard Ralph
tags:
  - review
  - observability
  - milestone-4
  - operability
---
# Review: Observability readiness for M4 real-world testing

> A readiness assessment, not a proposal. It answers one question — *when M4 is
> reached and real-world testing begins, will the system provide enough insight to
> diagnose problems?* — against the design as it **actually stands in the code and
> the existing draft proposals**, and names the smallest slice that must be built
> before testing starts. The observability *design* already exists and is
> comprehensive ([proposal 0008][p8], [proposal 0009][p9]); the gap is that almost
> none of it is wired into a runnable binary, and M4 ([proposal 0007][p7]) does not
> add it.

## Bottom line

**As the code stands today: no — and the shortfall is larger than "the request and
capacity planes aren't instrumented yet."** Even the **durability plane**, the one
plane that is fully implemented and DST-verified, **is not wired into any deployable
process**: the `custodian` crate (which owns the loops *and* the telemetry seam) is
a dependency of the `dst` test harness only (`crates/dst/Cargo.toml:35`) — the
`server` binary does not depend on it, runs no custodian loop
(`crates/server/src/cli.rs:47`), installs no `tracing` subscriber, and constructs no
metrics exporter (`DurabilityTelemetry::new` and `ExporterConfig` appear **only in
tests**). A `wyrd` process you can deploy at M4 today emits **no logs, no metrics, and
runs none of the maintenance loops** that produce the durability signals.

**As designed: yes — comprehensively.** [Proposal 0008][p8] designs all three §8.3
planes (request RED + traces, durability, capacity), the audit log, and curated
`deploy/grafana/` dashboards; [proposal 0009][p9] owns d-server "why is it slow"
diagnosis. The problem is **designed-but-unbuilt-and-unwired**, sequenced after M4,
and not pulled forward by anything M4 itself scopes ([proposal 0007][p7] contains
zero observability work).

So the decision in front of real-world testing is **not** "write another
observability proposal" — that scope already exists. It is "**carve the minimum
diagnostic floor out of the existing design and wire it into the binary before
testing starts.**"

## What I verified (evidence)

| Claim | Status | Evidence |
|---|---|---|
| Durability telemetry seam exists, dual-export, backend-agnostic | **True, library-only** | `crates/custodian/src/telemetry.rs` |
| …but constructed/wired in a runnable binary | **False — tests only** | `DurabilityTelemetry::new` / `ExporterConfig` have no non-test caller |
| Custodian loops run in the deployable server | **False** | custodian is a `dst`-only dep (`crates/dst/Cargo.toml:35`); `crates/server/src/cli.rs:47` |
| A `tracing` subscriber is installed at any binary entry | **False** | no `tracing-subscriber`/`RUST_LOG`/`.init()` outside tests; `crates/server/src/main.rs` is a thin M0 dispatch |
| Operational logging on the access path | **Absent (deferred)** | `crates/server/src/cli.rs:8-9` |
| `ChunkStore::health()` implemented | **True** (corrects an earlier stale read) | `crates/chunkstore-fs/src/lib.rs:320` |
| …exposed as an orchestrator readiness/liveness probe | **False** | no `tonic_health`/health service anywhere |
| Errors are typed enough to classify a failure | **False** | `BoxError` everywhere bar `IntegrityFault` (`crates/traits/src/lib.rs:59`) |
| Request- / capacity-plane metrics | **Absent (designed in 0008)** | [proposal 0008 §Observability][p8] |

## The three questions you'll actually ask — at M4 as it stands

- **"Why is it slow?"** — No answer. No request-plane latency/throughput metrics, no
  d-server perf instrumentation ([0009][p9] designs it, unbuilt), no logs.
- **"Which node is down?"** — No answer. `health()` exists per store but is exposed
  through no probe; no custodian runs to notice under-replication; no metric surfaces.
- **"Why did this request fail?"** — Barely. `BoxError` collapses network/store/timeout
  into one opaque string; only `IntegrityFault` is distinguishable.

## The diagnostic floor to build before testing (minimum to diagnose *at all*)

A small subset, drawn mostly from the *existing* design — not new scope. Ordered by
"you cannot test without it":

1. **Wire the telemetry seam + a `tracing` subscriber into the server binary.** This
   is the highest-leverage item and a prerequisite for *every* metric and log,
   including the durability plane that is already written. Construct
   `DurabilityTelemetry` (or its generalization) and install a subscriber with the
   `MetricsLayer` + an exporter at the binary entry. Without this, nothing emits no
   matter how much is instrumented. *(Carve-out of [0008][p8]'s telemetry seam; not
   gated on its management API / auth / RBAC.)*
2. **Run the custodian loop in a deployable role.** The durability metrics only exist
   while the loops run; today they run only inside DST. A `custodian` subcommand/role
   in `server` (the `d-server` precedent) makes the durability plane real in a process.
3. **Operational logging** — `tracing-subscriber` + `RUST_LOG`/`--log-level`, lifting
   the `crates/server/src/cli.rs:8-9` M0 deferral. Cheap, universal, and it falls out
   of item 1.
4. **Minimal request + capacity emission** — per-op latency + error counters keyed by
   a typed failure class, and admission-control *events* (admitted/shed/timed-out,
   currently silent). The thin telemetry slice of [0008][p8], decoupled from its API.
5. **Expose `health()` as a gRPC readiness/liveness probe** (`tonic_health`) so
   "which node is down" is answerable by an orchestrator. The method already exists;
   only the surface is missing.
6. **Typed errors at the trait seam** — extend the `IntegrityFault` precedent to
   distinguish transient from terminal, so "why did it fail" has a class. **See the
   sequencing note below — this one can collide with M4.**

Everything else in [0008][p8]/[0009][p9] — the full management API, OIDC+mTLS, RBAC,
day-2 ops, audit log, polished Grafana dashboards, the d-server perf harness, full
distributed tracing — is the "diagnose *elegantly*" ceiling and can fast-follow.
Early testing can query Prometheus directly and read logs.

## Genuine gaps not owned by 0008 or 0009

Most of the plan's intended scope is already covered. These few are not:

- **Cross-plane request↔durability correlation.** [0008][p8] lists "request plane —
  RED metrics + traces" but does **not** address the hard part: joining a request that
  lands a fragment to the durability event that fragment produces *minutes later, in a
  different process, triggered by the custodian, not the request*. There is no live
  span to propagate onto; the durability events must carry enough **provenance**
  (which chunk, which placement decision) to be joined back after the fact. This is
  the architecturally novel, diagnostically decisive piece, and it is unowned — a
  candidate **new ADR** (next free number is 0036) or an explicit addition to [0008][p8].
- **Operational logging / subscriber wiring** (floor items 1, 3) — implied by [0008][p8]'s
  telemetry but not called out; today it is *nothing*, and it blocks even the existing
  durability metrics from surfacing.
- **Orchestrator health/readiness probe** (floor item 5) — [0008][p8]'s "health" is
  reconciliation *status* via the management API, which is a different thing from a
  process liveness/readiness endpoint an orchestrator polls.
- **Typed errors** (floor item 6) — owned by neither draft.

## Sequencing note — the one real M4 collision

The **typed-errors change is at the trait seam (`crates/traits/src/lib.rs`); M4
swaps the metadata backend *behind that same trait*** ([proposal 0007][p7]). A richer
error enum can ripple into the TiKV `MetadataStore` implementation M4 adds (it must
produce the new variants). Before doing the two in parallel, confirm whether they
churn the same trait definitions; if so, **sequence deliberately** — land the enum
first so M4's backend targets the final shape, or land it after and adapt. Cheap to
decide now, expensive to discover at merge. The rest of the floor (telemetry wiring,
custodian role, logging, health probe) is disjoint from M4's metadata surface and
parallelizes freely.

## Recommendation

1. **Do not** author a new observability proposal — [0008][p8] already owns that scope
   and already proposes the ADR-0011/0012/0013 ratification; a second proposal would
   duplicate and contradict it.
2. **Pull the diagnostic floor (items 1–6) forward** as a small implementation slice
   *ahead of* the full [0008][p8] management surface — most of it is a carve-out of
   0008's telemetry plane plus the four small gaps, and item 1 (wiring) is the single
   thing that turns the existing, tested durability plane from simulation-only into
   something a running process emits.
3. **Resolve the typed-errors/M4 sequencing** before parallelizing.
4. **Open the cross-plane-correlation question** as its own ADR (or a 0008 addendum)
   when the floor is in — it is the one genuinely new design decision.

---

*Filed under `docs/design/reviews/` (a new directory — move it if the repo prefers
another home for assessment notes). Verified against the working tree at the time of
writing; re-check the wiring claims if a custodian/telemetry integration PR has landed
since.*

[p7]: ../proposals/accepted/0007-milestone-4-production-metadata-backend.md
[p8]: ../proposals/draft/0008-management-and-administration.md
[p9]: ../proposals/draft/0009-d-server-performance.md
