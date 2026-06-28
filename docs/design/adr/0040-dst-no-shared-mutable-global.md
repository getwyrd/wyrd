---
created: 28.06.2026 14:30
type: adr
status: Proposed
tags:
  - adr
  - testing
  - correctness
  - ci
---
# 0040. No shared mutable global may influence a DST outcome; DST observes through injected seams

## Context

ADR-0009 makes deterministic simulation testing the spine: every component is
written against abstract time/network/disk seams in `testkit`, the whole zone runs
in a single-threaded simulated world, and **a bug reproduces from its seed** (a
bug-finding seed becomes a permanent regression). That invariant is only as strong
as the guarantee that *nothing outside the seed* can change a run's outcome.

The Tier-0 custodian campaign surfaced a counter-example. The durability-emission
property (`crates/dst/tests/custodian.rs`, proposal 0005 §13 property 6) asserts
the custodian's metrics by capturing `tracing` events through a **scoped**
`with_subscriber(...)` layer. The custodian emits those metrics straight to the
process-global `tracing` dispatcher — `emit_*` → `tracing::info!(monotonic_counter.* = …)`
(`crates/custodian/src/reconstruction.rs:482-538`, and the sibling `gc.rs` /
`scrub.rs` / `rebalance.rs` emitters; 22 callsites in all). The bridge that
consumes them in production is itself a `tracing` layer
(`DurabilityTelemetry::metrics_layer`, `crates/custodian/src/telemetry.rs`,
ADR-0012). The defect is in how `tracing` resolves a callsite:

- Each callsite caches its *interest* once, in a per-callsite `AtomicU8`,
  registration guarded by a one-shot state machine
  (`tracing-core` `callsite.rs`: `DefaultCallsite::{register, interest}`).
  `interest()` returns the cached value **without consulting the current
  dispatcher**, and the event macro short-circuits on `interest.is_never()`.
- Only `set_global_default` rebuilds that cache (`dispatcher.rs` →
  `register_dispatch` → `rebuild_interest`). The scoped, thread-local
  `set_default` that `with_subscriber` uses does **not** — it swaps a thread-local
  and bumps a scoped counter.

So the **first thread to reach a callsite fixes its interest process-wide.**
`cargo test` runs `#[madsim::test]` functions on parallel OS threads sharing
process globals; each spins its own madsim runtime, but the `tracing` interest
cache is shared across them. If a non-capturing property reaches a custodian
callsite first (global default = `NoSubscriber`, interest `never`), the callsite
caches `never`, and the durability property's later scoped capture records nothing.
The poisoning is decided by inter-test OS-thread scheduling **outside** the
simulated world, so the same `MADSIM_TEST_SEED` can pass or fail depending on
sibling-test timing — a determinism hole the simulator cannot see, and a direct
violation of ADR-0009's "reproduces from a seed."

The existing mitigation, `install_metric_dispatch()` (#241), installs a permissive
**process-global** default once so callsites register as enabled rather than
`never`. It is correct for the one observed callsite, but it (a) relies on every
property calling it first **by convention**, enforced by nothing
(`custodian.rs:354`, the per-test call sites — the subject of companion #243), and
(b) it treats the symptom (the `tracing` cache) rather than the class (any shared
mutable global reaching a DST outcome).

Scope, from a sweep of the DST-reachable production crates: the `tracing` interest
cache is the **only** such global today. `crates/core` (the read/write paths the
`network.rs` / `concurrency.rs` campaigns exercise) emits **no** `tracing`
callsites, so those campaigns are clean and need no workaround; the 22 emitting
callsites are all in the custodian loops. There are no `lazy_static`/`once_cell`
globals, ID generators, `HashMap`-iteration-order dependencies, real clocks, or
`std::thread` reachable from the simulated paths. The class is real but currently
has exactly one member — which is why the rule must be stated and the one member
fixed, not a heavyweight reachability lint built for offenders that do not exist
yet.

Provenance: surfaced by the Act review of PDCA cycle `issue_205` (PR #241) and
routed as a design issue (`process/act-log.md`, 2026-06-23). Companion: #243
(the convention-not-a-barrier and swallowed-error half).

## Decision

**The invariant (refining ADR-0009's enforcement clause): no shared mutable global
may influence a DST outcome.** A DST run must be a pure function of its seed; any
process-global an `#[madsim::test]` can observe, and whose value depends on
anything other than the seed (thread scheduling, sibling tests, load order), is a
determinism defect to be removed — not papered over per call site.

The mechanism is **observation through an injected seam, not a process-global**,
plus a structural barrier as the bridge while the seam lands:

1. **Seam (the durable fix).** Custodian metric emission moves behind an injected
   **sink** carried on the reconcile contexts (`ReconstructionContext` and the GC /
   scrub / rebalance siblings), homed with the existing telemetry seam
   (`crates/custodian/src/telemetry.rs`). Production injects a sink that forwards to
   the `tracing` / OpenTelemetry bridge — the ADR-0011 metric set and the ADR-0012
   dual-export are **unchanged**; only the dispatch path becomes injectable. DST
   injects a deterministic in-process capturing sink. The process-global `tracing`
   dispatcher is then present **only on the production sink, never on the DST
   observation path**, so the interest-cache race cannot reach a seed's outcome.
   `install_metric_dispatch()` and the scoped `with_subscriber` captures are removed
   with it.
   - Fidelity note: DST then asserts the **metric values** the custodian computes,
     not the OTel/Prometheus wiring. That wiring (ADR-0012's dual-export) stays
     proven by the per-slice telemetry tests, which is the right division — a
     determinism-critical campaign should not depend on a global export bridge.

2. **Barrier (the bridge, and the resolution of #243).** Until the seam lands, the
   permissive global default is installed as a **structural barrier**, not a
   per-test convention — through a single fixture / test wrapper every property is
   constructed through (so "forgot to install it" is unrepresentable), and the
   `set_global_default` result is **surfaced, not swallowed** (`custodian.rs:354-358`).
   This closes #243 immediately and de-risks the window before the seam is wired.

3. **Audit, not a gate.** A new shared mutable global reaching a simulated path is a
   deliberate violation of this ADR, caught by review against a documented checklist
   (and, optionally, an advisory `cargo xtask` grep for new `static` / `OnceLock` /
   `set_global_default` in the DST-reachable crates). It is **not** a gating lint:
   "reachable from `#[madsim::test]`" is not precisely computable, and a blunt scan
   flags benign one-shots (`Once`, the test fixtures' own `AtomicBool`, `NEEDS_SUDO`).
   The wall is the stated rule plus review, not a machine that cries wolf.

This **refines** ADR-0009's determinism-enforcement clause; it does not edit it.
ADR-0009's madsim / two-tier / CI decisions all stand. The frozen ADR-0009 file is
left byte-for-byte untouched; the relationship is recorded in the index (ADR-0038 /
ADR-0037 / ADR-0001).

## Consequences

- The durability-emission property becomes a **pure function of its seed** again:
  with observation behind an injected sink, no sibling test or thread order can
  change what a seed captures. ADR-0009's reproduces-from-a-seed invariant holds for
  the one place it was leaking.
- **#242 and #243 are co-resolved** by one decision: the seam removes the global
  from the DST path (#242), and the barrier-as-bridge makes the install
  unforgettable and its error visible (#243). The implementation lands as follow-on
  PRs (the seam in `custodian` + `dst`; the barrier/fixture + un-swallowed error),
  not as part of this ADR — design records intent.
- The custodian gains a small **telemetry seam** on its contexts. Cost: a production
  change to the emission path. It is consistent with ADR-0011/0012 (the metric
  contract and dual-export are preserved; only dispatch becomes injectable) and with
  ADR-0009's own philosophy (every observable output reached through a seam, as
  clock/network/disk already are). Implementation must confirm the production sink
  still dual-exports.
- The rule is **general**; the enforced surface today is **one** global. New
  offenders are made deliberate violations rather than silent gaps — the audit is a
  checklist, accepted as heuristic rather than a hard gate, because the class is real
  but sparsely populated and a precise reachability lint is not worth its
  false-positive cost yet. If the class grows, a later ADR can promote the audit to a
  gate.
- Refines ADR-0009 (the determinism invariant and its enforcement); preserves
  ADR-0011 (durability telemetry) and ADR-0012 (OpenTelemetry dual-export); follows
  ADR-0037 / ADR-0001 / ADR-0038 (the frozen ADR is not edited; the relationship is
  recorded in the index). Closes the design half of #242 and #243.
