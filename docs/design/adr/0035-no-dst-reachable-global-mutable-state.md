---
created: 24.06.2026 19:32
type: adr
status: Proposed
tags:
  - adr
  - testing
  - correctness
  - dst
  - determinism
  - observability
---
# 0035. No DST-reachable shared mutable global state may influence a campaign outcome

## Context

ADR-0009 makes deterministic simulation testing (DST) the correctness spine: a
campaign runs in a single-threaded simulated world and **every outcome is a pure
function of its seed**, so a bug found at a seed reproduces forever. madsim
virtualises the *runtime* — scheduler, clock, network, randomness — and the
campaign draws all of its own randomness from the seed (`ChaCha8Rng` seeded from
`madsim::runtime::Handle::current().seed()`). That makes the work *inside* one
campaign deterministic.

madsim does not virtualise the *process*. Anything stored in a `static` lives in
the OS process, outside the simulated world, and is **not** reset between tests or
between seeds. Under `cargo test` the property tests of a campaign run on parallel
OS threads sharing one process, so any process-global mutable state they touch is
shared across threads, across the madsim seed sweep, and across test boundaries —
a channel through which a campaign outcome can come to depend on thread and
execution ordering rather than on the seed.

This is not hypothetical. It has already bitten the project through `tracing`.
The custodian DST campaign (`crates/dst/tests/custodian.rs`) asserts a
durability-emission property by capturing `tracing::info!(monotonic_counter.…)`
events emitted from the production repair path
(`crates/custodian/src/reconstruction.rs`) through a per-test scoped
`with_subscriber(...)`. `tracing` caches each callsite's *interest* —
`never` / `sometimes` / `always` — in process-global state, computed once on
first touch from the **global default** subscriber. A callsite first reached while
no global default is installed (`NoSubscriber`) latches `never` and short-circuits
*before* any later scoped subscriber is consulted. So a non-capturing property
that wins the race to the callsite poisons the cache, and the capturing property
silently observes nothing — an order-dependent, not seed-dependent, failure. The
same hazard was recorded earlier for the GC telemetry tests (issue #214), where
it was contained by giving the asserting leg its own test binary.

The custodian campaign was de-flaked by `install_metric_dispatch()`
(`crates/dst/tests/custodian.rs`): a `Once`-guarded
`set_global_default(tracing_subscriber::registry())` that installs a permissive
no-op global default before any callsite is hit, so interest can never latch
`never`; scoped subscribers still override it for routing. **That fix is correct
for the observed callsite and is not in question here.** What it exposes is the
gap this ADR closes:

- The fix is **convention, not a barrier** (issue #243). It is correct only
  because every one of the campaign's seven `#[madsim::test]` functions remembers
  to call it first. A property added without the call silently reintroduces the
  flake. Nothing in the type system or harness forces it.
- The fix **swallows its own failure** (`let _ = set_global_default(...)`, issue
  #243). `set_global_default` errors if a default is already installed; dropping
  the `Result` means the determinism guarantee can vanish with no signal.
- The fix is **specific to `tracing`** (issue #242), but the hazard is general:
  any DST-reachable `static mut`, `lazy_static!`, mutable `OnceLock`/`OnceCell`,
  `thread_local!`, or atomic used as cross-call shared state can defeat
  seed-determinism the same way. There is no rule and no enforcement.
- The fix lives in **one test file**, whereas ADR-0009 locates determinism in the
  *substrate*. The next DST binary inherits nothing.

There is genuine choice in how to close this — a custom test attribute, a harness
fixture, a lint, an explicit no-statics rule — so it is recorded as a decision
rather than applied as a cleanup. Issues #242 (the general rule) and #243 (the
barrier mechanics) are companions and are decided together here.

A process note, because it is the root cause of how the invariant eroded
unnoticed: the de-flake was bundled into PR #241 / commit `4fd5c32`, whose subject
is the unrelated d-server overload fix (#205). A seed-determinism invariant
patched as a side effect of an unrelated change, with no ADR and no enforcement,
is precisely how such an invariant decays. (Surfaced by the PDCA Act review of
`issue_205`, 2026-06-23.)

## Decision

**No shared mutable global state reachable from a DST campaign may influence its
outcome. Where such state is unavoidable (third-party process globals), it MUST be
neutralised by the DST substrate, once, before any campaign runs — never by
per-test convention.**

Concretely:

1. **Prevention is the default.** Production and DST-reachable code MUST NOT
   introduce process-global mutable state — `static mut`, `lazy_static!`,
   `OnceLock`/`OnceCell` holding mutable or interior-mutable values,
   `thread_local!`, `set_global_default`, or atomics used as cross-call shared
   state. Sources of nondeterminism (time, randomness, faults, IO) MUST be
   injected through an explicit seam, following the established `testkit` pattern
   (the `Clock` trait, a seed-derived `ChaCha8Rng`, the fault-injector traits) —
   not reached through a global.

2. **Unavoidable third-party globals are contained at the substrate.** `tracing`'s
   per-callsite interest cache cannot be removed, so every DST binary that emits
   or asserts on `tracing` events MUST install the permissive global default
   exactly once, before any campaign runs, **through a single shared entry point
   that a property test cannot bypass** — a harness fixture or a campaign-test
   macro that wraps `#[madsim::test]`. "Forgetting the barrier" MUST be
   unrepresentable, not merely discouraged. This generalises today's
   `install_metric_dispatch()` from a per-test call into a substrate property,
   which is where ADR-0009 says determinism belongs.

3. **The barrier MUST fail loud.** Installing the global default MUST surface its
   error rather than discard it: if a default is already set, the run fails. A
   silent no-op is itself a determinism hazard — the guarantee disappears with no
   signal — and is not acceptable.

4. **The rule is enforced by a gate, with a documented allowlist.** A lightweight
   `cargo xtask` check (the ADR-0016 single-source-of-truth style, alongside the
   existing `cargo-deny` gate) MUST scan DST-reachable crates for the forbidden
   patterns in (1) and fail CI on new occurrences. Audited seed-safe uses are
   allowlisted with a stated reason. The initial inventory:
   - **Seed-safe (allowlisted):** `ManualClock`'s `Arc<AtomicU64>` (single
     simulated executor; all clones observe the same seed-driven time);
     per-test `Mutex` / `Arc<Mutex>` fixtures created fresh inside a campaign
     (`MemMeta`, `MemCoordination`, the test `MetricCapture`).
   - **Latent hazard (must be addressed before it becomes reachable):** the
     `Gateway` id allocators `next_inode` / `next_chunk` (`crates/server/src/lib.rs`),
     `AtomicU64` counters that are deterministic today only because `Gateway` is
     not yet exercised under DST. A campaign over the server path would leak
     allocation order across parallel tests; the allocator MUST move behind a
     seam before such a campaign is written.

5. **The failure mode itself gets a regression test.** The containment in (2)
   relies on a bare `tracing_subscriber::registry()` causing callsites to cache a
   non-`never` interest — a `tracing-subscriber` behaviour nothing currently pins.
   A test MUST reproduce the poison race (a non-capturing property reaching the
   callsite first) and prove capture still succeeds, so a dependency upgrade that
   changed `Registry` interest semantics turns CI red instead of silently
   re-breaking determinism.

### Implementation requirements

These follow from the decision and are tracked as the implementation arm of
issues #242 and #243; they are not built by this ADR:

- A campaign-test entry point (macro or fixture) in `crates/dst` or
  `crates/testkit` that installs the global default once and is the only way to
  declare a custodian DST property; migrate the seven existing
  `#[madsim::test]`s onto it and delete the per-test `install_metric_dispatch()`
  calls.
- Surface the `set_global_default` error in that entry point (no `let _`).
- The `cargo xtask` statics gate and its allowlist, runnable locally and in CI.
- The poison-race regression test described in (5).
- Move the `Gateway` id allocation behind a seam, or document why it remains out
  of DST reach, before any DST campaign exercises the server path.

## Consequences

- Seed-determinism stops being an honour-system property of careful test authors
  and becomes a substrate guarantee plus a machine-checked gate — the same posture
  ADR-0009 already takes for licensing, DCO, and format conformance.
- The tracing barrier becomes impossible to forget: a new custodian property
  cannot be written without going through the entry point that installs it.
- A real install failure becomes a loud test failure instead of a silent loss of
  the guarantee.
- New cost: a small allowlist to maintain, and the gate may flag pre-existing
  benign statics that then need an explicit, reasoned annotation. This is accepted
  as the price of making the rule real.
- The `Gateway` allocator is now on record as a latent hazard with a required
  action, rather than an unstated assumption that DST happens not to touch it.
- This narrows future design space: genuinely global runtime state (a process-wide
  metrics registry, a shared cache) is now a decision that must clear this ADR, not
  a convenience that can be added quietly.

## Revisit when

- A feature needs genuinely process-global runtime state that cannot be expressed
  as an injected seam — at which point the trade-off and its DST containment must
  be designed explicitly.
- `tracing` / `tracing-subscriber` changes its callsite-interest or
  global-default semantics in a way that invalidates the containment in (2), which
  the regression test in (5) is designed to catch.
- A DST campaign is extended to cover the server / `Gateway` path, forcing the
  latent-hazard action in (4).
