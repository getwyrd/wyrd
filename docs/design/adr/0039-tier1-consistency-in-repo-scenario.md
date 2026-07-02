---
created: 28.06.2026 00:55
type: adr
status: Accepted
tags:
  - adr
  - testing
  - custodian
  - consistency
---
# 0039. Tier-1 consistency-over-repair as an in-repo Rust scenario; literal Jepsen deferred

## Context

Proposal 0005 (M3 — Custodians) §13.2 (`0005:408`) names, as a Tier-1
verification leg, a **Jepsen** consistency harness asserting Wyrd's consistency
contract over the custodian repair/reconstruction path under partitions and
crashes. "Jepsen" there is the recognizable shorthand for the *property* to be
verified — ADR-0015's contract (namespace ops linearizable; per-file writes
linearizable at the home zone; read-your-writes and monotonic reads; and, over
the repair path, read-after-commit, no torn/stale reads, and commit-point-atomic
repair that is neither lost nor duplicated). It was written before the data model
was built out.

Realizing that leg *literally* — Clojure/Jepsen with the Elle checker — does not
fit Wyrd. Elle's list-append and register checkers presuppose a **mutable,
linearizable register**: a key whose value is overwritten and read back over
time. Wyrd is an **immutable, single-write-per-key** object store; a chunk is
written once and never mutated. To feed Elle, the "list" or "register" has to be
invented client-side, so the checker validates the harness's own bookkeeping, not
Wyrd's contract. Eight iterations of the M3.11 work (#250) confirmed this
empirically: every literal-Jepsen attempt produced only **vacuous histories**.

The forces:

- The verification leg is **real and required** — the consistency contract over
  the repair path is exactly the kind of property that must be exercised under
  real partitions and crashes, not merely modelled (ADR-0009: a bug-finding run
  promoted to a permanent regression; the deferred-≠-unbuilt discipline).
- The literal public-Jepsen **credibility artifact** has independent value as an
  externally-recognizable proof, but is blocked on substrate that can yield a
  non-vacuous, checkable history against Wyrd's actual model.
- These tiers need privileged / containerized environments and so must stay out
  of the unprivileged, container-free `cargo xtask ci` merge gate (ADR-0016).
- Proposal 0005 is an accepted, immutable plan of record (ADR-0037); its §13.2
  text is frozen and must not be back-patched.

The genuine decision is therefore *how* to realize §13.2's Tier-1 consistency
leg now, given that its literal tactic does not fit the data model, while keeping
the property it stands for actually verified.

## Decision

We will realize proposal 0005 §13.2's Tier-1 "Jepsen consistency over the repair
path" as an **in-repo Rust scenario** that drives the **production** custodian
reconcile path (`custodian::reconcile_step` → `reconstruction::reconcile`)
against a real containerized D-server cluster, injects **both** a crash (a killed
node, `docker kill`) and an **isolation fault** (a node made unreachable
mid-repair via `docker pause`, then healed with `unpause`), and asserts the
ADR-0015 contract over the repair path directly: read-after-commit, no torn/stale
reads, commit-point-atomic repair that converges **exactly once** across the heal.
The isolation fault is a **process-freeze nemesis** (`docker pause` suspends the
container via the freezer cgroup), not a network-level packet-drop partition;
because Wyrd's D-servers are *dumb* storage that initiate no commits of their own,
a frozen node and a network-partitioned one are observably equivalent to the
custodian repair path under test — the node is unreachable, repair proceeds around
it, and the contract must hold across the heal. A stronger network-level partition
that keeps the isolated node *live* is an additive upgrade to this leg (#399), not
a change to the contract asserted. Its routing decision is a
test-observable value, and it runs in a dedicated **privileged** CI job kept out
of `cargo xtask ci` (ADR-0016). This is the leg shipped by #250, mirroring the
two merged sibling legs (#195 disk-fault, #196 kill-reconstruct).

We will **defer** the literal public Jepsen/Elle credibility artifact to a
follow-on (#329), to be taken up once the substrate supports a **non-vacuous**
run against Wyrd's consistency model. That follow-on, not this leg, is where a
literal Jepsen credibility claim is earned.

This ADR records the testing-methodology choice (parallel in spirit to ADR-0009);
it **refines how proposal 0005 §13.2's Tier-1 Jepsen line is realized** and does
**not** edit proposal 0005, whose accepted file stays byte-for-byte frozen
(ADR-0037; supersession/refinement is recorded in the index, not on the frozen
file — ADR-0038). Proposal 0005 otherwise stands.

## Consequences

- The consistency contract over the repair path is now **actually exercised**
  under crash and node-isolation faults, replacing inert dispatch scaffolding. The
  scenario binds the production reconcile API at compile time, so an API
  regression fails the merge gate even though the live run is off-Check.
- The verification is **in-repo and maintainable** — Rust the team already owns,
  no Clojure/JVM toolchain, no client-invented observable. The trade-off: it is
  **not** the externally-recognizable public Jepsen artifact. That credibility
  claim is explicitly deferred (#329), not made by this leg.
- The repair **trigger** was, when this record was authored, a sanctioned test
  stand-in (`repair::enqueue_repair`) because no production path yet enqueued
  repair for a simply-missing fragment; the reconstruction path itself is
  genuinely traversed. #330 has **since landed** (scrub now enqueues repair for a
  placed-but-missing fragment), so the stand-in can be dropped in favour of the
  production trigger.
- Reversing or extending this is cheap and additive: the literal-Jepsen artifact
  is a separate follow-on that supersedes nothing here; if it later becomes the
  preferred Tier-1 substrate, a further ADR records that.
- Refines proposal 0005 §13.2 (`0005:408`); aligns with ADR-0015 (the contract
  asserted), ADR-0009 (bug-finding run → regression), and ADR-0016 (privileged
  tiers out of `xtask ci`); follows ADR-0037 / ADR-0001 / ADR-0038 (the frozen
  proposal is not edited; the relationship is recorded in the index).
