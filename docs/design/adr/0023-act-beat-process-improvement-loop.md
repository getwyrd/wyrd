---
created: 20.06.2026 13:40
type: adr
status: Proposed
tags:
  - adr
  - process
  - quality
  - ci
---
# 0023. Adopt the Act beat: a process-improvement loop

## Context

Wyrd's quality process is strong on three of the four PDCA beats and silent on the fourth. *Plan* is carried by specs, ADRs, and proposals, each with its own change process. *Do* is constrained from day one by deterministic simulation testing against the `testkit` abstractions (ADR-0009). *Check* is the single-sourced `cargo xtask ci` gate wall — fmt, clippy, build, test, conformance, cargo-deny — run identically on a laptop and in CI (ADR-0009, ADR-0016), backed by `require-issue`, DCO, and ADR-immutability.

What has no home is *Act*: the deliberate step where a **recurring** defect class or process friction is turned into a systemic guardrail rather than fixed instance by instance. DST already embodies exactly this reflex for correctness — a seed that finds a bug is committed as a permanent regression test, so the bug cannot return unnoticed. But there is no analogue for the lessons that are not a single reproducible bug: a review miss that recurs, a class of CI flake, an onboarding friction, a convention violated because it was never written down. Without a place to record these and a rule that each produces an action, the same lesson is re-learned every cycle and never compounds into a rule.

The decision is whether to adopt the fourth beat, and how to do so without ceremony that outweighs the payoff at the current founding-maintainer, bootstrap scale (GOVERNANCE). It is deliberately independent of adopting any external PDCA tooling — this is the one beat Wyrd lacks, and it stands alone.

## Decision

**We will adopt a lightweight Act beat, recorded append-only in `docs/process/act-log.md`.** Each entry MUST name three things: the **trigger** (what recurred or went wrong), the **root-cause class** (not the single instance), and a concrete **outcome** — exactly one of: a new or changed `cargo xtask` gate; a new test, lint, or written convention; a checklist item; a follow-up ADR or proposal; or an explicit *"no action, accepted"* with its reason. An Act entry that names no outcome is not done.

**Cadence is milestone-driven, with ad-hoc entries for notable incidents.** A short retrospective accompanies each milestone close — the proposals already mark those boundaries (proposal 0002 and the per-milestone plans) — and an entry may be written at any time when an incident warrants one. The log is kept short on purpose; the discipline matters more than the ceremony.

**The Act beat routes into Wyrd's existing artifacts; it does not replace them.** Durable, machine-checkable rules become `xtask` gates so they are single-sourced and enforced (ADR-0016, ADR-0009); normative decisions become ADRs; structural change becomes proposals; correctness lessons remain DST regression seeds (ADR-0009). The act-log is the **ledger and index** of process change — what was learned and which guardrail it produced — not a parallel rulebook that gates could drift from.

**The log is distinct in kind from an ADR.** An ADR records a *decision and its why* and is immutable once Accepted; an act-log entry records a *process lesson and the guardrail it produced*. The log is therefore **not** immutability-gated — it is a working ledger, edited as an entry gains its outcome — but entries are not deleted, only superseded by later ones.

## Consequences

- A recurring lesson gains exactly one home and a rule that forces it toward a concrete guardrail; the project compounds process quality the way DST compounds correctness coverage, instead of re-learning the same lesson each cycle.
- A small recurring cost lands at milestone boundaries. The standing risk is the log decaying into an unread graveyard — mitigated by the "every entry names an outcome" rule and by tying cadence to milestone closes that already happen.
- Adopting the beat natively keeps Wyrd's gates single-sourced in `xtask` and the tree free of a second toolchain or license burden; it also de-risks a later, fuller PDCA-harness adoption by proving the highest-value beat first, without committing to Python, Copier, Obsidian, or a relicense (see `docs/process/pdca-harness-adoption-plan.md`).
- One new directory, `docs/process/`, enters the tree; the log is plain Markdown, consistent with docs-live-with-code and diagrams-as-code conventions.
