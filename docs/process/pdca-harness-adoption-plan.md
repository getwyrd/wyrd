---
created: 20.06.2026 13:40
updated: 20.06.2026 14:40
type: plan
status: piloted
author: Eduard Ralph
tags:
  - process
  - pdca
  - integration
  - cross-project
---
# Plan: making the PDCA harness applicable to Wyrd

> A roadmap for adopting the [pdca-harness](https://github.com/eduralph/pdca-harness)
> quality cycle in Wyrd. It is written to be shared with both projects: it lists
> the changes the **harness** needs to become genuinely project-neutral and the
> concretizations **Wyrd** must supply to consume it — and sequences them so the
> highest-value, lowest-risk step ships first and nothing is adopted before it
> earns its place. Early in the project the discipline matters more than the
> ceremony; this plan is deliberately phased, not all-or-nothing.

> [!note] Status update — 2026-06-20: **Phase 2 pilot shipped.**
> When this plan was first drafted, Workstream A (neutralize the harness) was assumed
> to be outstanding. Verifying against the harness repo showed it had **already
> shipped** at template `v0.24.0`: it is **Apache-2.0** (no relicense needed), it
> supports **wholesale gate delegation** (`[gates] runner` / `cmd = "cargo xtask ci"`),
> and its model ships as **plain Markdown** (`PCDA/quality-cycle/`, no Obsidian). So
> Phase 1 was a no-op, and the **Phase 2 out-of-tree pilot was executed**: a project
> rendered from the harness now lives beside this repo at **`../wyrd-pdca`**. It
> delegates every gate to `cargo xtask ci` through a thin `engine/xtask.sh` wrapper
> (no gate is re-declared), fills `docs/INTEGRATION.md` with Wyrd's values, and was
> verified end-to-end (`pdca gates --working-tree` drives `cargo xtask ci` to a green,
> gating pass; a toy bundle runs Plan→Do→Check→sign-off; its own Act log is live).
> **Act stays complementary, not either/or:** Phase 0's native Act beat (ADR-0023, the
> cheap proven first step) and the harness's richer Act tooling coexist exactly as
> ADR-0023 frames it — proving the highest-value beat natively *de-risks* the fuller
> harness adoption. The annotations below mark what is done; this tree is untouched.

## Goal

Bring PDCA's four beats — Plan, Do, Check, **Act** — to Wyrd *without*:

- introducing a **second source of truth for gates** (Wyrd's gates are single-sourced in `cargo xtask ci`, ADR-0016 / ADR-0009 — that must stay true);
- adding a **license burden** incompatible with Wyrd's Apache-2.0 + `cargo-deny` permissive allowlist (ADR-0003);
- forcing a **second toolchain or editor** (Wyrd is pure-Rust + plain-Markdown-in-repo; no Obsidian requirement);
- imposing **ceremony ahead of need** at the current founding-maintainer / bootstrap scale (GOVERNANCE).

Success means PDCA *adds* the Act loop and a structured per-cycle bundle on top of Wyrd's existing Plan/Do/Check machinery, while every gate stays defined exactly once.

## Where the two processes already meet

Wyrd implements three of PDCA's four beats natively, often more rigorously than a generic template would:

| PDCA beat | Wyrd's existing equivalent |
|-----------|----------------------------|
| **Plan** | `specs/` (RFC-2119 + conformance vectors), ADRs (immutable), proposals (draft → accepted → implemented) |
| **Do** | DST-constrained implementation against `testkit` abstractions (ADR-0009) |
| **Check** | `cargo xtask ci` — fmt, clippy `-D warnings`, build, test, cargo-deny, conformance — run identically on laptop and CI; plus `require-issue`, `dco`, `adr-immutability` |
| **Act** | native home proposed in `docs/process/act-log.md` (ADR-0023); the pilot adds richer tooling (`../wyrd-pdca/process/act-log.md` + `pdca act-index`/`act-log`) — complementary, per ADR-0023's own framing |

The integration work is therefore **not** "install a process Wyrd lacks." It is "make PDCA neutral enough to wrap the process Wyrd already has, and fill the one gap (Act)."

## Workstream A — make the harness project-neutral (pdca-harness side)

These are the changes that let the harness sit over an opinionated host like Wyrd rather than dictate to it.

1. **Relicense to a permissive license (Apache-2.0).**
   The harness is GPLv3 today; the rendered driver and templates land *inside* the consuming repo, so a GPLv3 template makes the rendered output a derivative that clashes with Wyrd's Apache-2.0 and would be denied by Wyrd's `cargo-deny` allowlist (which already denies AGPL/BSL/SSPL). Relicensing to **Apache-2.0** (matching Wyrd) removes the blocker, keeps DCO sign-off semantics aligned (ADR-0003 §1), and broadens the harness's own adoptability. This is a prerequisite for any in-tree use.

2. **Support a *delegated* gate, not a duplicated one.**
   The harness's headline feature is single-sourced gates defined in `pdca.toml` and run by both the driver and CI. Wyrd already single-sources its gates — in `cargo xtask ci`, deliberately in Rust and not YAML (ADR-0016). Neutrality requires that `pdca.toml` can **reference an external gate runner wholesale** (`leaves_mode = "command"` invoking `cargo xtask ci` and its named sub-gates) rather than re-declaring the gates. The harness must treat the host's runner as the source of truth and only *orchestrate* it — never become a parallel definition that can drift.

3. **De-Obsidian the model reference.**
   The vendored quality-cycle model ships as an Obsidian vault. Wyrd's convention is plain Markdown in the repo, published to getwyrd.dev. The model doc must be **readable and reviewable as plain Markdown** with no Obsidian dependency (vault config optional, not required).

4. **Make the Plan artifact a pointer, not a prescribed shape.**
   The template assumes Plan ≈ a single brief/spec. Wyrd's Plan is a *set* of artifacts (ADR / proposal / spec) with distinct change processes. Neutrality means PDCA's Plan step accepts **"the host's existing planning process" as the artifact** — a reference to the relevant ADR/proposal/issue — rather than imposing its own document format.

5. **Compose with the host's CI gates instead of replacing them.**
   The harness must accommodate Wyrd's `require-issue`, `dco`, and `adr-immutability` checks as the issue/PR-governance layer, mapping PDCA's "init from brief" onto a GitHub issue that already satisfies `require-issue`. PDCA's agent-merge-guard hooks should *supplement* these, not duplicate them.

## Workstream B — Wyrd-side concretizations (INTEGRATION.md fill-ins)

What Wyrd supplies once the harness is neutral. These are the `docs/INTEGRATION.md` TODOs the template asks the host to fill:

| Concretization | Wyrd value |
|----------------|-----------|
| Tracker | GitHub issues (already gated by `require-issue`) |
| Branch target | `main` |
| Gate command | `cargo xtask ci` (delegated, per A.2) — plus named sub-gates (`fmt`, `clippy`, `test`, `conformance`, `deny`) |
| Conformance rule | `cargo xtask conformance` against `specs/conformance/` (ADR-0002) |
| Sign-off authority | Maintainers, per GOVERNANCE (founding maintainer during bootstrap) |
| Plan reference | the issue's linked ADR / proposal / spec |
| **Act log** | `docs/process/act-log.md` (see the companion draft ADR) |

## Phased rollout

Ordered so unique value ships first and each phase gates the next.

- **Phase 0 — adopt the Act beat natively (now, zero harness dependency).** — **PROPOSED (ADR-0023).**
  The companion ADR-0023 (status: Proposed) decides to land Act natively as an append-only
  `docs/process/act-log.md`, routing lessons into `xtask` gates / ADRs / proposals. It stands on its
  own with no harness dependency, and — per its own Consequences — *de-risks* the fuller harness
  adoption by proving the highest-value beat first. It is **complementary** to the pilot's Act tooling,
  not superseded by it; the two can converge (the pilot's log pointing at the native one) if both stay.

- **Phase 1 — neutralize the harness (pdca-harness side).** — **DONE upstream (no work needed).**
  Workstream A (relicense to Apache-2.0; delegated-gate support; de-Obsidian; Plan-as-pointer) had
  all already shipped in pdca-harness by template `v0.24.0`. Verified against the repo, not assumed.

- **Phase 2 — pilot the harness out-of-tree against Wyrd.** — **DONE.**
  Rendered into the sibling project `../wyrd-pdca`, which **wraps** `cargo xtask` via `engine/xtask.sh`
  and never re-declares gates. `INTEGRATION.md` filled (Workstream B). Verified end to end with
  stub/offline leaves: `pdca gates --working-tree` drives `cargo xtask ci` to a green gating pass, and a
  toy bundle runs Plan→Do→Check→sign-off; no duplicated gate definitions, no new denied licenses, no
  second CI truth, no forced Obsidian, this tree untouched. **Remaining:** flip `leaves_mode = "command"`
  and run **one real Wyrd issue** with live model leaves before the keep/discard call.

- **Phase 3 — evaluate scale features only when volume justifies them.** — *unchanged (not yet due).*
  PDCA's batch / lane-concurrency / sign-off-queue features earn their keep with many concurrent issues and agent fleets. Hold them until contributor and issue volume (and any move past founding-maintainer bootstrap) makes them pay; otherwise they are ceremony.

## Decision gates / open questions

- **Does relicensing the harness happen, and to Apache-2.0 specifically?** — **RESOLVED:** the harness is already Apache-2.0 (`v0.24.0`); no relicense needed.
- **Can `pdca.toml` reference an external runner without re-declaring gates?** — **RESOLVED: yes.** `[gates] runner` + a bare `subcmd` (or a full `cmd = "cargo xtask ci"`) delegates wholesale; the pilot proves it driving `cargo xtask ci` with zero gate re-declaration. The non-negotiable (ADR-0016) holds.
- **Where does the rendered driver live** — in-tree or out-of-tree? — **RESOLVED: out-of-tree sibling** (`../wyrd-pdca`). Promote in-tree only if it later earns its weight.
- **Does the native-Act path (Phase 0) alone suffice?** — **STILL OPEN, and now complementary:** ADR-0023's native act-log stands on its own (Proposed); the pilot adds richer tooling on top. The live question is whether the **pilot** earns its keep once a real issue runs with live leaves — and, if both stay, whether to converge the two Act logs (the pilot's pointing at the native one). Discard the pilot cleanly if it doesn't pay; the native beat is unaffected either way.

## Success criteria

PDCA is "applicable to Wyrd" when, and only when, all hold:

1. Gates remain defined **once**, in `cargo xtask`; PDCA orchestrates, never re-declares.
2. No license incompatible with Apache-2.0 / the `cargo-deny` allowlist enters the tree.
3. No mandatory second toolchain or editor.
4. The **Act** loop is live and feeding xtask gates / ADRs / proposals.
5. Every adopted feature is justified by current scale, not anticipated scale.
