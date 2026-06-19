---
created: 17.06.2026 00:56
type: governance
status: draft
tags:
  - governance
  - process
  - maintainers
---
# Governance

> **Status: DRAFT — complete, pending ratification.** This records the project's decision-making structure. All roles, the architecture board, and the amendment process are now specified — **no [OPEN] items remain**. Under the amendment rule below, ratification is the unanimous act of the maintainers (currently just the founding maintainer); merging this document is that act.

## Purpose

Wyrd is an open-source project built initially by a small team and dependent on outside contributors (see [ADR-0001](../design/adr/0001-record-architecture-decisions.md)). This document makes explicit *who holds which responsibilities* and *how decisions are made*, so that authority is stated rather than assumed.

It complements, and does not override, the per-class processes documented elsewhere: architecture decisions follow [ADR-0001](../design/adr/0001-record-architecture-decisions.md), enhancement proposals follow `../design/proposals/`, and specifications follow `../design/specs/`.

## Roles

Participation is a ladder of four levels, each earned from the one below. Higher rungs carry more authority and more responsibility; all of them rest on the same contribution-under-DCO basis (ADR-0003). The architecture board (further below) is **not** a rung on this ladder — it is a body drawn from the maintainers.

| Level | In one line | GitHub mapping |
|-------|-------------|----------------|
| **Contributor** | anyone who opens an issue or PR | — (any account) |
| **Associate** | a trusted contributor with triage rights, on the path to maintainer | repo **Triage** |
| **Maintainer** | reviews and **merges**, triages, elects the architecture board | `maintainers` team, **Maintain** |
| **Founding maintainer** | the genesis seed that bootstraps the project | org **Owner / Admin** |

### Contributor

Anyone who opens an issue or a pull request. Contributions are accepted under the project's license and DCO sign-off (ADR-0003). No special status is required, and most participants never need more.

### Associate

A contributor recognised for sustained, quality work, granted **triage** rights — labelling, assigning, and shepherding issues and pull requests, and reviewing — but **without merge authority**. It is the deliberate step between Contributor and Maintainer: trusted people help run the project before they hold the keys, and it is where a prospective maintainer is observed.

- **How one becomes an Associate** — **nominated by a maintainer** for a track record of good contributions, and approved once a **second maintainer agrees** (two maintainers suffice).
- **Inactivity:** an Associate is removed after **six months** of inactivity, and may later return as **emeritus** (below).

### Maintainer

Maintainers review and **merge** contributions, triage, set day-to-day direction, and **elect the architecture board** (below). **New maintainers are elected by the existing maintainers**, normally promoted from Associates.

- **How one becomes a maintainer** — elected by the existing maintainers, from candidates with a **proven track record as a Contributor or Associate spanning at least a year**; confirmation is by a **unanimous vote of all existing maintainers**.
- **Day-to-day decision rule** — a maintainer decides **individually** on routine pull requests; only changes that touch a **spec, proposal, or ADR** go to the architecture board.
- **Inactivity:** a Maintainer is removed after **one year** of inactivity, and may later return as **emeritus** (below).
- **Cadence:** the maintainers hold at least one meeting a year — the *"state of the solution"* — to review the project's direction.

### Founding maintainer

A rule that maintainers are elected by maintainers needs a seed. **Eduard Ralph is the founding (first) maintainer**, by virtue of starting the project; the maintainer group grows from there by election. The founding maintainer is a full Maintainer, and additionally:

- **chairs the architecture board** (below) — a **standing** role;
- **retains a veto over any change to this governance document** (see *Amending this document*) — **standing**; it does not expire with the bootstrap;
- holds **bootstrap authority** until the maintainer group and the board are established — sole maintainer, and sole elector and chair of the initial board — which is **transitional** and dissolves once the board is constituted.

The founding-maintainer role **voids after one year of inactivity** and is then **left empty** — not re-filled or inherited. Its standing powers (the board chair and the governance veto) lapse with it; the project then continues under the maintainers and the board.

### Emeritus

A former **Associate** or **Maintainer** — removed for inactivity, or having stepped down — holds **emeritus** standing. An emeritus may be **re-elected to their former level without re-proving the track record** it normally requires (the year as Contributor or Associate for a Maintainer; the contribution record for an Associate). The level's confirmation step still applies — a second maintainer's agreement for an Associate, a unanimous maintainer vote for a Maintainer.

### Architecture board

The body with authority to **accept** and **supersede** the project's binding decisions — **ADRs** (per [ADR-0001](../design/adr/0001-record-architecture-decisions.md)), **specifications**, and **enhancement proposals**. Its members are **elected by the maintainers** and **may be drawn from any participation level** — a board seat reflects judgement on the architecture, not rank on the ladder.

- **Size:** **at least three** members.
- **Chair:** the **founding maintainer**.
- **Decision rule** — the board accepts (and supersedes) ADRs through the ADR-0001 lifecycle, and ratifies specifications and enhancement proposals through their own folder processes; ratification is by **simple majority** of the members, recorded as agreement on the pull request.
- **Bootstrap — fewer than three members.** Until the board reaches three, the **chair (the founding maintainer) may make acceptance decisions**. To remain consistent with ADR-0001 — acceptance is never a single maintainer's act — these interim decisions are **provisional**: the board reviews and confirms them once it is constituted. *(If instead the chair's bootstrap acceptances are meant to be final, that is a true exception to ADR-0001 and must be enacted by a superseding ADR, not this document.)*
- **Term:** elected members serve **two years** and may stand for re-election (the founding maintainer chairs *ex officio*, independent of these terms).
- **Election procedure:** board members are elected by a **majority of the maintainers**.

## Decision-making

| Decision type | Process | Authority |
|---------------|---------|-----------|
| Architecture decision (ADR) | [ADR-0001](../design/adr/0001-record-architecture-decisions.md) lifecycle (Proposed → Accepted → Superseded) | Architecture board |
| Enhancement proposal | `../design/proposals/` (draft → accepted) | Architecture board |
| Specification version bump | `../design/specs/` (strict change process) | Architecture board |
| Routine code change | Pull-request review | Maintainers |
| Changes to *this* document | See below | Unanimous maintainers + founding-maintainer veto |

A **new feature** is never a routine change: it enters through an enhancement proposal (`../design/proposals/`) and is therefore the **architecture board's** decision, not an individual maintainer's merge.

## Amending this document

Changes are made by pull request and require the **approval of all maintainers** (unanimous). The **founding maintainer additionally holds a veto** — no amendment takes effect over the founding maintainer's objection, even with unanimous maintainer approval. During bootstrap, with a single maintainer, that approval and the veto are the same person's.
