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

> **Status: DRAFT — skeleton, not yet ratified.** This records the project's decision-making structure. Firm so far: the parts anchored in an accepted ADR, the four-level participation ladder, the founding-maintainer bootstrap, and the architecture-board composition (below); the criteria and thresholds within each level remain **[OPEN]**. Everything marked **[OPEN]** is a placeholder for a decision the maintainers have not yet made, and is not binding.

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
- **Day-to-day decision rule** for routine changes (e.g. lazy consensus on pull requests): **[OPEN]**.
- **Inactivity:** a Maintainer is removed after **one year** of inactivity, and may later return as **emeritus** (below).
- **Cadence:** the maintainers hold at least one meeting a year — the *"state of the solution"* — to review the project's direction.

### Founding maintainer

A rule that maintainers are elected by maintainers needs a seed. **Eduard Ralph is the founding (first) maintainer**, by virtue of starting the project; the maintainer group grows from there by election. The founding maintainer is a full Maintainer, and additionally:

- **chairs the architecture board** (below) — a **standing** role;
- **retains a veto over any change to this governance document** (see *Amending this document*) — **standing**; it does not expire with the bootstrap;
- holds **bootstrap authority** until the maintainer group and the board are established — sole maintainer, and sole elector and chair of the initial board — which is **transitional** and dissolves once the board is constituted.

Any further standing for the founding maintainer beyond the bootstrap (e.g. a permanent role) is **[OPEN]**.

### Emeritus

A former **Associate** or **Maintainer** — removed for inactivity, or having stepped down — holds **emeritus** standing. An emeritus may be **re-elected to their former level without re-proving the track record** it normally requires (the year as Contributor or Associate for a Maintainer; the contribution record for an Associate). The level's confirmation step still applies — a second maintainer's agreement for an Associate, a unanimous maintainer vote for a Maintainer.

### Architecture board

The body with authority to **accept** and **supersede** ADRs, per [ADR-0001](../design/adr/0001-record-architecture-decisions.md). Its members are **elected by the maintainers** and **may be drawn from any participation level** — a board seat reflects judgement on the architecture, not rank on the ladder.

- **Size:** **at least three** members.
- **Chair:** the **founding maintainer**.
- **Decision rule** — the board accepts (and supersedes) ADRs through the ADR-0001 lifecycle; ratification is by **simple majority** of the members, recorded as agreement on the ADR's pull request.
- **Bootstrap — fewer than three members.** Until the board reaches three, the **chair (the founding maintainer) may make acceptance decisions**. To remain consistent with ADR-0001 — acceptance is never a single maintainer's act — these interim decisions are **provisional**: the board reviews and confirms them once it is constituted. *(If instead the chair's bootstrap acceptances are meant to be final, that is a true exception to ADR-0001 and must be enacted by a superseding ADR, not this document.)*
- **Term:** elected members serve **two years** and may stand for re-election (the founding maintainer chairs *ex officio*, independent of these terms).
- **Election procedure:** board members are elected by a **majority of the maintainers**.

## Decision-making

| Decision type | Process | Authority |
|---------------|---------|-----------|
| Architecture decision (ADR) | [ADR-0001](../design/adr/0001-record-architecture-decisions.md) lifecycle (Proposed → Accepted → Superseded) | Architecture board |
| Enhancement proposal | `../design/proposals/` (draft → accepted) | **[OPEN]** — board, maintainers, or both |
| Specification version bump | `../design/specs/` (strict change process) | **[OPEN]** |
| Routine code change | Pull-request review | Maintainers |
| Changes to *this* document | See below | Founding-maintainer veto; wider rule **[OPEN]** |

## Amending this document

Changes are made by pull request. The **founding maintainer retains a veto** over any governance change — no amendment takes effect over the founding maintainer's objection. The wider approval rule (which body proposes and ratifies an amendment, and by what majority, alongside that veto) remains **[OPEN]**. Until it is settled, this document remains a DRAFT, authoritative only for the parts that restate an accepted ADR or record a decision the founding maintainer has made.
