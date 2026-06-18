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

- **How one becomes an Associate** — nominated by a maintainer for a track record of good contributions; confirmation rule **[OPEN]**.
- **Inactivity / removal:** **[OPEN]**.

### Maintainer

Maintainers review and **merge** contributions, triage, set day-to-day direction, and **elect the architecture board** (below). **New maintainers are elected by the existing maintainers**, normally promoted from Associates.

- **How one becomes a maintainer** — elected by the existing maintainers; nomination criteria and confirmation threshold **[OPEN]**.
- **Day-to-day decision rule** for routine changes (e.g. lazy consensus on pull requests): **[OPEN]**.
- **Inactivity / removal policy:** **[OPEN]**.

### Founding maintainer

A rule that maintainers are elected by maintainers needs a seed. **Eduard Ralph is the founding (first) maintainer**, by virtue of starting the project; the maintainer group grows from there by election. The founding maintainer is a full Maintainer plus the **bootstrap authority** the genesis requires: until a second maintainer is elected, they hold the maintainer role solely and are the sole elector of the initial architecture board (whose size and election rule remain **[OPEN]** below).

- The bootstrap authority is **transitional** — it exists only until the maintainer group and the board are established. Whether the founding maintainer keeps any standing afterward (e.g. tie-break, emeritus) is **[OPEN]**.

### Architecture board

The body with authority to **accept** and **supersede** ADRs, per [ADR-0001](../design/adr/0001-record-architecture-decisions.md). Its members are **elected by the maintainers** and **may be drawn from any participation level** — a board seat reflects judgement on the architecture, not rank on the ladder.

- **Size:** **at least three** members.
- **Chair:** the **founding maintainer**.
- **Decision rule** — the board accepts (and supersedes) ADRs through the ADR-0001 lifecycle: its agreement on an ADR's pull request *is* the ratification. The numeric threshold among the members (simple majority vs. full consensus) is **[OPEN]**.
- **Bootstrap — fewer than three members.** Until the board reaches three, the **chair (the founding maintainer) may make acceptance decisions**. To remain consistent with ADR-0001 — acceptance is never a single maintainer's act — these interim decisions are **provisional**: the board reviews and confirms them once it is constituted. *(If instead the chair's bootstrap acceptances are meant to be final, that is a true exception to ADR-0001 and must be enacted by a superseding ADR, not this document.)*
- **Term and re-election cadence:** **[OPEN]**.
- **Election procedure** — how maintainers nominate and vote: **[OPEN]**.

## Decision-making

| Decision type | Process | Authority |
|---------------|---------|-----------|
| Architecture decision (ADR) | [ADR-0001](../design/adr/0001-record-architecture-decisions.md) lifecycle (Proposed → Accepted → Superseded) | Architecture board |
| Enhancement proposal | `../design/proposals/` (draft → accepted) | **[OPEN]** — board, maintainers, or both |
| Specification version bump | `../design/specs/` (strict change process) | **[OPEN]** |
| Routine code change | Pull-request review | Maintainers |
| Changes to *this* document | See below | **[OPEN]** |

## Amending this document

**[OPEN]** — the process for changing governance itself (who approves, and by what majority) is not yet decided. Until it is, this document remains a DRAFT and is authoritative only for the parts that restate an accepted ADR.
