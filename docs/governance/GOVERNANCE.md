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

> **Status: DRAFT — skeleton, not yet ratified.** This records the project's decision-making structure. Only the parts already anchored in an accepted ADR are firm; everything marked **[OPEN]** is a placeholder for a decision the maintainers have not yet made, and is not binding.

## Purpose

Wyrd is an open-source project built initially by a small team and dependent on outside contributors (see [ADR-0001](../design/adr/0001-record-architecture-decisions.md)). This document makes explicit *who holds which responsibilities* and *how decisions are made*, so that authority is stated rather than assumed.

It complements, and does not override, the per-class processes documented elsewhere: architecture decisions follow [ADR-0001](../design/adr/0001-record-architecture-decisions.md), enhancement proposals follow `../design/proposals/`, and specifications follow `../design/specs/`.

## Roles

### Contributors

Anyone who opens an issue or a pull request. Contributions are accepted under the project's license and DCO sign-off (ADR-0003). No special status is required to contribute.

### Maintainers

Maintainers review and merge contributions, triage issues, and elect the architecture board (below).

- **How one becomes a maintainer** — criteria and the nomination / confirmation procedure: **[OPEN]**.
- **Day-to-day decision rule** for routine changes (e.g. lazy consensus on pull requests): **[OPEN]**.
- **Inactivity / removal policy:** **[OPEN]**.

### Architecture board

The body with authority to **accept** and **supersede** ADRs, per [ADR-0001](../design/adr/0001-record-architecture-decisions.md). Its members are **elected by the maintainers**.

- **Size:** **[OPEN]**.
- **Term and re-election cadence:** **[OPEN]**.
- **Election procedure** — how maintainers nominate and vote: **[OPEN]**.
- **Decision rule for accepting an ADR** — quorum, majority, or full consensus of the board: **[OPEN]**.
- **Chair / tie-breaking:** **[OPEN]**.

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
