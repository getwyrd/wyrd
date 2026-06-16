---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - process
  - meta
---
# 0001. Record architecture decisions

## Context

This is an open-source infrastructure project built initially by a small team and dependent on outside contributors. Architectural questions ("why not Ceph?", "why client-side erasure coding?", "why is metadata never erasure-coded?") will recur in every new issue thread and pull request unless the reasoning is captured once, durably, where contributors can find it.

We also use a deliberately trimmed arc42 for the living architecture overview, which has a gravitational pull toward documentation theater — sections filled in because the template has them. We want a record of *why the overview is trimmed the way it is*, and more generally of every decision that shapes the system.

## Decision

We record architecture decisions as ADRs: short, numbered, append-only Markdown files in `docs/design/adr/`, one decision per file, following the Nygard template (Context, Decision, Consequences), with `created` / `type` / `status` / `tags` carried in YAML frontmatter (the project's Zettelkasten-style metadata). When a design question is settled — in discussion, in review, anywhere — the output artifact is an ADR, not a wiki paragraph or a chat message.

This is the meta-decision, and the process below governs the **ADR class only**. The other authored classes document their own process in their own folders — enhancement proposals in `../proposals/` (a `draft/` → `accepted/` flow) and specifications in `../specs/` (normative and versioned) — while the living architecture overview in `../architecture/` is descriptive and not part of any acceptance flow.

### Numbering and location

ADRs live in `docs/design/adr/` and are named `NNNN-kebab-title.md`, where `NNNN` is the next free zero-padded number. Numbers are assigned monotonically and **never reused**, even after an ADR is superseded. Every ADR is listed, with its current status, in the index at `docs/design/adr/README.md`.

### Lifecycle

The `Status:` field in an ADR's header is the source of truth for its state. An ADR moves through:

- **Proposed** — written up but not yet ratified. A Proposed ADR is a working draft: it may be edited freely while discussion continues (normally in the pull request that introduces it).
- **Accepted** — ratified. An ADR is moved to Accepted by a deliberate edit to its `Status:` line once the decision is agreed in review. **This transition is the act of acceptance; it is intentional, not an automatic consequence of merging.** From this point the ADR is immutable (see below).
- **Superseded** — a later ADR has replaced the decision; recorded as `Superseded by ADR-NNNN` (see "Immutability").

During the initial design phase the whole foundational set is deliberately held at **Proposed** until it has been reviewed and ratified together — being too quick to mark decisions Accepted is itself a mistake this process guards against.

### Immutability, and how a decision changes

Once **Accepted**, an ADR is never rewritten — that is exactly what makes "see ADR-NNNN" a durable, trustworthy reference. When a decision changes we do **not** edit the old record. Instead we write a **new** ADR that explains what changed and names the one it replaces (`Supersedes ADR-NNNN`), and we mark the old entry `Superseded by ADR-NNNN` in the index. The superseded ADR's own file is left untouched, so the history reads in order.

This is enforced mechanically: the `adr-immutability` CI check (`.github/workflows/adr-immutability.yml`) fails any pull request that modifies, deletes, or renames an Accepted ADR. The check is status-aware — it reads each ADR's `status` from the pull request's base version, so a Proposed draft may still be edited freely and the Proposed → Accepted transition itself is allowed, with the record frozen only from the point it is Accepted.

### Scope of arc42

We deliberately trim arc42 to the ~10 sections that earn their maintenance cost rather than completing all 12, and we state that here so nobody later "helpfully" completes the template.

### Starting a new ADR

Copy `../templates/adr.md`, give it the next free number, fill in Context / Decision / Consequences, open it as **Proposed**, and add a row to the index (`docs/design/adr/README.md`).

## Consequences

- Newcomers can read the ADR set to understand the *why* of the system quickly.
- The same debates are not relitigated; objections are met with "see ADR-NNNN".
- There is a small ongoing discipline cost: decisions must be written up.
- The ADR set is append-only history, distinct from the living overview in `docs/design/architecture/`, which always describes the current system.
- The Proposed → Accepted → Superseded lifecycle and its CI enforcement are specified here, so the state of any decision — and whether it may still be edited — is unambiguous.

## Template for new ADRs

The same skeleton is kept as a copy-paste file at `../templates/adr.md`.

```
---
created: <DD.MM.YYYY HH:MM>
type: adr
status: Proposed   # Proposed | Accepted | Superseded by ADR-NNNN
tags:
  - adr
---
# NNNN. Title

## Context
What is the issue and the forces at play?

## Decision
What did we decide?

## Consequences
What becomes easier, harder, or constrained as a result? Include the
honestly-accepted costs, not only the benefits.
```
