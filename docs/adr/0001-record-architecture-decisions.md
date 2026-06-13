# 0001. Record architecture decisions

Date: design phase
Status: Accepted

## Context

This is an open-source infrastructure project built initially by a small team
and dependent on outside contributors. Architectural questions ("why not Ceph?",
"why client-side erasure coding?", "why is metadata never erasure-coded?") will
recur in every new issue thread and pull request unless the reasoning is captured
once, durably, where contributors can find it.

We also use a deliberately trimmed arc42 for the living architecture overview,
which has a gravitational pull toward documentation theater — sections filled in
because the template has them. We want a record of *why the overview is trimmed
the way it is*, and more generally of every decision that shapes the system.

## Decision

We record architecture decisions as ADRs: short, numbered, immutable Markdown
files in `docs/adr/`, following the Nygard template (Context, Decision,
Consequences, Status). An ADR is never edited after acceptance; when a decision
changes, a new ADR supersedes the old one and references it.

When a design question is settled — in discussion, in review, anywhere — the
output artifact is an ADR, not a wiki paragraph or a chat message.

We deliberately trim arc42 to the ~10 sections that earn their maintenance cost
rather than completing all 12, and we state that here so nobody later
"helpfully" completes the template.

## Consequences

- Newcomers can read the ADR set to understand the *why* of the system quickly.
- The same debates are not relitigated; objections are met with "see ADR-NNNN".
- There is a small ongoing discipline cost: decisions must be written up.
- The ADR set is append-only history, distinct from the living overview in
  `docs/architecture/`, which always describes the current system.

## Template for new ADRs

```
# NNNN. Title

Date:
Status: Proposed | Accepted | Superseded by ADR-XXXX

## Context
What is the issue and the forces at play?

## Decision
What did we decide?

## Consequences
What becomes easier, harder, or constrained as a result? Include the
honestly-accepted costs, not only the benefits.
```
