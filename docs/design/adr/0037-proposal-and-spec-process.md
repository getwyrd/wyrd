---
created: 27.06.2026 15:24
type: adr
status: Accepted
tags:
  - adr
  - process
  - proposals
  - specs
  - documentation
---
# 0037. Proposal and specification process, lifecycle, and immutability

## Context

ADR-0001 deliberately defines the process for ADRs only. It says that other
authored classes have their own process: proposals move through `draft/` and
`accepted/`, specifications are normative and versioned, and the architecture
overview remains living documentation.

That split is correct, but the proposal and specification processes have so far
been lighter than the ADR process. The result is an uneven historical record:
Accepted ADRs are append-only and protected by CI, while Accepted proposals and
stable specifications can still be edited directly if the ordinary docs checks
pass. That is too weak for documents that carry review history, implementation
scope, compatibility contracts, or conformance requirements.

The distinction between document classes still matters:

- The architecture overview is descriptive and living. It should change whenever
  the system changes.
- ADRs are historical decisions. Once Accepted, they are immutable and change by
  supersession.
- Proposals are accepted implementation plans and change records. Once Accepted,
  they should be a stable record of what was approved.
- Specifications are normative compatibility contracts. Once Stable, a given
  version must not drift under readers, writers, tests, or operators.

We need one explicit lifecycle and immutability rule for proposals and specs,
parallel to ADR-0001, while preserving their different roles.

## Decision

Proposal and specification documents use YAML frontmatter as their lifecycle
source of truth, just as ADRs do. Their accepted/stable states are immutable by
default and change by supersession, replacement, or a new version rather than by
rewriting the historical file.

### Proposals

Proposals live under `docs/design/proposals/` and are named
`NNNN-kebab-title.md`, where `NNNN` is a monotonically assigned number that is
never reused. Draft proposals live in `draft/`; accepted, implemented, or
superseded proposals live in `accepted/`.

The `status:` field in proposal frontmatter is authoritative. Valid statuses are:

- **draft** — a working proposal. It may be edited freely while discussion
  continues.
- **accepted** — the proposal has been ratified as the plan of record. The
  Draft -> Accepted transition is intentional: the file moves from `draft/` to
  `accepted/`, and `status:` changes to `accepted`.
- **implemented** — the accepted proposal's scoped work has landed. This is a
  terminal historical state; the file remains immutable.
- **superseded** — a later proposal replaces or materially revises this proposal.
  The replacement is named with `superseded-by:` on the old proposal and
  `supersedes:` on the new proposal.
- **withdrawn** — the proposal was explicitly abandoned. Once withdrawn, it is
  historical and immutable.

After a proposal leaves `draft`, its file is not rewritten. If the plan changes
materially after acceptance, write a new proposal that supersedes or rescopes the
old one. Small implementation facts that belong in the living architecture or
code comments should be recorded there, not back-patched into the accepted
proposal.

### Specifications

Specifications live under `docs/design/specs/` and are named by domain and
version, for example `chunk-format/v1.md`. They use `../templates/spec.md`.

The `status:` field in spec frontmatter is authoritative. Valid statuses are:

- **draft** — a normative contract under development. It may be edited freely,
  and it must carry an explicit instability marker.
- **stable** — a ratified compatibility contract for its `version:`. The
  Draft -> Stable transition is intentional: `status:` changes to `stable`, and
  `stability:` changes to `stable`.
- **superseded** — a later spec version or replacement spec supersedes this one.
- **withdrawn** — the draft spec was abandoned before stabilization.

After a spec becomes `stable`, its file is not rewritten. Any normative change
to fields, wire behavior, on-disk bytes, compatibility guarantees, required
errors, conformance vectors, or reader/writer behavior requires a new spec
version or a superseding spec. Editorial improvements to stable specs are
recorded in the superseding document or in clearly separate errata, not by
rewriting the stable version in place.

Conformance vectors are part of the specification when they define required
reader or writer behavior. For a stable spec version, changing or deleting an
existing vector is a spec change and follows the same supersession/versioning
rule. Adding a vector to clarify an already-required behavior may be allowed only
when the governing spec explicitly permits compatible vector additions for that
version; otherwise it requires a new version.

### CI enforcement

The docs immutability check covers ratified design documents: ADRs, proposals,
and specs. It uses the same base-version rule as ADR-0001:

- The check reads `status:` from the pull request's base version.
- Draft/Proposed documents in the base may be edited, moved to their accepted or
  stable location, or ratified in the pull request.
- Documents that are already accepted, implemented, stable, superseded, or
  withdrawn in the base may not be modified, deleted, or renamed.
- New documents may be added.
- Index, README, and generated-list changes remain allowed.

This keeps acceptance and stabilization possible while freezing the record from
the point of ratification onward.

## Consequences

- Proposals and specs become trustworthy historical artifacts, not mutable notes.
- A merged Accepted proposal records what was approved at that time; later scope
  changes require an explicit superseding proposal.
- A Stable spec version records the exact compatibility contract for that
  version; later compatibility changes require a new version or superseding spec.
- CI enforcement can be generalized as `.github/workflows/docs-immutability.yml`
  rather than inventing a separate mechanism for each document class.
- There is a small documentation cost: follow-up changes need a new artifact
  instead of direct edits. That cost is intentional; it is the price of a durable
  design history and stable compatibility contracts.
