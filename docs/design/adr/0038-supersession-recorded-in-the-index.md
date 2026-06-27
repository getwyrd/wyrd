---
created: 27.06.2026 17:30
type: adr
status: Proposed
tags:
  - adr
  - process
  - proposals
  - specs
  - documentation
---
# 0038. Supersession is recorded in the index, not on the frozen file

## Context

ADR-0037 gave proposals and specifications an explicit lifecycle and an
immutability rule parallel to ADR-0001, and made supersession the way a ratified
document changes. But its §Proposals text describes the supersession *marker*
inconsistently with the rest of the regime: it says the replacement "is named
with `superseded-by:` on the old proposal and `supersedes:` on the new proposal" —
i.e. it instructs an edit to the **old, frozen** file.

That instruction conflicts with three things the same regime already commits to:

- **ADR-0037's own rule** that, after a proposal leaves `draft`, "its file is not
  rewritten."
- **ADR-0001's** long-standing mechanism: when a decision changes "we do **not**
  edit the old record … we mark the old entry `Superseded by ADR-NNNN` in the
  index. The superseded ADR's own file is left untouched."
- **The `docs-immutability` CI** that ADR-0037 introduced. Its editability gate
  permits modifying a proposal or ADR only while its base status is `draft` /
  `Proposed`; an `accepted`/`Accepted` (or `stable`) file may not be modified at
  all. So a `superseded-by:` edit to the old file is not just discouraged — it is
  **rejected by the required check**, and can never legally be applied.

This was surfaced concretely by the implementation-arc rescope: proposal 0002 was
superseded by proposal 0013, and a `superseded-by:` banner added to the accepted
0002 would have failed `docs-immutability`. The marker has to live somewhere the
frozen file is not.

ADR-0001 already answered this for ADRs — the index carries the status — but
proposals and specs were given no equivalent status index, so there was nowhere
ADR-0001's mechanism could be applied for them. The gap and the contradictory
instruction are the same problem from two sides.

## Decision

We will record supersession **in the per-class index, never on the frozen file**,
uniformly across ADRs, proposals, and specs. Concretely:

- The **new** document carries `supersedes: <old>` in its frontmatter. It is a
  newly added file, which the immutability check always permits.
- The **old** document is left **byte-for-byte untouched** — no `superseded-by:`
  field, no banner, no status flip. It stays exactly as it was ratified.
- The **index/README for the class** is the authoritative record of current
  status and supersession. The old entry is marked *superseded by NNNN* there.
  Index and README edits are explicitly allowed by `docs-immutability`.

This **refines ADR-0037**: its §Proposals "`superseded-by:` on the old proposal"
clause is corrected to "recorded in the proposals index," and ADR-0037's own
"its file is not rewritten" rule and ADR-0001's index mechanism now read the same
way for every authored class. ADR-0037 otherwise stands.

Because ADR-0001's index mechanism requires an index to exist, **each authored
document class MUST have an index/README that carries each document's current
status and any supersession link.** `docs/design/adr/README.md` already does this;
this decision adds the equivalent **proposals index** to
`docs/design/proposals/README.md` (and records proposal 0002 as superseded by
0013 there). Specifications gain the same treatment if and when a stable spec is
ever superseded.

This ADR follows its own rule: it does not edit ADR-0037's frozen file. ADR-0037
keeps its original text; the relationship is recorded in the ADR index
(`adr/README.md`), and the corrected mechanic is the one stated here and in
`AGENTS.md`.

## Consequences

- Supersession is uniform and consistent with the required `docs-immutability`
  check: the new file declares `supersedes:`, the old file is never touched, and
  the index carries the "superseded by" status. There is no longer an instruction
  in the regime that the CI forbids.
- The per-class index/README becomes the **single source of truth for current
  status**. A reader who opens a frozen document and wants to know whether it is
  still current consults the index, not the file — exactly as for ADRs today.
- A small standing obligation: the index must be kept current when a document is
  ratified or superseded. This is a README edit, always permitted, and is the
  price of keeping the frozen records frozen.
- ADR-0037's superseded `superseded-by:`-on-the-old-file wording remains in its
  immutable text; this ADR and the indexes are the governing correction. A future
  full restatement could supersede ADR-0037 outright, but that is not warranted
  for a single corrected clause.
- Refines ADR-0037; aligns with ADR-0001; applies the `docs-immutability` guard
  (ADR-0037) as the enforcement reference.
