---
created: 29.06.2026 14:00
type: adr
status: Accepted
tags:
  - adr
  - custodian
  - durability
  - metadata
---
# 0040. Mixed-era placement expansion: one identity-fallback rule, liberal read / strict maintenance

## Context

Proposal 0005 (M3 — Custodians) settles a question M2 left open: a chunk's
**placement is recorded at commit** (`0005:178-207`). Each `ChunkRef` carries a
`placement` vector — the stable D-server id holding each fragment — replacing
M2's stateless `index % n` fanout. The vector's length is the chunk's fragment
count: `EcScheme::None` → 1, `ReedSolomon { k, m }` → `k + m`.

The field was added additively to a schema that had not shipped a `placement`,
so it is `#[serde(default)]` (`crates/core/src/metadata.rs`): a record written
before the field — a **pre-M3 / mixed-era** record — decodes with an *empty*
vector. The read path intentionally treats an absent entry as **identity
placement**: fragment index `i` resolves to D-server `i`. This is encoded once,
in `ChunkRef::placed_dserver(i)` (`placement.get(i).unwrap_or(i)`), with
`ChunkRef::fragment_count()` defining the index space `0..n`. The compatibility
rule is load-bearing precisely at migration time, when committed chunks have not
yet been rewritten with an explicit placement and operators are most likely to
drain or decommission D-servers.

That rule must hold anywhere a committed chunk map is interpreted: the read path,
and the four custodian loops — GC, scrub, reconstruction, and rebalance. The
#287 fix centralized the fallback and wired read, GC, scrub, and reconstruction
through `placed_dserver`/`fragment_count`. The #292 audit then found that
**rebalance is the lone consumer still iterating the raw `placement` vector**
(`plan_evacuations`): for a pre-M3 chunk it sees an empty vector, evacuates
nothing, and silently leaves a live fragment on a draining server — total object
loss for `EcScheme::None`, redundancy erosion for Reed-Solomon (#346). There is
also no single "walk every fragment to its holding D-server" helper; consumers
open-code the `0..fragment_count()` walk, which is how the rebalance divergence
went unnoticed.

The audit raised five questions this record answers: the normative expansion
rule per scheme; whether short (non-empty, wrong-length) vectors are supported;
whether the fallback should be centralized; whether maintenance loops should
reject malformed lengths; and which compatibility paths are migration-only and
when they can be removed.

## Decision

We will adopt a single normative placement-expansion rule, expressed through one
helper, with a deliberately asymmetric strictness posture.

1. **Normative expansion rule.** A chunk's fragment index space is
   `0..fragment_count()` (`None` → 1; `ReedSolomon { k, m }` → `k + m`). Fragment
   index `i` resolves to D-server `placement[i]` when present, else `i` (identity).
   Every interpretation of a committed chunk map MUST use this rule.

2. **One expansion helper.** A single helper —
   `ChunkRef::fragments() -> impl Iterator<Item = (u16, DServerId)>` over the full
   index space — is *the* "walk every fragment to its holding D-server" call;
   `fragment_count()` and `placed_dserver()` remain its primitives. No consumer may
   iterate the raw `placement` vector — raw iteration is the defect class this record
   exists to foreclose. This bare helper is **liberal**: it applies the identity
   fallback unconditionally and does *not* validate length, so it is the read path's
   resolution. Maintenance loops resolve through the same expansion but only behind
   the validity gate of decision 4 — a malformed vector is rejected *before* it is
   expanded, never silently identity-filled. The gate is therefore a separate,
   fallible step, not a property of `fragments()` itself: expose it as a companion
   (e.g. `checked_fragments() -> Result<…, MalformedPlacement>`, or a
   `placement_is_valid()` predicate the loop checks first — #347/#348's call), while
   `fragments()` stays infallible for read.

3. **Validity of a placement vector.** A committed `placement` vector is valid
   **iff** it is empty (pre-M3 → identity) **or** `len == fragment_count()`
   (explicit). A non-empty vector of any other length is **malformed**: no writer
   produces one (the write path always emits a full-length vector; only
   `#[serde(default)]` yields empty), so in practice it can only mean truncation or
   corruption. Short non-empty vectors are NOT a supported steady state.

4. **Liberal read, strict maintenance.** The read path stays **liberal** — it keeps
   the per-index identity fallback so a still-readable record is never made
   unreadable over a length quirk (availability first). The maintenance loops are
   **strict**: they MUST classify the committed placement *before* expanding — empty
   or `len == fragment_count()` is valid and is walked via `fragments()`; a non-empty
   wrong-length vector is **malformed** and takes the path below WITHOUT expansion, so
   no identity entry is ever fabricated for it. GC and scrub fail safe — treat the
   chunk as fully
   referenced, never reclaim, and emit an audit event (ADR-0011); reconstruction
   and rebalance skip the chunk and flag it NEEDS-HUMAN. This costs nothing on real
   data (only empty or full vectors occur) and turns a corrupt vector into a visible
   signal instead of a silently fabricated placement.

5. **Repoints write a full-length placement.** Any loop that rewrites placement
   (rebalance evacuation, reconstruction re-placement) MUST materialize a
   **full-length, identity-resolved** vector (length `fragment_count()`) into the
   committed record — never a short or raw one. Expanding only an evacuation/repair
   *filter* while leaving the carried placement vector raw is incorrect: the
   downstream version-conditional commit indexes and writes that vector, so a raw
   empty/short vector panics or persists a malformed record (#346).

6. **The fallback is migration-only, behind a removal gate.** The empty-vector
   branch exists solely for pre-M3 / mixed-era records and MUST NOT remain
   load-bearing indefinitely. It becomes removable once (a) a backfill migration has
   rewritten every pre-M3 committed chunk map with an explicit identity placement,
   using the same version-conditional commit the custodians use, and (b) a scan
   confirms zero empty-placement committed records remain. At that point the empty
   branch SHOULD become a defensive error gated behind a metadata format-version
   bump. Until both hold, the fallback stays load-bearing (#350).

We reject: supporting short non-empty vectors as a valid steady state; silently
fabricating identity placement for malformed vectors inside maintenance loops; and
leaving the compatibility fallback in place with no defined removal path.

## Consequences

This record changes no code: the rebalance data-loss path (#346) remains **live on
`main`** — `plan_evacuations` still iterates `chunk.placement` raw and the
`fragments()` helper does not yet exist. Once #347 lands the helper and #346 routes
rebalance through it, there is a single definition of placement expansion and the
data-loss class is closed — and, because the helper always walks the full index
space, it then structurally cannot recur (a reviewer can grep for
`.placement.iter()` to catch any future regression). Malformed placement vectors
become an observable signal (audit event / NEEDS-HUMAN) once the strict-maintenance
stance is implemented (#348), rather than a silent identity resolution that masks
corruption.

The cost is real follow-on work, tracked as the #292 follow-ups, all on M3 —
Custodians: add the `fragments()` helper and migrate every consumer onto it
(#347); fix rebalance to expand *and* materialize a full-length placement (#346,
the priority); add the strict malformed-length handling with audit / NEEDS-HUMAN
(#348); implement the mixed-era test matrix across read / scrub / reconstruction /
rebalance, larger RS{6,3}, and a DST scenario seeded with an empty-placement chunk
(#349); and the backfill migration plus the format-version-gated removal of the
fallback (#350). The strict-maintenance stance adds fail-safe and NEEDS-HUMAN
paths that did not exist before.

This record refines proposal 0005's placement-record section without editing the
frozen proposal (per ADR-0037 / ADR-0001 / ADR-0038: the relationship is recorded
here and in the ADR index, never by back-patching 0005). It is load-bearing for
ADR-0033's durability argument — the distinct-failure-domain spread it requires is
only verifiable if placement resolves correctly for mixed-era chunks — and keys on
ADR-0034's first-class failure-domain identity. The atomic, version-conditional
placement repoint preserves ADR-0015's consistency contract over the repair path,
and the audit-event obligation follows ADR-0011's durability-telemetry stance.
Reversal is cheap while this is Proposed; once the backfill runs and the fallback
is removed behind a format-version bump (#350), reintroducing pre-M3 compatibility
would require restoring the compat branch and accepting empty-placement records
again — so the removal step is the point of no easy return, which is why it is
gated on an explicit scan rather than a time-based assumption.
