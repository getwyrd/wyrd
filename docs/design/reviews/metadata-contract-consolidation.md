---
created: 12.07.2026
type: review
author: Eduard Ralph
tags:
  - review
  - metadata
  - conformance
  - contract
  - milestone-4
---
# Consolidation: the `MetadataStore` contract, as the FoundationDB port left it

> The closing note #437 asks for. #437 was rescoped away from a *pre*-implementation
> freeze of the contract (spec-first in disguise, against ADR-0002's
> implementation-first posture for component interfaces) to a **post-port
> consolidation**: build the FoundationDB backend against the trait as it stands
> (#438), then write the rules down from what actually got built. This note records
> what was consolidated, the divergences the port exposed, and the "take it from
> there" verdict — whether anything gets pinned harder than it now is.

## What was consolidated

The five contract points now live in trait-level doc comments on `MetadataStore`,
`CommitOutcome` and `WriteBatch` (`crates/traits/src/lib.rs`), stated
backend-neutrally — the three shipped backends reach them by three different
mechanisms (redb serializes write transactions; TiKV takes pessimistic locking
reads; FoundationDB uses an optimistic read-conflict set), and a fourth backend may
use a fourth, but it must land in the same place. The clauses of the shared
`wyrd-metadata-conformance` suite (`run_all`) remain the *executable* record; the
prose says what they mean.

The port's real teaching was **clause 3 of the commit partition**, which no
document and no shared clause had stated: `Conflict` is the answer to *"your
precondition lost"*, so a **blind** batch — one carrying no preconditions — can
never conflict. It asserted nothing about prior state and has nothing to lose; a
backend that cannot apply one owes the caller an `Err`. This is not stylistic.
Blind writers across the codebase (`core::repair::enqueue_repair`, the custodian's
desired-state writes) `?` the commit and *ignore* the returned `CommitOutcome`, so
a `Conflict` handed back to them reads as success while the write silently
vanished. FoundationDB is what surfaced it: an optimistic backend receives **one**
lost-race error code for both batch shapes (`1020 not_committed`) and must route it
by shape, so the obvious implementation is the wrong one.

Both the FDB and TiKV drivers had worked this out and written it in their own
module docs; the trait — the thing a *future* backend reads — said only "when a
precondition fails", which is compatible with the wrong reading. That is the gap
this consolidation closes.

Alongside it, four properties that were likewise driver-local prose and are now
trait-level: preconditions are evaluated against committed state **atomically with
the batch's writes** (so a read-then-commit is safe by in-commit re-check, never by
read freshness); reads observe the latest committed state and a `scan` is one
consistent cut across however many pages a backend internally reads (ADR-0015
clause 3, #261); a `scan` is **complete or fails loudly**, never silently truncated
(#262 — a short `inode:` listing would shrink GC's never-reclaim safety set); and a
`WriteBatch` is **not guaranteed idempotent**, which is precisely what forbids a
backend from silently retrying an unknown-result commit.

## What the executable record gained

One clause: `contract_blind_batch_is_never_conflict`. It has a sequential half
(blind overwrites and deletes of existing keys commit, including on a key a
conditional writer just lost on) and a **concurrent** half (two blind batches raced
on one key; neither may come back `Conflict` — each must be `Committed` or `Err`).
The concurrent half is the load-bearing one: a blind batch can only *lose a race*
concurrently, and no other clause in the suite drives two commits at once.

Demonstrated red per the #419 convention (`tests/demonstrated_red.rs`): a
`RaceConflatingStore` that maps a lost race to `Conflict` regardless of batch shape
— the exact mistake an optimistic backend is invited to make — fails the new clause
and **passes all seven others**, including `contract_read_after_commit`. Without
this clause a backend could swallow every raced blind write and stay green.

Which of the two racers wins, and whether the loser errors, is deliberately *not*
asserted: that is backend latitude (an optimistic backend retries both to
`Committed`; a pessimistic one may report the loser's lock loss as `Err`). The
clause forbids exactly one answer.

## Divergences the port exposed, left as-built

The docs state the contract; these three are places where the *implementations* do
not yet meet it uniformly. They are recorded here rather than papered over, and
none is fixed by this change — each is backend work, not doc consolidation.

1. **TiKV has no typed unknown-result error.** FDB raises `CommitUnknownResult`
   (carrying the code, and `may_still_commit()` to separate "a re-read settles it"
   from "a re-read proves nothing"); TiKV's undetermined-commit case exists only as
   prose in the driver, so a caller cannot distinguish it from a plain fault
   without inspecting a raw `tikv_client::Error`. The contract says an
   unknown-result commit must be distinguishable; TiKV does not yet make it so.
2. **redb's `scan` has no cap.** The two distributed backends share an identical
   `SCAN_CAP` and `Err` above it (completeness-or-fail-loud); redb iterates the
   whole table with a `starts_with` filter and can materialize an unbounded `Vec`.
   It cannot *silently truncate*, so it does not violate clause 5 — but it does not
   enforce it either.
3. **Only FDB bounds operation time.** FDB's C client retries an unreachable
   cluster forever, so the driver sets an explicit per-transaction deadline; TiKV
   inherits tikv-client's defaults and redb is local. "Every operation terminates"
   is stated in the contract and enforced in one backend.

## The verdict: nothing gets pinned harder

**No formal pin, no specs document, no new CI gate.** #437 explicitly allows "no
further ceremony needed" as an answer, and that is the answer, for three reasons.

The trait docs plus the shared suite already *are* the pin, and the suite is the
half with teeth: it is executable, every backend runs the identical `run_all`, and
adding a property there promotes a requirement every present and future backend
must satisfy. A prose spec would be a second, weaker copy of a contract that already
has an executable one — and ADR-0002 keeps component interfaces implementation-first
precisely so this does not happen (the chunk format remains the only spec-first
surface; a `docs/design/specs/` document was a stated non-goal of #437).

Governance of *early* pins is already answered elsewhere: ADR-0044's
provisional-marker rule ("mark, don't gate") covers a property that lands before the
decision it encodes is ratified. Nothing in this consolidation is provisional —
`contract_blind_batch_is_never_conflict` encodes a decision the FDB port already
made and both drivers already implement — so no marker is needed, and no new
governance machinery is either.

A CI gate would have nothing to add. The FDB and TiKV conformance runs are already
jobs (`cargo xtask fdb-conformance`, `cargo xtask tikv-conformance`); what they lack
is not a gate but a *cluster* in CI, which is a fixture question (ADR-0043), not a
contract one.

**What is deferred, and to what.** The three divergences above want issues against
their backends, not a contract change — the contract is right and the backends are
behind it. And #442's fault + contention battery is still outstanding: this
consolidation used the port's evidence, not the battery's. If the battery surfaces
a contract point these docs get wrong — most plausibly around the unknown-result
class under real partitions, which is the one clause whose consequences are hardest
to see without faults — the right response is to amend these docs and the suite,
not to add ceremony around them. That is the standing invitation this note leaves
open.

## Evidence

All shipped backends pass the shared `run_all`, including the new clause, at
consolidation time — the acceptance criterion, and the standing guard #437 sets on
any suite change (a suite change must keep the existing backends green in the same
change, which is what stops the tests being reshaped around FDB's quirks):

| Backend | How it was run | Result |
| --- | --- | --- |
| redb | `cargo test -p wyrd-metadata-redb` | pass |
| TiKV | `cargo xtask tikv-conformance` (brings up `deploy/tikv-single-node`) | pass |
| FoundationDB | `cargo xtask fdb-conformance` (brings up `deploy/fdb-single-node`) | pass |

Plus `cargo test -p wyrd-metadata-conformance`: the new clause goes red against a
deliberately-violating store and green against every correct one.
