---
created: 12.07.2026
type: review
author: Eduard Ralph
tags:
  - review
  - verdict
  - metadata
  - foundationdb
  - milestone-4
  - release-gating
---
# FDB go/no-go: the M4 metadata fault + contention battery (#442)

> The verdict artifact #442 asks for. It records what was run, what was found, and what it
> licenses. **Verdict: GO on the FoundationDB driver's correctness under fault and
> contention** — with two things it does *not* license, named explicitly below, because a
> gate that quietly overstates itself is worse than no gate.

## Why this battery exists

FoundationDB was chosen (ADR-0042) partly *for* its simulation pedigree. But that pedigree
validates **their** code. It says nothing about **our mapping layer** — precondition →
read-conflict set, `1020 not_committed` → `Conflict`, the unknown-result rules, the retry
policy — which is entirely ours and entirely new. This battery is a black-box check of that
mapping against a real cluster being genuinely abused.

## What was built, and the one decision that shaped it

#442 says to reuse #257's harness. **It could not be reused**: #257's Tier-1 scenario was
written against the concrete `TikvMetadataStore`, and its fault-effect oracle asks **PD** for
a store heartbeat — a concept FoundationDB does not have. Pointing it at FDB was new code, not
a flag.

So the scenario was **lifted into a shared crate** (`wyrd-metadata-fault-conformance`), generic
over `MetadataStore`, with the two backend-shaped parts behind one seam (`ClusterFault`): *how
you cut a node*, and *how you ask its peers whether the cut bit*. Everything that decides PASS
or FAIL — the workload, the invariants, the signal arithmetic — is now **the same code for both
backends**. That is the only basis on which their verdicts can be compared; a private FDB notion
of "pass" would decide nothing. It is the discipline of the shared `metadata-conformance` suite
(ADR-0016), applied to the fault battery.

Runner: `WYRD_TIER1=1 cargo xtask fdb-metadata-tier1` — repeatable, self-contained (brings up
`deploy/fdb-multi-replica`, configures `double ssd`, writes the cluster file, tears down
unconditionally), and **opt-in but never silently skipped**: opted-in-with-Docker-missing is a
hard error, because a battery that quietly did not run would be recorded as a "go" nobody
earned.

## The three legs, and what each found

### 1. Contention — PASS

Three workloads, N=4 clients each, against the live 3-process cluster.

| Workload | The invariant | Result |
| --- | --- | --- |
| Rename races | Exactly one of N concurrent renames of the same binding may win; losers are `Conflict`; the binding ends up in exactly one place — never duplicated, never lost | pass |
| Inode-allocator hot path | N clients × 8 allocations through a CAS loop: **every id unique**, and the cell advances by exactly the number handed out | pass |
| Blind-batch storm | A precondition-free batch is **never** `Conflict` (it has nothing to lose); no write vanishes while its caller was told it committed | pass |

The allocator leg is the one with the sharpest teeth: a misclassified `Conflict` there — a lost
race reported as success — hands **two files the same inode**, the worst outcome the metadata
layer can produce. It did not happen.

### 2. Consistency under a symmetric cluster fault — PASS

The shared Tier-1 scenario, with the process holding FDB's **`master` role** symmetrically
isolated (bidirectional `iptables` DROP inside its own network namespace) while ≥2 contenders
race the same compare-and-swap.

Cutting an arbitrary node would have been **outcome-neutral** — FDB keeps quorum on the majority
side, so a bystander cut proves nothing and every assertion goes green for free. That is the
hollow flip #257's review caught in the TiKV leg. The target is therefore resolved at runtime
from `status json` (the `master`), and the fault is confirmed **from the peers' side** (a
*survivor's* `coordinators[].reachable` view of the cut node) — never by probing the dropped port
ourselves, which would only prove our own packets are dropped.

All four independent signals held: `read_after_commit`, `converged_once` (the contended CAS round
advanced the version by exactly one), `fault_materialized` (the cluster provably lost the node),
`no_lost_update` (exactly one contended winner; the stale-CAS probe rejected).

### 3. Mid-commit kill — PASS, and it induced real 1021s

`SIGKILL` on the process holding the **`commit_proxy`** role — the process a commit RPC is
actually sent to — fired from a background thread **while commits were in flight**.

**The first draft of this leg was a hollow green, and its own honesty check said so**: it killed
the *master* at a round boundary and reported *"the kill perturbed no commit — 40 committed, 0
unknown results"*. Killing between commits interrupts nothing, and the master is not the process
holding the RPC. Both corrections were load-bearing.

The corrected leg, over 400 rounds of a single-writer CAS chain:

```
398 committed, 1 settled by re-read, 2 unknown-result commit(s), 0 plain fault(s),
0 phantom conflict(s); final version 399
```

Both unknown results were **real `1021 commit_unknown_result`** from the live cluster, surfaced
as the seam's `CommitUnknownResult` — never as `Conflict`, never silently retried. And **every
1021 was accounted for** (#442's acceptance criterion): the writer did the one thing the contract
permits — re-read — and settled each one. Of the two, **one had landed and one had not**, which
is precisely why the contract forbids guessing.

A second run, after the leg was hardened (below), reproduced it independently: `399 committed, 0
settled by re-read, 1 unknown-result commit(s), 0 phantom conflict(s); final version 399` — that
1021's batch had *not* landed, and the accounting closes on the other side of the same rule.

**An unperturbed run now FAILS.** The leg originally *printed a note* when the kill perturbed no
commit and passed anyway. That was the round-boundary hollow green one level up: honest in the
log, wrong in the exit code — `xtask fdb-metadata-tier1` would have reported that FoundationDB
"passed the battery" for a run in which the hard path never executed, and a gate that can record
a GO on that is worth nothing. It now asserts `unknown_results + faults + settled_by_reread > 0`
and fails as **inconclusive** otherwise, telling the operator to re-run (the kill window is
timing-dependent). Caught by a `codex review` of #535 — a note is not a gate.

Three properties this leg pins that nothing else could:

- **No phantom `Conflict`.** With a single writer, nothing can legitimately lose a race, so an
  `Ok(Conflict)` would be a fault or an unknown result wearing the wrong hat — telling the caller
  *nothing was written* when something may have been. Zero occurred.
- **Atomicity across the kill.** Markers present were exactly `{0..final_version}` — no half-landed
  batch.
- **No double-apply.** 398 + 1 = 399 = the final version. Nothing landed that no caller was told
  about, and nothing was applied twice — which is the failure a silently-retried non-idempotent
  batch would produce.

## What this verdict does NOT license

**1. It is not a TiKV-vs-FDB comparison, because the TiKV leg is currently red — and not because
of this work.** The shared scenario's standing guard (TiKV must stay green in the same change)
could not be discharged: `WYRD_TIER1=1 cargo xtask metadata-tier1` fails at the rename, with
tikv-client's own 2 s per-RPC timeout surfacing during the forced leader election
(`PessimisticLockError { GrpcAPI(Cancelled, "Timeout expired") }`). This was **reproduced on
unmodified `main`, and again at `a4abb69` — before any of the #437/#515/#516/#517 work** — so it
is pre-existing and independent of the lift. It wants its own issue. Until it is green, the two
backends have been held to the same *code*, but only FDB has actually passed it here.

**2. It does not flip the production default.** That is a separate, deliberate step: the CLI
default is `redb` (`crates/server/src/cli.rs`), not TiKV, and `fdb` is an off-by-default cargo
feature because it links `libfdb_c` — which ADR-0042 explicitly relies on. Flipping it is a
build-system change plus a docs/canonical-stack change (`deploy/README.md`'s "currently
canonical"), not a match arm, and it should be its own reviewable change.

## Verdict

**GO** on the question this battery actually asked: *does our FoundationDB mapping layer hold up
under real faults and real contention?* It does. The commit classification is correct under a
lost race, under a symmetric isolation of the master, and under a commit proxy dying with the RPC
in flight; the unknown-result class is surfaced, distinguishable, never retried, and never
mistaken for a `Conflict`; and no batch was ever lost, torn, duplicated, or double-applied.

The two carve-outs above are not hedges — they are the parts a reader would otherwise assume were
covered. The evidence for everything claimed here is reproducible with one command:

```
WYRD_TIER1=1 cargo xtask fdb-metadata-tier1
```
