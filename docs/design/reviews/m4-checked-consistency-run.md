---
created: 16.07.2026
type: review
author: Eduard Ralph
tags:
  - review
  - verdict
  - consistency
  - elle
  - foundationdb
  - milestone-4
  - release-gating
---
# The checked consistency run: a recognized checker's verdict under a real fault (#408)

> The credibility artifact #329 asks for (DoD item 2). It records what was run, what a
> **recognized checker** said about it, and — as carefully as it records the verdict — what
> that verdict does **not** license. **Verdict: Elle returns `true` for both models over a
> genuinely concurrent history recorded under a materialized network partition of a live
> 3-node FoundationDB cluster.**

## Why this run exists

Every prior claim that Wyrd "doesn't lose data" was ours, checked by our own code. That is
worth exactly as much as the reader's trust in us. #329's point is to replace it with
something an outsider can inspect: a real workload, driven against a real cluster, while a
real fault bites, with the resulting history judged by a checker **we did not write** —
[Elle](https://github.com/jepsen-io/elle), via
[elle-cli](https://github.com/ligurio/elle-cli), the checker the distributed-systems
community already recognizes.

The value is entirely in the parts we don't control. So the run is built to make it *hard*
for us to fool ourselves, and every claim below is one the checker or the nemesis leg made,
not one the scenario asserted.

## What was run

Verbatim, the artifact the runner emitted (`target/consistency-run/report.md`) is appended at
the bottom of this document. In prose:

- **Workload** — three pools over the production S3 wire (signed HTTP → `S3Gateway` →
  `Gateway` → `FdbMetadataStore`), all driven **inside** the fault window:
  - a **register overwrite pool** (Elle-fed): one writer overwriting a shared key with a
    unique ascending version, one concurrent reader. 120 ops.
  - a **register delete pool** (Wyrd-checked): PUT/GET/DELETE on keys disjoint from the
    Elle-fed pool's, one writer per key. 480 ops.
  - a **directory create pool** (Elle-fed, `set` model): 120 create-only unique integer
    members, closed by ONE composed full-set read after heal + quiesce. 121 history ops.
- **Nemesis** — #407's `PartitionLeg`: `fdb0` (172.30.58.11:4500) severed from its peers with
  `iptables`. The leg's own typed evidence attests it bit: `peers_saw_target before=true
  during=false`, `target_running_during=true` (a crash is not a partition).
- **Checker** — elle-cli 0.1.9, upstream revision `6d4afc4c5f`, jar sha256
  `c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a`.
  `--model rw-register` over the register history, `--model set`
  over the directory history.
- **Verdict** — `true` for **both** models. The Wyrd-checked delete pool's three #406 checks
  (`session_read_your_writes`, `session_monotonic_reads`, `reads_monotone_per_key`) all held.

## The three things that stop this being theatre

A verdict is only worth the history it judged, so the run refuses to report one unless:

1. **The history is genuinely concurrent** (#406 INV-2). Not "two clients ran" — at least one
   *same-key read↔write overlap across distinct processes*, the only overlap that constrains a
   register. A history of parallel-but-unrelated ops proves nothing, and #250 is what happens
   when nobody checks. `genuinely_concurrent: true`.
2. **The fault materialized**, per the leg's **own sampled evidence**, not a boolean the
   scenario set. The v1 iteration of this work hard-coded `nemesis_materialized = true`; that
   is precisely the artifact-shaped nothing this run exists to not be.
3. **The composed final read is determinate.** If the post-heal sweep cannot resolve a member,
   the read is emitted `:info` and the run goes INCONCLUSIVE — see the carve-out below.

Failing any of these makes the run inconclusive, which is a **non-zero exit**, not a footnote.
And `:unknown` from the checker is inconclusive too, never a pass: elle-cli exits **0** on
`:unknown`, so the verdict is keyed on the token, never the exit code.

The fixtures self-check runs on every live run: known-good, known-bad, and a degraded history
are fed through the *same* jar that judged the real history, and must come back `true`,
`false`, and `:unknown` respectively. A checker build that blessed the known-bad fixture would
fail the run before its verdict was trusted.

## What this run does NOT license

These are not hedges. They are the parts a reader would otherwise reasonably assume were
covered, and naming them is the difference between an artifact and an advertisement.

1. **The client never saw the fault.** This is the most important line in this document. Every
   one of the 720 ops succeeded: `info: 0`, `fail: 0`. Partitioning one of three nodes leaves a
   2/3 coordinator quorum, and `double` replication keeps every key served — so FoundationDB
   absorbed the partition and the workload never observed an error. What this run proves is
   that **a real partition bit the cluster and the histories stayed linearizable**, not that
   Wyrd's mapping layer handles a *disruptive* fault gracefully. A partition that costs the
   quorum, or one that isolates the client from the whole cluster, is a **different run** and is
   not evidence in hand. (#442's battery is where the disruptive-fault classification is
   exercised.)
2. **The register verdict binds to the overwrite pool only** — one key, one writer, one reader.
   It says nothing about DELETE traffic: the `rw-register` model has no faithful encoding for a
   delete (a nil-write makes a *correct* history come back `false`, and a 404-after-delete read
   is indistinguishable from unwritten), so delete traffic runs on a **disjoint key set** and is
   judged by Wyrd's own INV-1-sound checks. Those checks are strictly weaker than a
   linearizability verdict, and they are ours. Nobody external has blessed them.
3. **The directory claim is set-consistency, not namespace linearizability.** `--model set`
   asks one question: was any acknowledged `:add` missing from the final read? It does not order
   the creates against each other, and directory deletes/probes are excluded from the model by
   pool construction.
4. **One leg, one run, one topology.** `network-partition` against `deploy/fdb-multi-replica`.
   Clock-skew and process-pause legs are selectable and unexercised here; TiKV is out of scope
   (ADR-0042 pins FDB). A single green run is not a distribution.
5. **This is not a gate.** The run is opt-in (`WYRD_TIER1=1`) and off-Check: it needs Docker, a
   JVM, and the elle-cli jar, which ADR-0041/ADR-0016 keep out of `cargo xtask ci`. Wiring it
   into scheduled CI is #409's.

## On the two honesty rules the serialization had to obey

Both are INV-1 — *never fabricate certainty* — and both bit in ways worth recording, because
each one, left alone, would have produced a **confident, wrong** artifact rather than an
obviously broken one:

- **An indeterminate op is `:info`, never a definite completion.** A write whose round trip
  failed may or may not have committed. Recording it as `:ok` invents a fact; dropping it from
  the history is worse, because the ops that raced the nemesis are exactly the ones that would
  vanish, handing the checker a history that reads like an unremarkable clean run.
- **The dual: never fabricate a *violation* either.** Two of these were caught before this run
  and are why the run is trustworthy. (a) The delete pool originally gave two processes disjoint
  version *bands* on one shared key — but the version tag is client-assigned, so it orders by
  *writer*, not by *commit*, and all three #406 checks compare raw tags. A perfectly
  linearizable execution would have been reported as a real violation on a live cluster. Fixed
  at the premise: one writer per key. (b) The post-heal sweep originally dropped members whose
  probe errored — and in the `set` model an acknowledged `:add` missing from a definite `:ok`
  read **is a lost element**, so an unanswered probe would have become a `false` from Elle. Now
  an unresolved member degrades the whole composed read to `:info`, which the real checker
  answers with `:unknown` ⇒ inconclusive. The honest outcome is enforced by the checker itself,
  not only by our gate.

## Reproducing it

```
export WYRD_ELLE_CLI_JAR=/path/to/elle-cli-0.1.9-standalone.jar
WYRD_TIER1=1 cargo xtask consistency-run
```

Needs Docker, a JVM, `unzip`, and `libfdb_c`. The runner stands up
`deploy/fdb-multi-replica` itself, drives the workload under the leg, tears the stack down
unconditionally, and writes the histories, the run summary, and the report under
`target/consistency-run/`. Both EDN histories are re-checkable by hand against any elle-cli:

```
java -jar $WYRD_ELLE_CLI_JAR --model rw-register target/consistency-run/register-history.edn
java -jar $WYRD_ELLE_CLI_JAR --model set         target/consistency-run/directory-history.edn
```

## The artifact, verbatim

The report exactly as the runner emitted it on 2026-07-16 — byte-for-byte, including the full
member-id map. The map is what lets a reader resolve any integer element in the checked `set`
history back to the object the run actually created on the wire, so it is reproduced in full
rather than summarised: an artifact that asks to be trusted should not ask the reader to take
its own key on faith. All 120 created elements were observed present in the composed final
read.

```
# Checked consistency run report (#408)

- **Workload:** register overwrite PUT/GET (Elle-fed: 1 writer + 1 reader, shared key) + register PUT/GET/DELETE (Wyrd-checked: 2 processes, disjoint key, judged by the #406 session/monotonicity checks) + directory create-only unique integer members (set model), over the S3 wire against the live FDB cluster; a composed post-heal full-set read closes the directory history
- **Nemesis:** `partition` on fdb0 (wyrd-consistency-run-fdb0-1 @ 172.30.58.11:4500) — materialized: true — evidence: peers_saw_target before=true during=false (must flip true→false), target_running_during=true (must be true — a crash is not a partition)
- **History size:** register (Elle-fed): 120 ops (OutcomeCounts { invoked: 120, ok: 120, fail: 0, info: 0 }); directory (set): 121 ops (OutcomeCounts { invoked: 120, ok: 120, fail: 0, info: 0 }, incl. ONE composed post-heal full-set read over 120 members — the sweep's per-member probes are that read's raw material, not history ops); delete pool (Wyrd-checked, not serialized to EDN): 480 ops (OutcomeCounts { invoked: 480, ok: 480, fail: 0, info: 0 })
- **Model:** rw-register (register), set (directory); the delete pool is judged by the #406 session/monotonicity checks, which no Elle model can represent
- **Checker:** elle-cli 0.1.9 (revision 6d4afc4c5f794e8cb038bb33de465f66cb21f3a4, jar sha256=c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a). fixtures self-check PASSED (both models, both polarities, plus the degraded composed read): register-history-known-good.edn (rw-register) -> Pass; register-history-known-bad.edn (rw-register) -> Violation("the checker's trailing token was `false` — a genuine consistency violation: `/home/eddie/development/wyrd/wyrd.pdca-wt/xtask/tests/fixtures/consistency-run/register-history-known-bad.edn \t false`"); directory-history-known-good.edn (set) -> Pass; directory-history-known-bad.edn (set) -> Violation("the checker's trailing token was `false` — a genuine consistency violation: `/home/eddie/development/wyrd/wyrd.pdca-wt/xtask/tests/fixtures/consistency-run/directory-history-known-bad.edn \t false`"); directory-history-indeterminate-final-read.edn (set) -> Inconclusive("the checker returned `:unknown` (it could not decide — often a rejected vocabulary) — inconclusive, never a pass: `/home/eddie/development/wyrd/wyrd.pdca-wt/xtask/tests/fixtures/consistency-run/directory-history-indeterminate-final-read.edn \t :unknown`")
- **Member-id map:** 120 members: 1 -> `dir/member-1`, 2 -> `dir/member-2`, 3 -> `dir/member-3`, 4 -> `dir/member-4`, 5 -> `dir/member-5`, 6 -> `dir/member-6`, 7 -> `dir/member-7`, 8 -> `dir/member-8`, 9 -> `dir/member-9`, 10 -> `dir/member-10`, 11 -> `dir/member-11`, 12 -> `dir/member-12`, 13 -> `dir/member-13`, 14 -> `dir/member-14`, 15 -> `dir/member-15`, 16 -> `dir/member-16`, 17 -> `dir/member-17`, 18 -> `dir/member-18`, 19 -> `dir/member-19`, 20 -> `dir/member-20`, 21 -> `dir/member-21`, 22 -> `dir/member-22`, 23 -> `dir/member-23`, 24 -> `dir/member-24`, 25 -> `dir/member-25`, 26 -> `dir/member-26`, 27 -> `dir/member-27`, 28 -> `dir/member-28`, 29 -> `dir/member-29`, 30 -> `dir/member-30`, 31 -> `dir/member-31`, 32 -> `dir/member-32`, 33 -> `dir/member-33`, 34 -> `dir/member-34`, 35 -> `dir/member-35`, 36 -> `dir/member-36`, 37 -> `dir/member-37`, 38 -> `dir/member-38`, 39 -> `dir/member-39`, 40 -> `dir/member-40`, 41 -> `dir/member-41`, 42 -> `dir/member-42`, 43 -> `dir/member-43`, 44 -> `dir/member-44`, 45 -> `dir/member-45`, 46 -> `dir/member-46`, 47 -> `dir/member-47`, 48 -> `dir/member-48`, 49 -> `dir/member-49`, 50 -> `dir/member-50`, 51 -> `dir/member-51`, 52 -> `dir/member-52`, 53 -> `dir/member-53`, 54 -> `dir/member-54`, 55 -> `dir/member-55`, 56 -> `dir/member-56`, 57 -> `dir/member-57`, 58 -> `dir/member-58`, 59 -> `dir/member-59`, 60 -> `dir/member-60`, 61 -> `dir/member-61`, 62 -> `dir/member-62`, 63 -> `dir/member-63`, 64 -> `dir/member-64`, 65 -> `dir/member-65`, 66 -> `dir/member-66`, 67 -> `dir/member-67`, 68 -> `dir/member-68`, 69 -> `dir/member-69`, 70 -> `dir/member-70`, 71 -> `dir/member-71`, 72 -> `dir/member-72`, 73 -> `dir/member-73`, 74 -> `dir/member-74`, 75 -> `dir/member-75`, 76 -> `dir/member-76`, 77 -> `dir/member-77`, 78 -> `dir/member-78`, 79 -> `dir/member-79`, 80 -> `dir/member-80`, 81 -> `dir/member-81`, 82 -> `dir/member-82`, 83 -> `dir/member-83`, 84 -> `dir/member-84`, 85 -> `dir/member-85`, 86 -> `dir/member-86`, 87 -> `dir/member-87`, 88 -> `dir/member-88`, 89 -> `dir/member-89`, 90 -> `dir/member-90`, 91 -> `dir/member-91`, 92 -> `dir/member-92`, 93 -> `dir/member-93`, 94 -> `dir/member-94`, 95 -> `dir/member-95`, 96 -> `dir/member-96`, 97 -> `dir/member-97`, 98 -> `dir/member-98`, 99 -> `dir/member-99`, 100 -> `dir/member-100`, 101 -> `dir/member-101`, 102 -> `dir/member-102`, 103 -> `dir/member-103`, 104 -> `dir/member-104`, 105 -> `dir/member-105`, 106 -> `dir/member-106`, 107 -> `dir/member-107`, 108 -> `dir/member-108`, 109 -> `dir/member-109`, 110 -> `dir/member-110`, 111 -> `dir/member-111`, 112 -> `dir/member-112`, 113 -> `dir/member-113`, 114 -> `dir/member-114`, 115 -> `dir/member-115`, 116 -> `dir/member-116`, 117 -> `dir/member-117`, 118 -> `dir/member-118`, 119 -> `dir/member-119`, 120 -> `dir/member-120`. Composed post-heal full-set read: DETERMINATE — observed 120 of 120 created elements present [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120]
- **Verdict:** register: Pass; directory: Pass; delete pool: all #406 checks held
```
