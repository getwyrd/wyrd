---
created: 02.07.2026 12:00
type: adr
status: Proposed
tags:
  - adr
  - testing
  - consistency
  - jepsen
---
# 0041. Consistency-checker substrate: model the mutable metadata register, not the immutable data path

## Context

ADR-0039 realized proposal 0005 §13.2's Tier-1 consistency leg as an in-repo Rust
scenario over the custodian repair path, and **deferred the literal, externally
recognizable Jepsen/Elle credibility artifact to issue #329**, "blocked on substrate
that can yield a non-vacuous, checkable history against Wyrd's actual model." This ADR
records that substrate decision, which is what unblocks #329.

The block was real but mis-framed. Eight iterations under #250 pointed a literal
Clojure/Jepsen/Elle harness at Wyrd and produced only **vacuous histories**, and
ADR-0039 drew the conclusion that Wyrd "is an immutable, single-write-per-key object
store" for which "the 'list' or 'register' has to be invented client-side, so the
checker validates the harness's own bookkeeping." That is true of the layer those
iterations observed — the **chunk/fragment data path** (`FragmentId → bytes`,
content-addressed, written once by `put_fragment`, never mutated), which is also the
layer ADR-0039's shipped Option-B scenario exercises over the repair path. A register
or list-append checker (Elle, Knossos) presupposes a **mutable, linearizable register**:
a key whose value is overwritten and read back over time. The immutable data path has
no such key, so any history over it is vacuous.

But Wyrd's consistency contract (ADR-0015) does not constrain the immutable data path.
It constrains the **metadata layer, which is mutable and already carries a genuine
linearizable register in the shipped product**:

- **Guarantee 2 — "a file's writes are linearizable at its home zone; the commit point
  totally orders its versions."** The file is the inode record keyed by `inode:<id>`,
  whose monotonic `version` is bumped under a full-value compare-and-set at the single
  commit point: `commit_overwrite` / `commit_chunk_map` write `version = prior.version
  + 1` gated on `require(prior)`, and "exactly one concurrent writer wins" (a stale
  writer loses with `Conflict`). A key overwritten v1→v2→v3 and read back over time,
  with an atomic CAS linearization point — this is precisely the mutable register Elle
  needs, and it exists today.
- **Guarantee 1 — "namespace operations are linearizable globally."** A directory's
  entry set (`dirent:<parent>/<name>`) is mutated by create / delete / rename (rename
  is a single dirent mutation); a directory is a mutable list/set.
- **Guarantee 3 — "per-session read-your-writes and monotonic reads."** A session's
  view of a file's current version, and the reserved `meta:version` high-water mark
  (the Option-C version fence), are session-scoped orderings over mutable values.

So the "substrate decision" is a **targeting decision**, not a missing capability: aim
the checker at the mutable metadata register the contract actually governs, not at the
single-write data path the Option-B scenario already covers.

## Decision

We will build #329's literal consistency artifact against the **mutable metadata
observables that ADR-0015 constrains**, modelling them with standard checker models:

1. **A file is a read-write register.** The primary, load-bearing model: a key is an
   object path / inode; a write is an overwriting PUT that bumps the inode `version`
   under the commit-point CAS; a read is a GET returning the current version and value.
   The workload drives **concurrent overwrites and reads of a small shared key set**
   under the fault nemesis, and the recorded history is checked for linearizability of
   the commit point (guarantee 2) — read-after-commit, no torn or stale reads,
   exactly-one-writer-wins, and no version regression. This is the non-vacuous history
   #329 requires; it MUST be modelled on the metadata register, never on the immutable
   fragment layer.
2. **A directory is a list (list-append) / set.** The secondary model: create appends a
   named entry, delete removes it, rename moves it; a directory read returns the entry
   set. Checked for namespace linearizability (guarantee 1) — no lost create, no
   resurrected delete.
3. **Sessions carry read-your-writes and monotonic-read checks** over the register and
   the `meta:version` high-water mark (guarantee 3).

The verdict engine SHOULD be **Elle**, or an equivalent externally recognized checker —
the artifact's value is external recognizability, so a recognized checker is preferred
over a bespoke one. The workload driver MAY be Rust emitting a checker-compatible
history rather than a full Clojure/Jepsen stack, provided the checker itself is the
recognized one; the choice of literal-Jepsen-vs-Rust-driver is left to #329's
implementation. Whatever the toolchain, the checker and any JVM/Clojure dependency
**MUST run only in a privileged off-Check job and MUST NOT enter `cargo xtask ci`**
(ADR-0016); the unprivileged merge gate stays pure-Rust.

We reject: modelling the immutable chunk/fragment data path as the checker's register
(the vacuous-history mistake of the #250 iterations); and inventing a client-side
"list" or "register" not backed by a real mutable key in the product (which validates
the harness's bookkeeping, not Wyrd's contract).

## Consequences

This unblocks #329: the substrate exists in the shipped product, so the artifact is now
a build task, not a research question. It complements ADR-0039 rather than superseding
it — the two observe different layers on purpose. Option B (ADR-0039) asserts
commit-point atomicity, exactly-once convergence, and byte-identical decode over the
**immutable repair path** with scripted oracles; this ADR aims a **randomized,
checker-validated** workload at the **mutable commit-point register**. Together they
cover the durability side (repair is atomic and loses nothing) and the consistency side
(concurrent writers linearize) of ADR-0015.

Two dependencies follow, and they place the artifact at M4, not earlier. First, a
**networked client observable**: there is no client-facing object API today (the gateway
is in-process, only the fragment-level gRPC `ChunkStore` is networked), so the harness
needs the S3 HTTP wire surface (#364) — or a thin client-facing shim — to drive
overwriting PUT / GET / list / rename with client-observed real-time order. Second, a
**real cluster and partition nemesis**: the M4 `deploy/` cluster and the `tc netem` /
`iptables` partition + clock-skew + process-pause nemesis that M4.6 (#257) already
scopes. #329 therefore builds on #257's cluster and nemesis, adding the register /
list-append models, the workload, and the recognized checker; the two are sequenced
(nemesis first, then the checked artifact), not merged.

Scope is single-zone for M4: the artifact validates guarantee 2's home-zone
linearizability. The cross-zone strengthening — home-zone failover surviving via the
`meta:version` high-water mark, the Option-C fence — is a later artifact at M10/M11, and
the register model here is exactly what extends to it. A bug the checker finds is
minimized to a seed and promoted into DST as a permanent regression (ADR-0009), the
same discipline as every other tier. The clock-skew nemesis interacts with ADR-0024's
clock-trust posture and with the checker's real-time order, which #329 must account for.

This commits the project to a public consistency artifact modelled on the metadata
register; reversing it would mean either accepting a bespoke non-recognized checker or
conceding that Wyrd has no non-vacuous consistency history — the latter now known to be
false. This ADR refines the deferral recorded in ADR-0039 and is the substrate decision
named in #329's definition of done; it references ADR-0015 (the contract checked),
ADR-0016 (privileged tiers out of `xtask ci`), and ADR-0009 (bug-finding run →
regression).
