---
created: 02.07.2026 12:00
type: adr
status: Accepted
tags:
  - adr
  - testing
  - consistency
  - jepsen
---
# 0041. Consistency-checker foundation: model the mutable metadata register, not the immutable data path

## Context

The goal is to prove Wyrd's consistency claims with a recognized, industry-standard
checker (Jepsen/Elle), so that there is an objective measurement of its performance. ADR-0039 built the Tier-1 consistency leg (from proposal
0005 §13.2) as an in-repo Rust scenario over the custodian repair path, but it
**deferred the actual Jepsen/Elle artifact to issue #329**, citing a block on
"substrate that can yield a non-vacuous, checkable history against Wyrd's actual
model." This ADR settles that foundation question, which is what unblocks #329.

The block was real, but it was aimed at the wrong layer. Attempts under #250
pointed a real Clojure/Jepsen/Elle harness at Wyrd and produced only **vacuous
histories** — runs that are technically valid but prove nothing. ADR-0039 concluded
from this that Wyrd "is an immutable, single-write-per-key object store" where "the
'list' or 'register' has to be invented client-side, so the checker validates the
harness's own bookkeeping." That conclusion holds — but only for the layer those
attempts were watching: the **chunk/fragment data path** (`FragmentId → bytes`),
which is content-addressed, written once by `put_fragment`, and never changed again.
That is also the layer ADR-0039's shipped Option-B scenario exercises over the repair
path.

Register and list-append checkers (Elle, Knossos) only work on a
**mutable key** — one whose value gets overwritten and read back over time. The
immutable data path has no such key, so any history over it is vacuous by
construction. The checker has nothing to check.

Wyrd's consistency contract (ADR-0015) was never about the immutable data path.
It is about the **metadata layer — which is mutable, and already ships with a genuine linearizable register today**:

- **Guarantee 2 — "a file's writes are linearizable at its home zone; the commit
  point totally orders its versions."** The "file" here is the inode record keyed by
  `inode:<id>`. Its `version` number only goes up, bumped under a full-value
  compare-and-set at a single commit point: `commit_overwrite` / `commit_chunk_map`
  write `version = prior.version + 1` gated on `require(prior)`, and "exactly one
  concurrent writer wins" (a stale writer loses with `Conflict`). A key that goes
  v1→v2→v3 and can be read back over time, with an atomic CAS as the linearization
  point — that is *exactly* the mutable register Elle wants, and it exists in the
  shipped product right now.
- **Guarantee 1 — "namespace operations are linearizable globally."** A directory's
  entry set (`dirent:<parent>/<name>`) changes via create / delete / rename (rename
  is one dirent mutation). A directory is a mutable list/set.
- **Guarantee 3 — "per-session read-your-writes and monotonic reads."** A session's
  view of a file's current version, plus the reserved `meta:version` high-water mark
  (the Option-C version fence), are per-session orderings over mutable values.

The "foundation decision" is therefore a **targeting decision**, not a missing
capability: point the checker at the mutable metadata register the contract actually
governs — not at the single-write data path the Option-B scenario already covers.

## Decision

#329's consistency artifact will be built against the **mutable metadata that
ADR-0015 actually constrains**, using standard checker models:

1. **A file is a read-write register.** This is the main, load-bearing model. The key
   is an object path / inode; a write is an overwriting PUT that bumps the inode
   `version` under the commit-point CAS; a read is a GET that returns the current
   version and value. The workload drives **concurrent overwrites and reads of a
   small shared set of keys** under the fault nemesis, and the recorded history is
   checked for linearizability of the commit point (guarantee 2): read-after-commit,
   no torn or stale reads, exactly-one-writer-wins, and no version going backward.
   This is the non-vacuous history #329 needs, and it MUST be built on the metadata
   register — never on the immutable fragment layer.
2. **A directory is a list (list-append) / set.** The secondary model: create appends
   a named entry, delete removes it, rename moves it; reading a directory returns its
   entry set. It is checked for namespace linearizability (guarantee 1): no lost
   create, no resurrected delete.
3. **Sessions carry read-your-writes and monotonic-read checks** over the register and
   the `meta:version` high-water mark (guarantee 3).

The verdict engine SHOULD be **Elle**, or an equally well-recognized checker. The
whole value of the artifact is that outsiders recognize it, so a recognized checker is
preferred over a homegrown one. The workload driver MAY be Rust that emits a
checker-compatible history rather than a full Clojure/Jepsen stack — provided the
*checker itself* is the recognized one. The literal-Jepsen-vs-Rust-driver choice is
left to #329's implementation. Either way, the checker and any JVM/Clojure dependency
**MUST run only in a privileged off-Check job and MUST NOT enter `cargo xtask ci`**
(ADR-0016); the unprivileged merge gate stays pure-Rust.

Two approaches are rejected: modelling the immutable chunk/fragment data path as the
checker's register (the vacuous-history mistake the #250 attempts made); and inventing
a client-side "list" or "register" that is not backed by a real mutable key in the
product (that only validates the harness's own bookkeeping, not Wyrd's contract).

## Consequences

This unblocks #329. The foundation already exists in the shipped product, so the
artifact is now a build task, not an open research question. It **complements**
ADR-0039 rather than replacing it — the two watch different layers on purpose.
Option B (ADR-0039) asserts commit-point atomicity, exactly-once convergence, and
byte-identical decode over the **immutable repair path** using scripted oracles; this
ADR aims a **randomized, checker-validated** workload at the **mutable commit-point
register**. Together they cover both sides of ADR-0015: durability (repair is atomic
and loses nothing) and consistency (concurrent writers linearize).

Two dependencies follow, and they place the artifact at M4, not earlier. First, a
**networked client observable** is required: there is no client-facing object API
today (the gateway is in-process; only the fragment-level gRPC `ChunkStore` is
networked), so the harness needs the S3 HTTP wire surface (#364) — or a thin
client-facing shim — to drive overwriting PUT / GET / list / rename with a
client-observed real-time order. Second, a **real cluster and partition nemesis** is
required: the M4 `deploy/` cluster and the `tc netem` / `iptables` partition +
clock-skew + process-pause nemesis that M4.6 (#257) already scopes. #329 therefore
builds on top of #257's cluster and nemesis, adding the register / list-append
models, the workload, and the recognized checker. The two are sequenced (nemesis
first, then the checked artifact), not merged.

Scope is single-zone for M4: the artifact validates guarantee 2's home-zone
linearizability. The cross-zone strengthening — home-zone failover surviving via the
`meta:version` high-water mark, the Option-C fence — is a later artifact at M10/M11,
and the register model here is exactly what extends to it. Any bug the checker finds
is minimized to a seed and promoted into DST as a permanent regression (ADR-0009),
the same discipline as every other tier. The clock-skew nemesis interacts with
ADR-0024's clock-trust posture and with the checker's real-time order, which #329
must account for.

This commits the project to a public consistency artifact built on the metadata
register. Reversing it would mean either accepting a homegrown, non-recognized checker
or conceding that Wyrd has no non-vacuous consistency history — and the latter is now
known to be false. This ADR refines the deferral ADR-0039 recorded and is the
foundation decision named in #329's definition of done. It references ADR-0015 (the
contract being checked), ADR-0016 (privileged tiers stay out of `xtask ci`), and
ADR-0009 (bug-finding run → regression).
