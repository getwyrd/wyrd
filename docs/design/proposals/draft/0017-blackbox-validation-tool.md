---
created: 16.08.2026 10:42
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#769"
tags:
  - proposal
  - s3
  - testing
  - endurance
  - validation
  - blackbox
---
# Proposal: `wyrd-validate` — blackbox protocol, workflow, and endurance validation

> **What this settles.** One out-of-process tool that exercises Wyrd through its S3 front door
> the way a client does — with no `wyrd-*` crate in its dependency closure, mechanically
> enforced — across three substrates and five named scenarios, from a two-minute correctness
> sweep to a months-long endurance run. It settles the **capability matrix** (which operations
> are required, which are expected-unsupported and against which issue), the **oracle** (what
> "correct" means for a byte), the **failure taxonomy** (what stops a run and what is merely
> counted), the **vacuity gate** (what makes a PASS meaningless), and the **two outputs** (a
> live event stream, and a durable verdict).
>
> **What this settles by exclusion.** The verdict covers **correctness only**. Throughput and
> latency are reported as evidence and never judged — there is no performance pass bar, because
> the right bar depends on hardware the tool cannot know. An operator reads the numbers and
> decides for themselves.
>
> **What this does not settle.** The Hetzner substrate — provisioning the
> [blueprint](../../architecture/m4-first-deployment-blueprint.md) §B.1 topology is a deployment
> concern that outlives this tool and is shared with the M7.6 graduation drill ([#485][i485]),
> tracked as [#737][i737].
> Nor any gateway behaviour: where the tool finds a gap, the gap is filed, not fixed here —
> including the three product changes its coverage wants (*Dependencies*). The `aws-cli` /
> `boto3` client-matrix leg ([#512][i512]) stays as scoped — see *Alternatives considered*.
>
> **Reading the citations.** `path:line` pairs are pinned to `origin/main` at commit `c824243`.
> Lines move; treat the path plus the quoted contract as authoritative.

## In plain language

Wyrd claims to speak S3. Today that claim is checked by tests living inside the Wyrd
workspace, compiled against Wyrd's own types, running for a few seconds. This adds a separate
program that knows nothing about Wyrd except its address. It uploads files, downloads them,
checks every byte came back, deletes whole directories, lists them, and keeps a private ledger
of what should be there. Point it at a laptop and it finishes in two minutes. Point it at
rented servers and it runs for months.

It has two audiences asking two questions with the same tool. **We** ask "is Wyrd correct."
An **operator** asks "is my hardware and configuration sound." Same method — drive the front
door, check what comes back — so the tool ships in the operator distribution alongside the
roles.

What it is really built to catch is not a wrong status code. It is the bug that only appears
at hour 40: the leak, the reclamation that falls behind, the listing that gets slower every
day. Those need wall-clock, and nothing in the tree currently supplies it.

## Motivation

**There is no endurance testing.** `grep -rn "soak\|endurance\|longevity"` across the tree
returns two hits, both about on-disk format longevity (ADR-0032, ADR-0042) and neither a test.
Every existing gate is short: `cargo xtask ci` is seconds-to-minutes, the DST tier is
deterministic and in-process, the Tier-2 integration tier brings a cluster up and tears it down
inside one job. ADR-0009's two tiers are both *short*. Time is an untested axis.

That axis has known bugs on it already:

- **[#560][i560]** — a buffered `put_object` never renews its lease, so a PUT slower than the
  30-second TTL fails with `Conflict`. The S3 gateway does not take that path — it streams, and
  the streaming path renews (`crates/server/src/lib.rs:322`) — but that is the point: the
  lease-lapse class is live in this codebase, and the renewal loop the S3 path *relies on* has
  never been exercised by a write long enough to need it. A multi-GB object over a real network
  is that write.
- **[#625][i625]** — abandoned multipart uploads are never reaped. Staged bytes accumulate.
  Invisible to any run that ends before anything is abandoned.
- **[#674][i674]** — a read's byte footprint at the `MetadataStore` seam is unbounded. A
  growth-shaped problem, and one that needs a large keyspace before it bites.

**How many of those the gating run actually reaches, stated plainly.** #560's path is disclaimed
two bullets up — the S3 gateway streams and does not take it. #625 needs multipart, which is
`unsupported(#508)` for the whole matrix, so an abandoned upload cannot be created at all. #674
lives in the `listing` scenario, not the gating `endurance` one. So of three bugs cited, the
**7-day gating run reaches none directly**; what it reaches is the *class* — leaks, growth and
reclamation over wall-clock — of which these three are the currently-known instances.

That is a weaker claim than the list implies on first reading, and it is the honest one. The
argument for building this is not "these three bugs will be caught by the gate"; it is that **no
mechanism exists to find the next one**, and every instance so far was found by reading code
rather than by running anything. A gate that reaches the class is worth having even when today's
known instances sit outside it — but the motivation should not borrow their credibility.

**There is no measured client bar.** [#512][i512] says it plainly: "usable S3" is asserted, not
tested. The existing interop tests (`crates/server/tests/s3_http_wire.rs`) cover the
PUT/GET/DELETE floor through `aws-sdk-s3`, already a dev-dependency
(`crates/server/Cargo.toml:126`).

**Unimplemented is not the same as safe.** [#504][i504] is the proof: `CopyObject` silently
overwrote the destination with an empty body because `x-amz-copy-source` was ignored. Live data
loss, from an operation nobody had implemented. The fix was to reject it
(`crates/gateway-s3/src/lib.rs:1730`) — but nothing continuously checks that it *stays*
rejected, or that the next unimplemented operation fails loudly rather than quietly. A
declared, asserted capability matrix is the general form of that fix.

**A container fleet cannot prove what Alpha claims.** [#485][i485] states it directly:
failure-domain independence "is a topology property, not a process count — the one thing no
single-host container cluster can prove."

## Design

### 1. Three substrates, one binary

Not interchangeable. Each proves something the others cannot, so the tool runs unmodified
against all three and the verdict records which one it faced.

| Substrate | Shape | Proves | Cannot prove |
|---|---|---|---|
| **loopback** | one `wyrd s3` process, redb + local FS | protocol semantics, gateway-level leaks | anything distributed |
| **fleet** | `deploy/small-multi-node-fdb`, 21 containers, 3 gateways on host ports 8081–8083 | topology, wiring, integration | independent failure, honest throughput |
| **hetzner** | [blueprint](../../architecture/m4-first-deployment-blueprint.md) §B.1: ~13–15 servers, private vSwitch | honest failure domains, honest performance, real endurance | — |

Substrate selection is not a tool concern. `wyrd-validate` takes `--endpoint` and credentials.
Everything else is the launcher's problem, and on Hetzner there is no launcher.

### 2. Layering

The repo's own convention, stated in `xtask/src/consistency_run_runner.rs:8-12`: everything
decision-shaped is pure and Check-tested; the runner owns "only the I/O."

```
┌─ crates/validate ──────────── package + binary: wyrd-validate ──────────┐
│  deps: aws-sdk-s3, tokio, sha2, serde  —  NO wyrd-* (enforced, §9)      │
│                                                                          │
│  pure, Check-tested        matrix.rs     the capability table            │
│                            scenario.rs   the five declared shapes        │
│                            oracle.rs     model + verification            │
│                            payload.rs    seed → deterministic bytes      │
│                            verdict.rs    coverage floor → PASS/FAIL/INC  │
│                                                                          │
│  I/O only                  client.rs     aws-sdk-s3 calls                │
│                            flow.rs       cp / ls / rm -r / sync          │
│                            state.rs      checkpoint save + restore       │
│                            emit.rs       event stream + verdict artifact │
│                            main.rs       arg parsing, pool supervision   │
└──────────────────────────────────────────────────────────────────────────┘
              ▲                                    ▲
              │ exec                               │ exec (nothing between)
┌─────────────┴───────────────────┐   ┌────────────┴────────────────────────┐
│ xtask/src/s3_blackbox.rs        │   │ Hetzner: run by hand, or a systemd  │
│ compose up → wait healthy       │   │ unit on its own node. Faults are    │
│ → run wyrd-validate             │   │ manual (`hcloud server delete`).    │
│ → compose down (always)         │   │ Substrate provisioned separately.   │
│ ALSO owns fault injection (§11) │   └─────────────────────────────────────┘
│ loopback + fleet ONLY           │
└─────────────────────────────────┘
```

`wyrd-validate` **ships in the operator tarball** ([#570][i570]'s `cargo xtask dist` pipeline),
so an operator can validate their own deployment. That makes it the tarball's second binary:
`deploy/dist/README.md` currently opens "One `wyrd` binary serves every role as a subcommand",
which stays true — it describes Wyrd's *roles*. The validation tool is deliberately not one of
them, and must not become a `wyrd` subcommand: sharing a binary would share a dependency
closure and destroy the property §9 exists to enforce.

**Shipping it is a larger change than "the staging grows an entry", which an earlier revision
implied.** `cargo xtask dist` does not assemble a tarball from build outputs — it "extracts the
binary out of the image (`docker create` + `docker cp`), so the tarball's `bin/wyrd` is
bit-identical to the image's `/usr/local/bin/wyrd`" (`xtask/src/dist.rs:6-7`). The production
Dockerfile builds `--bin wyrd` and describes itself as hosting "just the `wyrd` binary";
`install.sh` installs one path and uninstalls one path; `.github/workflows/release.yml`
smoke-tests `/usr/local/bin/wyrd` and asserts its absence after uninstall. So a second binary
touches the Dockerfile, the dist extraction, the installer, the uninstaller and the release
workflow.

It also poses a question this proposal does not answer: **should the production OCI image carry a
validation tool at all?** Shipping it in the tarball and keeping it out of the image is coherent —
operators install from the tarball — but it breaks the bit-identical guarantee dist currently
relies on, so the two artifacts would need separate assembly paths. That is a packaging decision
for whoever owns the release pipeline.

One further caveat: `xtask/tests/dist_templates.rs` is "container-free by design … every template
assertion is a file read + substring check", with the real build deferred to the release workflow.
So it can pin *templates* naming a second binary, but it cannot observe a tarball. Any
definition of done phrased as "the tarball contains both binaries" is unverifiable by the gate it
would name.

### 3. The capability matrix

The centrepiece. Every operation carries a **declared expected state**, and the state is
asserted — not just the success path.

- `required` — must succeed, and its result must satisfy the oracle.
- `unsupported(#NNN)` — must fail, **with the declared status and S3 error code**. Not a 500,
  not a hang, and emphatically not a success.

An `unsupported` operation that succeeds is a failure of class `unexpected-support`, and it is
fatal. That is [#504][i504] generalised: the day an unimplemented operation starts half-working
is the day you want to hear about it, not the day you find the empty objects.

**Protocol operations** (state as of `c824243`):

| Operation | State | Reference |
|---|---|---|
| `PutObject` | required | floor, [#364][i364] |
| `GetObject` (+ `Range`, conditional) | required | [#510][i510] |
| `HeadObject` | required | [#506][i506] |
| `DeleteObject` | required | floor |
| `DeleteObjects` (bulk `POST ?delete`) | required | [#509][i509]; body cap 8 MiB, `lib.rs:439` |
| `ListObjectsV2` (+ v1 shim, prefix/delimiter/pagination) | required | [#507][i507] |
| `CopyObject` | `unsupported(#766)` | rejected at `lib.rs:1730`; [#504][i504] closed the *reject*, [#766][i766] is the feature |
| `CreateMultipartUpload` | `unsupported(#508)` | via [#668][i668] |
| `UploadPart` | `unsupported(#508)` | via [#669][i669] |
| `CompleteMultipartUpload` | `unsupported(#508)` | via [#670][i670] |
| `AbortMultipartUpload` / `ListParts` / `ListMultipartUploads` | `unsupported(#508)` | via [#668][i668] |
| `CreateBucket` / `DeleteBucket` / `HeadBucket` / `ListBuckets` / `GetBucketLocation` | `unsupported(#511)` | ADR-0046 accepted, unbuilt |

**Workflows**, implemented in the tool over those primitives — the `aws s3` level:

| Workflow | Decomposes to | State |
|---|---|---|
| `cp` (upload) | `PutObject`, or multipart above threshold | required (multipart leg gated on [#508][i508]) |
| `cp` (download) | `GetObject` | required |
| `ls` | `ListObjectsV2` with `delimiter=/` | required |
| `rm --recursive` | paginated `ListObjectsV2` + `DeleteObjects` batched at 1000 | required |
| `sync` | `ListObjectsV2` + size/mtime compare + `PutObject`/`GetObject` | required |
| `mv` | `CopyObject` + `DeleteObject` | **blocked on [#766][i766]** |
| `rb` | `DeleteBucket` | **blocked on [#511][i511]** |

Two operations the Alpha bar names in everyday terms map onto gaps, and the matrix is where
that becomes visible rather than surprising. **"Rename" is not an S3 operation** — it is
copy-then-delete, so `mv` cannot work until [#766][i766] lands. [#504][i504] closed the
*safety* half (stop the silent overwrite); the implementation was never filed until this review
found the gap, so an earlier revision of this proposal named a closed issue as a live blocker.
**"Directory removal" is not one either** — it is list-then-bulk-delete, and both halves are
built, so `rm --recursive` works today.

**Negative authentication vectors** — bad signature, wrong region scope, expired timestamp,
missing `x-amz-content-sha256`, and [#491][i491]'s percent-encoded subresource bypass. These
assert the gateway is fail-closed, which is exactly what an operator wants to know about their
own deployment. **Off by default** so long runs do not spend operations on them; the `smoke`
scenario declares them **on** as part of its shape, so the correctness sweep always includes
them.

**[#491][i491] is currently a live defect, so this makes `smoke` red on every existing build.**
Its percent-encoded spelling of `partNumber` misses the raw-string denylist, falls through to the
plain object PUT path, and overwrites the whole object instead of returning 501 — a data-loss bug,
open against *Foundations*. A vector that asserts the correct behaviour therefore fails today, by
design: that is the vector working.

Two consequences, both stated rather than discovered later. **#491 is a hard dependency of any
graduation criterion that requires a green `smoke`** — see *Graduation criteria*, where criterion
1 is scoped accordingly. And until it closes, a red `smoke` on that vector is the expected state,
not a regression, so the run must distinguish "known-open defect" from "new failure" rather than
reading as broken.

**The matrix must be keyed to the server version.** Expectations change as slices land, so a
tool validating an older release would otherwise fail on rows naming issues fixed after that
build. The gateway advertises no version today — no `Server:` header, nothing on the wire — so
this needs the version header of [#736][i736] (*Dependencies*). Until it lands, `--server-version` is an
operator-supplied parameter, with the honest weakness that a parameter can be silently wrong.

**Profiles are versioned and retained — rows are never flipped in place.** An earlier revision
said "when #508 lands, the change is flipping six rows", which contradicts the version-keying
requirement two paragraphs up: flipping discards the expectations for every release where those
operations *were* unsupported. A newer tool pointed at an older release would then classify its
correct `NotImplemented` responses as failures, even when handed the right `--server-version`.

So the matrix is a set of **immutable per-release profiles** keyed by the version range each
applies to. Landing #508 adds a profile whose multipart rows are `required`; it does not edit the
profile that preceded it. Selecting a server version selects a profile, which is what makes
version-keying mean anything rather than being a flag the tool accepts and ignores.

Each profile is a reviewed constant with citations, and the retained ones are how a validator
shipped in an operator tarball stays useful against the release that operator actually runs.

### 4. Size classes

Chosen to straddle boundaries on both sides of the wire — S3's, and Wyrd's own.

| Size | Boundary it straddles | Source |
|---|---|---|
| 0 B | empty object | S3 edge case |
| 1 B | minimum non-empty | — |
| 4 KiB, 64 KiB | sub-chunk | — |
| `C` − 1, `C`, `C` + 1 | **the configured chunk size** | `--chunk-size` |
| 5 MiB | S3 minimum part size | S3 spec |
| 8 MiB | stock SDK multipart threshold; bulk-delete body cap | `lib.rs:439` |
| 64 × `C` | multi-chunk, many fragments | — |
| segmentation threshold ± | **chunk-map segmentation boundary** | `--segment-threshold-bytes`; **needs [#635][i635]** |
| 5 GiB | S3's largest accepted single PUT — **must succeed** | S3 spec |
| 5 GiB + 1 | first size past it — **must fail `EntityTooLarge`** | [#671][i671]; **unimplemented today** |
| > 5 GiB | multipart-only territory | gated on [#508][i508] |

Two corrections to an earlier revision, both material to what the matrix can assert:

**The 5 GiB boundary is two rows, not one.** It previously read "S3 single-PUT maximum →
`EntityTooLarge`", which is self-contradictory: 5 GiB is the largest single PUT S3 *accepts*, so
it must **succeed**; 5 GiB + 1 is the first that must fail. Conflating them left the matrix
unable to assign a required outcome at the boundary it exists to test. Note also that
`EntityTooLarge` appears **nowhere** in `crates/gateway-s3/` today — the row is
`unsupported(#671)` until that lands, not a required behaviour.

**The segmentation class is multipart-only, and unreachable through `PutObject` by
construction.** Two revisions got this wrong — first omitting the dependency entirely, then
naming [#635][i635] as the blocker. #635 is necessary but not sufficient, and the deeper
constraint is a *decision*, not a missing implementation. Proposal 0016 settles it:

> A published map of `≤ MAX_MAP_CHUNKS` chunks stays a flat inline map (unchanged, today's
> shape); a larger map is produced **only** by a multipart session, and is segmented.

A single `PutObject` publishes a **flat** map and reaches large sizes by *raising its chunk size*
— `chunk_size_effective = max(DEFAULT_CHUNK_SIZE, ⌈Content-Length / MAX_MAP_CHUNKS⌉)` — because it
carries a declared `Content-Length` and has "no session record to fence, no epoch to key segments
by, and so no anchor for the staged-publication protocol".

Three consequences for this table:

- The **segmentation threshold class must go through `CompleteMultipartUpload`**, so it is gated
  on [#508][i508], not merely on #635. Driving it with a large `PutObject` would exercise a
  large *flat* map — a different code path wearing the same size label, which is worse than not
  testing it, because the run would report coverage it never had.
- **`--chunk-size` does not describe large objects.** The gateway overrides it above the flat-map
  ceiling, so the `C ± 1` classes are meaningful for small objects only. The tool cannot assume
  the configured value was the one used, and the verdict should not imply it was.
- The gateway's own `EntityTooLarge` guard fires when a PUT cannot fit even at `chunk_size_max`,
  which 0016 records as "a guard, not a routine path" since anything inside S3's 5 GiB range
  always fits. So the 5 GiB + 1 row tests **S3's** limit, not Wyrd's guard; the two share an error
  code and are different assertions.

#635 remains a prerequisite for the resolver those objects need on read
(`crates/core/src/metadata.rs:262-266`: the slice "lands the shape … no resolver, no producer",
and every existing consumer treats `ChunkMap::Segmented` as `SegmentedMapUnsupported`) — but it
is the second gate, behind #508.

**The tuning values are inputs, not assumptions.** A blackbox tool cannot import
`DEFAULT_CHUNK_SIZE` or `MAX_ROOT_VALUE_BYTES` — that is the price of §9. Rather than hardcode
a table that goes stale when either moves, the tool takes them as parameters and computes the
sizes at runtime:

```
--chunk-size BYTES                  the deployment's configured chunk size
--max-root-value-bytes BYTES        the deployment's configured root budget
--segment-threshold-bytes BYTES     object size at which the chunk map must segment
                                    (optional; derived from the two above when omitted)
```

Both depend on becoming deployment configuration rather than compile-time constants — tracked
separately (*Dependencies*). Until then the defaults mirror today's values and the verdict
records what was used, so a run's size coverage is readable against the deployment it faced.

The **segmentation threshold** is the one value the tool cannot compute from the other two,
because the per-chunk record's encoded width is an internal detail. It is an explicit parameter
with a derived default. The obligation is only to *cross* the boundary — segmentation is not
observable over the S3 wire, so the tool exercises it, it does not assert it.

Payloads are **derived, never stored**: `bytes = f(seed, key, size, generation)`. A checkpoint
keeps the seed, size, generation and digest — not the object. Without this a months-long run would
need disk proportional to everything it ever wrote.

**The generation is load-bearing, not decoration.** An earlier revision derived from
`(seed, key, size)` alone, which regenerates *identical bytes* whenever a key is overwritten at
the same size — the common case in the contention and churn pools. Identical bytes give an
identical digest and an identical `ETag`, so a read that returned the **prior generation** would be
indistinguishable from a correct one, and §6 declares exactly that read fatal. The oracle would
have been structurally unable to detect the failure it names. Including the generation makes every
overwrite a distinguishable value, which is what makes the assertion checkable at all.

This matters beyond the oracle: rapid overwrites can also share a second-resolution wire timestamp
([#767][i767]), so content is the only reliable discriminator between generations.

### 5. Worker pools

Concurrency is structured so the oracle stays readable. The disjoint/contention split mirrors
the [checked consistency run](../../reviews/m4-checked-consistency-run.md), which kept its
register and delete pools on deliberately disjoint keys for the same reason.

| Pool | Keyspace | Exercises |
|---|---|---|
| **disjoint** | worker `n` owns `<run>/w{n}/` | parallel PUT/GET/HEAD/DELETE across size classes; single writer per key, so the model needs no locking |
| **contention** | one shared key, all workers | last-writer-wins, monotonic reads, overwrite under concurrency |
| **listing** | reads the disjoint pool's keys | pagination and delimiter rollups **while the keyspace churns** |
| **workflow** | `<run>/flow/` | `cp` / `sync` / `rm --recursive` / `ls` over synthetic directory trees |
| **multipart** | `<run>/mp/` | concurrent parts within an object *and* concurrent objects — gated on [#508][i508] |

**Every key is scoped by a run id.** This is a safety requirement, not an isolation nicety: the
tool ships to operators, deletes what it writes (§8), and must never delete anything it did not
create. A run id in the prefix is what makes "delete only mine" mechanical, and it also lets two
runs share a cluster. It is the primary defence; §15 adds the guards that sit behind it.

**Global LIST reconciliation cannot run concurrently with the mutating pools** — it would
produce failures that are artefacts of timing. It happens at declared quiesce points, on a
cadence that scales with duration. The consistency run reached the same conclusion: one composed
full-set read after quiesce, not a continuous sweep.

### 6. The oracle

Per key, the model holds `{size, sha256, etag, content-type, last-modified, generation}`.

**What the oracle may assert depends on whether the tool controls the write order**, and
conflating the two would make it report valid behaviour as failure. Two tiers:

**Single-writer keys** (the disjoint and workflow pools, §5 — one worker owns each key, and its
own writes are sequential). Here the tool knows the serialization order because it created it, so
the full set applies:

- bytes read back hash to the recorded digest — the whole object, and every `Range` slice
- `Content-Length`, `ETag`, `Content-Type` match what was written (ADR-0047)
- a deleted key `404`s, and stays `404`
- an overwritten key never serves a prior generation
- conditional requests answer per the recorded `ETag` / `Last-Modified`

**Concurrently-written keys** (the contention pool). The tool observes *completion* order at the
client, which is **not** commit order at the server. Two overlapping PUTs have no
client-observable serialization, so "last writer wins" and "never serves a prior generation" are
unassertable — asserting them would report correct linearizations as fatal `oracle` failures, and
`oracle` stops the run. The contention pool therefore asserts only what is sound without an
ordering model:

- every read returns bytes that hash to **some** value actually written to that key (never a
  torn, invented or foreign value — the atomicity property that matters)
- a read never returns a value written by a PUT that had not yet started when the read completed
  — causality, which needs no serialization order to check

**And nothing about ordering, including per-session monotonicity.** An earlier revision kept a
monotonic-reads assertion here on the reasoning that both reads belong to the same worker, so the
tool can order them. It can order the *reads*; it cannot order the *writes* they observe. Worker
W reads generation 5, then reads again; meanwhile worker Y's PUT of generation 3 — issued before
W's first read — commits between them. W's second read returns 3. That is a valid serialization
in which Y simply committed last, and the assertion would call it a fatal failure. Client-assigned
generation numbers do not order concurrent commits, so no rule built on them is sound while writes
are in flight.

Wyrd does contract monotonic reads (ADR-0015), and it is worth checking. But the tool can only
check it where it can establish order, which is **at quiesce**: writes stopped, in-flight requests
drained, and then the value is determinate. So the contention pool's ordering checks move there —
the final value must be one of the written values, and repeated reads must agree on it. That is
the same shape the consistency run uses when it takes "ONE composed full-set read after heal +
quiesce" rather than sampling continuously.

**Which means this tool does not cover monotonic reads *under concurrency*, and should not claim
to.** Quiesced checks confirm the value settles; they cannot exercise ADR-0015's guarantee while
commits are in flight, which is the only interesting case. That coverage belongs to
`cargo xtask consistency-run`, where a checker we did not write judges a recorded history — and
losing it here is the price of not inventing false failures. The contention pool's contribution is
narrower than an earlier revision implied: atomicity and causality under concurrency, plus a
determinate settle at quiesce.

Establishing a total order over genuinely concurrent writes needs a real linearizability checker,
which is what `cargo xtask consistency-run` already does with Elle. Duplicating it here in
hand-rolled form is how a validator invents its own false failures.

**At quiesce**, with mutation stopped: the full `ListObjectsV2` sweep equals the model's live set
exactly — no extra key, no missing key, correct `CommonPrefixes` rollups under `delimiter=/`.

**`Last-Modified` is real wall-clock but not directly comparable across surfaces.**
`now_millis()` reads `SystemTime::now()` (`crates/server/src/lib.rs:756`) and objects are stamped
at `:197` and `:348`. But the value the wire returns differs by surface and is not ordered across
a fleet:

- A listing renders `<LastModified>` through `iso8601`, keeping milliseconds; `GET`/`HEAD` render
  the header through `http_date`, which is IMF-fixdate and second-granular. The same object
  therefore reports two different times, up to 999 ms apart ([#767][i767]).
- Each gateway stamps from its own clock with no monotonicity guard at overwrite commit, so on
  the blueprinted N-gateway fleet `Last-Modified` can go **backwards** across an overwrite
  ([#768][i768]).

So the oracle verifies the timestamp **per surface** — a listing time against the listing format,
a header time against the header format — and never compares one to the other or assumes ordering.
`sync`'s comparison (§7 workflows) inherits the same limit and is discussed there.
(madsim virtualises the read under DST, so the stamp is deterministic in simulation and real
everywhere else.)

Verification is where the CPU goes. Every downloaded byte is hashed and every payload is
generated. That is the load the tool must sustain without becoming the bottleneck, and the
reason this is a compiled binary.

### 7. Failure taxonomy

A months-long run must not abort at hour three because one request timed out. It must abort
instantly when a byte comes back wrong.

| Class | Meaning | Disposition |
|---|---|---|
| `integrity` | bytes, digest, or size differ | **fatal** — stop, preserve state and the object |
| `oracle` | model and server disagree (deleted key readable, stale generation, listing mismatch) | **fatal** |
| `unexpected-support` | an `unsupported` operation succeeded | **fatal** |
| `protocol` | wrong status, error code, or XML shape | **fatal**, in every scenario |
| `availability` | transport failure, connection reset, 5xx | **budgeted** — counted, rate-limited, fails only above a declared threshold |

**`protocol` is fatal everywhere.** An earlier revision budgeted it in long scenarios, which is
incoherent: a wrong status or malformed XML is a *correctness* violation, not transient
unavailability. Under a budget, a rare malformed response during a week-long run could stay below
threshold and the run could still report `PASS` — while the verdict claims to cover correctness
and while `smoke` treats the identical response as fatal. Only transport-shaped failures are
budgeted. Rarity is not a defence for a wrong answer.

**Every operation is bounded, and unbounded waits are not an availability class at all.** A peer
that accepts a request or body stream and then stops producing data cannot be *classified* by the
budget, because no error is ever returned — the worker simply stays pending, and with it quiesce,
checkpointing, shutdown and the verdict. So the tool imposes explicit deadlines on connect, on the
operation, and on the response-body stream, and cancels outstanding worker tasks rather than
waiting for them. This is a standing rule in the target repo, not a preference of this proposal —
`AGENTS.md` requires that "every await on external work is bounded (timeout, fail-closed);
spawned helper tasks are aborted on drop; shutdown never joins a potentially infinite stream."
Classifying returned timeouts is what the taxonomy does; *producing* them is a precondition for
the taxonomy applying at all.

**A timed-out mutation leaves the model wrong, and must be resolved rather than budgeted.** If a
PUT, overwrite or delete times out *after* the server committed it, counting the timeout as
budgeted availability and continuing leaves the oracle holding the pre-request state. The next
read — or the next quiesce sweep — then reports a fatal `integrity` or `oracle` failure even
though the server performed exactly one valid operation. That is a false failure manufactured by
the tool's own bookkeeping.

So an indeterminate mutation triggers an **outcome-resolution step**: probe the key, compare
against both the pre- and post-request expectations, and record whichever the server actually
holds. Where the probe cannot settle it — the key is unreadable, or a concurrent writer makes the
observation ambiguous — the run goes **INCONCLUSIVE** rather than continuing against a state the
client no longer knows. Continuing on a guess is how a validator produces confident nonsense.

### 8. Scenarios

A scenario fixes the **shape** of a run: which pools are active and at what weight, how sizes
are distributed, whether the working set is bounded or growing, the quiesce cadence, and the
coverage floor. Scale is orthogonal — `--duration`, `--workers` and substrate apply to any of
them, and any scenario runs at any length, from minutes to months.

| Scenario | Shape | Catches what the others structurally cannot |
|---|---|---|
| `smoke` | every required op × every size class, once; every unsupported op asserted to fail correctly; auth-negative vectors on | nothing time-related — a correctness sweep with no time axis |
| `endurance` | all pools, moderate weights, realistic size mix, **bounded** working set | leaks, unbounded growth, listing degradation, reclamation falling behind over time |
| `large-object` | few objects, enormous bytes; sizes weighted to the top of the table | single-write paths: lease renewal during one long write, chunk-map segmentation, `EntityTooLarge` |
| `churn` | small objects, very high delete and overwrite rate, keys reused, working set flat | reclamation under pressure: tombstone accumulation, staged-byte reclaim, custodian repair load |
| `listing` | grows to millions of small keys, then lists them under churn | keyspace-scale effects: pagination, continuation tokens, delimiter rollups, [#674][i674] |

`endurance` is the Alpha gating scenario. Its **bounded working set** is what makes it a
stability test rather than a capacity test: each worker holds N live objects and deletes as it
creates, alongside a slowly growing archive set so long-lived data exists. Unbounded growth
would measure how fast you fill disks.

`large-object` exists because `endurance`'s realistic mix — mostly small, occasionally large —
would take weeks to accumulate the same coverage. `churn` is deliberately hostile to
reclamation in a way a stable set with gentle turnover never is. `listing` is the only long
scenario that grows unbounded on purpose, because every other one bounds its working set and so
never builds a keyspace large enough for pagination to matter.

**Cleanup.** Delete on success; **retain on failure**. The object whose bytes came back wrong is
the evidence. Only run-id-scoped keys are ever deleted (§5).

**The vacuity gate.** The Elle run refuses a verdict unless the history it judged was genuinely
concurrent — a run that proved nothing must not read as a pass. Same discipline: a run is
**INCONCLUSIVE** (non-zero exit, never PASS) unless it clears the scenario's declared floor —
every required operation exercised N times, every size class touched, a minimum byte volume, at
least one quiesce reconciliation, and wall-clock within tolerance of `--duration`. A run meant
to last seven days that lost its substrate at hour 40 is inconclusive: not a pass, and not a
failure of the system under test.

### 9. The blackbox property, as a lint

`crates/validate` is a workspace member — so `fmt`, `clippy -D warnings`, `cargo deny` and
`#![forbid(unsafe_code)]` all cover it — but the **normal** dependency closure of its binary
target must contain **no** `wyrd-*` crate.

Normal, not total: `cargo metadata` walks dev-dependencies too, and the §14 fixtures are
dev-only. Nothing that ships in the binary may reach a workspace crate; what the test harness
links is unconstrained.

`xtask/src/repo_guard.rs` gains this as a third invariant, alongside the stray-gitlink and
`forbid(unsafe_code)` checks. It uses the same discovery route as the existing scan — `cargo
metadata`, chosen there specifically "so no manifest override or unconventional layout can hide
a root" (`repo_guard.rs:26-31`).

This is what makes "blackbox" a property rather than a promise in a doc comment, and it is why
the tool must stay a separate binary rather than becoming a `wyrd` subcommand.

### 10. Driver placement, and what a number is worth

Recorded, not assumed. `--driver-placement internal|external`:

- **internal** — its own small node on the private vSwitch (blueprint §B.1's network split). No
  CPU contention with the roles under measurement, no billed egress on verification downloads,
  no WAN in the latency figures. Costs one server.
- **external** — from a laptop or CI runner over the public endpoint. Nothing to provision, and
  it is the path a real user takes — but every verification download is billed egress and every
  latency number carries WAN noise.

The verdict states which. A latency figure that travels without its placement will eventually be
quoted wrongly. This matters more here than usual because the tool reports performance without
judging it (see the summary's *settles by exclusion*): the reader does the judging, so the
reader needs the context.

Related, worth knowing before a Hetzner run: the S3 endpoint is the only publicly exposed
surface in §B.1, and TLS on it is blocked — binding a real listener needs a rustls crypto
provider outside the `deny.toml` allowlist, flagged NEEDS-HUMAN at
`crates/gateway-s3/src/lib.rs:50-57`. Until that is decided the endpoint is plain HTTP behind an
operator's own terminator.

### 11. Faults belong to the launcher — because of §9, not because the legs are stuck

An earlier revision of this proposal argued that fault injection had to live in the launcher
because the existing legs could not be driven independently of a workload. **That was wrong**,
and the correction matters because it changes what the launcher is for.

The legs **do** detach, and cleanly. `crates/metadata-fault-conformance/src/nemesis.rs:250`
declares `pub trait NemesisLeg`, whose `apply()` (`:267`) injects a fault and runs no workload at
all. `:305` provides

```rust
pub fn drive_leg<L, W, T>(leg: &L, workload: W) -> Result<T, String>
where L: NemesisLeg, W: FnOnce() -> T
```

— the workload is a caller-supplied closure, so fault and workload are decoupled by
construction. Legs take container names as constructor arguments rather than compose project
names, and the seam is already consumed outside the metadata crates:
`crates/server/tests/consistency_run_fdb.rs:118` imports `PartitionLeg` / `ClockSkewLeg` /
`ProcessPauseLeg` and drives an S3 workload under them.

So the real constraint is **§9, not reusability**. `wyrd-metadata-fault-conformance` is a
`wyrd-*` crate. A tool that linked it to drive its own faults would forfeit the blackbox property
that is this tool's entire premise — the lint would have to be scoped away, and once scoped away
it stops meaning anything.

The placement is therefore unchanged and the reasoning is replaced: **fault injection lives in
the launcher, which links the seam the tool may not.** `cargo xtask s3-blackbox --nemesis <leg>`
calls `drive_leg` with a workload closure that runs `wyrd-validate` as a subprocess — using the
real seam rather than reimplementing it, and keeping the tool's dependency closure clean. The
tool never knows a fault happened; it reports what it saw. Correlation happens in the sink, where
both timestamped streams land (§13).

On **hetzner** there is no launcher, so faults are manual. Blueprint §B.2 names the honest one:
`hcloud server delete d<n>` destroys a real machine, which is what a rack loss looks like and what
no single-host container cluster can imitate. That is a **Cloud** capability — Server Auction
bare-metal is monthly with a minimum term, so a machine cannot be destroyed and replaced as a test
action. Bare-metal remains faultable (`systemctl stop`, a firewall rule); the fault set is smaller,
not empty. Since long runs are driven by hand, manual faults fit.

### 12. Resumability

A months-long run cannot die with an SSH session.

- oracle state checkpoints to disk at every quiesce point
- `--resume` restores from checkpoint and continues to the original deadline
- ships as a systemd unit for long runs, mirroring the three role units in `deploy/dist/systemd/`

**Resuming from a checkpoint alone is not safe, and an earlier revision advertised it as if it
were.** Operations that completed after the last checkpoint are real on the server and absent from
the restored model. Continuing directly from that state reports failures against a **correct**
server: an upload since the checkpoint appears as an extra key at the next quiesce sweep, a delete
appears as a key that should be present and is not, and an overwrite leaves the model holding a
generation the server no longer has — a fatal `integrity` mismatch on the next read.

Two mechanisms, and the run needs both:

- **A mutation journal**, appended *before* each mutation is issued and marked on completion, so a
  restart knows which operations were in flight and which keys are therefore indeterminate. This
  is the same intent-then-act ordering the mutation-resolution rule in §7 relies on.
- **Reconciliation into a well-defined model at resume**, not blind continuation: enumerate the run
  prefix, resolve every key the journal marks indeterminate through the §7 outcome-resolution step,
  and only then start issuing new work. Keys that resolve are adopted with their observed state;
  keys that cannot be resolved are excluded from the oracle and reported, never guessed at.

That ordering also keeps §15's ownership invariant intact: the journal can only make the model a
*better* lower bound on what the run wrote — it never causes the tool to claim a key it did not
create.

**Checkpoints are bounded by format, not by hoping the workload stays small.** §4's derived
payloads keep object *bodies* off disk, but the model still holds metadata for every live key —
and the `listing` scenario deliberately grows to millions of them. A whole-file rewrite at every
quiesce would reach hundreds of megabytes, rewritten repeatedly, distorting the very workload
being measured. So the on-disk form is an **incremental ledger**: append mutations, compact on a
declared schedule, and bound the resident model by the scenario's own working-set budget. A
scenario whose keyspace is unbounded by design (§8, `listing`) declares how its ledger is bounded
as part of its shape, rather than inheriting a promise the format cannot keep.

### 13. Two outputs, one source of truth

**The stream** is primary: structured events and metrics emitted as the run proceeds, to
whatever sink the environment provides. Watch it, alert on it, and correlate it with the roles'
own telemetry — and with the launcher's fault window (§11) — wherever both land. This follows
the existing convention: `ExporterConfig::{Prometheus, Both}` is how `cmd_s3` and `cmd_custodian`
already choose their export surface (`cli.rs:2165-2170`), under ADR-0012.

**The verdict artifact** is the durable summary, because a release gate cannot hang on a log
stream that scrolled past. One small file, **rewritten at every quiesce point** so a run killed
at week six still leaves a current verdict rather than nothing:

- **identity** — tool version, `aws-sdk-s3` version, server version, substrate, driver
  placement, seed, run id
- **deployment tuning** — chunk size, root-value budget, segmentation threshold (§4). A size
  coverage claim is unreadable without them: "crossed the segmentation boundary" means nothing
  unless the verdict says where that boundary was.
- **what ran** — scenario, requested and actual duration, workers, op counts, bytes moved,
  size-class coverage
- **performance** — latency percentiles and throughput, **reported, not judged**
- **failures** — every one, with class, key, expected, observed
- **coverage** — the §8 vacuity floor, item by item
- **verdict** — `PASS` | `FAIL` | `INCONCLUSIVE`, on correctness only, keyed on the coverage
  floor and the failure classes, never on the exit code alone
- **what this does not license** — prose, per run shape. A loopback PASS licenses no distributed
  claim. A fleet PASS licenses no failure-domain claim. A no-fault PASS licenses no recovery
  claim. And no PASS licenses a performance claim at all.

Both outputs are derived from the same events, so nothing is counted twice. **Output paths are
configurable** (`--out DIR`, sink configuration) — the environments this runs in differ, and the
tool must not assume a repo layout. Where a *gating* run's artifact gets committed is a
convention for that run, not a property of the tool.

### 14. Proving the tool works

A validator nobody has shown can fail is worth nothing. If the byte comparison silently
regressed to "these match", every run would pass forever and a passing run would be
indistinguishable from a real one — while gating a release. So the tool proves itself against a
**second S3 implementation**, in both directions.

**The positive half — is our reading of S3 correct?** A **MinIO** container is the reference.
Running `smoke` against it checks that the expectations — status codes, XML shapes, ETag rules,
`sync`'s size-and-mtime comparison — match an independent implementation rather than our own
reading of the spec. It also runs in CI with no Wyrd present at all, which catches regressions
in the tool itself quickly.

Against MinIO the operations Wyrd marks `unsupported` actually **work**, so the matrix carries a
**reference profile** where those rows are expected to succeed. That inversion is a proof rather
than a nuisance: a row that passes against MinIO and fails against Wyrd genuinely describes
*Wyrd's* gap, not our misunderstanding of S3.

Two size classes collapse in this profile — the chunk-size and segmentation boundaries (§4) are
Wyrd internals and mean nothing to another implementation, so they degrade to ordinary
large-object cases.

*Honest limit:* MinIO is not canonical S3. Where MinIO and AWS disagree, AWS is right. A
protocol misreading we happen to share with MinIO would pass unnoticed. That is a smaller gap
than having no reference at all, and it is free; a periodic run against real AWS would close it
and is deliberately out of scope here.

**The negative half — does our detection work?** A small **corrupting proxy** sits between the
tool and MinIO and misbehaves on demand: flip one byte in a returned object, report a wrong
`Content-Length` or `ETag`, still serve a deleted key, succeed at an operation the matrix marks
`unsupported`, truncate a `Range`. Each fixture is fed through the **same oracle** that judges
live runs, and each **must** come back as the matching failure class (§7). A fixture that comes
back clean means the checker is broken.

This is the consistency run's discipline applied to a different checker: it feeds known-good,
known-bad and degraded histories through the same jar that judged the real one and requires
`true`, `false` and `:unknown` respectively — "a checker build that blessed the known-bad fixture
would fail the run before its verdict was trusted."

**One fixture needs the Wyrd profile, not the reference profile.** A third review round caught
that `unexpected-support` cannot be tested against a plain MinIO backend: MinIO genuinely
*implements* the operations Wyrd marks unsupported, and §14's reference profile expects exactly
those successes — so a real success proves nothing about detecting an unexpected one. That fixture
therefore runs MinIO under the **Wyrd** profile, where `CopyObject` succeeding is a declared
failure. The proxy is not needed for it; the profile mismatch is the fixture.

**It gates the build, not each run.** An earlier revision said the fixture set "executes before
every live run, including the Alpha gating run". That is incompatible with keeping the fixtures
dev-only: a hand-run Hetzner execution has no launcher, and an operator's installed binary has
neither MinIO nor the proxy. Requiring a preflight nobody can perform would make the requirement
decorative.

So the self-check is a **release gate on the tool itself**, run in CI: no `wyrd-validate` build
ships without its fixture set passing. Each live run then records the tool build it used, and
inherits the guarantee from that build rather than re-proving it in the field. A build whose
detection has stopped working never reaches a substrate — which is the property that was wanted,
placed where it can actually hold.

Both MinIO and the proxy are **test fixtures**, not dependencies: containers and a dev-only
binary, never linked into `wyrd-validate`. §9's lint is scoped accordingly.

**A licensing note.** MinIO's server is, to the best of the author's knowledge, AGPL-licensed,
and `deny.toml` denies AGPL/BSL/SSPL under ADR-0003 §2. That wall governs **dependencies linked
into a binary**; a separate server process run as a test fixture is not one, exactly as the
existing container fixtures are not. Confirm before adoption anyway — "any new dependency or
license" is a declared NEEDS-HUMAN item (INTEGRATION.md §4), and this is the kind of distinction
worth recording rather than assuming.

### 15. Safety — it deletes things, on someone else's cluster

The tool writes objects and deletes them (§8), and it ships to operators who will point it at
their own storage. The failure that matters is silent and permanent: someone runs it against a
bucket holding real data. Deleted storage does not come back.

Run-id-scoped keys (§5) are the primary defence and the right design — but they are a *single*
line, effective only while the code is correct. Three guards sit behind them:

**Refuse an occupied namespace.** Scoped to the **run prefix**, not to the bucket — which is what
makes it both sound and cheap. `<run-id>/` is a namespace the tool is about to create, so one
`ListObjectsV2` with `prefix=<run-id>/` and `max-keys=1` returning anything at all is conclusive:
something is already there, and the run exits without writing a byte.

An earlier revision listed one page of the whole configured prefix and read an absence of foreign
keys as proof the target was clean. That is unsound and was removed: one page establishes nothing
about later pages, and on `--resume` the run's own keys can fill page one while foreign content
sits past the boundary. A guard that looks like a proof and is not is worse than no guard, because
it gets trusted.

Scoping to the run prefix proves the property that actually matters — *nothing of mine is already
here, so nothing of anyone else's can be mistaken for mine* — in one request. The tool makes **no
claim** about the rest of the bucket, and says so rather than implying safety it never
established. `--allow-non-empty` overrides, for a deliberate `--resume` onto an existing run id.

**`--dry-run`.** Enumerate every key the run would create and every key it would delete, print
them, touch nothing, exit. An operator gets to look before committing, and it doubles as the
cheapest possible smoke test of a new deployment's credentials and endpoint.

**Delete-time ownership verification — membership, not name shape.** Before issuing any delete,
require that the key is **present in this run's model as something this run wrote**. Checking only
that the key carries the run id is not an ownership proof, and an earlier revision made exactly
that mistake: if `--allow-non-empty` admitted pre-existing objects under `<run-id>/`, those objects
also carry the run id, so a prefix-shape check passes and `rm --recursive` or cleanup deletes data
the tool never wrote. The name proves where a key lives; only the model proves who put it there.

That also narrows `--allow-non-empty` to what it should always have meant. It permits the run to
**start** over an occupied namespace; it never adopts what it found. Pre-existing keys are absent
from the model, so they are never reported as oracle failures and never counted as coverage.

**But "absent from the model" is not enough on its own**, and a third review round caught why:
nothing stopped a *generated* key from colliding with a pre-existing one. The tool would overwrite
it, that key would then legitimately enter the model, and cleanup would delete it — destroying
data the run did not create, through the guard rather than around it. So under
`--allow-non-empty`, **cleanup is disabled for the whole run** (`--cleanup never` is forced, not
merely defaulted). A run that starts over occupied space leaves everything it wrote behind, and
says so in the verdict. Deleting nothing is a recoverable inconvenience; deleting someone else's
object is not.

**Ownership may under-approximate. It must never over-approximate.** A checkpoint is written at
quiesce, so a crash leaves writes that succeeded after the last one absent from the restored
model. Those orphans are indistinguishable from foreign keys, and the tool therefore treats them
as foreign: it does not delete them, and it reports them as an orphan set for a human to clear.
That direction leaks storage, which is visible and fixable. The opposite direction — inferring
ownership to reclaim them — would delete data on a guess. The invariant is stated deliberately
because the two failure modes are not symmetric.

On `--resume` the model is restored from the checkpoint, so membership up to that point is known
exactly and the previous run's own keys are legitimately owned.

Belt and braces against the write path and the delete path disagreeing — the one bug class where
run-id scoping fails exactly when it matters.

Together with retain-on-failure (§8), the invariant is: *the tool never removes a byte it did
not write, and never writes into occupied space it was not explicitly pointed at.*

What none of this protects against is an operator who passes `--allow-non-empty` at a bucket of
production data. That is their call to make, and the flag's name is chosen so it cannot be made
by accident.

## Dependencies

Three product changes the tool's coverage wants. None blocks a first slice — defaults mirror
today's values and the verdict records what was used — but each makes it accurate against a real
deployment rather than against the tree.

**The gateway advertises no version.** No `Server:` header, nothing on the wire
(`crates/gateway-s3/src/lib.rs`). A shipped tool cannot discover what it is validating, so the
capability matrix cannot key its expectations to a release. `Server: wyrd/<version>`, sourced
from the same `normalize_describe` over `git describe` that `cargo xtask dist` already uses
(`xtask/src/dist.rs:119,356`) rather than the `version = "0.0.0"` placeholder. Interim:
`--server-version`. Tracked as [#736][i736] (0.1 Alpha — a gateway change, not tooling).

**The `s3` role does not expose `--chunk-size`.** The seam exists —
`Gateway::with_chunk_size` (`crates/server/src/lib.rs:145`, doc-commented "mainly so tests can
force multi-chunk objects") — and `wyrd put` already takes the flag (`cli.rs:555`). `cmd_s3`
never wires one, so every deployed gateway chunks at the 1 MiB default. Tracked as [#738][i738].

**The chunk-map budget constants are compile-time.** `MAX_ROOT_VALUE_BYTES` (50,000) and
`MAX_ROOT_SEGMENTS` (512), `crates/core/src/metadata.rs`. Tracked as [#739][i739].

An earlier revision of this section called the blast radius small and cited four supporting
facts. **Two of the four were wrong**, and the corrected scope is materially larger:

- `:688` is **not** an enforcement site. It is the `Display` arm for
  `ChunkMapError::TooManySegments`, interpolating `{MAX_ROOT_SEGMENTS}` by inline format capture —
  so with a runtime value it *fails to compile*. The fix is a `ceiling` field on a **public** error
  variant, the shape `SegmentValueOverCeiling { ceiling }` already uses. The only true enforcement
  site is `:2450`.
- "Already patch-aware" is **backwards**. `crates/core/tests/segmented_map_record.rs` text-greps
  the source for `pub const <name>: … = <integer literal>;` and **panics** when that declaration
  stops being a literal — which is exactly what making it configurable does, taking the whole
  Criterion-3 suite with it.
- The load-bearing invariant — proposal 0016's `max_segref_bytes × MAX_ROOT_SEGMENTS ≤ V/2` —
  exists **only as a measurement**: the test encodes worst-case tables and asserts on
  `encode(...).len()`. No production function computes that bound, so nothing can be "moved" to
  startup validation; a closed-form must be newly derived. Concretely, an operator setting
  `MAX_ROOT_SEGMENTS = 4096` passes the `:354` assertion and still produces roots around 400 KB,
  four times `MAX_VALUE_BYTES` — objects that can never be re-written, and therefore never
  repaired.
- Enforcement at `:2450` sits inside private `read_group_range`, reached through
  `metadata::resolve_chunk_map` from six call sites across three crates. Threading configuration
  there is a signature change with a wide blast radius; a process-global instead breaks tests that
  exercise both sides of the ceiling in one binary.

What **did** survive checking: the custodian is genuinely insulated (its checks go through
`metadata::flat_value_ceiling_crossed` against `MAX_VALUE_BYTES`, which is not changing), multipart
derives nothing from the knob (every reference in `multipart.rs` is a doc comment), and
`MAX_ROOT_VALUE_BYTES` has no production consumer beyond its own assertion.

**And one design question this raises, which is not the tool's to answer.** If the ceiling is
per-gateway configuration, a root published under a raised value becomes permanently unresolvable
the moment any gateway runs a lower one — decode is deliberately liberal, so the failure surfaces
at resolve time on a durable object. Multipart already solved the same problem by **storing** its
budget profile in the `mpuctl` record rather than deriving it per gateway, precisely so "a rolling
configuration change cannot leave two gateways enforcing different bounds"
(`crates/core/src/multipart.rs:1139-1144`). Whether a segmented root must likewise carry its own
ceiling is an architecture decision, and #739 should not be built until it is settled.

**`aws-sdk-s3` moves from a dev-dependency into a shipped artifact.** It is currently dev-only
(`crates/server/Cargo.toml:120-129`, noted there as "not compiled into the production binary").
Making it a *normal* dependency of a shipped workspace member brings roughly a hundred crates
inside `deny.toml`'s frame — whose own header states it "guards the default feature graph — the
artifact we ship". INTEGRATION.md §4 names "any new dependency or license" a human-only
NEEDS-HUMAN item, so this needs the ADR-0003 three-test audit before the crate lands, not after.
It is a larger item than §14's MinIO note and lands earlier.

**The tool cannot speak HTTPS, and the endpoint it is meant to validate is TLS-terminated.**
§10 and blueprint §B.1 make the S3 port the only public exposure, behind an operator's own
terminator. Nothing in this proposal adds a TLS client, and adding one meets the same wall the
gateway already sits behind: a rustls crypto provider outside the `deny.toml` allowlist
(`crates/gateway-s3/src/lib.rs:50-57`), a declared NEEDS-HUMAN dependency decision. Until that is
resolved the tool validates the plain-HTTP configuration — which is a real limitation of the Alpha
evidence artifact, not a detail: a 7-day run proving correctness over plain HTTP with static
credentials has not exercised the transport an operator is told to deploy. Resolve the provider
decision, or state in the verdict that transport is out of scope for what the run licenses.

**`MAX_VALUE_BYTES` (100,000) is explicitly not in scope.** It is FoundationDB's hard limit,
inherited by every backend so the tightest governs, and decimal rather than `100 * 1024` for
that reason. A knob there only lets a deployment mint writes the backend rejects at commit. If
it should change, the change is to make it backend-derived — each `MetadataStore` declaring its
ceiling, the seam taking the minimum — which is different work.

## Alternatives considered

**Python with boto3 and aws-cli.** The truest blackbox — the exact clients users run — and what
[#512][i512] names. Rejected as the foundation on three grounds: verification is CPU-bound
(every downloaded byte hashed, every payload generated), so a long run's load generator would be
fighting the GIL to keep the cluster busy; a months-long run wants a single binary to copy onto
a node, not an interpreter and a venv; and it would introduce a Python toolchain and CI lane to
a workspace with two Python files, both in the docs pipeline. Not rejected as a complement.

**Shelling out to `aws-cli` for the workflow layer.** Dropped in favour of one clean Rust client
at both levels. The cost, stated honestly: the workflow semantics have no specification outside
aws-cli's source. `mv` is copy-then-delete, `rm --recursive` is paginated list plus
`DeleteObjects` batched at 1000, `sync` compares size and mtime. Writing our own means we can be
correct against *our reading* of `sync` and still diverge from a user's actual aws-cli.
Mitigation: each workflow's algorithm carries a comment pinning it to observed aws-cli
behaviour, and the layer is described as "our reading of aws-cli", never as "aws-cli
compatible". An optional differential leg remains available later.

**A `wyrd` subcommand instead of a second binary.** Rejected. It would share a dependency
closure with the roles and make §9's lint meaningless — the blackbox property is the whole point,
and a tarball can carry two binaries.

**A performance pass bar.** Rejected. The right threshold depends on hardware the tool cannot
know, and a wrong bar is worse than none: it either fails good deployments or blesses bad ones.
The tool reports; the operator compares.

**Folding this into [#512][i512].** Partly rejected, and an earlier revision overstated the case.

The claim was "different things — #512 is one pass with stock tooling, this is endurance." That
holds for `endurance`, `churn`, `listing` and `large-object`, which have no counterpart in #512.
It does **not** hold for `smoke`. #512's own scope reads: "exercise the Alpha operation set end to
end with aws-cli and boto3 (put/head/get/list/copy/delete, multipart, range, bulk-delete),
asserting **byte-identical round-trips and correct S3 XML/error responses**", it folds in
[#491][i491]'s bypass vectors, and it says "**Define the pass bar as the Alpha S3 gate**". That is
the same work as this proposal's matrix, `smoke`, and auth-negative vectors, with a different
client. Two issues currently both claim to be the Alpha S3 gate.

The honest boundary, proposed here for #512's owner to accept or reject:

- **#512 owns client diversity.** Does *stock aws-cli and boto3* work — the tooling a user
  actually runs. That is the question this proposal explicitly declined to answer when it chose
  one Rust client, and nothing here replaces it.
- **This proposal owns the time axis and the operator-facing tool.** Endurance, churn, listing at
  scale, resumability, the shipped binary. None of that is in #512.
- **The overlap is the correctness matrix itself**, and it should be single-sourced rather than
  written twice. The declared operation/status/error-code table is the artifact both need; #512's
  client matrix and this tool's `smoke` should assert the *same* table through different clients.

Until that is settled with #512's owner, this proposal duplicates #512's correctness pass. Saying
so is better than the earlier claim that they were simply different.

**Extending `cargo xtask consistency-run`.** Rejected. That run exists so a checker *we did not
write* judges a history under a fault. Bolting endurance onto it would dilute a credibility
artifact whose value is its narrowness.

**Making a long run a throughput benchmark.** Rejected. `cargo xtask bench` exists and is
deliberately outside CI because runner wall-clock is noisy (`xtask/src/main.rs:159-163`), and
§B.3 is explicit that trustworthy numbers need bare-metal while fault campaigns want
hourly-billed cloud.

## Graduation criteria

1. `cargo xtask s3-blackbox --scenario smoke` passes against loopback and against
   `deploy/small-multi-node-fdb`, wired as an advisory Tier-2 job — **with the [#491][i491]
   vector recorded as a known-open defect rather than a failure**, since asserting the correct
   behaviour is red until #491 closes. "Passes" means: no failure outside the declared
   known-open set, and that set is named in the verdict. When #491 closes, the entry is removed
   and the criterion tightens automatically.
2. The matrix asserts every `unsupported` row's declared status and error code — including
   [#504][i504]'s `CopyObject` rejection, a standing regression guard against the
   silent-overwrite class — and the auth-negative vectors under `smoke`.
3. **The §14 self-check passes**: `smoke` is clean against MinIO under the reference profile,
   and every corrupting-proxy fixture comes back as its matching failure class. Wired as a
   preflight that must pass before any live run, including the gating one.
4. A 24-hour `endurance` run completes against the fleet. **Its availability-failure budget is
   set by that run, not before it** — the first clean run establishes the threshold (Open
   question 1), so this criterion reads "completes, and the observed failure rate becomes the
   declared budget", not "completes below a budget nobody has computed". Only the second such run
   can be gated on a number.
5. A **7-day `endurance` run completes against the Hetzner substrate**, verdict committed. This
   is the Alpha evidence artifact.

   **Where "0.1 Alpha does not tag without it" is enforced.** As prose it is unfalsifiable:
   tagging runs `.github/workflows/release.yml` on a `v*` push, and nothing in `cargo xtask ci`,
   `xtask dist` or any checklist reads a verdict artifact. This proposal does not add a machine
   gate — a release-blocking check on a hand-run week-long job is the wrong shape. It names a
   **release-runbook step** instead: the tag procedure requires the committed verdict, its
   substrate recorded as `hetzner`, and its scenario as `endurance`. That is a human step, and
   calling it one is more honest than implying automation that does not exist.

   **What this run does and does not license** (§13's prose, applied here): endurance of the
   build it ran, over plain HTTP, on the substrate named. It licenses no claim about TLS
   transport (*Dependencies*), no failure-domain claim unless faults were injected, and no
   performance claim at all.
6. `wyrd-validate` ships in the operator tarball, and `xtask/tests/dist_templates.rs` pins the
   two-binary layout.
7. Every failure found is filed; deterministic ones land seeded DST regressions (ADR-0009).
8. §9's dependency lint is green in `cargo xtask ci`.
9. The pure modules (matrix, scenario, oracle, payload, verdict) carry flippable unit tests —
   the `consistency_run` split, where the decision-shaped core is Check-tested and the runner is
   thin I/O.

## Backward compatibility

No on-disk format change, no consistency-contract change, no public API change. The tool is
additive and reads only the S3 wire. The tarball gains a second binary — an addition to its
layout, not a change to the existing one.

Two forward couplings, both deliberate:

- The **capability matrix** must be updated when [#504][i504], [#508][i508] or [#511][i511]
  land. That is the mechanism working: the matrix failing loudly on a newly-supported operation
  is the signal it exists to give.
- The **size table** (§4) takes the deployment's tuning as input rather than importing it, so it
  cannot go stale — but it is only as accurate as what it is passed. The verdict records the
  values used, which is what makes a wrong one visible after the fact rather than silent.

## Open questions

1. **What is the availability-failure budget?** §7 needs a number, and it should differ by
   substrate — a shared-vCPU fleet is not bare metal. Derive it from a first clean 24-hour run
   rather than guessing now.
2. **Quiesce cadence per duration.** Hourly at 24h, six-hourly at a week, daily beyond is a
   starting proposal, not a measured one.
3. **The segmentation-threshold default.** Derivable in principle from chunk size and root
   budget, but the per-chunk encoded width makes it approximate. Needs one measurement.
4. **Can the container fleet sustain a long run?** Disk growth across nine D-server volumes plus
   FDB over days is unmeasured. It no longer blocks the design — the Alpha gate is the Hetzner
   run — but it decides whether the fleet is useful past 24 hours.
5. **Where does a gating verdict get committed?** `docs/design/reviews/` alongside the
   consistency run, or `docs/design/runbooks/drills/` where [#485][i485] puts its drill record.
   A convention for the gating run, not a property of the tool.
6. **The [#512][i512] boundary needs its owner's agreement.** *Alternatives considered* proposes
   single-sourcing the correctness matrix and splitting client-diversity from the time axis.
   Until that is accepted, two issues both claim to be the Alpha S3 gate and the correctness pass
   is written twice.
7. **Must a segmented root carry its own ceiling?** Raised by *Dependencies*: a per-gateway
   `MAX_ROOT_SEGMENTS` makes a root published under a high value unresolvable to a gateway running
   a lower one, on a durable object. Multipart stores its budget in the record rather than deriving
   it per gateway, for exactly this reason. An architecture decision, and [#739][i739] should not be
   built before it is settled.
8. **The rustls provider decision gates whether the tool can ever validate a real endpoint.**
   Until it is made (*Dependencies*), every run — including the Alpha gating run — exercises plain
   HTTP, which is not the transport operators are told to deploy.
9. **Does the production OCI image carry the validation tool?** §2: dist extracts the tarball
   binary from the image, so shipping a second binary in the tarball but not the image breaks the
   bit-identical guarantee and needs separate assembly. A release-pipeline decision.

[i364]: https://github.com/getwyrd/wyrd/issues/364
[i485]: https://github.com/getwyrd/wyrd/issues/485
[i491]: https://github.com/getwyrd/wyrd/issues/491
[i504]: https://github.com/getwyrd/wyrd/issues/504
[i506]: https://github.com/getwyrd/wyrd/issues/506
[i507]: https://github.com/getwyrd/wyrd/issues/507
[i508]: https://github.com/getwyrd/wyrd/issues/508
[i509]: https://github.com/getwyrd/wyrd/issues/509
[i510]: https://github.com/getwyrd/wyrd/issues/510
[i511]: https://github.com/getwyrd/wyrd/issues/511
[i512]: https://github.com/getwyrd/wyrd/issues/512
[i560]: https://github.com/getwyrd/wyrd/issues/560
[i570]: https://github.com/getwyrd/wyrd/issues/570
[i625]: https://github.com/getwyrd/wyrd/issues/625
[i668]: https://github.com/getwyrd/wyrd/issues/668
[i669]: https://github.com/getwyrd/wyrd/issues/669
[i670]: https://github.com/getwyrd/wyrd/issues/670
[i671]: https://github.com/getwyrd/wyrd/issues/671
[i635]: https://github.com/getwyrd/wyrd/issues/635
[i674]: https://github.com/getwyrd/wyrd/issues/674
[i766]: https://github.com/getwyrd/wyrd/issues/766
[i767]: https://github.com/getwyrd/wyrd/issues/767
[i768]: https://github.com/getwyrd/wyrd/issues/768
[i736]: https://github.com/getwyrd/wyrd/issues/736
[i737]: https://github.com/getwyrd/wyrd/issues/737
[i738]: https://github.com/getwyrd/wyrd/issues/738
[i739]: https://github.com/getwyrd/wyrd/issues/739
