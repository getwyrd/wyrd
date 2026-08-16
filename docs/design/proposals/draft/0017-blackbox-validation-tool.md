---
created: 16.08.2026 10:42
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#735"
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
closure and destroy the property §9 exists to enforce. `install.sh`, the dist staging, and the
pinned layout contract in `xtask/tests/dist_templates.rs` all grow a second entry.

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
| `CopyObject` | `unsupported(#504)` | rejected at `lib.rs:1730` |
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
| `mv` | `CopyObject` + `DeleteObject` | **blocked on [#504][i504]** |
| `rb` | `DeleteBucket` | **blocked on [#511][i511]** |

Two operations the Alpha bar names in everyday terms map onto gaps, and the matrix is where
that becomes visible rather than surprising. **"Rename" is not an S3 operation** — it is
copy-then-delete, so `mv` cannot work until [#504][i504]'s implementation half lands.
**"Directory removal" is not one either** — it is list-then-bulk-delete, and both halves are
built, so `rm --recursive` works today.

**Negative authentication vectors** — bad signature, wrong region scope, expired timestamp,
missing `x-amz-content-sha256`, and [#491][i491]'s percent-encoded subresource bypass. These
assert the gateway is fail-closed, which is exactly what an operator wants to know about their
own deployment. **Off by default** so long runs do not spend operations on them; the `smoke`
scenario declares them **on** as part of its shape, so the correctness sweep always includes
them.

**The matrix must be keyed to the server version.** Expectations change as slices land, so a
tool validating an older release would otherwise fail on rows naming issues fixed after that
build. The gateway advertises no version today — no `Server:` header, nothing on the wire — so
this needs the version header of [#736][i736] (*Dependencies*). Until it lands, `--server-version` is an
operator-supplied parameter, with the honest weakness that a parameter can be silently wrong.

The matrix is a reviewed constant with citations. When [#508][i508] lands, the change is
flipping six rows; the tool needs no other edit. That property is the point.

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
| segmentation threshold ± | **chunk-map segmentation boundary** | `--segment-threshold-bytes` |
| 5 GiB | S3 single-PUT maximum → `EntityTooLarge` | [#671][i671] |
| > 5 GiB | multipart-only territory | gated on [#508][i508] |

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

Payloads are **derived, never stored**: `bytes = f(seed, key, size)`. A checkpoint keeps the
seed, size and digest, not the object. Without this a months-long run would need disk
proportional to everything it ever wrote.

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
Checked on every read:

- bytes read back hash to the recorded digest — the whole object, and every `Range` slice
- `Content-Length`, `ETag`, `Content-Type` match what was written (ADR-0047)
- a deleted key `404`s, and stays `404`
- an overwritten key never serves a prior generation
- conditional requests answer per the recorded `ETag` / `Last-Modified`
- at quiesce: the full `ListObjectsV2` sweep equals the model's live set exactly — no extra key,
  no missing key, correct `CommonPrefixes` rollups under `delimiter=/`

`Last-Modified` is real wall-clock and safe to verify: `now_millis()` reads `SystemTime::now()`
(`crates/server/src/lib.rs:756`), documented as "the access layer's single wall-clock source —
lease stamps and ADR-0047 publication timestamps all read THIS fn", and objects are stamped at
`:197` and `:348`. That is also what makes `sync`'s size-and-mtime comparison implementable.
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
| `protocol` | wrong status, error code, or XML shape | **fatal** in `smoke`; budgeted in long scenarios |
| `availability` | timeout, connection reset, 5xx | **budgeted** — counted, rate-limited, fails only above a declared threshold |

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

### 11. Faults belong to the launcher, not the tool

Investigated rather than assumed, and the answer changed the design.

The existing fault legs **do not detach**. `xtask/src/nemesis.rs` is clean — 151 lines of pure
routing with no topology — and there is a real seam, `ClusterFault`
(`crates/metadata-fault-conformance/src/lib.rs:87`), with `apply`, `heal` and peer-side
materialization evidence, built so "one battery judges two backends". The iptables agent image
has already been reused three times (`fdb_faults.rs:47`, `consistency_run_runner.rs:42`, the
Jepsen leg), so the technique travels.

But every leg is packaged as `cargo test -p <crate> --test <binary> -- --ignored --exact <fn>`,
with the fault injection and the workload living **together inside that test function**. There
is no "inject a fault, then run any workload" entry point. And `ClusterFault` lives in a
`wyrd-*` crate, so the tool cannot link it without breaking §9.

So **fault injection stays in the launcher**. `cargo xtask s3-blackbox --nemesis <leg>` injects
against the compose stack while `wyrd-validate` runs the workload as a separate process that
never knows a fault happened — it reports what it saw. Correlation happens in the sink: both
streams are timestamped and land in the same place, so the fault window overlays the workload's
error rate there (§13). That is cleaner than teaching the tool about faults, and it keeps §9
intact.

On **hetzner** there is no launcher, so faults are manual. Blueprint §B.2 already names the
honest one: `hcloud server delete d<n>` destroys a real machine, which is what a rack loss
looks like and what no single-host container cluster can imitate. That is a **Cloud** capability
— Server Auction bare-metal is monthly with a minimum term, so a machine cannot be destroyed and
replaced as a test action. Bare-metal is still faultable (`systemctl stop`, a firewall rule);
the fault set is smaller, not empty. Since long soaks are run by hand, manual faults fit.

### 12. Resumability

A months-long run cannot die with an SSH session.

- oracle state checkpoints to disk at every quiesce point
- `--resume` restores from checkpoint and continues to the original deadline
- ships as a systemd unit for long runs, mirroring the three role units in `deploy/dist/systemd/`
- checkpoints are small by construction (§4: seeds, not payloads)

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

**It runs as a preflight, not a suite.** The fixture set executes before every live run,
including the Alpha gating run. A build whose detection has stopped working fails *before* its
verdict is trusted, not after.

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

**Refuse a non-empty target.** At startup, list one page under the configured bucket and
prefix. Anything present that does not carry this run's id means the tool exits without writing
a byte, naming what it found. `--allow-non-empty` overrides, deliberately and by name. Bounded
to one page so the check costs the same against a bucket with a thousand keys or a billion.

**`--dry-run`.** Enumerate every key the run would create and every key it would delete, print
them, touch nothing, exit. An operator gets to look before committing, and it doubles as the
cheapest possible smoke test of a new deployment's credentials and endpoint.

**Delete-time key verification.** Before issuing any delete, re-check that the key carries this
run's id. Belt and braces against the write path and the delete path disagreeing — the one bug
class where run-id scoping would fail exactly when it matters.

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
`MAX_ROOT_SEGMENTS` (512), `crates/core/src/metadata.rs`. **The blast radius is small, and was
measured rather than assumed**: `MAX_ROOT_SEGMENTS` is enforced in exactly two code sites, both
in that file (`:688` resolve ceiling, `:2450` accounting); `MAX_ROOT_VALUE_BYTES` appears in no
production code outside its own definition. The custodian is insulated — its ceiling checks go
through `metadata::flat_value_ceiling_crossed` against `MAX_VALUE_BYTES`, which is not changing.
And `crates/core/tests/segmented_map_record.rs` is already patch-aware, reading the constants
from source via `production_constant(...)` and measuring "whatever `MAX_ROOT_SEGMENTS` and
`MAX_VALUE_BYTES`" are (`:364`). What moves: the `const` assertion at `:354` becomes fail-closed
startup validation. It must fail closed — the reserve is a durability property, since "a root
that cannot be re-written is an object whose placement can never be repaired". Tracked as
[#739][i739].

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

**Folding this into [#512][i512].** Rejected — different things. #512 is a client-compatibility
matrix: does stock tooling work, one pass. This is protocol and endurance: does it stay correct
for a month. Different clients, different durations, different pass bars. They can share a
substrate.

**Extending `cargo xtask consistency-run`.** Rejected. That run exists so a checker *we did not
write* judges a history under a fault. Bolting endurance onto it would dilute a credibility
artifact whose value is its narrowness.

**Making a long run a throughput benchmark.** Rejected. `cargo xtask bench` exists and is
deliberately outside CI because runner wall-clock is noisy (`xtask/src/main.rs:159-163`), and
§B.3 is explicit that trustworthy numbers need bare-metal while fault campaigns want
hourly-billed cloud.

## Graduation criteria

1. `cargo xtask s3-blackbox --scenario smoke` passes against loopback and against
   `deploy/small-multi-node-fdb`, wired as an advisory Tier-2 job.
2. The matrix asserts every `unsupported` row's declared status and error code — including
   [#504][i504]'s `CopyObject` rejection, a standing regression guard against the
   silent-overwrite class — and the auth-negative vectors under `smoke`.
3. **The §14 self-check passes**: `smoke` is clean against MinIO under the reference profile,
   and every corrupting-proxy fixture comes back as its matching failure class. Wired as a
   preflight that must pass before any live run, including the gating one.
4. A 24-hour `endurance` run completes against the fleet with a clean verdict.
5. A **7-day `endurance` run completes against the Hetzner substrate**, verdict committed. This
   is the Alpha evidence artifact, and **0.1 Alpha does not tag without it**.
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
[i674]: https://github.com/getwyrd/wyrd/issues/674
[i736]: https://github.com/getwyrd/wyrd/issues/736
[i737]: https://github.com/getwyrd/wyrd/issues/737
[i738]: https://github.com/getwyrd/wyrd/issues/738
[i739]: https://github.com/getwyrd/wyrd/issues/739
