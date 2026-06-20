---
created: 20.06.2026 14:42
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#118"
tags:
  - proposal
  - milestone-2
  - implementation-plan
  - networked-storage
---
# Proposal: Milestone 2 — networked D servers (implementation plan)

> The implementation plan for the second *widening* step of the [implementation
> arc](../accepted/0002-implementation-arc.md) (proposal 0002). [Proposal
> 0003](0003-milestone-1-erasure-coding.md) built (in plan) real Reed-Solomon
> erasure coding in the client data path, evolving the `ChunkStore` to a
> **fragment-addressed** trait. M2 keeps that slice intact and replaces its one
> remaining placeholder — the **in-process filesystem `ChunkStore`** — with
> **networked gRPC D servers behind the unchanged trait**, so the client writes
> the *n* fragments of a chunk **directly and in parallel** to distinct storage
> servers and reads back **any *k* that arrive first**. It records *how* M2 is
> built; the *why* of the data-path design lives in the architecture and the ADRs
> it references ([§6.1][s6], [ADR-0009][a9], [ADR-0010][a10]). M2 is
> implementation-first behind versioned protobuf ([ADR-0002][a2]): the gRPC
> surface is a **wire contract**, discovered by building it — **no new spec** is
> required (spec-first effort is reserved for the on-disk format only).

## Motivation

M2 proves that the **direct client→D-server data path works over the network** —
the basis of the throughput-scaling claim ([§6.1][s6]: "bulk data crosses no
shared component"; arc M2). Until now every `ChunkStore` call has been an
in-process function call; M2 turns it into a real gRPC round-trip to a separate
process, **without changing the trait**. The risk this retires is precise: that
the in-process abstraction hid a problem the real gRPC `ChunkStore` will expose —
serialization, connection lifecycle, partial-failure, deadlines, backpressure —
and so proving the **trait seam is real** (arc M2). If the seam holds, every
consumer above it (`core`'s write/read paths, the gateway) is validated against
the real transport with no edit.

The ordering principle of the arc is **risk retired, not features delivered**.
M0 retired atomic-commit-across-paths; M1 retired the coding math. M2 retires the
**networking** risk *in isolation from coding* — exactly the separation M1's
in-process proof was designed to enable. Proving the coding in one process first
([0003][p3] Motivation) meant a failure now isolates cleanly to the *transport*
and not to the surrounding plumbing or the EC loop. This is why networking comes
**after** coding: each risk is validated alone, against an already-working slice.

M2 is the milestone where the system first spans **more than one process** and
the first hardware beyond a laptop appears ([§7.1][s7]). It is the natural first
**split point** of the build ([§9][s9]; arc: "M2 and M3 have the most independent
surface"). It is a **soft stopping point** (arc): a small-multi-node system
exists, but it carries **no production durability promise** — a co-located or
single-chassis profile cannot deliver independent failure domains ([ADR-0014][a14]).
The first deployable product is still M4.

## Design

### Scope boundary

**In scope** — exactly what retires the networking risk:

- A **gRPC `ChunkStore` service** in the `wyrd.v0` protobuf package — `PutFragment`,
  `GetFragment`, `Health` rpcs, **fragment-addressed** to match the M1 trait — and
  the `build.rs` switch from `prost-build` to `tonic`-codegen ([ADR-0002][a2]).
- A new **`chunkstore-grpc`** crate: a client `GrpcChunkStore` implementing
  `traits::ChunkStore` over tonic, and the D-server-side service hosting an
  injected `S: ChunkStore` (in production, `FsChunkStore`).
- The **D server as a role/subcommand** of the existing `server` binary
  ([ADR-0014][a14]/[ADR-0016][a16]), hosting the gRPC service over its local
  filesystem store; the gateway's `ChunkStore` becomes the gRPC **client**.
- **Parallel fan-out write** (commit after all *n* ack, fail-closed) and the
  **any-*k*-arrive-first** parallel read with re-read-on-failure — the read policy
  M1 explicitly deferred ([§6.2][s6]; [0003][p3] Open questions).
- **Minimal placement**: spread a chunk's *n* fragments across *n* **distinct**
  D-server endpoints, discovered through the **L5 `Coordination` seam** ([ADR-0010][a10]).
- **Tier-1 DST over a simulated network** (madsim + `madsim-tonic`, no containers)
  and the **birth of Tier-2** integration tests against real gRPC D servers under
  containers ([ADR-0009][a9]).

**Out of scope** — deferred to the milestone that actually retires their risk,
their hooks already present where retrofit is expensive:

- **Failure-domain-aware placement / durability math** → custodians (M3) and the
  placement service (L2, M6). M2 spreads fragments across *distinct endpoints*,
  **not** independent failure domains ([§7.3][s7]); the RS(6,3) durability math is
  **not claimed**. M2 proves the *data path*, not the *placement guarantee*.
- **Write-back repair, scrub, rebalance, repair-vs-serve throttling** → M3
  custodians ([§6.3][s6], [§8.9][s8]). There are no repair reads to throttle until
  custodians exist; the read-retry path is the **reserved seat** that throttling
  later attaches to, built so as not to foreclose it.
- **Degraded-write tolerance** (commit with < *n*, let custodians backfill) → M3.
  M2 commits only after **all *n*** ack; a partial fan-out **fails closed**
  ([§8.9][s8]: "never a silent half-write").
- **Durability telemetry / observability** → arrives with the custodians at M3
  ([ADR-0011][a11]). M2 sits on the **request plane** only; it emits no
  under-replicated count, repair-queue depth, or scrub coverage. Verified by DST
  and integration, not metrics — as M0/M1.
- **mTLS / provider-CA PKI** → reserved composition seam ([ADR-0005][a5]). M2
  introduces the first real network link; it **structures** the tonic transport so
  mTLS can be configured in (a `server`-level concern, [ADR-0010][a10]), but the
  dev/DST profiles run plaintext. The D server stays **identity-dumb and
  tenant-oblivious** ([§8.5][s8]) — it trusts that a request reaching it was
  already authorized at the gateway.
- **Real etcd-backed dynamic discovery** → a composition swap behind the unchanged
  `Coordination` trait ([ADR-0006][a6]); M2 uses the in-memory concrete and static
  endpoints, not etcd.
- **Production metadata backend (TiKV)** → M4; **cross-zone layers** → M5–M7.

### What carries over from M0/M1, unchanged

Networking slots into **phase 2 (the data path) only**. Everything that *is* the
commit guarantee is untouched — M2 widens phase-2 **transport**, exactly as M1
widened phase-2 *coding*:

- The four-phase write/commit protocol ([§5][s5]): intent → data path → commit →
  release. The commit point is still **one redb write transaction at the home
  zone**; concurrent writers still conflict there and exactly one wins ([ADR-0015][a15]).
  The network data path does **not** move or weaken the commit point.
- The redb metadata model — `inode` / `dirent` / `pending:<chunk_id>` ledger /
  `meta:version` fence — and the M1 chunk map (per-chunk EC scheme + logical length,
  *n* fragments). In M2's single-zone profiles the namespace folds into the same
  embedded store ([ADR-0020][a20]); there is no separate L2 to entangle.
- The **`ChunkStore` trait itself is unchanged** — already fragment-addressed
  (`FragmentId { chunk: ChunkId, index: u16 }`) from M1. M2 implements this exact
  contract over gRPC; there is **no Rust trait change left to make**. The
  *protobuf request messages*, however, are still chunk-addressed from M0 and must
  be fragment-addressed to match (see below).
- The EC coding loop (`reed-solomon-simd`, encode → *n* shards, reconstruct from
  any *k*) is untouched. M2 changes only *where the bytes go* between coding and
  commit.

### Where the network sits in the data path

The client library is the only thing that changes shape: the `ChunkStore` call
that was a function call becomes a gRPC round-trip, and the sequential loop
becomes a parallel fan-out. The bytes cross **no shared component** — straight
from client to the distinct D servers ([§6.1][s6]).

```
write:  chunk ──[erasure-code: k+m]──► n fragments ──┬─► gRPC PutFragment ─► D server 0 (FsChunkStore)
                                                      ├─► gRPC PutFragment ─► D server 1
        (fan-out, all n in parallel)                 └─► … ─► D server n-1
                                          ──[await all n ack]──► commit (one redb txn, unchanged)

read:   ──┬─► gRPC GetFragment ─► D server i ──┐
          ├─► gRPC GetFragment ─► …            ├─[first k that verify]─► [decode] ─► [truncate] ─► bytes
          └─► gRPC GetFragment ─► …            ┘   (re-read elsewhere on miss/corrupt/timeout)
        (fan-out to n; reconstruct from whichever k arrive first; cancel the rest)
```

Both directions are **direct, client→D-server**, the same no-shared-component rule
([§6.1][s6], [§6.2][s6]). In M2's single-zone profile, home-zone resolution and the
chunk-map fetch fold into the local metadata store, so M2's genuinely new code is
the data-path steps — the fan-out put and the any-*k* get.

### The gRPC `ChunkStore` (the load-bearing change)

The `proto` package today (`wyrd.v0`) defines **message shapes only**: there is no
`service` keyword and the `build.rs` uses `prost-build`, not tonic. M2 adds the
transport:

```protobuf
// wyrd.v0, additive — evolved by addition behind versioned protobuf (ADR-0002)
message FragmentId { ChunkId chunk = 1; uint32 index = 2; }   // matches traits::FragmentId

service ChunkStore {
  rpc PutFragment (FragmentPutRequest) returns (FragmentPutResponse);
  rpc GetFragment (FragmentGetRequest) returns (FragmentGetResponse);
  rpc Health      (HealthRequest)      returns (HealthResponse);
}
```

- **Fragment-addressing the wire.** The M0 `ChunkPutRequest`/`ChunkGetRequest`
  carry a bare `ChunkId` — chunk-addressed, a mismatch with the fragment-addressed
  trait they would serve. M2 therefore **replaces the request messages** (the
  `FragmentPutRequest`/`FragmentGetRequest` above), not only wraps them in a
  service: each put/get now carries a `FragmentId`. This is the natural unit M1
  designed the trait around — each fragment independently placed and fetched.
- **`build.rs` switch.** The descriptor-compile step moves from `prost-build` to
  `tonic`-codegen (`tonic-prost-build`), now emitting **client + server** stubs and
  **keeping the `protox` no-`protoc` frontend** if compatible. The package stays
  `wyrd.v0`; the service is **additive** ([§8.7][s8]: fields never repurposed,
  one-version-gap interop) — no `format_version`-style break, no new spec.
- **`Health` over the wire** is the unchanged M0 trait method transported as-is —
  for **connection/discovery liveness only**, **self-reported and trusted**
  ([ADR-0005][a5]). It is **not** durability telemetry, under-replication
  signalling, or proof-of-storage — all of which are M3 ([ADR-0011][a11]).
- **tonic** is pinned once in `[workspace.dependencies]`; under `--cfg madsim` it
  resolves to **`madsim-tonic`** (dep-aliasing) so the *same client code* runs on
  the simulated network in Tier-1 (see DST).

### D server: the networked storage role

The new **`chunkstore-grpc`** crate hosts both sides of the seam (coarse-then-split,
[ADR-0016][a16] — a single firming boundary sharing the generated stubs; its server
side is the coarse start of the architecture's named **`dserver`** crate [§5][s5]):

- **`client`** — `GrpcChunkStore`, a `traits::ChunkStore` impl that dials a
  D-server endpoint and translates `put_fragment` / `get_fragment` / `health` into
  tonic client calls. It is a *consumer* of the trait it implements: it depends on
  `traits` + `proto`, **never** on `chunkstore-fs` ([ADR-0016][a16]).
- **`server`** — the D-server service, generic over an injected `S: ChunkStore`, so
  it hosts `FsChunkStore` in production and a fault-injecting fake under DST. It
  **verifies checksums on put** ([§5][s5] write step 2) and reports self-reported
  `Health`, staying **deliberately dumb** — no placement, no metadata, no identity
  ([§5][s5] L4 table, [§8.5][s8]).

The **D server is a role/subcommand of the existing `server` binary**
([ADR-0014][a14] single-binary-dev, [ADR-0016][a16] coarse-then-split) — a
`d-server` subcommand that opens a local `FsChunkStore` and serves it over gRPC.
Splitting it into a separately-published binary is reserved for the production
role-split, not M2 risk. The gateway's composition (the one place that knows
concretes, [ADR-0010][a10]) swaps `FsChunkStore` for `GrpcChunkStore` in the
networked profile — the trait bound `C: ChunkStore` and every caller in `core` are
**untouched**: composition swaps the concrete, the callers never know.

### Placement & discovery, minimal

M2 introduces exactly one placement primitive: **flat fan-out of a chunk's *n*
fragments across multiple D-server endpoints**, *preferring* distinct endpoints so a
single D-server loss costs at most one fragment and any-*k* read stays meaningful.
This is **best-effort selection, not an enforced placement guarantee** — not even
endpoint-distinctness is a DST-gated invariant. It is the floor that the read policy
and the direct-path throughput claim require, and nothing more.

M2 **does not claim** failure-domain spread (rack/power/switch) or the durability
math. Architecture [§7.3][s7] assigns domain-spread enforcement to the **placement
service (L2, M6)** and the **custodians (L4, M3)**; [ADR-0010][a10]/[ADR-0014][a14]
put placement and durability *authority* there, not in M2's fan-out. M2's
distinctness is **endpoint-distinct, not domain-distinct**.

Discovery goes **only through the L5 `Coordination` trait** (`register` /
`discover` under a group key), **never** an orchestrator API ([ADR-0010][a10],
[§7.2][s7]). This generalizes the gateway's existing `announce`/`nodes` shape: D
servers register their endpoints under a discovery group; the `GrpcChunkStore`
client resolves them via `discover`. M2 uses the **in-memory `Coordination`
concrete** for Tier-1 DST (deterministic, `Clock`-driven lease expiry) and
**static/configured endpoints** for the Tier-2 integration test. Where M0's
registration had no renewal loop ("a networked node would renew"), M2 is where a
real D server first needs **leased registration with renewal** — stale endpoints
now route real bytes. Real etcd dynamic discovery is a pure composition swap behind
the same trait ([ADR-0006][a6]), not M2 risk; the concrete stays wired only in
`server` ([ADR-0010][a10]).

### Parallel write & any-*k* read

**Write — parallel fan-out, commit after all *n* ack.** M2 replaces `core`'s
sequential nested put-loop with a concurrent fan-out: for each chunk, fire all *n*
`put_fragment` calls at once and join on every one. The commit point is unchanged —
the single atomic metadata transaction runs **only after all *n* ack** ([§6.1][s6]).
On a partial fan-out (any put fails or times out), the write **fails closed**: it
aborts *before* the commit, surfaces a retryable error, and leaves the acked
fragments as **leased garbage** the pending-ledger sweep reclaims ([§8.9][s8]:
"never a silent half-write"; [§5][s5]: "failures here are harmless garbage").

**Read — any-*k*-arrive-first.** M2 replaces the in-order serial fetch with a
parallel one: fire `get_fragment` to all *n* indices, reconstruct as soon as the
first *k* fragments **verify their checksums**, then cancel the outstanding
fetches ([§6.2][s6]). A fragment that fails its checksum or times out is treated as
**absent**, and the client **re-reads elsewhere** — turning EC into a tail-latency
*advantage* rather than a tax. `EcScheme::None` stays a single fetch. The read-retry
path is the **reserved seat** where M3's repair-vs-serve throttling later attaches.

**Error taxonomy.** The gRPC `ChunkStore` distinguishes four failures, mapped from
tonic `Status` codes onto the trait's `Result<T, BoxError>` as a small typed enum
(so callers branch on kind, not strings):

- **Unavailable D server** (`UNAVAILABLE` / transport error) → retry elsewhere.
- **Not-found** (`get_fragment`) → `Ok(None)`, *not* an error — preserves the
  trait's `Option` contract.
- **Integrity failure** (checksum mismatch; verified D-server-side on put, client-side
  on read) → treated as **absent** on read, re-read elsewhere.
- **Timeout** (deadline exceeded) → treated as a slow/dead fragment, re-read elsewhere.

What stays unchanged: the commit is still *one* metadata transaction at the home
zone; concurrent-writer-one-wins and never-a-hybrid-read ([ADR-0015][a15]) are
untouched; the read never surfaces uncommitted fragments. M2 moves *bytes over the
wire* — it does not move the commit or weaken linearizability.

### DST and integration tests (the heart of M2)

[ADR-0009][a9] is M2's defining ADR: madsim "simulates time, scheduling, network,
and randomness — the whole runtime," and "Jepsen-style fault injection begins as
soon as there is a networked path" — that path is M2. The madsim runner and the seed
sweep **already exist** — the `dst` crate runs the commit protocol on madsim under
`--cfg madsim`, and `cargo xtask dst` re-runs it across a seed sweep
(`MADSIM_TEST_NUM`, currently 50). M2 **extends** them with a **network** seam; it
does not introduce madsim. `testkit` grows a network abstraction alongside its existing `Clock`/`Disk`
seams; `madsim-tonic` (cfg-aliased) lets the *real* `GrpcChunkStore` code run on
the simulated network.

**Tier-1 — DST over the simulated network** (deterministic, seed-reproducible, no
containers, in the sweep). Determinism must hold *despite* the new parallelism:
`select`/`join` ordering is seed-driven, not wall-clock. The properties asserted:

1. **Parallel-write durability** — after a successful fan-out commit, all *n*
   fragments are readable on their distinct D servers.
2. **k-of-*n* over the network with drops** — reconstruct byte-identical when
   madsim drops/delays up to *m* fragment fetches.
3. **Re-read-on-corruption** — a checksum-failing fragment is treated as absent and
   re-read elsewhere; the read still succeeds.
4. **Fail-closed partial write** — a partial fan-out (injected drop/partition/timeout)
   **aborts pre-commit** and leaves only leased garbage; **no half-committed chunk**
   ([§8.9][s8]).
5. **Commit suite re-run over the network** — the M0/M1 commit-protocol property
   suite (concurrent-writer-one-wins, atomicity, no-hybrid-read) re-runs **unchanged**
   with the gRPC `ChunkStore`, **proving the trait seam is real** (arc M2).

A DST seed that finds a bug is committed as a permanent regression test (the
M0/[ADR-0009][a9] rule).

**Tier-2 — integration against real backends, born at M2** ([ADR-0009][a9],
[0001][p1]). The in-process store is swapped for **real networked gRPC D servers**
running under docker-compose / testcontainers; the test drives a write and read
end-to-end over real tonic — real HTTP/2 framing, real prost (de)serialization of
the fragment-addressed messages, real connection lifecycle and backpressure.
Tier-2 **validates that the abstractions Tier-1 simulates match reality**; it is
non-deterministic and does not replace the deterministic spine. This is the first
hardware beyond a laptop ([§7.1][s7]) and the first container job in CI.

### Benchmarks

The throughput-scaling claim ([§10][s10] Q6: "scales close to linearly with
D-server count, divided by EC amplification") becomes **first measurable** at M2 —
but only on real hardware in Tier-2, **not** in the deterministic CI tier (CI
wall-clock is noisy and the simulated network has no physical throughput). M2 adds
a criterion/Tier-2 **aggregate write/read throughput** benchmark across D-server
counts, wired as `cargo xtask bench` ([ADR-0016][a16]) and entering CI as
**tracked numbers for regression visibility, not a hard gate**. The *number* lands
on real hardware; M2's in-CI obligation is only to prove the data path **does not
build a shared bottleneck** that would preclude Q6.

### Crate touch-points

Building on the workspace as it stands after M1 (`chunk-format`, `traits`, `proto`,
`core`, `chunkstore-fs`, `metadata-redb`, `coordination-mem`, `testkit`, `server`,
`dst`, `xtask`):

- **`proto`** — add the `FragmentId` message, **fragment-address** the put/get
  requests, add the `ChunkStore` **service**; `build.rs` `prost-build` →
  `tonic`-codegen (keep `protox`); `Cargo.toml` gains `tonic`.
- **`chunkstore-grpc`** (**new**) — `client` (`GrpcChunkStore: ChunkStore`) +
  `server` (tonic service over injected `S: ChunkStore`); deps `traits`, `proto`,
  `tonic`, `bytes`, `async-trait`; cfg-aliased `madsim-tonic`.
- **`core`** — trait unchanged; `write_fragments` → **parallel fan-out**,
  `read_chunk` → **any-*k*-arrive-first** with re-read-on-failure; typed transport
  error enum.
- **`server`** — the networked-profile composition swaps `wyrd-chunkstore-fs` for
  `wyrd-chunkstore-grpc`; new **`d-server` subcommand** hosting `FsChunkStore`
  behind the service; D-server endpoint discovery via `Coordination::discover`.
- **`testkit`** — a **network seam** (drop/delay/partition fault points) alongside
  the existing `Clock`/`Disk` seams.
- **`dst`** — network-DST tests under `--cfg madsim` + `madsim-tonic`; new seeds.
- **`xtask`** — Tier-2 integration runner (docker-compose/testcontainers); throughput
  `bench`.
- **`Cargo.toml`** (workspace) — pin `tonic` (and `madsim-tonic`) once in
  `[workspace.dependencies]`; add `crates/chunkstore-grpc` to `members`.
- **`deny.toml`** — `tonic` drags in `hyper`/`h2`/`tower` (`prost` already allowed);
  extend `[licenses].allow` for vetted transitives ([ADR-0003][a3] gate); any
  test-only `madsim-tonic` advisory follows the existing `RUSTSEC-2025-0141`
  `ignore` precedent.

## Alternatives considered

- **A bulk-data hop through the gateway** instead of client→D-server direct:
  rejected. It defeats the "bulk data crosses no shared component" property that
  *is* the throughput-scaling claim ([§6.1][s6], [§10][s10] Q6) — the whole reason
  M2 exists.
- **Keeping the M0 chunk-addressed proto messages and wrapping them in a service**:
  rejected. The trait is fragment-addressed; a chunk-addressed wire cannot serve it.
  The request messages must grow a `FragmentId` — fragment-addressing is the unit
  M1 designed for independent placement and fetch ([0003][p3]).
- **A non-tonic / hand-rolled gRPC or a non-gRPC RPC**: rejected. tonic is the
  de-facto prost-based Rust standard and the `proto` crate is already prost; gRPC
  is implementation-first behind versioned protobuf ([ADR-0002][a2]). The named
  D-server reimplementation seam is its protobuf contract ([ADR-0004][a4]).
- **A separate published D-server binary**: deferred. Single-binary-dev
  ([ADR-0014][a14]) and coarse-then-split ([ADR-0016][a16]) bless a `d-server`
  **subcommand**; the binary split waits for the production role-split.
- **Real etcd discovery in M2**: deferred to a composition swap ([ADR-0006][a6]).
  In-memory `Coordination` serves Tier-1 DST and static endpoints serve Tier-2;
  etcd is a production backend behind the same trait, not M2 risk.
- **Failure-domain-aware placement (and durability math) in M2**: that is L2
  (M6) + custodians (M3) ([§7.3][s7]). M2 spreads across distinct endpoints only;
  claiming the durability math here would assert a guarantee M2's topology cannot
  yet keep ([ADR-0014][a14]).
- **A ChunkStore-level in-sim fake for Tier-1** (no tonic in DST): kept as the
  explicit **fallback**, not the primary. It sidesteps the `madsim-tonic` version
  risk entirely but tests a *fake*, not the wire code, pushing all
  transport-correctness onto the non-deterministic tier — inverting [ADR-0009][a9]'s
  intent. Primary is `madsim-tonic` (real wire code under deterministic faults); the
  fake is the documented retreat if the PR-1 version spike shows `madsim-tonic`
  cannot track a shippable `tonic`.

## Graduation criteria (definition of done)

- The in-process filesystem store is **replaced by networked gRPC D servers behind
  the unchanged `ChunkStore` trait** — `core` and the gateway compile and pass with
  no caller edit.
- A file written via S3 PUT under `rs(6,3)` against a **multi-D-server** networked
  profile reads back **byte-identical** via GET — the client **writing the 9
  fragments directly and in parallel** to distinct D servers, the read
  reconstructing from **whichever 6 arrive first**.
- A chunk's *n* fragments are written **directly and in parallel to multiple
  networked D servers** (distinct-endpoint selection best-effort, not a gated
  placement guarantee); D servers are discovered through the **L5 `Coordination`
  seam**, never an orchestrator API.
- **Fail-closed write** proven: an injected partial fan-out **aborts before commit**,
  leaves only leased garbage, and never commits a half-written chunk.
- **Any-*k* read with drops/corruption** proven over the simulated network: up to
  *m* dropped/corrupt fragments still read byte-identical, re-reading elsewhere.
- The **M0/M1 commit-protocol property suite still passes** with the gRPC
  `ChunkStore` under madsim network simulation — seed-reproducible, seeds committed.
- **Tier-2 integration** stands up: real gRPC D servers under containers, an
  end-to-end write/read over real tonic green in CI.
- M2 emits **no durability telemetry** (deferred to M3) and **claims no
  failure-domain durability math**.
- `fmt`/`clippy` clean; `Cargo.lock` updated; `cargo-deny` passes with `tonic` and
  its vetted transitives.

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M2** milestone:

1. **`madsim-tonic` / `tonic` version spike + proto service** — pin the newest
   `tonic` that `madsim-tonic` supports; add the `FragmentId` message,
   fragment-address the requests, add the `ChunkStore` service; switch `build.rs` to
   tonic-codegen; clear the `cargo-deny` license wall. *DoD:* `proto` builds client
   + server stubs; the version-compat matrix is settled; `cargo-deny` green.
2. **`chunkstore-grpc` crate** — `GrpcChunkStore` client + the D-server service over
   an injected `S: ChunkStore`; checksum-verify on put; `Health` over the wire.
   *DoD:* a round-trip `put`/`get`/`health` against an in-process tonic server passes.
3. **`d-server` role + discovery** — the `server` subcommand hosting `FsChunkStore`;
   leased registration + `discover` through the `Coordination` seam. *DoD:* a D
   server registers and is discovered; the gateway resolves *n* distinct endpoints.
4. **Parallel fan-out write** — `core::write_fragments` → concurrent puts,
   commit-after-all-*n*-ack, fail-closed on partial. *DoD:* an `rs(6,3)` write fans
   out to distinct D servers; commit still atomic; partial fan-out aborts pre-commit.
5. **Any-*k*-arrive-first read** — `core::read_chunk` → parallel fetch, reconstruct
   from first *k* that verify, re-read elsewhere, cancel the rest; typed error enum.
   *DoD:* an `rs(6,3)` GET is byte-identical; reconstructs with up to *m* missing/slow.
6. **Tier-1 network DST** — `testkit` network seam + `madsim-tonic`; drop/delay/
   partition/corruption properties; the commit suite re-run over gRPC. *DoD:* all
   green in the 50-seed sweep; bug-finding seeds committed.
7. **Tier-2 integration + throughput bench** — docker-compose/testcontainers D
   servers; end-to-end write/read over real tonic; the tracked throughput benchmark.
   *DoD:* the container integration test passes in CI; first throughput data points
   recorded.

## Backward compatibility

- **On-disk format** — **unchanged**. M2 transports M1's fragments over the wire; it
  does not touch the fragment layout or `format_version`. The format stays
  v0/unstable; no production data exists to migrate.
- **Wire contract** — the gRPC `ChunkStore` is a **new** inter-component contract in
  `wyrd.v0`, **evolved by addition** ([§8.7][s8]): fields are never repurposed, so
  neighbours interoperate across a one-version gap. The fragment-addressing of the
  request messages is a change to **never-yet-shipped** shapes (no transport
  existed), not a break of a deployed contract.
- **Trait / internal API** — the `ChunkStore` trait is **unchanged** (already
  fragment-addressed at M1); M2 only adds a new concrete (`GrpcChunkStore`) behind
  it. The swap is composition-local ([ADR-0010][a10]); consumers see only the trait.
- **Public API / deployments** — none yet (the first deployable product is M4);
  nothing to stay compatible with. The single-binary profile remains dev/eval-only
  ([ADR-0014][a14]).

## Open questions

- **Placement record vs. stateless selection** — M2's *own* read path, after a
  process restart, must resolve each fragment index back to the endpoint that holds
  it, so *some* location record is M2-intrinsic. The open question is its shape:
  stateless per-read selection (re-discover and probe any sufficient set), or a
  chunk's fragment *i* **recorded** at commit (a placement map in the chunk map)?
  Current lean: record placement at commit; M3 custodians will *also* consume it for
  scrub/reconstruct, but the M2 read need stands on its own. Confirm the shape at M3.
- **Lease TTL / renewal cadence and mid-write staleness** — what TTL and renewal
  interval does a registered D server use, and how does the client handle a
  discovered-but-dead endpoint mid-write — **abort-and-fail-closed** (M2) vs.
  **re-place** (M3)? M2 takes abort-and-fail-closed; re-place is degraded-write
  tolerance, deferred.
- **`madsim-tonic` version tracking** — can `madsim-tonic` track a `tonic` recent
  enough to ship? Settled empirically in PR-1; the ChunkStore-level in-sim fake is
  the recorded fallback if not ([ADR-0009][a9]).
- **mTLS seat shape** — the transport reserves an mTLS termination point
  ([ADR-0005][a5], status *Proposed*); whether that seat is configured at the tonic
  channel or wrapped a layer up is left to the PKI slice, not decided here.
- **Note on Proposed ADRs** — M2's telemetry-deferral ([ADR-0011][a11]) and
  trust-boundary ([ADR-0005][a5]) claims rest on **Proposed** (not yet Accepted)
  ADRs; M2 leans hardest on [ADR-0009][a9] (Accepted). Recorded for traceability.

[p1]: ../accepted/0001-milestone-0-walking-skeleton.md
[p3]: 0003-milestone-1-erasure-coding.md
[s5]: ../../architecture/05-building-block-view.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s9]: ../../architecture/09-build-order-and-roadmap.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a4]: ../../adr/0004-rust-as-implementation-language.md
[a5]: ../../adr/0005-single-provider-closed-federation.md
[a6]: ../../adr/0006-etcd-for-coordination.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a15]: ../../adr/0015-consistency-contract.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a20]: ../../adr/0020-global-namespace-store.md
