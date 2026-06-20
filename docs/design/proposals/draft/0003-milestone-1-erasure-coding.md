---
created: 20.06.2026 02:14
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#100"
tags:
  - proposal
  - milestone-1
  - implementation-plan
  - erasure-coding
---
# Proposal: Milestone 1 — erasure coding (implementation plan)

> The implementation plan for the first *widening* step of the [implementation
> arc](../accepted/0002-implementation-arc.md) (proposal 0002). [Proposal
> 0001](../accepted/0001-milestone-0-walking-skeleton.md) built the walking
> skeleton — one atomic write and read in a single process, the commit protocol
> proven under fault injection. M1 keeps that slice intact and replaces its one
> placeholder — `replication(1)`/`none` durability — with **real Reed-Solomon
> erasure coding in the client data path**. It records *how* M1 is built; the
> *why* of the EC design lives in the ADRs it references ([ADR-0008][a8],
> [ADR-0019][a19], [ADR-0021][a21]). M1 is implementation-first behind the
> already-specified format ([ADR-0002][a2]): no new spec is required, because the
> EC-scheme identifiers were fixed in `chunk-format/v1.md` as part of the format
> prerequisite.

## Motivation

M1 proves that **real Reed-Solomon encode/decode and reconstruction-from-any-*k*
works in the client data path** ([§9][s9] widening step 1; arc M1). This is the
hottest, most failure-critical loop in the system: every byte written is
erasure-coded and every byte read may be reconstructed, so a correctness bug here
is silent data corruption at scale.

The ordering principle of the arc is **risk retired, not features delivered**.
M0 retired the largest risk — that atomic commit across separate metadata and
data paths is hard. M1 retires the *next*: that the coding math, the
stripe/fragment layout, and the any-*k* reconstruction are correct. It is
de-risked **against the already-working slice** rather than in the abstract — the
M0 commit protocol gives EC a correct write path to encode into and a correct
read path to reconstruct out of, so a failure isolates cleanly to the coding
loop and not to the surrounding plumbing. This is why EC comes before networked
D servers (M2): proving the coding in one process separates the *EC-correctness*
risk from the *networking* risk, and each is validated alone.

M1 is a **soft stopping point** (arc): the system becomes space-efficient
(≈1.5× overhead instead of replication's 2–3×) but is still single-process and
not yet deployable. The first deployable product is M4.

## Design

### Scope boundary

**In scope** — exactly what retires the EC-correctness risk:

- Real Reed-Solomon encode/decode in the client, via **`reed-solomon-simd`** (the
  decided library, [ADR-0003][a3]/[ADR-0004][a4]), parameterized over RS(*k*,*m*).
- The chunk → stripe → fragment model: a chunk is one RS stripe of *n* = *k*+*m*
  fragments, written through the `ChunkStore`.
- **Evolving the `ChunkStore` trait** from chunk-addressed to **fragment-addressed**
  (the load-bearing structural change — see below).
- The read path: gather any *k* of *n* fragments, verify checksums, reconstruct,
  return byte-identical data; survive up to *m* fragment losses without read error.
- Producing real `ec_scheme_type = 2` (reed-solomon) fragments and landing a
  Reed-Solomon **conformance vector**.
- DST property tests for the coding loop and EC benchmarks in CI.

**Out of scope** — deferred to the milestone that actually retires their risk,
their hooks already present where retrofit is expensive:

- **Networked D servers / real failure domains** → M2. M1's *n* fragments
  co-locate in the single in-process filesystem store; there is no placement and
  no domain spread yet ([§7][s7], [§8.9][s8]). M1 proves the *coding*, not the
  *placement*, so durability math ([§7][s7]: fragments in independent failure
  domains) is not yet claimed.
- **Write-back repair / healing** → M3 (custodians). M1 does **read-time
  reconstruction** ([§6.2][s6]) only: it reconstructs to satisfy a read, it does
  **not** rebuild the lost fragment back into the store. Scrub → reconstruct →
  re-place → commit-point-atomic location update is the custodian loop
  ([§6.3][s6]), the *second* home of correctness risk, retired later with real
  data to maintain.
- **Production metadata backend (TiKV)** → M4; **cross-zone layers** → M5–M7.
- **Small-file inlining** → its own later slice. M1 stays EC-only; it ships the
  benchmarks that make the inline-threshold decision measurable, but does not
  build the inlining mechanism (it does not retire the EC risk).
- **Telemetry / observability** → arrives with the custodians at M3
  ([ADR-0011][a11]); M1 is verified by DST, not metrics, exactly as M0.

### What carries over from M0, unchanged

EC slots into **phase 2 (the data path) only**. Everything that *is* the
differentiator is untouched:

- The four-phase write/commit protocol ([§5][s5]): intent → data path → commit →
  release. The commit point is still **one redb write transaction**; concurrent
  writers still conflict there and exactly one wins.
- The redb metadata model: `inode` / `dirent` / `pending:<chunk_id>` ledger /
  `meta:version` fence.
- Minimal S3 PUT/GET, in-memory `Coordination`, the test-invoked ledger sweep.

The only metadata change is additive: the committed **chunk map** now records, per
chunk, the EC scheme (`ec_scheme_type`, *k*, *m*) and the chunk's **logical
length** (pre-padding), and references *n* fragments instead of one. The
atomicity is unchanged — still the single transaction that writes the chunk map,
sets `COMMITTED`, and bumps the version conditional on the prior. M1 widens the
proven slice **without disturbing the proof**.

### Where EC sits in the data path

```
write:  chunk ──[erasure-code: k data + m parity shards]──► n fragments ──► ChunkStore.put ──► commit (unchanged)
read:   ChunkStore.get (any k of n) ──[verify checksums]──► [decode/reconstruct] ──[truncate to logical length]──► bytes
```

EC is the client-library layer directly above the `ChunkStore` and below the
(future) client-side encryption: per [ADR-0021][a21], the client encrypts
*before* erasure coding and decrypts *after* reconstruction, so EC always
operates on the stored bytes. M1 has no encryption, so it codes plaintext; the
ordering is fixed now so the encryption slice (later) inserts cleanly above EC.

### The chunk → stripe → fragment model

A file is split into **chunks** (the unit of placement and erasure coding,
[§10 glossary][s10]). Each chunk is encoded as exactly one **stripe**:

1. The chunk's bytes are divided into ***k* equal-size data shards** (zero-padded
   to equal length, and to the library's alignment requirement). `reed-solomon-simd`
   computes ***m* parity shards** of the same size, giving *n* = *k*+*m* shards.
2. Each shard is wrapped in the v1 fragment header ([spec][spec]) with
   `ec_scheme_type = 2`, `ec_k = k`, `ec_m = m`, `ec_fragment_index = 0..n-1`, all
   *n* sharing the chunk's u128 `chunk_id`; the payload is the shard bytes; the
   `payload_checksum` (crc32c) is over the stored shard.
3. The chunk's **logical length** is recorded in the chunk-map metadata, so the
   reader strips padding after reconstruction. The per-fragment `payload_length`
   is the *shard's* stored length; the chunk's logical length is authoritative
   metadata. (The header's EC fields MUST agree with that metadata — [spec][spec];
   the writer fills both from the same source, a disagreement is detectable
   corruption.)

Reconstruction: gather **any *k*** of the *n* fragments, verify each fragment's
header and payload checksums, feed the surviving data+parity shards (by index) to
`reed-solomon-simd` to recover the missing data shards, concatenate the *k* data
shards, and truncate to the logical length.

### The `ChunkStore` trait evolution (the load-bearing change)

M0's `ChunkStore` is deliberately coarse — its own doc comment says the
signatures "will firm up as the commit protocol and the DST harness pin the
semantics." It addresses a fragment by **`chunk_id` alone**:

```rust
async fn put_fragment(&self, id: ChunkId, fragment: Bytes) -> Result<()>;
async fn get_fragment(&self, id: ChunkId) -> Result<Option<Bytes>>;
```

RS(*k*,*m*) maps one `chunk_id` to ***n* fragments**, so this firms up at M1 to
**fragment-addressed**: the key becomes `FragmentId { chunk: ChunkId, index: u16 }`
(the `ec_fragment_index` of the stored fragment). This is the natural addressing
unit for M2's networked D servers — each fragment is independently placed and
fetched — so the change is made **once**, here, and M2's gRPC `ChunkStore`
inherits the same contract. It subsumes M0's `replication(1)` cleanly (index 0).

- The `chunkstore-fs` implementation keys storage by `(chunk_id, index)`
  (e.g. a per-chunk directory with one file per index).
- The read path knows *n* from the chunk map's scheme, so it fetches indices
  `0..n` and takes the first *k* that return **and** verify; no separate listing
  operation is required at M1. (A `list`/`has` affordance may be added when M2's
  networked discovery needs it.)
- The trait stays behind the seam ([ADR-0010][a10]): this is a composition-local
  change to `traits` + `chunkstore-fs`, not a refactor of consumers.

### The on-disk format and conformance

The EC fields were **specified at M0** (reserved-and-decided, [ADR-0019][a19]);
M1 is the first writer to **populate** `ec_scheme_type = 2`. Producing
reed-solomon fragments therefore *exercises an already-specified code point* — a
**backward-compatible** use of the format that "MUST be accompanied by new
conformance vectors" but is **not** a `format_version` increment ([spec][spec],
Versioning). M1 lands at least one RS vector under
`specs/conformance/vectors/v1/` (a reed-solomon data shard, and a parity shard,
with `ec_scheme_type: "reed-solomon"`, `ec_k: 6`, `ec_m: 3`, and the
`ec_fragment_index`), generated via `cargo xtask gen-vectors` and checked by
`cargo xtask conformance`. The format **stays v0/unstable** through M1 (decided):
the real RS path is new enough that retaining the freedom to adjust the layout is
worth more than an early freeze; `v1` is stamped later at a deliberate trigger
(see Open questions), not as a side effect of this milestone.

### Default scheme and configuration

`durability` becomes a real selectable value: `rs(k,m)` joins `none` and
`replication(n)`. The **default is `rs(6,3)`** — the scheme the architecture
already uses as its running example ([§8.2][s8] redundancy model; the [§10][s10]
Q6 ≈1.5× throughput scenario), so the docs stay self-consistent. The code is
parameterized over (*k*,*m*) and tested across a matrix regardless; the default
only fixes the config default and the headline conformance vector. Backend
selection remains composition-in-`server` ([ADR-0008][a8]), not a runtime flag.
The **chunk/stripe size** stays a hardcoded default — it is the deferred
empirical open question, now the subject of M1's benchmarks (see Open questions).

### DST and property tests (the heart of M1)

Reed-Solomon encode/decode is pure, deterministic computation — an ideal fit for
the `testkit` DST harness ([ADR-0009][a9]). The property tests assert:

1. **Roundtrip** — `decode(encode(chunk))` is byte-identical, across a matrix of
   (*k*,*m*) and chunk sizes including the edges: empty, 1 byte, exactly *k*
   bytes, a non-shard-aligned size, and large multi-shard.
2. **Reconstruct-from-any-*k*** — for the default RS(6,3), *enumerate* all
   C(9,6) = 84 *k*-subsets and decode each to the original; for wider schemes,
   sample subsets deterministically by seed. This is the literal arc criterion
   "read back by reconstructing from any *k*."
3. **Loss survival** — deleting up to *m* fragments (any combination) still reads
   byte-identical; deleting *m*+1 yields a **clean typed error** (insufficient
   fragments), never a panic or corrupted bytes. The error-vs-corruption
   distinction is itself a property.
4. **Corruption detection** — a bit-flipped fragment fails its `payload_checksum`,
   is **excluded** by the read path (treated as missing) and reconstructed around
   if ≥ *k* remain, else a clean error. A checksum-failing shard is **never** fed
   to the decoder (which would silently produce garbage). This ties EC to the
   format's integrity guarantee.
5. **Commit-protocol integration under EC** — the full M0 property suite re-run
   with the EC data path swapped in: a file written via S3 PUT under `rs(6,3)`
   reads back byte-identical via GET; concurrent writers → exactly one commit
   wins; a reader sees pre- or post-commit, never a hybrid; crash between phases
   3 and 4 → the file is fully visible or not at all, and no committed chunk-map
   entry references a fragment the sweep would reclaim — now with *n* fragments
   per chunk instead of one.
6. **Mixed-era read** — a chunk written under M0's `replication(1)`/`none` and a
   chunk written under `rs(6,3)` both read correctly through the same read path,
   driven by the per-chunk/per-fragment scheme — the [ADR-0008][a8] mixed-era
   claim validated in miniature.

A DST seed that finds a bug is committed as a permanent regression test (the
M0/[ADR-0009][a9] rule).

### Benchmarks in CI

Criterion micro-benchmarks for **encode**, **decode** (no loss), and
**reconstruct** (with loss) across schemes {RS(4,2), RS(6,3), RS(10,4)} and shard
sizes, wired as `cargo xtask bench` (logic in Rust, [ADR-0016][a16]) and run in
CI. They enter CI as **tracked** numbers for regression visibility, **not** as a
hard pass/fail gate — CI-runner wall-clock is noisy, and the throughput-scaling
claim is only truly measurable on real hardware at M2+ (the M0 proposal's point).
The pure-CPU coding loop is the one performance number meaningful even on a
laptop, and it is the first real data point informing the deferred chunk/stripe
size (Open questions).

### Crate touch-points

Building on the M0 layout (`chunk-format`, `traits`, `proto`, `core`,
`chunkstore-fs`, `metadata-redb`, `testkit`, `server`, `xtask`):

- **`traits`** — `ChunkStore` → fragment-addressed (`FragmentId`).
- **`core`** — a small `erasure` module wrapping `reed-solomon-simd`; the client
  write/read paths gain encode/decode.
- **`chunkstore-fs`** — fragment-addressed storage.
- **`chunk-format`** — round-trip `ec_scheme_type = 2`; new RS vectors.
- **`testkit`** — a fault-injection hook to drop/corrupt a stored fragment.
- **`xtask`** — `bench` command; RS vector generation.
- **`server`** — `durability = rs(k,m)` config; chunk map carries scheme +
  logical length (metadata schema is implementation-first, [ADR-0002][a2]).
- **deps** — add `reed-solomon-simd`; confirm it under the `cargo-deny`
  permissive-license allowlist ([ADR-0003][a3]).

## Alternatives considered

- **Per-fragment distinct `chunk_id`s** instead of `(chunk_id, index)`: rejected.
  The glossary and format define a fragment as a piece of a chunk that *shares*
  the chunk's id with a per-stripe index ([§10][s10], [spec][spec]); distinct ids
  would break the self-describing stripe grouping and the metadata model.
- **Stuffing all *n* fragments into one blob under the M0 chunk-addressed key**:
  rejected. It defeats the independent per-fragment placement and fetch that M2's
  networked D servers need, and the any-*k*-arrive-first tail-latency advantage of
  the read path ([§6.2][s6]).
- **Write-back repair/healing in M1**: deferred to M3. M1's risk is coding
  correctness, not the maintenance loop; conflating them widens M1 past the risk
  it exists to retire.
- **Networked D servers (and thus real failure domains) in M1**: that is M2.
  Proving the coding in-process first isolates EC-correctness from networking risk
  — the arc's risk-ordering.
- **A dedicated EC ADR**: not minted. The EC decisions are already on record — the
  library ([ADR-0003][a3]/[ADR-0004][a4]), the scheme encoding ([ADR-0019][a19]),
  per-zone config and mixed-era ([ADR-0008][a8]), order-vs-encryption
  ([ADR-0021][a21]) — and M1 is implementation-first behind them ([ADR-0002][a2]),
  so the data-path and trait-evolution details are recorded here and in-code. (Whether
  the `ChunkStore` fragment-addressing warrants its own ADR once M2 confirms the
  networked shape is an Open question.)

## Graduation criteria (definition of done)

- A file written via S3 PUT under `durability = rs(6,3)` reads back **byte-identical**
  via GET — the data path erasure-coding the chunk into 9 fragments, the read
  reconstructing from any 6.
- **Reconstruct-from-any-*k*** proven: decoding from every *k*-subset (enumerated
  for RS(6,3)) yields the original.
- **Up to *m* fragment losses survived** without read error; *m*+1 losses produce
  a clean typed error, never corruption.
- A **bit-flipped fragment is excluded by its checksum** and the read reconstructs
  from the remaining *k* (or errors cleanly) — never returns garbage.
- The M0 **commit-protocol property suite still passes** with the EC data path
  (one-writer-wins, no-hybrid-read, crash-3–4 atomicity), seed-reproducible.
- At least one **reed-solomon conformance vector** lands under `specs/conformance/`
  and the reference reader accepts it; the format **stays v0/unstable**.
- **EC micro-benchmarks** (encode/decode/reconstruct across schemes and sizes) run
  in CI as tracked numbers.
- `fmt`/`clippy` clean; `Cargo.lock` updated; `cargo-deny` passes with
  `reed-solomon-simd`.

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M1** milestone:

1. **`ChunkStore` → fragment-addressed** (`FragmentId{chunk,index}`); update
   `chunkstore-fs` and the M0 `replication(1)` write/read to the new key (index 0).
   *DoD:* existing M0 tests pass through the new addressing.
2. **`erasure` coder** over `reed-solomon-simd` (encode chunk → *n* shards, decode
   any-*k* → chunk), pure and DST-friendly. *DoD:* roundtrip + any-*k* property
   tests green across the (*k*,*m*) matrix.
3. **Write path** — client erasure-codes the chunk in phase 2 and writes *n*
   fragments; chunk map records scheme + logical length; commit unchanged. *DoD:*
   an `rs(6,3)` PUT stages 9 fragments; commit still atomic.
4. **Read path** — gather any *k* (checksum-verify, exclude bad), reconstruct,
   truncate, return. *DoD:* an `rs(6,3)` GET is byte-identical; reconstructs with
   up to *m* missing.
5. **`chunk-format` RS vectors** — round-trip `ec_scheme_type = 2`; land the RS
   conformance vector(s) via `xtask gen-vectors` and wire into the conformance
   check. *DoD:* the reference reader accepts the RS vector.
6. **DST campaign** — loss-survival, corruption-detection, *m*+1-clean-error, the
   M0 property suite re-run under EC, and the mixed-era read. *DoD:* all green,
   seeds committed.
7. **Benchmarks** — criterion EC benches + `cargo xtask bench`, into CI as tracked
   numbers. *DoD:* benches run in CI; first encode/decode/reconstruct data points
   recorded.

## Backward compatibility

- **On-disk format** — the EC fields were specified at M0; M1 is the first to
  populate `ec_scheme_type = 2`, a **backward-compatible** use of an already-decided
  code point (new conformance vectors, **no** `format_version` increment). The
  format **stays v0/unstable**; no production data exists to migrate.
- **Mixed-era data** — chunks written under M0's `none`/`replication(1)` remain
  readable: the per-chunk scheme in the chunk map and fragment header drives the
  read path ([ADR-0008][a8]), validated by M1's mixed-era test.
- **Trait / internal API** — the `ChunkStore` fragment-addressing change is
  internal (pre-1.0, no published API) and composition-local; M2's gRPC
  `ChunkStore` is designed to the new contract from the start.
- **Public API / deployments** — none yet (the first deployable product is M4);
  nothing to stay compatible with.

## Open questions

- **Chunk/stripe size** — stays **deferred to measurement** (proposal 0002). M1
  produces the first benchmarks that *inform* it, but the value remains a
  hardcoded default **set against, not frozen by**, M1's numbers, because the
  throughput/overhead trade also depends on real disk and network behavior at M2+.
- **`v1`-stamping trigger** — with M1 staying v0, what *will* stamp `v1`: a second
  independent reader implementation, or a dedicated sustained fault-injection
  campaign distinct from M1's correctness suite? Recorded here, not decided.
- **Dedicated EC / fragment-addressing ADR** — whether the `ChunkStore` evolution
  warrants its own ADR once M2 confirms the networked shape, or whether this
  proposal plus in-code records suffice (current lean: the latter,
  [ADR-0002][a2]).
- **Partial-availability read policy** — M1 fetches indices `0..n` from the single
  store and takes the first *k* that verify; the tail-latency-optimal "whichever
  *k* arrive first" policy ([§6.2][s6]) only becomes meaningful with networked D
  servers (M2), where fragments have independent latencies. M1's single-store
  policy is a placeholder.

[s5]: ../../architecture/05-building-block-view.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s9]: ../../architecture/09-build-order-and-roadmap.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[spec]: ../../specs/chunk-format/v1.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a4]: ../../adr/0004-rust-as-implementation-language.md
[a8]: ../../adr/0008-tikv-metadata-and-pluggable-backends.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a19]: ../../adr/0019-chunk-format-layout.md
[a21]: ../../adr/0021-encryption-at-rest-and-key-management.md
