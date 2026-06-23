---
created: 23.06.2026 13:50
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#220"
tags:
  - proposal
  - performance
  - d-server
  - storage
  - networking
  - deployment
  - operations
---
# Proposal: D-server performance

> Draft. The performance program for the **D server** — Wyrd's "deliberately dumb"
> storage server that receives opaque, erasure-coded **ciphertext** fragments over
> gRPC, writes them to local NVMe, and serves them back by chunk id (architecture
> §5, §6.1; ADR-0025). It organizes the levers into tiers — code fixes behind the I/O
> seams, `deploy/`-layer OS/kernel tuning (the "custom Linux" question), mTLS/transport,
> architectural + durability changes, and the exotic kernel-bypass tier — each with an
> honest verdict and a Wyrd-fit rationale. The durability lever is recorded separately
> as ADR-0033 because it is correctness-sensitive. Nothing here is adopted on faith:
> DST never measures performance, so every item is validated on real hardware (Tier
> 2/3) and every code-level change lands behind a seam so the deterministic correctness
> tier is untouched.

## Motivation

The D server is where Wyrd's throughput-scaling claim lives: the client writes a
chunk's `n` fragments **directly and in parallel** to distinct D servers, so bulk
data crosses no shared component (architecture §6.1; scenario Q6). Making a single D
server fast therefore compounds across the fleet. But "fast" must be defined against
what actually binds.

The decisive framing comes from the *shape* of the real hot path, not yet from
measurement (mTLS is not even wired): a D server is a thin move-bytes-and-checksum
store, and for a `k`-of-`n` fragment store **behind mTLS gRPC**, the throughput/latency
ceiling is **expected to be set by network + TLS + redundancy economics before raw NVMe
IOPS** — the hypothesis the Tier-2/3 harness exists to confirm. The exotic IOPS
techniques (SPDK, DPDK, RDMA,
ZNS, io_uring thread-per-core runtimes) chase a number Wyrd cannot spend, and each
breaks a load-bearing constraint: deterministic simulation as the correctness
authority (ADR-0009 — and `madsim` simulates tokio only, not io_uring/glommio/monoio),
pure-Rust + cross-compile + single static binary + NAS portability (ADR-0008,
ADR-0014; the on-disk format is ADR-0019), and the mTLS gRPC data path (ADR-0025).
Three facts about the
code as it stands sharpen this:

- **Durability is already redundancy + crash-atomic `rename`, not fsync** — the put
  path takes no fsync (`crates/chunkstore-fs/src/lib.rs:71-85`). The single biggest
  write lever is already realized; recording and bounding it is ADR-0033.
- **mTLS is not wired yet** — the serve loop is plaintext tonic
  (`crates/server/src/dserver.rs:159-162`; tonic pinned without `tls` to keep the DST
  path plaintext, `crates/proto/Cargo.toml:34-36`). ADR-0025 mandates mTLS on the
  whole data path, fail-closed. Today's plaintext numbers are transitional; every
  budget must assume TLS is permanently on the hot path.
- **True zero-copy is structurally precluded** — `get_fragment` copies the fragment
  into a `Vec<u8>` for protobuf (`crates/chunkstore-grpc/src/server.rs:63-79`), crc32c
  is verified over stored bytes on every GET (`crates/chunkstore-fs/src/lib.rs:95`,
  per ADR-0019), and per-frame mTLS re-encrypts on the way out. The realistic target
  is **zero-*re*-copy in userspace**, not zero-copy to the wire.

So the program is: take the cheap, high-ROI code and `deploy/` wins; record and bound
the durability model; and explicitly *not* spend effort on the exotic tier, with the
reasons on the record so the question stays answered.

## Design

Every code-level lever lands **behind the `ChunkStore`/`PlacementChunkStore` trait or
the testkit `Disk` seam** (`crates/traits/src/lib.rs:85-161`,
`crates/testkit/src/lib.rs:79-121`), so the deterministic correctness tier keeps
running the same source over `madsim-tonic` (ADR-0009) while the real backend evolves
independently. Every OS/kernel lever lands in **`deploy/`, outside the Rust
workspace** (ADR-0010), so the single binary "comes up identically under
systemd/compose/k8s." Verdicts are tagged **[adopt-now]**, **[measure-first]**,
**[reserve]**, **[avoid]**.

### Tier 0 — code fixes behind the seams (DST-safe; do first)

- **[adopt-now] Offload blocking `std::fs` off the tokio worker threads — the biggest
  code lever.** `FsChunkStore`'s `async fn`s are sync-bodied: they run blocking
  `fs::write`/`rename`/`read` inline (`crates/chunkstore-fs/src/lib.rs:71-97`). On the
  production multi-thread runtime (`crates/server/src/cli.rs:266-279`) a sync-bodied
  store op awaited on a tonic handler blocks that tokio worker for the I/O's duration,
  capping concurrency and inflating tail latency under fan-in. **The offload must not
  go inside `FsChunkStore`**: it is deliberately runtime-agnostic (no `tokio`
  dependency; the workspace keeps the store-level async surface runtime-free) and is
  exercised **directly under `madsim`** (`crates/dst/tests/concurrency.rs:14,39`),
  where its determinism rests on those ops being synchronous with no `.await` inside —
  so injecting `spawn_blocking` there would both couple the leaf crate to tokio and
  perturb the DST interleaving. Instead, add a **composition-layer offload decorator**
  — a `ChunkStore` that wraps a `ChunkStore` and runs the inner (sync-bodied) op via
  `spawn_blocking` — wired **only into the production D-server path**
  (`crates/server/src/dserver.rs`), leaving `FsChunkStore` and the `madsim` DST path
  untouched (so: no wire/format change, and no DST-determinism impact). Ship with a
  concurrency/tail regression test that injects disk latency via the testkit `Disk`
  seam (adding a small latency injector alongside the existing `DiskWrite`/`DiskSync`
  fault points).
- **[adopt-now] Eliminate redundant userspace copies.** The fragment is copied several
  times per op: client `fragment.to_vec()`, server `Bytes::from`/`bytes.to_vec()`, the
  `fs::read`→`Vec`→`Bytes` round-trip, prost `Vec<u8>`↔`Bytes` at both wire boundaries
  (`crates/proto/proto/wyrd/v0/chunk.proto:25-41`), and `payload.to_vec()` in decode
  (`crates/chunk-format/src/codec.rs:64-154`). Configure prost to carry the fragment
  field as `bytes::Bytes`, pass refcounted `Bytes` end-to-end (the `clone()` at
  `write.rs:238` is already a refcount bump), and use vectored `writev`. Target
  zero-*re*-copy in userspace; true wire zero-copy is off the table (see Motivation).

### Tier 1 — `deploy/` OS & kernel tuning (the "custom Linux" answer)

The honest answer to "can custom Linux builds help": **yes, substantially — but the
high-ROI version is a recent stock kernel plus configuration, not a bespoke kernel.**
All of it is `deploy/` config (ADR-0010), travels with the image, and never touches
the DST tier.

- **[adopt-now] Recent mainline/LTS kernel (not bespoke).** A current LTS kernel ships
  io_uring, blk-mq, NVMe multiqueue + poll queues, kTLS (`CONFIG_TLS`), and BBR out of
  the box — every primitive below, without owning a kernel. Pin a known version per
  image.
- **[adopt-now] `tuned` `throughput-performance` profile** — the 80%-without-a-custom-kernel
  lever (governor=performance, server-class I/O scheduler, throughput sysctls) in one
  declarative, reversible profile. Not the `latency-performance` profiles — wrong trade
  for a throughput/redundancy store.
- **[measure-first→adopt] Block layer (the single biggest deploy lever):** a udev rule
  for `scheduler=none` (NVMe gains nothing from reordering; lowers median *and* tail
  latency), `rq_affinity=2` (complete I/O on the issuing CPU), and `fio`-validated
  `nr_requests`/`read_ahead_kb`.
- **[adopt-now] Filesystem:** XFS or ext4 + `noatime`. Avoid ZFS (write amplification,
  out-of-tree module, and its checksums are redundant with the store's crc32c).
- **[adopt-now] Network sysctls + NIC offloads:** socket buffers, `somaxconn`,
  `netdev_budget`, `vm.dirty_ratio`/`dirty_background_ratio`; TSO/GSO/GRO, RSS, aRFS,
  interrupt coalescing (tune under load); **jumbo frames (MTU 9000)** on a controlled
  storage fabric; **`TCP_NODELAY`** (a clear win for gRPC control frames). Keep CUBIC
  by default — BBR only if Tier-2 shows a high-BDP/lossy path (BBR's unfairness is a
  poor fit for a fabric shared with repair, §8.9).
- **[adopt-now] CPU/jitter:** governor=performance, disable deep C-states (~27% lower
  tail latency), disable or `madvise`-gate THP (avoids 50–100 ms compaction stalls on a
  sparse small-file store), NUMA locality (pin worker threads + memory + NVMe/NIC IRQs
  to the local node), IRQ affinity off the serving cores. (`isolcpus`/`nohz_full`/`rcu_nocbs`
  are **[measure-first]** — heavy, justify by a measured jitter target.)
- **[measure-first] cgroup v2 io/cpu isolation** (`io.weight`/`io.max`/`io.latency`,
  `cpu.weight`) — operationalizes "throughput is shared with repair; repair preempts
  near the durability floor" (§8.9) at the OS level **without** moving durability or
  placement out of the custodians (ADR-0010). Prefer weights over hard caps.
- **[adopt-now] Minimal/immutable OS image** as the reproducible production artifact
  (Flatcar or Buildroot for systemd prod; Talos only if prod commits to k8s;
  Yocto/Buildroot for NAS) — the single static binary drops in cleanly and the image
  bakes in the kernel version + tuned profile + sysctls + udev rules + boot params. Per
  ADR-0014 this is the **production multi-node** artifact; the NAS/dev profile stays a
  simpler binary drop.

### Tier 2 — mTLS + transport (forward-looking; sets the real baseline)

- **[adopt-now, separate workstream] Wire rustls-based mTLS on tonic** (ADR-0025):
  pure-Rust, fail-closed, no plaintext fallback; add the tonic `tls` feature + cert
  plumbing to `chunkstore-grpc`, a dev-CA path for the single-binary profile
  (ADR-0014), and keep the `--cfg madsim` build deterministic (tls cfg'd out/stubbed on
  the DST path, ADR-0009). Primarily a security item, but it sets the honest perf
  baseline — and the D server still only ever sees ciphertext (ADR-0021), so this only
  cheapens the mandatory transport crypto, it unlocks no plaintext tricks.
- **[measure-first] HTTP/2 connection pool** — a small per-core client-side pool to
  spread across RSS queues and bound TCP head-of-line blocking; a config knob, default
  conservative, raised only if Tier-2 shows single-connection RSS-pinning. (Cross-server
  HoL is already bounded by `k`-of-`n` fan-out.)

### Tier 3 — architecture & durability (correctness-sensitive → ADR + DST)

- **[adopt-now as ADR-0033] Durability = redundancy + crash-atomic rename, not
  per-write fsync.** The single biggest lever, and *already the default*; the
  deliverable is a proof and a bound, not a port — recorded as ADR-0033 (the
  correlated-power-loss soundness condition, the distinct-failure-domain↔power-domain
  alignment, the async-batched-`fdatasync` contingency, and the deferral of
  degraded-write). Do **not** add synchronous per-put fsync.
- **[adopt-now] Keep tokio.** Do **not** adopt a thread-per-core runtime
  (glommio/monoio): real wins for connection-dense networking, but it breaks the
  `madsim` DST seam (ADR-0009), is Linux-only (dents cross-compile/NAS portability),
  glommio is effectively unmaintained, and `k`-of-`n` fan-in is the skewed-load case
  shared-nothing handles worst.
- **[measure-first] io_uring as a NON-default backend behind the `Disk` seam** — the
  architecturally-correct way to add it (registered buffers, IOPOLL, SQPOLL). But
  off-the-shelf over libaio is marginal, the big numbers need a redesign, and `madsim`
  cannot simulate it — so it can only ever be a real-hardware backend exercised at
  Tier-2, and **only if Tier-2 proves NVMe I/O (not network/TLS/redundancy) is the
  binding constraint**, which for this store is unlikely to bind first.
- **[adopt-now] Read-path tail policy** behind the `Disk` seam: page cache + readahead
  for hot/re-read fragments, O_DIRECT for cold/scrub/streaming reads (avoids cache
  pollution), honoring the M3 repair-vs-serve reserved seat. `any`-`k` reads make a slow
  tail a non-event *unless* it becomes the common case (§6.2, §6.3).
- **[measure-first] Fragment packing** (Haystack/Tectonic/f4-style log-structured
  segments) — kills per-file inode/IOPS overhead for *small* fragments at high fan-out,
  but adds a segment format + id→offset index + compaction/GC + crash recovery (new
  correctness surface, DST burden) and is entangled with the chunk/stripe/inline-threshold
  sizing ADR-0019 deliberately deferred to measurement — so do **not** choose it before
  that sizing is measured. It is a backend detail behind the `ChunkStore` seam, like the
  one-file-per-fragment layout ADR-0032 records.

### Tier 4 — exotic (explicit verdicts)

- **[reserve] `mitigations=off`** — up to ~50% recovery on syscall-/IO-heavy paths,
  softened by the D server holding only ciphertext (ADR-0021) and reads being `k`-of-`n`.
  But the D server is a named compromise target in the threat model (§14), so this is
  defensible only on dedicated single-tenant prod hardware as an explicitly measured,
  threat-model-signed-off `deploy/` decision — never a default, never on dev/NAS.
- **[reserve] Bespoke custom kernel** — most wins (server preempt model, io_uring,
  kTLS, NVMe MQ) are already in stock LTS; owning a kernel re-introduces a per-target
  build/patch/reproduce burden against the portability posture (ADR-0008/0014/0019).
  Reserve for a dedicated, measured fleet where a feature is provably missing.
  **[avoid] PREEMPT_RT** — trades throughput for latency-determinism Wyrd does not need,
  and needs a custom kernel.
- **[measure-first, mostly avoid] kTLS** — would offload only symmetric crypto (no
  zero-copy here); the rustls `ktls` integration needs raw-fd ownership tonic holds and
  fights the `madsim` seam; pursue only if Tier-2 proves AEAD is a real CPU bottleneck.
  **[avoid] NIC inline TLS offload** as a design assumption (ConnectX-6 Dx/7-class
  SmartNICs are absent on commodity hardware; acceptable only as an opportunistic
  deploy bonus).
- **[avoid] SPDK** — DPDK C dependency + hugepages + device-unbind + no filesystem +
  burns full cores; would force re-implementing crash-atomic put / idempotent delete /
  exact list and re-spec'ing the on-disk format. io_uring already reaches ~80% of SPDK's
  ceiling with the kernel stack intact.
- **[avoid] DPDK / userspace TCP / RDMA (RoCE/InfiniBand)** — sit below or instead of
  the kernel TCP socket, so they lose gRPC + rustls mTLS (or force re-implementing TLS
  over a custom transport), need special HCAs/lossless fabric, and break
  portability/single-binary/DST. The D server is durability-, not latency-, dominated.
- **[avoid] ZNS zoned namespaces** — the mature path is the C++ RocksDB/ZenFS plugin
  (incompatible with the pure-Rust/single-binary posture, ADR-0008/0014/0019); a
  pure-Rust zone allocator would be a large new on-disk-format surface (ADR-0019).

### Verification posture

DST is the correctness authority and **never measures performance** (ADR-0009;
architecture §13). All performance is measured on real hardware: **Tier-2** (single
node, real fsync/NVMe) and **Tier-3** (cross-node, the Q6 scaling claim). The gate is a
benchmark harness — `fio` for raw NVMe baselines and an end-to-end PUT/GET
throughput + p50/p99/p999 benchmark over the real gRPC path, run **both** plaintext and
(once Tier 2 lands) mTLS, since TLS is the real baseline — added as a `cargo xtask`
perf job explicitly **outside** the deterministic CI tier. The decision rule for every
`measure-first`/`reserve` item: adopt only on a Tier-2/3 number showing the targeted
resource (not network/TLS/redundancy) is the binding constraint.

## Alternatives considered

- **Chase raw NVMe IOPS first (io_uring everywhere, SPDK):** rejected as the starting
  point — the binding constraint for a `k`-of-`n` store behind mTLS gRPC is
  network/TLS/redundancy, not device IOPS, so this optimizes a number Wyrd cannot
  spend, and it breaks the DST seam and portability.
- **Adopt a thread-per-core runtime for the whole server:** rejected — breaks the
  `madsim` correctness tier, is Linux-only, and handles `k`-of-`n` fan-in skew worst;
  keep tokio and fix the blocking-I/O bug instead.
- **Harden the store with synchronous per-put fsync:** rejected (ADR-0033) — it
  converts the central throughput advantage into the bottleneck; the contingency, if
  ever needed, is an asynchronous batched flush.
- **Pursue sendfile/splice zero-copy to the wire:** rejected — crc32c-on-read and
  per-frame mTLS both force the bytes through CPU; target zero-*re*-copy in userspace.
- **Build a bespoke kernel / immutable OS as the default:** the immutable image is
  adopted for prod, but a bespoke *kernel* is reserved — stock LTS already carries the
  needed primitives, and owning a kernel fights the portability posture.

## Graduation criteria

- The Tier-2/Tier-3 benchmark harness exists and runs outside the deterministic CI
  tier, capturing throughput + p50/p99/p999 before/after each change, for plaintext and
  mTLS.
- The Tier-0 code fixes land behind the `ChunkStore` seam with `cargo xtask ci` green on
  both backends and a concurrency/tail regression test (via the `Disk` seam) proving
  the blocking-I/O fix; the copy-elimination change shows a measured reduction in
  per-op allocations/copies.
- The Tier-1 `deploy/` bundle exists (kernel pin, tuned profile, udev/sysctl/IRQ/NUMA
  config, cgroup isolation, minimal image) and a Tier-2 run shows the block-layer +
  jitter knobs improve tail latency, with the binary coming up identically under
  systemd/compose/k8s (ADR-0010).
- ADR-0033's durability model is validated: DST crash/correlated-loss/reconstruct
  properties green and seed-reproducible, plus a Tier-2 kill-and-reconstruct on real
  NVMe.
- Each `measure-first`/`reserve` item is either adopted with a Tier-2/3 number
  justifying it or recorded as declined with the measurement that declined it.

## Backward compatibility

- **On-disk format:** unchanged — no Tier-0/1 item touches the fragment byte layout
  (ADR-0019) or the FsChunkStore directory layout (ADR-0032). Fragment packing, if ever
  pursued, is a separate backend-internal migration, not a `format_version` break.
- **Wire:** the prost `bytes::Bytes` change is a code-side codec choice, not a wire
  change; the gRPC contract is untouched and evolves under versioned protobuf with
  disciplined interface versioning (ADR-0002; by addition, never repurposing a field).
  Wiring
  mTLS is additive (the `tls` feature + certs), gated by a one-version-gap rollout.
- **Deployments:** the `deploy/` bundle is new and optional; the dev/NAS single-binary
  profile (ADR-0014) is unaffected and keeps coming up with no special tuning. The
  production multi-node profile gains the immutable image as its artifact.
- **Behaviour:** the durability model (ADR-0033) is *recorded*, not changed — the store
  already acks before fsync; the only behavioural change would be the optional
  async-batched flush contingency, which is additive and off the ack path.

## Open questions

- **Minimal-image choice** — Flatcar/Buildroot (systemd) vs Talos (k8s) vs Yocto (NAS)
  depends on the committed production substrate; decide when the prod profile firms up.
- **Block-layer values** — `nr_requests`/`read_ahead_kb` and queue depth are
  device-specific and need `fio` baselines per hardware class.
- **mTLS-under-`madsim`** — confirm the exact mechanism (cfg-out vs stub) by which the
  tonic `tls` feature stays deterministic on the `--cfg madsim` build (ADR-0009).
- **AEAD CPU cost** — measure on the target CPUs (AES-NI vs ChaCha20 on NAS) before
  deciding kTLS is ever worth its integration tax.
- **cgroup weights** — the serve-vs-repair `io`/`cpu` weights need tuning under
  concurrent serve+repair load to honor the §8.9 repair-preempts-near-floor rule
  without starving serving.
- **Fragment-packing trigger** — only revisit once the ADR-0019 chunk/stripe/inline-threshold
  sizing is measured and small-fragment IOPS is shown to dominate.

## Out of scope — routed elsewhere

- **Degraded-write / commit-below-`n`** — reverses the fail-closed admission invariant;
  a future slice gated by its own ADR (ADR-0033; §8.9).
- **The at-scale `ChunkStore` backend** (prefix fan-out / streaming enumeration / object
  store) — a separate future decision owed by ADR-0032, not this proposal.
- **Metadata-tier performance** — this proposal is the D server (the data tier); the
  TiKV metadata backend is M4 (proposal 0007).
- **The kernel-bypass tier as a build target** (SPDK/DPDK/RDMA) — recorded here as
  avoided, not pursued.

## Relationship to existing decisions

- **Introduces** ADR-0033 (fragment durability via redundancy) as the correctness-sensitive
  record of the no-fsync model this proposal's throughput rests on.
- **Builds on** ADR-0009 (DST as the correctness authority, behind the I/O seam),
  ADR-0010 (OS/deploy tuning is config, not code coupling), ADR-0025 (mandatory mTLS on
  the data path), ADR-0021 (the D server holds only ciphertext), ADR-0008/0014/0019
  (pure-Rust, single-binary, cross-compile, the on-disk format), and the M3 custodian
  work (the repair/scrub recovery loop and the distinct-failure-domain placement
  invariant).
- **Adjacent to** ADR-0032 (the FsChunkStore on-disk layout these writes land in; its
  noted O(N) enumeration and no-fan-out scaling characteristic is the at-scale-backend
  prerequisite, not this proposal's concern).
