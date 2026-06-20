---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - quality
  - risks
  - glossary
  - testing
---
# 10–13. Quality, risks, glossary, and testing strategy

> Living document. Combines arc42 sections 10 (quality scenarios), 11 (risks and technical debt), and 12 (glossary), plus Wyrd's testing strategy (section 13), trimmed to what earns its maintenance.

## 10. Quality scenarios

Concrete, measurable scenarios that operationalize the quality goals (section 1.3). Numbers are targets to be set during implementation; the *shape* is fixed.

| # | Scenario | Measure | Goal |
|---|----------|---------|------|
| Q1 | A D server is lost from a zone | All affected chunks return to full redundancy within time-to-repair budget T, with no read errors during repair (reads reconstruct from survivors) | 1 |
| Q2 | Silent bit rot corrupts a fragment | Scrubber detects it within scrub-cycle period P, before the data is needed; reconstruction follows | 1 |
| Q3 | A write is interrupted at any point | No torn state observable; either the old version or the new, never a hybrid; orphaned fragments collected by GC | 1 |
| Q4 | A zone is lost entirely | Under-replicated files re-replicated from survivors within budget; bounded by cross-region bandwidth | 1 |
| Q5 | Concurrent writers to the same file | Exactly one commit wins (version conflict); the other retries or fails cleanly | 1 |
| Q6 | Aggregate write throughput vs. cluster size | Scales close to linearly with D-server count, divided by EC amplification (~1.5× for RS(6,3)) | 2 |
| Q7 | Small-file (sub-threshold) workload | Dominated by metadata-op rate, not byte rate; served by the inline path; scales with the metadata tier | 2 |
| Q8 | Rolling upgrade across a version-skewed fleet | No downtime, no data errors, neighbors interoperate across one version gap (section 8.7) | 3 |
| Q9 | Operator drains a D server | Data evacuated while maintaining redundancy and failure-domain invariants throughout; resumable; observable progress | 3 |

These belong in CI as load/fault tests where feasible (Q1–Q3, Q5–Q7) and in the DR runbook drills otherwise (Q4, Q8, Q9). Goals 4 (scale range) and 5 (replaceability) are validated *structurally* — the same binary running every deployment profile (section 7) and backend swaps as composition tests — rather than by a load/fault scenario.

## 11. Risks and technical debt

| Risk | Nature | Mitigation |
|------|--------|------------|
| Correctness bugs in the client library and custodians | These are where most correctness risk concentrates (the thick client, the repair loops); bugs here are silent corruption, not crashes | DST from day one; Jepsen runs; conformance vectors; Rust's type system in exactly this code |
| Small-file performance | A Drive workload is millions of small files; without the inline path it hammers the metadata tier and never reaches data-path scaling | Inline-data path below a size threshold (parameter **[OPEN]**); metadata-tier sharding |
| Multi-language client divergence | A non-Rust client re-implementing the thick logic incorrectly breaks atomicity or readability | Spec + conformance suite; consider a Rust core with FFI bindings rather than reimplementation |
| Contributor on-ramp (Rust) | Smaller drive-by-contributor pool; steeper ramp | Excellent CONTRIBUTING; good-first-issues in non-hot-path crates; coarse-to-fine crate split so newcomers aren't lost in plumbing |
| Compile times on a large workspace | Slows iteration | Disciplined crate split (the trait boundaries are also compile-unit boundaries); start coarser, split as needed |
| Treating etcd as a database | The classic L5 failure mode; etcd is wonderful until used as a store | Architectural rule: nothing data-proportional in L5; sized in kilobytes |
| Spanner-class store operational weight | A full SQL store (TiDB, YugabyteDB) is heavy for L2 | Justified because L2 is small and off the data path; revisit only if it proves a bottleneck |
| Key management as a new failure domain | Encryption puts the KMS on the read/write path for encrypted tenants; a KMS outage or a lost KEK is catastrophic | Pluggable `KeyService`; bounded-lifetime DEK caching; fail closed on KMS loss without losing ciphertext durability; KEK backups independent of the system (ADR-0021) |

### Conscious exclusions (not debt — decisions)

- Cross-provider / untrusted federation (ADR-0005) — reversible, with stated cost.
- Full POSIX semantics over EC storage (FUSE is second-class).
- Single-binary as a production tier (ADR-0014).
- Application-level logic (collaborative-editing merge / OT / CRDT) — only the storage primitives are in scope, with append / CAS / watch reserved (ADR-0007).
- The observability storage / visualization stack — shipped as a reference, never a dependency the binary is aware of (ADR-0012).

## 12. Glossary

| Term | Meaning |
|------|---------|
| Commit point | The single linearizable metadata mutation that makes a write visible. The locus of the atomicity guarantee. |
| Zone | One datacenter/region's complete L4+L5 stack. The unit of atomicity and intra-provider federation. |
| Control plane | L2 specifically — the global namespace, placement, and zone registry (section 5). Distinct from the *management surface* (the operator API, ADR-0013), which is occasionally also called "control". |
| D server | A "dumb" storage server: stores and returns fragments by chunk ID, verifies checksums; no placement or metadata logic. |
| Custodian | A background maintenance service: GC, scrub, reconstruction, rebalancing (L4) or cross-zone re-replication (L3). |
| Chunk | The unit of placement and erasure coding; a file is split into chunks. |
| Fragment | One erasure-coded piece of a chunk; *n* fragments produced, any *k* reconstruct. |
| Inode / dirent | Metadata records: the inode is "what this thing is" (+ chunk map); the dirent is "what it is called and where it lives in the tree". |
| Inline data | Small-file optimization: data stored directly in the metadata record, skipping chunking and EC. |
| Home zone | The authoritative zone for a file; where its writes linearize. |
| Pending-chunk ledger | The GC table tracking leased, not-yet-committed chunks so failed writes become collectable garbage. |
| DST | Deterministic simulation testing: running the whole system in a simulated world where every bug reproduces from a seed. |
| EC | Erasure coding (Reed-Solomon). RS(k,m): *k* data + *m* parity fragments. |
| Wyrd | The project, and the name for the system's total woven state: committed + in-flight + owed, seen as one consistent whole. *The Norns weave the Wyrd.* See ADR-0017. |
| Urth | The Norn of "what has become"; names the durable committed record (metadata store + on-disk truth). |
| Verdandi | The Norn of "what is becoming"; names the write/commit path (client library + commit protocol). |
| Skuld | The Norn of "what is owed / what shall be"; names pending work and reconciliation (replication queue, repair backlog, unsatisfied policy). |
| The Norns | Collective label for the three conceptually load-bearing components (Urth, Verdandi, Skuld). Lower-level mechanical parts keep plain descriptive names. |

## 13. Testing strategy

> How Wyrd is verified, across tiers from reproducible simulation to real multi-region hardware. This subsection describes the *approach*; the DST decision itself is ADR-0009, the quality scenarios it must satisfy are above (section 10), and the milestone ordering that each tier attaches to is in section 9 and proposal 0002 (the implementation arc).

### 13.1 The principle: simulation is primary, real environments are complementary

Correctness is the project's reason to exist, so the **primary** verification mechanism is **deterministic simulation testing** (DST, ADR-0009): the whole zone — metadata, D servers, custodians, faults, clock skew — run single-threaded in a simulated world where every bug reproduces from a seed. DST owns the things that matter most and that real hardware tests *badly*: atomicity under fault injection, race conditions, partition handling, commit-protocol invariants. These are verified reproducibly, in CI, in seconds.

Real environments are the **second line**, and they exist to catch precisely what simulation structurally cannot:

- **Real performance** — actual latency, real EC encode/decode throughput on real CPUs, real disk and network throughput, tail latencies, and the linear-throughput-scaling claim (scenario Q6).
- **Real I/O and OS behaviour** — honest fsync semantics, real filesystem quirks, io_uring, page cache, the real network stack under load.
- **Unmodeled faults** — the failures we did *not* think to inject: a disk failing slowly rather than cleanly, a NIC corrupting rather than dropping, an OOM-killer intervening, clock skew weirder than the model.
- **Real integration** — actual etcd, actual TiKV, actual gRPC over a real network, behaving as they really do rather than as their in-memory fakes do.

A real environment is therefore never used to test correctness the simulation already covers. If validating the commit protocol's atomicity seems to need a real cluster, that is a signal something is wrong — that is DST's job.

**The compounding loop.** Every real-world discovery is promoted back into DST: a fault or workload shape found on real hardware is encoded as a new seed-driven DST scenario, becoming a permanent, free, reproducible regression test. The real tiers are *fault-discovery* tools; their findings become cheap simulation tests. This is the highest-leverage idea in the strategy — it turns expensive, one-off real-world findings into permanent CI coverage.

### 13.2 The tiers

The tiers escalate in realism and cost. Each catches what the one below cannot; none replaces DST as the correctness authority. They attach to the implementation arc (proposal 0002) at the milestone where they first add value, so test investment stays matched to the milestone that needs it.

#### Tier 0 — Deterministic simulation (from M0, always)

`testkit` and the commit-protocol property tests (ADR-0009). The correctness authority. Runs in CI from the first milestone and grows with every subsequent one. Everything below is complementary to this, never a substitute.

#### Tier 1 — Local software-defined distributed testing (from M2)

A single powerful workstation running a logical cluster of many processes — multiple D servers, a metadata store, coordination — as real separate processes over real gRPC, with software fault injection. This is the workhorse tier and covers most day-to-day distributed testing at near-zero marginal cost:

- **Many D-server processes on one host** — exercises control-plane, placement, custodian, and repair logic at higher logical node counts than physical hardware would afford.
- **Network fault injection** — `tc netem` (latency, jitter, loss, reordering, corruption), `iptables`/`nftables` partitioning (real split-brain between specific node pairs), cgroup throttling (the *slow-not-dead* node, the nastiest real fault and the one clean kills miss).
- **Disk fault injection** — device-mapper targets (`dm-flakey`, `dm-error`) and FUSE fault injectors to make a virtual block device intermittently fail, corrupt, or drop writes — testing the scrubber and checksum-verification paths against real block-layer misbehaviour, deterministically.
- **Clock faults** — `libfaketime` and deliberate NTP manipulation to inject skew, drift, and jumps, testing failover and monotonic-read survival under the condition that breaks naive implementations.
- **Consistency testing — Jepsen** — Jepsen against a handful of local containers, with its nemesis injecting partitions, clock skew, and pauses, validates the consistency contract (ADR-0015) under real (not modelled) partitions. This is the real-cluster complement to Tier 0 for consistency specifically, and a clean public Jepsen result is itself a credibility artifact, like the conformance vectors. Begins as soon as there is a networked path (section 9, M2–M3).

The reference host for this tier is a single beefy workstation (see 13.3). Its EC-performance numbers are *honest* because the silicon is real (server-class cores with SIMD, real NVMe) — but it is one machine, so the one thing it cannot validate is throughput scaling *across independent nodes* (Q6), which needs Tier 3.

#### Tier 2 — First real-world hardware experience (from M2–M3)

A single real, owned machine used as the primary local distributed-test host and the first "does this deploy and operate as claimed on real silicon" environment: real fsync, real NVMe latency, real SIMD-accelerated erasure coding, real OS behaviour. It validates honest single-node performance and real I/O semantics that the in-memory fakes abstract away, and it is the day-to-day host for the Tier-1 software-defined testing above. It is a *single failure domain*, so it proves real-silicon behaviour and single-node performance, not failure-domain independence (that is Tier 3).

#### Tier 3 — Real multi-node, multi-region (from M5)

Rented, on-demand, separate machines in separate real failure domains and — for the cross-zone milestones — separate real regions, so the two things one machine structurally cannot provide become real rather than simulated:

- **Real failure-domain independence** — genuinely separate power, network, and kernels, for true yank-a-node / sever-a-link / kill-a-disk physical-fault testing across independent hardware.
- **Real inter-region WAN latency** — for the cross-zone replication (L3) and the home-zone-authority consistency contract (ADR-0015) under real failover, which a rig on one desk (all nodes on one LAN) cannot produce convincingly.
- **Throughput scaling across nodes** — the Q6 linear-scaling claim, the one performance claim that genuinely requires multiple independent nodes.

This tier is rented on demand for specific test campaigns (a weekend of benchmarking, a cross-region consistency run) and torn down after, so it has no idle cost. It is the natural place for a prospective operational partner's own infrastructure to enter — testing on a real provider's hardware is both more realistic and the kind of engagement that turns a partner into an adopter.

### 13.3 Reference environments

The tiers above are deliberately tool- and vendor-neutral; what follows are the concrete environments the project uses, recorded so the strategy is reproducible. They are reference choices, not requirements — the strategy stands on any equivalent hardware.

| Tier | Reference environment | Role | Cost shape |
|------|----------------------|------|------------|
| 0 | CI runners | DST, property tests, conformance | free, always on |
| 1 | The Tier-2 workstation | Software-defined faults, Jepsen, logical-scale | free marginal use |
| 2 | A single high-core workstation with real NVMe and SIMD (e.g. a 16-core Zen 5 / Strix Halo-class mini-PC with PCIe-4 NVMe) | Primary local test host; honest single-node performance and real I/O | owned; justified independently of Wyrd, used here as free marginal capacity |
| 3 | On-demand EU bare-metal and cloud VMs across real regions (e.g. Hetzner Falkenstein / Nuremberg / Helsinki) | Failure-domain independence; real inter-region WAN; cross-node scaling | rented per campaign, torn down after |

Two notes on the reference choices:

- **The single workstation is free marginal capacity, not a Wyrd purchase.** Its value to storage testing is the 16 real cores (SIMD-accelerated EC) and the real NVMe (honest fsync); its AI-oriented features (large unified-memory bandwidth, iGPU, NPU) are irrelevant to storage and should not be counted as test value. It earns its place as a capable local host, not as a rig bought for the purpose.
- **Sovereignty alignment.** Tier 3 on EU bare-metal (Hetzner, Scaleway, OVH, or a Sovereign Cloud Stack provider) keeps the entire performance-and-WAN test story on sovereign infrastructure — on-message for the project's constraints, and cheaper than the US hyperscalers besides. Using a US hyperscaler for the multi-region rig would be a (minor) sovereignty compromise worth noting if ever chosen.

### 13.4 Mapping to the arc

| Milestone (proposal 0002) | Tiers active |
|---------------------------|--------------|
| M0 — walking skeleton | Tier 0 |
| M1 — erasure coding | Tier 0; Tier 1/2 for first honest EC benchmarks |
| M2 — networked D servers | Tier 0; Tier 1 (software faults), Tier 2 (real host); Jepsen begins |
| M3 — custodians | Tier 0–2; Jepsen consistency; disk-fault injection for scrub/repair |
| M4 — production metadata backend | Tier 0–2 against real TiKV |
| M5 — cross-zone replication | + Tier 3 (real multi-region WAN) |
| M6 — global control plane | Tier 0–3; cross-region consistency contract |
| M7 — failover & DR, drilled | Tier 0–3; real zone-loss on independent hardware |

The escalation is deliberate: the expensive Tier-3 rig is not stood up until the cross-zone milestones genuinely require real WAN and real failure-domain independence, and even then only on demand.
