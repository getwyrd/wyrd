---
created: 13.06.2026 12:16
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: TBD
tags:
  - proposal
  - milestone-0
  - implementation-plan
---
# Proposal: Milestone 0 — the walking skeleton (implementation plan)

> This is the bootstrap proposal: the implementation plan for the project's first vertical slice. It is filed as a proposal because the template fields map cleanly onto an implementation plan and the process is deliberately lightweight this early (see `../README.md`). It records *how* M0 is built; the *why* of the architecture lives in the ADRs it references.

## Motivation

Milestone 0 proves the **commit protocol — the entire differentiator** — end to end in a single process: one atomic write and one read, with the commit shown atomic under fault injection in simulation. Per the build strategy ([architecture §4.2][s4], [§9][s9]) this is a **vertical slice**, not bottom-up: the layers that matter for one real operation, wired thinly, then widened by risk in later milestones.

Building this slice first de-risks the project's central claim before any breadth (erasure coding, networked storage, custodians, multi-zone) is added, and it establishes the trait seams and the DST harness that everything after it attaches to.

## Design

### Scope boundary

In scope: S3 PUT/GET (minimal) → client library (chunk + commit) → embedded metadata store (redb) → filesystem chunk store → in-memory coordination, plus the `testkit` DST harness and the commit-protocol property tests.

Explicitly **out** of M0 (deferred to the risk-ordered widening in [§9][s9]): erasure coding (`replication(1)`/`none` only), networked gRPC D servers, custodians, TiKV, and every cross-zone layer (L2/L3). Their *hooks* are respected where retrofitting is expensive (see Backward compatibility).

### Workspace and crate scaffold (coarse start, [ADR-0016][a16])

A Cargo workspace, starting **coarse** and splitting later. Trait boundaries exist from day one even where crate boundaries do not. `Cargo.lock` is committed (this is an application).

| Crate | M0 contents | Notes |
|-------|-------------|-------|
| `chunk-format` | Fragment header encode/decode against [the spec][spec] | Dependency-light; spec-first ([ADR-0002][a2]) |
| `proto` | Minimal protobuf/prost message shapes (commit, chunk put/get) | gRPC *transport* deferred to widening step 2; shapes defined now |
| `traits` | `ChunkStore`, `MetadataStore`, `Coordination` definitions only | The keystone — consumers/impls depend here, never on concretes ([ADR-0010][a10]) |
| `core` | Client library, commit protocol, redb `MetadataStore`, filesystem `ChunkStore`, in-memory `Coordination` | Combined crate; split as boundaries firm up ([ADR-0016][a16]) |
| `testkit` | Abstract time/disk; deterministic seed-driven runner; fault-injection hooks | First-class, not a helper ([ADR-0009][a9]) |
| `server` | The binary; wires concretes; hosts minimal S3 PUT/GET | Only crate that knows concrete backends |
| `xtask` | Codegen, conformance-vector run | `cargo xtask <thing>`; no `make` ([ADR-0016][a16]) |

CI adds `cargo build/test/fmt/clippy`; the `adr-immutability` check already exists.

### Metadata model — redb behind `MetadataStore` ([§5 L4][s5])

Hierarchical **inode + dirent**, not path-as-key, so create writes inode + dirent atomically and rename is a single dirent mutation:

- `inode:<id>` → attributes, chunk map (or inline data for small files), state, version.
- `dirent:<parent_id>/<name>` → child inode id.
- `pending:<chunk_id>` → lease/expiry — the pending-chunk GC ledger.
- `meta:version` counter **reserved now** for the [ADR-0015][a15] consistency fence (not yet enforced).

The atomic commit is a **single redb write transaction** spanning these keys. That transaction *is* the commit point.

### Chunk / fragment format (`chunk-format` + [the spec][spec])

Implement the v1-draft header: magic, format version, EC-scheme id + fragment-index-in-stripe, chunk id, payload checksum (algorithm id), payload length. For M0 the EC scheme is `replication(1)`/`none` (no real EC yet), but the scheme id is recorded per chunk so later mixed-era data reads correctly. The `[TO BE SPECIFIED]` byte layout, endianness, checksum algorithm, and scheme ids are **decided during M0 and written back into [the spec][spec] in the same PR**; seed conformance vectors land in `specs/conformance/`. The format stays **v0 / unstable** — `v1` is stamped only after a second reader or a sustained fault-injection run validates it (per the spec's own rule).

### Write / commit protocol (the differentiator) — [§5][s5]

Implement all four phases:

1. **Intent** — client registers chunk ids in the pending ledger with a lease. The chunks exist nowhere in the namespace.
2. **Data path** — client writes fragment(s) to the filesystem `ChunkStore`, which verifies checksums. Failures here are harmless garbage.
3. **Commit** — one atomic redb transaction writes the chunk map, sets state `COMMITTED`, and bumps the version **conditional on the prior version**. Concurrent writers conflict here; exactly one wins. **This is the atomicity.**
4. **Release** — delete the ledger entries.

Crash between 3 and 4 leaves ledger entries for a sweep; crash before 3 leaves leased garbage. M0 has no custodians, so a **minimal ledger-sweep function** (invoked by tests, not a running service) stands in; the full custodian is a later milestone. Readers never consult the ledger — they see the old version or the new one, never a hybrid.

### Access (L1) and coordination (L5), minimal

- **S3 PUT/GET** in `server`: PUT object → client write path; GET → read path (single fragment, since `replication(1)`). Just enough for an end-to-end test; full S3 semantics are deferred.
- **In-memory `Coordination`** behind the trait: discovery / leader election / locks are trivial in one process, but the trait shape is fixed now so etcd drops in later as a composition change.

### DST harness + property tests — attach at M0 ([ADR-0009][a9])

`testkit` provides abstract time/disk and a single-threaded, seed-reproducible runner. The commit-protocol property tests assert the core invariants:

- Concurrent writers to the same inode: **exactly one commit wins**; the loser observes the version conflict.
- A reader sees either the pre-commit or post-commit version, **never a hybrid**.
- **Fault injection**: crash between phases 3 and 4 → the file is either fully visible or not at all, and no committed chunk-map entry references a fragment the sweep would reclaim.

## Alternatives considered

- **Bottom-up** (all D servers / EC first): rejected by [§4.2][s4] / [ADR-0009][a9] — a vertical slice proves the differentiator sooner and gives every layer a trivial-then-real path.
- **Fine-grained crates from day one**: rejected by [ADR-0016][a16] (premature Cargo plumbing and visibility friction); a combined `core` now, split later.
- **Networked gRPC D servers in M0**: deferred to widening step 2 — the in-process filesystem store proves the commit protocol with far less surface; the `ChunkStore` trait means the swap is composition, not refactor.
- **Full S3 compatibility in M0**: deferred — minimal PUT/GET is enough to drive the slice end to end.

## Graduation criteria (definition of done)

- A file written via S3 PUT is read back via GET **byte-identical**.
- The commit is **proven atomic under fault injection in simulation**, reproducible from a seed.
- Commit-protocol property tests are green; `fmt`/`clippy` clean; `Cargo.lock` committed.
- The chunk-format byte layout is recorded in [the spec][spec] with at least one conformance vector the reference reader accepts.

### Suggested PR sequence (each with its own definition of done)

1. Workspace scaffold + `traits` + `testkit` skeleton + CI.
2. `chunk-format` encode/decode + conformance vector + spec byte-layout filled in.
3. redb `MetadataStore` (inode / dirent / ledger) behind the trait.
4. Filesystem `ChunkStore` behind the trait.
5. Client library: chunk → write fragment → four-phase commit.
6. In-memory `Coordination`.
7. Minimal S3 PUT/GET in `server` + end-to-end write/read test.
8. DST commit-protocol property tests + crash-between-3-and-4 fault injection.

## Backward compatibility

- **On-disk format**: M0 fixes the chunk-format byte layout but it remains **v0 / unstable**; no stability promise until `v1` is stamped. No existing data to migrate.
- **Deferred-with-reserved-seats** honored now because retrofitting is expensive ([§9][s9]): append/CAS/watch primitives ([ADR-0007][a7]) accommodated by the ledger + schema shape; the `meta:version` fence counter reserved ([ADR-0015][a15]); trait seams for etcd/TiKV/openraft.
- **API / deployments**: none yet, so nothing to stay compatible with.

## Open questions

- Checksum algorithm for the fragment header (crc32c vs blake3) and exact field widths / endianness — **[TO BE SPECIFIED]** during step 2.
- Small-file **inline threshold** ([§5][s5] mentions inlining): implement in M0 or defer to the EC widening step?
- Does the minimal S3 surface stay in `server` for M0, or warrant a `gateway-s3` crate immediately?
- redb key encoding (byte order of `<id>`, dirent name normalization).

[s4]: ../../architecture/04-solution-strategy.md [s5]: ../../architecture/05-building-block-view.md [s9]: ../../architecture/09-build-order-and-roadmap.md [spec]: ../../specs/chunk-format/v1.md [a2]: ../../adr/0002-spec-first-on-disk-format-only.md [a7]: ../../adr/0007-reserve-append-cas-watch.md [a9]: ../../adr/0009-deterministic-simulation-testing.md [a10]: ../../adr/0010-pluggable-deployment-substrate.md [a15]: ../../adr/0015-consistency-contract.md [a16]: ../../adr/0016-monorepo-and-crate-structure.md
