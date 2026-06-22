---
created: 23.06.2026 00:10
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#201"
tags:
  - proposal
  - milestone-4
  - implementation-plan
  - metadata-backend
  - tikv
---
# Proposal: Milestone 4 — production metadata backend (TiKV) (implementation plan)

> The implementation plan for the fourth — and **release** — step of the
> [implementation arc][p2] (proposal 0002). [Proposals 0001–0005][p5] built a
> single-zone object store that is erasure-coded ([0003][p3]), networked
> ([0004][p4]), and self-maintaining ([0005][p5]) — but it runs entirely on the
> **embedded redb** metadata backend, which by decision carries **no production
> durability promise** ([ADR-0014][a14]: redb is "for development and evaluation
> only"). M4 swaps that backend for **distributed TiKV behind the *unchanged*
> `MetadataStore` trait**, and in doing so **proves the central pluggability
> claim**: that the trait abstraction did not leak — that a real distributed
> metadata store is a **composition change in `server`, not a refactor**
> ([ADR-0008][a8]; arc M4). It records *how* M4 is built; the *why* of the
> pluggable-backend design lives in the architecture and the ADRs it references
> ([§5][s5], [ADR-0008][a8], [ADR-0010][a10], [ADR-0016][a16]). M4 is
> **implementation-first behind an already-Accepted ADR** ([ADR-0002][a2]): the
> TiKV backend and its wire/transaction mapping are discovered by building them —
> **no new spec, and — unlike M3 — no ADR ratification, is required** (all four
> load-bearing ADRs, 0008/0010/0014/0016, are already Accepted; M4 is the first
> *implementation* of ADR-0008's TiKV half, not a status flip).

## Motivation

M4 proves that **pluggability is real — the `MetadataStore` trait survives
swapping embedded redb for distributed TiKV under load** ([§9][s9]; arc M4). M0
retired atomic-commit-across-paths; M1 retired the coding math; M2 retired the
networking; M3 retired the background-repair correctness risk. Each of those was
de-risked **on the embedded backend**, where atomicity is cheap because the store
is **single-process**: redb gives multi-key atomicity "directly … because redb
serializes write transactions" (`crates/metadata-redb/src/lib.rs:6-10`). M4 opens
the last load-bearing Step-2 risk: that this cheapness was **load-bearing** — that
the system's correctness quietly depended on an embedded store's serialized
transactions, and that a genuinely distributed backend (concurrent, contended,
partitionable) would require a **refactor of the consumers** rather than a new
implementation behind the seam.

The arc's ordering principle is **risk retired, not features delivered**. M4's
risk is exactly one proposition, and it is falsifiable:

- **The seam holds, or it does not.** The whole point of choosing TiKV over a
  single-row-atomic store (HBase) was **native multi-key transactions**, so the
  inode + dirent atomic create needs **no intent-log gymnastics** — "the exact
  contortion Tectonic's authors describe regretting" ([ADR-0008][a8]). If the
  `MetadataStore` trait leaked, the property that distinguishes Wyrd from
  Tectonic/HBase fails **precisely here**, at the first real distributed commit.
  M4 is the empirical test of [ADR-0008][a8]'s own stated consequence: *"backend
  choice becomes a composition concern in `server`, not a refactor."*

M4 is the **★ Step-2 release point** (arc): the result — a self-hostable,
EC-efficient, atomically-consistent single-zone object store (and, after M3/M4,
**self-maintaining** and **production-metadata-durable**) — is the **first
genuinely useful product**, worth
announcing and deploying even if Step 3 never follows ([p2][p2]). That status has
two consequences the proposal honors throughout: (1) **production durability for
metadata begins here** — redb stays the dev/eval backend, TiKV becomes the
production backend ([ADR-0014][a14]); and (2) **backward-compatibility obligations
begin here** — before M4 there was "no public API/deployments … nothing to stay
compatible with" ([p2][p2]); at M4 the metadata trait, the deployment surface, and
(on its own trigger) the on-disk format acquire stability duties.

A second, quieter motivation: M4 is the **second implementation** of the
`MetadataStore` trait. Wyrd's discipline is that a trait's semantics are "pinned
by two implementations" before an embedded backend is trusted under DST
([ADR-0006][a6]; echoed for the namespace store in [ADR-0020][a20]). redb was the
first; TiKV is the second. So M4 does not merely *use* the trait — it **pins and
hardens** it, surfacing any semantics that were accidentally redb-shaped.

## Design

### Scope boundary

**In scope** — exactly what retires the pluggability risk and makes the metadata
layer production-durable:

- A new **`metadata-tikv` crate** — a sibling of `metadata-redb`, depending **only
  on `traits`** (plus `tikv-client` and `tokio`), implementing `MetadataStore`
  over TiKV's **transactional** API. The naming convention (`metadata-<backend>`)
  and the dependency rule ("implementations … depend on the `traits` crate, never
  on each other's concretes," [ADR-0016][a16]) are already established.
- **The atomic conditional commit** — mapping `WriteBatch { preconditions, puts,
  deletes }` onto **one TiKV transaction** that checks every precondition and
  applies every put/delete **all-or-nothing**, returning `CommitOutcome::Conflict`
  (an `Ok`, not an `Err`) on a failed precondition and reserving `Err` for genuine
  backend faults (`crates/traits/src/lib.rs:182-207`). This is the heart of M4.
- **Native prefix scan** — replacing redb's whole-table-filter shortcut
  (`metadata-redb/src/lib.rs:58-70`) with a TiKV **range scan** `[prefix,
  prefix_upper)`, paged internally and materialized into the trait's
  `Vec<(Vec<u8>, Bytes)>` (order stays unspecified, [§traits][s5]).
- **Backend selection in `server`** — parameterizing the metadata-handling
  helpers in `crates/server/src/cli.rs` (today hard-coded to `RedbMetadataStore`)
  over `M: MetadataStore`, and adding the **redb | tikv** selector by config. This
  is the **composition change** the milestone exists to demonstrate, confined to
  the one crate [ADR-0016][a16] designates as the place concretes are wired.
- **The single-zone production deployment** — TiKV (small) + its PD cluster
  (TiKV's own coordinator) + a 3-node etcd ensemble for L5 Coordination +
  local-disk D servers ([§7.1][s7]), brought up from **`deploy/`
  artifacts outside the Rust workspace** ([ADR-0010][a10]), with peers discovered
  through **L5 coordination** and **no crate coupling to any orchestrator API**.
- **The consistency-contract demonstration** — proving the **single-zone subset**
  of the five-clause contract ([ADR-0015][a15], [§8.1][s8]) holds under the
  distributed store: a file's writes **linearizable at the commit point** (clause
  2), and **multi-key atomic directory operations** (create / rename / delete)
  all-or-nothing under real contention — the verbatim M4 definition of done.
- **The test surface that "under load" demands** — **Tier-0 DST stays on the
  deterministic backend** (it never moves to TiKV — that would violate
  [ADR-0009][a9]); the TiKV backend is validated at **Tier-1** (real cluster under
  software-defined faults + a **Jepsen** consistency campaign) and **Tier-2**
  (single real machine, real fsync/NVMe), per [§13.4][s10]'s "M4 — Tier 0–2
  against real TiKV." Every real-world discovery is promoted back to a **seeded
  DST regression** where the trait exposes it ([ADR-0009][a9]).
- **Carrying the reserved seats forward intact** — the `meta:version` consistency
  fence ([ADR-0015][a15] Option C) and the append/CAS/watch hooks ([ADR-0007][a7])
  must remain expressible on TiKV; M4 does **not build** them, but must not
  **foreclose** them.

**Out of scope** — deferred to the milestone that actually retires its risk, the
seats kept open where retrofit is expensive:

- **The global namespace store (L2) and cross-region linearizability** —
  [ADR-0020][a20] (NamespaceStore, default TiDB) is **status: Proposed** and is an
  **M6** concern. M4 is **strictly the zonal L4 `MetadataStore`** ([ADR-0008][a8]),
  linearizable **within one zone**. In a single-zone deployment the namespace
  shares the same metadata store as L4 — [ADR-0020][a20]:32 pins it to "the same
  embedded redb as L4 in the single-binary and small profiles," with TiDB
  appearing only at the provider-fleet tier — so global linearizability (clause 1)
  **collapses into** zonal linearizability and M4 neither builds nor ratifies the
  L2 split. `NamespaceStore` has **zero references in `crates/`** today; M4 scopes
  to `MetadataStore` and flags NamespaceStore-on-the-production-backend separately
  (see Open questions).
- **Cross-zone replication, replication-lag-bounded reads, sync-N-zone opt-in**
  (consistency clauses 4–5) → **M5** (L3). There is **no second zone** at M4 to
  measure lag against or replicate to; deferring is explicit, not silent.
- **The version-high-water-mark *failover* behavior** ([§8][s8]; [ADR-0015][a15])
  → **M7**. M4 **preserves the mechanism** (the per-file version is bumped in the
  atomic commit) but does not exercise it under zone loss.
- **`RocksDB` as a third backend** — [ADR-0008][a8] keeps it "a fallback behind the
  trait," not a deliverable. M4 builds **only** the TiKV backend.
- **The per-zone chunk durability scheme** (none / replication(n) / rs(k,m)) —
  [ADR-0008][a8] bundles this with the metadata-backend decision, but it was
  exercised by M1 (EC) and M3 (custodians). **M4 owns only the metadata-backend
  half**; it re-opens no durability-scheme decision.
- **`watch` / change-feed** ([ADR-0031][a31], Proposed) — M4 keeps the seat
  reservable, builds nothing.
- **A new paginated/streaming `scan` method** — would be a **trait change**, out of
  the "unchanged trait" requirement (see Open questions).

### What carries over from M0–M3, unchanged

M4 adds a *backend behind the seam*; it touches **neither the trait nor the
consumers above it**. The audit (below) confirms the following carry over
verbatim:

- **The `MetadataStore` trait** — exactly three async methods, `get` / `scan` /
  `commit(WriteBatch)`, no associated types, object-safe, `Send + Sync`
  (`crates/traits/src/lib.rs:173-187`). Its doc-comment already names the M4 swap
  as the motivating example: filesystem semantics live "*through* this primitive
  by the metadata model in `core`, never baked into the trait … makes a backend
  swap (redb → TiKV) a composition change" (`lib.rs:163-172`). M4 must keep this
  trait **byte-for-byte unchanged**.
- **The metadata model in `core`** — `inode:<id>` / `dirent:<parent>/<name>` /
  `pending:<chunk_id>` / `meta:version`, all opaque-bytes keys with JSON values,
  and the **per-inode version CAS** expressed as a **full-value precondition**:
  `commit_chunk_map` does `require(inode_key, encode(prior)).put(inode_key,
  encode(next))` with `version = prior.version + 1`
  (`crates/core/src/metadata.rs`). TiKV stores these bytes verbatim; it never
  interprets a key or value.
- **The commit point** — one `MetadataStore::commit` of a version-conditional
  `WriteBatch` ([§5][s5], [§6.1][s6]); write, repair ([§6.3][s6]), and delete
  ([§6.7][s6]) all reuse it. M4 changes **where** that commit lands, not **what**
  it means.
- **Placement records (M3)** — embedded in the inode value
  (`ChunkRef.placement: Vec<DServerId>`, `#[serde(default)]`,
  `core/src/metadata.rs`), **not** a separate keyspace. They ride inside the opaque
  inode JSON TiKV already stores; M4 adds **no keyspace** and gets pre-M3 backward
  decode for free — provided it never re-serializes the value (which would break
  the byte-exact CAS — see below).
- **The custodian plane (M3)** — GC, scrub, reconstruction, rebalance all operate
  over `&dyn MetadataStore` / `&impl MetadataStore` and carry **zero redb
  dependency** (`crates/custodian/`). M3 was deliberately built to leave the trait
  unchanged so "the M4 swap stays a composition change" ([0005][p5]); M4 confirms
  the custodians' atomic location updates hold on TiKV.
- **Coordination (L5) and fencing** — `elect_leader` / `Leadership` /
  `FencingToken` are a **separate trait and seam** (`traits/src/lib.rs:258-337`).
  M4 swaps the **metadata** backend only; Coordination is untouched.
- **The EC engine, the gRPC `ChunkStore`, and the any-*k* read path** ([0003][p3],
  [0004][p4]) — none cross the `MetadataStore` seam; all unchanged.

### The `MetadataStore` contract M4 must honor verbatim

The contract is small and the whole milestone rests on reproducing it exactly. In
full (`crates/traits/src/lib.rs:173-219`):

```rust
async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;          // point read; None = absent
async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>>; // prefix scan; order UNSPECIFIED
async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome>;   // the atomic commit point

// WriteBatch { preconditions: Vec<Precondition>, puts: Vec<(K,V)>, deletes: Vec<K> }
// Precondition { key, expected: Option<Bytes> }   // Some = require exact value; None = require absent
// CommitOutcome { Committed, Conflict }            // Conflict is Ok, NOT Err
```

Four properties are load-bearing and each is a porting obligation:

1. **All-or-nothing across keys.** "Either every precondition holds and every
   put/delete lands, or nothing changes" (`lib.rs:182-186`). Preconditions and
   mutations must execute in **one** TiKV transaction spanning all keys — never
   per-key operations or separate raw calls.
2. **CAS is value-equality, not an engine timestamp.** `Precondition.expected`
   is `Some(bytes)` (exact match) or `None` (require-absent). The "version" is an
   **application-level counter inside the JSON value**, so CAS is "the whole prior
   record's bytes equal `expected`," checked by **reading the key inside the
   txn**. Any TiKV-side value normalization (re-serialization, recompression) that
   changes a byte would silently turn **every** CAS into a spurious `Conflict`; M4
   must store values **byte-identically**.
3. **`Conflict` is an `Ok`, distinct from `Err`.** A failed precondition is
   `Ok(CommitOutcome::Conflict)` "so a stale writer is rejected distinguishably
   from a backend fault" (`lib.rs:184-186`). There is **no** `Conflict` error
   variant — the trait error channel is a boxed `BoxError` reserved for faults
   (`lib.rs:56-61`). This is the one subtle behavioral contract beyond raw KV, and
   the most error-prone part of the port (see the conflict-classification rule
   below).
4. **`scan` order is unspecified, results are materialized.** Callers must not —
   and, per the audit, **do not** — rely on order. M4 may return TiKV's naturally
   key-ordered range, but must page through it and collect into the owned `Vec`.

### Mapping the contract onto TiKV (the transactional translation)

TiKV's transactional API supports the contract cleanly, with **one mandatory
implementation rule** and **one semantic translation**. (Verified against
tikv.org and the `tikv-client` Rust crate — `tikv/client-rust`, current 0.4.x;
pin the exact version in `Cargo.toml` and reconfirm the locking-read and
write-conflict signatures at build time — see Open questions.)

- **The primitive.** `TransactionClient::begin_optimistic()` /
  `begin_pessimistic()` returns a `Transaction` exposing `get`, `get_for_update`
  (a **locking** read), `scan`, `key_exists`, `put`, `insert`, `delete`,
  `commit`, `rollback`. The model is **Google Percolator** two-phase commit
  (prewrite-locks-then-commit), coordinated by the **PD timestamp oracle**, giving
  **snapshot isolation**; the commit point's clause-2 linearizability rests on the
  **locking-read rule below** (read-only precondition keys taken with
  `get_for_update`), not on SI alone. Transactions span **multiple keys across
  multiple Raft regions natively** — exactly the property [ADR-0008][a8] cited.
- **The mapping.** `commit(WriteBatch)` becomes: *begin txn → for each
  precondition, read the key **inside** the txn and byte-compare against `expected`
  (or assert absence for `None`); on any mismatch, **roll back** and return
  `Ok(Conflict)` with zero side effects; otherwise issue all `put`/`delete`s and
  **2PC-commit**.* Multi-key preconditions and multi-key mutations all land in the
  one transaction.
- **The mandatory rule — lock read-only precondition keys.** TiKV snapshot
  isolation detects conflicts only on keys a transaction **writes**; a key it only
  **reads** is not conflict-checked (classic write-skew). A `Precondition` whose
  key is **also** in `puts`/`deletes` (e.g. `commit_chunk_map`'s `require`+`put`
  on the same `inode_key`) is safe even in optimistic mode. But a precondition on
  a key the batch **only reads** — possible in the general `WriteBatch`, and in the
  **`rename`** pattern (the `get` happens *outside* the txn; correctness rests
  entirely on the commit's `require` re-pinning that value atomically) — is **not**
  protected by optimistic SI alone. M4 must therefore read **every** precondition
  key with **`get_for_update`** (a locking read), and SHOULD default to
  **`begin_pessimistic`**, trading some throughput for the atomicity the trait
  demands. Benchmark the pessimistic-locking cost against expected metadata
  contention.
- **The semantic translation — two conflict signals, one outcome, faults stay
  faults.** Both (a) an M4-detected precondition mismatch and (b) a **TiKV
  write-write race** (surfaced as `KeyError::WriteConflict` from `commit`) must
  fold into `Ok(CommitOutcome::Conflict)` — a lost race **is** the trait's
  "conflict." A write-conflict must **not** be propagated as `Err` and must **not**
  be blindly retried at this layer. Everything else — network errors,
  region-unavailable, PD timeouts, lock-resolution/deadlock — is a genuine fault
  and stays in `Err(BoxError)`. Getting this partition exactly right is the
  single most delicate piece of the port.
- **Do not reach for the raw API.** `RawClient::compare_and_swap` is **single-key
  only** and cannot be mixed with the transactional API on the same database. The
  multi-key version-CAS **must** be the transactional read-check-write above.

### Composition, not refactor — the thesis, with the honest count

The milestone's claim is that the swap is "a `server`-crate composition change,
not a refactor" (arc M4). An audit of every `MetadataStore` consumer
(`crates/core`, `crates/custodian`, `crates/server`) **confirms the thesis** — and
states its honest size rather than overselling it:

- **`core` carries zero production redb dependency.** Its metadata model, write
  path, read path, and repair queue are generic over `&impl/&dyn MetadataStore`
  and import only `wyrd_traits`. `wyrd-metadata-redb` appears in `core/Cargo.toml`
  **only under `[dev-dependencies]`** (one restart-regression test). No change for
  M4.
- **`custodian` carries zero redb dependency**, production or dev
  (`custodian/Cargo.toml`). The L4 maintenance plane needs **no change** for TiKV.
- **The `Gateway` is generic** — `Gateway<M, C, Co> where M: MetadataStore`,
  holding `meta: M` by value, handling results purely through the `CommitOutcome`
  enum (`server/src/lib.rs`). Backend selection is "pass a different concrete," not
  a `dyn`/feature/config reach-through.
- **No consumer relies on redb's read-snapshot isolation across calls**, and **no
  consumer relies on `scan` order** — the "never a hybrid version" read guarantee
  rests on a **single atomic inode `get`** plus chunk immutability, not on a
  read-transaction spanning multiple calls (`core/src/read.rs`); every `scan`
  caller collects into a set/map. **No direct redb API** (`Database`,
  `begin_read/write`, `TableDefinition`) appears anywhere outside
  `metadata-redb/src/lib.rs:16`.

The **entire** production redb coupling is therefore in **`crates/server/src/cli.rs`** —
the one crate ADR-0016 designates for wiring concretes — and it is three concrete
things, all addressable inside M4:

1. **~8 sites construct/​type `RedbMetadataStore` concretely** (the disk-open and
   helper signatures; ~9 references counting the import). M4 parameterizes these
   over `M: MetadataStore` and adds the selector. "One-line switch" would be
   **overstated**: it is ~8 sites in one file, plus building the selection
   mechanism that **does not exist today** (no feature flag, config key, or enum
   dispatch chooses the backend).
2. **The async-runtime wiring.** redb's methods are async-signature but
   **synchronous-bodied**; the local CLI paths drive them with `pollster::block_on`.
   A `tokio`-bound TiKV client cannot run under `pollster::block_on` for real
   async I/O, so the local paths must move onto the `tokio` runtime the cluster
   paths already use. Confined to `cli.rs`; the trait is **already async**, so no
   signatures ripple into `core`/`custodian`.
3. **`alloc_inode`'s unbounded retry-on-`Conflict` loop** (`cli.rs:352-368`). Over
   embedded redb a conflicting commit is a sub-microsecond local transaction, so a
   busy spin is harmless; over distributed TiKV **every iteration is a network
   round-trip**, so the backoff-free spin is a latency/load footgun. M4 gives it
   **bounded retries with backoff**. (Note: this is dev-CLI inode allocation; the
   deployable `Gateway` uses an in-process `AtomicU64` and has **no** such loop,
   and `core`'s commit callers **propagate** `Conflict` rather than retrying — so
   no library code assumes cheap commits.)

None of these is a cross-cutting refactor of consumer logic. **The real
engineering weight — the atomic multi-key precondition CAS — lands in the new
`metadata-tikv` crate**, behind the seam, exactly as the thesis predicts.

### Multi-key atomic directory operations under the distributed store

The verbatim M4 definition of done is that "**multi-key atomic directory
operations hold under the distributed store**" (arc M4). The inode + dirent model
makes these concrete ([§5][s5]):

- **create** = atomic `{ put inode + put dirent }`, guarded by `require_absent` on
  both keys (no allocator in the store; the caller supplies the id —
  `core/src/metadata.rs`).
- **rename** = atomic `{ delete old dirent + put new dirent }`, with `require` on
  the source value re-pinned at commit and `require_absent` on the target.
- **delete** = atomic `{ remove dirent + tombstone/advance inode state + bump
  version }`.
- **the file commit** = the version-conditional `{ write chunk map + set
  COMMITTED + bump version }`, "concurrent writers conflict here; exactly one
  wins" ([§5][s5]).

A **distributed** backend is the *real* test of these because embedded redb makes
them cheap by serializing the whole process; TiKV faces genuine concurrency,
contention, and partitions. M4 demonstrates the **single-zone subset** of the
frozen five-clause consistency contract ([ADR-0015][a15], [§8.1][s8]): **clause 2**
(a file's writes linearizable at the commit point) is the load-bearing
demonstration; **clause 1** collapses into zonal linearizability (single zone, no
distinct L2 — [ADR-0020][a20]:32); **clause 3** (read-your-writes / monotonic
reads) is **trivially satisfied** at one zone and only becomes a testable
cross-zone behavior at M5/M6; **clauses 4–5** (replication lag, sync-N-zone) are
cross-zone and deferred. M4 must also keep **bumping `meta:version` inside the
atomic commit**, preserving the reserved Option-C fence as a non-breaking future
strengthening — the regression risk here is *dropping the reservation*, not
*using* it.

### Deployment: TiKV/PD as a stateful, disk-affine, orchestrator-agnostic tier

M4 introduces the system's first heavy external **stateful** dependency, and
[ADR-0010][a10] constrains how it lands. The M4 topology is the single-zone
**"Small multi-node Production"** profile ([§7.1][s7]): **TiKV (small) + its own
PD cluster + a 3-node etcd ensemble for L5 Coordination + local-disk D servers —
no L2/L3/TiDB**. The two coordinators are **distinct** and both are required: PD
(the Placement Driver) ships with TiKV and coordinates its regions and timestamp
oracle; the **etcd ensemble** backs Wyrd's `Coordination` trait — discovery,
leader election, fencing ([ADR-0006][a6]; [§7.1][s7] lists the profile's
coordination as "3-node etcd"). TiKV and PD are stateful and want **node affinity
to disks**: deployed as StatefulSets with local persistent volumes and an operator
on Kubernetes, **or** systemd on the storage hosts — but **"Kubernetes is
available, never required,"** "no code couples to orchestrator APIs," and "peers
are discovered through L5" ([ADR-0010][a10]; [§7.2][s7]). The bring-up artifacts
live in **`deploy/`, outside the Cargo workspace** — the structural guard that
"makes it hard for orchestrator coupling to sneak into a component." M4 ships a
**docker-compose** stack (TiKV + PD + the L5 etcd ensemble) for CI/eval; the Helm
chart/operator are later.

### Backends, profiles, and what "production" means

M4 **adds** a backend; it does **not** replace redb. [ADR-0014][a14] is explicit:
the single-binary/redb profile is **dev/eval only**, with **no production
durability promise**, and "production durability begins at the real multi-node
backends." So after M4:

- **redb** stays the **dev / single-binary / NAS** metadata backend (deterministic,
  disk-free in-memory variant for tests, embedded on disk for the NAS profile).
- **TiKV** is the **production** metadata backend — the one that carries the
  durability promise the release point announces.

M4 must **not** quietly expand redb's role into a supported production tier; that
"would require a stated durability floor … and the corresponding test surface;
that is a future decision, not this one" ([ADR-0014][a14]).

### DST and tests (the heart of M4)

[ADR-0009][a9] remains the correctness authority, and M4's most important
architectural decision about testing is **what *not* to do**: **TiKV does not go
inside the deterministic simulator.** The `MetadataStore` seam splits the burden —
**DST proves the *system*; the real tiers prove the *backend*** — and the line is
principled, not a workaround.

**Tier-0 — deterministic simulation (the spine, unchanged).** The commit protocol,
the version-conditional CAS (exactly-one-winner, [§Q5][s10]), no-torn-write
([§Q3][s10]), the GC-ledger lifecycle, repair/delete reuse of the commit point,
and the monotonic `meta:version` bump all stay proven against the **deterministic
in-memory/redb backend**, single-threaded and seed-reproducible. This is sound
precisely because the production logic is **byte-identical across backends** (it
depends only on the trait), so proving it over the deterministic backend proves it
for any backend. The strategy forbids the alternative outright: "a real
environment is therefore never used to test correctness the simulation already
covers … that is DST's job" ([§13.1][s10]; [ADR-0009][a9]: DST "complements, it
does not replace" the real tiers). **M4 must not re-prove atomicity against TiKV.**

**Tier-1 — software-defined faults against a *real* TiKV cluster.** This is where
the M4-specific evidence lives — what the simulator structurally cannot show: that
the abstractions DST simulates **match the real store** ([§13.1][s10]). On the M4
single-zone topology under `tc netem` / `iptables` partitions / cgroup throttling
/ `libfaketime` clock skew:
- **Integration** — the bare composition swap: the same system runs on TiKV behind
  the unchanged trait; multi-key atomic create/rename/delete succeed. The direct
  M4 DoD and trait-leak retirement.
- **Jepsen consistency** — a nemesis injecting **real** partitions, clock skew, and
  process pauses against the TiKV-backed cluster, validating the single-zone
  consistency clauses (linearizability of the commit point; exactly-one-winner
  under genuine concurrency). The Jepsen line **began at M2** over the networked
  path ([0004][p4]; [§13.4][s10]) and was extended over the repair path at M3
  ([0005][p5]); M4 extends it across the backend swap. A clean public Jepsen result
  is itself a credibility artifact.

**Tier-2 — first real-world hardware (single owned machine).** Real `fsync`, real
NVMe latency, real OS behavior against real TiKV — honest single-node performance
and I/O semantics the in-memory fakes abstract away (a single failure domain, so
it proves real-silicon behavior, **not** failure-domain independence).

**Tier-3 — multi-region — does *not* begin until M5** ([§13.4][s10]): single-zone
M4 has no use for real WAN or cross-zone failure independence.

> **Numbering note.** This proposal uses the architecture **realism ladder**
> (Tier 0 DST · Tier 1 software faults + Jepsen · Tier 2 single machine · Tier 3
> multi-region), matching [0005][p5] and [§13.2/§13.4][s10]. The CI/code taxonomy
> ([0004][p4]'s test taxonomy) uses a **different** scheme where "Tier-1" is the
> in-process DST/wire suite and "Tier-2" is the container integration job — a clash
> [§13][s10] flags explicitly. Both schemes appear in the tree; this document means
> the realism ladder throughout.

**Fidelity to ADR-0009 — the compounding loop.** Every behavior the real TiKV
cluster surfaces that the redb fake did not model (a transaction-conflict timing,
a PD timestamp-oracle edge, a fault shape) is **promoted back into DST as a new
seeded regression** *wherever it manifests through the trait contract* — the
FoundationDB/TigerBeetle pattern, "the highest-leverage idea in the strategy"
([§13.1][s10]). The trait surface is both the thing M4 validates **and** the
channel through which real-world findings flow back into the deterministic spine;
DST never cedes authority to the real tiers.

**Pinning the trait with the second implementation.** Because M4 is the **second**
`MetadataStore` implementation ([ADR-0006][a6] discipline), M4 hardens the trait:
it adds a **deterministic contract/property harness** that drives **both** backends
through the **identical** suite, and revisits the one **redb-shaped determinism
rationale** in the DST harness — the concurrency test argues "each `commit()` is
internally synchronous (one redb write transaction, no `await` inside)," which is
**not** true of a TiKV commit that awaits on network I/O. The "exactly one wins"
invariant still holds under TiKV's CAS, but the harness's stated reasoning (and
possibly its interleaving coverage) needs updating when a simulated-TiKV model is
added.

### Crate touch-points

Building on the workspace as it stands after M3 (`chunk-format`, `chunkstore-fs`,
`chunkstore-grpc`, `coordination-mem`, `core`, `custodian`, `dst`, `metadata-redb`,
`proto`, `server`, `testkit`, `traits`):

- **`metadata-tikv`** (**new**) — `impl MetadataStore for TikvMetadataStore` over
  the `tikv-client` transactional API: the atomic conditional commit, native
  prefix scan, the conflict-classification rule, the locking-read rule. Deps
  `traits` + `tikv-client` + `tokio`; **never** `core` or another concrete
  ([ADR-0016][a16]).
- **`server`** — parameterize the `cli.rs` metadata helpers over `M:
  MetadataStore`; add the **redb | tikv** backend selector (config); move the
  local paths onto `tokio`; give `alloc_inode` bounded backoff. redb stays the
  dev/single-binary default ([ADR-0014][a14]).
- **`traits`** — **unchanged** (the milestone's whole premise; any change here is a
  failure of M4's thesis).
- **`core`, `custodian`** — **unchanged** (zero redb dependence confirmed by the
  audit).
- **`dst`** — a deterministic simulated-TiKV model (or a trait-level contract
  property harness) so the property suite drives both backends; update the
  `concurrency.rs` determinism rationale for an await-inside-commit backend; new
  seeds.
- **`testkit`** — a real-TiKV-cluster fault seam (partition/latency/pause) for the
  Tier-1 integration + Jepsen runs.
- **`xtask`** — a TiKV integration runner and a Jepsen-against-TiKV runner; wire
  the `deploy/` docker-compose TiKV+PD into CI.
- **`deploy/`** (**new, outside the workspace**) — docker-compose for TiKV (small)
  + its PD cluster + a 3-node etcd ensemble (L5 Coordination) for CI/eval;
  Helm/operator deferred ([ADR-0010][a10]).
- **deps** — `tikv-client` (+ its `tokio`/`grpcio` transitive tree); confirm under
  the `cargo-deny` allowlist ([ADR-0003][a3]).

## Alternatives considered

- **Put TiKV inside DST (containerized) as the correctness authority:** **rejected**
  — [ADR-0009][a9] forbids using a real environment for correctness DST already
  covers, and containers break seed determinism. TiKV is validated at Tier 1–2 as
  a *complement*; DST stays on the deterministic backend.
- **Optimistic transactions with plain reads for preconditions:** **rejected as the
  default** — TiKV SI does not conflict-check read-only keys, so a precondition on
  a key the batch does not also write is exposed to write-skew. M4 reads
  precondition keys with `get_for_update` and defaults to **pessimistic**
  transactions; optimistic mode is acceptable **only** if every read-only
  precondition key is locked, and is a measured throughput trade, not a default.
- **The raw API with single-key `compare_and_swap`:** **rejected** — single-key
  only, cannot express the multi-key inode+dirent atomicity that is the entire
  reason TiKV was chosen over HBase ([ADR-0008][a8]), and cannot be mixed with the
  transactional API on one database.
- **Map a TiKV write-conflict to `Err` (or retry it inside the backend):**
  **rejected** — a lost race **is** the trait's `Conflict`; surfacing it as `Err`
  would conflate a stale writer with a backend fault and break the
  `version_cas_rejects_a_stale_writer` property. The backend folds write-conflicts
  into `Ok(Conflict)` and reserves `Err` for faults.
- **Add a paginated/streaming `scan` to the trait for large directories:**
  **deferred** — it is a **trait change**, violating the "unchanged trait"
  requirement that is the milestone's point. M4 buffers (`scan` already returns a
  materialized `Vec`); whether very large directories warrant a later trait
  evolution is an Open question, decided by measurement, not now.
- **Replace redb with TiKV everywhere (drop the embedded backend):** **rejected** —
  [ADR-0014][a14] keeps redb the dev/eval backend; M4 **adds** a production choice,
  it does not remove the dev one.
- **Build the L2 NamespaceStore (TiDB) in the same milestone:** **rejected /
  deferred to M6** — [ADR-0020][a20] is Proposed and is cross-region; M4 is
  strictly the zonal L4 store, and in single-zone deployments the namespace folds
  into the same store.
- **Mint a new ADR for the TiKV backend:** **not minted** — [ADR-0008][a8] already
  decides it and is **Accepted**; M4 is its first *implementation*, not a new
  decision. (Contrast M3, which *proposed* ratifying the still-Proposed
  ADR-0011/0012 as part of its milestone.)

## Graduation criteria (definition of done)

- **The same system runs on TiKV behind the *unchanged* `MetadataStore` trait** —
  selected by config in `server`; `traits`, `core`, and `custodian` are byte-for-byte
  unchanged.
- **Multi-key atomic directory operations hold under the distributed store** —
  create / rename / delete and the file commit are all-or-nothing on TiKV, proven
  under contention and partition (Tier-1 integration + Jepsen).
- **The version-conditional CAS contract is preserved exactly** — exactly-one-winner
  under concurrency; a stale writer gets `Ok(Conflict)`; `require_absent` collision
  guards hold; a TiKV write-conflict surfaces as `Conflict` (not `Err`) and a
  genuine fault surfaces as `Err`; values are stored byte-identically so no CAS is
  spuriously rejected.
- **The swap is a `server`-crate composition change, not a refactor** — the diff
  outside `metadata-tikv` is confined to `server` (selection + async wiring +
  bounded `alloc_inode` backoff) plus test/deploy scaffolding; no consumer logic is
  refactored.
- **`meta:version` is bumped inside the atomic commit on TiKV**, preserving the
  reserved Option-C fence; the append/CAS/watch seats remain expressible.
- **Tier-0 DST stays green and seed-reproducible** on the deterministic backend
  (seeds committed); the trait is **pinned by both implementations** through the
  shared property harness.
- **Tier-1 integration + Jepsen consistency green** against real containerized TiKV
  under fault injection; **Tier-2** single-node real-I/O run green. Any bug-finding
  discovery is promoted to a **seeded DST regression** where the trait exposes it.
- **The production deployment stands up from `deploy/`** (TiKV-small + its PD
  cluster + a 3-node etcd ensemble for L5 + local-disk D servers), peers discovered
  through L5, with **no crate importing an orchestrator API**.
- **redb remains the dev/single-binary backend** with no production-durability
  promise ([ADR-0014][a14]); TiKV carries the production promise.
- `fmt`/`clippy` clean; `Cargo.lock` updated; `cargo-deny` passes with the
  `tikv-client` dependency tree.

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M4** milestone (branch
`feat/m4.<n>-<slug>`, commit subject `feat(<crate>): … (M4.<n>, #<issue>)`):

1. **`metadata-tikv` skeleton + the conformance suite as a contract test** — new
   crate (`traits` + `tikv-client` + `tokio`); a throwaway single-node TiKV in
   `deploy/` for CI; run the **existing** `MetadataStore` conformance suite (the one
   redb passes) against TiKV for `get`/`scan`/`commit` basics. *DoD:* TiKV passes
   the **shared** (not forked) conformance suite for the basic operations; CI can
   reach a TiKV.
2. **The atomic conditional commit + conflict semantics** — `commit(WriteBatch)` as
   one transaction: lock + read + byte-compare every precondition
   (`get_for_update`, pessimistic by default), all-or-nothing puts/deletes; map a
   precondition miss **and** a write-write race to `Ok(Conflict)`; reserve `Err` for
   faults; never use the raw `compare_and_swap`. *DoD:* the version-CAS property
   tests (exactly-one-winner, stale-writer-rejected, `require_absent` collision)
   pass over TiKV; a forced write-conflict surfaces as `Conflict`, not `Err`.
3. **Native prefix scan + documented read consistency** — replace the
   whole-table-filter shortcut with a TiKV range scan `[prefix, prefix_upper)`,
   paged and materialized; document the read-snapshot semantics. *DoD:* dirent
   prefix scans return correct sets at scale; no consumer order-dependence
   regressed; `rename`'s read-then-commit pattern is safe under the locking rule.
4. **Backend selection in `server` (the composition change)** — parameterize the
   ~7 `cli.rs` helpers over `M: MetadataStore`; add the **redb | tikv** selector by
   config; move the local paths onto `tokio`; give `alloc_inode` bounded backoff.
   redb stays the dev default. *DoD:* `server` runs identically on redb (dev) and
   TiKV (prod) by config; the diff outside `metadata-tikv` is confined to `server`;
   `core`/`custodian` untouched; `cargo xtask ci` green on both backends.
5. **Production deployment — TiKV/PD as a stateful, disk-affine, orchestrator-agnostic
   tier** — `deploy/` docker-compose for TiKV-small + its PD cluster + a 3-node etcd
   ensemble (L5 Coordination) + local-disk D servers; discovery through
   L5/Coordination; no orchestrator coupling in any crate. *DoD:* the single-zone
   "Small multi-node Production" stack stands up from
   `deploy/`; peers discovered through Coordination; no crate imports a k8s/orchestrator
   API.
6. **Tier-1 integration + Jepsen consistency; Tier-2 single-node** — end-to-end
   PUT/GET + multi-key create/rename/delete on TiKV under `tc netem`/`iptables`
   faults; a Jepsen nemesis (partitions, clock skew, pauses) over the single-zone
   consistency clauses; a Tier-2 run on one real machine (real fsync/NVMe). Promote
   any discovery to a seeded DST regression. *DoD:* integration + Jepsen green in
   CI; multi-key atomic dir ops proven to hold under partition/contention; a
   bug-finding seed committed; the Tier-2 job green.
7. **DST: pin the trait with the second implementation** — a deterministic
   simulated-TiKV model (or trait-level contract harness) so the property suite
   drives **both** backends; update the `concurrency.rs` determinism rationale for
   an await-inside-commit backend. *DoD:* DST drives both backends through the
   identical property suite, green and seed-reproducible; the two-implementations-pin
   discipline ([ADR-0006][a6]) is satisfied.

(M4 is sized like M1/M2's seven slices and narrower in surface than M3's eight —
it is one new backend crate plus a composition switch plus the real-store test
campaign, not a new plane. Slices 1–4 are the implementation spine; 5–7 are the
release-grade deployment and proof. The crate boundary means slice 1 can begin in
parallel with the tail of M3 once the trait is confirmed frozen.)

## Backward compatibility

M4 is the **first release point**, so compatibility duties begin here — but
narrowly and deliberately:

- **The `MetadataStore` trait** — **unchanged**, and now **pinned by two
  implementations**. After M4 it is a real internal contract; a future change is a
  trait evolution with both backends to carry, not a free edit. (Pre-1.0, still no
  *published* API.)
- **The metadata model / keyspace** — **unchanged** and additive: the same
  `inode:`/`dirent:`/`pending:`/`meta:version` keys and JSON values, stored
  byte-identically on TiKV. Placement (M3) stays embedded in the inode value with
  `#[serde(default)]`, so pre-M3 records still decode. **No data migration** —
  redb-profile data is dev/eval only ([ADR-0014][a14]) and is not migrated into a
  production TiKV deployment; a fresh production zone starts on TiKV.
- **The on-disk *fragment* format** — **unchanged** by M4 (M4 touches metadata, not
  the chunk layout) and **stays v0/unstable**. `v1` stamping is **not** automatically
  tied to M4: its gate is "a second independent reader **or** a sustained
  fault-injection run" ([p2][p2]). M4's **Tier-2 sustained run on real TiKV** is a
  **candidate** `v1` trigger — recorded here, not decided (consistent with
  [0003][p3]/[0005][p5]).
- **The deployment surface** — first stabilized here: the `deploy/` topology
  (TiKV+PD+D-servers, L5 discovery) becomes the documented single-zone production
  shape an operator can depend on.
- **Reserved seats honored** — the `meta:version` fence ([ADR-0015][a15]) and the
  append/CAS/watch hooks ([ADR-0007][a7]) remain expressible on TiKV; M4 builds
  none of them but forecloses none.

## Open questions

- **Optimistic vs pessimistic transactions, measured.** M4 defaults to pessimistic
  (or optimistic-with-`get_for_update`-on-every-precondition-key) for correctness;
  the **throughput cost** under real metadata contention is a measurement, and the
  default should be revisited against Tier-1/Tier-2 numbers. Async-commit/1PC
  optimizations exist and change latency without changing the atomicity/conflict
  semantics relied on here — to evaluate later, not in the first cut.
- **`tikv-client` version and exact API shapes.** Pin the crate version in
  `Cargo.toml` and reconfirm at build time the **locking-read** entry point
  (`get_for_update` vs a `TransactionOptions` flag) and the **write-conflict**
  error path (`tikv_client::Error` wrapping `KeyError::WriteConflict`) — these were
  verified across crate docs and TiKV PRs but the concrete Rust signatures should
  be confirmed against the pinned version. Confirm the client's futures are
  `Send + Sync` for the object-safe, simulator-driven trait.
- **Read consistency to document.** The trait promises nothing about `get`/`scan`
  snapshot semantics; TiKV reads default to a snapshot at a timestamp. M4 must
  **decide and document** the read consistency (especially the `rename` `get` that
  precedes `commit`) — the commit precondition re-check is what guarantees safety,
  but read staleness interacts with read-your-writes ([ADR-0015][a15] clause 3),
  which only becomes a cross-zone behavior later.
- **Large-directory `scan` buffering.** `scan` returns a materialized `Vec`; a
  directory with very many dirents buffers fully in memory. Whether M4 accepts this
  or a later trait evolution adds a paginated/streaming scan is a **measurement**
  question — a trait change is out of M4's "unchanged trait" scope.
- **The `meta:version` fence as a `v1`-stamping trigger.** M4's sustained
  real-store fault-injection is a candidate trigger for stamping the on-disk format
  `v1`; recorded, not decided.
- **DST model fidelity for an await-inside-commit backend.** How faithfully a
  *deterministic simulated-TiKV* should model 2PC/TSO interleavings (vs a
  trait-level contract harness) to keep the "exactly one wins" coverage honest is
  an M4 design point, surfaced by the second-implementation pinning.
- **`NamespaceStore`-on-TiKV.** The deployment view says the namespace shares the
  metadata store in single-zone profiles, but `NamespaceStore` is **not in code**.
  When it lands, does it ride the same TiKV `MetadataStore` instance in single-zone
  deployments, becoming a distinct TiDB deployment only at the provider-fleet tier
  ([ADR-0020][a20])? Out of M4 scope; flagged so the boundary stays deliberate.

[p1]: ../accepted/0001-milestone-0-walking-skeleton.md
[p2]: ../accepted/0002-implementation-arc.md
[p3]: ../accepted/0003-milestone-1-erasure-coding.md
[p4]: ../accepted/0004-milestone-2-networked-d-servers.md
[p5]: ../accepted/0005-milestone-3-custodians.md
[s4]: ../../architecture/04-solution-strategy.md
[s5]: ../../architecture/05-building-block-view.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s9]: ../../architecture/09-build-order-and-roadmap.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a6]: ../../adr/0006-etcd-for-coordination.md
[a7]: ../../adr/0007-reserve-append-cas-watch.md
[a8]: ../../adr/0008-tikv-metadata-and-pluggable-backends.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a15]: ../../adr/0015-consistency-contract.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a20]: ../../adr/0020-global-namespace-store.md
[a31]: ../../adr/0031-watch-and-change-feed-contract.md
