---
created: 06.07.2026 12:00
type: adr
status: Proposed
tags:
  - adr
  - metadata
  - storage
  - pluggability
  - sovereignty
---
# 0042. Production metadata backend: reaffirmed two-slot design, three finalists

## Context

[ADR-0008](0008-tikv-metadata-and-pluggable-backends.md) defined a narrow
`MetadataStore` trait and chose **two** concrete backends from one codebase: an
**embedded** backend (`redb`, pure-Rust) for dev / eval / NAS-class, and a
**distributed** backend (TiKV) for production. That two-slot shape is doctrine,
and ADR-0010's dependency rule makes selecting a backend a composition change in
`server`, never a refactor.

The choice of *distributed* backend is under review for one reason: the native
Rust client for TiKV, `client-rust` 0.4.0, states verbatim that it is "not
suitable for production use — APIs are not yet stable and the crate has not been
thoroughly tested in real-life use" (#435, #260). TiKV the *server* is
production-grade; the *client* Wyrd links is not. That is the pain point, and
nothing else about ADR-0008 is in question.

**What is NOT reopened:** the two-slot design, `redb` in the embedded slot
(effectively uncontested — pure-Rust, single-process ACID, trivial to
cross-compile to musl/NAS), and the gate below. Only the production distributed
backend is re-decided here.

### Requirements

The zonal metadata model is hierarchical **inode + dirent**. File creation writes
an inode *and* its dirent atomically; rename is a single dirent mutation; the
commit point is one linearizable, version-conditional mutation. That shape, plus
the two-slot and sovereignty doctrine, yields the full requirement set the
production distributed backend MUST satisfy — stated once here so the appendix
matrices read as the *output* of a filter, not a free-standing table, and so the
finalist field is **derived, not inherited from ADR-0008**.

**Requirements (R1–R7).**

- **R1 — Native cross-shard multi-key atomic transactions, linearizable commit.**
  inode + dirent written as one indivisible act; the version-conditional commit
  point totally orders a file's versions. *Not* single-row atomicity, *not*
  single-partition LWT, *not* eventual-by-default. (This is the gate that selected
  TiKV in ADR-0008.)
- **R2 — Ordered keyspace with range scan.** dirent enumeration (list a
  directory), the pending-chunk ledger sweep, and prefix scans over the
  inode/dirent tree require ordered keys — not a hash-only KV.
- **R3 — Conditional / compare-on-version write.** "Exactly one commit wins"
  (Q5) is a CAS on `meta:version`; the store must carry the precondition
  atomically with the mutation.
- **R4 — Within-zone horizontal shard + consensus replication, never a single
  writer.** Metadata is small but precious — losing it orphans every chunk
  (§8.2) — so it is Raft-class replicated and sharded across failure domains
  *inside one zone*.
- **R5 — Data-proportional scale envelope.** The store holds storage metadata
  (it grows with the namespace), not kilobytes of coordination state — the axis
  that separates a metadata store from an L5 coordinator.
- **R6 — Behind the narrow `MetadataStore` trait, distributed profile only.** The
  embedded slot is `redb`; the distributed backend is a composition change in
  `server` (ADR-0010), never a refactor. The distributed backend need not also be
  embeddable — that is `redb`'s job.
- **R7 — Open and sovereign.** Permissive / clearly-licensed (Apache-2.0 house
  posture, ADR-0003), self-hostable, no hard dependency on a US-controlled
  *service*; governance and continuity weighed as *longevity* risk.

### Why R1–R5 are hard gates — architectural necessity and user-visible stakes

Two justifications run in parallel and both are load-bearing, so the ADR records
both. *Architecturally*, R1–R5 are not a wishlist — they are the
**specification of the commodity primitive the differentiator consumes.** Each,
if waived, either breaks a headline guarantee or forces Wyrd to rebuild the
property itself, spending novelty budget outside the moat and violating
reinvent-vs-consume. *Experientially*, each maps to a failure a **sovereign
challenger cannot survive selling**: a hyperscaler absorbs a corruption incident
as a footnote, but a new EU entrant whose entire pitch is "trust us with your
data instead of the US hyperscaler" is finished by the first silent-loss event.
R1–R5 are therefore the minimum bar to be a credible alternative at all — each is
at once a line on the datasheet and a question in a hostile technical eval.

- **R1 — atomic linearizable commit.** *Drop it:* single-row atomicity forces
  either denormalizing the dirent into the inode (rename becomes a mass key
  rewrite, killing scalability) or a write-ahead intent log (the Tectonic
  contortion ADR-0008 rejects); mere eventual ordering leaves "the file exists"
  with no defined moment, so torn state becomes observable — and with no
  linearization point there is nothing to prove the commit protocol atomic
  *against*, so the moat itself evaporates. *Felt as:* the end-user never sees a
  half-written file — an upload fully appears or does not exist; a listing never
  shows a phantom that 404s or opens as a 0-byte corpse (Q3); the app developer
  gets an honest contract (a `200` on PUT means fully-there-and-readable) and
  writes no verify-after-write defensive code. *Who it sinks:* R1 is the answer to
  the most-asked question in a storage sales cycle — "what happens if a write is
  interrupted?" — and trust in storage is binary: "sometimes files are weird"
  reads as "unusable."
- **R2 — ordered keyspace + range scan.** *Drop it:* the hierarchical
  inode/dirent model (chosen so rename is one mutation, ADR-0008) is *defined* by
  ordered keys — directory listing is a prefix scan, the GC ledger sweep is a scan
  for expired leases (§8.2); a hash-only KV forces either a self-maintained ordered
  index (a second consistency domain, a fresh correctness surface) or path-as-key
  with O(n) rename. *Felt as:* listings are fast and complete at millions of
  objects and a folder move is instant, not a multi-second half-completing freeze;
  the developer's `LIST` behaves as the S3 compatibility you sold actually
  promises. *Who it sinks:* without R2 the bolted-on index drifts and the developer
  intermittently sees a file in a listing that GETs a 404 — the most maddening
  class of support ticket, and it silently voids the "S3-compatible" claim. R2 also
  makes GC a scan, so orphaned fragments are reclaimed and capacity does not leak —
  directly the operator's $/TB and their ability to price against AWS without
  subsidy.
- **R3 — compare-on-version write.** *Drop it:* "exactly one concurrent writer
  wins" (Q5) is arbitrated *inside* the atomic commit by a CAS on `meta:version`;
  without it, concurrent writers silently clobber (lost updates, Q5 unprovable) and
  the only recourse is pessimistic locking — a distributed lock on the write hot
  path, violating the rule that nothing data-proportional touches L5 (§11).
  *Felt as:* the user syncing one file from two devices gets no silent lost update
  — one commit wins, the other is told to reconcile; the developer gets real
  If-Match/CAS to build correct multi-writer logic on, and it is the primitive the
  reserved collaborative-editor future stands on (ADR-0007). *Who it sinks:* the
  locking alternative is felt by the user as "why is saving slow when a colleague
  has the folder open?" and by the operator as lock-contention incidents — R3 is
  the difference between "fast and correct under concurrency" and "pick one."
- **R4 — synchronous consensus, never a single writer.** *Drop it:* metadata is
  the map to the bytes, so losing it orphans every EC fragment — a total-loss event
  though every byte survived (§8.2); an async-failover single primary loses the
  last commits silently, i.e. the maps to chunks already stored successfully. And a
  single writer caps whole-system metadata throughput regardless of D-server count,
  with the small-files Drive workload (Q7) hitting the ceiling first. *Felt as:*
  "acknowledged means durable, full stop" is a promise you can make (Q4) — no
  user's "saved" file quietly vanishes days later while its data blocks sit intact
  and unreferenced; and the product scales by *adding nodes*, not "it got slow as
  it got popular." *Who it sinks:* the silent-loss variant is the single
  unsurvivable trust-and-compliance event for a sovereign provider, and the
  throughput ceiling is "we can't take more customers without a forklift" — R4
  turns growth into a node addition instead of a wall.
- **R5 — data-proportional scale envelope.** *Drop it:* the seductive shortcut —
  "we already run etcd for L5, put metadata there too" — demos beautifully and
  pilots fine, then degrades months into production as the namespace grows into the
  range etcd was never sized for: the "treating etcd as a database" failure the
  risk register names (§11), in the one component that must never wobble. *Felt
  as:* the pilot's behaviour predicts production — the operator is not ambushed
  *after* shipping, when migration cost is highest and customers are already on it;
  the user never meets the system-wide slowdown that ambush produces. *Who it
  sinks:* this is the cruelest mode because it is *delayed* — a provider staking its
  name on Wyrd cannot afford to be blindsided by its own success. R5 keeps etcd
  correctly consumed at L5 (ADR-0006) and forces the distributed slot to be a real,
  sharding, data-proportional store (which is also why `redb` is fine embedded but
  insufficient distributed).

R6 and R7 are motivated differently — by house doctrine (pluggability behind the
trait; sovereignty-first), not by a per-requirement architectural failure — so
their justification lives with the design doctrine, not here.

### The elimination filter

Gates in kill-order; each candidate is cut at the *earliest* gate it fails, and
G5's continuity clause *ranks* the survivors rather than eliminating. Reasoned
once so it is never relitigated (per-candidate detail in the appendix):

| Gate | Requirement | Cut at this gate |
|---|---|---|
| **G1** multi-key atomic + linearizable | R1, R3 | single-row / region-local: **HBase, Bigtable, Accumulo**; single-partition-LWT / eventual-by-default: **Cassandra, ScyllaDB, Riak, Couchbase** |
| **G2** ordered range scan | R2 | hash-only / cache-shaped: **Redis / Valkey, Memcached** |
| **G3** distributed, no single writer | R4 | single-primary servers: stock **PostgreSQL, MySQL** — they cluster for HA, not for sharded writes (the "just use Postgres" reflex ends here) |
| **G4** correct scale envelope | R5 | right primitive, wrong layer: **etcd, rqlite / dqlite** — these *are* L5; "treating etcd as a database" is the classic failure mode |
| **G5** open + sovereign | R7 | **cut:** proprietary managed services — **Spanner, DynamoDB, Cosmos** (US-controlled); non-OSI single-vendor — **MongoDB** (SSPL); no-longer-permissive — **CockroachDB** (CSL, ex-Apache). **ranked down on continuity:** single-vendor VC (**YugabyteDB**) below foundation-governed (**TiKV**/CNCF, **FoundationDB**/Apache) |

**What passes.** Exactly two pure transactional ordered-KV survivors — **TiKV**
and **FoundationDB** — plus the SQL-adjacent pair that clears the gates while
paying SQL impedance: **YugabyteDB** and **TiDB** (the latter *is* TiKV with a SQL
layer it does not need, so it collapses into TiKV-direct and is not carried
separately). **CockroachDB** would have passed on architecture and is out on G5
alone. That is the three-finalist field of the Decision — derived from R1–R7, not
inherited.

**Two doctrine notes the filter makes explicit.** *Sovereignty is a property of
the deployment, not the steward's passport.* A self-hosted Apache-2.0 binary is
sovereign even when the steward is American (FDB / Apple) or Chinese (TiKV /
PingCAP), because there is no runtime dependency on a foreign-controlled *service*
and the operator owns the bits; a hosted US service (Spanner, DynamoDB) is the
hard fail. Governance domicile is therefore a *continuity* risk (G5's ranking
clause), **not** a runtime-sovereignty violation — which is why TiKV and FDB both
clear G5 and only Yugabyte is ranked down. *And the survivor set exposes a real
gap:* there is no EU-origin, mature, distributed transactional ordered-KV. That
absence is an instance of exactly the gap this program exists to close — but it is
emphatically **not** one Wyrd should fill. Building a distributed transactional KV
would spend the entire novelty budget on a *consumed* primitive, violating
reinvent-vs-consume at its core. The sovereign move is to consume a survivor, own
it by self-hosting, and hedge with a full-history source mirror — never to
fork-and-maintain or rebuild.

## Two concepts this decision turns on

The candidates differ along two axes that are easy to wave at and easy to get
wrong, so both are defined here precisely; the per-candidate reasoning below
refers back to these.

### Link-graph purity

Wyrd is a Rust codebase (ADR-0004) that ships as a single static binary for the
NAS/embedded profile (ADR-0014) and must cross-compile cleanly to musl and
aarch64 targets. **Link-graph purity** is the property that everything *linked
into a Wyrd binary* is pure Rust — no C/C++ library in the graph.

Why it matters, concretely:

- **Cross-compilation.** A pure-Rust graph cross-compiles to `x86_64-unknown-linux-musl`
  / `aarch64` with `cargo build --target` and nothing else. A C/C++ dependency
  drags a C toolchain, a sysroot, and ABI/`libc` coupling (glibc-vs-musl) into
  every target — the friction the NAS profile exists to avoid.
- **Auditability & supply chain.** `cargo-deny` / `cargo-audit` see the whole
  graph when it is Rust; a linked C library (its source, its CVEs, its build
  flags) sits *outside* that wall and must be tracked by hand.
- **One build, one language.** No second build system, no `bindgen`, no vendored
  headers, no "works on my libc" class of bug.

The critical scoping point: purity is about the **client** side linked into
Wyrd's binaries, **not** the server. TiKV's storage engine is C++ (RocksDB), but
that runs in the *deployed TiKV process*, reached over gRPC — it is never linked
into Wyrd, so it costs no purity. By contrast FoundationDB's `libfdb_c` **is**
linked into every Wyrd gateway that talks to it: that is a purity cost. This
distinction is the whole reason TiKV and FDB land differently on this axis
despite both having C/C++ somewhere.

### SQL impedance

The `MetadataStore` trait is a **key-value + atomic-batch** interface:
`get(key)`, `scan(prefix)`, `commit(WriteBatch)` where a batch carries
preconditions plus puts/deletes and returns `Committed` / `Conflict`. A SQL
database speaks tables, rows, and a query language. **SQL impedance** (an
impedance *mismatch*) is the translation layer between the two: mapping KV
operations onto `INSERT`/`UPDATE`/`SELECT`, expressing a precondition as a
`WHERE`/`SELECT … FOR UPDATE`, and paying SQL's parse/plan cost on the hot path.

The load-bearing observation: this mismatch lives **entirely inside the
concrete**. A SQL-backed `MetadataStore` implementation hides all of it behind
the same trait; no SQL leaks past the seam, and callers (`core`, `custodian`,
the gateways) are unchanged. So SQL impedance is an *implementation cost and
risk to contain*, not an architecture violation — the two-slot design absorbs a
SQL backend exactly as it absorbs a KV one.

## Decision

1. **Reaffirm the two-slot `MetadataStore` design** (ADR-0008): `redb` embedded,
   one distributed backend behind the same trait, selected as a composition
   change (ADR-0010). This is not in question.

2. **Narrow the production distributed field to three finalists** — **TiKV**,
   **FoundationDB**, **YugabyteDB** — all of which clear the gate. The full
   candidate field, with each rejection reasoned so it is never relitigated, is
   the appendix.

3. **Each finalist carries exactly one primary liability; it is adopted only
   with the named mitigation below.** The finalists trade along three axes —
   *client maturity · governance/continuity · link-graph purity* — and no
   candidate wins all three. The honest shape is **pick two**:
   - TiKV = governance + link-graph purity (− client maturity)
   - YugabyteDB = client maturity + link-graph purity (− governance)
   - FoundationDB = client maturity + best-consistency, but − governance **and**
     − link-graph purity (its second win, strict serializability, sits on a
     *fourth* axis outside the triangle; it is the least-sovereign finalist).

4. **The #257 conformance + contention + Jepsen battery is the tiebreaker, and
   TiKV is the incumbent default.** TiKV remains the production backend until a
   challenger *passes that battery* and its liability mitigation is demonstrated.
   Therefore **this ADR does not itself supersede ADR-0008** — it reaffirms the
   two-slot design and reopens only the distributed slot with TiKV still
   standing. If a challenger wins, *that* outcome is recorded as a later
   supersession of ADR-0008's TiKV clause (per [ADR-0038](0038-supersession-recorded-in-the-index.md);
   the on-file `Superseded` stamp is now available, #444).

### The three finalists — and how we address what each brings

#### TiKV (incumbent) — liability: an immature, correctness-critical Rust client

`client-rust` 0.4.0 is pre-1.0 and self-declared not production-ready. It has
already shown three sharp edges — a drop-time panic path (`CheckLevel::Panic`),
eager pessimistic locks in `put`/`delete`, and write-conflict errors wrapped
several layers deep. How we address it:

- **Fence the client as an untrusted boundary.** Every one of those three edges
  was *caught by Wyrd's own conformance + contention battery before merge* — that
  battery is the compensating control, and it stays the gate the client must pass
  on every pin bump (the nightly TiKV conformance job, #420).
- **Pin exactly, and carry a vendored fork if needed.** Pin the precise version
  (#260); upstream is active (the tonic/TLS fix merged 2026-06-26), so a fork is
  a *patch-carrier*, not a maintenance island — we cherry-pick a fix for the
  specific defect Jepsen finds rather than owning the client.
- **Quarantine the advisory surface.** The pinned tonic tree pulls
  RUSTSEC-flagged `rustls-webpki`; keep it behind the feature-gated ADR-0003
  deferral until a tagged release clears it, tracked so it cannot rot silently
  (#420).
- **Escalation path is this ADR's other two finalists.** If the client cannot be
  made trustworthy under the #257 gate, that *is* the trigger to promote a
  challenger — the mitigation for TiKV's liability failing is FDB/YugabyteDB, not
  a heroic client rewrite.

#### FoundationDB — liabilities: (a) a C library in the link-graph; (b) no-foundation governance

- **`libfdb_c` in the link-graph — confine it to the production tier.** The
  purity cost is real but *bounded*: the `fdb` feature is off by default, so the
  embedded/NAS single-static-binary profile stays pure-Rust `redb`, untouched.
  The C client is linked only into the production gateway build, which is already
  a multi-node, non-NAS deployment where a dynamically-linked, version-matched
  `libfdb_c` shipped in the OCI image is the norm every FDB product accepts. This
  is reconciled with ADR-0014 (single-binary is explicitly dev-only), and the
  dependency is *documented*, not hidden: the audit policy records `libfdb_c`'s
  licence/provenance out-of-cargo, and startup fails closed on a client↔server
  `protocol_version` mismatch with a guided error rather than the classic
  indefinite "waiting for cluster" hang. The multi-version client bridges lockstep
  upgrades.
- **No-foundation governance — the permissive licence is the safety net.**
  FoundationDB is Apache-2.0. Even without a neutral foundation, Apache-2.0 code
  is **forkable**, so a relicense *trap* is impossible: we mirror the source, and
  the correctness-critical core is simulation-tested upstream (the same DST
  methodology as our own ADR-0009), so the maintenance surface we would inherit
  on a fork is thin. The residual risk is *direction*, not *capture* — acceptable
  and named.

#### YugabyteDB — liabilities: (a) SQL impedance; (b) single-vendor governance; (c) a heavy, self-managed server (plus a non-issue to dispatch: global-topology licensing)

- **SQL impedance — contain it behind the trait on a tiny, fixed SQL surface.**
  The `metadata-yugabyte` concrete maps the trait onto one small schema (an
  inode/dirent pair, or a single `kv(key BYTEA PRIMARY KEY, value BYTEA)` table)
  driven entirely by **prepared statements** — no dynamic SQL, no ORM, no query
  building on the hot path. `commit(batch)` is one serializable transaction;
  preconditions become `SELECT … FOR UPDATE` / conditional `UPDATE … WHERE`;
  `CommitOutcome::Conflict` is the serialization-failure / zero-rows-affected
  signal. The client is `tokio-postgres` — pure Rust and mature — so this is the
  finalist that gets a *production-ready client for free while keeping link-graph
  purity*. A bonus for correctness doctrine: a SQL-shaped third implementation
  alongside `redb` and the DST sim *strengthens* the ADR-0006 trait-pinning story
  (the more differently-shaped the implementations, the better the trait is
  pinned).
- **Single-vendor governance (CLA-not-DCO) — pin the Apache-2.0 core, treat
  pgwire as the open standard.** The CLA governs *contribution*, not the right to
  run or fork the *released* code, which is Apache-2.0 for the core database
  (`YugabyteDB Anywhere` is Polyform and is explicitly out of scope). We pin to
  the Apache-2.0 core, mirror the source, and depend on the **PostgreSQL wire
  protocol** — an open, decades-stable standard — so the *client* is not
  lock-in even if the server's vendor changes course. Continuity is weaker than
  CNCF-TiKV; that is the trade this option makes and the ADR names it.
- **Global-topology licensing — a non-issue for Wyrd.** YugabyteDB's commercial
  tiers (the *Aeon* managed / BYOC product) gate "global deployment topologies"
  behind paid plans, which can read as "can't grow globally without a licence."
  Two reasons it does not bite. (1) The gate is around the *managed product*, not
  the engine: the **Apache-2.0 core** self-hosts multi-region clusters,
  row-level geo-partitioning, follower reads, and synchronous + asynchronous
  (xCluster) replication — and since early 2025 the formerly-enterprise features
  (distributed backup, encryption at rest, read replicas) are in the OSS project
  too. (2) **Wyrd never asks the metadata backend to grow globally.**
  `MetadataStore` is the *per-zone* store; Wyrd supplies the global layer itself
  — L2's globally-consistent namespace plus cross-zone replication (M9) and the
  global control plane (M10) — *above* it, and ADR-0015's home-zone authority
  deliberately keeps the commit point zone-local. So Wyrd runs one
  **single-region cluster per zone** (multi-node within the zone for HA), squarely
  inside the free core; a globe-spanning metadata DB would be the *wrong* shape,
  fighting the zone-local contract. What is genuinely licensed — *YugabyteDB
  Anywhere* / *Aeon* — is the management/orchestration convenience, addressed next.
- **Heavy Postgres-fork server + self-managed operations — accept it for the prod
  tier, own it with Wyrd's substrate.** Two real costs, neither a wall. (i)
  Per-node footprint: a full Postgres-fork is heavier than TiKV or FDB, but the
  production tier is multi-node regardless of backend, it is self-hostable and
  EU-hostable, and the embedded slot stays `redb` so "heavy" never reaches
  dev/NAS. (ii) The OSS path forgoes *YugabyteDB Anywhere* (the Polyform control
  plane), so cluster bring-up, upgrade, backup, and monitoring are self-managed
  with the OSS tooling (`yugabyted`, `yb-admin`, the open-source Kubernetes
  operator / Helm) rather than the paid UI — which is exactly the posture ADR-0003's
  control-resilience test *wants* (self-host, no *required* managed service). How
  that per-zone operation is owned — for all three finalists — is treated next.

#### Common to all three: the gate does not shrink

Whichever backend is trialled, the decision is **provisional until it passes the
#257 battery** against Wyrd's contract. A mature client (YugabyteDB) or a
simulation-tested core (FDB) buys confidence but does not exempt the backend:
with YugabyteDB you are trusting *its* distributed-transaction correctness
instead of a client's; with FDB you are trusting *your mapping layer* onto its
optimistic model. The battery tests exactly those.

#### Common to all three: operating the per-zone cluster

The distributed backend is deployed **once per zone** (multi-node within the zone
for HA), and that cluster must be brought up, upgraded, backed up, and monitored.
This is a real ops cost, but a bounded one — and it is not a licensing wall for
any finalist:

- **It is not a new capability.** A Wyrd zone already runs a per-zone stateful
  quorum — **etcd** for L5 coordination (ADR-0006) — alongside the D-server fleet
  and the leader-elected custodians. The metadata cluster is one more stateful
  component in a pattern the zone already operates.
- **Wyrd's own substrate owns it, not a vendor SaaS.** ADR-0010 makes the
  deployment substrate pluggable (systemd on storage hosts / docker-compose / k8s
  with a placement-aware operator) and forbids coupling to orchestrator APIs; M8
  (manageability — CLI + portal) and the day-one runbook (#367) own zone
  lifecycle. So operating the cluster depends on **none** of *YugabyteDB Anywhere*,
  a *TiDB Dashboard* SaaS, or an FDB vendor console — the metadata cluster is
  deployed and observed the same way D-servers, etcd, and custodians already are.
- **Ops weight differs — it is a comparison axis the runbook decides.** TiKV adds
  **PD** (the placement driver) plus `tiup`/operator and rolling upgrades;
  FoundationDB uses `fdbmonitor`/`fdbcli` + the FDB operator and imposes the
  strictest discipline — **lockstep protocol upgrades**, bridged by the
  multi-version client; YugabyteDB has the lightest bring-up (`yugabyted`,
  one command) but the heaviest per-node footprint. Whichever wins, the #367
  first-deployment gate must stand the per-zone cluster up **end-to-end on OSS
  tooling** — that run *is* the evidence the management story holds, riding the
  same gate as the #257 correctness battery.

## Consequences

- The production backend decision is **framed, not yet made**: three finalists,
  each with a named liability and a concrete mitigation, decided by a battery
  that binds all three. TiKV stands until a challenger earns the swap.
- ADR-0008 is **reaffirmed, not superseded**, by this ADR; a challenger win is a
  future supersession event.
- The downstream `metadata-client` milestone issues (#437–#443) that currently
  assume FoundationDB are **conditional on this decision** and must be reframed to
  the chosen backend (`metadata-<winner>`, its packaging/link-graph story, its
  battery) once it lands.
- The two concept definitions (link-graph purity, SQL impedance) become shared
  vocabulary for this and later backend decisions.
- Each requirement carries its own justification inline (architectural failure
  + user-visible consequence, R1–R5), so the filter reads as reasoned, not
  asserted, and doubles as the technical-eval defence for the storage pitch.
- The candidate field is recorded once, with reasons, so the wide-column /
  proprietary / embedded-slot rejections are not relitigated.

## Appendix — candidates considered

Licences verified July 2026. ✅ meets / good · ⚠️ partial / caveat · ❌ fails /
absent.

These matrices are the per-candidate instantiation of the R1–R7 filter in the
Context: each verdict traces to the earliest gate a candidate clears or fails.

### Matrix A — architectural fit (does it do the job?)

| Candidate | Cross-shard multi-key txn | Consistency | Embedded (dev/NAS) | Distributed (prod scale) | Rust client |
|---|---|---|---|---|---|
| **TiKV** | ✅ native (Percolator 2PC) | Snapshot isolation, linearizable Raft | ❌ multi-node minimum | ✅ excellent | ⚠️ native but **pre-1.0, immature** (the pain point) |
| **FoundationDB** | ✅ native | **Strict serializable** (gold standard) | ⚠️ single dev process only | ✅ excellent | ⚠️ binding over C API + BindingTester (mature) — but drags `libfdb_c` |
| **YugabyteDB** | ✅ native (Spanner-arch) | Serializable / SI, Raft, Jepsen-tested | ❌ `yugabyted` single-node is heavy | ✅ excellent | ✅ **mature via Postgres wire** (`tokio-postgres`) — but SQL, not KV |
| TiDB | ✅ (SQL over TiKV) | SI | ❌ | ✅ | SQL driver — but adds a SQL layer you don't need over TiKV |
| CockroachDB | ✅ | Serializable | ❌ | ✅ | pgwire |
| Google Spanner | ✅ | External consistency | ❌ | ✅ (managed) | ❌ |
| etcd | ⚠️ mini-txn (limited) | Linearizable | ✅ (in-mem dev, already used) | ⚠️ **not data-proportional — kB only** | ✅ |
| **redb** | ✅ (single-process ACID write txn) | Serializable (single node) | ✅✅ **pure-Rust, the point** | ❌ single-process | ✅ native (it *is* Rust) |
| sled | ⚠️ txns exist but beta | — | ✅ pure-Rust | ❌ | native — but **stalled** |
| SQLite | ✅ (ACID) | Serializable, single-writer | ✅ (but C) | ❌ | `rusqlite` (C FFI) |
| RocksDB | ✅ (`TransactionDB`, 1 node) | — | ✅ (but C++) | ❌ (engine, not a store) | `rust-rocksdb` (C++ FFI) |
| HBase | ❌ row / region-local only | Strong per-row | ❌ (JVM + HDFS) | ✅ | none real |
| Cassandra | ❌ single-partition LWT | Tunable, **eventual by default** | ❌ | ✅ | ❌ |
| ScyllaDB | ❌ single-partition LWT | Tunable | ❌ | ✅ | ❌ |
| Google Bigtable | ❌ single-row | Strong per-row | ❌ | ✅ (managed) | ❌ |
| MongoDB | ✅ multi-doc txn (4.0+) | Tunable, causal | ❌ (`mongod` heavy) | ✅ (sharded) | ✅ (official driver) |
| PostgreSQL | ✅ (ACID) | Serializable | ⚠️ server, not embeddable | ❌ **doesn't shard natively** | ✅ `tokio-postgres` |

### Matrix B — sovereignty & doctrine fit (can we adopt it under the house posture?)

| Candidate | Licence | Governance / continuity | Language / link-graph | Ops weight | Sovereignty |
|---|---|---|---|---|---|
| **TiKV** | Apache-2.0 | **CNCF graduated** (neutral foundation) — low relicense risk | Rust server + **pure-Rust client**; C++ RocksDB confined to the *server* | Medium (PD + multi-node) | ✅ EU-self-hostable |
| **FoundationDB** | Apache-2.0 | Community, ex-Apple, **no foundation** — medium | **`libfdb_c` (C) enters the link-graph** — the sovereignty cost | Medium | ✅ self-host; ⚠️ C in every gateway |
| **YugabyteDB** | Apache-2.0 (core; Anywhere = Polyform) | **Single-vendor, no foundation**, CLA-not-DCO — medium/high relicense risk | C++ / Postgres-fork server; Rust via pgwire | **Heavy** (full distributed SQL) | ✅ self-host |
| TiDB | Apache-2.0 | PingCAP (TiKV under it is CNCF-graduated) | +SQL layer | Heavier than TiKV | ✅ — but redundant vs TiKV-direct |
| CockroachDB | ❌ **CSL, proprietary** (since Nov 2024) + **mandatory telemetry** | Single-vendor, relicensed twice | pgwire | Medium | ❌ proprietary **and** phones home |
| Google Spanner | ❌ proprietary | Google | managed only | n/a | ❌ US-controlled |
| etcd | Apache-2.0 | CNCF graduated | Go server, Rust client | Light | ✅ — but **wrong role** (L5, kB) |
| **redb** | MIT/Apache-2.0 | Single maintainer, but small forkable codebase | **pure-Rust** ✅✅ | Trivial (embedded) | ✅✅ ideal for the embedded slot |
| sled | MIT/Apache-2.0 | **Effectively unmaintained** | pure-Rust | Trivial | ❌ maturity |
| SQLite | Public domain | Rock-solid, permanent | **C** | Trivial | ⚠️ licence/gov ✅; link-graph ❌ — redb dominates it |
| RocksDB | Apache-2.0 / GPLv2 | Meta | **C++** | Embedded | ❌ link-graph — ADR fallback only |
| HBase | Apache-2.0 | **ASF (best governance here)** | **JVM + HDFS** | Very heavy | ❌ on arch + ops, not licence |
| Cassandra | Apache-2.0 | ASF | JVM | Heavy | ❌ on arch |
| ScyllaDB | ❌ **source-available (2025.1)**; AGPL 6.2 final OSS | Single-vendor | C++ | Heavy | ❌ licence + arch |
| Google Bigtable | ❌ proprietary | Google | managed | n/a | ❌ US-controlled |
| MongoDB | ❌ **SSPL (non-OSI)** | Single-vendor | C++ server | Heavy | ❌ licence + wrong shape |
| PostgreSQL | PostgreSQL licence (permissive) | **Community — gold-standard governance** | C | Medium | ⚠️ gov/licence ✅ — but no native distributed tier; Citus (MS-owned) reintroduces a US vendor |

### What survives

**Embedded slot — effectively uncontested: `redb`.** Pure-Rust, single-process
ACID, multi-key within a write transaction, trivial to cross-compile to
NAS/musl. sled is unmaintained; SQLite and RocksDB reintroduce a C/C++
link-graph the embedded profile exists to avoid.

**Distributed slot — the genuine three-way** carried into the Decision above:
TiKV (governance + pure-Rust client, − client maturity), FoundationDB
(consistency + conformance, − governance and − link-graph purity), YugabyteDB
(mature client + link-graph purity, − governance + SQL impedance + ops weight).

### Rejected, with reason (so it is never relitigated)

- **Wide-column family** (HBase, Cassandra, ScyllaDB, Bigtable, Accumulo) — fail
  the multi-key gate (single-row / single-partition atomicity); most add
  JVM/HDFS weight or non-open licences.
- **CockroachDB** — proprietary CSL + mandatory telemetry (both since Nov 2024).
- **MongoDB** — SSPL (non-OSI), wrong data shape.
- **Google Spanner / Bigtable** — proprietary, US-controlled.
- **etcd** — already consumed at L5; not data-proportional, must not become a
  metadata store ("treating etcd as a database" is the classic L5 failure mode).
- **PostgreSQL** — permissive and superbly governed, but no native horizontal
  shard for the production tier; the reflexive "just use Postgres" answer routes
  you to Citus (Microsoft-owned) or … YugabyteDB.
- **sled / SQLite / RocksDB** — embedded-slot also-rans behind `redb` on maturity
  (sled) or link-graph purity (SQLite/RocksDB); RocksDB is retained only as the
  ADR-0008 engine fallback behind the trait.
