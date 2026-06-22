---
created: 23.06.2026 01:30
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#206"
tags:
  - proposal
  - management
  - administration
  - operability
  - observability
  - telemetry
  - operations
  - multi-tenancy
  - security
---
# Proposal: Management and administration — the operator surface

> Draft. The operator-facing **management and administration surface** for Wyrd:
> the API-first management plane (ADR-0013), the observability planes and audit
> log (ADR-0011, ADR-0012), the day-2 operations (§8.4), and multi-tenancy
> administration (ADR-0022). M3 built the *engine* — the declarative
> desired-state hook (`crates/custodian/src/desired_state.rs`) and the
> durability-plane telemetry (`crates/custodian/src/telemetry.rs`) — and
> deliberately deferred the operator-facing *surface* to ADR-0013. This proposal
> builds that surface. It is the **first real implementation** of the management
> contract, so it **proposes to ratify ADR-0011, ADR-0012, and ADR-0013** (all
> still Proposed) — Proposed → Accepted, subject to the architecture board. It is
> scoped to **single-zone** management — the M0–M4 product; the cross-zone global
> control plane (L2) is M6 and is explicitly out of scope.

## Motivation

**Operability is quality goal #3** — "the durability state of the system must be
observable, and the dangerous-but-routine operations (draining, upgrades,
recovery) must be safe, resumable, and observable" (§1.3). The rationale is that
at fleet scale draining, upgrading, and recovering "happen continuously, and they
are where data loss and outages actually originate, not steady-state serving"; an
operator must be able to answer *"is my data meeting its durability policy right
now?"* and run these procedures so that "a half-finished drain must resume, not
fall off a cliff" (§1.3; Q8–Q9).

M3 delivered the **mechanism** for this — the custodians reconcile, and they emit
the durability plane from their first commit (ADR-0011; proposal 0005). But M3
deliberately built **only the hook**: a `desired:dserver:<id>` ledger entry the
operator writes and a `ReconciliationStatus` the rebalance loop computes
(`crates/custodian/src/desired_state.rs:10-11` — "The full API-first management
surface and its CLI are ADR-0013, deferred"). There is **no operator-facing API,
no CLI, no auth, and no tenant administration** yet: the only network service in
the workspace is the `ChunkStore` gRPC for the data path
(`crates/proto/proto/wyrd/v0/chunk.proto`), and the only binary surface is a
hand-rolled dev-only CLI with four subcommands (`crates/server/src/cli.rs:57-60`,
documented dev-only at `:1-9`).

This proposal closes that gap. It builds the **operator surface** on top of the
M3 engine: an API-first management plane, its thin CLI, its authentication, the
three observability planes and the audit log, the four first-class day-2
operations, and multi-tenancy administration. It is the first implementation of
the management contract that ADR-0011/0012/0013 describe. All three remain
**status: Proposed** today; M3 *recommended* ratifying ADR-0011/0012 in its Open
questions but **deferred the act**, and M4 needed no flip (its load-bearing ADRs
were already Accepted, 0007:31-33). So per the ADR lifecycle — where Proposed →
Accepted is "the act of acceptance … intentional, not an automatic consequence of
merging" (ADR-0001) — this proposal, as the first real implementation of all three
contracts, is the natural vehicle to actually **perform the flip**, subject to the
architecture board (see Open questions on whether they may ratify ahead of the
foundational set).

## Design

The whole surface follows one principle and one altitude. The principle is
**declarative reconciliation, not imperative administration**: an operator
expresses *desired state* and the custodians reconcile reality toward it. The
altitude is the architecture's own charter for the API — "mostly read/write
desired state + observe reconciliation progress: a small, safe, auditable
surface" (§8.4). Every part below is a consequence of holding to those two.

A terminology note the glossary itself flags (§12): the **control plane** is L2
(the global namespace, placement, zone registry); the **management surface** is
the operator API (ADR-0013). They are distinct and this proposal is about the
*latter*. Single-zone, there is no separate L2 — desired state folds into the
zonal store (ADR-0020 decision 5) — but the surface is the same.

### The model — declarative reconciliation, built on the M3 hook

M3 already encodes the model in `crates/custodian/src/desired_state.rs`: the
operator writes desired state (`set_lifecycle` / `clear_lifecycle` over the
`desired:dserver:<id>` ledger), the rebalance loop reconciles, and the
**"policy changed" vs "policy satisfied"** distinction is made observable by
`ReconciliationStatus { NotRequested, Pending, Satisfied }` — where *Satisfied*
is computed from the committed placement records, not asserted
(`desired_state.rs:78-150`). This is the binding contract of ADR-0011's
declarative-management rule, stated in its Consequences (and in §8.4): "'Policy
changed' (recorded) and 'policy satisfied' (replicas exist) are distinct,
observable moments."

The management surface is the generalization of this hook to every desired-state
object, exposed over an API. It invents no reconciliation logic; it **wraps the
existing custodian functions** and the loops M3 built. Crucially, the management
plane **never drives D servers directly** and never issues imperative, ordered
placement commands — that would violate the custodians' placement authority
(ADR-0010: "placement and durability authority must live in the custodians"; §7.3)
and ADR-0011 rule 2. It writes desired state; the custodians own convergence.

### The management API — the source of truth (ADR-0013)

ADR-0013 is the charter: "A clean gRPC/REST management API is the source of truth
for desired state and reconciliation status. Ship a thin CLI over it for v1.
Defer a web UI. The API is the authoritative interface; CLI, dashboards, and any
future UI are consumers of it." The API is therefore the system of record — even
for telemetry, "the management API is the system of record; Grafana is one
consumer" (ADR-0012).

- **Shape.** A new `Management` gRPC service, defined in a new proto beside
  `chunk.proto` / `commit.proto` (the `proto` crate is the existing wire seam). A
  REST/JSON surface is offered as a gateway over the same service (gRPC-first; see
  Open questions). The method set is deliberately minimal and maps to the model:
  *write desired state* (drain/decommission a D server, set a tenant's policy
  bundle, register added capacity), *read desired state*, and *observe
  reconciliation* (the `changed`-vs-`satisfied` status per object, plus the
  durability/capacity planes and the audit/event stream). It wraps the M3
  `desired_state` functions and the telemetry handle as RPCs.
- **`changed` vs `satisfied` as a first-class field.** Every desired-state object
  carries an observed-reconciliation status (the Kubernetes
  generation/observed-generation shape); the existing per-file `meta:version`
  counter (§8.1) is the natural generation/fence hook. The operator's *"is my
  change done yet?"* is answered by reading the status, never inferred.
- **Where it lives.** Following ADR-0016 coarse-then-split (trait boundaries from
  day one; only `server` wires concretes), the management role starts as a new
  subcommand + tonic role module in `server` — the exact precedent the networked
  `d-server` role set (`crates/server/src/dserver.rs`) — and splits into a
  dedicated `management` crate when the boundary firms. Either way it depends
  **only on the `traits` seam and the custodian desired-state functions**, never a
  concrete backend (ADR-0016). The server is **stateless**, so it is
  orchestrator-friendly and horizontally scalable, discovering peers through L5
  coordination, never an orchestrator API (ADR-0010).

### The thin CLI — a consumer, not a second source of truth (ADR-0013)

A thin CLI ships over the API for v1: a clap-based gRPC client carrying **no
management logic of its own**. It is distinct from today's bespoke dev-only
parser (`crates/server/src/cli.rs:568-601`), which stays for local dev. "Automation-
friendly and scriptable from day one" (ADR-0013) is the property to preserve — the
CLI, any future dashboards, and the deferred web UI are all consumers of the one
API.

### Management-plane authentication and authorization

The management API authenticates with **OIDC + mTLS** — the only external surface
that adds mTLS on top of an identity token (§8.5). The design:

- **A separate listener** (a recommendation of this proposal, not a quoted
  architecture fact). The management plane is its own socket/endpoint with its own
  (stronger) auth profile, distinct from the data-plane S3/WebDAV/SDK gateway. §3.2
  lists Management as a distinct inbound *interface* — a logical separation; the
  architecture does not pin process topology, so the separate listener is our
  inference, not a stated requirement. "The gateway is the authentication boundary"
  (§8.5) is a statement about *layering* (auth happens at L1, never re-checked
  below), not about co-locating every surface in one process. A separate listener lets the operator network-restrict management
  (admin VLAN / private ingress) and scale it independently, and matches its
  larger blast radius (it can drain a zone or rewrite a tenant's replication
  factor).
- **mTLS is a composition, not a new PKI.** The management-plane mTLS reuses the
  **same provider-CA / SPIFFE-SPIRE fabric** that authenticates services
  (ADR-0005, ADR-0025) — "the mTLS fabric is an internal PKI, not a
  cross-organization trust system" (ADR-0005). mTLS proves the calling client is a
  sanctioned operator endpoint inside the provider's one trust domain and gives a
  fail-closed channel; **OIDC layered on top identifies the human/automation
  principal** behind that endpoint, so the audit log records *who* acted, not
  merely a network position. A request missing **either** factor fails closed,
  mirroring the fabric's no-plaintext-fallback rule (ADR-0025).
- **Authorization.** A small declarative RBAC over the API's verbs — *read-only*
  (observe), *operator* (desired-state edits: drain/add capacity, rolling upgrade,
  tenant policy CRUD, backup/restore), and a separately-gated *high-consequence*
  subset (tenant deletion / crypto-erase, GC-affecting actions) — keeping the
  surface "small, safe, auditable" (§8.4). An operator role may be **scoped to a
  tenant subtree** (a support engineer who may adjust only one tenant's quota),
  realized as a scope claim on the OIDC principal checked against the tenant's L2
  record — the same *L2-records-policy, L1-enforces* pattern the data plane uses
  (ADR-0022). **Tenant administration is a different plane:** a tenant has its own
  identity domain / OIDC realm and operates within its own namespace through the
  data surfaces (ADR-0022, which keeps the policy/ACL tier centralized in L2); the
  provider operator sets the tenant's *policy bundle* through the management API. In
  v1 a tenant does **not** call the operator management API (self-service
  delegation is a deliberate later option — see Open questions).

### Observability — three planes and the audit log (ADR-0011, ADR-0012)

This builds directly on M3's `DurabilityTelemetry`
(`crates/custodian/src/telemetry.rs`): OpenTelemetry via `tracing` +
`tracing-opentelemetry`, **dual-exported** over both a Prometheus-scrapeable
registry and OTLP push, with **no backend hardcoded** (`ExporterConfig
{ Prometheus, Otlp, Both }`, `telemetry.rs:27-61`; ADR-0012). The proposal extends
emission to the full §8.3 plane model:

- **Request plane** — RED metrics per layer + traces (the data-path surfaces M2/M4
  add).
- **Durability plane** — the differentiator, "the part that must be designed in":
  the **five single-zone metrics** M3 already emits — under-replicated chunk
  count, repair-queue depth, time-to-repair distribution, scrub coverage,
  scrub-detected corruption rate. (The sixth ADR-0011 metric, replication lag per
  zone pair, has no zone pair single-zone and is **deferred to M5**, per proposal
  0005.) This plane answers the headline operability query directly.
- **Capacity plane** — per-server / per-zone / **per-failure-domain** utilization
  and growth rate (§8.3, §8.9); the leading indicator for the add-capacity
  decision.
- **The append-only audit/event log** — significant state transitions: placement,
  repairs, admissions, **policy changes, deletions** (§8.3). It is both the
  operational-debugging record and the **compliance / GDPR-deletion proof** (§6.7,
  §14), and is bound to the OIDC operator principal so every desired-state change
  records *who*.

The **management API is the system of record**; OTel/Prometheus/Grafana are
replaceable consumers. Curated Grafana dashboards (durability, capacity, request)
ship as version-controlled artifacts under `deploy/grafana/` — "a gift, not a
gate" (ADR-0012). The instrumentation **seam** and the metric/audit inventory are
v1; **polished dashboards and alerting are explicitly later and cheap to add once
the hooks emit** (ADR-0011). Validation reuses M3's in-process read-back
(`DurabilityTelemetry::gather_prometheus`, `telemetry.rs:135-142`) plus a live
scrape / OTLP collector run off-Check.

### Day-2 operations — first-class: safe, resumable, observable (§8.4)

§8.4 names exactly four operations that "must be first-class, safe, resumable, and
observable." Each is a desired-state write reconciled by the custodians, with both
the *changed* and *satisfied* moments observable.

- **Add / drain capacity.** Drain "separates real storage systems from toys"
  (§8.4) and is the worked example of the whole model. It already exists in M3 as
  `set_lifecycle(Draining | Decommissioning)` + the rebalance loop; the management
  API exposes it and its `ReconciliationStatus`. The acceptance bar is Q9: data
  evacuated **while maintaining redundancy and failure-domain invariants
  throughout** (not merely at the end), **resumable**, with **observable
  progress**. *Add capacity* registers a new D server / failure domain (the
  least-specified op — see Open questions); the capacity plane is its leading
  indicator and a drain must respect per-failure-domain room (§8.9).
- **Rolling upgrades.** "Version skew is the normal state" (§8.4). Safety rests on
  two compatibility axes already decided (ADR-0002): the **wire** is versioned
  protobuf evolved by addition with one-version-gap interop (so upgrade one
  version at a time, never skip), and the **on-disk format** is version-tagged and
  read-never-rejected (no flag-day reformat). The new `Management` service obeys
  the same evolve-by-addition rule. The acceptance bar is Q8: **no downtime, no
  data errors, neighbours interoperate across one version gap** — validated under
  DST with mixed-version nodes, a CI mixed-version matrix, and a DR drill (§8.7,
  §10). Where an upgrade touches placement (e.g. drain-before-upgrade of a
  stateful D server), it is expressed as desired state; the custodians keep
  placement authority.
- **Policy changes as managed rollouts.** A policy edit is *changed* when recorded
  and *satisfied* only when reality (replicas, placement) matches — "different
  moments" (§8.4). The API models this as the per-object reconciliation status, so
  an operator watches a replication-factor change converge.
- **Backup / restore.** Asymmetric by layer (§8.2). §8.2 applies the
  out-of-band-backup principle system-wide; the per-data-shape *mechanism* is the
  synthesis this proposal draws from it together with the restore ordering of
  §6.4–6.5: **chunk data is not separately byte-backed-up** — it is EC-durable
  intra-zone and re-replicated from survivors by the custodians once the map
  exists, so out-of-band backup is reserved for the small, precious,
  non-reconstructible data: the **metadata** (the zonal L4 store — "small but
  precious; losing it orphans all chunks") and the **audit log**, because
  "backups must not depend on the system they back up" and "replication faithfully
  replicates logical disasters (bad migration, errant delete)" (§8.2). Restore
  follows a strict order (§6.5): (1) L5 coordination, (2) L2/L4 metadata from the
  independent backup, (3) L3 verify + re-replicate the bytes from survivors —
  restoring bytes before the map is useless. The DR ordering is a **runbook that
  must be written and drilled** before it is needed. (Single-zone backup/restore
  is in scope here; the multi-zone disaster-recovery *drill* is M7.)

### Multi-tenancy administration (ADR-0022)

A tenant is "the unit of isolation, policy, and billing — a namespace partition
plus an identity domain," owning a subtree of the namespace, a credential set, and
a **policy bundle: residency, replication factor, encryption, quotas, rate
limits** (ADR-0022). The operator administers that bundle through the management
API as **desired state**; tenant lifecycle, quota, and per-tenant replication
factor are reconciled like any other policy. Per-tenant durability is the
per-zone scheme — none / replication(n) / rs(k,m) (ADR-0008) — within a zone;
synchronous N-zone replication is a per-tenant opt-in but is **cross-zone (M5+)**.

Isolation is enforced **at L1 and L2, never below** (ADR-0022): the gateway (L1)
authenticates the tenant and checks quota (bytes / object count; hard rejects,
soft warns, at admission) and rate (per-tenant token bucket); authorization
resolves against L2 ACLs; D servers and the metadata store stay tenant-oblivious,
trusting an admitted request. The management API's job is to make the tenant
policy bundle a first-class desired-state object; the runtime enforcement points
already have a home.

### Single-zone now, the global control plane (L2) later — the scope fence

A single-zone deployment **has** desired state and reconciliation; it is folded
into the zonal store rather than a geo-distributed L2 (ADR-0020: "In a single-zone
deployment the namespace IS the zonal store … becomes a distinct geo-distributed
TiDB deployment only at the provider-fleet tier"). This proposal targets that
single-zone product (the M0–M4 result). The **same desired-state / reconciliation
API shape** extends across zones later: only the *backend* changes (zonal store →
geo-distributed TiDB L2) and *cross-zone verbs* are added — the per-zone-pair
replication-lag metric arrives with cross-zone replication at **M5**; placement
across zones and residency redirect arrive with the global control plane at
**M6**. The v1 API must be designed so the cross-zone surface is **additive, not a
re-architecture** — consistent with ADR-0020 calling the global plane "a
composition choice."

### Phasing — v1 vs deferred

**v1 (this proposal):** the gRPC management API + thin CLI; OIDC+mTLS management
auth + operator RBAC; the three telemetry planes (five single-zone durability
metrics, capacity, request) on the dual Prometheus/OTLP seam; the append-only
audit log; the four day-2 ops (drain/decommission, rolling upgrade,
policy-as-rollout, single-zone backup/restore); multi-tenancy administration of
the policy bundle. (M3 already shipped the telemetry seam and the
drain/decommission hook; v1 generalizes the hook into the API and fills in the
rest.)

**Deferred:** the **web UI** (a consumer of the v1 API — ADR-0013; its browser
transport and auth prerequisites are recorded in Open questions); **polished
dashboards and alerting** ("a gift, not a gate" — ADR-0012; "later and cheap"
— ADR-0011); the **cross-zone global control plane** (L2 placement, geo namespace)
→ **M6** (and the per-zone-pair replication-lag metric with cross-zone replication
→ **M5**); **Zanzibar-class fine-grained authorization** (v1 is
POSIX-ish ACLs + bucket/prefix policy, §8.5); **tenant self-service admin
delegation** (this proposal keeps administration operator-only in v1; ADR-0022
keeps the policy tier centralized, so delegation is a deliberate later option).

## Alternatives considered

- **CLI-first or UI-first management** — rejected (ADR-0013). The audience is
  Kubernetes-shaped; the authoritative interface must be programmatic and
  scriptable. A UI built first would have to be re-architected; built as a
  consumer of the API it need not be.
- **Imperative admin RPCs** (the management API drives D servers / issues ordered
  placement commands) — rejected. It violates the custodians' placement authority
  (ADR-0010; §7.3) and ADR-0011 rule 2. Management writes desired state;
  reconciliation is the custodians' job. This also keeps the surface "small, safe,
  auditable."
- **Co-locating management on the data-plane gateway** — rejected in favour of a
  **separate listener**: management has a stronger auth profile (OIDC + mTLS), a
  larger blast radius, and benefits from independent network restriction and
  scaling. "The gateway is the authentication boundary" is satisfied by management
  being its own L1 authentication point, not by sharing a process.
- **A separate PKI for management clients** — rejected. Reuse the one provider
  CA / SPIFFE-SPIRE fabric (ADR-0005, ADR-0025); operator staff and tooling live
  inside the single provider trust domain. Management auth is a composition of the
  existing mTLS fabric + OIDC, not a new mechanism.
- **Hardcoding Prometheus/Grafana (or any one backend)** — rejected (ADR-0012).
  OTel is the only telemetry commitment; everything downstream is the operator's
  swappable choice. The management API, not a dashboard, is the system of record.
- **Backing up chunk bytes** — rejected. Bytes are EC-durable and
  re-replicated from survivors; what needs out-of-band backup is the metadata and
  the audit log, which survive the logical disasters replication faithfully
  copies.
- **A dedicated `management` crate from day one vs a role in `server`** — deferred
  to implementation (ADR-0016 coarse-then-split). Starting as a role in `server`
  is cheapest and follows the `d-server` precedent; splitting when the boundary
  firms honours the trait-boundary rule. Recorded as an Open question.

## Graduation criteria

- **Drain (Q9)** — a drained/decommissioned D server is evacuated while redundancy
  and failure-domain invariants hold **throughout**; the operation is **resumable**
  after a crash; progress is observable as `ReconciliationStatus` moving
  Pending → Satisfied. Proven in DST and in a DR drill.
- **Rolling upgrade (Q8)** — a version-skewed fleet upgrades with **no downtime,
  no data errors, one-version-gap interop**, including the new `Management`
  service. Validated by DST mixed-version nodes, a CI mixed-version matrix, and a
  DR drill.
- **API is the source of truth** — desired-state writes land; reconciliation
  status reads back the `changed`-vs-`satisfied` state per object; the CLI is a
  pure consumer carrying no management logic.
- **Auth** — the management listener enforces OIDC + mTLS **fail-closed on either
  factor**; operator RBAC (read-only / operator / high-consequence, optionally
  tenant-scoped) is enforced; every desired-state change records the OIDC operator
  principal in the audit log.
- **Telemetry** — the three planes emit (the five single-zone durability metrics,
  per-failure-domain capacity, request RED) over the dual Prometheus + OTLP seam
  with no hardcoded backend; the append-only audit log records placement/repairs/
  admissions/policy-changes/deletions. Validated by in-process assertion (the M3
  `gather_prometheus` pattern) and a live scrape/collector off-Check.
- **Multi-tenancy** — a tenant's policy bundle (residency, replication factor,
  encryption, quotas, rate limits) is a desired-state object; quota and rate are
  enforced fail-closed at L1/L2 admission; the storage tier stays tenant-oblivious.
- **Backup / restore** — metadata and the audit log are backed up out-of-band to
  independent storage; the restore order (L5 → L2/L4 → L3) is written as a runbook
  and exercised in a drill; chunk bytes are re-replicated from survivors, not
  restored.
- **Ratification** — on architecture-board agreement (Open questions), ADR-0011,
  ADR-0012, and ADR-0013 are moved Proposed → Accepted (both the ADR front-matter
  and the `adr/README.md` index), per ADR-0001, as the first implementation of
  their contract.
- DST / property tests green and seed-reproducible (ADR-0009); `fmt`/`clippy`
  clean; `cargo-deny` passes with the new dependencies (tonic management service,
  the OIDC verifier, the CLI client).

## Backward compatibility

- **On-disk format** — unchanged; management touches no chunk layout.
- **Wire** — the `Management` gRPC service is **additive** (a new proto/service,
  versioned protobuf, ADR-0002); the `ChunkStore` data-path wire is untouched. The
  one-version-gap evolve-by-addition rule applies to the management service from
  its first release.
- **Existing deployments** — redb / single-binary stays the **dev/eval** profile
  with no production-durability promise (ADR-0014); production management targets
  the multi-node profile. The dev-only bespoke CLI is superseded by the thin gRPC
  CLI — a pre-1.0 dev-tooling change, not a public-API break.
- **The M3 hook** — the `desired:dserver:` ledger encoding and `ReconciliationStatus`
  shape are explicitly **illustrative**; the *two observable moments* are the
  binding contract (`desired_state.rs:13-18`). The management API may formalize the
  encoding behind the API without breaking that contract.
- **Public API** — this proposal **defines** the first management API; from
  acceptance it carries compatibility duties (versioned, evolve-by-addition,
  one-version-gap during rollout).

## Open questions

- **Coarse vs split** — a management role in `server` (cheapest, d-server
  precedent) vs a dedicated `management` crate from the start (cleaner under the
  ADR-0016 trait-boundary rule, since `server` is already the S3-gateway / dev
  tooling crate). Decide at implementation.
- **gRPC + REST** — gRPC-first with a REST/JSON gateway over the same service, or
  both native? ADR-0013 says "gRPC/REST"; the exact REST story is unspecified.
- **Operator RBAC model** — the roles and per-tenant operator scoping are
  introduced by this proposal; the ADRs commit only to OIDC+mTLS authn and
  data-plane ACLs. The exact role set and scoping claim format need pinning.
- **Operator client-certificate issuance** — whether operator mTLS certs are
  issued by the same SPIRE instance as workload certs or a sibling intermediate
  under the provider root is an implementation detail to specify (ADR-0025).
- **Web UI prerequisites** — the web UI is deferred (ADR-0013) and is a *consumer*
  of the v1 API, but a **browser** frontend (unlike the v1 CLI) needs two things
  this proposal must settle so the UI is genuinely additive, not a re-architecture:
  (1) **transport** — a browser cannot speak raw gRPC, so the API must expose a
  **REST/JSON or gRPC-web** surface (this is the gateway half of the "gRPC + REST"
  question above; the CLI does not force it, the UI does); and (2) **browser auth**
  — the management plane is OIDC + mTLS, but browser client certificates are
  impractical, so the UI path needs OIDC in the browser with the **mTLS identity
  held by a backend-for-frontend** (a server-side consumer of the management API),
  not a client cert in the browser. Both are reserved seats, not v1 work — naming
  them keeps the deferred UI a clean consumer.
- **Backup mechanics** — cadence, format (metadata-store snapshot vs logical
  export), and audit-log retention / out-of-band posture are unspecified (§8.2
  gives the principle, not the operation).
- **Drain progress & reversibility** — Q9 requires resumability; the docs are
  silent on checkpoint granularity and on aborting/rolling back an in-flight
  desired-state change (cancel a drain mid-flight, roll back a policy change).
- **Capacity-add flow** — the least-specified op: the operator procedure to
  introduce a new D server / failure domain (§7.3, §8.9 cover the signal, not the
  flow).
- **Tenant self-service administration** — ADR-0022 keeps the policy/ACL tier
  centralized; v1 keeps administration operator-only. Whether a tenant may
  self-serve parts of its own policy bundle is a question this proposal raises (not
  one ADR-0022 records) and leaves for later.
- **Batch ratification** — ADR-0001 holds the foundational ADR set at Proposed
  "until reviewed and ratified together." Confirm with the architecture board
  whether ADR-0011/0012/0013 (and the management/admin slice of ADR-0022 — see
  Relationship) may ratify ahead of the set with this proposal, or must move as a
  group.

## Out of scope — routed elsewhere

- **The cross-zone global control plane** (L2 placement, the geo-distributed global
  namespace / registry / replica catalog, cross-region/residency policy) → **M6**
  (ADR-0020, status Proposed).
- **Cross-zone data replication** (L3), the sync-N-zone per-tenant opt-in, and the
  per-zone-pair replication-lag metric (which has no zone pair to measure until
  cross-zone replication exists) → **M5**.
- **The multi-zone disaster-recovery drill** → **M7**. Single-zone backup/restore
  is in scope; the cross-zone DR exercise is a distinct milestone.
- **The web management UI**, **polished dashboards and alerting**, and
  **Zanzibar-class fine-grained authorization** — deferred consumers/features, not
  re-architectures.
- **Physical / dedicated-hardware per-tenant isolation** — a possible future tier,
  explicitly not v1 (ADR-0022).

## Relationship to existing decisions

- **Proposes to ratify** ADR-0011 (durability telemetry + declarative management),
  ADR-0012 (OpenTelemetry instrumentation), and ADR-0013 (API-first management) —
  Proposed → Accepted, as their first real implementation, subject to the
  architecture board's agreement on ratifying ahead of the foundational set (Open
  questions).
- **First implements the management/admin slice of** ADR-0022 (multi-tenancy) — the
  tenant policy bundle as a desired-state object. ADR-0022 is itself still
  **Proposed**, and its runtime isolation-enforcement points land with later
  milestones (L1/L2 admission at M2+/M6), so this proposal implements only its
  administration slice and defers its ratification (carried in the Batch
  ratification open question).
- **Builds on** ADR-0005 / ADR-0025 (provider CA, service mTLS), ADR-0008 (per-zone
  durability schemes), ADR-0002 (versioned
  protobuf + on-disk format — the rolling-upgrade foundation), ADR-0010 (pluggable
  substrate / operator), ADR-0014 (single-binary dev-only), ADR-0016 (crate
  structure), ADR-0009 (DST as the correctness authority), and the M3 custodian
  work (proposal 0005: `desired_state.rs`, `telemetry.rs`, the rebalance loop).
- **Scoped against** ADR-0020 (the global namespace / L2 control plane, M6) and the
  implementation arc (proposal 0002: M5 cross-zone replication, M6 global control
  plane, M7 disaster-recovery drill).
