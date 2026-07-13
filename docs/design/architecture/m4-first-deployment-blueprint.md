---
created: 23.06.2026 00:00
type: architecture
status: living
tags:
  - architecture
  - deployment
  - operations
  - milestone-4
---
# M4 first-deployment blueprint — single-zone, homelab and Hetzner

> An operational note, not a normative spec. It describes how to stand up the
> first *real* single-zone Wyrd deployment at the first-deployment point (the
> M4 data plane plus the M5 trust fabric; the ★ Step-2 release is M8 —
> proposal 0013) — the
> "Small multi-node Production" profile (a distributed metadata store, a separate
> 3-node etcd coordination ensemble, local-disk D servers, gateway, and custodian) —
> in two concrete shapes: a homelab and a Hetzner rental.
> The governing constraint throughout is making the **RS(6,3) durability math
> actually true**, which is a *topology* property, not a process-count property.
> The conceptual framing lives in the [deployment view](07-deployment-view.md)
> (§7.3–§7.4); this note is the operational detail behind it.

> [!IMPORTANT]
> **The metadata tier in this blueprint is written against TiKV + PD, and that is no
> longer the production backend.** ADR-0042 supersedes ADR-0008 and chooses
> **FoundationDB**, which passed the M4 fault + contention battery (#442); the
> canonical single-zone stack is `deploy/small-multi-node-fdb/`. TiKV is a **retained
> fallback with active development stood down** (#443) — kept and buildable, but
> carrying unpatched `tikv-client` advisories (#543) and a red Tier-1 fault leg
> (#537).
>
> Everything in this note that is **topology** — the RS(6,3) failure-domain math, the
> D-server spread, the control-tier/data-tier asymmetry, the "back up the map, not the
> fragments" model, the restore *ordering* — is backend-independent and stands
> unchanged. Read "the metadata tier" wherever it says "TiKV + PD": FoundationDB
> occupies the same slot, with the same criticality and the same blast radius.
>
> What does **not** yet exist is the FoundationDB *operational* detail that would
> replace the TiKV-specific commands and analysis below: the `fdbbackup`/`fdbrestore`
> equivalents of the BR snapshot model, FDB's process classes and their placement, its
> PITR/RPO story, and re-sized control-plane nodes. Those are deliberately **not
> invented here** — writing unverified backup and restore instructions into a
> first-deployment runbook is worse than admitting the gap. They are a named follow-up
> (see *Backup model* below), and until that lands the TiKV-shaped operational
> sections are **historical**: accurate for the fallback, not instructions for a new
> FoundationDB deployment.

## The one number that drives everything: RS(6,3)

The default scheme is **RS(6,3)** — 6 data + 3 parity = **9 fragments per chunk**,
any **6** of which reconstruct the data. The system tolerates the loss of **up to
3 fragments** of a chunk before that chunk is unrecoverable. The custodian's job
is to rebuild lost fragments back to full redundancy *faster than* further loss
accumulates (durability ≈ the probability that more than 3 fragments are lost
within one repair window).

This number dictates the **failure-domain** requirement, and it is the single
thing most first deployments get wrong. M3's placement selector spreads a chunk's
9 fragments across **distinct failure domains**. The durability math is only real
if those domains **fail independently**. If 9 fragments land on 9 processes that
share one power supply, one disk, or one machine, then RS(6,3) is a lie — one
power cut takes all 9 and the data is gone despite "9× redundancy."

So the central design question for *any* M4 deployment is: **what is your unit of
independent failure, and do you have enough of them?** With RS(6,3) you want
**at least 9 independent failure domains** to place one fragment each (ideal), or
fewer domains with multiple fragments each and a consciously-reduced tolerance.
Everything below is organized around getting that right.

## Component inventory (both variants)

The M4 stack has seven process roles. Their *placement* differs between homelab and
Hetzner; the *roles* do not.

| Role | What it is | Failure-domain sensitivity |
|------|-----------|---------------------------|
| **D servers** | Dumb fragment storage; one fragment of each chunk lands here. The thing whose independent failure the durability math depends on. | **Critical** — these define the failure domains. Spread them. |
| **Metadata store** | The distributed metadata store (the commit point). Holds inodes/dirents/chunk-maps. Small but precious — losing it orphans all chunks. **FoundationDB** in production (ADR-0042): a ≥3-process cluster, `double ssd` redundancy. On the retained TiKV fallback (#443) this role is **TiKV** (Raft, 3×) *plus* **PD**, TiKV's own placement driver/coordinator, which embeds an internal etcd and is distinct from the L5 coordination etcd below. | **High** — quorum-replicated; wants its own independent nodes, not co-located with D servers ideally. Lose quorum and metadata stalls. |
| **etcd (L5 coordination)** | The `Coordination` seam ([ADR-0006](../adr/0006-etcd-for-coordination.md)): service discovery, leader election, distributed locks (with fencing), and **D-server registration + leases**. A **separate 3-node ensemble** from TiKV's PD; every role dials it as `--coordination <L5-endpoint>`. | **High** — 3 nodes for quorum; lose 2 and discovery/registration/leader-election stall. Small but control-critical. |
| **CA (step-ca)** | The internal PKI that issues short-lived mTLS certs for the fabric ([ADR-0036](../adr/0036-internal-ca-step-ca-spire.md)); the certificate is each component's identity ([ADR-0025](../adr/0025-internal-service-to-service-trust.md)). step-ca now, SPIRE reserved for fleet scale; the dev profile uses a built-in self-signed CA behind the same seam. | **High** — fail-closed mTLS (ADR-0025) makes an unreachable CA halt *every* new dial and cert rotation; run it **HA, off the D-server hosts**. |
| **Gateway** | Stateless S3 front door; embeds the client library (chunk, EC, commit). | **Low** — no durable state, restartable. Safe for **active/active** (#477): inodes from the shared `meta:next_inode` CAS allocator, chunk ids coordination-free (random epoch, ADR-0019). |
| **Custodian** | Runs GC/scrub/reconstruct/rebalance; emits durability telemetry. **Run exactly one** against a distributed store: leadership today is granted by the *process-local* `MemCoordination`, so two custodians on a shared FDB/TiKV store both self-grant and reconcile concurrently (no corruption — the repoint is a CAS commit — but duplicated repair work). Cross-host fencing via the etcd `Coordination` seam is #365. | **Low** — stateless logic; a restart resumes. |

The asymmetry to internalize: **D servers carry the durability**, **the metadata store
carries the map**, **etcd carries the coordination plane**, and **step-ca carries the
trust plane** (all three control tiers are precious, small, and HA/quorum-replicated),
and **gateway+custodian are stateless** (cheap to restart/move). A good topology spends
its failure-domain budget on the D servers first, gives the metadata, coordination
(etcd), and trust (step-ca) tiers their own HA/quorum spread, and treats
gateway+custodian as movable. This asymmetry is backend-independent: it holds
identically whether the metadata tier is FoundationDB or the TiKV+PD fallback. The same asymmetry drives
**backup** (see *Backup model* below): only the metadata (and the small L5 config)
is backed up out-of-band — D-server fragments are **not**, because EC + custodian
reconstruction already protects them.

---

## Variant A — Homelab

The honest framing first: a homelab on one desk **shares a building, a power
circuit, and an internet connection**, so its true independent-failure unit is the
**individual machine and its disk**, not anything finer. RS(6,3) across 9
fragments therefore wants **enough independent machines/disks** that no single
physical failure takes more than 3 fragments of any chunk. You will almost
certainly have fewer than 9 machines, so the design is about *honest reduced
tolerance*, not pretending to datacenter independence.

### A.1 The minimum honest homelab (small)

The smallest topology that makes RS(6,3) *mean something* rather than nothing:

- **6 D-server nodes**, each a separate physical machine with its own disk and —
  ideally — its own power brick (so a single PSU or disk failure costs at most one
  fragment). Cheap N100/N305 mini-PCs or Raspberry Pi 5s with NVMe HATs work; **use
  real SSDs, not SD cards** (SD cards lie about fsync and will mask or fake
  durability bugs).
- **9 fragments across 6 domains** means some domains hold 2 fragments. With 6
  domains and RS(6,3), losing **one** whole node costs at most 2 fragments (still
  within the 3-tolerance), so the cluster survives **any single-node loss** and
  the custodian rebuilds. Losing two nodes *might* exceed tolerance depending on
  placement — so 6 domains gives you **single-node-fault tolerance**, honestly
  stated. (To survive *two* arbitrary node losses you need ≥ 9 domains so no node
  holds more than one fragment of a chunk — see A.2.)
- **metadata store + etcd**: 3 small nodes forming the **control quorum** — TiKV and PD
  (metadata) plus the L5 **etcd** coordination ensemble, co-located (all three are
  light). These should be **3 *different* machines** from each other for Raft/etcd
  quorum, but in a small homelab they can share machines with D servers if you must
  — accepting that such a machine is now a domain whose loss hits storage *and*
  control. Cleaner: 3 dedicated small nodes for the metadata store + etcd. (etcd can have its own
  3 nodes if you want metadata and coordination to fail independently; for M4 small,
  co-locating it with PD is the standard, honest simplification — note both default
  to client port `2379`, so remap one when co-located.)
- **Gateway + custodian**: run on any node (or a spare), stateless. One of each is
  fine; the custodian leader-elects (via etcd) so a second is optional.

**Machine count, minimum honest:** ~6 D-server boxes + 3 metadata+etcd control boxes
= **9 machines**, or fold the control tier onto 3 of the D-server boxes for **6
machines** with the stated coupling caveat. Gateway/custodian ride along.

### A.2 The "real failure-domain" homelab (better)

To make RS(6,3) deliver its *full* 3-fault tolerance, you want **9 independent
domains, one fragment each**:

- **9 D-server nodes**, one fragment of each chunk per node — any **3** node losses
  survived, custodian rebuilds. This is RS(6,3) at full strength.
- Group them into **power/switch domains** if you can: e.g. 3 nodes per power strip
  × 3 strips, and label the failure domains by strip (the M3 selector consumes
  these labels). Then a whole *strip* failing (3 nodes) is still exactly the
  3-fault tolerance boundary — the data survives a power-strip loss. This is the
  homelab approximation of "rack/power/switch" domains from
  [§7.3](07-deployment-view.md).
- **metadata store + etcd on 3 dedicated control nodes**, ideally on a different power
  strip again — the metadata *and* L5 coordination quorum.
- Total: **~12 machines** (9 D + 3 metadata+etcd control nodes). This is a *serious*
  homelab but it makes the durability story genuinely true rather than aspirational.

### A.3 Homelab honesty box

Even A.2 shares **one building, one grid feed, one ISP**. So the homelab is a
*real test of disk/server/power-strip independence and the full M3 repair story* —
which is genuinely valuable and the thing DST can't give you — but it is **not**
disaster-recoverable (no second site; that's M9+). Treat homelab M4 as: a real,
production-durable-*against-component-failure* store for **non-sole-copy data** —
excellent for learning, dogfooding, demos, and feeding real faults back into DST,
**not** for the only copy of anything you can't lose. Apply
[§8.2](08-crosscutting-concepts.md)'s rule: keep an out-of-band backup that does
not depend on this cluster.

---

## Variant B — Hetzner

Hetzner changes the calculus in two ways that matter: (1) you can get **genuine
independent failure domains** cheaply (separate physical servers, and across
their EU locations even separate *datacenters*), and (2) it's **rented and
on-demand**, so you pay only while testing and it's EU-sovereign (on-message for
the project). For M4 *single-zone*, you stay within **one region** (cross-region
is M9+), but you use Hetzner's physical-server independence to make the
failure-domain math honest in a way one desk cannot.

### B.1 The Hetzner single-zone topology

"Single zone" = one Hetzner location (e.g. Falkenstein **or** Nuremberg **or**
Helsinki — pick one for M4; multi-location is M9). Within it:

- **D servers — Cloud `CX` servers for the campaign, bare-metal for the benchmark
  (see §B.3), as your failure domains.** A separate Hetzner server — cloud or
  bare-metal — each with a real NVMe gives you honest per-server independence:
  separate hardware, separate failure. For RS(6,3) at full strength, **9 D-server
  instances** (one fragment each — 3-fault tolerance). For a cheaper first cut,
  **6** (single-fault tolerance, as in A.1). Bare-metal gives honest fsync/NVMe
  performance numbers; cloud servers are easier to spin up/down for a test campaign.
- **metadata store + etcd — 3 dedicated cloud servers**, separate from the D servers,
  forming the **control quorum**: TiKV + PD for metadata **and** the L5 **etcd**
  coordination ensemble ([ADR-0006](../adr/0006-etcd-for-coordination.md)) that the
  `--coordination <L5-endpoint>` of every role dials. All three are light at this
  scale, so co-locating them on the same 3 nodes is the standard M4 choice (remap
  one of the two `2379` defaults); give etcd its own 3 nodes only if you want
  metadata and coordination to fail independently. Keeping the control tier off the
  D-server hosts means a control-node loss and a storage-node loss are independent
  events.
- **Internal CA (step-ca) — on the 3 control nodes (or a small dedicated node).**
  Issues the short-lived mTLS certs the whole fabric dials with
  ([ADR-0036](../adr/0036-internal-ca-step-ca-spire.md)). Because mTLS is fail-closed
  (ADR-0025), it **must be up before any other role** and run **HA, off the D-server
  hosts** ([§7.3](07-deployment-view.md)). HA is **not** automatic from running three
  copies: step-ca's embedded Badger/BoltDB is single-node, so a load-balanced HA CA
  means **multiple instances behind a load balancer sharing a PostgreSQL/MySQL backend
  and identical root/intermediate signing material**. Co-locating those on the 3
  control nodes adds no machine but does add that shared SQL backend. SPIRE reserved
  for fleet scale; single-binary uses a dev-CA.
- **Gateway — 1 small cloud server for first deployment** (add a warm standby behind
  the LB if you want S3-front-door failover). The gateway holds **no durable object
  state** — object metadata lives in TiKV, chunks fan out to the D servers over gRPC,
  and coordination is in etcd (the gateway composes over the cluster backends, #454) —
  so a restarted or replaced gateway migrates nothing: on startup `recover()` re-seeds the
  shared `meta:next_inode` allocator floor above the persisted high-water mark, and chunk
  ids need no recovery (a fresh random epoch per process).
  **Active/active is safe (#477).** The gateway allocates inodes from the shared
  `meta:next_inode` **CAS** allocator (`cli::alloc_inode`, the same cluster path the CLI
  uses) and mints chunk ids **coordination-free** — a per-process random 63-bit epoch as the
  high bits plus a monotonic sequence as the low bits (ADR-0019) — so two *active* gateways
  never mint a colliding inode, and collide on a chunk id only if they draw the same epoch
  (~2⁻⁶³ per gateway pair). The **one-shared-front-door / no-affinity / plain-round-robin**
  property (#454) therefore holds: N active gateways behind a round-robin LB is supported.
  (A deterministic upgrade that drops even the ~2⁻⁶³ chunk residual — CAS block-reservation
  for chunk ids — is tracked as #478.) Wyrd still ships **no** load balancer of its own: the
  S3 front door is a standard cloud LB (Hetzner LB / k8s Service / nginx / HAProxy), an
  operator concern, not a Wyrd component.
- **Custodian — co-locate with a gateway or a small dedicated server**; stateless. Run
  **one** against a distributed metadata store — cross-host single-active fencing is not
  enforced yet (#365; see B.5).
- **Network**: Hetzner's **private network (vSwitch)** for the client→D-server,
  gateway→TiKV, and all-roles→etcd coordination traffic, so bulk fragment data and
  the control plane flow on the internal network, not the public interface. etcd in
  particular **must be network-isolated** (ADR-0006 / [§8.5](08-crosscutting-concepts.md)),
  with its auth only defense-in-depth behind the mTLS fabric. The S3 endpoint is the
  only thing that needs public exposure (behind the LB).

**Instance count, Hetzner full-strength:** 9 D + 3 metadata+etcd/step-ca control + 1–2
gateway/custodian = **~13–14 servers** (the CA co-locates on the control nodes, so it
adds none). Cheaper first cut: 6 D + 3 metadata+etcd/step-ca control + 1 gateway = **10**.
All in one location, on a private network, rentable for the duration of a test
campaign and torn down after — so the *cost* is hours-of-rental, not owned
hardware.

### B.2 Why Hetzner is the better first *real* deployment

- **Honest failure domains.** Separate physical servers fail independently in a way
  co-located processes on one homelab box do not — so RS(6,3)'s math is real, and a
  `hetzner server delete <one-d-server>` is a genuine, honest failure-domain-loss
  test of the M3 reconstruction path.
- **Honest performance.** Real server CPUs (SIMD for the EC loop), real NVMe
  (honest fsync), real GbE/10GbE between nodes — the first *trustworthy* throughput
  and latency numbers, and the Q6 linear-scaling claim becomes measurable across
  real independent D servers. *Caveat:* shared-vCPU Cloud servers have
  noisy-neighbour CPU steal, so the genuinely trustworthy numbers come from
  bare-metal — see §B.3 for which Hetzner product to rent for the benchmark run.
- **Sovereign and cheap.** EU infrastructure — pay-per-hour Cloud for the fault
  campaign, per-month Server Auction bare-metal for the benchmark (§B.3) — cheaper
  than the US hyperscalers, and torn down after. On-message for the project's whole
  reason to exist.
- **The bridge to M9.** When you reach cross-zone (M9), the *same* topology
  replicated across Hetzner's **three EU locations** (FSN/NBG/HEL) gives real
  inter-region WAN — so the Hetzner single-zone deployment is the natural seed of
  the eventual multi-region one.

### B.3 Cloud vs bare-metal — which Hetzner product, and when

Hetzner sells these as distinct families, and the choice is governed by *what the
campaign is for*, because they bill differently:

| Family | Examples | Billing | Best for |
|--------|----------|---------|----------|
| **Cloud, shared vCPU** | `CX23`/`CX33` (Intel), `CAX` (ARM), `CPX` (AMD) | **hourly**, instant create/delete | the M3 fault/durability campaign |
| **Cloud, dedicated vCPU** | `CCX13`+ | hourly | shared-cluster perf runs where noisy-neighbour steal must go but hourly teardown stays |
| **Dedicated / bare-metal** | `AX` (AMD), `EX` (Intel), `RX` (ARM64), `SX` (storage) | **monthly + one-time setup fee**, minimum terms | committed deployments, not weekend campaigns |
| **Server Auction** | refurbished bare-metal | monthly, **no setup fee, no minimum contract** | the one *honest-performance* benchmark run |

The thing to internalize: **the fault-tolerance campaign and the honest-performance
benchmark want different products.**

- **Fault/durability testing → Cloud (`CX23`/`CX33`).** The whole
  `hcloud server delete d<n>` failure test and the hours-of-rental cost story
  depend on *hourly* billing and instant teardown. Bare-metal's monthly + setup-fee
  model defeats both. Stay on Cloud here — this is the §B.1 topology.
- **Honest performance numbers (§B.2's pitch) → Server Auction bare-metal.**
  Shared-vCPU Cloud has noisy-neighbour CPU steal, so its throughput/latency
  figures are *not* the trustworthy numbers B.2 promises. Real NVMe fsync, the SIMD
  EC loop on a real core, and 10GbE between nodes need bare-metal — and the
  **Server Auction** (no setup fee, no minimum contract, ≈€35–49/month for a
  64 GB / 2×NVMe box, and exempt from the 15 June 2026 standardization) is the only
  bare-metal option still teardown-friendly. Use the **`RX` line** if the honest
  numbers you want are ARM64. Do *not* use standard `AX`/`EX` for a tear-down
  campaign — the monthly rate plus one-time setup fee defeats the cost rationale.

**Role placement is identical on either family** — only the D-server *performance*
numbers change, not the topology:

- **D servers** are still your failure domains; one bare-metal box = one domain,
  exactly as one cloud server = one domain.
- **TiKV + PD** still want their own three nodes, off the D-server hosts.
- **Gateway and custodian stay stateless and movable.** Co-locate the **custodian**
  with a gateway (or on any spare node / a small `CX23`) on both Cloud and
  bare-metal. Nothing about bare-metal changes the custodian's placement: it
  carries no durable state, leader-elects on restart, and emits the durability
  telemetry from wherever it runs. If you split the campaign across both families
  (Cloud for fault tests, auction boxes for the benchmark), the custodian and
  gateway are the cheap, restartable roles to move between them; the D servers and
  metadata tier are what you actually re-provision.

### B.4 Hetzner honesty box

One Hetzner *location* is still one datacenter = one zone. M4 single-zone on
Hetzner survives **server/disk failures** (the M3 story, now with honest
independent hardware) but **not the loss of the whole location** — that's exactly
what M9 cross-zone replication adds. So the Hetzner shape is a production-durable
single-*site* store with honest failure domains: a real, deployable thing for
non-disaster-recovery use and for pre-release early adopters (the ★ release
itself is M8, per proposal 0013), with the same "keep an
independent out-of-band backup" rule ([§8.2](08-crosscutting-concepts.md))
applying.

---

## Day-one operations (both variants)

What to actually do and watch once it's up:

1. **Label the failure domains.** Configure each D server's domain label
   (rack/power/switch on Hetzner; power-strip/machine in the homelab) so the M3
   selector spreads fragments across them. *This is the step that makes RS(6,3)
   real — do not skip it.*
2. **Verify placement spread.** After writing test objects, confirm a chunk's 9
   fragments actually landed in 9 distinct domains (the custodian/telemetry should
   expose this). If they didn't, the durability math isn't holding — fix labels
   before trusting it.
3. **Watch the durability plane from minute one.** The five M3 metrics —
   under-replicated chunk count, repair-queue depth, time-to-repair, scrub
   coverage, scrub-detected corruption rate — over Prometheus/OTLP. The one that
   matters most: **under-replicated count should be zero in steady state**; a
   non-zero value that doesn't return to zero means repair isn't keeping up.
4. **Do the honest failure test on day one.** Kill one D server (homelab: pull a
   cable; Hetzner Cloud: delete the instance; bare-metal: power it off via Robot).
   Watch: reads keep working (reconstruct from
   survivors), under-replicated count rises, the custodian rebuilds, count returns
   to zero. *If that loop doesn't work, you are not production-durable yet* — and
   it's far better to learn it on day one with test data than later with real data.
5. **Set up the backups — but only where they're needed** (see *Backup model*
   below). Back up the **TiKV metadata** out-of-band to independent storage — this
   is the mandatory one; losing it orphans all chunks. Snapshot **etcd** for fast
   config recovery. **Do not** back up D-server fragments — EC + custodian
   reconstruction is their mechanism. Per [§8.2](08-crosscutting-concepts.md),
   backups must not depend on this cluster. M4 is single-zone; this is your
   disaster-recovery story until M9.
6. **Promote any surprise into DST.** Anything the real deployment does that the
   simulator didn't model — a seeded DST regression. This is how the first
   deployment *earns trust* rather than just running.

## Backup model (per tier)

Backup is **asymmetric by tier** ([§8.2](08-crosscutting-concepts.md)), and the
governing fact is that **replication is not backup**: Raft and EC survive *node*
loss but faithfully replicate *logical* disasters — a bad migration, an errant
delete, a software bug. So back up only the small, precious, non-reconstructible
state:

| Tier | Backup? | Why |
|------|---------|-----|
| **D servers (fragments)** | **No** | RS(6,3) + custodian reconstruction *is* the durability; a per-server backup is redundant with the EC (§8.2 row 1). |
| **Metadata (L4)** | **Yes — mandatory** | Tiny but total blast radius: lose it and every chunk is orphaned even with all fragments intact. Take **online, consistent snapshots** (no downtime — both candidate backends are MVCC) to **independent** storage; for M4 assume *periodic* snapshots, not continuous PITR — see the realtime note below (§8.2 row 3). **The concrete FoundationDB procedure is not yet written** — see the note below. |
| **etcd (L5 coordination)** | **Rebuildable — snapshot optional** | Most L5 state reconstructs from the running fleet (re-registration, leases, re-election), so DR *stands etcd up* rather than restoring it ([§6.5](06-runtime-view.md)). See the etcd note below. |
| **PD** *(TiKV fallback only)* | Via the TiKV cluster backup | Placement metadata, captured by BR with the cluster; not a separate job. FoundationDB has no PD-equivalent role to back up. |
| **Gateway / custodian** | **No (config only)** | Stateless; their state lives in the metadata store + etcd. Capture config in version control / IaC. |

> [!WARNING]
> **The FoundationDB backup/restore procedure is an open item, not a solved one.** The
> analysis below is TiKV's (BR, `txnkv` vs RawKV, log-backup PITR). FoundationDB's
> equivalents — `fdbbackup`/`fdbrestore`, its continuous-backup and PITR semantics, its
> achievable RPO, and how a restore interacts with the custodian's reconstruction pass —
> have **not** been established or drilled for this deployment, and are deliberately not
> guessed at here. Until that work lands, a FoundationDB operator must treat backup as
> **unspecified** and derive it from upstream FDB documentation rather than from this
> note. What *is* settled and backend-independent: **the map is backed up, the fragments
> are not**, and the **restore order** is (1) etcd/L5, (2) the L4 metadata from the
> independent backup, (3) let the custodian reconstruct. Restoring bytes before the map
> is useless either way.

**Restore order is the inverse of "what's reconstructible"**, per the documented DR
ordering ([§6.5](06-runtime-view.md); [proposal 0008](../proposals/draft/0008-management-and-administration.md)):
**(1) etcd/L5 coordination, (2) TiKV/L4 metadata from the independent backup,
(3) let the custodian verify and re-replicate/reconstruct fragments from
survivors.** Restoring bytes before the map is useless — fragments are
unaddressable without the chunk-map. The full single-zone DR sequence is a
**runbook that must be written and drilled before it is needed**, not improvised
during an incident — and that runbook, plus backup cadence/format/retention, is
still an open item in proposal 0008, not yet specified.

**Can the metadata backup be taken in realtime?** Two senses. *Online /
non-blocking* — **yes**: TiKV is MVCC, so a consistent snapshot is read at a
timestamp while the cluster keeps serving, no downtime. *Continuous near-realtime
(low RPO)* — **only partially, and unverified for this deployment**: TiKV/TiDB
log-backup PITR streams change logs with an RPO floor of ~5 minutes (never zero),
**but that is a TiDB-*cluster* feature**, while Wyrd runs **standalone transactional
TiKV** (`txnkv`, no TiDB — [proposal 0007](../proposals/accepted/0007-milestone-4-production-metadata-backend.md))
and the standalone TiKV-BR continuous path is documented for *RawKV*, not `txnkv`.
So for M4 the safe assumption is **periodic online snapshots** (RPO = the snapshot
interval); whether log-backup PITR works against bare `txnkv` must be **verified
against the pinned `tikv-client` / TiKV-BR version** (an open item in 0007) before
it is relied on. Treat restore as fallible and **drill it** (cf. TiKV issue #13281,
PITR restore inconsistency), don't just configure the backup.

**Why etcd needs little backup — and why a *stale restore* is the wrong instinct.**
The reason isn't that its config is static; it's that nearly all L5 state is
**reconstructable from the running fleet** — D servers re-register, leases renew,
the leader re-elects — which is exactly why [§6.5](06-runtime-view.md) *stands up*
L5 first rather than restoring it from a backup (only L2/L4 metadata is
backup-restored). The one genuinely durable bit is **zone-wide config**; keep that
in declarative IaC where possible so etcd is a cache, not the sole source of truth.
A periodic `etcdctl snapshot save` is cheap insurance, but on DR **prefer
re-bootstrapping a fresh etcd over restoring a stale snapshot**: a restore rolls
etcd's mvcc revision backward and can **regress lock fencing tokens**, re-admitting
a stale lock holder — a correctness hazard worse than the soft state you'd recover.
So: low backup need, yes — but for the *reconstructability* reason, with "rebuild
fresh" as the default DR move.

This per-tier model is the operational detail behind §8.2; the whole-store,
object-level out-of-band copy those same sources call for (day-one step 5) is
complementary — it adds protection against logical errors at the object level and
is your only disaster-recovery story until M9 cross-zone replication.

## The honest scope statement to a first user

> This is a single-zone, production-durable, S3-compatible object store. It
> survives disk and server failures gracefully and tells you about its own
> durability. It does **not** survive loss of the whole site (no cross-zone
> replication yet), it is a young, pre-release system still earning its
> battle-testing (the ★ release is M8), and the on-disk format is not yet
> stamped stable. Run it for
> data you have another copy of, or for a tier that can tolerate that maturity —
> not as the sole copy of anything you cannot lose.

That sentence is not a weakness to hide; stating it plainly is what makes you
trustworthy to the kind of early adopter who will give you real faults and become
the operator-partner the project needs.

## Sizing cheat-sheet

| | Homelab min (A.1) | Homelab full (A.2) | Hetzner cheap (B.1) | Hetzner full (B.1) |
|--|--|--|--|--|
| D servers | 6 (2 frags/domain) | 9 (1 frag/domain) | 6 | 9 |
| Fault tolerance | any 1 node | any 3 nodes | any 1 server | any 3 servers |
| Control nodes (metadata+etcd) | 3 (may share) | 3 dedicated | 3 dedicated | 3 dedicated |
| Internal CA (step-ca) | co-located | co-located (HA: +shared SQL) | co-located on control | co-located on control (HA: +shared SQL) |
| Gateway/custodian | on spare | dedicated | 1 shared | 1–2 + LB |
| Total machines | ~6–9 | ~12 | ~10 | ~13–14 |
| Hetzner product | — | — | Cloud `CX23` (D) / `CX33` (metadata) | Cloud `CX23` (D) / `CX33` (metadata) |
| Disaster-recoverable? | No (one site) | No (one site) | No (one location) | No (one location) |
| Best for | learning, dogfooding | real homelab durability test | first honest cloud deploy | full RS(6,3) fault campaign |

The recommendation for a *first real deployment*: **Hetzner cheap (B.1, 6 D
servers, single-fault tolerance)** to start — honest independent hardware, cheap,
torn down after — then scale to 9 for full RS(6,3) tolerance once the basics hold.
The homelab is the better *permanent* dogfooding/learning rig and the better place
to abuse hardware physically; Hetzner is the better *honest first production-shape*
deployment and the seed of the eventual multi-region M9 topology.

Both Hetzner columns above are **Cloud** (`CX`) sizing for the hourly fault
campaign. The separate honest-*performance* benchmark runs on **Server Auction
bare-metal** (§B.3), billed per month rather than per hour, so it sits outside this
cheat-sheet's machine-count/teardown model — size it when you reach the benchmark
phase, not the first fault deployment.

---

# Setup instructions

> These instructions stand up the deployments above. A note on honesty about
> *boundaries*: the **infrastructure** steps (provisioning, networking,
> failure-domain layout, verification) are well-specified and given concretely.
> The **Wyrd-process** steps depend on the `deploy/` artifacts, config keys, and
> CLI flags that M4 is building (`deploy/` ships a docker-compose
> stack per metadata backend — `small-multi-node-fdb/` is canonical, `small-multi-node/`
> is the TiKV fallback; the Helm/operator path is later). Where a step depends on a Wyrd
> config/flag whose exact name is set by the M4 implementation, it is marked
> **[wyrd-config]** — fill it from `deploy/` and the `server`/`d-server`/
> `custodian` `--help` once M4 lands. The shape is correct; the exact flag
> strings are M4's to fix.

## Prerequisites (both variants)

- The Wyrd binary (single binary with `d-server`, `custodian`, gateway, and
  metadata-backend-selector subcommands/roles) built for the target arch
  (x86_64, or aarch64 for ARM nodes/NAS), **built with the features this topology needs**.
  `crates/server/Cargo.toml` has `default = []`: the distributed metadata backends *and* the
  etcd coordination backend are all off by default, so a default build can select **none** of
  them. For production that is **`--features fdb,etcd`** — `fdb` links the system `libfdb_c`,
  whose version must match the cluster's exactly (§7.6), and `etcd` is what makes
  `--coordination-backend etcd` selectable at all. This is exactly the feature set the
  canonical `wyrd:fdb` image bakes in (#470).
- The `deploy/` directory from the repo (the docker-compose metadata **and etcd** stack
  and any systemd unit templates).
- A FoundationDB release (or, on the retained fallback, a TiKV + PD release) **and an
  etcd release** (the `deploy/` stacks pin versions;
  don't mix). etcd backs the L5 `Coordination` seam
  ([ADR-0006](../adr/0006-etcd-for-coordination.md)); it is an external dependency,
  not a Wyrd subcommand.
- **A step-ca release** for the internal CA
  ([ADR-0036](../adr/0036-internal-ca-step-ca-spire.md)), plus a chosen step-ca
  **provisioner** for node enrollment. The single-binary dev profile needs none — it
  uses a built-in self-signed dev-CA behind the same `CertificateAuthority` seam.
- `cargo-deny`-clean, M4-tagged build — a tagged soft-stopping-point build
  (M4 completes the data plane; the ★ release is M8), not a mid-milestone
  checkout.

---

## Variant B — Hetzner (the recommended first deployment)

> **Scope of these steps.** The `hcloud` provisioning below is the **Cloud**
> path — the hourly, instant-teardown fault/durability campaign (§B.3). The
> separate honest-*performance* benchmark uses **Server Auction bare-metal**,
> provisioned through Hetzner **Robot** (not `hcloud`) and billed per month; the
> Wyrd-process steps (B.2–B.5) are identical there, only the provisioning and
> teardown differ (see the notes in B.1 and B.6).

### B.1 Provision the nodes

Pick **one** location (FSN1, NBG1, or HEL1 — single zone for M4). For the
fault/durability campaign use the cheap shared **`CX`** lines (hourly, instant
teardown — see §B.3); `CAX` (ARM) is no longer cheaper than `CX23` and loses the
x86 SIMD EC path. Dedicated `CCX` or bare-metal is only for the separate
honest-performance benchmark, not this functional run.

```
# Roles — instance sizing (test scale; adjust up for real load)
#  D servers       : CX23  (2 vCPU, 4 GB, 40 GB NVMe)  × 6  (or 9 for full RS(6,3))
#  CA+metadata+etcd : CX33  (4 vCPU, 8 GB, 80 GB NVMe)  × 3  (control plane: step-ca + metadata + L5 coordination)
#  gateway+custod  : CX23                              × 1  (or 2 + LB for HA)
```

Provision via the Hetzner Cloud console, the `hcloud` CLI, or Terraform. The
`hcloud` CLI is the quickest for a tear-down-after test campaign:

```sh
# one-time
hcloud context create wyrd-m4
# create a private network for internal traffic (bulk fragment data stays off public)
hcloud network create --name wyrd-zone --ip-range 10.0.0.0/16
hcloud network add-subnet wyrd-zone --network-zone eu-central --type cloud --ip-range 10.0.1.0/24

# D servers (label each with its failure domain — see the failure-domain note below)
for i in $(seq 0 5); do
  hcloud server create --name d$i --type cx23 --image ubuntu-24.04 \
    --location fsn1 --network wyrd-zone --ssh-key <key> \
    --label role=dserver --label fd=fd$((i % 3))   # 6 D servers across 3 failure-domain labels
done

# TiKV + PD nodes
for i in $(seq 0 2); do
  hcloud server create --name tikv$i --type cx33 --image ubuntu-24.04 \
    --location fsn1 --network wyrd-zone --ssh-key <key> --label role=tikv
done

# gateway + custodian
hcloud server create --name gw0 --type cx23 --image ubuntu-24.04 \
  --location fsn1 --network wyrd-zone --ssh-key <key> --label role=gateway
```

> **Failure-domain note on Hetzner.** Distinct *cloud servers* in one location
> are independent hardware, so each server is a real failure domain for
> disk/host loss. They are **not** independent for a *whole-location* outage —
> that is M9 cross-region. For M4, label each D server with a `fd=` group; with 6
> D servers and 3 fd-labels you get the A.1 single-fault profile, with 9 D
> servers and 9 labels the full 3-fault profile (B.1 full).

### B.2 Stand up the internal CA (step-ca) — before anything that dials

Internal mTLS is **required and fail-closed**
([ADR-0025](../adr/0025-internal-service-to-service-trust.md)): no component completes
a single dial until the CA exists and every peer holds a cert — so the CA comes up
**first**, before the control tier. The technology is **step-ca**, with SPIRE reserved
for fleet scale ([ADR-0036](../adr/0036-internal-ca-step-ca-spire.md)); the
single-binary dev profile uses a built-in self-signed CA behind the same
`CertificateAuthority` seam.

Run step-ca on the **3 control nodes** (co-located with etcd/PD/TiKV — it is small) or
on a small dedicated node for stricter isolation. It must be **HA and
network-isolated**: a single-instance CA is a zone-wide SPOF the moment a cert needs to
rotate ([§7.3](07-deployment-view.md)). **HA is not just "run three copies":** step-ca's
embedded Badger/BoltDB is single-node, so the supported load-balanced HA pattern is
**N instances behind a load balancer sharing one PostgreSQL/MySQL backend and the same
root/intermediate signing material** ([smallstep's documented HA shape](https://smallstep.com/docs/step-ca/configuration/#databases)).
Three identical step-ca instances on the control nodes + one shared SQL backend gives
HA at no extra machine; three instances each on an embedded DB are three *independent*
CAs, not HA.

```sh
# on each control node, BEFORE the control tier — [wyrd-config] exact deploy/ unit + flags
# 1) bring up step-ca for HA: N instances sharing ONE PostgreSQL/MySQL backend +
#    identical root/intermediate signing material, behind the LB (embedded DB is single-node)
docker compose -f deploy/step-ca.yml up -d        # [wyrd-config] exact filename from deploy/

# 2) distribute the CA trust bundle (root) to every node so peers can verify each other
step ca bootstrap --ca-url https://10.0.1.<ca-lb>:9000 --fingerprint <root-fingerprint>

# 3) each role gets a short-lived, auto-renewed cert with a SPIFFE-shaped SAN
#    (trust domain "wyrd"; ADR-0036 req 3):
#       spiffe://wyrd/<zone>/<role>/<instance>   role ∈ {dserver,tikv,pd,etcd,gateway,custodian}
#    components consume it as an abstract PeerIdentity, never raw cert fields (ADR-0036 req 2–3)
```

**Initial enrollment is the one unpinned piece.** How a brand-new node first proves its
identity to step-ca to obtain its *first* cert — a join token, a cloud
instance-identity document, a bootstrap provisioner — is **left open by ADR-0036**; pick
a step-ca provisioner and record it in the runbook. After first issuance certs
**auto-renew**, and every component MUST consume a renewable source rather than cache a
cert for the process lifetime (ADR-0036 req 4). Verify before continuing: the CA is
reachable, a test cert issues and verifies against the trust bundle, renewal works, and
a **plaintext dial is refused**.

### B.3 Bring up the control tier (metadata store + etcd)

On the three control nodes, which also run step-ca from B.2 and the L5 **etcd** ensemble
([ADR-0006](../adr/0006-etcd-for-coordination.md)). In a real deployment every dial here is
mTLS under the step-ca certs from B.2 (ADR-0025) — no plaintext. **[wyrd-config]** the exact
compose files / systemd units come from `deploy/`.

> [!CAUTION]
> **The `deploy/` compose stacks are single-host eval/CI fixtures, not this topology.**
> `deploy/small-multi-node-fdb/` brings up *every* role — the metadata cluster **and** the 9
> D servers, custodians and gateways — on **one** Docker host, and it runs **plaintext**
> (`http://` etcd, no FDB TLS). It is how you evaluate Wyrd on a laptop, and it is what CI
> drives. It is **not** a three-control-node production control tier, it does not give you
> failure-domain independence, and it does not satisfy the mTLS requirement above. Do not
> paste it into this procedure; B.4/B.5 would then start those roles a second time.

The etcd ensemble is the same either way:

```sh
# etcd ensemble (3-node quorum) — the L5 Coordination backend (client 2379, peer 2380, mTLS)
docker compose -f deploy/etcd.yml up -d        # [wyrd-config] exact filename from deploy/
```

**The metadata tier depends on the backend, and the two paths differ.**

#### FoundationDB — the canonical backend (ADR-0042)

The **shape** is settled, and it is backend-specific in one important way: FoundationDB has
**no PD-equivalent role**. The coordinators *are* the quorum. Across the three control nodes
you want a ≥3-process cluster with `double` redundancy (tolerating the loss of one process);
a fresh cluster is **inert until configured exactly once** (`fdbcli --exec "configure new
double ssd"`) — the step most easily missed, after which the cluster is up but serves nothing;
and every role that opens metadata needs the **cluster file** (`WYRD_FDB_CLUSTER_FILE`), which
is how the client finds the coordinators. Verify with `fdbcli --exec status`: the database
reports **available**, the configured redundancy is **satisfied**, and the coordinators are
reachable.

> [!IMPORTANT]
> **The multi-node FoundationDB control tier is NOT yet an established procedure, and is
> deliberately not invented here.** What is missing: the process-class layout (`storage`,
> `log`, `stateless`, coordinators) and its placement across the three nodes; TLS
> configuration for the FDB cluster; `fdb.cluster` distribution and rotation; systemd units;
> and the backup/restore path (see the *Backup model* warning). None of it has been run or
> drilled for this deployment, and writing plausible-looking `fdbcli` incantations into a
> first-deployment runbook would be worse than admitting the gap. It is tracked as **#546**.
> Until that lands, derive the control tier from upstream FoundationDB documentation — and
> treat everything above as the *requirements it must meet*, not as the procedure.

#### TiKV — the retained-fallback path (#443)

Only for the stood-down TiKV backend (`deploy/small-multi-node/`); not for a new deployment.
PD must come up **before** TiKV, because TiKV registers with PD:

```sh
# PD ensemble (3-node quorum). PD also defaults to 2379 — if co-located with etcd,
# remap one (e.g. etcd → 2381).
docker compose -f deploy/tikv-pd.yml up -d     # [wyrd-config] exact filename from deploy/
```

Verify: **PD has quorum** (3 nodes, lose at most 1) and TiKV stores show healthy
(`pd-ctl` / `tikv-ctl`, or the deploy stack's health check).

#### Either way

Verify **etcd has quorum** (3 nodes, lose at most 1 — `etcdctl endpoint health`) before adding
storage. The `<L5-endpoint>` every other role dials is this etcd ensemble, e.g.
`10.0.1.<e0>:2379,10.0.1.<e1>:2379,10.0.1.<e2>:2379`.

### B.4 Bring up the D servers (with failure-domain labels)

On each `d*` node, run the Wyrd `d-server` role, pointed at its local disk and
**labeled with its failure domain**. This label is the single most important
config in the whole deployment — it is what makes RS(6,3) real.

```sh
# on each d-server node. --bind is the gRPC listen addr (50051 is Wyrd's own default);
# --advertise-addr is what peers dial, so it must be the node's routable address;
# --failure-domain MUST be set and MUST be honest — it is what makes RS(6,3) real;
# --coordination-backend etcd registers id·endpoint·fd and renews the lease (needs the
# `etcd` cargo feature, and WYRD_ETCD_ENDPOINTS points at the B.3 ensemble).
export WYRD_ETCD_ENDPOINTS=10.0.1.<e0>:2379,10.0.1.<e1>:2379,10.0.1.<e2>:2379
wyrd d-server \
  --data-dir /var/lib/wyrd/fragments \
  --bind 0.0.0.0:50051 \
  --advertise-addr 10.0.1.<n>:50051 \
  --id <n> \
  --failure-domain $FD_LABEL \
  --coordination-backend etcd \
  --group <zone-name>
```

> [!NOTE]
> **[wyrd-config]** The mTLS flags this step ought to carry (`--tls-cert` / `--tls-key` /
> `--tls-ca`, step-ca-issued per B.2, ADR-0025) **do not exist in the CLI yet**
> (`cli.rs::usage`). Until they do, this traffic is not mTLS-protected by following the
> command above — keep it on the private network (M5).

The D server registers itself (id + endpoint + failure-domain label) through the
L5 coordination seam; the gateway discovers it. **Never** point two D servers
that share real hardware at different `fd` labels — the label must reflect actual
independence or the durability math lies.

### B.5 Bring up the gateway + custodian

On `gw0`, select the metadata backend (the M4 composition switch — the *whole* point of the
`MetadataStore` seam) and start the gateway and the custodian. **FoundationDB is the
production backend** (ADR-0042); the binary must be built `--features fdb,etcd` (both are
off by default — see Prerequisites), and `fdb` links `libfdb_c` (§7.6). Unlike TiKV, FDB
takes no endpoint flag: the client reads a **cluster file**, whose path comes from
`WYRD_FDB_CLUSTER_FILE`.

Both roles take their backends by flag and their endpoints by environment — the shape the
compose fixtures use, so these commands mirror something that actually runs (the S3 gateway
role is `wyrd s3`; there is no `wyrd gateway` subcommand):

```sh
# Shared by every role that opens metadata or coordination.
# WYRD_FDB_CLUSTER_FILE is the file fdbcli wrote in B.3 — distribute it to each host.
export WYRD_FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
export WYRD_ETCD_ENDPOINTS=10.0.1.<e0>:2379,10.0.1.<e1>:2379,10.0.1.<e2>:2379
# Or pass --access-key / --secret-key instead of these:
export WYRD_S3_ACCESS_KEY=... WYRD_S3_SECRET_KEY=...

# S3 gateway — stateless front door. --metadata-backend is the redb|tikv|fdb selector;
# --coordination-backend etcd needs the `etcd` cargo feature (see Prerequisites);
# --endpoints are the D servers from B.4.
wyrd s3 \
  --metadata-backend fdb \
  --coordination-backend etcd \
  --s3-listen 0.0.0.0:8080 \
  --region <your-region> \
  --endpoints http://10.0.1.<d0>:50051,...

# custodian — reconstruction/repair; emits durability telemetry.
# --otlp-endpoint is how that telemetry LEAVES the process: set it (see the note below).
wyrd custodian \
  --zone <zone-name> \
  --metadata-backend fdb \
  --endpoints http://10.0.1.<d0>:50051,... \
  --ids 0,1,... \
  --failure-domains fd0,fd1,... \
  --otlp-endpoint <your-collector>
```

> [!WARNING]
> **Run exactly ONE custodian against a distributed metadata store — "single-active" is not
> enforced for you.** The custodian campaigns for leadership through the *process-local*
> `MemCoordination` (`cli.rs`), which always grants the lone process leadership. On embedded
> `redb` that is genuinely single-active, because the store's exclusive file lock keeps a
> second custodian off the same `--data-dir`. A **shared networked store (FoundationDB or
> TiKV) has no such lock**: start two custodians and *both* self-grant and reconcile
> concurrently — the role logs a WARNING saying exactly this rather than advertise a safety
> property it does not have.
>
> Nothing corrupts — the reconstruction repoint is a version-conditional (CAS) commit, so two
> racing custodians never both win — but you get duplicated repair work and wasted bandwidth,
> which is precisely what you do not want during a real incident. Real cross-host fencing
> arrives when the etcd-backed `Coordination` replaces `MemCoordination` behind the same seam
> (ADR-0006, **#365**). Note the `deploy/` compose profiles start **three** custodians: that
> is a throughput/eval fixture, not a pattern to copy into production.

> [!IMPORTANT]
> **Set `--otlp-endpoint`, or you get no durability telemetry at all.** There is no
> Prometheus scrape endpoint to fall back on: with `ExporterConfig::Prometheus` the process
> builds an **in-process** registry (`DurabilityTelemetry::gather_prometheus`) and **binds no
> HTTP listener** — nothing external can reach it. That path exists for tests and the
> zero-dependency dev profile (ADR-0012), not for a deployment. So the OTLP push is the only
> way repair/scrub/under-replication signal leaves the custodian, and durability telemetry is
> the one thing you cannot afford to discover you were not collecting. Stand up the collector
> first (#446).

> [!NOTE]
> **[wyrd-config] The mTLS flags this procedure assumes do not exist yet.** B.2 requires every
> internal dial to present a step-ca cert (ADR-0025), but the CLI exposes no `--tls-cert` /
> `--tls-key` / `--tls-ca` surface today (`cli.rs::usage`), and the compose fixtures run
> plaintext. Until that lands, this tier is **not** mTLS-protected merely by following the
> commands above — keep it on the private network and treat the trust plane as the open item
> it is (M5).

On the retained TiKV fallback (#443) the only difference is the selector and the fact that
TiKV *does* take endpoints — `--metadata-backend tikv` with `WYRD_TIKV_PD_ENDPOINTS` pointed at
the PD ensemble instead of `WYRD_FDB_CLUSTER_FILE`; everything else above is unchanged. That is
the seam earning its keep: swapping the metadata tier changes a flag and an env var, not a
topology.

Expose **only** the gateway's S3 port publicly (behind the Hetzner LB if you want
HA); everything else stays on the `10.0.1.0/24` private network.

### B.6 Tear down (the cost-control step)

```sh
# delete everything when the campaign ends — billing stops at delete, not stop
hcloud server delete d0 d1 d2 d3 d4 d5 tikv0 tikv1 tikv2 gw0
hcloud network delete wyrd-zone
```

> **Bare-metal teardown differs.** Server Auction / dedicated boxes are *cancelled*
> through Robot, not `hcloud server delete`, and billing stops at the end of the
> paid month, not the minute — so plan the benchmark to fit a billing month rather
> than a weekend.

---

## Variant A — Homelab

The infra steps differ (your own machines, your own network); the **Wyrd
process** steps (B.2–B.5) are identical — same step-ca CA, same `deploy/` stack, same
`d-server` / gateway / custodian roles, same failure-domain labeling.

### A.1 Prepare the machines

- Flash each node (mini-PC / Pi 5) with a Linux you know (Ubuntu Server 24.04 is
  fine; Pi OS Lite for Pis). **Boot from SD is OK; store fragments on a real SSD**
  (USB3/NVMe), never on the SD card — SD cards lie about fsync.
- Give each node a static IP on your LAN, and a hostname matching its role
  (`d0..d8`, `tikv0..2`, `gw0`).
- **Define your failure domains physically:** e.g. 3 nodes per power strip × 3
  strips → label by strip (`fd=strip0/strip1/strip2`). A node's `fd` label must
  match the physical thing that fails together (the strip/PSU). This is the
  homelab equivalent of rack/power labels.

### A.2 Bring up the stack

Identical to Hetzner B.2–B.5, with LAN IPs instead of the Hetzner private
network. step-ca on the 3 control nodes **first** (mTLS is fail-closed), then
the metadata store + etcd on those nodes (or folded onto 3 D-server nodes for the 6-machine
minimum, accepting the coupling), D servers on the `d*` nodes with their `fd` labels,
gateway+custodian on `gw0` or a spare.

### A.3 The homelab-specific value

Because the hardware is yours and physical, you can do the fault tests Hetzner
can't: literally pull a power-strip cable to test a whole-failure-domain loss,
yank an SSD mid-write, unplug a network cable. These are the unmodeled physical
faults DST can't inject and a cloud instance abstracts away — the homelab's
unique contribution.

---

## Day-one verification (both variants) — do this before trusting it

Run these *in order*; each gates the next. This is the difference between "the
processes are running" and "it is actually production-durable."

```
1. Health:        CA (step-ca) reachable, certs issued + auto-renewing, and a
                  plaintext (non-mTLS) dial is REFUSED (ADR-0025); etcd quorum
                  healthy (L5 Coordination up); all D servers registered &
                  discovered via etcd; PD quorum healthy; custodian leader
                  elected; gateway serving S3.
2. Round-trip:    S3 PUT an object → GET it back byte-identical.
3. Spread check:  inspect a written chunk's 9 fragments → confirm they landed in
                  9 DISTINCT failure domains (telemetry/custodian exposes this).
                  IF NOT: fix your fd labels before going further. The math is
                  only real if this passes.
4. Telemetry:     under-replicated count = 0 in steady state; scrub running;
                  metrics flowing to Prometheus/OTLP.
5. THE FAILURE TEST (the one that matters):
                  Kill one D server (Hetzner Cloud: `hcloud server delete d<n>`;
                  bare-metal: power off via Robot; homelab: pull its cable).
                  WATCH:
                    a. reads keep succeeding (reconstruct from survivors) — no errors
                    b. under-replicated count rises above 0
                    c. the custodian rebuilds the lost fragments onto healthy
                       nodes in correct failure domains
                    d. under-replicated count returns to 0
                  IF this loop completes → you are production-durable against
                  single-node loss. IF it does not → you are NOT; stop and fix
                  before putting any real (even non-sole-copy) data on it.
6. Backup:        back up TiKV metadata out-of-band to independent storage (the
                  mandatory one) + snapshot etcd; do NOT back up D-server fragments
                  (EC + reconstruction covers them). Know the restore order:
                  etcd → metadata → re-replicate from survivors. See "Backup model
                  (per tier)". M4 is single-zone, so this is your only DR until M9.
7. Promote:       anything surprising in steps 1–6 → a seeded DST regression.
```

## What you'll spend (Hetzner, verified post-15-June-2026 pricing)

**Cloud, B.1-cheap topology** (6× `CX23` D + 3× `CX33` metadata + 1× `CX23`
gateway): at €0.0064/h (`CX23`) and €0.0104/h (`CX33`) that is **≈€0.076/hour for
the whole cluster** — **~€3–4 a weekend**, **~€11 a week**, **under €1 for a
focused day**. The 15 June 2026 price adjustment did not move this materially, so
the figure still holds. Full-strength (9 D + 3 metadata + 2 gateway) ≈
**€0.10/hour**. New accounts get ~€20 credit (several campaigns).

**Bare-metal, the benchmark run** (§B.3): Server Auction boxes run
≈**€35–49/month** each with no setup fee and no minimum contract — *month*
granularity, not hourly, so budget per month of benchmarking, not per weekend.
Standard `AX`/`EX` lines add a one-time setup fee and minimum terms; avoid them
for short campaigns.

Confirm live per-instance prices before sizing — Hetzner repriced repeatedly in
2026 (three increases plus the 15 June standardization). The cheap `CX` lines are
the ones to use for Cloud; EU-sovereign alternatives (netcup, Contabo, OVH,
Scaleway, IONOS/STACKIT) hit similar test economics if Hetzner's trajectory
concerns you.
