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
> first *real* single-zone Wyrd deployment at the M4 release point — the
> "Small multi-node Production" profile (TiKV + PD/etcd + local-disk D servers +
> gateway + custodian) — in two concrete shapes: a homelab and a Hetzner rental.
> The governing constraint throughout is making the **RS(6,3) durability math
> actually true**, which is a *topology* property, not a process-count property.
> The conceptual framing lives in the [deployment view](07-deployment-view.md)
> (§7.3–§7.4); this note is the operational detail behind it.

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

The M4 stack has five process roles. Their *placement* differs between homelab and
Hetzner; the *roles* do not.

| Role | What it is | Failure-domain sensitivity |
|------|-----------|---------------------------|
| **D servers** | Dumb fragment storage; one fragment of each chunk lands here. The thing whose independent failure the durability math depends on. | **Critical** — these define the failure domains. Spread them. |
| **TiKV** | Distributed metadata store (the commit point). Holds inodes/dirents/chunk-maps. Small but precious — losing it orphans all chunks. | **High** — replicated (Raft, 3×); wants its own independent nodes, not co-located with D servers ideally. |
| **PD** | TiKV's placement driver / coordinator (a 3-node etcd-class ensemble). | **High** — 3 nodes for quorum; lose 2 and metadata stalls. |
| **Gateway** | Stateless S3 front door; embeds the client library (chunk, EC, commit). | **Low** — stateless, horizontally scalable, restartable. |
| **Custodian** | One active (leader-elected); runs GC/scrub/reconstruct/rebalance; emits durability telemetry. | **Low** — stateless logic; a restart re-elects. |

The asymmetry to internalize: **D servers carry the durability**, **TiKV+PD carry
the metadata** (precious, small, separately replicated), and **gateway+custodian
are stateless** (cheap to restart/move). A good topology spends its
failure-domain budget on the D servers first, gives TiKV+PD their own quorum
spread, and treats gateway+custodian as movable.

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
- **TiKV + PD**: 3 small nodes running TiKV and PD co-located (PD is light). These
  should be **3 *different* machines** from each other for Raft quorum, but in a
  small homelab they can share machines with D servers if you must — accepting that
  a machine running both a D server and a TiKV node is now a domain whose loss hits
  both tiers. Cleaner: 3 dedicated small nodes for TiKV+PD.
- **Gateway + custodian**: run on any node (or a spare), stateless. One of each is
  fine; the custodian leader-elects so a second is optional.

**Machine count, minimum honest:** ~6 D-server boxes + 3 TiKV/PD boxes = **9
machines**, or fold TiKV/PD onto 3 of the D-server boxes for **6 machines** with
the stated coupling caveat. Gateway/custodian ride along.

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
- **TiKV + PD on 3 dedicated nodes**, ideally on a different power strip again.
- Total: **~12 machines** (9 D + 3 TiKV/PD). This is a *serious* homelab but it
  makes the durability story genuinely true rather than aspirational.

### A.3 Homelab honesty box

Even A.2 shares **one building, one grid feed, one ISP**. So the homelab is a
*real test of disk/server/power-strip independence and the full M3 repair story* —
which is genuinely valuable and the thing DST can't give you — but it is **not**
disaster-recoverable (no second site; that's M5+). Treat homelab M4 as: a real,
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
is M5+), but you use Hetzner's physical-server independence to make the
failure-domain math honest in a way one desk cannot.

### B.1 The Hetzner single-zone topology

"Single zone" = one Hetzner location (e.g. Falkenstein **or** Nuremberg **or**
Helsinki — pick one for M4; multi-location is M5). Within it:

- **D servers — dedicated/bare-metal or dedicated vCPU servers, as your failure
  domains.** Hetzner's cheap dedicated servers (or `CCX`/`CPX` cloud servers) each
  with a real NVMe give you honest per-server independence: separate hardware,
  separate failure. For RS(6,3) at full strength, **9 D-server instances** (one
  fragment each — 3-fault tolerance). For a cheaper first cut, **6** (single-fault
  tolerance, as in A.1). Bare-metal gives honest fsync/NVMe performance numbers;
  cloud servers are easier to spin up/down for a test campaign.
- **TiKV + PD — 3 dedicated cloud servers**, separate from the D servers, for the
  metadata quorum. Small (TiKV-small + PD is not heavy at this scale). Keeping them
  off the D-server hosts means a metadata-node loss and a storage-node loss are
  independent events.
- **Gateway — 1–2 small cloud servers**, stateless, behind Hetzner's load balancer
  if you want HA on the S3 front door. (One is fine for first deployment.)
- **Custodian — co-locate with a gateway or a small dedicated server**; stateless,
  leader-elected.
- **Network**: Hetzner's **private network (vSwitch)** for the
  client→D-server and gateway→TiKV traffic, so bulk fragment data flows on the
  internal network, not the public interface. The S3 endpoint is the only thing
  that needs public exposure (behind the LB).

**Instance count, Hetzner full-strength:** 9 D + 3 TiKV/PD + 1–2 gateway/custodian
= **~13–14 servers**. Cheaper first cut: 6 D + 3 TiKV/PD + 1 gateway = **10**.
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
  real independent D servers.
- **Sovereign and cheap.** EU bare-metal, pay-per-hour, torn down after — the
  performance-and-real-fault story on sovereign infrastructure, cheaper than the US
  hyperscalers. On-message for the project's whole reason to exist.
- **The bridge to M5.** When you reach cross-zone (M5), the *same* topology
  replicated across Hetzner's **three EU locations** (FSN/NBG/HEL) gives real
  inter-region WAN — so the Hetzner single-zone deployment is the natural seed of
  the eventual multi-region one.

### B.3 Hetzner honesty box

One Hetzner *location* is still one datacenter = one zone. M4 single-zone on
Hetzner survives **server/disk failures** (the M3 story, now with honest
independent hardware) but **not the loss of the whole location** — that's exactly
what M5 cross-zone replication adds. So Hetzner M4 is a production-durable
single-*site* store with honest failure domains: a real, deployable thing for
non-disaster-recovery use and for early adopters, with the same "keep an
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
   cable; Hetzner: delete an instance). Watch: reads keep working (reconstruct from
   survivors), under-replicated count rises, the custodian rebuilds, count returns
   to zero. *If that loop doesn't work, you are not production-durable yet* — and
   it's far better to learn it on day one with test data than later with real data.
5. **Set up the out-of-band backup.** Per [§8.2](08-crosscutting-concepts.md), a
   copy that does not depend on this cluster. M4 is single-zone; the backup is your
   disaster-recovery story until M5.
6. **Promote any surprise into DST.** Anything the real deployment does that the
   simulator didn't model — a seeded DST regression. This is how the first
   deployment *earns trust* rather than just running.

## The honest scope statement to a first user

> This is a single-zone, production-durable, S3-compatible object store. It
> survives disk and server failures gracefully and tells you about its own
> durability. It does **not** survive loss of the whole site (no cross-zone
> replication yet), it is a recently-released system still earning its
> battle-testing, and the on-disk format is not yet stamped stable. Run it for
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
| TiKV/PD nodes | 3 (may share) | 3 dedicated | 3 dedicated | 3 dedicated |
| Gateway/custodian | on spare | dedicated | 1 shared | 1–2 + LB |
| Total machines | ~6–9 | ~12 | ~10 | ~13–14 |
| Disaster-recoverable? | No (one site) | No (one site) | No (one location) | No (one location) |
| Best for | learning, dogfooding | real homelab durability test | first honest cloud deploy | honest perf + fault campaign |

The recommendation for a *first real deployment*: **Hetzner cheap (B.1, 6 D
servers, single-fault tolerance)** to start — honest independent hardware, cheap,
torn down after — then scale to 9 for full RS(6,3) tolerance once the basics hold.
The homelab is the better *permanent* dogfooding/learning rig and the better place
to abuse hardware physically; Hetzner is the better *honest first production-shape*
deployment and the seed of the eventual multi-region M5 topology.

---

# Setup instructions

> These instructions stand up the deployments above. A note on honesty about
> *boundaries*: the **infrastructure** steps (provisioning, networking,
> failure-domain layout, verification) are well-specified and given concretely.
> The **Wyrd-process** steps depend on the `deploy/` artifacts, config keys, and
> CLI flags that M4 is building (the M4 proposal ships a docker-compose TiKV+PD
> stack; the Helm/operator path is later). Where a step depends on a Wyrd
> config/flag whose exact name is set by the M4 implementation, it is marked
> **[wyrd-config]** — fill it from `deploy/` and the `server`/`d-server`/
> `custodian` `--help` once M4 lands. The shape is correct; the exact flag
> strings are M4's to fix.

## Prerequisites (both variants)

- The Wyrd binary (single binary with `d-server`, `custodian`, gateway, and
  metadata-backend-selector subcommands/roles) built for the target arch
  (x86_64, or aarch64 for ARM nodes/NAS).
- The `deploy/` directory from the repo (docker-compose TiKV+PD stack and any
  systemd unit templates).
- A TiKV + PD release (the M4 `deploy/` stack pins versions; don't mix).
- `cargo-deny`-clean, M4-tagged build — i.e. an actual M4 release, not a
  mid-milestone checkout.

---

## Variant B — Hetzner (the recommended first deployment)

### B.1 Provision the nodes

Pick **one** location (FSN1, NBG1, or HEL1 — single zone for M4). Use the cheap
shared lines (CX/CAX); dedicated CCX is not needed at test scale.

```
# Roles — instance sizing (test scale; adjust up for real load)
#  D servers      : CX23  (2 vCPU, 4 GB, 40 GB NVMe)   × 6  (or 9 for full RS(6,3))
#  TiKV + PD      : CX33  (4 vCPU, 8 GB, 80 GB NVMe)   × 3
#  gateway+custod : CX23                                × 1  (or 2 + LB for HA)
```

Provision via the Hetzner Cloud console, the `hcloud` CLI, or Terraform. The
`hcloud` CLI is the quickest for a tear-down-after test campaign:

```sh
# one-time
hcloud context create wyrd-m4
# create a private network for internal traffic (bulk fragment data stays off public)
hcloud network create --name wyrd-zone --ip-range 10.0.0.0/16
hcloud network add-subnet wyrd-zone --network-zone eu-central --type cloud --ip-range 10.0.1.0/24

# D servers (label each with its failure domain — see B.3)
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
> that is M5 cross-region. For M4, label each D server with a `fd=` group; with 6
> D servers and 3 fd-labels you get the A.1 single-fault profile, with 9 D
> servers and 9 labels the full 3-fault profile (B.1 full).

### B.2 Bring up the metadata tier (TiKV + PD)

From the M4 `deploy/` stack, on the three `tikv*` nodes. The M4 proposal ships a
**docker-compose TiKV+PD** for CI/eval; for a real deployment use the same images
with PD pointed at all three nodes. **[wyrd-config]** the exact compose file /
systemd units come from `deploy/`.

```sh
# on each tikv node: install docker, pull the deploy/ stack, start PD then TiKV
# PD ensemble (3-node quorum) must come up first; TiKV registers with PD.
# The deploy/ compose file wires this; point each node at the other two PDs.
docker compose -f deploy/tikv-pd.yml up -d     # [wyrd-config] exact filename from deploy/
```

Verify the metadata tier before adding storage: PD has quorum (3 nodes, lose at
most 1), TiKV stores show healthy. (`pd-ctl`/`tikv-ctl` from the TiKV release, or
the deploy stack's health check.)

### B.3 Bring up the D servers (with failure-domain labels)

On each `d*` node, run the Wyrd `d-server` role, pointed at its local disk and
**labeled with its failure domain**. This label is the single most important
config in the whole deployment — it is what makes RS(6,3) real.

```sh
# on each d-server node — [wyrd-config] exact flags from `wyrd d-server --help`
wyrd d-server \
  --data-dir /var/lib/wyrd/fragments \
  --listen 10.0.1.<n>:50051 \         # Wyrd D-server gRPC default (crates/server/src/cli.rs)
  --failure-domain $FD_LABEL \          # e.g. fd0/fd1/fd2 — MUST be set, MUST be honest
  --coordination <L5-endpoint>          # registers via the Coordination seam
```

The D server registers itself (id + endpoint + failure-domain label) through the
L5 coordination seam; the gateway discovers it. **Never** point two D servers
that share real hardware at different `fd` labels — the label must reflect actual
independence or the durability math lies.

### B.4 Bring up the gateway + custodian

On `gw0`, select the **TiKV** metadata backend (the M4 composition switch) and
start the gateway and the custodian.

```sh
# gateway — TiKV backend selected by config (the M4 release point)
wyrd gateway \
  --metadata-backend tikv \             # [wyrd-config] the M4 redb|tikv selector
  --tikv-pd 10.0.1.<pd0>:2379,10.0.1.<pd1>:2379,10.0.1.<pd2>:2379 \
  --s3-listen 0.0.0.0:8080 \
  --coordination <L5-endpoint>

# custodian — one active, leader-elected; emits durability telemetry
wyrd custodian \
  --metadata-backend tikv --tikv-pd <...> \
  --coordination <L5-endpoint> \
  --otlp-endpoint <your-collector>      # or scrape its Prometheus endpoint
```

Expose **only** the gateway's S3 port publicly (behind the Hetzner LB if you want
HA); everything else stays on the `10.0.1.0/24` private network.

### B.5 Tear down (the cost-control step)

```sh
# delete everything when the campaign ends — billing stops at delete, not stop
hcloud server delete d0 d1 d2 d3 d4 d5 tikv0 tikv1 tikv2 gw0
hcloud network delete wyrd-zone
```

---

## Variant A — Homelab

The infra steps differ (your own machines, your own network); the **Wyrd
process** steps (B.2–B.4) are identical — same `deploy/` stack, same `d-server` /
gateway / custodian roles, same failure-domain labeling.

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

Identical to Hetzner B.2–B.4, with LAN IPs instead of the Hetzner private
network. TiKV+PD on the 3 `tikv` nodes (or folded onto 3 D-server nodes for the
6-machine minimum, accepting the coupling), D servers on the `d*` nodes with
their `fd` labels, gateway+custodian on `gw0` or a spare.

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
1. Health:        all D servers registered & discovered; PD quorum healthy;
                  custodian leader elected; gateway serving S3.
2. Round-trip:    S3 PUT an object → GET it back byte-identical.
3. Spread check:  inspect a written chunk's 9 fragments → confirm they landed in
                  9 DISTINCT failure domains (telemetry/custodian exposes this).
                  IF NOT: fix your fd labels before going further. The math is
                  only real if this passes.
4. Telemetry:     under-replicated count = 0 in steady state; scrub running;
                  metrics flowing to Prometheus/OTLP.
5. THE FAILURE TEST (the one that matters):
                  Kill one D server (Hetzner: `hcloud server delete d<n>`;
                  homelab: pull its cable).
                  WATCH:
                    a. reads keep succeeding (reconstruct from survivors) — no errors
                    b. under-replicated count rises above 0
                    c. the custodian rebuilds the lost fragments onto healthy
                       nodes in correct failure domains
                    d. under-replicated count returns to 0
                  IF this loop completes → you are production-durable against
                  single-node loss. IF it does not → you are NOT; stop and fix
                  before putting any real (even non-sole-copy) data on it.
6. Backup:        configure the out-of-band backup (§8.2) — M4 is single-zone,
                  so this is your only disaster-recovery story until M5.
7. Promote:       anything surprising in steps 1–6 → a seeded DST regression.
```

## What you'll spend (Hetzner, current pricing)

A test campaign on the B.1-cheap topology (10 nodes, CX/CX33, hourly, torn down
after) is roughly **€0.07/hour for the whole cluster** — **~€3–4 a weekend**,
**~€11 a week**, **under €1 for a focused day**. New accounts get ~€20 credit
(several campaigns). Confirm live per-instance prices before sizing — Hetzner
repriced repeatedly in 2026; the cheap CX/CAX lines are the ones to use, and
EU-sovereign alternatives (netcup, Contabo, OVH, Scaleway, IONOS/STACKIT) hit
similar test economics if Hetzner's trajectory concerns you.
