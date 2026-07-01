---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - deployment
---
# 7. Deployment view

> Living document.

## 7.1 The three deployment profiles

The same codebase, composed differently. The composition lives in the `server` crate; switching profiles swaps backends behind the `ChunkStore`, `MetadataStore`, `NamespaceStore`, and `Coordination` traits.

| Profile | Coordination | Identity / PKI | Metadata (L4) | Global plane (L2/L3) | Chunk store | Durability | Status |
|---------|--------------|----------------|---------------|----------------------|-------------|------------|--------|
| Single binary (dev) | in-memory | dev-CA (in-process) | redb (embedded) | none — single-zone | filesystem | none / replication(1) | Dev & eval only (ADR-0014) |
| Small multi-node | 3-node etcd | step-ca (SPIRE reserved) | TiKV (small) / redb | none — single-zone | local-disk D servers | replication(n) or rs(k,m) | Production |
| Provider fleet | dedicated etcd per zone | step-ca, HA (SPIRE reserved) | TiKV | TiDB + L3 replication (multi-region) | local-disk D servers | rs(k,m) | Production |

The single-binary profile collapses all components into one process: gateway, embedded metadata, one logical D server, custodians, in-memory coordination, and a built-in **dev-CA** in place of the production CA (ADR-0025, ADR-0036) so a one-process system is not gated on a full PKI. It exists for development and evaluation and carries **no production durability promise** — a single chassis cannot deliver independent failure domains.

The **Identity / PKI** column tracks the trust plane behind the mTLS fabric (§8.5, ADR-0005/0025): a built-in dev-CA in the single binary, and a **self-hosted provider CA — `step-ca` now, with SPIRE reserved** — for the production profiles (ADR-0036). It follows the same dev→fleet gradient as coordination, and — because internal mTLS is fail-closed (ADR-0025) — it is a first-class control-plane dependency, not an afterthought (see §7.3 and the §6.5 restore order). SPIRE (secret-less workload attestation) is the *reserved* upgrade, adopted at the fleet scale where its benefit outweighs its operational cost — a SPIRE Server plus a per-node Agent and attestation config; until then `step-ca` issues the short-lived, auto-rotated certs behind a `CertificateAuthority` seam, so the eventual switch is a composition change. The full rationale (why step-ca now, why SPIRE deferred, why not Vault) lives in ADR-0036.

**There is no separate L2/L3 below the multi-zone tier.** The single-binary and small-multi-node profiles are *single-zone*: there is one home zone, so file→home-zone is trivial and the global namespace folds into the zonal store — `NamespaceStore` is backed by the same redb instance as `MetadataStore`, and cross-zone replication (L3) does not exist. A distinct, geo-distributed **L2** (TiDB behind `NamespaceStore`, ADR-0020) and the **L3** replication layer appear only at the **provider-fleet** profile — the first genuinely multi-region tier. This is why the build order (section 9) puts L2/L3 last.

## 7.2 Deployment substrate (pluggable)

Kubernetes is *available but never required* (ADR-0010):

- **Stateless components** (gateways, replication workers, leader-elected custodians) are Kubernetes-friendly: a Deployment behind a Service, or systemd units, or compose.
- **Stateful components** (D servers; self-hosted metadata/etcd) want node affinity to their disks. On Kubernetes: StatefulSets with local persistent volumes and strict anti-rescheduling, managed by an operator that understands the custodians own placement. Off Kubernetes: systemd on the storage hosts.

What is shipped, in priority order: a single static binary (primary), an OCI image (same binary), a docker-compose (small multi-node), and later a Helm chart and operator (`deploy/`). **No code knows it is on Kubernetes** — peers are discovered through L5, never through orchestrator APIs. The system must come up identically whether launched by systemd, compose, or a Kubernetes pod.

## 7.3 Failure domains

Durability math depends on fragments landing in independent failure domains (rack, power, switch). The placement service (L2) and custodians (L4) enforce domain spread for the configured EC scheme. A leading-indicator capacity metric is per-failure-domain utilization: running out of room *in a specific domain* blocks new EC writes before total capacity is exhausted.

**Independence is a topology property, not a process count.** The durability of an EC scheme is only real if the domains it spreads across *fail independently*. RS(6,3) writes 9 fragments per chunk and tolerates the loss of any 3; that promise holds only when no single physical failure can take more than 3 fragments. Nine fragments on nine processes that share one power supply, one disk, or one host is not 9× redundancy — one power cut destroys the chunk. So the load-bearing question for any deployment is **what is the unit of independent failure, and are there enough of them**:

- **Full tolerance** wants **≥ 9 independent domains**, one fragment each — any 3 domains can be lost and the custodian rebuilds.
- **Fewer domains** is honest only with **consciously-reduced tolerance**: e.g. 6 domains holding ~2 fragments each survives *any single domain* loss but not two arbitrary ones.

A domain *label* must reflect actual shared-fate hardware. Labelling two processes that share a chassis as distinct domains does not create independence — it only makes the durability math lie. This is why day-one verification (§7.4) checks that a written chunk's fragments actually landed in distinct domains before the deployment is trusted.

**The control and trust planes are failure domains too.** The math above concerns *D-server* domains, but two small control services carry their own availability requirement. The **coordination + metadata quorum** (etcd, PD/TiKV — 3 nodes, tolerate one loss not two) is the familiar one. Less obvious is the **provider CA trust plane** (`step-ca` now, SPIRE reserved — ADR-0036): because internal mTLS is **fail-closed** (ADR-0025), an unreachable CA halts *every new dial and certificate rotation* in the zone — so it is a hard availability dependency, not a background service. Run it **HA across independent domains** (as with the etcd quorum) and keep it **off the D-server hosts**, so a storage-node loss and a control-plane loss stay independent. A single-instance CA is a zone-wide single point of failure the moment a certificate needs to rotate.

## 7.4 Worked example — first single-zone deployment (M4–M5)

The **Small multi-node** profile becomes concrete at the first real deployment ([single-zone deployment diagram](diagrams/single-zone-deployment.mermaid)): M4 completes the data plane it runs on and M5 supplies its trust fabric; the ★ Step-2 release point itself is **M8** (proposal 0013). Two shapes illustrate the same topology reasoning — a **homelab** (own machines, one building/grid/ISP) and a **Hetzner single-zone** rental (genuine per-server hardware independence within one EU location). Both spend their failure-domain budget on the D servers first, give TiKV+PD their own quorum spread, and treat gateway/custodian as movable, stateless roles.

| Shape | D-server domains | Fault tolerance | Disaster-recoverable? |
|-------|------------------|-----------------|-----------------------|
| Minimum honest | 6 (≈2 fragments/domain) | any single domain | No — one site |
| Full RS(6,3) | 9 (1 fragment/domain) | any 3 domains | No — one site |

Single-zone — homelab *or* one Hetzner location — survives disk/host/domain failure (the M3 repair story, on honest hardware) but **not loss of the whole site**; cross-zone replication (L3) is M9+. So the first deployment is a production-durable *single-site* data plane — a soft stopping point on the way to the M8 release (proposal 0013), not yet the released product — governed by §8.2's rule: keep an out-of-band backup that does not depend on this cluster.

Day-one verification gates trust, in order: (1) **label** each D server's failure domain so the selector spreads fragments; (2) **verify spread** — confirm a written chunk's 9 fragments landed in 9 distinct domains, or fix the labels; (3) **watch the durability plane** — under-replicated count must sit at zero in steady state; (4) **do the failure test** — kill one D server and watch reads keep serving from survivors, under-replicated count rise, the custodian rebuild, and the count return to zero. If that loop does not complete, the deployment is not yet production-durable.

The full operational note — concrete machine counts, Hetzner `hcloud` provisioning, the `deploy/` TiKV+PD bring-up, failure-domain labelling, and the day-one runbook — is the [M4 first-deployment blueprint](m4-first-deployment-blueprint.md). It is operational guidance, not a normative spec.

## 7.5 Communication paths, ports, and protocols

The wiring of a production single-zone (Small multi-node) deployment — who dials whom, over what protocol, on which port, with what transport security. The dev single-binary profile collapses all of these to in-process calls (no sockets); §3.2 gives the external-interface view, this table the deployment-internal one.

| Path | Protocol | Default port | Transport security | Reference |
|------|----------|--------------|--------------------|-----------|
| App → S3 gateway | HTTP / S3 | operator-set (e.g. `443`, `8080` in dev) | TLS; **S3 SigV4** | §3.2, §8.5 |
| App → SDK gateway | gRPC | operator-set | TLS; **OIDC** | §3.2 |
| Operator → Management API | gRPC / REST | operator-set | **mTLS + OIDC** | ADR-0013, §8.5 |
| Gateway / client lib → D server | gRPC | **`50051`** (Wyrd default) | **mTLS, no plaintext fallback** | direct **bulk fragment** I/O; ADR-0025; `crates/server/src/cli.rs:32` |
| Gateway / client lib → metadata (TiKV) | gRPC | PD `2379`, TiKV `20160` | mTLS | atomic multi-key commit; ADR-0008. *redb backend is embedded — in-process, no port* |
| Custodian → D servers | gRPC | `50051` | mTLS | scrub / repair / reconstruct |
| Custodian → metadata (TiKV) | gRPC | PD `2379`, TiKV `20160` | mTLS | under-replication scan, chunk-maps |
| All components → Coordination (etcd) | etcd gRPC | client `2379`, peer `2380` | mTLS | discovery, leader election, locks; ADR-0006 |
| D server → Coordination (etcd) | etcd gRPC | `2379` | mTLS | registration (id · endpoint · fd label), lease renewal |
| Components → OTLP collector | OTLP/gRPC, OTLP/HTTP | `4317`, `4318` | TLS | outbound push; ADR-0012 |
| Prometheus → components | HTTP | operator-set | TLS | metrics scrape; ADR-0012 |

**Port honesty.** Only `50051` (the D-server gRPC bind) is fixed in Wyrd's own code today (`crates/server/src/cli.rs:32`). The metadata, coordination, and telemetry ports — PD `2379` / TiKV `20160`, etcd `2379`/`2380`, OTLP `4317`/`4318` — are the **upstream projects' conventional defaults**, not Wyrd's to assign. The S3, SDK, management, and Prometheus-scrape *listen* addresses are **operator-configured**; M4 fixes the exact flag names (see the blueprint's `[wyrd-config]` markers). All internal service-to-service dials are **mTLS under the provider CA with no plaintext fallback** (ADR-0005, ADR-0025); a plaintext internal dial is refused (M2).

**Two planes.** Bulk **fragment** data flows directly client/gateway → D servers (the gRPC chunk path that scales with the fleet); the **metadata commit** is a separate, smaller gRPC path to TiKV. Keeping them distinct is the Colossus-class separation that lets throughput scale with D servers rather than through a metadata bottleneck. On a private-network deployment (e.g. Hetzner vSwitch) only the gateway's S3 port is exposed publicly; every other path stays on the internal network.

**Out of single-zone scope.** Cross-zone replication (L3) uses **NATS JetStream** (client `4222`) and appears only at the **provider-fleet** profile (M9+); it is not part of a single-zone deployment (ADR-0027).
