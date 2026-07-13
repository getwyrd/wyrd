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
| Small multi-node | 3-node etcd | step-ca (SPIRE reserved) | **FoundationDB** / redb | none — single-zone | local-disk D servers | replication(n) or rs(k,m) | Production |
| Provider fleet | dedicated etcd per zone | step-ca, HA (SPIRE reserved) | **FoundationDB** | TiDB + L3 replication (multi-region) | local-disk D servers | rs(k,m) | Production |

**The production metadata backend is FoundationDB** (ADR-0042, which supersedes ADR-0008's TiKV choice). It cleared the M4 fault + contention battery — the go/no-go gate on our mapping layer, not on FDB itself (`docs/design/reviews/m4-fdb-go-no-go.md`, #442). The canonical single-zone stack is `deploy/small-multi-node-fdb/`.

**TiKV is a retained fallback, and active development on it is stood down** (#443). The `metadata-tikv` crate, the `tikv` feature, the CLI backend variant and the TiKV deploy stacks all remain in the tree, buildable and community-continuable — nothing was removed, and the continuation backlog stays open under the *Metadata Store TiKV* milestone. But it is not a production path: `tikv-client` 0.4.0 is abandoned upstream and carries unpatched advisories in its TLS stack, including a live DoS in CRL parsing (RUSTSEC-2026-0104, high); the exposure boundary is recorded in `deny-all-features.toml` (#543). Choose it only if you are continuing that backlog, never for a new deployment.

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
| Gateway / client lib → metadata (**FoundationDB**) | FDB client protocol (`libfdb_c`) | `4500` (coordinators) | TLS | atomic multi-key commit; ADR-0042, §7.6. *redb backend is embedded — in-process, no port* |
| Custodian → D servers | gRPC | `50051` | mTLS | scrub / repair / reconstruct |
| Custodian → metadata (**FoundationDB**) | FDB client protocol (`libfdb_c`) | `4500` | TLS | under-replication scan, chunk-maps |
| *(fallback)* → metadata (TiKV) | gRPC | PD `2379`, TiKV `20160` | mTLS | the retained-fallback path only (#443); ADR-0008, superseded by ADR-0042 |
| All components → Coordination (etcd) | etcd gRPC | client `2379`, peer `2380` | mTLS | discovery, leader election, locks; ADR-0006 |
| D server → Coordination (etcd) | etcd gRPC | `2379` | mTLS | registration (id · endpoint · fd label), lease renewal |
| Components → OTLP collector | OTLP/gRPC, OTLP/HTTP | `4317`, `4318` | TLS | outbound push; ADR-0012 |
| Prometheus → components | HTTP | operator-set | TLS | metrics scrape; ADR-0012 |

**Port honesty.** Only `50051` (the D-server gRPC bind) is fixed in Wyrd's own code today (`crates/server/src/cli.rs:32`). The metadata, coordination, and telemetry ports — FDB `4500`, PD `2379` / TiKV `20160`, etcd `2379`/`2380`, OTLP `4317`/`4318` — are the **upstream projects' conventional defaults**, not Wyrd's to assign. The S3, SDK, management, and Prometheus-scrape *listen* addresses are **operator-configured**; M4 fixes the exact flag names (see the blueprint's `[wyrd-config]` markers). All internal service-to-service dials are **mTLS under the provider CA with no plaintext fallback** (ADR-0005, ADR-0025); a plaintext internal dial is refused (M2).

**Two planes.** Bulk **fragment** data flows directly client/gateway → D servers (the gRPC chunk path that scales with the fleet); the **metadata commit** is a separate, smaller path to the metadata store (FoundationDB in production). Keeping them distinct is the Colossus-class separation that lets throughput scale with D servers rather than through a metadata bottleneck. On a private-network deployment (e.g. Hetzner vSwitch) only the gateway's S3 port is exposed publicly; every other path stays on the internal network.

**Out of single-zone scope.** Cross-zone replication (L3) uses **NATS JetStream** (client `4222`) and appears only at the **provider-fleet** profile (M9+); it is not part of a single-zone deployment (ADR-0027).

## 7.6 FoundationDB metadata backend: packaging and version coupling

ADR-0042 chose FoundationDB (`fdb`, behind the off-by-default `--features fdb`) as the production `MetadataStore` for the Small multi-node and Provider fleet profiles (§7.1). Unlike the pure-Rust TiKV client, the `foundationdb` crate binds a **shared C library** (`libfdb_c`) whose wire protocol is **exactly** coupled to the cluster's — a client built against one FDB version cannot talk to a cluster running another, at all, ever. This section is the packaging contract that follows from that fact: how the client library reaches a Wyrd process, and what happens when it disagrees with the cluster (#441).

### Container path (primary)

The primary distribution is an OCI image carrying `wyrd` built with `--features fdb` plus a matching `libfdb_c`. Building and publishing that image is **#470's** deliverable, not this section's; this section fixes the *decision* the image implements. The image pins one `libfdb_c` version at build time — the same version-coupling rule as bare metal (below) applies unchanged; an image is a packaging choice, not an exemption from FDB's protocol coupling.

### Bare-metal path

Outside a container, the host needs FoundationDB's own `foundationdb-clients` `.deb`/`.rpm` installed (it ships `libfdb_c.so` and, optionally, `fdbcli`) — a **host prerequisite**, not something Wyrd's own build vendors or statically links (see "The single-binary trade" below). The cluster file (`fdb.cluster`) is located via `WYRD_FDB_CLUSTER_FILE` (`crates/metadata-fdb/src/lib.rs:386`), falling back to FoundationDB's own default `/etc/foundationdb/fdb.cluster` (`:390`, resolved by `config::cluster_file`, `:436`) — the same default a stock FoundationDB install already writes there, so a host that followed FoundationDB's own install instructions needs no Wyrd-specific configuration to be reached.

### Version coupling and the fail-closed guard

Before #441, a version-mismatched client and a genuinely unreachable cluster produced the **same** symptom: a bounded but anonymous `1031 transaction_timed_out` (the per-transaction deadline #438 added, `crates/metadata-fdb/src/lib.rs:424`). An operator who mismatched their client saw exactly the error an operator with a down cluster would see. The defect was **misdiagnosis**, not hanging.

`FdbMetadataStore::connect()` (`crates/metadata-fdb/src/lib.rs:1250`) — the constructor `open_fdb_meta` calls (`crates/server/src/cli.rs:175`), i.e. every `wyrd … --metadata-backend fdb` invocation — now performs a bounded readiness probe (`preflight`, `:1291`) via `Database::get_client_status()` before returning `Ok`. The pure, non-feature-gated `wyrd_metadata_fdb::preflight` module (`:832`, sibling to `classify`/`config`) classifies the result into `Ready` / `VersionSkew` / `Unreachable`; a non-`Ready` verdict fails the connect with a message naming the **cluster's** protocol version and pointing at the upgrade procedure below — instead of the anonymous timeout.

The discriminator is `Compatible == false` on a connection whose `Status` is `"connected"`. It is deliberately **not** "zero reachable coordinators" (under skew the coordinator list stays populated) and not `Healthy == false` alone (false in both failure cases). Anything the probe cannot positively identify as skew — an unparsable, novel, or late status — degrades to `Unreachable` *with* a version-coupling hint, **never** to a guessed `VersionSkew`: an operator whose cluster is merely down must not be sent hunting for a version mismatch.

Two shapes this guard deliberately keeps:

* `connect()` and its probe are **`async`**, awaited on the caller's runtime — exactly like the TiKV peer `open_tikv_meta` (`crates/server/src/cli.rs:147`). All seven `open_fdb_meta` call sites are already inside a Tokio runtime, so a probe that drove itself on a runtime of its own would panic with *"Cannot start a runtime from within a runtime"* on every invocation.
* `FdbMetadataStore::open()` (`crates/metadata-fdb/src/lib.rs:1326`), used only by the cluster-file-gated test harnesses, keeps **no** probe — so tests that point at an unreachable coordinator on purpose (`tests/timeout.rs`) are unaffected. The probe belongs to `connect()`, the *operator* path.

### The multi-version client upgrade dance

FoundationDB's own answer to a lockstep cluster upgrade is the **multi-version client**: a directory of additional `libfdb_c` versions the client loads via the `ExternalClientDirectory` network option, so one client process can speak to a cluster mid-upgrade. Wyrd exposes this as `WYRD_FDB_EXTERNAL_CLIENT_DIR` (`crates/metadata-fdb/src/lib.rs:398`), consumed by `ensure_network()` (`:1114`) when the client network boots; unset, behaviour is byte-identical to a client with no multi-version support.

The upgrade sequence:

1. **Add** the cluster's *new* `libfdb_c` version to every client's external-client directory (pointed to by `WYRD_FDB_EXTERNAL_CLIENT_DIR`) — the *old* library stays in place too, so the client can still speak the version the cluster is currently running.
2. **Upgrade** the cluster to the new FoundationDB version. During the transition the multi-version client speaks whichever protocol the coordinators currently answer with; a rolling cluster upgrade does not require every client to be updated in the same instant.
3. **Drop** the old `libfdb_c` from the external-client directory only once every client in the fleet has the new one and the cluster upgrade is complete — the mirror image of step 1.

This turns a cluster upgrade into a **configuration/image change** (update the external-client directory, then the cluster) rather than an architecture change — no code in Wyrd's own tree needs to move.

### Manual repro of the guided error

Reproducing the guided error needs a cluster running a FoundationDB version the client was not built against. #470 automates this against the `wyrd:fdb` image it builds (its acceptance criterion 4); until then, this is the manual procedure — **run and observed** against a `libfdb_c` 7.3.77 client and a `foundationdb/foundationdb:7.1.61` cluster:

```sh
# 1. Bring up a cluster running an OLDER FoundationDB than the client wyrd was built
#    against. --network host, matching deploy/fdb-single-node/docker-compose.yml's own
#    note: a libfdb_c client on the HOST must reach the address the server ADVERTISES
#    (127.0.0.1:4500), which a default bridge-network port mapping does not give you.
docker run -d --name fdb71 --network host \
  -e FDB_NETWORKING_MODE=host -e FDB_PORT=4500 -e FDB_PROCESS_CLASS=unset \
  -e FDB_CLUSTER_FILE_CONTENTS="docker:docker@127.0.0.1:4500" \
  foundationdb/foundationdb:7.1.61
docker exec fdb71 fdbcli --exec "configure new single memory"

# 2. Point a cluster file at it and run a `wyrd` built with --features fdb:
printf 'docker:docker@127.0.0.1:4500\n' > /tmp/skew.cluster
echo hello > /tmp/payload
WYRD_FDB_CLUSTER_FILE=/tmp/skew.cluster \
  wyrd put /tmp/payload --key smoke --metadata-backend fdb --data-dir /tmp/skew-data
```

Observed: the connect fails in **~200 ms** with **exit status 1** (not a panic, not a hang) and a message containing `client/cluster protocol version mismatch`, the cluster's reported protocol version `fdb00b071010000`, and a pointer to the multi-version upgrade procedure above — instead of the anonymous `1031 transaction_timed_out` this produced before #441.

For contrast, a cluster file naming an unreachable coordinator (`x:x@192.0.2.1:4500`, RFC 5737 TEST-NET-1) reports `cluster unreachable … reported as unreachable rather than a guessed version skew` and never claims a mismatch. Tear the skew cluster down with `docker rm -f fdb71`.

### The single-binary trade

The **single binary (dev)** profile (§7.1) stays a true static binary with **zero new demands** from this section: `crates/metadata-fdb/Cargo.toml`'s `default = []` means the default build never links `libfdb_c` — indeed a machine without it could not compile this crate's real driver at all if the feature were on by default. `fdb` is only reachable via the explicit, off-by-default `--features fdb` build — a **production**-tier build, per ADR-0014 (`docs/design/adr/0014-single-binary-dev-only.md:18`: the single-binary profile is *"for development and evaluation only"*, explicitly not a supported production tier).

FoundationDB itself does not support static-linking `libfdb_c`, and doing so would defeat the multi-version client above — the only sanctioned upgrade mechanism — so an `fdb`-backed Wyrd is **never** a single static binary; it always carries a shared-library dependency, container or bare metal. This is a property of FoundationDB's own distribution model, not a Wyrd shortcoming, and no ADR change follows from stating it: ADR-0014 already scopes single-binary to dev/eval, and `fdb` was never that profile.
