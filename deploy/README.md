# `deploy/` — bring-up artifacts, outside the Cargo workspace

Deployment artifacts live here, **outside** the Rust workspace (ADR-0010): the
structural guard that "makes it hard for orchestrator coupling to sneak into a
component" — no crate imports an orchestrator API; peers are discovered through L5
Coordination (proposal 0007 §"Deployment").

## Profile matrix — the two metadata backends across the three ADR-0043 fixture tiers

Wyrd's two production-candidate metadata backends — **TiKV** (the retained fallback,
#443) and **FoundationDB** (the chosen production backend, ADR-0042) — each have a stack
at all three fixture tiers (ADR-0043): a throwaway single-node testbed for conformance, a
≥3-node multi-replica cluster with a fault sidecar for the fault battery, and a full
single-zone "small multi-node" stack for the first-deployment gate. Which suite drives
which tier is fixed:

| Tier | TiKV | FoundationDB | Driven by |
| --- | --- | --- | --- |
| single-node (conformance testbed) | `tikv-single-node/` | `fdb-single-node/` | conformance run (`cargo xtask tikv-conformance` / `cargo xtask fdb-conformance`) |
| multi-replica (fault battery) | `tikv-multi-replica/` | `fdb-multi-replica/` | #257 Tier-1 leg / #442 fault battery |
| small-multi-node (single-zone) | `small-multi-node/` | `small-multi-node-fdb/` | #367 first-deployment gate |

(The separate `etcd-single-node/` testbed is an L5-Coordination fixture, not a
metadata-backend tier, so it is outside this matrix.)

**The single-zone pair, and which is canonical.** The two single-zone stacks are a named
**pair**, not one "the" stack and one variant. `small-multi-node/` keeps its unqualified
name and **is the TiKV peer of** `small-multi-node-fdb/`; `small-multi-node-fdb/` is the
FoundationDB peer. The **canonical** single-zone stack is `small-multi-node-fdb/`
(FoundationDB): #442's fault + contention battery recorded **"go"** on the FDB driver
(`docs/design/reviews/m4-fdb-go-no-go.md`), so FoundationDB is the production backend per
ADR-0042 and the canonical stack flipped to it (#443). Both stay runnable.
The rename of `small-multi-node/` → `small-multi-node-tikv/` is deferred (to avoid
churning PR #457's landed paths); until it happens, this pairing is recorded here in prose
so the unqualified `small-multi-node/` name is not read as "the" stack.

> **TiKV is the retained fallback; active development is stood down (#443).** It is kept
> in the tree, buildable and community-continuable — not removed — but FoundationDB is the
> production backend (ADR-0042). Do not pick the TiKV stacks for a new production
> deployment: the `tikv` feature pulls in `tikv-client` 0.4.0, which is abandoned upstream
> and carries **known unpatched advisories in its TLS path** — including a live DoS in
> certificate-revocation-list parsing (RUSTSEC-2026-0104, high). The boundary is recorded
> in `deny-all-features.toml` (#543). The continuation backlog is the **Metadata Store TiKV** milestone,
> open for anyone to pick up.

## `tikv-single-node/` — throwaway single-node TiKV (M4.1)

A minimal `pd` + `tikv` pair for CI/eval, so the `metadata-tikv` conformance suite
can run against a real TiKV (proposal 0007 §"Suggested PR sequence" item 1). It is
**not** the production tier: the consolidated single-zone stack (3-node TiKV + its PD
ensemble + a 3-node etcd ensemble for L5 + D servers / custodians / gateways) is
M4.5 (#256) — see `small-multi-node/` below.

Driven by `cargo xtask tikv-conformance`, which brings the stack up, points
`WYRD_TIKV_PD_ENDPOINTS` at PD (`127.0.0.1:2379`), runs
`cargo test -p wyrd-metadata-tikv --features tikv`, and tears the stack down. Or by
hand:

```sh
docker compose -f deploy/tikv-single-node/docker-compose.yml up -d
WYRD_TIKV_PD_ENDPOINTS=127.0.0.1:2379 \
  cargo test -p wyrd-metadata-tikv --features tikv --test conformance
docker compose -f deploy/tikv-single-node/docker-compose.yml down -v
```

With no TiKV configured, the conformance test **skips cleanly** and `cargo xtask ci`
stays green.

## `fdb-single-node/` — throwaway single-node FoundationDB (M4, ADR-0042)

The FoundationDB peer of `tikv-single-node/`: a single `fdbserver` (its own coordinator,
storage, and transaction role) for the `metadata-fdb` conformance run (ADR-0042, issue
#438). Like the TiKV testbed it is **not** the production tier — the single-zone stack
`small-multi-node-fdb/` is.

Driven by `cargo xtask fdb-conformance`, which brings the stack up, runs `configure new`
once (a fresh `fdbserver` reports `configuration missing` until configured), writes the
matching host-side cluster file, points `WYRD_FDB_CLUSTER_FILE` at it, and runs the
`metadata-fdb` conformance suite. It uses `network_mode: host` **deliberately** — the
opposite of the multi-replica stack below — so a host `libfdb_c` client can dial the
address the server advertises (`127.0.0.1:4500`); `FDB_CLUSTER_FILE_CONTENTS` is pinned so
the host-side cluster file is byte-identical. With no FDB configured the conformance test
**skips cleanly** and `cargo xtask ci` stays green.

## `fdb-multi-replica/` — throwaway ≥3-process FoundationDB cluster (#442)

The FoundationDB peer of `tikv-multi-replica/`: three `fdbserver` processes
(`fdb0..fdb2`) in `double` redundancy (`configure new double ssd`), each its own
coordinator, so the cluster survives a single-process loss. This is the stack #442's fault
battery — the go/no-go gate for making FDB the default — drives; a single-process cluster
cannot exhibit the replica-loss and mid-commit-kill faults it samples.

Like `tikv-multi-replica/`, it uses a **bridge network with one netns per node** (static
IPs `172.30.58.11..13`) so a partition injected with `iptables` inside a target node's
netns is genuinely bidirectional — under host networking every node would source its
traffic from `127.0.0.1` and an `iptables -s/-d <ip>` cut would never match (the
iteration-13 topology fix, recorded on the TiKV peer). The fault sidecar is **reused
as-is** from `tikv-multi-replica/iptables-agent/` (a generic `iptables`-entrypoint image,
not TiKV-specific); it is declared behind the `fault` compose profile so a plain `up`
never starts it, and run on demand in a target node's netns:

```sh
docker compose -f deploy/fdb-multi-replica/docker-compose.yml up -d
docker compose -f deploy/fdb-multi-replica/docker-compose.yml exec fdb0 \
  fdbcli --exec "configure new double ssd"
fdbcli -C <(echo 'docker:docker@172.30.58.11:4500,172.30.58.12:4500,172.30.58.13:4500') \
  --exec "status minimal"                       # -> "The database is available."
# build the fault agent, then partition fdb2 from fdb0/fdb1 (heal by replaying with -D):
docker compose --profile fault -f deploy/fdb-multi-replica/docker-compose.yml build iptables-agent
docker run --rm --privileged --network container:$(docker compose \
  -f deploy/fdb-multi-replica/docker-compose.yml ps -q fdb2) \
  wyrd-iptables:local -A INPUT -s 172.30.58.11 -j DROP
docker compose -f deploy/fdb-multi-replica/docker-compose.yml down -v
```

## `small-multi-node/` — the TiKV single-zone stack, one of the single-zone pair (M4.5)

The **TiKV peer** of the single-zone pair (see the profile matrix above): `small-multi-node/`
**is the TiKV peer of** `small-multi-node-fdb/`. It is the **retained fallback**, not the
canonical stack — #442 recorded "go" on FoundationDB, so `small-multi-node-fdb/` is canonical
and active TiKV development is stood down (#443). This stack stays runnable and is still the
right thing to bring up when working the TiKV continuation backlog; it is **not** the stack to
pick for a new production deployment (see the stand-down note in the profile matrix above).
A minimum-viable single-zone deployment for testing (M4.5, #256;
proposal 0015 §"Deployment: TiKV/PD as a stateful, disk-affine, orchestrator-agnostic
tier"; architecture §7.1's "Small multi-node" profile row). Every role at a real
single-zone cardinality — no L2/L3/TiDB (ADR-0020 is out of scope until M9+):

| Role | Count | Why |
| --- | --- | --- |
| etcd ensemble (L5 Coordination, ADR-0006) | 3 | minimum quorum; separate from PD's own embedded etcd |
| PD ensemble (TiKV's coordinator) | 3 | minimum quorum |
| TiKV store | 3 | minimum quorum for a replicated Raft group |
| D servers (`wyrd d-server`) | 9 | one per failure domain fd0..fd8 — matches default `rs(6,3)` (9 fragments, survives 3 losses) |
| custodians (`wyrd custodian`) | 3 | leader-elected reconstruction/repair |
| S3 gateways (`wyrd s3`) | 3 | S3-compatible HTTP front door |

Multiple replicas of the same role mean the stack uses the default compose **bridge**
network with per-service DNS names and distinct published host ports (host networking
cannot give same-port replicas distinct addresses). The wyrd roles all run the SAME
`wyrd` binary (different subcommand), built **once** here from
`crates/chunkstore-grpc/tests/dserver/Dockerfile` with `--features tikv,etcd` (build
arg `FEATURES`) and tagged **`wyrd-single-zone:local`** — a distinct tag from the
integration fixture's default-feature `wyrd-dserver:test`, so the two never clobber
each other (ADR-0043).

```sh
docker compose -f deploy/small-multi-node/docker-compose.yml up -d
# etcd (L5 Coordination): 127.0.0.1:12379 / 22379 / 32379
# PD (TiKV's own coordinator): 127.0.0.1:23791 / 23792 / 23793
# TiKV: 127.0.0.1:20160-20162 (store), 127.0.0.1:20180-20182 (status)
# D servers (fd0..fd8): 127.0.0.1:50061 .. 50069
# S3 gateways: 127.0.0.1:8081 / 8082 / 8083 (SigV4; test creds wyrd-test / wyrd-test-secret)
# custodians: no published port (in-process Prometheus read-back only)
docker compose -f deploy/small-multi-node/docker-compose.yml down -v
```

Or via `cargo xtask deploy-small-multi-node`, which brings the stack up, waits for
every published component to accept connections, and tears it down — a bring-up
**smoke check**, mirroring `tikv-conformance`'s docker-availability gating (hard
failure in CI, warn-and-skip locally with no Docker). Like `integration` /
`tikv-conformance`, it needs a container runtime and is **not** part of
`cargo xtask ci`.

**Genuinely wired backends.** Built with `--features tikv,etcd`, so the D servers dial
the etcd ensemble for L5 Coordination (`--coordination-backend etcd` +
`WYRD_ETCD_ENDPOINTS`, #449) — a real cross-process connect + register, not the
in-process `MemCoordination` default — and the custodians open the TiKV metadata
backend (`--metadata-backend tikv` + `WYRD_TIKV_PD_ENDPOINTS`). Caveat: each D-server
registration advertises the server's *bound* address (`http://0.0.0.0:50051`), which
is not routable for cross-container discovery until d-server gains `--advertise-addr`
(#458); it is unused here, since the custodians dial static `--endpoints` rather than
discovering through etcd.

**Two honest limits — this is a topology bring-up / smoke target, not an end-to-end
object pipeline (tracked, #454 → #455):**
- The `wyrd s3` gateway is **standalone** (#454). `cmd_s3` (`crates/server/src/cli.rs`)
  hardcodes local redb + FS + `MemCoordination` and ignores
  `--metadata-backend`/`--coordination-backend`, so each of the 3 gateways is an
  independent single-node island — not a pool over the D servers / TiKV / etcd.
- There is **no closed write path** yet (#455, depends on #454). Nothing writes cluster
  metadata into TiKV or fans chunks to the 9 D servers, so a `--metadata-backend tikv`
  custodian opens an (initially empty) store and sees zero repair obligations until
  exercised out-of-band, e.g. `wyrd put <file> --key k --endpoints
  dserver0:50051,…,dserver8:50051 --metadata-backend tikv` (with `WYRD_TIKV_PD_ENDPOINTS`
  set) against the running stack.

The 3 custodians all self-elect under the process-local `MemCoordination` (no
cross-container fencing) and reconcile concurrently — safe because the reconstruction
repoint is a version-conditional CAS commit; real fencing awaits the etcd-backed
custodian coordination (#365).

**No orchestrator coupling.** ADR-0010's invariant — "no code couples to
orchestrator APIs" — is checked by `cargo xtask ci`'s `deploy-guard` step
(`xtask/src/main.rs`'s `run_orchestrator_guard`, backed by the library module
`xtask::deploy_guard`) and exercised at Check by
`xtask/tests/deploy_no_orchestrator_coupling.rs`, which also asserts this stack's
`docker compose config` parses and declares all four component roles.

## `small-multi-node-fdb/` — the FoundationDB single-zone stack, the other single-zone peer

The **FoundationDB peer** of the single-zone pair. It has the **identical role topology**
to `small-multi-node/` (3-node etcd ensemble + 9 D servers + 3 custodians + 3 S3 gateways)
with the metadata tier swapped: PD + TiKV are replaced by a 3-process FoundationDB cluster
(`fdb0..fdb2`, `configure new double ssd`), and every wyrd role that opens a metadata store
opens `--metadata-backend fdb` instead of `--metadata-backend tikv` (the 3 custodians and 3
gateways; the 9 D servers open no metadata backend, exactly as on the TiKV peer).
`small-multi-node/` **is the TiKV peer of** `small-multi-node-fdb/`; this FDB stack is the
**canonical** single-zone stack, since #442 recorded "go" on FoundationDB and #443 stood the
TiKV backend down to a retained fallback. Two directories, not a
compose override: the metadata tier swap is a whole service-set change, and the per-backend
convention already uses one directory per backend profile.

The wyrd roles run the **`wyrd:fdb` image** (#470), built once here (on `dserver0`) from
`deploy/docker/wyrd/Dockerfile` with `--features fdb,etcd` — that Dockerfile installs the
FoundationDB client so `foundationdb-sys` can link `libfdb_c` (the default test Dockerfile
cannot build `--features fdb`, which is why #470 is a hard prerequisite). The custodians
and gateways open the FDB store through the cluster file on the shared `fdb-cluster` volume
(`WYRD_FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster`); D servers dial etcd for L5
Coordination (`--coordination-backend etcd`), exactly as on the TiKV peer.

**Bring-up is the deferred, maintainer-confirmed leg.** A full bring-up is 21 containers
and requires the multi-GB `wyrd:fdb` image to be built (`cargo build --release --features
fdb,etcd`), so there is **no `cargo xtask` arm** for it yet (a one-command runner is a named
follow-up, natural home alongside #442's battery). The maintainer confirms it by hand:

```sh
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml up -d
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml exec fdb0 \
  fdbcli --exec "configure new double ssd"
# etcd (L5 Coordination): 127.0.0.1:12379 / 22379 / 32379
# D servers (fd0..fd8): 127.0.0.1:50061 .. 50069
# S3 gateways: 127.0.0.1:8081 / 8082 / 8083 (SigV4; test creds wyrd-test / wyrd-test-secret)
curl -s http://127.0.0.1:8081/    # an S3 gateway answers with --metadata-backend fdb
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml down -v
```

**Same honest limits as the TiKV peer** (#454 → #455): the `wyrd s3` gateway is standalone
(`cmd_s3` hardcodes local redb + FS + `MemCoordination`, #454) and there is no closed write
path yet (#455). "An S3 gateway answers with `--metadata-backend fdb`" means the role
starts and serves, not that an object round-trips through FDB and the 9 D servers — the
same topology bring-up / smoke bar its TiKV peer documents, which is what #442 needs. The
full closed write path remains #455's demonstration.
