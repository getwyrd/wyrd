# `deploy/` — bring-up artifacts, outside the Cargo workspace

Deployment artifacts live here, **outside** the Rust workspace (ADR-0010): the
structural guard that "makes it hard for orchestrator coupling to sneak into a
component" — no crate imports an orchestrator API; peers are discovered through L5
Coordination (proposal 0007 §"Deployment").

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

## `small-multi-node/` — the consolidated single-zone stack (M4.5)

The one canonical, minimum-viable single-zone deployment for testing (M4.5, #256;
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
`WYRD_ETCD_ENDPOINTS`, #449) — real cross-process registration, not the in-process
`MemCoordination` default — and the custodians open the TiKV metadata backend
(`--metadata-backend tikv` + `WYRD_TIKV_PD_ENDPOINTS`).

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
