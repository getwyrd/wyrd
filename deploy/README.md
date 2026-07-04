# `deploy/` — bring-up artifacts, outside the Cargo workspace

Deployment artifacts live here, **outside** the Rust workspace (ADR-0010): the
structural guard that "makes it hard for orchestrator coupling to sneak into a
component" — no crate imports an orchestrator API; peers are discovered through L5
Coordination (proposal 0007 §"Deployment").

## `tikv-single-node/` — throwaway single-node TiKV (M4.1)

A minimal `pd` + `tikv` pair for CI/eval, so the `metadata-tikv` conformance suite
can run against a real TiKV (proposal 0007 §"Suggested PR sequence" item 1). It is
**not** the production tier: the "Small multi-node Production" stack (TiKV-small +
its PD cluster + a 3-node etcd ensemble for L5) is M4.5 (#256).

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

## `small-multi-node/` — the single-zone "Small multi-node Production" bring-up (M4.5)

The production topology (M4.5, #256; proposal 0015 §"Deployment: TiKV/PD as a
stateful, disk-affine, orchestrator-agnostic tier"; architecture §7.1's "Small
multi-node" profile row): **TiKV-small + its own 3-node PD ensemble + a SEPARATE
3-node etcd ensemble for L5 Coordination (ADR-0006) + local-disk D servers** — no
L2/L3/TiDB. Unlike `tikv-single-node/`'s throwaway single-node pair on host
networking, this stack has multiple replicas of the same role (3 PD, 3 etcd), so it
uses the default compose **bridge** network with per-service DNS names and distinct
published host ports instead. The D servers reuse the SAME `wyrd-dserver:local`
image the root dev `docker-compose.yml` builds
(`crates/chunkstore-grpc/tests/dserver/Dockerfile`) and the `wyrd d-server` role —
this is a fresh stack, not an edit to that repo-root file.

```sh
docker compose -f deploy/small-multi-node/docker-compose.yml up -d
# etcd (L5 Coordination): 127.0.0.1:12379 / 22379 / 32379
# PD (TiKV's own coordinator): 127.0.0.1:23791 / 23792 / 23793
# TiKV-small: 127.0.0.1:20160 (store), 127.0.0.1:20180 (status)
# D servers: 127.0.0.1:50061 / 50062 / 50063
docker compose -f deploy/small-multi-node/docker-compose.yml down -v
```

Or via `cargo xtask deploy-small-multi-node`, which brings the stack up, waits for
every component to accept connections, and tears it down — a bring-up **smoke
check**, mirroring `tikv-conformance`'s docker-availability gating (hard failure in
CI, warn-and-skip locally with no Docker). Like `integration` / `tikv-conformance`,
it needs a container runtime and is **not** part of `cargo xtask ci`.

**Deployment prerequisite (out of scope here — proposal 0015's "Deployment
prerequisite" note, tracked as #365 plus an untracked gateway/custodian item).**
`wyrd d-server` does not yet dial an external `Coordination` endpoint — each
instance registers against its own in-process `MemCoordination`
(`crates/server/src/cli.rs`) — and there is no runnable gateway/custodian process
role yet. So this stack stands up on **static endpoints**; "peers discovered
through L5" is this prerequisite's DoD, not this slice's.

**No orchestrator coupling.** ADR-0010's invariant — "no code couples to
orchestrator APIs" — is checked by `cargo xtask ci`'s `deploy-guard` step
(`xtask/src/main.rs`'s `run_orchestrator_guard`, backed by the library module
`xtask::deploy_guard`) and exercised at Check by
`xtask/tests/deploy_no_orchestrator_coupling.rs`, which also asserts this stack's
`docker compose config` parses and declares all four component roles.
