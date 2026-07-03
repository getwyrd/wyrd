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
