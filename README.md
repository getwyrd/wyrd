# Wyrd

> **What is written, stays written.**

Wyrd is a globally scalable, atomically consistent distributed file storage
foundation. It keeps metadata and bulk data architecturally separate, yet every
write commits as a single, indivisible act: from the caller's point of view, a
write either happened in full or not at all. It scales from one static binary on
a laptop to a multi-region fleet in production — the same system, configured
differently. Written in Rust, licensed Apache-2.0.

**Status:** early implementation. The single-process slice works end to end —
Milestone 0 (atomic commit, proven under deterministic simulation) and
Milestone 1 (real Reed-Solomon erasure coding in the data path) are complete;
networked multi-process storage (Milestone 2) is next. Wyrd is **not yet
deployable** and carries no durability or stability promise at this stage.

## Why it exists

Many systems do part of this; no open-source one combines all of it: a rigorous
atomic commit point, a globally consistent namespace, erasure coding within a
zone and geo-replication across zones, pluggable metadata and chunk backends
behind narrow interfaces, an on-disk format specified so the data outlives the
software that wrote it — and correctness treated as the headline feature,
verified with deterministic simulation testing from day one.

| Property | What it means |
|----------|---------------|
| **atomic** | Commit-point atomicity. A write is linearized at a single point — no torn state, no half-written files, ever observable. |
| **global** | One strongly consistent namespace across many zones; per-file writes linearize at a home zone. |
| **durable** | Configurable per zone: replication or Reed-Solomon erasure coding within a zone, whole-copy geo-replication across them. |
| **pluggable** | Narrow backends — an embedded store for development, a distributed store for production — behind the same interface. |
| **provable** | Correctness as a feature: deterministic simulation testing, where every bug reproduces from a seed. |

The model follows the Colossus-class lineage — a global control plane over
per-zone storage, with bulk data flowing directly between client and storage
servers so throughput scales with the fleet rather than through a bottleneck.

## The name

In Norse and Old English myth, *wyrd* is fate — not a fixed script, but the
woven web of what has happened, what is happening, and what is yet owed. A
storage system is, in the end, a keeper of wyrd: it holds what was written,
weaves in what is being written, and carries the debts of what it still owes.
The components are named for the Norns who tend it — **Urth** (what has become),
**Verdandi** (what is becoming), and **Skuld** (what is owed). It is also,
cheerfully, a homophone of *weird* — the correct word for what happens to
distributed storage at 3 a.m. See [`docs/NAME.md`](docs/NAME.md).

## Documentation

This repository is the single source of truth for Wyrd's documentation,
authored in Markdown and published to [getwyrd.dev](https://getwyrd.dev).

- [`docs/`](docs/README.md) — where everything lives, and how it's organized.
- [`docs/design/`](docs/design/README.md) — **start here**: architecture,
  specifications, decision records (ADRs), and proposals.

## Repository layout

Wyrd is a Cargo workspace. Following [ADR-0016], it starts coarse — foundation
crates plus a combined `core` — and splits as boundaries firm up. The same ADR
sets the dependency rule: implementations and consumers depend on `traits`,
never on each other's concretes; only the `server` binary wires concretes
together.

| Crate | Role |
|-------|------|
| [`crates/traits`](crates/traits) | The narrow interfaces everything depends on. |
| [`crates/chunk-format`](crates/chunk-format) | The on-disk chunk/fragment format codec and conformance vectors. |
| [`crates/proto`](crates/proto) | Protocol definitions. |
| [`crates/core`](crates/core) | Combined core logic (split as boundaries firm up). |
| [`crates/server`](crates/server) | The binary that wires concrete implementations together. |
| [`crates/testkit`](crates/testkit) | Shared test scaffolding and fixtures. |
| [`xtask`](xtask) | Workspace automation tasks. |

Build and test the workspace with the standard Cargo flow:

```sh
cargo build
cargo test
```

## Development & testing

The full development workflow lives in [`cargo xtask`](xtask) — automation
written in Rust rather than YAML, so the same checks run on a laptop and in CI
([ADR-0016]):

| Command | What it runs |
|---------|--------------|
| `cargo xtask ci` | The local **merge gate**, and the single check CI calls: `fmt --check`, `clippy -D warnings`, build, test, `cargo deny check`, the conformance vectors, and the madsim DST sweep. Run it before pushing. |
| `cargo xtask conformance` | The on-disk chunk-format reader against the committed conformance vectors (also run inside `ci`). |
| `cargo xtask dst` | The madsim deterministic-simulation commit-protocol tests across a seed sweep (also run inside `ci`). |
| `cargo xtask integration` | The **Tier-2** end-to-end test against a cluster of real, networked gRPC D servers. **Not** part of `ci` — it needs a container runtime (see below). |
| `cargo xtask bench` | The tracked throughput benchmarks (tracked for regression visibility, not gated). |

Plain `cargo test` silently **skips** the Tier-2 integration test — it is
`#[ignore]`d and needs a live cluster; run it through `cargo xtask integration`.

**`cargo xtask integration` prerequisites:** a running **Docker** daemon and the
**Docker Compose plugin** (`docker compose`). The tier stands up a cluster of D
servers under docker-compose; without Docker it is skipped locally and fails in
CI. Set **`WYRD_DSERVER_COUNT`** (default 9, minimum 2) to change how many D
servers the cluster runs.

### Try it

A self-contained S3 PUT/GET round-trip — no cluster, no setup:

```sh
cargo run -p wyrd-server --bin wyrd -- demo
```

## Run a local cluster

You can stand up the consolidated single-zone cluster on one machine and drive it
with the `wyrd` gateway client mode — the binary fans each object's erasure-coded
fragments across the cluster's networked D servers over gRPC. The bring-up
fixtures under `deploy/` are organised as testing **profiles** (ADR-0043), each the
minimal topology for a class of test; `deploy/small-multi-node-fdb/` is the full
single-zone profile and the one to poke by hand.

Bring the stack up — a 3-node etcd + a 3-process **FoundationDB** cluster (the
production metadata backend, ADR-0042) + **9** D servers
(one per failure domain, matching the default `rs(6,3)` = 9 fragments, any **k = 6**
of which reconstruct the data) + custodians + S3 gateways:

```sh
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml up -d
# a fresh FoundationDB cluster must be configured once before it will serve reads/writes
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml exec fdb0 \
  fdbcli --exec "configure new double ssd"
```

The first `up` builds the `wyrd:fdb` image (`--features fdb,etcd`), which links the
FoundationDB client library — a multi-GB build the first time. See `deploy/README.md`
for the full bring-up, the published ports, and teardown.

> The TiKV peer of this stack (`deploy/small-multi-node/`) is still in the tree and
> still runs, but TiKV is a **retained fallback with development stood down** (#443).
> Use FoundationDB.

Point the gateway client at the D servers' published endpoints. `--endpoints`
switches `put`/`get` from the local-disk path to the **static-endpoints gateway
client mode**: fragments fan out over gRPC to the listed D servers, while the
object metadata (and the persisted inode allocator) is held locally under
`--data-dir`.

```sh
ENDPOINTS=http://127.0.0.1:50061,http://127.0.0.1:50062,http://127.0.0.1:50063,http://127.0.0.1:50064,http://127.0.0.1:50065,http://127.0.0.1:50066,http://127.0.0.1:50067,http://127.0.0.1:50068,http://127.0.0.1:50069

# PUT: erasure-code each object and fan its fragments across the D servers.
cargo run --bin wyrd -- put ./somefile --key obj/one \
  --endpoints "$ENDPOINTS" --data-dir ./cluster-meta
cargo run --bin wyrd -- put ./otherfile --key obj/two \
  --endpoints "$ENDPOINTS" --data-dir ./cluster-meta

# GET: reconstruct them from fragments read back over gRPC, byte-identical.
cargo run --bin wyrd -- get obj/one --out ./out-one.bin \
  --endpoints "$ENDPOINTS" --data-dir ./cluster-meta
cargo run --bin wyrd -- get obj/two --out ./out-two.bin \
  --endpoints "$ENDPOINTS" --data-dir ./cluster-meta

diff ./somefile ./out-one.bin && diff ./otherfile ./out-two.bin && echo "round-trip ok"
```

The two `put`s above share one `--data-dir`: each gets its own inode from the
persisted allocator there, so storing several distinct objects across separate
`wyrd` invocations works (the metadata, not the D servers, tracks which object
owns which fragments).

Tear the cluster down (and drop its volumes) when finished:

```sh
docker compose -f deploy/small-multi-node-fdb/docker-compose.yml down -v
```

Notes and current limits:

- This client-mode endpoint list is **static** — the gateway client dials exactly
  the D servers you name. The stack's D servers *do* connect to and register through
  the etcd L5 Coordination backend (ADR-0006), but `wyrd put/get --endpoints` bypasses
  that and talks to the listed endpoints directly; discovery-driven placement is later
  work (and the registered address is not yet routable across containers, #458).
- Metadata is held locally under `--data-dir`, so the `put` and `get` above must
  share it. Wiring the S3 gateway role itself over the cluster's shared backends
  (FoundationDB metadata + D-server fanout) is tracked as #454 → #455.
- The other `deploy/` profiles are for automated tests, not hand-driving: the
  single-node conformance fixtures (`fdb-single-node/`, `etcd-single-node/`), the
  ephemeral integration fixture (`crates/chunkstore-grpc/tests/docker-compose.yml`,
  `cargo xtask integration`), and the fault-injection fixture
  (`fdb-multi-replica/`). The TiKV peers of those fixtures (`tikv-single-node/`,
  `tikv-multi-replica/`) remain for the retained fallback (#443). See ADR-0043 for
  which test class uses which.

## Security

Wyrd is pre-release software and carries no security promise yet, but we still
want to hear about vulnerabilities early. Please report them **privately** — see
[SECURITY.md](SECURITY.md) for how.

## Contributing & governance

Contributions are welcome. Please read the
[Code of Conduct](docs/governance/CODE_OF_CONDUCT.md) and
[Governance](docs/governance/GOVERNANCE.md) documents before getting involved.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE)
for attribution.

[ADR-0016]: docs/design/adr/0016-monorepo-and-crate-structure.md
