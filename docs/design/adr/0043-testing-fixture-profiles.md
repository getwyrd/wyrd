---
created: 06.07.2026 19:45
type: adr
status: Accepted
tags:
  - adr
---
# 0043. Testing fixture profiles: one minimal container topology per test class

## Context

Container-backed tests need real external components — a real TiKV to check the
metadata adapter's contract, a real etcd to check the coordination adapter's, a
network that can be partitioned to check consistency under faults, and the full
single-zone topology to check that a deployment stands up. Over milestones 2–4 these
accreted as separate `docker compose` fixtures under `deploy/` plus an integration
fixture beside the crate that needs it, and — until this ADR — a hand-driven
four-D-server stack at the repo root (`./docker-compose.yml`).

With the consolidated single-zone stack now standing every role up
(`deploy/small-multi-node/`: 3-node etcd + 3-node PD + 3-node TiKV + 9 D servers +
custodians + S3 gateways), a reasonable question is whether that one stack should be
*the* target for all container tests, retiring the smaller fixtures. The forces:

- **Feedback loop.** The full stack is 24 containers behind a feature-built
  (`tikv,etcd`) image. A test that needs only a single etcd should not wait for it.
- **Attributability.** The Tier-1 metadata-consistency suite (`metadata-tier1`)
  injects a *symmetric partition* of the TiKV region leader via an `iptables` agent
  running in the target's netns, which requires the deterministic static-IP topology
  of `deploy/tikv-multi-replica/`. On the full stack's bridge network with dynamic IPs
  and 24 containers, a partition is neither deterministic nor cleanly attributable.
  (D-server-level chaos — kill/pause mid-repair — is a *different* regime, driven
  against the ephemeral integration fixture, not this one.)
- **Blast radius.** A conformance run for the etcd adapter should not be able to fail
  because the D-server image is broken. Every extra container a test depends on is
  extra flake surface.
- **Contract vs. deployment.** The adapter conformance suites are *contract* tests
  (does this backend implement the trait?), not *deployment* tests; they want the
  smallest thing that is a real backend, on the simplest wiring (host networking).
- **Duplication.** Separate compose files repeat some service definitions, and the
  root stack's "distributed cluster on one machine" role now fully overlaps the
  small-multi-node profile.

This ADR extends the testing strategy of ADR-0009 and the pluggable-substrate stance
of ADR-0010 down to the concrete bring-up fixtures. It does not change what the
single-binary dev profile is for (ADR-0014).

## Decision

We will treat each `deploy/` (and integration) fixture as a named **testing profile**
and require every container-backed test to run against the **smallest profile that
exercises its subject**. A test MUST NOT be gated behind a heavier profile than it
needs. A profile MAY back several `xtask` drivers; each container-backed driver
targets exactly one profile (a driver with no container fixture at all — see
`disk-faults` below — is possible). The profiles, and every driver that runs against
each:

| Profile (fixture) | Topology | Driver(s) | Test class |
| --- | --- | --- | --- |
| **tikv-single-node** — `deploy/tikv-single-node/` | 1 PD + 1 TiKV, host net | `tikv-conformance`, `metadata-tier2` | `metadata-tikv` trait contract; single-owned-node metadata scenario (Tier-2) |
| **etcd-single-node** — `deploy/etcd-single-node/` | 1 etcd, host net | `etcd-conformance` | `coordination-etcd` trait contract |
| **integration** — `crates/chunkstore-grpc/tests/docker-compose.yml` | N D-server replicas, ephemeral ports | `integration`, `jepsen`, `kill-reconstruct` | component/gRPC integration; D-server chaos (`docker kill`/`pause` mid-repair) |
| **tikv-multi-replica** — `deploy/tikv-multi-replica/` | 1 PD + 3 TiKV (Raft), static IPs + `iptables` agent | `metadata-tier1` | metadata consistency under a symmetric leader partition (Tier-1) |
| **single-zone** — `deploy/small-multi-node/` | 3 etcd + 3 PD + 3 TiKV + 9 D servers + 3 custodians + 3 gateways | `deploy-small-multi-node` | deployment bring-up, day-one runbook (#367), end-to-end (#454 → #455) |

Not every driver has a container profile: **`disk-faults`** injects a disk-medium
fault against a real `dm-error` block device via `dmsetup`
(`crates/custodian/tests/tier1_disk_faults.rs`, root-required), so it sits **outside**
this table by design — its "fixture" is a kernel device-mapper target, not a container.

We will **remove the repo-root `./docker-compose.yml`.** Its role — stand up a
distributed cluster on one machine and drive it by hand with `wyrd put/get
--endpoints` — is subsumed by the **single-zone** profile, which is a strict superset
(9 D servers + real backends). The hand-driving instructions in the top-level README
now point at `deploy/small-multi-node/`.

We will add a new profile only when **no existing profile provides the required
topology or fault surface** — not for convenience or conceptual separation. The
driver→profile mapping above is the executable index; container profiles remain
outside `cargo xtask ci` (they need a runtime; Docker-available gating stays, per the
existing convention).

We will **not**, at this time, refactor the profiles into a shared base compose with
`profiles:` / override layering. The duplication across five small, stable files is
tolerable; a base+override consolidation is recorded here as an available future step,
not adopted now (it would trade duplication for indirection with no test-behaviour
change).

## Consequences

- **Faster, more attributable CI.** Contract suites boot a single-node backend (one
  etcd container; one PD + one TiKV) in ~seconds; a failure implicates one component.
  Fault tests keep the deterministic topology their injection requires. No fast test
  is held hostage to a 24-container bring-up.
- **One fewer bring-up artifact to maintain**, and no ambiguity about "the" way to
  stand a cluster up by hand — it is the single-zone profile. The two wyrd images now
  use distinct tags and never contend: the integration profile builds
  `wyrd-dserver:test` (default features), the single-zone profile builds the
  feature-tagged `wyrd-single-zone:local` (`tikv,etcd`). Removing the root compose
  retired the `wyrd-dserver:local` tag it alone built — no fixture references it now.
- **The mapping is a standing rule for new tests:** pick a profile from the table;
  justify (in review) any new profile against the "no existing profile fits" bar.
- **Accepted cost:** the five compose files still duplicate some service definitions.
  If that duplication grows painful, the deferred base+override refactor is the exit —
  reversible and behaviour-preserving.
- **Structural guard unchanged:** `xtask/tests/deploy_no_orchestrator_coupling.rs`
  continues to validate the single-zone profile's compose structurally and to enforce
  ADR-0010's no-orchestrator-coupling scan; removing the root compose does not touch
  either signal.
