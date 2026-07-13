---
created: 13.06.2026 11:57
updated: 01.07.2026
type: architecture
status: living
tags:
  - architecture
  - roadmap
  - build-order
---
# 9. Build order and roadmap

> Living document. For an open-source project the build order *is* the contribution roadmap — it is how newcomers know where to start. The governing record of the ordering is [proposal 0013](../proposals/accepted/0013-implementation-arc-rescoped.md) (the implementation arc, rescoped — supersedes 0002); this section is its always-current summary and tracks build status.

The strategy (ADR-0009, section 4.2): **not bottom-up**. A vertical slice through the layers that matter for one operation, then widen by risk — **risk retired, not features delivered**. Trait boundaries exist from day one even where crate boundaries do not yet; traits are cheap to define and expensive to retrofit, crate splits are the reverse.

## The three steps

| Step | Goal | Ends with |
|------|------|-----------|
| **1. Prove the differentiator** | One atomic write and read, end to end, in one process, shown atomic under fault injection in simulation. | The central claim is no longer a claim. |
| **2. A real single-zone system** | Widen the proven slice, in risk order, into a self-hostable, erasure-coded, atomically-consistent — and then *secured, encrypted, and operable* — object store within one datacenter. | ★ the first genuinely useful product (M8). |
| **3. A multi-region sovereign substrate** | The cross-zone layers that turn independent single-datacenter systems into one geographically distributed foundation. | ★ the mission artifact (M11). |

## The milestones

| # | Milestone | Proves / retires | Status |
|---|-----------|------------------|--------|
| M0 | Walking skeleton | the commit protocol — the entire differentiator — end to end, atomic under DST fault injection | ✅ built |
| M1 | Erasure coding | real RS(k,m) in the hottest loop; reconstruction from any *k* | ✅ built |
| M2 | Networked D servers | the direct client→D-server data path; the `ChunkStore` seam is real | ✅ built |
| M3 | Custodians | the system maintains its own durability (GC, scrub, reconstruction, rebalance) and reports it | ✅ built (closing) |
| M4 | Production metadata backend | pluggability is real: redb→FoundationDB behind the unchanged trait is a composition change, not a refactor — proven twice, since the backend choice itself moved from TiKV to FDB (ADR-0042) without touching a consumer | FoundationDB is the production backend (ADR-0042, #442 "go"); TiKV retained as a stood-down fallback (#443) — proposals 0007, 0015 |
| M5 | Internal CA (step-ca) | the fabric authenticates itself; least authority on a SPIFFE-shaped identity | planned — proposal 0011 |
| M6 | Encryption at rest (KeyService/KMS) | envelope encryption behind the `KeyService` trait against a real KMS | planned — proposal 0012 |
| M7 | Failover & DR, single-datacenter | node / disk / rack loss survived and recovered, drilled rather than asserted | planned — proposal owed |
| M8 ★ | Manageability (CLI + portal) | the zone is *operable*: day-2 ops, tenant admin, the observability planes surfaced | planned — proposal 0008; **Step-2 release point** |
| M9 | Cross-zone replication (L3) | committed chunks replicate between zones with no half-copied replica ever visible | Step 3 |
| M10 | Global control plane (L2) | the global namespace and the home-zone consistency contract operate across zones | Step 3 |
| M11 ★ | Cross-zone failover & DR, drilled | the guarantees survive real zone loss | Step 3 — **release point, the mission artifact** |

M4 completes the single-zone **data plane** — feature-complete and pluggable, but a *soft stopping point*, not a deployable product: no internal trust fabric, no encryption at rest, no drilled local recovery, no operator surface. M5–M8 are what turn it into one; that is why the Step-2 ★ sits at **M8**, not M4. Step 3 is pursued in full only with a concrete operational owner committed to adopting it (proposal 0013, open question).

A single zone of this design is already the first genuinely useful product — a self-hostable, EC-efficient, atomically-consistent, **secured, encrypted, and operable** object store — which earns adoption and contributors long before the global federation exists.

## Where to start contributing

Every active milestone has a tracking issue and dependency-ordered per-slice issues on its GitHub milestone board; the per-milestone implementation plans are proposals 0001–0012. The DST harness (`testkit`) attaches at M0 and is extended at every milestone — it is not a one-time build but a growing dependency (ADR-0009), and extending it is always a welcome contribution. The most independent surface today: the M4 slices, the observability floor (proposal 0010), and the D-server performance program (proposal 0009).

## Deferred-with-reserved-seats

These are not built early, but their *hooks* must exist from the relevant milestone because they are expensive to retrofit:

- Append / CAS / watch storage primitives (ADR-0007) — the commit protocol and metadata schema accommodate them from M0.
- The version-fence for Option C consistency (ADR-0015) — the `meta:version` counter is reserved from M0.
- Encryption format hooks (ADR-0019/0021) — `flags`, `encryption_scheme`, and key-version are reserved in the on-disk format from M0; the `KeyService` lands at M6.
- Observability hooks (metric emission points, audit event stream, desired-state API) — emitted since the custodians' first commit (M3, ADR-0011); the floor that wires them into deployable binaries is proposal 0010; dashboards beyond M8's operable floor stay deferred (ADR-0013).
- openraft embedded coordination backend (ADR-0006) — etcd is the production backend; openraft reserved behind the same trait.
- SPIRE workload attestation (ADR-0036) — step-ca now; SPIRE reserved behind the `CertificateAuthority` seam for fleet scale.

**Attached programs, not milestones.** Three standing bodies of work deliberately attach to milestones rather than being ordered as milestones (the arc's rule: if it does not retire a load-bearing risk, it attaches): object lifecycle & retention (proposal 0006 — enforcement spine near M6–M7, tenant-facing API after M8), the D-server performance program (proposal 0009 — tiers attach from M4's deployment onward), and the observability floor (proposal 0010 — gates the M4 real-world campaign).
