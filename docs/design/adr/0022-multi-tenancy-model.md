---
created: 18.06.2026 23:05
type: adr
status: Proposed
tags:
  - adr
  - multi-tenancy
  - isolation
  - policy
---
# 0022. Multi-tenancy model

## Context

The primary deployment is a cloud provider serving many tenants (section 1.4), so a tenant is a first-class concept — yet the architecture only had scattered per-tenant *hooks* (replication factor, sync-replication opt-in, envelope encryption, quotas) and no model: what a tenant *is*, how tenants are isolated, and where isolation is enforced. For a provider substrate that is load-bearing, not a detail.

Much of the shape is already fixed by prior decisions. Single-provider, closed federation (ADR-0005) means all tenants belong to one operator, so the threat model is **isolation and fairness, not mutual distrust between operators**. The L2 global namespace (ADR-0020) already partitions the directory tree; per-tenant envelope encryption (ADR-0021) already gives a cryptographic data boundary; and the access layer (L1) is already the authentication/authorization enforcement point (section 8.5).

## Decision

1. **A tenant is the unit of isolation, policy, and billing** — a namespace partition plus an identity domain. Each tenant owns a subtree of the global namespace, a set of credentials (S3 access keys / an OIDC realm), and a policy bundle (residency, replication factor, encryption, quotas, rate limits).

2. **Multi-tenancy is logical, not physical.** Tenants share zones, D servers, and the metadata / namespace tiers; isolation is the composition of four boundaries, not dedicated hardware (single-provider, ADR-0005). A dedicated-zone or dedicated-hardware tier is a possible future option, not v1.

3. **Four isolation boundaries, each at a definite enforcement point:**
   - **Namespace** — the L2 global namespace is partitioned per tenant; cross-tenant naming or traversal is impossible by construction (ADR-0020).
   - **Data** — per-tenant envelope encryption makes the boundary cryptographic; D servers hold opaque, mixed-tenant ciphertext (ADR-0021).
   - **Capacity** — per-tenant quotas (bytes, object count) checked at **admission** in the L1 gateway against the tenant's L2 record; a hard limit rejects, a soft limit warns.
   - **Performance** — per-tenant request-rate limits (token bucket) at L1, and placement spreads a tenant across failure domains so one tenant cannot monopolise a domain (noisy-neighbour control).

4. **Enforcement lives at L1 and L2, never below.** The gateway authenticates the tenant, authorizes against L2 ACLs, and checks quota/rate before issuing any storage operation; D servers and the metadata store stay **tenant-oblivious**, trusting an admitted request. The storage tier stays dumb; the policy tier stays centralized.

## Consequences

- Tenancy is now a named model with concrete enforcement points, not scattered hooks.
- The boundary is defense-in-depth — namespace + crypto + quota + rate — so a single bug does not cross tenants (the cryptographic boundary still holds even if namespace isolation is breached).
- Sharing infrastructure keeps utilization high (the provider economics, goal 2) at the cost of needing real fairness controls, which become first-class admission/backpressure concerns (section 8.9).
- Per-tenant policy is data in L2, read on the admission path, so it is cached and version-fenced like ACLs (section 8.5); a stale quota cannot admit past a hard limit because the hard check re-reads on conflict.
- **[OPEN]** billing/metering event stream (likely the audit/event log, section 8.3), and whether very large tenants warrant a dedicated namespace shard or zone.
- **[OPEN]** cross-tenant sharing (a dirent pointing at another tenant's object) — allowed, forbidden, or capability-gated.
