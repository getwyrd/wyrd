---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - federation
  - scope
---
# 0005. Single-provider, closed federation

## Context

Regions and nodes cooperate within one provider; there is no cross-provider or untrusted-operator operation. A trust model spectrum exists from "cooperating operators" (mutual TLS, contracts) to "verify everything" (proof-of-storage challenges, as in decentralized storage networks).

## Decision

Adopt a **closed, single-provider federation** trust model. Zones authenticate with mTLS under a provider-operated CA (SPIFFE/SPIRE as internal PKI). Zone health is self-reported and trusted. No proof-of-storage protocols. Per-tenant envelope encryption is an optional feature, not a structural necessity.

## Consequences

- A large amount of protocol surface is removed: no zone-challenge scrubbing across distrusting parties, no proof-of-storage, simpler admission.
- The mTLS fabric is an internal PKI, not a cross-organization trust system.
- This is a conscious exclusion, and someone will later propose cross-provider federation. The cost to reverse: wire protocols would need spec-first promotion (ADR-0002), the admission and scrubbing designs would need verify-everything variants, and envelope encryption would likely become mandatory. Recorded here so the exclusion is explicit and the re-entry cost is known.
