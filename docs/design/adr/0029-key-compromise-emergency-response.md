---
created: 22.06.2026 20:15
type: adr
status: Proposed
tags:
  - adr
  - security
  - key-management
  - incident-response
  - encryption
---
# 0029. Key-compromise emergency response

## Context

ADR-0021 and ADR-0026 cover the *normal* key lifecycle: rotation is re-wrap (cheap, metadata-only —
the bulk ciphertext is never touched), and deletion is crypto-erase. Neither covers a *compromised*
key. The threat model (section 14.5) lists "Tenant-key (KEK) compromise → per-tenant separation
bounds blast radius; rotation is re-wrap", but that mitigation is incomplete: re-wrap rotation gives
the DEKs a new wrapping but **does not re-encrypt the bulk data**, so an attacker who captured the
old KEK — or any DEK it unwrapped — can still decrypt every existing chunk. A real compromise
therefore needs a path that *planned rotation deliberately avoids*: actual re-encryption of the
affected data under fresh keys. Section 14.9 flagged this as a genuine open item; the identity
sibling project records the analogous decision for signing keys (wholesale revocation plus forced
re-issue). Wyrd has no equivalent runbook for "a tenant key has leaked".

## Decision

1. **Revocation is a distinct `KeyService` operation, separate from rotate and destroy.** A
   compromised key version is marked **revoked** in the KMS and MUST NOT be used to unwrap
   thereafter. New writes immediately use a fresh KEK; this is not the graceful, overlap-based
   rotation of ADR-0021 §3 but an abrupt cut-over.

2. **Forced re-encryption, not just re-wrap.** Because the compromised key (or a DEK it exposed) can
   still decrypt existing ciphertext, the affected data is **re-encrypted under fresh DEKs** by a
   priority background custodian job — the same machinery as repair/reconstruction (section 5),
   prioritised like a durability deficit. Re-wrap alone is explicitly insufficient here.

3. **Blast radius stays bounded by per-tenant separation (ADR-0021).** A compromise is scoped to one
   tenant's key; other tenants are unaffected. Revocation marks the key version compromised across
   the tenant; re-encryption proceeds per object until no ciphertext remains under the revoked key.

4. **Interaction with holds (ADR-0028).** Re-encrypting held data is permitted — it preserves the
   data and changes only the key, so it does not violate a hold. But the revoked key MUST be
   **retained, marked unusable, and not destroyed** until re-encryption of all data under it is
   complete *and* no hold pins it; revoked-not-destroyed is a distinct key state.

5. **The runbook is part of the decision.** Detect (audit anomaly or disclosure) → revoke the key
   version in the KMS → cut new writes over to a fresh KEK → schedule priority re-encryption →
   verify no ciphertext remains under the revoked key → destroy it → and, per the compounding loop
   (section 13.1), add a seed-reproducible DST regression and update the threat model. Re-encryption
   backlog and time-to-re-encrypt are emitted on the durability plane (ADR-0011) as first-class
   signals.

Revocation and re-encryption timestamps are taken from trusted time (ADR-0024).

## Consequences

- A real key compromise has a defined, bounded, testable response that is correctly *different* from
  planned rotation — re-encryption closes the exposure that re-wrap leaves open.
- The `KeyService` trait (ADR-0026) gains a **revoke** operation and a **revoked-not-destroyed** key
  state, alongside rotate and destroy.
- Cost: forced re-encryption is heavy — it reads and rewrites bulk data, unlike the metadata-only
  re-wrap — and is accepted as the unavoidable price of an actual compromise; per-tenant key
  separation is what keeps that cost bounded to the affected tenant.
- The held-data interaction (point 4) composes with ADR-0028: a hold blocks *destruction* but not
  *re-encryption*, so a compromise of held data is still recoverable without violating WORM.
- Refines ADR-0021 and ADR-0026; closes the section 14.9 key-compromise open item; depends on
  ADR-0024 (timestamps) and relates to ADR-0028 (held-data handling).
