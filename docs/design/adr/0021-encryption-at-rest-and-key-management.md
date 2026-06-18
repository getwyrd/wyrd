---
created: 18.06.2026 23:05
type: adr
status: Proposed
tags:
  - adr
  - security
  - encryption
  - key-management
---
# 0021. Encryption at rest and key management

## Context

Section 8.5 named "per-tenant envelope encryption" as an optional feature but never decided the mechanism — where keys live, the key hierarchy, how rotation works, and what "delete" means cryptographically. A multi-tenant provider substrate needs a concrete answer: the gap between *"we encrypt"* and an auditable key lifecycle is exactly where compliance stories fail.

Constraints fix much of the shape. Single-provider, closed federation (ADR-0005) means keys are **provider-held** — there is no untrusted-operator or cross-org threat model to escrow against. D servers are deliberately dumb and may run on less-trusted storage hardware, so keeping them **ciphertext-only** is what relaxes that trust. The on-disk format is spec-first and versioned (ADR-0002 / ADR-0019), so any encryption marker is a format concern, not an afterthought. And the delete fast-path already leans on crypto-erase (section 6.7), so a key lifecycle that makes that real is owed.

## Decision

1. **Envelope encryption, two-level hierarchy.** Bulk data is encrypted with per-object (or per-chunk) **data-encryption keys (DEKs)**; each DEK is wrapped by a per-tenant **key-encryption key (KEK)**; KEKs live in an external **KMS** behind a narrow `KeyService` trait (provider KMS / Vault / cloud KMS in production; a file-backed stub for dev). The system never persists an unwrapped KEK, and the KMS never sees plaintext data.

2. **Crypto stays in the client library; storage stays dumb.** The client encrypts on the write path (before erasure coding) and decrypts after reconstruction — encryption is a thick-client concern, like EC (section 8.6). D servers and the metadata store hold only ciphertext, the wrapped DEK, and a key-version id; the wrapped DEK travels in the chunk / metadata records, marked in the on-disk format (ADR-0019).

3. **Rotation is re-wrap, not re-encrypt.** Rotating a tenant KEK re-wraps its DEKs — a cheap, metadata-only operation; the bulk ciphertext is never touched. True DEK rotation (re-encryption) is a background custodian job, reserved for the rare cases that require it.

4. **Crypto-erase is a first-class delete mode.** Destroying a tenant or object KEK renders all ciphertext under it permanently unrecoverable, independent of GC — the section 6.7 delete fast-path and a defensible right-to-erasure story (section 8.3).

5. **Optional, per-tenant, off by default in dev.** Encryption is a per-tenant policy, like replication factor; the single-binary / dev profile defaults to none. The AEAD cipher (e.g. AES-256-GCM or XChaCha20-Poly1305) and whether the unit is per-object or per-chunk are **[OPEN]**, settled with the format work.

## Consequences

- Storage-hardware trust is relaxed: a stolen disk or a compromised D server yields only ciphertext.
- Delete gains a cryptographic guarantee that does not wait on GC — auditable erasure for compliance.
- The `KeyService` seam keeps the KMS pluggable, consistent with the narrow-trait dependency rule (ADR-0010).
- Cost: the client library and the on-disk format carry encryption metadata from the start — a reserved seat even when encryption is off — and key-management operational burden (rotation, KMS availability) lands on the operator.
- The KMS sits on the read/write path for encrypted tenants, so its latency and availability matter; bounded-lifetime **DEK caching** mitigates, and KMS unavailability must **fail closed** without compromising the durability of the (still-encrypted) data. Caching policy is **[OPEN]**.
- **[OPEN]** key-hierarchy depth for very large tenants (whether a per-bucket intermediate key is warranted) and HSM-backing for KEKs.
