---
created: 22.06.2026 18:45
type: architecture
status: living
tags:
  - architecture
  - security
  - threat-model
---
# 14. Threat model

A living, adversarial view of the storage system: what is worth attacking, where the
trust boundaries are, who the adversaries are, and which attacks a distributed file store
must withstand — each mapped to the decision that mitigates it. The trust framing follows
single-provider, closed federation (section 8.5, ADR-0005); this chapter is the model
itself. **STRIDE** classifies (section 14.4); the reader and abuse-case tests are the
verification rubric, run under the deterministic-simulation harness (ADR-0009, section 13).
It is **v0/evolving** and hardens as surfaces are built — a threat model is never "complete",
and most of Wyrd's attack surface (the network and the multi-tenant boundary) does not exist
in code until M2.

## 14.1 Assets

| Asset | Why it matters |
|-------|----------------|
| Bulk chunk data / fragments (D servers) | Confidentiality + integrity of the stored bytes; ciphertext-only when encrypted, integrity-checksummed (crc32c) so corruption is detectable without the key (ADR-0019, ADR-0021). |
| Tenant keys (KEKs / DEKs) | Decrypt everything under a tenant if stolen; held in an external KMS behind the `KeyService` trait, never persisted unwrapped, per-tenant separation bounds blast radius (ADR-0021). |
| Metadata / namespace (inodes, dirents, chunk maps, ACLs) | Tamper to lose / resurrect files or orphan chunks; the global namespace (L2) is linearizable, zonal metadata (L4) consensus-replicated (ADR-0020, ADR-0008). |
| The commit point (the `meta:version` counter) | The single linearization authority; tamper for torn state or a lost write (ADR-0015). |
| Coordination state — leases, fencing tokens (L5) | Leases gate GC reclamation; fencing tokens gate the single-active custodian; tamper for split-brain or premature deletion (ADR-0006, ADR-0007). |
| The audit / event log | Tamper to hide a deletion, placement, or admission; append-only, the provider's compliance (GDPR-deletion) proof (section 8.3). |
| Service identities (internal mTLS certs) | Impersonate a component on the internal fabric; the certificate *is* the identity (ADR-0005, ADR-0025). |
| External access credentials (OIDC tokens, S3 access keys) | Impersonate an external principal at the gateway (section 8.5). |

## 14.2 Trust boundaries

Boundaries from least- to most-trusted. The shape follows the dumb-storage / thick-client /
centralized-policy split (sections 3, 5, 8.5).

- **Internet** — external clients (SDK, S3, WebDAV callers), relying applications. Untrusted.
- **Access layer / gateway (L1)** — the authentication *and* authorization boundary: it
  resolves the caller, fetches the version-fenced ACL, and enforces quota / rate *before*
  issuing any storage operation. Nothing below L1 re-authenticates the external principal
  (section 8.5).
- **D servers (the dumb storage tier)** — the **least-trusted internal data tier**:
  tenant-oblivious, holding only opaque (ciphertext, when encryption is on) fragments and
  their checksums, addressed by chunk id. They may run on less-trusted storage hardware
  (ADR-0021).
- **Zonal metadata + coordination (L4 / L5)** — the commit authority and the lease/fencing
  store; network-isolated to the zone's control components, fronted by the mTLS fabric
  (section 8.5).
- **Global namespace / control plane (L2)** — the **most-trusted** tier: the linearizable
  namespace, placement, zone registry, and the authoritative ACLs (ADR-0020).
- **Custodian (L4)** — the single-active, fenced maintenance plane; it holds the GC *delete*
  authority, so it is a high-consequence component (ADR-0011).
- **KMS / `KeyService`** — the key-custody boundary; it holds KEKs and never sees plaintext
  data (ADR-0021).
- **Admin / management plane** — the operator surface (ADR-0013).
- **Tenant isolation** — a cross-cutting boundary: no cross-tenant read or traversal on any
  path (section 8.8, ADR-0022).

**D-server-compromise blast radius (a stated property).** An attacker who fully compromises
a D server gains **only ciphertext fragments and their checksums** — no keys (KMS-held), no
metadata or namespace, no ACLs. They **cannot** forge a commit (the commit point is the
metadata store's, fenced by version), and they **cannot** cause durability loss of a chunk
they do not hold (its *k+m* fragments are placed across distinct failure domains, so any *k*
survivors reconstruct it). They **can** deny service for the fragments they hold, or return
a *stale / corrupted* fragment — which is caught on read by the crc32c payload checksum and
by the scrubber, then reconstructed from survivors. This bounded blast radius is the security
payoff of the dumb-D-server / thick-client split (section 8.5, ADR-0019, ADR-0021) and is the
storage analog of an edge-cache compromise: a less-trusted tier holds no secret worth stealing.

## 14.3 Adversaries

Anonymous internet attacker · malicious or compromised external client (S3 / SDK / WebDAV) ·
malicious or compromised tenant administrator · a co-tenant (isolation breach) · network MITM
on the internal fabric · a compromised D server (less-trusted storage hardware) · a compromised
custodian or coordination node · malicious insider / operator · supply-chain attacker · a
non-Rust thick-client re-implementer that writes corrupt or non-atomic data (a *correctness*
adversary, section 8.6).

## 14.4 STRIDE, per boundary

| | D server (storage tier) | Gateway (L1) | Metadata · coordination · custodian (core) |
|---|---|---|---|
| **Spoofing** | mTLS service identity, no plaintext fallback (ADR-0025) | external principal auth: OIDC / S3 SigV4 / mTLS (section 8.5) | mTLS + fencing-token identity (ADR-0006) |
| **Tampering** | crc32c checksums + chunk-id binding; EC reconstruction (ADR-0019) | request authorized before any storage op | the commit point + append-only audit log (ADR-0015, section 8.3) |
| **Repudiation** | — | per-request audit at admission | the event stream is the audit truth (section 8.3) |
| **Information disclosure** | ciphertext-only at rest (ADR-0021) | ACL-gated reads, version-fenced | envelope encryption; tenant partition (ADR-0020, ADR-0022) |
| **Denial of service** | scrub + reconstruct around a lost/slow D server | **the write-path DoS target** → admission quota + rate (section 8.9) | per-tenant op-rate fairness; nothing data-proportional in L5 (ADR-0006) |
| **Elevation of privilege** | none (no secrets, tenant-oblivious) | stale-ACL defeat → linearizable, version-fenced ACLs (section 8.1) | premature GC / split-brain → reader-safe grace + fencing (custodian) |

## 14.5 The storage attack catalog

Each defended attack maps to the decision that owns it.

| Attack | Mitigation | Owned by |
|--------|-----------|----------|
| Malformed / malicious fragment parse (overrun, integer overflow, oversized payload-length) | header checksum verified *before* any field is acted on; every read bounds-checked; reserved flags and unknown code points rejected; `#![forbid(unsafe_code)]`; continuously fuzzed | ADR-0019, section 13 |
| Silent bit rot / fragment corruption | crc32c header + payload checksums verified on read; scrubber detects within scrub-cycle P; reconstruct from *k* survivors | ADR-0019, section 8.2 (Q2) |
| Tampered-fragment substitution by a compromised D server | payload checksum over the *stored* bytes + chunk-id binding; EC reconstruction from *k* honest survivors | ADR-0019 |
| Torn write / half-commit observable | one linearizable commit point; pending-chunk ledger makes a failed write's chunks collectable, never half-visible | ADR-0015 (Q3) |
| Premature GC of a still-referenced fragment (silent data loss) | reader-safe grace window; lease-expiry + orphan-ledger discipline; the "never reclaim a referenced fragment" invariant asserted under DST | custodian, ADR-0011 (Q3) |
| Clock skew extends a lease or breaks GC timing | bounded skew tolerance + authenticated time source + implausible-clock fail-closed / drain | ADR-0024 |
| Split-brain custodian / two active maintainers | leadership fencing token; single-active reconciliation; a stale term is fenced | ADR-0006, ADR-0007 |
| Read against a stale / just-revoked ACL | ACLs live in the globally linearizable namespace (L2) and are version-fenced at the gateway before the op | section 8.1, 8.5; ADR-0015 |
| Cross-tenant read / traversal | four boundaries — namespace partition + envelope crypto + quota + rate (defense-in-depth) | ADR-0022, ADR-0020, ADR-0021 |
| Stolen disk / compromised D server reads data | ciphertext-only at rest; per-tenant envelope encryption; key never leaves the KMS | ADR-0021 |
| Tenant-key (KEK) compromise | per-tenant key separation bounds blast radius; rotation is re-wrap; crypto-erase; KMS custody behind `KeyService` | ADR-0021 |
| Internal service spoofing / MITM on the fabric | mTLS under the provider CA; certificate-as-identity; least authority per component; **no plaintext fallback** | ADR-0005, ADR-0025 |
| External-principal impersonation | OIDC / OAuth2 bearer, S3 SigV4, OIDC + mTLS for management — all at the gateway | section 8.5 |
| Write-path DoS / resource exhaustion | admission control: quota + rate + failure-domain room, all fail-closed | section 8.9, ADR-0022 |
| Treating coordination (etcd, L5) as a database | architectural rule: nothing data-proportional in L5; sized in kilobytes | section 11 (risks), ADR-0006 |
| Supply-chain compromise | permissive-only dependency criteria; `cargo-deny` advisory + license wall; `cargo-machete`; DCO | ADR-0008 |
| Multi-language client divergence (corrupt / non-atomic data) | spec-first on-disk format + conformance vectors; a Rust core with FFI is the reserved alternative to reimplementation | ADR-0002, section 8.6, 8.7 |
| Backup that cannot restore (recursive dependency) | backups must not depend on the system they back up; the recursion bottoms out in independent storage | section 8.2 |

## 14.6 Abuse-case tests

The catalog above is made *mechanical*: each defended attack is asserted **to fail**,
seed-reproducible, from the milestone where its surface first exists — the security analog of
the conformance vectors (section 8.6). Representative cases:

- a fragment with a corrupted header checksum is rejected, before any header field is trusted (M0);
- a fragment with a flipped payload byte is rejected, and the chunk still reconstructs from *k* survivors (M1);
- a write interrupted at every step leaves either the old version or the new, never a hybrid, and its orphans are collected (M0, Q3);
- a referenced fragment is never reclaimed while a reader holds the old version (M3, Q3);
- a lease expired past the skew budget does not renew (M3, ADR-0024);
- a fragment offered under the wrong chunk id is rejected (M2);
- a plaintext (non-mTLS) internal dial is refused (M2, ADR-0025).

A defended attack that ever succeeds becomes a permanent regression. The reader's parse path
(`chunk_format::decode`) should additionally be fuzzed continuously (a planned addition, section
14.9), since it is the one component that consumes fully untrusted bytes — a fixed conformance
corpus cannot explore the input space the way coverage-guided fuzzing does.

## 14.7 Threat → test map

The catalog mapped to where each threat is verified. Tiers are the realism-and-cost ladder of
section 13; "DST" is Tier 0, the correctness authority.

| Threat | Test | Tier | Milestone |
|--------|------|------|-----------|
| Malformed-fragment parse | `chunk-format` reader unit + conformance vectors (fuzzing planned, §14.9) | 0 | M0 → continuous |
| Bit rot / corruption detection | checksum-verify + scrub property tests | 0–1 | M1 / M3 |
| Torn write / atomicity | commit-protocol property tests under fault injection (`wyrd-dst`) | 0 | M0 |
| Premature GC | custodian GC property test: referenced fragment never reclaimed | 0 | M3 |
| Clock skew / lease expiry | manual-clock skew injection (DST); `libfaketime` / NTP manipulation (Tier 1) | 0–1 | M3 |
| Split-brain custodian | fencing-token property test | 0 | M3 |
| Cross-tenant isolation | namespace + crypto + quota + rate isolation tests | 0–1 | M2 → M4 |
| Internal service spoofing | mTLS-required dial test; plaintext refused | 1 | M2 |
| Consistency under partition | Jepsen against a local cluster (ADR-0015) | 1 | M2 → |
| Supply-chain | `cargo-deny` advisory + license wall in CI | 0 | M0 |
| Static-analysis findings | CodeQL (Rust), non-gating (planned, §14.9) | 0 | M0 → |

## 14.8 Residual and accepted risks

- **Replication-lag staleness window** — stale-tolerant reads may lag the latest by up to the
  replication lag; the deliberate cost of edge-local reads (section 8.1).
- **Bounded repair / scrub latency** — corruption is detected within the scrub-cycle period P,
  not instantaneously (Q2).
- **D-server-held ciphertext** — a compromised D server holds ciphertext; confidentiality rests
  on the KEK staying in the KMS, bounded by per-tenant key separation (ADR-0021).
- **Admission fail-closed availability cost** — under pressure a write is refused, never
  half-done; correctness outranks admission (section 8.9).
- **Single-provider operator trust** — operators are trusted within the provider (no
  cross-operator verify-everything, ADR-0005); insider risk is bounded by the append-only
  audit log and envelope encryption (the operator cannot read a tenant's directory).

## 14.9 Gaps and their disposition

The threat model records its own holes and tracks them to closure.

**Decided in (Proposed) ADRs** (they refine the architecture):

- **Clock / time-source trust** — bounded skew + authenticated time + implausible-clock
  fail-closed (ADR-0024).
- **Internal service-to-service trust** — the M2 enforcement contract for the mTLS fabric
  already chosen in ADR-0005: least authority per component, certificate-as-identity, no
  plaintext fallback (ADR-0025).

**Still genuinely open**:

- **Key-compromise emergency response** — a runbook for wholesale KEK revocation + forced
  re-encryption (distinct from planned re-wrap rotation, ADR-0021) is not yet written.
- **Build / release integrity** — signed releases + SLSA provenance + pinned Actions are not
  yet decided.
- **The proto / network input-validation surface** — fuzzing and hardening of the gRPC decode
  path land with the networked tier (M2); only the on-disk reader is hardened today.
- **CI security workflows** — a CodeQL (Rust) static-analysis workflow and a coverage-guided
  fuzz target on `chunk_format::decode` are *planned* additions, alongside the existing
  `cargo-deny`, Dependabot, and `cargo-mutants` jobs; OSS-Fuzz / Scorecard enrolment is reserved.
- **Privacy threat modeling (LINDDUN)** — a deeper privacy pass over metadata / PII is reserved,
  not built into this STRIDE pass.
