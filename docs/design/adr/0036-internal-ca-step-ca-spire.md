---
created: 26.06.2026
type: adr
status: Proposed
tags:
  - adr
  - security
  - pki
  - mtls
  - identity
  - trust
  - deployment
---
# 0036. Internal CA and identity fabric: step-ca now, SPIRE reserved

## Context

The closed, single-provider federation (ADR-0005) authenticates zones and
services to one another with **mTLS under a provider-operated internal PKI** —
not a public web-PKI, not a cross-organization trust system. The architecture
(section 8.5) named **SPIFFE/SPIRE** as the candidate for that PKI. ADR-0025 then
fixed the *enforcement* contract on that posture — mTLS required with no plaintext
fallback, the certificate as identity, and least authority per component — while
leaving open *which* CA issues those certificates. This ADR answers that open
question: *which* technology to run, and *when*.

The requirements the fabric CA must meet:

- **Service-to-service mTLS**, internal only. (The S3 gateway's *public* TLS
  endpoint is a separate, smaller concern — public ACME/Let's-Encrypt territory —
  and is explicitly **out of scope** of this ADR.)
- **Short-lived, automatically-rotated certificates**, because a version-skewed
  fleet during rolling upgrades is the normal state; long-lived,
  manually-managed certs are an operational and security liability.
- **Workload identity, not host identity.** Under the one-D-server-per-disk model
  (ADR-0034), many D servers share a host, so host-based identity is too coarse;
  identity must name *the process and its role*.
- **Sovereignty and licensing**, consistent with the dependency doctrine:
  permissive/clearly-licensed, foundation-governed or self-hostable, no hard
  dependency on a US-controlled hosted service.

Candidates evaluated:

- **SPIFFE/SPIRE** (CNCF, Apache-2.0) — the purpose-built standard for
  secret-less *workload identity* via attestation. The architecturally complete
  answer, and the section 8.5 candidate. Cost: substantial operational weight — a
  stateful SPIRE Server (a new root of trust needing its own datastore/HA) plus a
  SPIRE Agent on every node, and non-trivial node- and workload-attestation
  configuration.
- **step-ca / Smallstep** (Apache-2.0) — a self-hosted online CA purpose-built
  for "internal PKI with short-lived, auto-rotated certs" (ACME and other
  provisioners). Far lighter to operate (a single CA service); does **not**
  provide SPIFFE's secret-less workload attestation, but issues exactly the
  short-lived auto-renewed certs the fabric needs.
- **Vault PKI** — capable, but **rejected on licensing grounds**: Vault
  moved to the BSL (source-available, non-OSI) in 2023, the same disqualifier
  applied to CockroachDB in the dependency doctrine. (OpenBao — the MPL-licensed
  Linux Foundation fork — would be the sovereignty-clean way to get Vault's model
  if ever wanted; noted, not adopted.)
- **Roll-your-own CA** — rejected by the reinvent-vs-consume doctrine: cert
  lifecycle, rotation, revocation, and attestation are exactly the subtle,
  well-solved security primitives to *consume*, not rebuild. The novelty budget
  is spent on the commit protocol.

Two facts tilt the *timing*: (1) at the project's current stage (single-zone,
approaching M4, first real deployments, an open observability gap), SPIRE's
benefit — secret-less attestation — mostly materializes at fleet scale, while its
operational and attestation-debugging cost lands now, competing for attention
with proving the differentiator; and (2) the trust model is deliberately
*higher-trust* than SPIRE's zero-trust target — ADR-0005 has zones self-report
health and be trusted, with no proof-of-storage — so "do we even need full
attestation yet" is a genuine open question best answered by operating something
simpler first.

## Decision

Adopt **step-ca as the internal CA now.** It issues the short-lived,
auto-rotated mTLS certificates for the service fabric, self-hosted and
sovereignty-clean.

**SPIRE is reserved as the future upgrade**, not built — the architecturally
complete workload-identity destination, to be adopted when fleet scale makes
secret-less attestation worth its operational weight. The dev/single-binary
profile uses a trivial self-signed / in-memory CA behind the same seam (mirroring
in-memory coordination in dev, etcd in prod).

This is the same "reserve the heavier option behind the seam" pattern used for
openraft (ADR-0006) and Model B (ADR-0034). The reserved seat is kept open by
**two load-bearing requirements the implementation MUST honor from now on:**

> **1. The CA / identity provider MUST sit behind a narrow seam** (a
> `CertificateAuthority` / `IdentityProvider` trait), so step-ca, SPIRE, and the
> dev self-signed CA are composition choices wired only in `server` — never a
> dependency the components couple to directly (ADR-0010).
>
> **2. Peer identity MUST be a first-class abstraction, distinct from raw
> certificate fields.** Components MUST authenticate and authorize peers against
> an abstract identity (role + zone + disk/instance), *derived* from the
> certificate by an adapter — never by matching raw DNS names, SANs, or
> SPIFFE-ID strings directly in authorization logic.

Requirement 2 is the one that decides whether the later SPIRE switch is a *cheap
composition change* or a *painful rewrite* — exactly as ADR-0034's first-class
failure-domain label decides whether Model B is an extension or a refactor. If
authorization logic matches raw cert fields, SPIRE means touching every auth
check; if it matches an abstract `PeerIdentity` behind an adapter, the switch is
"step-ca-cert → PeerIdentity" and "SPIFFE-ID → PeerIdentity" as two adapters,
with the consuming logic unchanged.

## Architecture and implementation requirements

To follow this decision and keep the SPIRE upgrade cheap, the implementation MUST:

1. **Put the CA behind a `CertificateAuthority` / `IdentityProvider` trait.**
   Components ask the seam for their identity; they do not call step-ca (or, later,
   the SPIRE Workload API) directly. Only `server` wires the concrete backend.
   Provide a trivial self-signed backend for the dev profile.

2. **Model peer identity as a first-class type** (e.g. `PeerIdentity { role,
   zone, instance }`), produced by an adapter from the presented certificate.
   **No authorization logic may inspect raw cert fields.** A test MUST assert that
   peer authorization is keyed on the abstract identity, not on cert internals —
   the standing guard that the reserved seat has not silently closed (mirroring
   the ADR-0034 placement-keyed-on-label test).

3. **Shape the abstract identity like a SPIFFE ID from day one.** Issue step-ca
   certificates carrying SPIFFE-ID-shaped SANs
   (`spiffe://<trust-domain>/<zone>/<role>/<instance>`) and derive `PeerIdentity`
   from that structure. This collapses the *identity-format* change to near-zero
   at SPIRE-switch time: only the *issuer and acquisition path* change, not the
   identity shape the rest of the system reasons about.

4. **Make identity acquisition pluggable at startup and rotation-aware.**
   A component obtains its identity from the seam (a CA call under step-ca; the
   local Workload API under SPIRE) and MUST consume it from a source that can
   deliver a **rotated** certificate at runtime — never read a cert once and cache
   it for the process lifetime. This serves step-ca renewal *and* SPIRE's
   streamed rotation, and is required regardless of backend.

5. **Keep the fabric CA decision separate from external gateway TLS.** The
   internal mTLS fabric (this ADR) and the S3 gateway's public endpoint are
   different trust contexts; the public endpoint uses public ACME / the provider's
   public-cert process and is not governed here.

6. **Document the reserved SPIRE upgrade** in the deployment view (section 7) and
   section 8.5, with its trigger (fleet scale at which secret-less workload
   attestation earns its operational weight) and its irreducible cost (new SPIRE
   Server + per-node Agent + node/workload attestation config — a deploy-time
   migration no abstraction removes), so a future implementer knows it was a
   deliberate deferral with a defined upgrade path.

## Consequences

- The fabric gets **self-hosted, sovereign, short-lived auto-rotated mTLS** now,
  at low operational weight, matched to the project's current stage and to the
  trusted-closed-federation model (ADR-0005) — without paying SPIRE's attestation
  tax before its benefit materializes.
- **The SPIRE upgrade stays cheap on the code side** *provided* requirements 2–3
  are honored: the per-component change is identity *acquisition*, and the
  *authorization* logic is unchanged because it consumes an abstract,
  SPIFFE-shaped `PeerIdentity`. If that abstraction is ever collapsed into raw
  cert matching, this consequence is lost and SPIRE becomes a pervasive rewrite —
  so the abstraction is the thing reviewers must protect.
- The SPIRE upgrade's **operational cost is irreducible**: standing up the SPIRE
  Server and per-node Agents and configuring attestation is real deploy-time work
  no seam removes. This is accepted — it is a bounded, deferred, deploy-time cost,
  not a pervasive code change.
- Vault PKI is **closed off by the same licensing doctrine** that closed off
  CockroachDB — a consistency the dependency-governance posture already implies.
- A latent risk to watch (same shape as ADR-0034): because step-ca identity is
  1:1-ish with a service today, it is tempting to authorize peers by a raw cert
  field. That collapse silently closes the SPIRE door. The first-class
  `PeerIdentity` and its guard test are what keep it open.
- Adopting step-ca adds one stateful service (the CA) to production deployments;
  modest, and far lighter than SPIRE's two-component topology.

## Revisit when

Fleet scale, multi-zone operation, or a hardening of the trust model (e.g. a move
away from trusted self-reporting toward zero-trust-between-workloads) makes
secret-less workload attestation worth SPIRE's operational weight. Until then,
step-ca behind the seam stands, and the SPIRE-shaped identity abstraction keeps
the upgrade a composition change rather than a rewrite.
