---
created: 22.06.2026 18:55
type: adr
status: Proposed
tags:
  - adr
  - mtls
  - spiffe
  - trust
  - deployment
---
# 0025. Internal service-to-service trust

## Context

ADR-0005 already chose the *posture*: zones, D servers, custodians, and coordination
authenticate to each other with mTLS under a provider-operated CA (SPIFFE / SPIRE as internal
PKI), and section 8.5 elaborates — "identity is the certificate, and there are no shared service
secrets on the wire." What is *not* yet recorded is the enforcement contract that posture implies
once there is a network to enforce it on: which authority a given certificate grants, what happens
when mTLS is absent, and how the property is tested. The networked tier (M2) makes this concrete,
and the gap is already visible in code: `chunkstore-grpc` builds tonic with "default-features only
— no `tls`", so today an internal dial is plaintext. Promoting the wire to mTLS without deciding
*least authority per component* would authenticate the fabric while still letting any authenticated
component do anything — a spoofing fix that leaves elevation-of-privilege open.

## Decision

1. **mTLS is required, with no plaintext fallback.** Every internal RPC (client ↔ D server,
   custodian ↔ D server, any component ↔ coordination / metadata) is mutually authenticated under
   the provider CA. A dial that cannot complete the mTLS handshake **fails closed** — it is never
   silently downgraded to plaintext. The tonic `tls` feature is enabled and the handshake is
   mandatory, not opportunistic.

2. **The certificate is the identity, and authority is least-privilege per component.** A
   component's workload identity grants it only what its role needs: a **D server** is
   tenant-oblivious and authorized only to store and return fragments by chunk id — its identity
   grants it *nothing* on the metadata store, the namespace, or the KMS, so a compromised D server
   has no credential worth stealing (reinforcing the D-server blast-radius property of section 14.2);
   the **custodian** may drive GC / repair against D servers and read placement state, but is not a
   write authority on the namespace; the **gateway** authenticates external principals and is the
   only component that resolves ACLs. Authorization is checked against the presented identity at
   each callee, not assumed from network position.

3. **Consistent with the substrate and the closed-federation framing.** This runs on the pluggable
   deployment substrate (ADR-0010) and the single-provider posture (ADR-0005): the fabric is about
   isolation, fairness, and mutual authentication *within* one operator — not cross-operator
   distrust. SPIRE-issued short-lived certificates keep it rotation-friendly with no long-lived
   shared secrets, and coordination's own auth (etcd) stays defense-in-depth behind the fabric,
   never the primary boundary (section 8.5).

The property is verified under DST and the local-cluster tier (section 13): a plaintext or
wrong-identity dial is refused, and a component presenting a valid identity is still denied an
operation outside its role.

## Consequences

- A compromised or spoofed internal component cannot impersonate another, and lateral movement is
  bounded by least authority — authentication and authorization are decided together, so the wire
  fix does not leave privilege escalation open.
- The D-server blast radius (section 14.2) is reinforced at the network layer: even with a valid
  workload identity, a D server is authorized for nothing on the core.
- Cost: certificate provisioning and rotation (SPIRE or equivalent) become a deployment dependency,
  the `tls` feature and handshake config land in `chunkstore-grpc` and every networked component,
  and local/dev runs need a dev-CA path so the single-binary profile (ADR-0014) is not gated on a
  full PKI. The fail-closed rule means a misconfigured CA is an outage, not a silent plaintext
  downgrade — the intended trade.
- Refines ADR-0005 and builds on ADR-0010; closes the internal-trust enforcement gap noted in
  section 14.9.
