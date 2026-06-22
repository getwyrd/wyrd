---
created: 22.06.2026 19:10
type: adr
status: Proposed
tags:
  - adr
  - security
  - encryption
  - key-management
  - kms
  - sovereignty
---
# 0026. Key management: the KeyService contract and KMS backend selection

## Context

ADR-0021 chose envelope encryption — per-object/per-chunk DEKs wrapped by a per-tenant KEK,
the KEK held in "an external KMS behind a narrow `KeyService` trait" — but it named the KMS
only in passing ("provider KMS / Vault / cloud KMS … a file-backed stub for dev") and left the
trait surface, the backend, DEK caching, and HSM-backing `[OPEN]`. The KMS is therefore the one
major dependency that never received the selection rigor every other backend got: coordination
(ADR-0006), the metadata store (ADR-0008), and the namespace store (ADR-0020) each have an ADR
that picks a default, names alternatives behind a trait, and passes the dependency lens. The KMS
has none. It is also load-bearing and risky: on the read/write path for encrypted tenants, a
single new failure domain (section 11: "a KMS outage or a lost KEK is catastrophic").

The dependency-selection lens (ADR-0003) applies, with one distinction that decides the outcome.
The cargo license wall in `deny.toml` (permissive crates only; AGPL / BSL / SSPL denied) governs
**Rust crates linked into the binary**. A KMS is a separate deployed **service**, like etcd and
TiKV — so it is judged by ADR-0003's broader **control-resilience** test (forkable, self-hostable,
no forced phone-home, foundation-governed where possible, data residency under the operator's
control), not the crate allowlist. That broader test is what makes a weak-copyleft *service*
acceptable even though its licence is not on the crate allowlist.

The lens immediately disqualifies the product ADR-0021 named first. **Vault relicensed
to BUSL-1.1 (BSL) in 2023** — a licence `deny.toml` explicitly denies and ADR-0003 was written to
catch. Vault is the KMS-shaped repeat of the CockroachDB case: caught at selection time, not after
adoption. This ADR records the contract and the replacement.

The requirements the contract must meet, drawn from ADR-0021 and the decisions around it: the
envelope operations; per-tenant KEK isolation; **crypto-erase** by KEK destruction (ADR-0021 §4),
which must not fire while a retention / legal hold is active (the WORM precedence, reserved for a
later ADR); **residency-pinned KEK custody** (the crypto-residency answer to metadata sovereignty,
ADR-0018, section 8.8); a trusted clock for any time-bound key operation (ADR-0024); low latency,
high availability, and fail-closed behaviour because the KMS sits inline.

## Decision

1. **The `KeyService` trait is the architectural commitment; no single product is.** It joins
   `ChunkStore` / `MetadataStore` / `NamespaceStore` / `Coordination` in the `traits` crate under
   the narrow-trait dependency rule (ADR-0010). Its surface is normative:
   - **generate-data-key** — return a fresh DEK both as plaintext (for the client to encrypt with,
     immediately) and wrapped under a named per-tenant KEK; the client stores only the wrapped DEK
     and the key-version id.
   - **unwrap** a wrapped DEK (with its key-version) → the plaintext DEK, for the read path.
   - **rotate-KEK** — re-wrap existing wrapped DEKs under the new KEK version without touching bulk
     ciphertext (the cheap, metadata-only rotation of ADR-0021 §3).
   - **destroy-KEK** — crypto-erase (ADR-0021 §4); it MUST be refused while any object under that
     KEK is under a retention / legal hold.
   - per-tenant KEK namespacing and monotonic key-version ids.

   Wrap, unwrap, and rotate happen **inside the KMS** (the "encryption-as-a-service" shape): the
   trait MUST NOT expose KEK material to Wyrd, Wyrd MUST NOT persist an unwrapped KEK, and the KMS
   never sees plaintext bulk data.

2. **Default backend: OpenBao, via its Transit engine.** OpenBao is MPL-2.0 and Linux-Foundation-
   governed — the community fork of Vault taken from its last open release, i.e. the un-rug-pullable
   answer to the Vault relicensing. It is the **default** for three reasons, in priority order:
   - *Operational weight.* It is a single Go binary with pluggable storage, which fits Wyrd's
     single-binary-to-fleet gradient (ADR-0014) and keeps the file-stub → real-KMS path short.
   - *Purpose-built contract.* Its Transit secrets engine **is** the item-1 contract almost
     one-to-one — a `datakey` endpoint, `encrypt` / `decrypt` / `rewrap`, key versioning and
     rotation, an audit log — so the envelope operations are native, not emulated, and KEK material
     never leaves the service.
   - *Governance.* Foundation-governed and self-hostable, so the key-custody story stays sovereign
     and un-rug-pullable (ADR-0003).

   **Vault itself is rejected** on its BUSL-1.1 licence (ADR-0003; `deny.toml`'s BSL denial).

3. **Alternative backend: OpenStack Barbican**, behind the same trait — Apache-2.0, OpenInfra-
   governed, with pluggable PKCS#11 / HSM and KMIP backends. Both backends pass the ADR-0003 lens
   (Barbican's Apache-2.0 is marginally cleaner than MPL-2.0, but licence does not separate two
   foundation-governed *services*), so the default is decided on weight and fit, not licence.
   Barbican is the **alternative, not the default**, because it is a heavier subsystem — a Python
   service needing a SQL database, a queue, and (by default) Keystone for identity — built to live
   inside an OpenStack deployment. That weight is exactly why it **wins in one case**: a provider
   already running OpenStack, where Keystone and the KMIP / PKCS#11 plugins are already present and
   Barbican is the native key manager.

4. **Exchangeability is engineered, not hoped — standardize the seam on PKCS#11 and KMIP, not a
   vendor.** The measures that keep the backend swappable are deliberate:
   - *Common-denominator trait.* The `KeyService` surface (item 1) is defined to what any conforming
     KMS provides — the envelope operations — and **not** to any OpenBao-Transit-specific API, so a
     Barbican or raw-HSM adapter is never forced to emulate Transit.
   - *Standard custody interface.* KEK custody SHOULD be HSM-backable through **PKCS#11** (the HSM
     interface) and **KMIP** (the OASIS interop protocol) rather than a product API, so a hardware
     HSM, a cloud HSM, SoftHSM2 (dev), OpenBao, or Barbican all satisfy the same seam — mirroring
     `ChunkStore` targeting the S3 API: the durable seam is a standard, not a vendor.
   - *Single wiring point.* Only the `server` crate knows the concrete backend (ADR-0010), so
     OpenBao ↔ Barbican ↔ a raw HSM is a composition change, not a refactor — and the choice MAY
     differ **per deployment** (an OpenStack provider on Barbican, a bare-metal provider on OpenBao,
     behind one trait).

   The honest limit: adapters are **not feature-identical below the trait** — rotation mechanics,
   audit-log shape, and latency differ — so the trait surface is held to what all conforming
   backends can guarantee, and anything richer stays out of it.

5. **The dev / single-binary profile uses a file-backed stub, no KMS**, with SoftHSM2 (BSD-2) as an
   optional realistic PKCS#11 path. This follows encryption being off by default in dev (ADR-0021
   §5) and single-binary being dev-only (ADR-0014). The stub MUST be unmistakably non-production
   (keys in cleartext on local disk) so it cannot be taken for real custody.

6. **Inline-dependency posture (settles ADR-0021's operational `[OPEN]`s in shape, not numbers).**
   Because the KMS is on the data path, Wyrd caches unwrapped DEKs for a **bounded lifetime** (the
   bound is mandatory; the value is per-deployment), **fails closed** on KMS unavailability without
   endangering the durability of the still-encrypted data, and **pins KEK custody to the tenant's
   residency region** (crypto-residency, ADR-0018). Time-bound key operations (scheduled rotation,
   hold expiry) depend on the trusted clock of ADR-0024.

7. **Licence and governance are a standing selection gate, not a one-time check (ADR-0003).** The
   chosen backend's licence and governance MUST be re-confirmed at adoption and watched thereafter —
   the deployed-service analog of the `cargo-deny` advisory wall. The licence facts in this ADR are
   point-in-time (the Vault relicensing is exactly why); they are re-verified, not assumed, at
   selection.

## Consequences

- The KMS finally has the selection rigor the other backends carry, and the `KeyService` trait is
  the commitment — so OpenBao ↔ Barbican ↔ a raw HSM is a composition change in `server`, not a
  refactor (ADR-0010).
- The key-custody story stays sovereign and un-rug-pullable: OpenBao keeps it foundation-governed
  and self-hostable, and the Vault rejection is a worked example of the ADR-0003 lens rather than a
  hypothetical.
- Crypto-erase and compliance hold get a concrete locus: KEK destruction is a KMS operation gated by
  hold state — which makes the still-undecided **retention / hold-vs-erase precedence** a named
  dependency of this contract, not an afterthought.
- Cost and burden land on the operator: running a KMS as a highly-available, audited service, with
  **KEK backups kept independent of Wyrd** (section 11), and KMS HA becomes a hard requirement for
  encrypted tenants. The client library and on-disk format carry key-version metadata (already
  reserved, ADR-0021).
- Still `[OPEN]`, to settle with the encryption implementation (M2+): the DEK-cache lifetime values,
  key-hierarchy depth for very large tenants (a per-bucket intermediate key, ADR-0021), and the
  precise PKCS#11 / KMIP profile and OpenBao HSM-backing maturity (historically Vault-Enterprise-
  gated; to be verified for the open fork).
- Refines ADR-0021; applies ADR-0003; depends on ADR-0024 (clock) and the reserved retention / WORM
  decision.
