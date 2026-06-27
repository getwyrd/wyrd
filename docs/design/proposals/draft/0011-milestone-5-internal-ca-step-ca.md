---
created: 27.06.2026
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: TBD
tags:
  - proposal
  - milestone-5
  - implementation-plan
  - security
  - pki
  - mtls
  - identity
---
# Proposal: Milestone 5 — Internal CA (step-ca integration) (implementation plan)

> The implementation plan for the fifth step of the [implementation arc][p2]
> (proposal 0013, which **supersedes proposal 0002**: Step 2 extended past the former M4
> release to make the single-zone product *production-usable*, with the ★ release
> point moved to **M8**). [M0–M4][p5] built and hardened a single-zone object
> store that is erasure-coded ([0003][p3]), networked ([0004][p4]), self-maintaining
> ([0005][p5]), and metadata-durable ([0007][p7]) — but **every internal RPC on
> that fabric is still plaintext**: `chunkstore-grpc` builds tonic
> "default-features only — no `tls`" ([ADR-0025][a25]; confirmed below — the root
> `Cargo.toml` declares `tonic = "0.14"` with no features table, and
> `GrpcChunkStore::connect` dials with `Endpoint::…connect()` and no
> `.tls_config`). M5 promotes that wire to **mutually-authenticated mTLS, required
> and fail-closed**, behind a `CertificateAuthority`/`IdentityProvider` seam, and
> authorizes peers against a **first-class, SPIFFE-shaped `PeerIdentity { role,
> zone, instance }`** with least authority per component — closing the internal
> service-to-service trust gap ([§14.9][s14]; [ADR-0025][a25]). It records *how* M5
> is built; the *why* of the trust posture and the CA choice lives in the
> architecture and the ADRs it references ([§8.5][s8], [ADR-0025][a25],
> [ADR-0036][a36], [ADR-0005][a5], [ADR-0010][a10]). M5 is **implementation-first
> behind already-decided ADRs**: ADR-0025 fixed the enforcement contract and
> ADR-0036 chose the CA technology (step-ca now, SPIRE reserved) and the two
> load-bearing seams — **no new spec and no ADR ratification is required**; M5 is
> the first *implementation* of ADR-0036, not a status flip.

## Motivation

M5 proves that **the service fabric authenticates *itself* — every internal RPC is
mutually authenticated under a provider-operated CA, and peers are authorized by a
first-class identity rather than by network position** ([§8.5][s8]; arc M5). The
arc's ordering principle is **risk retired, not features delivered**, and M5's
risk is a *pair* of falsifiable propositions, both already named in the threat
model and the deciding ADR:

- **The wire is authenticated, or it is not — and "authenticated" is not enough.**
  Today an internal dial is plaintext, so any process that can reach a D server's
  port can read or write fragments; there is no spoofing protection on the data
  path. Promoting the wire to mTLS retires the *spoofing* risk — but ADR-0025's
  whole point is that **a wire promoted to mTLS that still lets any authenticated
  component do anything is a spoofing fix that leaves elevation-of-privilege open**
  ([ADR-0025][a25]; [§14.9][s14]). M5 must therefore retire *authentication* and
  *authorization* together: mTLS required, fail-closed, **and** least authority
  enforced at each callee against the presented identity.

- **The reserved SPIRE seat stays open, or it silently closes.** ADR-0036 reserves
  SPIRE behind the same seam, exactly as openraft is reserved behind Coordination
  ([ADR-0006][a6]) and Model B behind the failure-domain label ([ADR-0034][a34]).
  The requirement that decides whether the later SPIRE switch is a *cheap
  composition change* or a *painful rewrite* is **requirement 2 of ADR-0036**:
  authorization MUST consume an abstract `PeerIdentity` via an adapter, never match
  raw DNS / SAN / SPIFFE-ID strings. "If authorization logic matches raw cert
  fields, SPIRE means touching every auth check; if it matches an abstract
  `PeerIdentity` behind an adapter, the switch is two adapters with the consuming
  logic unchanged" ([ADR-0036][a36]). M5's standing **guard test** — that
  authorization is keyed on the abstract identity, not on cert internals — is "the
  standing guard that the reserved seat has not silently closed," directly
  mirroring ADR-0034's placement-keyed-on-label test ([ADR-0036][a36];
  [ADR-0034][a34]).

M5 is **not a release point** — the ★ moved to M8 in the revised arc, where the
single-zone product becomes *operable* ([p2][p2]). But M5 is a hard *dependency* of
the rest of Step 2: **M6 (encryption at rest) needs "the trust fabric the KMS
authenticates against"** ([p2][p2] M6), and **M8 (manageability) reuses this CA /
identity fabric for the management plane's OIDC + mTLS auth** ([p2][p2] M8). So M5
de-risks the security substrate the remaining Step-2 milestones build on; it earns
its place by retiring a trust risk the production deployment cannot ship without,
not by adding a user-visible feature.

A second, quieter motivation: M5 is the **first** `CertificateAuthority` /
`IdentityProvider` implementation, and like every Wyrd seam its semantics are not
trusted until pinned by a second backend ([ADR-0006][a6] discipline). M5 builds
**two** backends from day one — the trivial dev self-signed CA and the production
step-ca client, behind one trait — so the seam is pinned by construction, and the
dev backend is not a throwaway stub but the profile the single-binary
build ([ADR-0014][a14]) runs against.

## Design

### Scope boundary

**In scope** — exactly what retires the internal-trust enforcement risk and keeps
the SPIRE seat open:

- A new **`CertificateAuthority` / `IdentityProvider` seam in `traits`** —
  joining `ChunkStore` / `PlacementChunkStore` / `MetadataStore` / `Coordination`
  (`crates/traits/src/lib.rs`) as the keystone of the dependency rule
  ([ADR-0010][a10], [ADR-0016][a16]). It is the place step-ca, SPIRE, and the dev
  self-signed CA become composition choices wired only in `server`. Object-safe,
  `Send + Sync`, `async` via `async_trait`, matching every existing trait.
- The **trivial self-signed / in-memory dev CA** behind that seam — the dev /
  single-binary profile's backend, mirroring `coordination-mem` ([ADR-0014][a14];
  [ADR-0036][a36]). Built in M5.1 alongside the trait so the seam is exercised by
  two implementations from the first slice.
- A **first-class `PeerIdentity { role, zone, instance }` type** and the
  **cert → identity adapter** that derives it from a presented certificate — never
  the reverse coupling where authorization reads cert fields. The standing **guard
  test** that authorization keys on the abstract identity lands with it
  ([ADR-0036][a36] req 2).
- **SPIFFE-ID-shaped SANs from day one** —
  `spiffe://<trust-domain>/<zone>/<role>/<instance>` minted by both backends, so
  the later SPIRE switch is "only the *issuer and acquisition path* change, not the
  identity shape the rest of the system reasons about" ([ADR-0036][a36] req 3).
- **The step-ca production backend** behind the seam, plus a **throwaway
  single-node step-ca in `deploy/`** for CI (no `deploy/` directory exists today —
  it is net-new, structured *outside* the Cargo workspace per [ADR-0010][a10]).
- **Rotation-aware identity acquisition** — a component obtains its identity from a
  source that can deliver a *rotated* certificate at runtime, **never read-once,
  cached-for-process-life** ([ADR-0036][a36] req 4). This serves step-ca renewal
  now and SPIRE's streamed rotation later, and is required regardless of backend.
- **mTLS required and fail-closed on the wire** — enable the tonic `tls` feature
  across `chunkstore-grpc` and every networked component; a dial that cannot
  complete the handshake **fails**, never silently downgrades to plaintext
  ([ADR-0025][a25] §1). The dev-CA path keeps the single-binary profile ungated
  ([ADR-0014][a14]).
- **Least authority per component** — authorization checked against the *presented
  identity* at each callee ([ADR-0025][a25] §2): a **D server** is
  tenant-oblivious, authorized only to store and return fragments by chunk id, with
  *nothing* on metadata / namespace / KMS (reinforcing the §14.2 blast radius); a
  **custodian** drives GC / repair and reads placement but is **not a namespace
  write authority**. A valid identity is still **denied an out-of-role operation**.
- **Backend selection in `server`** — wire `dev-self-signed | step-ca` behind the
  seam at the single composition root (`crates/server/src/cli.rs`), the one crate
  [ADR-0016][a16] designates for concretes.
- **The test surface the property demands** — **Tier-0 DST** proves the
  *authorization logic on the abstract identity* (deterministic, backend-free);
  **Tier-1 (local cluster)** injects the mTLS handshake / identity faults the
  simulator structurally cannot (wrong-identity dial refused, plaintext dial
  refused, a rotated cert picked up without restart). Document the reserved SPIRE
  upgrade in [§7][s7] and [§8.5][s8].

**Out of scope** — deferred to the milestone that actually retires its risk, the
seats kept open where retrofit is expensive:

- **External / tenant authentication** (OIDC / OAuth2 bearer, S3 SigV4, the
  gateway as the external-principal authentication boundary, [§8.5][s8]) — this is
  the **L1 access-layer auth plane, a distinct trust context from the internal
  fabric** ([ADR-0025][a25] explicitly separates the two; [ADR-0036][a36] req 5
  keeps "the fabric CA decision separate from external gateway TLS"). M5 is
  **strictly the internal service-to-service fabric**. Whether the management
  plane's external OIDC reuses this CA fabric is an **M8** concern ([p2][p2] M8;
  [0008][p8]) — flagged in Open questions, not built.
- **The S3 gateway's public TLS endpoint** — public ACME / Let's-Encrypt
  territory, "a separate, smaller concern … explicitly out of scope" of the CA
  decision ([ADR-0036][a36]; [ADR-0025][a25]). M5 governs only the internal mTLS
  fabric.
- **SPIRE itself** — **reserved behind the seam, not built** ([ADR-0036][a36]).
  Its operational cost (a stateful SPIRE Server + a per-node Agent +
  node/workload-attestation config) is "irreducible … a deploy-time migration no
  abstraction removes," to be paid when **fleet scale** makes secret-less
  attestation worth it. M5's whole job is to make that future switch a composition
  change; it does not pre-pay the operational cost.
- **Cross-zone / multi-zone trust** — there is one zone at M5. Zone-to-zone mTLS
  under the federation posture ([ADR-0005][a5]) becomes a live concern at **M9**
  (cross-zone replication) and the global control plane at **M10**; M5 authenticates
  *within* one zone.
- **Vault PKI / OpenBao as the CA** — Vault is **closed off on licensing grounds**
  (BSL, non-OSI), the same disqualifier as CockroachDB ([ADR-0036][a36]). step-ca
  is the decided backend; M5 builds no alternative production CA.
- **Encryption at rest and the `KeyService` / KMS** — [ADR-0021][a21] /
  [ADR-0026][a26] are **M6**, and M6 *needs* M5 ([p2][p2] M6). M5 must leave the
  KMS-authenticates-against-the-fabric path **expressible** (a `KeyService` peer is
  just another identity behind the seam) but builds none of it.
- **Hardening of the gRPC decode / input-validation surface** ([§14.9][s14]) — a
  parallel M2-era gap, not M5's. M5 secures *who* may dial; it does not re-harden
  *what bytes* the decoder accepts.

### What carries over from M0–M4, unchanged

M5 adds an *identity seam and a wire-security layer*; it touches **neither the data
traits nor the consumers' business logic**. The audit (below) confirms the
following carry over verbatim:

- **The `ChunkStore` / `PlacementChunkStore` / `MetadataStore` / `Coordination`
  traits** — exactly as they stand after M4 (`crates/traits/src/lib.rs`). M5
  **adds** a sibling identity trait; it does **not** edit the data/coordination
  traits. Authorization is a layer *in front of* the `ChunkStore` service impl, not
  a method on `ChunkStore` — the trait stays "deliberately dumb" (its own
  doc-comment, L4).
- **The metadata model, the commit point, the EC engine, the any-*k* read path** —
  none cross the identity seam; all unchanged. M5 secures the *transport and the
  caller's right to invoke*, not the operation's semantics.
- **The custodian plane (M3)** — GC, scrub, reconstruction operate over injected
  `&dyn ChunkStore` trait objects (`fleet: &[(DServerId, &dyn ChunkStore)]` in
  `crates/custodian/src/{reconstruction,scrub,gc}.rs`), **not** by dialing D
  servers themselves. The custodian's `&dyn ChunkStore` members are
  `GrpcChunkStore` clients in production (proven by the Tier-2
  `tier2_kill_reconstruct` harness), so the custodian's mTLS handshake **is the
  same `GrpcChunkStore` dial** the gateway uses — there is no separate custodian
  dial site to secure, and the custodian's *logic* needs no change.
- **The `DServerRegistration { id, endpoint, failure_domain }` record** (M3.1,
  `crates/server/src/dserver.rs`) — the existing identity-ish record. M5 **does not
  repurpose** `failure_domain`: it is the *placement* label (an opaque rack/power
  domain consumed by the placement selector, `crates/core/src/placement.rs`), a
  distinct concept from the *security* identity. `PeerIdentity.zone` and
  `.instance` have loose analogs here (`instance` ≈ `DServerId`), but `role` is
  **entirely new**, and `PeerIdentity` is derived from the *certificate*, not from
  the placement registration. The two stay separate (see Composition, below).
- **The deterministic-simulation harness (Tier-0)** and the realism ladder
  ([ADR-0009][a9]; [§13][s10]) — the spine M5 extends with authorization tests, not
  a structure M5 changes.

### The `CertificateAuthority` / `IdentityProvider` contract M5 introduces

ADR-0036 requirement 1 names the seam; M5 gives it a shape. There is **no existing
`CertificateAuthority`, `IdentityProvider`, `PeerIdentity`, `spiffe`, or `step-ca`
reference anywhere in `crates/`** (grep confirms zero matches) — this is greenfield
behind an already-decided contract. The seam decomposes into two responsibilities
the ADR keeps distinct: **acquiring my own identity** (the rotation-aware issuance
path) and **deriving a peer's identity** (the adapter from a presented cert). The
shape follows every existing trait — object-safe, `Send + Sync`, `async`:

```rust
/// A first-class peer identity, SPIFFE-shaped, distinct from raw cert fields
/// (ADR-0036 req 2/3). Authorization keys on THIS, never on DNS/SAN/SPIFFE-ID
/// strings. Issued and parsed as spiffe://<trust-domain>/<zone>/<role>/<instance>.
pub struct PeerIdentity {
    pub role: Role,        // d-server | custodian | gateway | …  (NOT a free string match)
    pub zone: Zone,
    pub instance: Instance,
}

/// Acquire and keep current THIS component's own credential. Rotation-aware:
/// the returned source can deliver a ROTATED cert at runtime — never read-once.
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// My own SPIFFE-shaped identity (for issuing my role claim).
    async fn identity(&self) -> Result<PeerIdentity>;
    /// A handle to my current credential that re-resolves on rotation; the wire
    /// layer reads it per-handshake, so a renewed cert is used without restart.
    async fn current_credential(&self) -> Result<Credential>;
    /// Derive a peer's first-class identity from the cert it presented. This is
    /// the ONE adapter; no other code parses cert internals.
    fn peer_identity(&self, presented: &PresentedCert) -> Result<PeerIdentity>;
}

/// Mint short-lived, auto-rotated certs (step-ca / SPIRE / dev self-signed).
#[async_trait]
pub trait CertificateAuthority: Send + Sync {
    /// Issue a short-lived credential for `identity`, with a SPIFFE-shaped SAN.
    async fn issue(&self, identity: &PeerIdentity) -> Result<Credential>;
    /// The trust roots a peer's cert must chain to, for handshake verification.
    async fn trust_bundle(&self) -> Result<TrustBundle>;
}
```

Four properties are load-bearing and each is a porting obligation onto step-ca:

1. **The adapter is the only place a raw cert is read.** `peer_identity` is the
   single function that inspects DNS/SAN/SPIFFE-ID bytes; everything downstream
   consumes `PeerIdentity`. This is requirement 2 made mechanical, and the **guard
   test** asserts it ([ADR-0036][a36]). A reviewer protecting the SPIRE seat looks
   for exactly one cert-parsing call site.
2. **SANs are SPIFFE-shaped from day one** —
   `spiffe://<trust-domain>/<zone>/<role>/<instance>` — under **both** backends, so
   SPIRE later changes the *issuer*, not the *shape* (requirement 3).
3. **Credentials are rotation-aware, never read-once.** `current_credential`
   returns a handle the wire layer re-resolves *per handshake*; "never read a cert
   once and cache it for the process lifetime" (requirement 4). A backend that
   returns a frozen cert violates the contract even if it compiles and the
   handshake succeeds.
4. **The CA backend is a composition choice, wired only in `server`.** Components
   "ask the seam for their identity; they do not call step-ca (or, later, the SPIRE
   Workload API) directly" (requirement 1). Any `crate` outside `server`/`traits`
   that names step-ca is a violation of the seam.

### Mapping the contract onto step-ca (the issuance translation)

step-ca supports the contract cleanly, with **one provisioner decision** and **one
acquisition discipline**. (To be verified against the pinned step-ca / client
version at build time — see Open questions; the *contract* is fixed here, the exact
client API is confirmed against the pin.)

- **The primitive.** step-ca is "a self-hosted online CA purpose-built for internal
  PKI with short-lived, auto-rotated certs" via provisioners — ACME, or JWK / OIDC
  ([ADR-0036][a36]). The production `IdentityProvider` backend acquires its
  credential from step-ca over its chosen provisioner, requesting a cert whose SAN
  is the component's SPIFFE-shaped identity, and **renews before expiry** so
  `current_credential` always hands the wire a live cert.
- **The mapping.** `CertificateAuthority::issue(identity)` becomes a step-ca sign
  request carrying `spiffe://<trust-domain>/<zone>/<role>/<instance>` as the SAN;
  `trust_bundle()` returns step-ca's root(s). `IdentityProvider::peer_identity`
  parses the SPIFFE SAN off the *presented* peer cert (verified by the handshake to
  chain to the trust bundle) into `PeerIdentity`.
- **The acquisition discipline — rotation without restart.** step-ca certs are
  short-lived by design; a component must renew on a schedule (well before expiry)
  and surface the *current* cert through `current_credential`. The wire layer reads
  that handle on each new handshake, so a renewed cert is adopted **without process
  restart**. This is the mechanism M5.4 tests directly, and the seam that later
  accepts SPIRE's streamed rotation unchanged.
- **The dev backend mirrors the shape, not the issuer.** The dev self-signed /
  in-memory CA issues the **same SPIFFE-shaped SANs** and satisfies the **same**
  `IdentityProvider` contract — so DST and the single-binary profile exercise the
  real identity shape and the real authorization logic, against a CA that needs no
  external service ([ADR-0014][a14]; [ADR-0036][a36]). The dev backend is a *peer*
  of step-ca behind the seam, not a bypass of it.

### Composition, not refactor — the thesis, with the honest count

The milestone's claim is that securing the fabric is a **layer added behind the
seam plus a composition change in `server`**, not a refactor of consumer logic. An
audit of every transport boundary and authorization site **confirms the thesis** —
and states its honest size:

- **There are exactly two production transport boundaries, both in `server`.** The
  **serve side** is `DServer::serve` (`crates/server/src/dserver.rs`), which builds
  the one production `tonic::transport::Server::builder()` and
  `.add_service(ChunkStoreServer::new(...))`. The **dial side** is `connect_fanout`
  (`crates/server/src/cli.rs`) → `GrpcChunkStore::connect`
  (`crates/chunkstore-grpc/src/client.rs`), which today calls
  `Endpoint::…connect()` with **no `.tls_config`**. mTLS lands at these two sites:
  `.tls_config(ServerTlsConfig)` on the server build, a TLS `Channel` on the dial.
- **`chunkstore-grpc` already has the injection seam.** `GrpcChunkStore::new(channel:
  Channel)` accepts an already-built `Channel`, so a TLS-configured channel is
  injected **without touching call sites**; and the crate exposes the service impl
  (`ChunkStoreService<S>`) without building the `Server` itself, so the
  authorization layer is a tower layer / interceptor added at `DServer::serve`, not
  a change to the service methods.
- **`core` and `custodian` carry zero transport coupling.** `core`'s read/write
  paths operate over `&impl/&dyn` data traits; the custodian operates over injected
  `&dyn ChunkStore` fleet objects. **No consumer dials**, so **no consumer logic
  changes** — securing the dial in `server` secures the custodian's fleet members
  for free (they *are* `GrpcChunkStore` clients).
- **No code matches raw cert fields today** because no code touches certs at all —
  so requirement 2 is satisfiable by *construction*: M5 introduces cert handling
  with the adapter already the sole entry point, rather than retrofitting an
  abstraction over scattered cert matching.

The composition weight is therefore three concrete things, all in `server` (the
ADR-0016 wiring crate):

1. **The CA/identity backend selection** — construct `dev-self-signed | step-ca`
   behind the seam in `crates/server/src/cli.rs` (the same composition root that
   today picks `RedbMetadataStore` / `MemCoordination` / `FsChunkStore` /
   `connect_fanout`), and thread the chosen `IdentityProvider` into `DServer::serve`
   (for `.tls_config` + the authorization layer) and into the dial path (for the
   TLS `Channel`). There is **no backend-selection mechanism today** for identity —
   it is net-new, like M4's redb|tikv selector was.
2. **The wire-security wiring** — `.tls_config` on the server build,
   TLS `Channel` on the dial, both reading the **rotation-aware** credential handle
   so a renewed cert is adopted without restart. Confined to `dserver.rs` /
   `cli.rs` / `client.rs`; the data traits are untouched.
3. **The per-callee authorization layer** — a tower layer / interceptor at
   `DServer::serve` that derives the caller's `PeerIdentity` via the adapter and
   **denies an out-of-role operation**, even from a valid identity ([ADR-0025][a25]
   §2). This is new code, but it sits *in front of* `ChunkStoreServer`, not inside
   the `ChunkStore` trait — the dumb-store discipline holds.

None of these is a cross-cutting refactor of consumer logic. **The real
engineering weight — the rotation-aware issuance and the SPIFFE adapter — lands in
the new identity backends behind the seam**, exactly as the thesis predicts.

### Least authority per component — what each identity grants

ADR-0025 §2 fixes the authority each role's identity carries; M5 enforces it at the
callee, against the *presented* identity, "not assumed from network position":

- **D server** — **tenant-oblivious**, authorized **only** to store and return
  fragments by chunk id. Its identity grants it *nothing* on the metadata store,
  the namespace, or the KMS, "so a compromised D server has no credential worth
  stealing" — reinforcing the §14.2 D-server blast-radius property at the network
  layer ([ADR-0025][a25]; [§14.2][s14]). The authorization layer at
  `DServer::serve` admits a `role = d-server`-or-caller-authorized request and
  denies anything outside the fragment-bytes contract.
- **Custodian** — may drive GC / repair against D servers and **read** placement
  state, but is **not a write authority on the namespace** ([ADR-0025][a25]). Since
  the custodian acts through `GrpcChunkStore` clients, its identity is what a D
  server sees on the serve side; the D server authorizes the custodian for the
  fragment operations repair/GC need, nothing more.
- **Gateway** — authenticates *external* principals and is the only component that
  resolves ACLs ([ADR-0025][a25]) — but that **external** auth plane is **M8 /
  out of M5 scope** ([§8.5][s8]; [ADR-0036][a36] req 5). Within M5 the gateway is
  just another internal identity on the fabric.

The verbatim M5 definition of done — "a valid identity is still denied an
out-of-role operation" ([p2][p2] M5) — is exactly this layer, proven by a test
where a correctly-authenticated peer is refused an operation outside its role.

### DST and tests — the split the seam enables

[ADR-0009][a9] remains the correctness authority, and M5's most important testing
decision is **what each tier proves**. The identity seam splits the burden
cleanly: **DST proves the authorization *logic*; the local cluster proves the
*handshake and rotation*.**

**Tier-0 — deterministic simulation (the authorization spine).** The authorization
decision is **pure logic over an abstract `PeerIdentity`** — given a derived
identity and a requested operation, is it allowed? That is deterministic,
backend-free, and seed-reproducible, so it lives in DST: least-authority decisions
(D-server-denied-metadata, custodian-denied-namespace-write, valid-identity-denied-
out-of-role), and the **guard test** that authorization keys on the abstract
identity and never on raw cert fields. Because the production authorization logic
consumes only `PeerIdentity`, proving it over the dev backend's identities proves
it for any backend — the same soundness argument M4 used for the byte-identical
commit logic ([ADR-0009][a9]; [§13.1][s10]). DST does **not** model the TLS
handshake's cryptography — that is the real tier's job, and re-proving it in DST
would violate "a real environment is never used to test correctness the simulation
already covers" run *backwards*.

**Tier-1 — local cluster, mTLS handshake / identity fault injection.** This is where
the M5-specific evidence the simulator structurally cannot show lives:
- **Wrong-identity dial refused** — a peer presenting a cert that derives to the
  wrong role/zone is rejected at the handshake or the authorization layer.
- **Plaintext / handshake-fail dial refused** — a plaintext dial, or one that
  cannot complete the mTLS handshake, **fails closed**, never silently downgrades
  ([ADR-0025][a25] §1). "The fail-closed rule means a misconfigured CA is an
  outage, not a silent plaintext downgrade — the intended trade" ([ADR-0025][a25]).
- **Rotation without restart** — a cert is rotated under a running component and the
  new cert is picked up on the next handshake **without a process restart** (the
  rotation-aware-acquisition property, [ADR-0036][a36] req 4), against the dev CA
  and against the throwaway step-ca in `deploy/`.

**Tier posture and DST fidelity (an Open question made concrete).** mTLS handshake
and identity faults are a **Tier-1** concern (real TLS, real certs); DST proves the
authorization logic on the abstract identity. The open design point is **how
faithfully to model cert rotation / expiry inside DST** — a deterministic
*time-advances-cert-expires* model would let DST exercise the "renew before expiry"
scheduling logic, but the cryptographic handshake stays at Tier-1. M5 decides this
fidelity line explicitly rather than letting it drift (see Open questions).

**Fidelity to ADR-0009 — the compounding loop.** Any behavior the real handshake /
step-ca surfaces that the dev fake did not model (a renewal-timing edge, a
trust-bundle-rotation race) is **promoted back into DST as a seeded regression
wherever it manifests through the seam** ([§13.1][s10]) — the same FoundationDB
pattern M4 used. The seam is both what M5 validates and the channel real-world
findings flow back through.

### Crate touch-points

Building on the workspace as it stands (`chunk-format`, `chunkstore-fs`,
`chunkstore-grpc`, `coordination-mem`, `core`, `custodian`, `dst`, `metadata-redb`,
`proto`, `server`, `testkit`, `traits`; `xtask` at the repo root, not under
`crates/`):

- **`traits`** — **add** the `CertificateAuthority` / `IdentityProvider` traits and
  the `PeerIdentity { role, zone, instance }` type (and `Role` / `Zone` /
  `Instance` / `Credential` / `TrustBundle` / `PresentedCert` supporting types).
  The existing data/coordination traits are **unchanged** (any edit to them is a
  failure of M5's thesis).
- **`identity-dev`** (**new**) — the trivial self-signed / in-memory
  `CertificateAuthority` + `IdentityProvider`; the dev / single-binary profile's
  backend. Deps `traits` + a rustls/cert-gen crate; **never** `core`/`server`
  concretes ([ADR-0016][a16]). (Naming mirrors `coordination-mem` /
  `metadata-redb`; the exact crate name confirmed at build.)
- **`identity-stepca`** (**new**) — the production step-ca `IdentityProvider` /
  `CertificateAuthority` client: SPIFFE-shaped SAN issuance, rotation-aware
  acquisition, trust-bundle resolution. Deps `traits` + the step-ca client + a
  TLS/cert crate.
- **`chunkstore-grpc`** — enable the tonic `tls` feature; accept a TLS `Channel`
  through the existing `GrpcChunkStore::new(channel)` seam; surface no plaintext
  fallback. No change to the service methods.
- **`server`** — wire `dev-self-signed | step-ca` behind the seam in `cli.rs` (the
  single composition root); `.tls_config` on `DServer::serve`'s `Server::builder`;
  the TLS `Channel` on `connect_fanout`'s dial; the **per-callee authorization
  layer** (tower layer / interceptor) at `DServer::serve`. redb-style: the dev CA
  stays the single-binary default ([ADR-0014][a14]).
- **`core`, `custodian`** — **unchanged** (zero transport coupling confirmed by the
  audit; the custodian's fleet members are secured `GrpcChunkStore` clients
  transparently).
- **`dst`** — the authorization-logic property suite over abstract `PeerIdentity`
  (least authority, out-of-role denial) and the **guard test**; optionally a
  deterministic cert-expiry/rotation model (fidelity per Open questions). New seeds.
- **`testkit`** — a local-cluster mTLS fault seam (wrong-identity / plaintext /
  handshake-fail dial; cert rotation under load) for the Tier-1 security suite.
- **`xtask`** — a Tier-1 security-suite runner; wire the `deploy/` throwaway
  single-node step-ca into CI.
- **`deploy/`** (**new, outside the workspace**) — a throwaway single-node step-ca
  for CI/eval; the production step-ca topology (one stateful CA service,
  [ADR-0036][a36]) documented but Helm/operator deferred ([ADR-0010][a10]).
- **deps** — the step-ca client + a TLS/cert crate (rustls family); **add any new
  license to `deny.toml`'s allowlist as a deliberate edit** ([ADR-0003][a3]) — a
  non-allowlisted license fails `cargo deny` under `cargo xtask ci`.

## Alternatives considered

- **Adopt SPIRE now (the architecturally complete answer):** **rejected /
  reserved** — SPIRE's secret-less workload-attestation benefit "mostly
  materializes at fleet scale, while its operational and attestation-debugging cost
  lands now" ([ADR-0036][a36]). SPIRE is reserved behind the same seam; M5's job is
  to make that later switch a composition change (two adapters), not to pay its
  attestation tax before the benefit materializes.
- **Vault PKI as the CA:** **rejected on licensing grounds** — Vault moved to the
  BSL (source-available, non-OSI) in 2023, the same disqualifier applied to
  CockroachDB ([ADR-0036][a36]). (OpenBao, the MPL Linux-Foundation fork, is noted
  as the sovereignty-clean way to get Vault's model if ever wanted — not adopted.)
- **Roll our own CA:** **rejected** by the reinvent-vs-consume doctrine — "cert
  lifecycle, rotation, revocation, and attestation are exactly the subtle,
  well-solved security primitives to *consume*, not rebuild. The novelty budget is
  spent on the commit protocol" ([ADR-0036][a36]).
- **Match raw DNS / SAN / SPIFFE-ID strings in authorization (skip the abstract
  `PeerIdentity`):** **rejected** — this is precisely the collapse ADR-0036
  forbids; it "silently closes the SPIRE door" and makes the later switch "a
  pervasive rewrite" ([ADR-0036][a36] req 2). M5 routes every authorization through
  the abstract identity behind the adapter, guarded by a standing test.
- **Authenticate the wire but skip per-component authorization (mTLS only):**
  **rejected** — "a wire promoted to mTLS that still lets any authenticated
  component do anything … leaves elevation-of-privilege open" ([ADR-0025][a25];
  [§14.9][s14]). M5 retires authentication and authorization together.
- **Opportunistic mTLS with plaintext fallback:** **rejected** — ADR-0025 §1
  requires a dial that cannot complete the handshake to **fail closed**, never
  silently downgrade. "A misconfigured CA is an outage, not a silent plaintext
  downgrade — the intended trade."
- **Identity per host instead of per workload:** **rejected** — under the
  one-D-server-per-disk model ([ADR-0034][a34]) "many D servers share a host, so
  host-based identity is too coarse; identity must name the process and its role"
  ([ADR-0036][a36]). `PeerIdentity` is workload identity.
- **Read the cert once at startup and cache it for the process lifetime:**
  **rejected** — short-lived auto-rotated certs require runtime renewal; "never read
  a cert once and cache it for the process lifetime" ([ADR-0036][a36] req 4). The
  acquisition seam is rotation-aware, serving step-ca renewal and SPIRE streamed
  rotation alike.
- **Reuse `DServerRegistration.failure_domain` as the security identity:**
  **rejected** — `failure_domain` is the *placement* label ([ADR-0034][a34]), a
  distinct concept derived from registration, not from a verified certificate.
  Security identity is `PeerIdentity` derived by the adapter from the presented
  cert. Conflating them would couple authorization to unverified self-reported
  registration data.
- **Govern the gateway's public TLS / external OIDC in the same milestone:**
  **rejected / deferred to M8** — the external access plane is a distinct trust
  context ([ADR-0025][a25]; [ADR-0036][a36] req 5); M5 is strictly the internal
  fabric.
- **Mint a new ADR for the CA technology:** **not minted** — [ADR-0036][a36]
  already decides step-ca-now / SPIRE-reserved and [ADR-0025][a25] fixes the
  enforcement contract, both standing; M5 is the first *implementation*, not a new
  decision.

## Graduation criteria (definition of done)

- **The `CertificateAuthority` / `IdentityProvider` seam exists in `traits`** —
  object-safe, `Send + Sync`, async — with **two** implementations (dev
  self-signed, step-ca) behind it, wired only in `server` ([ADR-0036][a36] req 1;
  [ADR-0016][a16]). The existing data/coordination traits are byte-for-byte
  unchanged.
- **Peers authenticate and authorize against a first-class
  `PeerIdentity { role, zone, instance }`**, derived by a single adapter from the
  presented certificate; **no authorization logic inspects raw cert fields**, and
  the **standing guard test** asserts it ([ADR-0036][a36] req 2).
- **Certs carry SPIFFE-ID-shaped SANs** `spiffe://<trust-domain>/<zone>/<role>/<instance>`
  under both backends, so the reserved SPIRE switch is issuer-only ([ADR-0036][a36]
  req 3).
- **mTLS is required and fail-closed across every networked component** — the tonic
  `tls` feature enabled in `chunkstore-grpc` and threaded through `server`'s serve
  and dial sites; a plaintext or handshake-failing dial is **refused**, never
  downgraded ([ADR-0025][a25] §1).
- **Identity acquisition is rotation-aware** — a rotated cert is adopted at runtime
  **without process restart**, proven at Tier-1 ([ADR-0036][a36] req 4).
- **Least authority holds per component** — a D server is tenant-oblivious
  (fragments by chunk id only), a custodian is not a namespace write authority, and
  **a valid identity is still denied an out-of-role operation** ([ADR-0025][a25]
  §2; [p2][p2] M5).
- **The dev / single-binary profile is ungated** — the trivial self-signed CA
  behind the same seam keeps the single-binary build runnable without a full PKI
  ([ADR-0014][a14]; [ADR-0036][a36]).
- **Tier-0 DST proves the authorization logic** (least authority, out-of-role
  denial, the guard test) green and seed-reproducible; **Tier-1 local-cluster
  security suite green** (wrong-identity refused, plaintext/handshake-fail refused,
  rotation-without-restart). Any discovery promoted to a seeded DST regression.
- **The reserved SPIRE upgrade is documented** in [§7][s7] and [§8.5][s8] with its
  trigger and irreducible cost ([ADR-0036][a36] req 6).
- `fmt`/`clippy` clean; `Cargo.lock` updated; **`cargo-deny` passes** with the new
  step-ca / TLS dependency tree, any new license added to the allowlist deliberately
  ([ADR-0003][a3]).

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M5** milestone (branch
`feat/m5.<n>-<slug>`, commit subject `feat(<crate>): … (M5.<n>, #<issue>)`):

1. **M5.1 — the seam + the dev backend** (`feat/m5.1-ca-identity-seam`,
   `feat(traits): …`). Add `CertificateAuthority` / `IdentityProvider` and
   `PeerIdentity { role, zone, instance }` to `traits` (deps = `traits` only); add
   the `identity-dev` trivial self-signed / in-memory backend; **fix the
   SPIFFE-shaped SAN structure** `spiffe://<trust-domain>/<zone>/<role>/<instance>`.
   *DoD:* the seam compiles object-safe and is exercised by the dev backend; the dev
   CA issues SPIFFE-shaped certs; data/coordination traits unchanged.
2. **M5.2 — first-class `PeerIdentity` + adapter + the guard test**
   (`feat/m5.2-peer-identity-adapter`, `feat(traits): …`). The cert → identity
   adapter (`peer_identity`) as the sole cert-parsing site; the **standing guard
   test** that authorization keys on the abstract identity, never on raw cert fields
   (mirroring ADR-0034's placement-keyed-on-label test). *DoD:* the adapter derives
   `PeerIdentity` from a presented cert; the guard test fails if any authorization
   path reads a raw cert field; DST green.
3. **M5.3 — the step-ca backend + CI step-ca** (`feat/m5.3-stepca-backend`,
   `feat(identity-stepca): …`). The production step-ca `IdentityProvider` /
   `CertificateAuthority` behind the seam; a throwaway single-node step-ca in
   `deploy/` for CI; pin step-ca / client versions; **`cargo-deny` gate** updated
   for the new dependency tree ([ADR-0003][a3]). *DoD:* the step-ca backend passes
   the **shared** identity-seam conformance suite the dev backend passes; CI can
   reach a step-ca; `cargo deny` green with the new licenses allowlisted.
4. **M5.4 — rotation-aware acquisition** (`feat/m5.4-rotation-aware-acquisition`,
   `feat(identity-stepca): …`). `current_credential` re-resolves on renewal; the
   wire layer reads it per-handshake; **never read-once-cache-for-life**. *DoD:* a
   rotated cert is picked up at runtime **without restart**, proven against the dev
   CA and the `deploy/` step-ca; the read-once anti-pattern is structurally
   impossible (no cached cert field).
5. **M5.5 — mTLS required & fail-closed on the wire** (`feat/m5.5-mtls-fail-closed`,
   `feat(chunkstore-grpc): …`). Enable the tonic `tls` feature; `.tls_config` on
   `DServer::serve`'s `Server::builder`; TLS `Channel` on `connect_fanout` /
   `GrpcChunkStore::connect`; **no plaintext fallback**; the dev-CA path keeps the
   single-binary profile ungated. *DoD:* every internal dial is mTLS; a plaintext or
   handshake-failing dial is **refused** (Tier-1); the single-binary build still
   runs on the dev CA.
6. **M5.6 — least authority per component** (`feat/m5.6-least-authority`,
   `feat(server): …`). The per-callee authorization layer at `DServer::serve`:
   authorize against the presented `PeerIdentity`; D server tenant-oblivious;
   custodian not a namespace write authority. *DoD:* a valid identity is **denied an
   out-of-role operation** (Tier-0 logic + Tier-1 wire); the D-server identity grants
   nothing on metadata/namespace/KMS; DST authorization suite green.
7. **M5.7 — backend selection in `server` + the security suite + docs**
   (`feat/m5.7-server-wiring-and-suite`, `feat(server): …`). Wire
   `dev-self-signed | step-ca` behind the seam at the single `cli.rs` composition
   root; the DST authorization suite + the Tier-1 local-cluster security suite
   (wrong-identity / plaintext refused; the guard test; the rotation test); document
   the reserved SPIRE upgrade in [§7][s7] + [§8.5][s8] ([ADR-0036][a36] req 6).
   *DoD:* `server` runs identically on the dev CA (dev) and step-ca (prod) by config
   from one wiring point; the full security suite is green in CI; the SPIRE upgrade
   is documented with trigger and irreducible cost.

(M5 is sized like M1/M2's seven slices: two new identity-backend crates plus a
seam in `traits`, a wire-security layer, and the local-cluster security campaign —
not a new plane. Slices 1–2 are the seam and the SPIRE-protecting abstraction;
3–4 are the production backend and its rotation discipline; 5–6 are the wire and
authorization enforcement; 7 is the composition switch and the proof. The crate
boundary means M5.1 can begin against **the tail of M4** once the data traits are
confirmed frozen — exactly the "Needs M2; can begin against the tail of M4"
sequencing the arc records.)

## Backward compatibility

M5 lands inside Step 2, before the M8 ★ release, so the compatibility duties are
deliberately narrow:

- **The data / coordination traits** — **unchanged**. M5 *adds* an identity seam;
  it does not evolve `ChunkStore` / `MetadataStore` / `Coordination`. Any edit
  there is a failure of M5's thesis.
- **The `CertificateAuthority` / `IdentityProvider` seam** — **new**, and **pinned
  by two implementations** (dev self-signed + step-ca) from M5.1, so it is a real
  internal contract from birth, not a single-backend shape that ossifies. (Pre-1.0,
  still no *published* API.)
- **The SPIFFE-shaped SAN format** — fixed here
  (`spiffe://<trust-domain>/<zone>/<role>/<instance>`) and chosen **so SPIRE is a
  non-breaking issuer swap** — the format the rest of the system reasons about does
  not change at SPIRE-switch time ([ADR-0036][a36] req 3). This is the one shape M5
  must get right precisely because it is the SPIRE-cheapness contract.
- **The wire** — promoting plaintext to required mTLS is a **breaking change to the
  network protocol**, but there are "no public deployments … nothing to stay
  compatible with" at this pre-release stage ([p2][p2]); a fresh secured deployment
  starts on mTLS, and the dev CA keeps local runs trivial. No migration of an
  existing plaintext fleet is in scope.
- **The on-disk fragment / metadata formats** — **untouched** by M5 (it secures the
  transport and the caller's right to invoke, not the bytes at rest). No `v1`
  stamping is tied to M5.
- **Reserved seats honored** — the SPIRE upgrade ([ADR-0036][a36]) and the M6
  `KeyService`-authenticates-against-the-fabric path ([ADR-0021][a21] /
  [ADR-0026][a26]) remain expressible; M5 builds neither but forecloses neither.

## Open questions

- **step-ca provisioner choice (ACME vs JWK / OIDC) and the SPIFFE trust-domain
  name.** The provisioner shapes how each component proves itself to step-ca at
  issuance, and the trust-domain string is the root of every SPIFFE SAN
  (`spiffe://<trust-domain>/…`). Both are pinned at M5.3 against the chosen step-ca
  version; the trust-domain naming should be decided once and documented, since it
  is part of the identity shape SPIRE later inherits.
- **The SPIRE-upgrade trigger.** At what **fleet scale** does secret-less workload
  attestation earn SPIRE's irreducible operational weight (a stateful SPIRE Server +
  a per-node Agent + node/workload-attestation config)? Recorded as a **deliberate
  deferral with a defined upgrade path** ([ADR-0036][a36] req 6), not decided here —
  M5's job is to keep the switch a composition change, documented in [§7][s7] /
  [§8.5][s8].
- **Whether the management plane's external OIDC (M8) reuses this CA fabric.** M8
  "reuses the CA / identity fabric for OIDC + mTLS auth" ([p2][p2] M8), but the
  *external* principal auth is a distinct trust context ([ADR-0025][a25];
  [ADR-0036][a36] req 5). The dependency is real; the design of that reuse is **M8 /
  [0008][p8]**, flagged here so the internal/external boundary stays deliberate.
- **DST fidelity for cert rotation / expiry.** How faithfully should DST model
  *time-advances-cert-expires-renew-before-expiry* (to exercise the renewal
  scheduling logic deterministically) versus leaving all rotation behavior at
  Tier-1 with real certs? The **cryptographic handshake** stays Tier-1 regardless;
  the open point is whether the *renewal-timing* logic earns a deterministic model.
  An M5 design point, surfaced because the rotation-aware seam is the one piece with
  real time-dependence.
- **The exact step-ca client / TLS-stack crate versions and API shapes.** Pin the
  step-ca client and the rustls-family crate in `Cargo.toml`, reconfirm the
  issuance / renewal / trust-bundle entry points against the pin, and confirm their
  futures are `Send + Sync` for the object-safe, simulator-driven seam. Add any new
  license to `deny.toml` deliberately ([ADR-0003][a3]).
- **Tonic `tls` backend selection.** tonic 0.14's TLS is a non-default feature
  (`tls-ring` / `tls-aws-lc`); which crypto provider M5 enables, and whether the
  `madsim-tonic` simulation alias needs a matching shim, is confirmed at M5.5
  against the pinned tonic version.

[p2]: ../accepted/0013-implementation-arc-rescoped.md
[p3]: ../accepted/0003-milestone-1-erasure-coding.md
[p4]: ../accepted/0004-milestone-2-networked-d-servers.md
[p5]: ../accepted/0005-milestone-3-custodians.md
[p7]: ./0007-milestone-4-production-metadata-backend.md
[p8]: ./0008-management-and-administration.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[s14]: ../../architecture/14-threat-model.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a5]: ../../adr/0005-single-provider-closed-federation.md
[a6]: ../../adr/0006-etcd-for-coordination.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a21]: ../../adr/0021-encryption-at-rest-and-key-management.md
[a25]: ../../adr/0025-internal-service-to-service-trust.md
[a26]: ../../adr/0026-key-service-and-kms-backend-selection.md
[a34]: ../../adr/0034-d-server-disk-model.md
[a36]: ../../adr/0036-internal-ca-step-ca-spire.md
