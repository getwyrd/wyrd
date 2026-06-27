---
created: 27.06.2026
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: TBD
tags:
  - proposal
  - milestone-6
  - implementation-plan
  - security
  - encryption
  - key-management
  - kms
---
# Proposal: Milestone 6 — encryption at rest (KeyService / KMS) (implementation plan)

> The implementation plan for the sixth step of the [implementation arc][p2]
> (proposal 0013, which supersedes proposal 0002). M4 made metadata production-durable behind
> the unchanged `MetadataStore` seam ([0007][p7]); **M5** stood up the internal
> trust fabric — every internal RPC mutually authenticated under a provider CA,
> peers authorized by a SPIFFE-shaped identity ([ADR-0036][a36], [ADR-0025][a25]).
> M6 adds the **second load-bearing security tier**: per-object **envelope
> encryption at rest**, the data-encryption keys wrapped by per-tenant
> key-encryption keys held in an external **KMS behind a narrow `KeyService`
> trait** ([ADR-0026][a26]; arc M6). Like M4, M6 is **implementation-first behind
> already-decided ADRs** — [ADR-0021][a21] (envelope encryption) and [ADR-0026][a26]
> (the `KeyService` contract and OpenBao default) carry the *why*; this plan
> records the *how*. M6's distinguishing trait is that it lands the system's
> **first inline, on-the-data-path external dependency**: unlike TiKV or etcd, a
> KMS sits *between* the client and its bytes for every encrypted read and write,
> so M6's whole risk is **the new failure domain that posture creates**, not the
> cryptography. The format's encryption hooks were reserved from M0 and the
> `KeyService` surface is already normative; **no new spec and no ADR ratification
> is required** (ADR-0021/0026 are the decisions; M6 is their first
> *implementation*).

## Motivation

M6 retires **the KMS-as-a-new-failure-domain risk** ([§11][s10]: "a KMS outage or
a lost KEK is catastrophic"; arc M6). The arc's ordering principle is **risk
retired, not features delivered**, and M6's risk is unusually sharp because of
*where* the dependency sits. Every prior external dependency — etcd (L5), TiKV
(L4), the D-server fleet — is **off the critical bytes path**: losing coordination
"loses no data … what is lost is the ability to *react*" (`crates/traits/src/lib.rs`,
`Coordination` doc-comment); a stalled custodian degrades durability slowly, not a
single read. The KMS is the **first dependency on the read/write path itself**: for
an encrypted tenant, a `get` cannot return plaintext without an `unwrap`, and a
`put` cannot place a fragment without a fresh wrapped DEK. ADR-0021 names this
plainly — "the KMS sits on the read/write path for encrypted tenants, so its
latency and availability matter." M6's job is to **introduce that inline
dependency without making it a new way to lose, leak, or block data**.

That decomposes into three falsifiable propositions, each one a thing that either
holds or does not:

- **The seam is the envelope contract, not a vendor API.** The `KeyService` trait
  (ADR-0026 §1) must express *only* what every conforming KMS guarantees —
  generate-data-key, unwrap, rotate-KEK, destroy-KEK — and must **not** leak
  OpenBao-Transit-specific shapes, or the "OpenBao ↔ Barbican ↔ raw HSM is a
  composition change in `server`, not a refactor" claim (ADR-0026 §4) fails the
  first time a second backend is wired. This is the M4 thesis re-run on a new seam:
  the trait holds, or it does not.
- **KEK material never escapes the KMS, and Wyrd never persists an unwrapped KEK.**
  Wrap / unwrap / rotate happen **inside** the KMS — the "encryption-as-a-service"
  shape (ADR-0026 §1). If any code path reads a KEK out of the KMS, the
  crypto-erase guarantee (destroy the KEK → data unrecoverable, ADR-0021 §4) is
  hollow, because the key escaped.
- **KMS unavailability fails *closed* without endangering the durability of the
  still-encrypted data.** ADR-0021 and ADR-0026 §6 both demand this exact shape: a
  KMS outage must make encrypted data temporarily *unreadable*, never *lost* — the
  ciphertext, the wrapped DEK, and the key-version id are all still on durable
  storage; only the unwrap is unavailable. Conflating "can't reach the KMS" with
  "data is gone" would invert the risk M6 exists to retire.

A second, quieter motivation mirrors M4's "second implementation pins the trait."
M6 is the **first** `KeyService` implementation of consequence — the file-backed
dev stub and the OpenBao backend land together, so the trait is born with **two
backends** (ADR-0006's discipline, applied to the new seam from the start): the
stub keeps the contract honest about what is *common-denominator*, OpenBao keeps it
honest about what a *real* KMS can do. Anything one backend can do that the other
structurally cannot is, by construction, kept *out* of the trait.

## Design

### Scope boundary

**In scope** — exactly what retires the inline-KMS-failure-domain risk and makes
encryption-at-rest real behind the seam:

- **A `KeyService` trait in `traits`** — joining `ChunkStore` / `MetadataStore` /
  `NamespaceStore` / `Coordination` under the narrow-trait dependency rule
  ([ADR-0016][a16]: "implementations and consumers depend on the `traits` crate …
  only the `server` binary wires concretes"). Object-safe, `async` (via
  `async_trait`), `Send + Sync`, surfacing the normative envelope operations of
  ADR-0026 §1 and nothing Transit-specific. The trait is the milestone's central
  commitment (ADR-0026 §1).
- **A file-backed dev stub** behind the trait — unmistakably non-production (keys
  in cleartext on local disk, ADR-0026 §5), so it cannot be mistaken for real
  custody. It exists to keep the dev/single-binary path KMS-free (ADR-0014) and to
  pin the trait's *common-denominator* shape from day one.
- **An OpenBao Transit backend** behind the same trait — the default (ADR-0026 §2),
  MPL-2.0, Linux-Foundation-governed. Transit's `datakey` / `encrypt` / `decrypt`
  / `rewrap`, key versioning, and audit log map almost one-to-one onto the
  contract, so the envelope ops are *native, not emulated* and KEK material never
  leaves the service.
- **Envelope encryption on the client read/write path** in `core` — the write path
  encrypts each object/chunk's bulk bytes under a fresh DEK *before* erasure coding
  (ADR-0021 §2), stores the **wrapped DEK + key-version id** in the metadata, and
  stamps `flags` bit 0 / `encryption_scheme` in the fragment header; the read path
  unwraps the stored DEK and decrypts *after* reconstruction. Encryption is a
  **per-tenant policy, off by default in dev** (ADR-0021 §5).
- **The bounded DEK cache** — unwrapped DEKs cached for a **bounded lifetime** (the
  bound is *mandatory*; the value is per-deployment, ADR-0026 §6), so the inline
  KMS is not consulted on every block.
- **Fail-closed on KMS unavailability** — a read whose DEK is uncached and whose
  KMS is unreachable **errors** (encrypted data is temporarily unreadable) rather
  than returning ciphertext or fabricating a key; a write that cannot mint a fresh
  wrapped DEK **refuses** rather than writing unencrypted-but-marked-encrypted
  bytes. Neither path endangers the **durability** of already-stored ciphertext
  (ADR-0021; ADR-0026 §6).
- **Crypto-erase + rotation as KMS operations** — rotate-KEK is **re-wrap, not
  re-encrypt** (cheap, metadata-only, never touches bulk ciphertext, ADR-0021 §3);
  destroy-KEK renders all ciphertext under it unrecoverable (ADR-0021 §4) and is
  **refused while any object under that key is under a retention / legal hold**
  (ADR-0026 §1; the enforcement point of [ADR-0028][a28] §3).
- **Residency-pinned KEK custody** — KEK custody is pinned to the **tenant's
  residency region** (crypto-residency, the answer to metadata sovereignty
  [ADR-0018][a18], [§8.8][s8]); a tenant's KEK does not leave its region.
- **Backend selection in `server`** — the **file-stub | OpenBao** selector at the
  one wiring point ADR-0016 designates, with the choice allowed to **differ per
  deployment** (ADR-0026 §4). This is the composition-change demonstration for the
  new seam.
- **The test surface an inline dependency demands** — Tier-0 DST proves the
  *envelope protocol and its fail-closed/erase/rotation logic* against a
  deterministic in-memory `KeyService`; the OpenBao backend is validated at
  **Tier-1/Tier-2 against a real KMS** ([§11 ladder][s10]: "M6 — Tier 0–2 against a
  real KMS (OpenBao); fail-closed on KMS unavailability; crypto-erase-vs-hold
  tests"). Every real-KMS discovery is promoted to a **seeded DST regression**
  ([ADR-0009][a9]).

**Out of scope** — deferred to the milestone that actually retires its risk, the
seats kept open where retrofit is expensive:

- **A hardware/cloud-HSM-backed KEK custody path (PKCS#11 / KMIP against real
  HSM)** — ADR-0026 §4 *standardizes the custody seam* on PKCS#11 and KMIP, and M6
  defines the trait to that common denominator; but a production **hardware HSM**
  integration (and the OpenBao-HSM-backing maturity question, historically
  Vault-Enterprise-gated) is **not built** by M6. SoftHSM2 (BSD-2) is the optional
  *realistic-but-dev* PKCS#11 path (ADR-0026 §5). HSM hardening is an operational
  follow-on; the seam must not foreclose it (see Open questions).
- **The OpenStack Barbican backend** — ADR-0026 §3 names it the alternative behind
  the same trait, winning only where a provider already runs OpenStack. M6 builds
  the **default (OpenBao)** and the **dev stub**; Barbican is a *third*
  implementation that the seam must admit but M6 does not write. Its value is
  precisely as the second *production* backend that re-tests the no-vendor-leak
  claim — recorded as a candidate follow-on, not deferred to a numbered milestone.
- **Key-hierarchy depth for very large tenants** (a per-bucket intermediate key,
  ADR-0021) — M6 ships **per-tenant KEK + per-object/chunk DEK** (two levels). A
  third, per-bucket intermediate level is *forced* toward existence by ADR-0028 §4
  (DEK-level erasure precision), but **which** granularity and depth is an
  [OPEN] this milestone surfaces, not settles (see Open questions).
- **The retention / WORM *policy*** (which force wins, how a queued erasure runs on
  hold release) — [ADR-0028][a28] is **status: Proposed** and decides the
  *policy*; M6 builds only the **`destroy-KEK` gate mechanism** ADR-0026 §1
  already commits to (refuse while held) and **depends on** ADR-0028 for the
  precedence the gate enforces. This is a **named dependency**, flagged below, not
  an afterthought.
- **Cross-zone / cross-region KEK replication and multi-region key custody** →
  **M9 / M10**. Residency-pinning is single-region at M6; there is no second region
  at M6 to replicate a KEK across or to fail a key-custody domain over to.
- **True DEK rotation (re-encryption of bulk ciphertext)** — ADR-0021 §3 keeps this
  a "background custodian job, reserved for the rare cases that require it." M6
  builds **re-wrap rotation only**; bulk re-encryption is a later custodian slice.
- **Compression** (a sibling reserved `flags` bit and header-extension field,
  format spec §"Reserved for future use") — orthogonal to encryption; untouched.

### What carries over from M0–M5, unchanged

M6 adds a *new seam and a new client-side crypto stage*; it touches **neither the
storage primitives below nor the metadata/coordination contracts beside them**. The
audit confirms the following carry over verbatim:

- **The `MetadataStore` trait and its commit point** — the wrapped DEK and
  key-version id are **opaque bytes inside the inode/chunk JSON value** TiKV/redb
  already store verbatim (the same channel placement (M3) rides, `#[serde(default)]`,
  `crates/core/src/metadata.rs`). M6 adds **no keyspace** and **no trait method**:
  the commit that carries a chunk map already carries arbitrary value bytes, and a
  wrapped DEK is just more of them. The version-conditional CAS (`require` on the
  prior inode value, M4) is unaffected — provided the wrapped-DEK fields serialize
  byte-stably, exactly the constraint M4 already imposed.
- **The `ChunkStore` / `PlacementChunkStore` seam** — stays **deliberately dumb**:
  it "moves those bytes and verifies their integrity, but does not interpret them"
  (`crates/traits/src/lib.rs`, `ChunkStore` doc). D servers hold **ciphertext only**
  (ADR-0021 §2), which is exactly what relaxes their trust. M6 changes **what the
  payload bytes *are*** (ciphertext, not plaintext), not how they are stored,
  fetched, scrubbed, or reconstructed.
- **The fragment integrity story is untouched and was *designed* for this.** The
  payload checksum is computed over the **stored** bytes — "the ciphertext when a
  payload is encrypted … so integrity is verifiable without the decryption key"
  (`crates/chunk-format/src/codec.rs` module doc; spec §Checksums). Scrub, repair,
  reconstruction, and the `IntegrityFault` classification (`traits/src/lib.rs`) all
  operate on stored bytes and need **no change** — a D server or scrubber detects
  bit-rot on ciphertext without ever holding a key.
- **Erasure coding order** — the client encrypts **before** EC and decrypts
  **after** reconstruction (ADR-0021 §2): EC operates on ciphertext, so the
  any-*k* read path (`crates/core/src/read.rs`, `crates/core/src/erasure.rs`) is
  unchanged; only the bytes that enter the coder and leave the decoder change.
- **The M5 trust fabric** — the KMS is *authenticated against* M5's CA / identity
  (arc M6 "Needs M5"): the `server`-side KMS client dials OpenBao under the same
  mTLS / `PeerIdentity` posture M5 established (ADR-0036/0025). M6 **consumes** that
  fabric; it does not re-open it.
- **Coordination (L5), the custodian plane, the EC engine, the gRPC `ChunkStore`** —
  none cross the `KeyService` seam; all unchanged.

### The `KeyService` contract M6 introduces (its normative surface)

The trait is the milestone's central commitment, and ADR-0026 §1 fixes its surface.
M6 reproduces it **exactly** and adds nothing Transit-specific. It joins the other
seam traits in `crates/traits/src/lib.rs`, matching their established shape —
object-safe, `#[async_trait]`, `Send + Sync`, with a `BoxError` result channel
(the same `Result<T> = std::result::Result<T, BoxError>` alias the file already
defines) so backends surface their own detail and richer typed errors are a later
refinement pinned by an implementation. The normative operations:

- **generate-data-key** — return a fresh DEK **both** as **plaintext** (the client
  encrypts with it immediately) **and wrapped** under a *named per-tenant KEK*; the
  client persists only the **wrapped DEK + the key-version id**, never the
  plaintext past the encrypt. (ADR-0026 §1; ADR-0021 §1.)
- **unwrap** — a **wrapped DEK + its key-version** → the **plaintext DEK**, for the
  read path. (The only way ciphertext becomes readable.)
- **rotate-KEK** — re-wrap existing wrapped DEKs under the **new KEK version**
  **without touching bulk ciphertext** — the cheap, metadata-only rotation of
  ADR-0021 §3.
- **destroy-KEK** — **crypto-erase** (ADR-0021 §4); **MUST be refused while any
  object under that KEK is under a retention / legal hold** (ADR-0026 §1, the
  enforcement point of ADR-0028 §3).
- **per-tenant KEK namespacing** and **monotonic key-version ids** — a tenant's
  keys live in its own namespace; a rotation mints a strictly higher version id, so
  a stored `(wrapped DEK, key-version)` unambiguously names the KEK version that
  wrapped it.

Three invariants are **load-bearing across the whole trait** and are the things M6
exists to keep true:

1. **The trait MUST NOT expose KEK material.** No method returns, and no parameter
   accepts, a raw KEK. Wrap / unwrap / rotate execute **inside** the KMS
   (encryption-as-a-service). A `generate-data-key` returns a *DEK* (plaintext +
   wrapped); it never returns the KEK that wrapped it.
2. **Wyrd MUST NOT persist an unwrapped KEK**, anywhere — not in metadata, not on a
   D server, not in the DEK cache (which holds *DEKs*, not KEKs). This is what makes
   crypto-erase (destroy the KEK) a real guarantee.
3. **The KMS never sees plaintext bulk data.** Only DEKs and key-version ids cross
   the seam; the bulk bytes are encrypted client-side and the KMS handles *keys*,
   not *data*. (This is also why the trait is *not* "encrypt these bytes" — that
   would push bulk data through the KMS; it is "wrap/unwrap this key.")

The **honest limit** (ADR-0026 §4): conforming backends are **not feature-identical
below the trait** — rotation mechanics, audit-log shape, and latency differ — so the
surface is held to **what all conforming backends guarantee**, and anything richer
(a Transit-only batch endpoint, a Barbican-only KMIP attribute) stays **out** of
the trait.

### Mapping the contract onto OpenBao Transit

OpenBao's **Transit** secrets engine *is* the contract almost one-to-one (ADR-0026
§2), which is the operational reason it is the default. The mapping (verified
against the OpenBao Transit engine, the open fork of Vault's Transit taken from its
last MPL-2.0 release; **pin the OpenBao version and re-confirm the endpoint
surfaces and the HSM-backing maturity at build time** — see Open questions):

- **generate-data-key** → Transit **`datakey`** (`/transit/datakey/plaintext/<key>`),
  which returns *both* the plaintext DEK and its ciphertext (the wrapped DEK) in one
  call — exactly the contract's "plaintext + wrapped" return. The named key is the
  **per-tenant KEK**; Transit holds it and never emits it.
- **unwrap** → Transit **`decrypt`** (`/transit/decrypt/<key>`) of the wrapped DEK,
  which Transit decrypts *inside the engine* using the stored KEK and returns the
  plaintext DEK. The key-version id rides in Transit's ciphertext envelope
  (`vault:v<N>:…`), so the version is carried by the wrapped value itself.
- **rotate-KEK** → Transit **key rotation** (`/transit/keys/<key>/rotate`) mints a
  new KEK version; **`rewrap`** (`/transit/rewrap/<key>`) re-wraps an existing
  wrapped DEK under the **latest** version **without exposing the DEK plaintext** —
  the metadata-only rotation of ADR-0021 §3, native to Transit.
- **destroy-KEK** → Transit key **deletion / `min_decryption_version` advance**
  (configurable `deletion_allowed`), which renders ciphertext under the destroyed
  version undecryptable — crypto-erase. **The hold gate (ADR-0028) is enforced in
  Wyrd *above* this call**, never inside Transit: Wyrd refuses to issue the destroy
  while a hold is active.
- **per-tenant KEK namespacing** → a Transit **named key per tenant** (or an
  OpenBao namespace per tenant); **monotonic key-version ids** → Transit's own
  monotonic key versions.
- **encryption-as-a-service** → Transit **never returns the KEK** and **never sees
  bulk data**; only `datakey`/`decrypt`/`rewrap` of *keys* cross the seam. This is
  the property the trait was shaped to require, and Transit provides it natively.

The audit log Transit emits is the per-deployment audit surface; the trait does
**not** expose it (an example of "richer stays out of the trait").

### Composition, not refactor — the thesis on a new seam, with the honest count

ADR-0026 §4 claims "OpenBao ↔ Barbican ↔ a raw HSM is a composition change in
`server`, not a refactor." M6 is the first test of that claim, and — unlike M4,
where the consumers already existed and the audit was "does the seam already not
leak" — M6 **adds a new consumer stage** (client-side crypto) at the same time as
the seam. So the honest count has two parts:

- **The seam itself is a clean composition point.** `server` already concentrates
  *all* concrete wiring — `crates/server/src/cli.rs` constructs `RedbMetadataStore`,
  `FsChunkStore`, `MemCoordination`, `GrpcChunkStore` and is the *only* crate that
  names a concrete (`grep` confirms no concrete backend type appears in `core` or
  `custodian`). Adding a `file-stub | OpenBao` selector is the **same** shape as M4's
  `redb | tikv` selector: a new construction site plus the selection mechanism,
  confined to `cli.rs`. Today there is **no** `KeyService`, no KMS client, and no
  encryption anywhere in `crates/` (`grep -rn "KeyService\|kms\|OpenBao\|envelope"
  crates/` returns nothing outside this proposal) — so M6 builds the seam, not a
  refactor of an existing one.
- **The new client-crypto stage is genuinely new code in `core`, and that is where
  the weight is.** The write path (`crates/core/src/write.rs`) today encodes
  fragments via `wyrd_chunk_format::{encode, FragmentHeader}` with **no encryption
  stage** — `FragmentHeader::new_v1` sets `flags: 0`, `encryption_scheme:
  EncryptionScheme::None` (`crates/chunk-format/src/header.rs`), and the v1 codec
  **refuses** a fragment with `FLAG_ENCRYPTED` set (`codec.rs`:
  `EncryptedPayloadUnsupported`). M6 inserts an **encrypt-before-EC** stage on write
  and an **unwrap-then-decrypt-after-reconstruct** stage on read
  (`crates/core/src/read.rs`), threads a `&dyn KeyService` (and the bounded DEK
  cache) through both, and carries the wrapped DEK + key-version in `ChunkRef` /
  `InodeRecord` (`crates/core/src/metadata.rs`). This is **additive** — it is
  gated by per-tenant policy and **off by default in dev** (ADR-0021 §5), so the
  unencrypted path stays byte-for-byte the M0–M5 path when the policy is off — but
  it is **not** a one-line composition switch; it is a new pipeline stage behind a
  policy flag. Naming this honestly (as M4 named its "~8 sites, not one line") is
  the point: **the seam is a composition point; the crypto stage is new
  consumer-side code, correctly placed in `core`, not in the trait.**

The crypto code stays in the **client library** (`core`), exactly as ADR-0021 §2
requires ("crypto stays in the client library; storage stays dumb"), so the seam's
honesty (`ChunkStore` interprets nothing) is preserved.

### Deployment: the KMS as a stateful, audited, highly-available tier

M6 introduces the system's first **inline, on-the-data-path** external service, and
[ADR-0010][a10] constrains how it lands the same way it constrained TiKV. The KMS
runs as a **stateful, highly-available, audited tier** brought up from **`deploy/`
artifacts outside the Cargo workspace** ([ADR-0010][a10]; the structural guard
"makes it hard for orchestrator coupling to sneak in"). The posture, drawn from
ADR-0026 §6 and §11:

- **HA is a hard requirement for encrypted tenants** (ADR-0026 consequences): the
  KMS is on the read/write path, so a single-instance KMS is a single point of
  unavailability for every encrypted read. M6 ships an **HA OpenBao** topology
  (multi-node with its own storage backend) in `deploy/`, not a single binary —
  this is the deployment difference from the dev stub.
- **KEK backups kept *independent of Wyrd*** (ADR-0026 consequences; §11): a lost
  KEK is catastrophic, so KEK custody is backed up out-of-band, under the operator's
  control, not inside a Wyrd backup.
- **KEK custody pinned to the tenant's residency region** (crypto-residency,
  ADR-0018, §8.8): a tenant's KEK does not leave its region; in a single-region M6
  deployment this is a one-region pin, generalizing to per-tenant-region custody at
  M9/M10.
- **Authenticated against the M5 fabric**: the `server` KMS client dials OpenBao
  under M5's mTLS / `PeerIdentity` (ADR-0036/0025) — the KMS is a *peer* in the
  trust fabric, not an unauthenticated network endpoint. Time-bound key operations
  (scheduled rotation, hold-expiry checks) evaluate against the **trusted clock**
  ([ADR-0024][a24]; ADR-0026 §6, ADR-0028 hold-expiry timing).
- **Orchestrator-agnostic**: "Kubernetes available, never required," no crate
  couples to an orchestrator API, OpenBao discovered through config/`deploy/`
  (ADR-0010). M6 ships a **docker-compose** OpenBao for CI/eval; an HA Helm/operator
  chart is later.
- **The dev / single-binary profile runs *no* KMS** — the file-backed stub only
  (ADR-0026 §5, ADR-0014), with SoftHSM2 (BSD-2) as the optional realistic PKCS#11
  path. Encryption is off by default there (ADR-0021 §5).
- **Licence/governance is a standing selection gate** (ADR-0026 §7; ADR-0003): the
  `deploy/` OpenBao pin records the licence/governance facts and they are
  **re-confirmed at adoption** — the deployed-service analog of `cargo-deny`. Vault
  is **rejected** on BUSL-1.1 (ADR-0003 `deny.toml` BSL denial); OpenBao (MPL-2.0,
  LF-governed) and Barbican (Apache-2.0, OpenInfra) both pass the control-resilience
  test (ADR-0003 §2).

### DST and tests (the heart of M6)

[ADR-0009][a9] remains the correctness authority, and M6 reuses M4's principled
split: **DST proves the *protocol*; the real KMS proves the *backend*.** A real KMS
never goes inside the deterministic simulator (it breaks seed determinism, exactly
as a containerized TiKV would); the `KeyService` seam is what lets a **deterministic
in-memory `KeyService`** drive the simulator instead.

**Tier-0 — deterministic simulation (the spine).** Against a deterministic
in-memory `KeyService` (and the deterministic in-memory/redb metadata + chunk
backends), DST proves the **envelope protocol and its hard edges**, seed-reproducible
and single-threaded:
- **Round-trip** — encrypted write then read returns the original plaintext;
  ciphertext on the (fake) D server is *not* the plaintext; the fragment header
  carries `FLAG_ENCRYPTED` + a non-`None` `encryption_scheme`.
- **Fail-closed** — with the KMS modelled **unavailable**, an uncached-DEK read
  **errors** (no plaintext, no ciphertext leak) and a write **refuses**; and —
  the load-bearing assertion — the **still-encrypted data's durability is
  untouched** (the ciphertext, wrapped DEK, and key-version remain on the fake
  store; the outage is *unreadability*, not *loss*). When the KMS returns, the read
  succeeds.
- **Crypto-erase** — destroy-KEK makes subsequent unwrap (and thus read) fail
  permanently; and the **hold gate**: destroy-KEK is **refused** while a modelled
  hold is active, the held object survives, and on hold release the queued erasure
  proceeds — the ADR-0028 §3 precedence, verifiable under DST exactly as ADR-0028
  anticipates ("a held object survives an erasure request; on hold release the
  queued erasure executes; a clock rollback does neither early"). The clock-rollback
  half drives the **`ManualClock`** seam (ADR-0024) so a skewed clock can neither
  release a hold early nor fire a queued erasure prematurely.
- **Rotation = re-wrap** — rotate-KEK mints a higher key-version, re-wraps the
  stored DEKs, and **does not change the bulk ciphertext bytes** (asserted by
  comparing stored fragments before/after); a read after rotation still succeeds.
- **Bounded DEK cache** — a cache hit does **not** consult the KMS; a cache entry
  past its bounded lifetime **does** re-consult; the bound is honoured (no unbounded
  cache).

**Tier-1 — software-defined faults against a *real* OpenBao.** The M6-specific
evidence the simulator structurally cannot give: that the in-memory `KeyService`
model **matches a real KMS**. On the `deploy/` HA OpenBao under `tc netem` /
`iptables` partitions / process pauses / `libfaketime` skew: end-to-end encrypted
PUT/GET; **fail-closed under a real KMS partition** (read errors, ciphertext
durability intact, recovery on heal); a real Transit **`rewrap`** rotation and a
real **destroy** crypto-erase; the **datakey/decrypt/rewrap** mapping exercised
against the live engine; and confirmation that **KEK material never crosses the
seam** (the client only ever holds DEKs). Every real-KMS behaviour the in-memory
model did not capture (a Transit latency edge, a key-version envelope quirk, a
partition timing) is **promoted to a seeded DST regression** where the trait exposes
it — the FoundationDB/TigerBeetle compounding loop (ADR-0009).

**Tier-2 — first real hardware (single owned machine).** Real OpenBao on real NVMe
under real fsync — honest single-node KMS latency and the *real* cost the bounded
DEK cache is sized against (a single failure domain, so it proves real-silicon
behaviour, not failure-domain independence).

**Tier-3 — multi-region — does not begin until M9.** Single-region M6 has no
cross-region key custody or KMS-region failover to drill.

> **Numbering note.** This proposal uses the architecture **realism ladder** (Tier 0
> DST · Tier 1 software faults · Tier 2 single machine · Tier 3 multi-region),
> matching [§13.4][s10]'s "M6 — Tier 0–2 against a real KMS (OpenBao)" and [0007][p7].
> The CI/code taxonomy uses a different "Tier-1/Tier-2" scheme; this document means
> the realism ladder throughout.

### Crate touch-points

Building on the workspace as it stands after M5 (`chunk-format`, `chunkstore-fs`,
`chunkstore-grpc`, `coordination-mem`, `core`, `custodian`, `dst`, `metadata-redb`,
`proto`, `server`, `testkit`, `traits`):

- **`traits`** — **add** the `KeyService` trait (object-safe, `#[async_trait]`,
  `Send + Sync`, `BoxError` result): generate-data-key, unwrap, rotate-KEK,
  destroy-KEK, plus the wrapped-DEK / key-version value types. The trait MUST NOT
  expose KEK material. This is the **only** change to the seam crate, and the
  milestone's central commitment.
- **`keyservice-file`** (**new**) — the dev stub `impl KeyService`: keys in
  cleartext on local disk, **unmistakably non-production** (ADR-0026 §5). Deps
  `traits` only; never `core` (ADR-0016).
- **`keyservice-openbao`** (**new**) — `impl KeyService` over OpenBao Transit
  (`datakey`/`decrypt`/`rewrap`/rotate/destroy); the per-tenant KEK namespacing and
  monotonic key-version handling; dials OpenBao under M5 mTLS. Deps `traits` + an
  HTTP client + `tokio`; never another concrete (ADR-0016).
- **`core`** — **add** the encrypt-before-EC / decrypt-after-reconstruct stages on
  the write/read paths, threaded over `&dyn KeyService` + the bounded DEK cache;
  carry the wrapped DEK + key-version in `ChunkRef`/`InodeRecord` (`#[serde(default)]`,
  off-by-default policy preserves the unencrypted path byte-for-byte); set
  `FLAG_ENCRYPTED` + `encryption_scheme` on encrypted writes. The **bounded DEK
  cache** lives here (the client side), not in the trait.
- **`chunk-format`** — **add** the AEAD `EncryptionScheme` code point(s) and the
  decode path for an encrypted payload (today `decode` *refuses* `FLAG_ENCRYPTED`
  with `EncryptedPayloadUnsupported`). The reserved hooks (`flags` bit 0,
  `encryption_scheme`, the header-extension region for nonce + key id) **already
  exist** (`header.rs`, `codec.rs`, spec v1.md §"Reserved for future use") — M6
  *activates* a reserved scheme, an **additive** change that **MAY be made without a
  format-version increment** (spec §Versioning: a new `encryption_scheme` code point
  is backward-compatible), accompanied by **new conformance vectors**.
- **`server`** — **add** the `file-stub | OpenBao` `KeyService` selector at the
  single wiring point in `cli.rs`; wire the per-tenant encryption policy; default to
  the file stub (dev) / off (ADR-0014/0021). The composition-change demonstration.
- **`dst`** — a deterministic in-memory `KeyService` model; the round-trip /
  fail-closed / crypto-erase-vs-hold / rotation / DEK-cache property suites; new
  seeds. Drives the `ManualClock` for the hold-expiry-vs-clock-rollback case.
- **`testkit`** — a real-OpenBao fault seam (partition/latency/pause) for the
  Tier-1 runs.
- **`xtask`** — an OpenBao integration runner; wire `deploy/` docker-compose OpenBao
  into CI; conformance-vector generation for the activated `encryption_scheme`.
- **`deploy/`** — an HA OpenBao topology (docker-compose for CI/eval; HA
  Helm/operator deferred), with the licence/governance pin recorded (ADR-0026 §7).
- **deps** — an OpenBao/HTTP client (+ `tokio` tree) and an AEAD crate (e.g.
  `aes-gcm` or `chacha20poly1305`, the AEAD choice an ADR-0021 [OPEN]); **confirm
  every new crate under the `cargo-deny` allowlist** (ADR-0003) and **the OpenBao
  service under the deployed-service control-resilience gate** (ADR-0026 §7).

## Alternatives considered

- **Put a real KMS inside DST (containerized) as the correctness authority:**
  **rejected** — ADR-0009 forbids using a real environment for correctness DST
  already covers, and a containerized KMS breaks seed determinism. The KMS is
  validated at Tier 1–2 as a *complement*; DST drives a deterministic in-memory
  `KeyService`.
- **"Encrypt-these-bytes" as a KMS endpoint (push bulk data through the KMS):**
  **rejected** — it violates "the KMS never sees plaintext bulk data" (ADR-0026 §1),
  puts every byte on the inline dependency, and would make the KMS a bulk-data
  bottleneck. The trait wraps/unwraps **keys**; the client encrypts **data**
  (ADR-0021 §2).
- **Vault as the KMS:** **rejected** — BUSL-1.1 since 2023, denied by `deny.toml`
  and caught by ADR-0003's control-resilience lens; the KMS-shaped repeat of the
  CockroachDB case (ADR-0026 §2). OpenBao (the LF-governed fork from the last open
  release) is the un-rug-pullable replacement.
- **Make the trait Transit-shaped (a `datakey`/`rewrap` surface):** **rejected** —
  it would leak a vendor API and force a Barbican or raw-HSM adapter to *emulate*
  Transit, breaking the composition-change claim (ADR-0026 §4). The trait is the
  **common denominator** — the envelope operations — and nothing more.
- **Hold a long-lived plaintext KEK in Wyrd (skip per-read unwrap):** **rejected** —
  Wyrd persisting an unwrapped KEK hollows out crypto-erase (the key escaped) and
  violates ADR-0026 §1. Wyrd caches **DEKs** (bounded), never KEKs.
- **Return ciphertext (or a fabricated key) when the KMS is unreachable
  (fail-open):** **rejected** — it would leak ciphertext or corrupt reads, inverting
  the risk M6 retires. The path **fails closed** (errors), and the *durability* of
  the still-encrypted data is independently preserved (ADR-0021; ADR-0026 §6) — the
  same fail-closed-without-fabrication discipline ADR-0024 demands of the clock.
- **Encryption on by default (including dev):** **rejected** — ADR-0021 §5 / ADR-0026
  §5 keep it a per-tenant policy, **off by default in dev**; the dev/single-binary
  profile runs no KMS. M6 **adds** an encrypted path; it does not make the
  unencrypted path disappear.
- **Bump the on-disk format to v2 for encryption:** **rejected** — activating a
  reserved `encryption_scheme` code point is a **backward-compatible addition** a
  conforming v1 reader handles by rejecting code points it does not recognize (spec
  §Versioning); it needs **new conformance vectors**, not a version increment.
- **Build Barbican and/or a hardware HSM in the same milestone:** **deferred** —
  Barbican (ADR-0026 §3) and a real HSM (ADR-0026 §4) are *further* implementations
  behind the **same** trait; M6's job is to prove the seam with the default
  (OpenBao) + the dev stub. Barbican is a candidate second *production* backend;
  the HSM seam (PKCS#11/KMIP) is *defined* but not hardware-hardened.
- **Mint a new ADR for the KMS backend:** **not minted** — ADR-0026 already decides
  the `KeyService` contract and the OpenBao default, and ADR-0021 decides envelope
  encryption; M6 is their first *implementation*, not a new decision. (ADR-0026 and
  ADR-0021 are status: Proposed; M6 does not flip them — it builds against them, as
  M4 built against the Accepted ADR-0008.)

## Graduation criteria (definition of done)

- **The `KeyService` trait lives in `traits`** — object-safe, async, `Send + Sync`,
  surfacing **exactly** the ADR-0026 §1 envelope operations and **nothing
  Transit-specific**; the trait **never exposes KEK material**.
- **Two backends ship behind it** — a file-backed dev stub (unmistakably
  non-production) and an OpenBao Transit backend — so the trait is born pinned by
  two implementations.
- **Encrypted round-trip holds end-to-end** — an encrypted write then read returns
  the original plaintext; the D server holds **ciphertext only**; the fragment
  header carries `FLAG_ENCRYPTED` + a non-`None` `encryption_scheme`; the bulk path
  encrypts **before** EC and decrypts **after** reconstruction.
- **KEK material never leaves the KMS and Wyrd never persists an unwrapped KEK** —
  asserted at Tier-1 against real OpenBao; the client only ever holds DEKs (bounded
  cache).
- **Fail-closed without endangering durability** — a KMS-unavailable read **errors**
  (no plaintext, no ciphertext leak) and a write **refuses**, while the
  already-stored ciphertext + wrapped DEK + key-version **remain durable**; recovery
  on KMS heal.
- **Crypto-erase + rotation are correct and gated** — destroy-KEK renders data
  unrecoverable and is **refused while a hold is active** (ADR-0028 §3 precedence);
  rotate-KEK is **re-wrap only** (bulk ciphertext byte-unchanged); the
  hold-expiry-vs-clock-rollback case holds under `ManualClock`.
- **Residency-pinned KEK custody** — a tenant's KEK does not leave its residency
  region (ADR-0018, §8.8).
- **The seam is a `server`-crate composition change** — the `file-stub | OpenBao`
  selector is confined to `cli.rs`; the new crypto **stage** lives in `core` (the
  client library), not the trait; `chunk-format`'s change is an additive reserved
  code point with new conformance vectors; encryption is **off by default in dev**
  and the unencrypted path is byte-for-byte the M0–M5 path.
- **Tier-0 DST green and seed-reproducible** (round-trip / fail-closed /
  crypto-erase-vs-hold / rotation / DEK-cache; seeds committed); **Tier-1 against
  real OpenBao + Tier-2 single-node** green; every real-KMS discovery promoted to a
  seeded DST regression.
- **The KMS stands up from `deploy/`** as an HA, audited tier, authenticated under
  the M5 fabric, with **KEK backups independent of Wyrd** and **no crate importing
  an orchestrator API**.
- `fmt`/`clippy` clean; `Cargo.lock` updated; **`cargo-deny` passes** with the new
  crates **and** the OpenBao service re-confirmed under the control-resilience gate
  (ADR-0026 §7).

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M6** milestone (branch
`feat/m6.<n>-<slug>`, commit subject `feat(<crate>): … (M6.<n>, #<issue>)`):

1. **M6.1 — `KeyService` trait + file-backed dev stub.**
   `feat(traits): add KeyService envelope contract (M6.1, #<issue>)` /
   `feat(keyservice-file): file-backed dev stub (M6.1, #<issue>)` — the trait
   (generate-data-key / unwrap / rotate-KEK / destroy-KEK, per-tenant KEK
   namespacing, monotonic key-version ids) in `traits` (deps = `traits` only); the
   stub `impl` (keys cleartext on local disk, unmistakably non-production). *DoD:*
   the trait is object-safe/async/`Send + Sync`, exposes no KEK material, and a
   shared `KeyService` **conformance suite** (round-trip wrap/unwrap, version
   monotonicity, per-tenant isolation) passes against the stub.
2. **M6.2 — OpenBao Transit backend + CI OpenBao.**
   `feat(keyservice-openbao): OpenBao Transit KeyService (M6.2, #<issue>)` — the
   backend (datakey/decrypt/rewrap, key versioning) behind the trait; a throwaway
   OpenBao in `deploy/` for CI; the **bounded DEK cache** (bound mandatory) and
   **fail-closed on KMS unavailability**; the licence/governance gate recorded
   (ADR-0003/0026 §7). *DoD:* OpenBao passes the **same** conformance suite the stub
   passes; a forced KMS partition makes an uncached read error (no ciphertext leak)
   with ciphertext durability intact; `cargo-deny` green on the new deps and the
   OpenBao service re-confirmed.
3. **M6.3 — envelope encryption on the read/write path.**
   `feat(core): envelope encryption on read/write path (M6.3, #<issue>)` (+
   `feat(chunk-format): activate AEAD encryption_scheme (M6.3, #<issue>)`) — write
   path encrypts-before-EC under a fresh DEK and stores the **wrapped DEK +
   key-version**; read path **unwraps** and decrypts-after-reconstruct; set
   `FLAG_ENCRYPTED`/`encryption_scheme`; **off by default in dev** (ADR-0021 §5).
   *DoD:* encrypted round-trip returns the original bytes; the D server holds
   ciphertext only; new chunk-format **conformance vectors** for the activated
   scheme; the unencrypted path is byte-for-byte unchanged when policy is off.
4. **M6.4 — crypto-erase, rotation, residency.**
   `feat(core): crypto-erase and re-wrap rotation (M6.4, #<issue>)` — destroy-KEK
   gated on retention/hold state (**depends on ADR-0028** precedence — flagged);
   rotate-KEK = re-wrap, **no bulk re-encrypt**; residency-pinned KEK custody
   (ADR-0018/§8.8); hold-expiry timing against the trusted clock (ADR-0024). *DoD:*
   destroy-KEK makes data unrecoverable **and is refused while a hold is active**;
   rotation leaves bulk ciphertext byte-unchanged; the clock-rollback case neither
   releases a hold nor fires a queued erasure early (under `ManualClock`).
5. **M6.5 — backend selection in `server` + HA deployment + the test campaign.**
   `feat(server): file-stub | OpenBao KeyService selection (M6.5, #<issue>)` — the
   single wiring point in `cli.rs` (choice MAY differ per deployment); the HA
   OpenBao posture in `deploy/` (KEK backups independent of Wyrd; authenticated
   under M5); DST property suites (round-trip / fail-closed / crypto-erase /
   rotation) seed-reproducible; **Tier-1/Tier-2 against real OpenBao**. *DoD:*
   `server` runs identically on the stub (dev) and OpenBao (prod) by config; the
   diff outside the two `keyservice-*` crates is confined to `server` + `core`'s
   crypto stage + test/deploy scaffolding; DST green and seed-reproducible; Tier-1
   integration + fail-closed + crypto-erase-vs-hold green; a bug-finding seed
   committed; no crate imports an orchestrator API.

(M6 is sized like M4 — two new backend crates, a new client-crypto stage in `core`,
an additive format activation, a composition selector, and the real-service test
campaign — not a new plane. Slices M6.1–M6.3 are the implementation spine; M6.4–M6.5
are the compliance-grade gating and the proof. M6.1 can begin against the tail of M5
once the trait surface is confirmed and the M5 mTLS fabric is available for the
OpenBao dial in M6.2.)

## Backward compatibility

M6 lands after the M4 release point, so the compatibility duties M4 opened apply —
narrowly:

- **The `KeyService` trait** — **new**, born pinned by two implementations (stub +
  OpenBao). After M6 it is a real internal contract; a future change is a trait
  evolution with both backends to carry. (Pre-1.0, still no *published* API.)
- **The metadata model / keyspace** — **unchanged and additive**: the wrapped DEK +
  key-version are `#[serde(default)]` fields inside the existing inode/chunk JSON
  value (the same channel placement (M3) rides), so a pre-M6 record decodes with the
  fields absent (= "not encrypted") and the read takes the plaintext path. **No
  keyspace, no new trait method, no data migration.** The version-conditional CAS
  (M4) is preserved, the new fields serializing byte-stably.
- **The on-disk *fragment* format** — **additively extended**: M6 activates a
  reserved `encryption_scheme` code point and the reserved header-extension region
  (nonce + key id), a **backward-compatible** change a conforming v1 reader handles
  by **rejecting** a code point it does not recognize (spec §Versioning; codec
  `EncryptedPayloadUnsupported`). It is accompanied by **new conformance vectors**
  and does **not** force a `format_version` increment. A pre-M6 (unencrypted) reader
  still reads pre-M6 fragments; an encrypted fragment is correctly *refused* by a
  reader that lacks the key, never misread. The format stays **v0/unstable** until
  its independent gate fires (a second reader **or** a sustained fault-injection
  run, spec §Versioning) — M6's Tier-2 sustained encrypted run is a **candidate
  `v1` trigger**, recorded, not decided.
- **The deployment surface** — extended with the KMS tier: the `deploy/` HA OpenBao
  topology becomes a documented part of the encrypted-tenant production shape.
- **Reserved seats honored** — the **key-hierarchy depth** seat (a per-bucket
  intermediate key, ADR-0021), the **HSM/PKCS#11/KMIP** custody seat (ADR-0026 §4),
  and the **Barbican** backend seat (ADR-0026 §3) all remain expressible behind the
  trait; M6 builds none of them but forecloses none.

## Open questions

- **DEK-cache lifetime values.** ADR-0026 §6 makes the *bound* mandatory and the
  *value* per-deployment. M6 ships a bounded default and a knob; the right
  default(s) are a **measurement** against the Tier-2 KMS-latency numbers, not a
  decision to take now (ADR-0021/0026 [OPEN]).
- **Key-hierarchy depth for very large tenants.** ADR-0021 left a per-bucket
  intermediate key [OPEN]; **ADR-0028 §4 now *forces* DEK-level (per-object /
  per-bucket) erasure granularity** so a single held object cannot block all
  erasure under a per-tenant KEK. M6 ships **per-tenant KEK + per-object/chunk DEK**;
  whether a **per-bucket intermediate** level is warranted (and the re-wrap
  accounting it grows) is surfaced here, settled by the erasure-precision work.
- **The AEAD cipher and the per-object-vs-per-chunk unit.** ADR-0021 §5 leaves the
  AEAD (AES-256-GCM vs XChaCha20-Poly1305) and the encryption unit [OPEN], "settled
  with the format work." M6 picks one to ship the encrypted path and records the
  choice as the activated `encryption_scheme`; the alternative stays a reserved code
  point.
- **The precise PKCS#11 / KMIP profile and OpenBao HSM-backing maturity.** ADR-0026
  §4 standardizes the custody seam on PKCS#11 + KMIP; the **exact profile** and
  **OpenBao's HSM-backing maturity for the open fork** (historically
  Vault-Enterprise-gated) must be **re-verified at adoption** — M6 defines the seam
  to the common denominator and ships SoftHSM2 as the dev path, but does not harden
  a hardware HSM.
- **Retention / WORM-vs-crypto-erase precedence (the named dependency).** The
  `destroy-KEK` hold gate (ADR-0026 §1) enforces a **policy** that **ADR-0028
  (Proposed) decides** — an active hold is absolute, a right-to-erasure against held
  data is *queued* not refused, and runs on hold release (ADR-0028 §1–2). M6 builds
  the **mechanism** (refuse-while-held) and **depends on ADR-0028** for the
  precedence; if ADR-0028 is not yet Accepted when M6.4 lands, the gate is built to
  its stated shape and the dependency is recorded.
- **Licence / governance as a standing selection gate.** ADR-0026 §7 requires the
  KMS's licence/governance to be **re-confirmed at adoption and watched thereafter**
  — the deployed-service analog of `cargo-deny`. M6 records OpenBao's MPL-2.0 /
  LF-governance facts in `deploy/` and re-verifies them at the pin; how that watch is
  operationalized (a periodic check, an alert) is an operational [OPEN].
- **`encryption_scheme` activation as a `v1`-stamping trigger.** M6's sustained
  real-KMS encrypted fault-injection run is a candidate trigger for stamping the
  on-disk format `v1` (spec §Versioning); recorded, not decided — consistent with
  [0007][p7].

[p1]: ../accepted/0001-milestone-0-walking-skeleton.md
[p2]: ../accepted/0013-implementation-arc-rescoped.md
[p3]: ../accepted/0003-milestone-1-erasure-coding.md
[p4]: ../accepted/0004-milestone-2-networked-d-servers.md
[p5]: ../accepted/0005-milestone-3-custodians.md
[p7]: ./0007-milestone-4-production-metadata-backend.md
[s4]: ../../architecture/04-solution-strategy.md
[s5]: ../../architecture/05-building-block-view.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s9]: ../../architecture/09-build-order-and-roadmap.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a2]: ../../adr/0002-spec-first-on-disk-format-only.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a6]: ../../adr/0006-etcd-for-coordination.md
[a8]: ../../adr/0008-tikv-metadata-and-pluggable-backends.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a14]: ../../adr/0014-single-binary-dev-only.md
[a16]: ../../adr/0016-monorepo-and-crate-structure.md
[a18]: ../../adr/0018-reserve-hooks-for-hyperscale-identity-consumer.md
[a19]: ../../adr/0019-chunk-format-layout.md
[a21]: ../../adr/0021-encryption-at-rest-and-key-management.md
[a24]: ../../adr/0024-clock-and-time-source-trust.md
[a25]: ../../adr/0025-internal-service-to-service-trust.md
[a26]: ../../adr/0026-key-service-and-kms-backend-selection.md
[a28]: ../../adr/0028-erasure-versus-retention-precedence.md
[a36]: ../../adr/0036-internal-ca-step-ca-spire.md
