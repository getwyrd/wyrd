---
created: 22.06.2026 19:45
type: proposal
status: draft
author:
tracking-issue:
tags:
  - proposal
  - lifecycle
  - retention
  - versioning
  - trash
  - worm
  - compliance
  - legal-hold
  - security
---
# Proposal: Object lifecycle and retention — versioning, trash, lifecycle, and compliance hold (WORM)

> Draft. Captures the object-lifecycle model surfaced while reviewing security, reliability, and
> Drive-product feature parity. The erasure-vs-retention precedence (Facet D / §4) is the
> substantive decision and is flagged for its own ADR.

## Motivation

A provider building a Drive-class product on Wyrd needs a coherent answer to one question seen from
four angles: **which versions of an object exist, how long they are retained, who may delete them,
and when destruction becomes irreversible.** Each angle is a feature users and regulators expect:

- **Version history** — keep prior versions, restore one (OneDrive / Google Drive / S3 versioning).
- **Trash / recycle bin** — a deleted file is recoverable for a grace window, not gone instantly.
- **Lifecycle policies** — auto-transition cold data to a cheaper tier, auto-expire stale data
  (S3 Lifecycle).
- **Compliance hold (WORM)** — specific objects *cannot* be deleted or modified until a retention
  period expires or a legal hold is released, **even by the tenant admin or the operator**
  (S3 Object Lock, SEC 17a-4).

Wyrd today has only the **terminal** end of this story — crypto-erase and a right-to-erasure path
(ADR-0021 §4) — and the `meta:version` counter, which is a *consistency* fence (ADR-0015), **not** a
retained version history. Everything between "an object is written" and "its bytes are crypto-erased"
is undesigned.

**Why these belong in one proposal.** They are not four features; they are four views of a single
model, sharing the same machinery: the metadata version record, the commit point, the custodian GC,
crypto-erase, and the trusted clock (ADR-0024). Designed piecemeal they will produce **conflicting
rules** — a lifecycle "expire after 90 days" silently destroying data under a legal hold, or trash
auto-purge reclaiming a held version. The correctness of the whole rests on a single **precedence
ordering**, which only a unified model can state. Now is the moment because all four touch the
metadata model and GC being built in M2–M3, and because the data is already write-once at the
fragment level — so this is largely *composition* on existing parts, not new storage mechanism.

## Design

### The shared model

Every object has a chain of **versions**; at most one is *current*, the rest *non-current*. Each
version's metadata record (the inode / version, ADR-0008; namespace policy in L2, ADR-0020) carries:

- **lifecycle state** — `current` · `non-current` · `trashed` · `pending-erase`.
- **retention** — `retain_until` (a trusted-time bound) and `legal_hold` (boolean, no time bound).
- **storage tier** — hot / cold, driven by lifecycle policy and placement (cost-tier, §5).

Four **shared enforcement points** carry all four facets — the same spine the compliance-hold work
already defined:

1. **Commit-point enforcement (L4).** Any transition that removes or replaces a version is a
   conditional commit (ADR-0015) gated on the version's state and retention. **Fails closed**: if
   state cannot be evaluated, the mutation is refused.
2. **GC honours retention (the custodian, L4).** The "never reclaim a referenced fragment" invariant
   extends to **"never reclaim a fragment a retention rule still protects."** The reclamation path
   checks lifecycle state, `retain_until`, and `legal_hold` before reclaiming.
3. **Crypto-erase is the terminal state (KMS, ADR-0026).** The only irreversible deletion is KEK /
   DEK destruction, and it is gated (§4 below).
4. **Trusted clock (ADR-0024).** Every time bound — `retain_until`, trash grace, lifecycle age — is
   evaluated against monotonic, authenticated time, fail-closed on an implausible clock.

### Facet A — Version history

Versioning is a **per-bucket / per-prefix opt-in** (S3 semantics). With it on, an overwrite creates a
new current version and demotes the prior to non-current rather than replacing it; a delete creates a
delete-marker. Non-current versions are retained per a policy (keep last *N*, and/or keep for a
duration), and the custodian prunes the excess — **subject to enforcement points 1–2**, so a
non-current version under a hold is never pruned. Restore promotes a non-current version to current
(a metadata mutation; the fragments already exist).

### Facet B — Trash / soft delete

A user delete is **soft by default** (configurable per tenant): the object moves to `trashed` and is
recoverable for a **grace window**, after which the custodian moves it to `pending-erase`. This reuses
the reader-safe-grace machinery the GC already has for orphans (the difference is the grace is a
policy duration, not just the reader-drain window). Restore-from-trash is a metadata mutation. A hard
delete (skip trash) is a privileged, audited operation — and still refused under a hold.

### Facet C — Lifecycle policies

Per-bucket / per-prefix rules, evaluated by the custodian against trusted time (ADR-0024):

- **Transition** — move a version's fragments to a colder tier after an age threshold (composes onto
  placement's cost-tier axis, §5; transparent to the client, the data-model discussion earlier in the
  review). Read latency of cold tiers is the operational tradeoff, not a correctness one.
- **Expiration** — after an age threshold, route a version to `trashed` (then the trash grace applies)
  — expiry is a *soft* delete, never a direct erase, and **never fires on a held version**.

### Facet D — Compliance hold (WORM)

The strongest retention, with its own enforcement detail. Two attributes (already in the shared
model): `retain_until` and `legal_hold`, in two modes mirroring S3 Object Lock:

- **Governance mode** — retention may be shortened / removed by a principal holding a distinct
  *compliance-bypass* permission.
- **Compliance mode** — retention cannot be shortened or removed by anyone (tenant admin or operator
  included) until `retain_until` passes. Legal hold is independent and indefinite.

A per-bucket / per-prefix **default retention** is applied at commit. Enforcement rides points 1–4,
with one rule that is the substantive decision of this proposal:

**Crypto-erase precedence (§4).** A KEK / DEK MUST NOT be destroyed while any object encrypted under
it is under an active hold — the `KeyService` `destroy-KEK` operation is refused (foreshadowed in
ADR-0026 §1). So **an active legal hold or unexpired retention beats crypto-erase, and beats
right-to-erasure (GDPR), until released or expired; on release, pending erasure proceeds.** This
precedence warrants ratification in its own ADR. (A single per-tenant KEK means one held object pins
the whole tenant's KEK against rotation/erasure — an argument for finer key granularity, the `[OPEN]`
key-hierarchy-depth question in ADR-0021.)

### The precedence ordering (the reason these are one proposal)

The facets are reconciled by a single, total ordering applied at enforcement points 1–2:

1. **An active hold (legal hold or unexpired compliance retention) is absolute** — no other rule
   (version pruning, trash purge, lifecycle expiry, user delete, crypto-erase) may remove or destroy a
   version it protects.
2. Below holds, **version pruning, trash auto-purge, and lifecycle expiry operate only on non-held
   versions**, each bounded by its own policy (keep-N, grace window, age).
3. **Every terminal destruction is crypto-erase**, gated by rule 1 via the KEK-destroy refusal.

Stated once, this prevents the conflicting-rules trap (lifecycle expiry vs. legal hold, trash purge
vs. retention) that piecemeal designs fall into.

### API surface (L1) and audit

- **Versioning & trash** — the S3 versioning API (`GetObjectVersion`, list versions, delete-marker
  semantics) and a trash/restore surface in the WebDAV/Drive API and native SDK.
- **Lifecycle** — the S3 Lifecycle configuration API per bucket.
- **Compliance hold** — S3 Object Lock-compatible operations (`PutObjectRetention`,
  `PutObjectLegalHold`, bucket default retention, `x-amz-object-lock-*` headers) so existing
  compliance tooling works unmodified.
- **Audit** — every retention transition (version prune, trash, restore, expire, hold set / release,
  and every refused delete) is written to the append-only audit / event log (§8.3) — the compliance
  and WORM proof, the same log that backs GDPR deletion proof.

## Alternatives considered

- **Four separate proposals.** Rejected: the precedence ordering above is cross-cutting and only
  expressible once; separate proposals would each re-derive a partial, mutually-inconsistent slice of
  it (the conflict trap).
- **Gateway-only (L1) enforcement.** Rejected: bypassable by a direct metadata mutation or a custodian
  pass. The guarantees must live at the commit point and the GC.
- **A separate immutable "WORM bucket" type instead of per-object holds.** Rejected: too coarse —
  compliance needs per-object holds and per-prefix defaults.
- **Lean on crypto-erase reversibility for trash.** Rejected: crypto-erase is irreversible by design;
  trash must keep the key until grace expires, which the model already does (erase is terminal).

## Graduation criteria

Each enforcement point and precedence rule gets a seed-reproducible DST property (ADR-0009):

- a non-current version under a hold is never pruned by the keep-N policy;
- a trashed object is restorable before its grace expires and erased only after;
- **lifecycle expiry does not fire on a held version** (the cross-facet conflict test);
- a delete / overwrite of a held object is refused at the commit point;
- a held version's fragments are never reclaimed across a full GC sweep;
- `destroy-KEK` is refused while any held object exists under that KEK;
- a clock rollback (injected via `ManualClock`) does not release a retention hold or expire a
  trash/lifecycle timer early;
- compliance-mode retention cannot be shortened even by an admin / operator principal.

Plus: conformance to the S3 versioning, Lifecycle, and Object Lock semantics subsets; telemetry
(ADR-0011) for non-current-version count, trash size, held-object count, and refused deletes, with a
**canary alert** if GC ever evaluates a retention-protected fragment as reclaimable (it must not).

## Backward compatibility

- **On-disk / metadata format.** Adds optional lifecycle-state and retention fields to the
  version record, defaulting to "current, no retention," so existing data is unaffected (ADR-0019 /
  ADR-0002 — optional, versioned fields).
- **Delete semantics.** Soft-delete-by-default changes what "delete" *does*; it is a per-tenant policy
  defaulting to the safe (soft) behaviour, with hard delete a privileged op. Versioning is opt-in per
  bucket (S3-compatible), off by default.
- **Version skew (the fleet is half-upgraded during rollout, Q8).** The retention check must be
  **deny-on-unknown**: a node that encounters a lifecycle/retention field it does not understand MUST
  refuse the delete / reclaim rather than ignore it — a rollout-ordering concern for the
  implementation.
- **Public API.** Additive — S3 versioning / Lifecycle / Object Lock endpoints and SDK surface.

## Open questions

- **Erasure-vs-retention precedence** (Facet D / §4) — confirm "hold wins until released, then erasure
  runs," and split into its own ADR before implementation.
- **Key granularity vs. hold blast radius** — does WORM force a per-object / per-bucket intermediate
  key so one held object does not pin the tenant KEK (ADR-0021 `[OPEN]`)?
- **Governance vs. compliance mode for v1**, and whether a **break-glass** path to remove a
  compliance-mode hold exists at all (a court order, a hold set in error) — itself policy-gated and
  audited, or deliberately absent.
- **Defaults** — trash grace duration, keep-N version count, and whether these are per-bucket only or
  also a per-tenant policy (ADR-0022 bundle).
- **Consistency** — does the delete-path retention read need the version-fence / consistency token
  (ADR-0015) for read-your-writes immediately after a hold or trash transition?

## Out of scope — related Drive-foundation gaps, routed elsewhere

These surfaced in the same feature-parity review but are **not** part of the lifecycle/retention model
and warrant their own artifacts; listed here so they are tracked, not folded:

- **Change feed / sync engine** — efficient watch + delta change-feed, the motor of any sync client.
  Reserved by ADR-0007; needs its own ADR de-reserving and specifying the watch primitive.
- **Deduplication / block-level delta sync / content-defined chunking** — a separate chunking and
  storage-efficiency proposal.
- **Resumable / multipart upload** — composes onto the pending-chunk ledger; a separate API proposal.
- **Rich sharing (share links, expiry, view/comment/edit tiers)** — partly the reserved Zanzibar-class
  authorization plane (ADR-0018), partly an L1 surface; not a storage-retention concern.

## Relationship to existing decisions

Composes onto the commit point (ADR-0015), the custodian GC (ADR-0011), crypto-erase and key custody
(ADR-0021, ADR-0026 — the `destroy-KEK` gate), trusted time (ADR-0024), the on-disk / metadata format
(ADR-0019, ADR-0002), placement cost-tiering (§5), multi-tenancy policy (ADR-0022), and the audit log
(§8.3). The erasure-vs-retention precedence is the one piece that warrants its own ADR before
implementation begins.
