---
created: 07.07.2026 19:45
type: adr
status: Proposed
tags:
  - adr
  - metadata
  - validation
  - robustness
---
# 0045. Metadata validation boundaries: parse-don't-validate at decode, liberal read / strict maintenance

## Context

Several operational paths today trust a metadata record the moment JSON decoding
succeeds. That is safe *only* while every record is produced by current, correct,
in-process Wyrd code. It stops being safe once we account for old on-disk
versions, bugs, manual repair, partial corruption, and — imminently — a
distributed backend: TiKV (ADR-0008) and now FoundationDB (ADR-0042) end the
store's process-privacy, so records become network-visible and writable by
non-Wyrd tooling, and proposal 0014's M7.3 restores metadata from backup into a
custodian verify pass, driving possibly-inconsistent restored records through
every one of these boundaries. Two concrete failures already surfaced and were
fixed piecemeal: #285 (invalid EC metadata such as `k = 0` panics lower layers)
and #290 (the read path preallocated directly from an untrusted inode size).

The systematic posture, however, already exists for exactly one field. ADR-0040
built it for placement: **classify-before-use** (`placement_is_valid` /
`checked_fragments`), a **typed error** (`MalformedPlacement`), a **liberal-read
/ strict-maintenance asymmetry** (serve what you safely can; never let a
maintenance loop act on or rewrite a record it cannot trust), NEEDS-HUMAN
emission, and GC treating an unparsable reference as *referenced* (fail-safe).
The open question is not what posture to invent — it is to **generalize
ADR-0040's posture across all metadata records**, and to pin the boundary so it
cannot silently regress.

## Decision

We adopt a single validation posture for all metadata, generalizing ADR-0040
(which it refines, not supersedes), answering the four questions #291 poses:

1. **Parse-don't-validate at decode, for structural invariants.** A validated
   `metadata::decode` path returns a typed `MetadataValidationError` for any
   record that violates a structural invariant; a value that decodes is
   structurally trustworthy thereafter. *Contextual* checks that need external
   state (placement-vs-fleet, domain membership) — and, explicitly, **placement
   length, which stays under ADR-0040's liberal-read rule** — remain at the
   operation boundary, never at decode, so a still-readable record is never turned
   into a read fault (ADR-0040 decision 4).
2. **Post-validation assumptions are documented per type.** After a successful
   validated decode, the erasure math MAY assume the scheme is
   `erasure::supported` (so `k ≥ 1` and `m ≥ 1`); indexing MAY trust
   `fragment_count()`; each type records which invariants downstream code is
   entitled to assume, so the assumption and its guarantee live together.
3. **Fail-closed surfacing is the ADR-0040 asymmetry, generalized.** The read
   path MUST surface a typed read fault, never panic (the #285 class). Maintenance
   loops (custodian, repair, GC sweeps) MUST classify, skip, and emit NEEDS-HUMAN —
   never act on or rewrite a malformed record. GC MUST fail safe (treat an
   unparsable reference as referenced), never reclaim on doubt.
4. **The boundary is pinned by an adversarial decode corpus.** A malformed-record
   table — `k = 0`, `m = 0` (and any `!erasure::supported(k, m)` pair), `k + m`
   overflow, size↔chunk-map mismatch, wrong-length placement, absurd size,
   `u64::MAX` version/lease — is run at Tier-0 against
   `decode` and each boundary, applying the `invalid/` conformance-vector pattern
   to metadata.

The structural invariants to pin, by record:

| Record | Invariants |
|---|---|
| `EcScheme::ReedSolomon` | `erasure::supported(k, m)` — the exact predicate the read path already applies (`read.rs`), so `k ≥ 1` **and** `m ≥ 1` (a stored `rs(k, 0)` is as invalid as `rs(0, m)`); `k`, `m` additionally bounded to the format header widths (`ec_k`/`ec_m` are `u8`, `ec_fragment_index` `u16`) so metadata can never describe a chunk the format cannot encode |
| `ChunkRef` | scheme validity; logical length consistent with the scheme. **Placement is not a decode-time invariant** — it stays under ADR-0040 decision 4's liberal-read / strict-maintenance split (reads keep the per-index identity fallback, so a still-readable record is never faulted over a length quirk; only maintenance gates on a malformed placement). Placement length is a boundary check, never a decode failure |
| `InodeRecord` | `size` ↔ chunk-map consistency; `state` validity; **checked arithmetic** on `version` increments |
| `PendingEntry` / repair / desired / orphan | lease-timestamp checked arithmetic; parseable keys (the #283 strict-parsing sibling) |
| read reassembly | allocation caps before buffering (the #290 pattern, generalized) |

## Consequences

Metadata is hardened against every untrusted-input path the distributed backend,
restore-verify, old versions, and corruption open — the read path degrades to a
typed fault instead of a panic, and maintenance loops quarantine rather than
propagate bad records. The posture is one rule, generalized from ADR-0040, so
there is a single mental model rather than per-field improvisation. The cost is
validation work on the decode path, kept bounded by validating only *structural*
invariants there and leaving contextual checks at operation boundaries.

This review produces the implementation issues #291's contract calls for; two are
already visible and filed against this ADR: (i) the validated-decode layer with
its typed `MetadataValidationError` and the adversarial decode corpus; (ii)
checked arithmetic for version increments and lease timestamps. Any further gaps
the implementation surfaces are filed the same way.

This should land before records go network-visible on the production path (the
FoundationDB backend, #438) and before M7.3's restore-verify pass exists, since
both make the untrusted-input assumption real. It commits the project to a typed
metadata-validation error surface and the liberal-read / strict-maintenance
asymmetry as standing rules; reversing toward trust-on-decode would reopen the
#285 panic class on any non-current record. Refs ADR-0040 (the posture template),
#285/#290 (closed inputs), #283 (strict-parsing sibling), #438 (FDB consumer).
