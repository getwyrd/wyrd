---
created: 22.06.2026 20:10
type: adr
status: Proposed
tags:
  - adr
  - retention
  - erasure
  - compliance
  - key-management
  - security
---
# 0028. Erasure-versus-retention precedence

## Context

Wyrd carries two directly opposed retention forces, and the architecture has never said which
wins. Crypto-erase (ADR-0021 §4) destroys a key to make data permanently unrecoverable — the
right-to-erasure ("delete my data", GDPR Article 17) story. Compliance hold / WORM (proposal 0006,
Facet D) requires that held data **cannot** be deleted until a retention period expires or a legal
hold is released, even by the operator (S3 Object Lock, SEC 17a-4). These collide head-on: a KEK
destroy defeats a hold, and a legal hold blocks an erasure request. The conflict is not theoretical
— litigation-hold-versus-right-to-erasure is a real legal tension that a storage substrate must
resolve explicitly and auditably, not by whichever code path happens to run first.

ADR-0026 §1 already gates the *mechanism* — the `KeyService` `destroy-KEK` operation is refused
while any object under that KEK is under a hold — but the *policy* it implements (which force wins,
and what happens when the hold lifts) was deferred to this ADR by proposal 0006 §4.

## Decision

1. **An active hold is absolute and wins over every deletion force.** A legal hold or unexpired
   **compliance-mode** retention overrides user delete, lifecycle expiry, trash purge, version
   pruning, crypto-erase, *and* a right-to-erasure request. While it holds, the data and the
   key material that decrypts it (KEK and the relevant DEKs) MUST persist. **Governance-mode**
   retention is not absolute: a principal holding the compliance-bypass permission may still erase.

2. **A right-to-erasure request against held data is queued, not refused.** It is recorded as
   *pending*, the requester and operator are informed that a legal obligation (the hold) blocks it —
   the lawful basis being GDPR Art. 17(3), where a legal obligation overrides erasure — and on the
   hold's release or expiry the pending erasure executes automatically. Erasure of held data is thus
   asynchronous, never silently dropped and never prematurely performed.

3. **The `destroy-KEK` gate (ADR-0026 §1) is the enforcement point.** `KeyService` refuses KEK/DEK
   destruction while any object under it is held; when the last hold over a key lifts, queued
   erasures proceed. The precedence lives at the key-custody boundary, not in the gateway, so it
   cannot be bypassed below L1.

4. **Erasure precision forces DEK-level granularity.** Because a single held object under a
   per-tenant KEK would otherwise block *all* erasure under that tenant, satisfying an erasure
   request for unheld data while held data persists requires destroying at the **DEK** level
   (per-object / per-bucket), not the KEK. This makes the key-hierarchy-depth `[OPEN]` in ADR-0021 a
   decision this precedence now forces, not an open option.

5. **Every conflict event is audited.** An erasure blocked by a hold, its queuing, and its later
   execution are written to the append-only audit / event log (section 8.3) — the dual proof that
   satisfies both the WORM auditor and the data-protection regulator.

Hold expiry timing is evaluated against trusted time (ADR-0024), so a skewed or rolled-back clock
can neither release a hold early nor trigger a queued erasure prematurely.

## Consequences

- The conflict has a single, legally defensible, auditable answer: a court-ordered hold provably
  beats a deletion request, and on release the deletion provably runs — neither is lost.
- Erasure becomes an asynchronous, queued operation for any data that is or may become held;
  callers MUST treat "erasure accepted" as "scheduled", not "done", when a hold is present.
- The DEK-level-destruction requirement (point 4) settles the ADR-0021 key-hierarchy `[OPEN]` toward
  finer keys, with the cost that the key hierarchy and re-wrap accounting grow per object/bucket.
- The precedence is verifiable under DST (ADR-0009): a held object survives an erasure request; on
  hold release the queued erasure executes; a clock rollback does neither early.
- Refines ADR-0021 and ADR-0026; ratifies the open precedence decision in proposal 0006; depends on
  ADR-0024 for hold-expiry timing.
