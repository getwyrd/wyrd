---
created: 18.07.2026 15:14
type: adr
status: Accepted
tags:
  - adr
  - s3
  - namespace
  - metadata
  - gateway
---
# 0046. Bucket model: real bucket records, not synthesized prefixes

## Context

The S3 gateway fakes buckets: the wire layer concatenates `{bucket}/{key}` into
one flat string and stores it as a single dirent name under the root inode
(`crates/gateway-s3/src/lib.rs`, `crates/server/src/lib.rs`). No bucket record,
existence check, or lifecycle exists, so every bucket-level request — CreateBucket,
HeadBucket, ListBuckets, DeleteBucket, and the bucket-scoped listing GET — fails the
object-path shape and returns 400. Issue #502 requires deciding the bucket model
before the listing (#507) and bucket-operations (#511) work under the 0.1-Alpha S3
epic (#513) can proceed, because the model shapes ListObjectsV2 delimiter semantics,
ListBuckets, and any future per-bucket state.

Two candidate models were on the table. **Option A** keeps buckets as pure prefixes
and synthesizes bucket operations: ListBuckets derived from the distinct first path
segments of all keys, CreateBucket/HeadBucket as no-ops. **Option B** introduces a
real bucket record in the `MetadataStore`.

Forces that are true regardless of the choice:

- The Alpha client bar (#512, #513) is stock aws-cli and boto3 for everyday object
  workflows; #511 additionally names SDK setup flows, `aws s3 mb`, and Terraform —
  clients that create and verify buckets before use and expect existence semantics:
  an empty bucket that lists as empty rather than not existing, HeadBucket
  distinguishing 404 from 200, PUT into an absent bucket failing with `NoSuchBucket`.
- Downstream designs already assume buckets carry per-bucket state: versioning,
  lifecycle, and retention are per-bucket/per-prefix opt-ins (proposal 0006), the
  v1 authorization model is ACLs plus bucket/prefix policy (proposal 0008,
  architecture section 8), and the key hierarchy reserves a per-bucket intermediate
  level (proposal 0012).
- The `MetadataStore` commit primitive already carries multi-key preconditions:
  `WriteBatch::require` (exact-value CAS) and `require_absent`
  (`crates/traits/src/lib.rs`), with `CommitOutcome::Conflict` distinguishing a lost
  precondition from a fault. Note the limits: a `Conflict` does not distinguish
  "already existed" from a concurrent race, and S3's CreateBucket conflict
  vocabulary (`BucketAlreadyExists` vs `BucketAlreadyOwnedByYou`) needs ownership
  context the primitive does not carry.
- The trait's only read-side bulk primitive is a whole-prefix, order-unspecified,
  `SCAN_CAP`-bounded `scan`. ListObjectsV2's pagination and ordering therefore lack
  a supporting primitive **under either option**; that gap belongs to #507 (with
  #262) and is not moved by this decision.
- The protocol-neutral gateway seam (`crates/gateway-core`) deliberately admits no
  S3 vocabulary — buckets included — so a second front-door (NFS or another verb)
  can implement the same seam without depending on the S3 crate (ADR-0010 wiring
  rule; issue #364 carry-forward).
- The custodian's GC reference-set and scrub scans read exactly the `inode:`,
  `pending:`, and `orphan:` prefixes, and allocator recovery reseeds from records
  carrying inode/chunk ids. A record under a new, disjoint prefix that carries no
  inode or chunk id is invisible to all of them.

## Decision

We adopt **Option B: a real bucket record in the `MetadataStore`**. We reject
Option A.

1. **We will introduce a bucket record** under the key prefix `bucket:{name}`,
   disjoint from `inode:`/`dirent:`/`pending:`/`orphan:`, JSON-encoded following
   the existing record pattern in `crates/core/src/metadata.rs`. The record is the
   authority on bucket existence.

2. **The Alpha bucket record is immutable** — bucket name and creation time,
   nothing else. This keeps the existence fence sound: object writes precondition
   on the exact marker bytes via `WriteBatch::require`, which is exact-value CAS,
   and an immutable record cannot change under an in-flight write. Future mutable
   per-bucket state (policy, versioning flags, lifecycle configuration) MUST live
   under separate keys so that mutating bucket state never conflicts with the
   existence fence.

3. **Object keys stay flat `{bucket}/{key}` dirents under the root inode at
   Alpha.** The bucket record adds existence and state; it does not introduce a
   namespace tree. Both options share this flat encoding, and promoting buckets to
   first-class containers of the global namespace (the ADR-0020 / ADR-0022 subtree
   direction) remains a future migration either way — the record anchors that
   migration, it does not avoid it.

4. **Per-operation semantics** (normative for #511 and #507):
   - CreateBucket commits the record gated on `require_absent`; a `Conflict` maps
     to S3's already-exists response (the `BucketAlreadyExists` vs
     `BucketAlreadyOwnedByYou` distinction is #511's concern, noting the single-
     owner Alpha deployment).
   - Object PUT and DeleteObject MUST carry the bucket-existence precondition —
     `require` on the marker bytes plus `require_absent` on the deletion fence of
     decision 5 — and map an absent (or deleting) bucket to `NoSuchBucket`.
   - Object GET and HEAD MUST consult the bucket record (a plain read) so an
     absent bucket answers `NoSuchBucket` rather than `NoSuchKey`.
   - HeadBucket answers 404 on an absent record; ListBuckets is one
     `scan("bucket:")` plus a deterministic sort (bounded by `SCAN_CAP`, an
     acceptable Alpha bound on bucket count); GetBucketLocation returns the
     configured region.
   - DeleteBucket MUST succeed only on an empty bucket.

5. **DeleteBucket's emptiness check is race-prone under scan-then-commit**: a
   concurrent PUT can land between the emptiness scan and the marker delete,
   stranding an object in a deleted bucket. Because the marker itself is immutable
   (decision 2), the deleting state lives in a **separate deletion-fence key**
   (e.g. `bucket-deleting:{name}`), never in the marker, and it is **exclusively
   owned**: DeleteBucket acquires the fence in one commit gated on the marker
   being present *and* the fence being absent, writing a unique attempt token as
   the fence value, so overlapping attempts cannot share it (the loser's
   acquisition conflicts; #511 maps that outcome). An installed fence makes every
   object write refuse with `NoSuchBucket` (their `require_absent` on the fence
   now fails). Every subsequent step MUST precondition on the exact fence token —
   removing fence and marker in one batch on a verified-empty bucket, or
   atomically removing the fence alone (retaining the marker, re-opening writes)
   to answer `BucketNotEmpty` or on any recoverable failure — so no attempt can
   clear a fence it does not hold. A fence orphaned by a crash is taken over by a
   later attempt via CAS on its exact stale bytes, which atomically invalidates
   the previous owner's remaining steps. Whether #511 implements this sketch or
   an equivalent with the same write-fence interaction is left open below.

6. **The bucket surface crosses the gateway seam as a protocol-neutral container
   concept.** `ObjectGateway` stays object-only and bucket-free. Bucket operations
   arrive as a narrow companion trait in `gateway-core` speaking container
   vocabulary (create/head/list/delete a named container), implemented by
   `wyrd-server` at the composition root per ADR-0010; the S3 crate alone projects
   containers as buckets. This is a new seam decision, consistent with the seam's
   existing no-S3-vocabulary rule.

7. **We reject Option A** because empty buckets, creation time, HeadBucket 404,
   and per-bucket state are unrepresentable in it; ListBuckets degrades to a
   full-namespace dirent scan synthesizing distinct first segments; and every
   downstream per-bucket feature (proposals 0006, 0008, 0012) would then force the
   migration Option B starts now.

## Consequences

- #511 becomes concrete: the marker record, the container companion seam, the
  per-operation table above, and the existence fence threaded through the object
  write paths. #507 gains its delimiter baseline: listing scans the per-bucket
  dirent prefix of the flat encoding, and ListObjectsV2 against an absent bucket
  answers `NoSuchBucket`.
- Every object PUT/DeleteObject carries one extra precondition and every GET/HEAD
  one extra record read — the price of correct S3 error semantics.
- The ListObjectsV2 pagination/ordering primitive gap is unchanged by this
  decision; it remains #507's scope (with #262). ListBuckets inherits `scan`'s
  unordered, capped contract and sorts client-side.
- Per-bucket policy, versioning, lifecycle, retention, and per-bucket keys gain a
  record to anchor on, under separate keys per decision 2.
- Objects written before this decision have no bucket record; Alpha's stance is
  that development clusters backfill markers for existing prefixes or reset. No
  production data predates the record.
- Foreclosed: synthesized-only bucket semantics (prefix-derived ListBuckets,
  no-op Create/Head). Not foreclosed: promoting buckets to first-class namespace
  containers (ADR-0020/0022 direction) — decision 3 keeps the flat encoding an
  implementation detail behind the container seam.
- **[OPEN]** the concrete DeleteBucket mechanism — the two-phase,
  separate-fence-key sketch of decision 5 or an equivalent #511 selects, provided
  the marker stays immutable and the write-fence interaction holds.
- **[OPEN]** whether the container seam's list operation grows a paging contract
  when a trait-level ranged scan lands (#507/#262), so ListBuckets and
  ListObjectsV2 share one shape.
