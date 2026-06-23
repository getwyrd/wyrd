---
created: 23.06.2026 13:00
type: adr
status: Proposed
tags:
  - adr
  - storage
  - chunkstore
  - on-disk-layout
  - scalability
---
# 0032. D-server on-disk fragment layout (FsChunkStore)

## Context

The `ChunkStore` trait is addressed by `FragmentId` (chunk id + fragment index), never by
path: `put_fragment(FragmentId, Bytes)` / `get_fragment(FragmentId)`. *How* a backend turns a
`FragmentId` into physical storage is exactly what the trait abstracts, and pluggable backends
are a deliberate design property (ADR-0008, ADR-0010): a future object-store or sharded backend
is meant to map `FragmentId` differently while interoperating perfectly, because the only
cross-implementation contract is the fragment *byte* format (ADR-0019, `specs/chunk-format`) plus
the trait — not the bytes' physical placement.

`FsChunkStore` (`crates/chunkstore-fs`) is the one on-disk `ChunkStore` implementation today. It
maps `FragmentId { chunk: u128, index: u16 }` to `root/<32-hex chunk-id>/<5-digit
ec-fragment-index>.frag` — a directory per chunk, one file per fragment index — written
crash-safely via a sibling temp file and `rename` (`crates/chunkstore-fs/src/lib.rs:203-206`,
`:81-83`). `get`/`delete` recompute the path directly (no scan); `list_fragments` is the
two-level inverse walk (`:99-136`). A fragment's authoritative identity lives in its **header**
(`chunk_id`, `ec_fragment_index`) and is re-verified against the path-derived id on every read
(`:53-66`), so the path is a lookup index, not the source of truth.

This layout has never been recorded as a decision; it was discovered in code, not chosen on the
record. Two facts make recording it worthwhile. First, `FsChunkStore` is described in-code as the
"dev and NAS profile" store, but the **networked production D server injects it today**
(`crates/server/src/cli.rs:267`; `crates/chunkstore-grpc/src/server.rs:21` names it "the
production injection") — so a dev/NAS-scoped layout is currently also the production backend, with
nothing flagging the tension. Second, the layout has a scaling characteristic worth stating
plainly: the chunk directories are **not** themselves sharded — the root holds one sub-directory
per chunk with **no prefix fan-out** — and because failure-domain placement puts at most one
fragment of a chunk on any one D server, the root directory grows roughly one entry per stored
fragment, while `list_fragments` is O(fragments) and materializes every id into a `Vec`.

## Decision

1. **Physical layout is a per-backend implementation detail, not a normative contract.** The
   normative, cross-implementation contracts are the fragment byte format (ADR-0019) and the
   `FragmentId`-addressed `ChunkStore` trait. A conformant `ChunkStore` MAY place bytes however
   it likes (object-store keys, a sharded tree, a database). The chunk-format spec is **not**
   extended to cover directories; doing so would forbid the backend pluggability ADR-0008 /
   ADR-0010 rely on.

2. **Record `FsChunkStore`'s layout as it stands.** `root/<32-hex chunk-id>/<5-digit
   ec-fragment-index>.frag`, written via temp-then-`rename`; `list_fragments` is the two-level
   inverse walk; `get`/`delete` are direct path computations. Identity is authoritative in the
   fragment header and verified against the path on every read — the path is an index, not the
   source of truth.

3. **Scope and fitness.** `FsChunkStore` targets the **dev / single-binary / NAS** profiles and
   serves as the **interim production networked-D-server backend** until the at-scale `ChunkStore`
   swap lands (ADR-0008, ADR-0010). The current layout is fit for dev / NAS / small deployments.
   It is **not** claimed fit for a high-fragment-count production D server: the root directory
   does not fan out, and `list_fragments` is O(N) and fully materialized, so enumeration cost,
   directory-block and inode overhead, and (on ext4) the htree index ceiling become binding as
   fragment count per server rises. The at-scale backend's layout — prefix fan-out, streaming
   enumeration, or an object store — is a **separate future decision**; this ADR records that it
   is owed, and does not pre-decide it.

4. **Within-backend longevity is the format's job, not the path's.** Because each fragment is
   self-describing and validates standalone, an out-of-band backup / salvage / fsck tool can
   recover a D server's fragments by decoding files and reading their headers, **independent of
   the directory convention** — consistent with the backup-bootstrap rule (section 8.2) and the
   disaster-recovery ordering (section 6.5). If such tooling is built and chooses to rely on the
   directory convention, that reliance is documented with the tool, still not in the chunk-format
   spec.

## Consequences

- The pluggability claim (ADR-0008) is protected on the record: a future object-store or
  prefix-sharded backend is explicitly conformant, because physical placement was never part of
  the contract.
- The previously-undocumented scaling characteristic is now on the record — a flag for anyone
  sizing a production `FsChunkStore` deployment and a stated prerequisite for the at-scale
  backend. This ADR **records** the characteristic; it does not change the layout.
- A future change to `FsChunkStore`'s own layout (e.g. adding prefix fan-out) is a
  **backend-internal data-migration** concern for already-written fragments, **not** a
  `format_version` break (the byte format is untouched). Existing on-disk data must remain
  readable or be migrated; this ADR flags that obligation without choosing the mechanism.
- ADR-0019 is unaffected and unsuperseded: it governs the fragment byte format, this ADR governs
  one backend's directory layout. They are adjacent, not in tension.
- No code changes. The at-scale `ChunkStore` backend, the prefix-fan-out / streaming-enumeration
  decision, and any out-of-band salvage tooling remain future work, tracked separately.
