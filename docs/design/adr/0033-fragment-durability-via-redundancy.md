---
created: 23.06.2026 13:40
type: adr
status: Proposed
tags:
  - adr
  - storage
  - chunkstore
  - durability
  - performance
  - erasure-coding
---
# 0033. Fragment durability: redundancy + crash-atomic rename, not per-write fsync

## Context

A D server stores an erasure-coded chunk as `n` fragments — at most one per D server, spread
across distinct failure domains (ADR-0032; the M3 distinct-domain placement invariant) — and the
client reconstructs a chunk from any `k` of them. The write commits only after all `n` fragments
are acknowledged; the commit point is a single metadata mutation, and a fragment is leased
garbage until then (`crates/core/src/write.rs:206-243`, `crates/core/src/read.rs:149-204`).

The D server's put path writes a fragment to a sibling temp file and `rename(2)`s it into place,
and **takes no fsync** of the file data or the parent directory before acknowledging
(`crates/chunkstore-fs/src/lib.rs:71-85`; there is no `fsync`/`fdatasync`/`sync_all` anywhere in
non-test code). So fragment durability today is supplied by **erasure-coded redundancy across
failure domains plus the M3 custodian scrub/reconstruct loops**, not by flushing each fragment to
stable media. This was never recorded as a decision; like the on-disk layout in ADR-0032, it was
discovered in code rather than chosen on the record.

It is worth recording, for three reasons.

- **It is the single largest write-throughput property of the store.** An fsync-backed durable
  write turns a put into a full filesystem transaction — data, inode, directory entry, extent
  map, and journal commit on the critical path — and each fsync on NVMe is a device cache-flush
  barrier. By relying on redundancy instead, the put path pays none of this. This is a deliberate
  advantage, not an oversight.
- **Atomic rename is not durable data.** POSIX `rename(2)` is atomic for the *name* — a crash
  sees the old name or the new one, never a half-updated directory — but atomicity of the name is
  independent of durability of the bytes: without an fsync of the file *and* its parent directory,
  a power loss can leave the rename recorded while the data (or the new directory entry) is still
  in volatile cache. This is the well-known ext4-2009 / leveldb#195 hazard. The store's
  temp-then-rename discipline (with `list` skipping `.tmp` siblings, `crates/chunkstore-fs/src/lib.rs:45-49,99-135`)
  buys crash-*atomicity* — never a torn or half-written fragment — but **not** crash-*durability*.
- **Redundancy covers independent failures; correlated power loss is the edge it does not
  automatically cover.** Reed–Solomon RS(k,m) survives `m` *independent* fragment losses. A single
  correlated power event — a rack PDU, a shared UPS — drops the un-flushed page-cache buffers of
  every fragment in its blast radius at once. A failure domain is a rack/power/switch (architecture
  §7.3) — coarser than a single D server — so although a chunk's fragments sit on distinct D
  servers across distinct failure domains, a single power/blast domain may still hold more than one
  of them. If one power event drops the un-flushed buffers of more than `m` of a chunk's fragments
  at once, that chunk can fall below `k` and become unrecoverable. The model is therefore sound
  only under stated conditions, which this ADR must make explicit rather than leave implicit in
  code.

This decision is adjacent to ADR-0032 (which records the FsChunkStore on-disk layout these writes
land in) and to ADR-0019 (the fragment byte format); it changes neither. It complements the
backup-bootstrap rule and the disaster-recovery ordering (architecture sections 8.2 and 6.5),
which protect against *logical* disasters (errant deletes, bad migrations) that redundancy
faithfully replicates — a separate concern from the *physical* crash-durability this ADR governs.

## Decision

We will treat fragment durability as a property of **redundancy + repair**, not of per-write
fsync.

1. **Crash-atomic, not synchronously durable.** A D server's put path MUST be crash-*atomic* — a
   crash never yields a torn, partial, or phantom fragment (the temp-then-`rename` discipline,
   with enumeration skipping temp siblings, already provides this). The put path MUST NOT be
   required to fsync the fragment or its parent directory before acknowledging. A fragment whose
   bytes are lost to a node crash before reaching stable media is, by construction, either
   **pre-commit leased garbage** (the write fails closed and the lease is swept) or **post-commit
   under-replication** (repaired from any `k` survivors by the reconstruction custodian).

2. **The soundness condition is explicit and load-bearing.** The model holds **iff no single
   correlated power event can drop the un-flushed buffers of more than `m` of a chunk's `n`
   fragments at once.** Two properties together provide this and MUST be maintained; they are
   **jointly necessary** — neither alone suffices when there are fewer independent power/blast
   domains than `n`:
   - **(a) Placement spread.** A chunk's fragments are placed at most one per D server, across
     distinct failure domains (ADR-0032; the M3 distinct-domain invariant). Where the
     failure-domain model is aligned with the power/blast domains, this caps the number of a
     chunk's fragments that can share a single power domain at `f = ⌈n / (independent power
     domains)⌉`; if `f ≤ m`, losing a whole power domain is survivable by redundancy alone.
   - **(b) Bounded un-flushed window.** Because a power/blast domain may still hold more than `m`
     of a chunk's fragments when power domains are few (a failure domain is coarser than a server,
     §7.3), the per-domain un-flushed window MUST be bounded so the number of a chunk's fragments
     **simultaneously un-flushed within any one power domain** stays `≤ m` at any instant.

   (a) minimizes how many of a chunk's fragments a single power event can reach; (b) minimizes how
   many of those it actually loses. Crash-atomicity is relied on **always**; crash-durability is
   supplied **only** through this redundancy envelope. Any change to placement, to the
   failure-domain↔power-domain alignment, or to the durability floor `m` must preserve this
   condition.

3. **No synchronous per-put fsync; the contingency is asynchronous and batched.** We will NOT add
   a synchronous per-put fsync to "harden" the store — that would convert the central design
   advantage into the central write bottleneck. If real-hardware measurement (point 4) shows
   redundancy + repair cannot hold the durability floor under realistic correlated power loss, the
   response is a **batched, asynchronous, off-the-acknowledgement-path `fdatasync`** of
   recently-written fragments and their directory, amortized across many puts and tuned so the
   number of a chunk's fragments simultaneously un-flushed within any one power domain stays within
   the `m`-fragment margin — never a synchronous commit on the put path.

4. **Validation is by simulation for correctness and real hardware for the envelope.** The model
   MUST be exercised in deterministic simulation — crash-mid-write, correlated-power-loss across a
   domain, and reconstruct-from-any-`k` fault models, via the existing testkit `Disk` seam
   (`crates/testkit/src/lib.rs:79-88`) and its `DiskSync` fault point (`:116`) and the
   `StorageFault` seam (`:240-322`) — and on real hardware at Tier 2/3 (single-node and cross-node
   kill-and-reconstruct on real NVMe, with real power-loss / fsync behaviour). Performance is never
   measured in simulation (ADR-0009).

5. **Degraded-write stays out of scope.** This ADR records the no-fsync durability model for the
   **full** `n`-fragment set only. Committing below `n` (degraded-write + custodian backfill)
   reverses the fail-closed "never a silent half-write" admission invariant (architecture section
   8.9) and remains a future slice gated by its own ADR.

## Consequences

- The put hot path pays none of the fsync tax — the dominant storage-write cost — so the
  throughput property is realized, not aspirational. This is why several otherwise-attractive
  storage techniques (synchronous group commit, per-write durability barriers) are explicitly the
  wrong move for this store.
- A previously-implicit correctness obligation is now named and load-bearing: the distinct-failure-domain
  placement invariant protects **durability under correlated power loss**, not only **availability
  under independent failure**, and only when failure domains are aligned with power domains. This
  alignment becomes a thing deployments must verify and reviewers must protect.
- A measurement obligation is created: the no-fsync model is sound only within a bounded
  un-flushed-loss envelope that must be validated on real hardware, with the asynchronous-batched
  `fdatasync` contingency as the pre-agreed, additive response if measurement breaches it.
- Reversing or tightening the model later is cheap and additive: the batched async flush lives
  behind the `Disk` seam and changes neither the on-disk format (ADR-0019, unchanged) nor the wire.
- This ADR is adjacent to ADR-0032 (layout) and the M3 reconstruction custodian (the recovery
  loop), and does not supersede them; it records the durability semantics of writes to that
  layout, repaired by that loop.
