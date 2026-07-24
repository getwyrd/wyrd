---
created: 23.07.2026 21:35
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#626"
tags:
  - proposal
  - s3
  - multipart
  - metadata
  - custodian
  - commit-protocol
---
# Proposal: The multipart commit protocol — publication, protection, and reclamation for assembled writes

> **What this settles.** Everything *underneath* `CompleteMultipartUpload`: what proves an
> assembled chunk map is safe to publish, what protects durable-but-unpublished bytes and
> which maintenance consumer sees them, the upload lifecycle and its failure semantics, the
> bounded-work pattern for objects whose fan-out is proportional to their size, the
> reclamation evidence a failed in-flight write must leave, the abandoned-upload **reaper**
> (both its protocol-facing half and its detection/sweep algorithm), and **chunk-map
> segmentation** — the record shape and staged publication that let a published object exceed
> what one metadata value can hold.
>
> **What this does not settle.** The S3 wire surface — routing, denylist removal, the
> percent-encoding fence, exact status and error codes — stays with the implementing slice
> [#508][i508]; it was stable across every round of adversarial review and is not in
> question. This proposal *decides*; it implements nothing. The reaper's **implementation**
> stays [#625][i625] (this document is its design), and the ETag basis is closed by
> [ADR-0047][a47]. Choosing knob *values* inside the valid ranges settled here, and wiring
> the metrics/alerts/CLI, stays with the implementing slices.
>
> **The class, not the feature.** Multipart is the first **assembled write** — a write whose
> durable staging outlives any single request or lease window. Server-side copy ([#504][i504]
> step 2) and any future resumable write have the same shape, which is why three of these
> decisions are recommended for graduation to ADRs (see *Graduation criteria*).
>
> **Reading the citations.** Every `path:line` citation is pinned to `origin/main` at commit
> `cd82a29`; lines move, so treat the path plus the quoted contract as authoritative and the
> line as a pin. This proposal is a **draft** under [ADR-0037][a37] and is editable until it
> is ratified; ratification (draft → accepted, architecture-board / founding-maintainer
> authority) is a separate governance act that is **not** part of this cycle.

## In plain language

This section is for readers who need to know **what the system does**, not how it is built. It
decides nothing — *Motivation* below carries the argument for why this proposal exists and
*Design* carries the normative decisions — but nothing here is a simplification either of them
contradicts. Terms in **bold** are the ones used throughout.

**The everyday scene.** A backup tool uploads a 50 GB file. No single request carries 50 GB, so
the S3 protocol splits it: the client says "start an upload" and gets an **upload id**; it sends
the file as numbered **parts**, often eight or sixteen at a time, over minutes or hours; finally
it says "assemble these parts, in this order, into one object". Only that last step —
**Complete** — makes the object visible. If the client walks away first, nothing should ever
appear.

**Why that is harder than it sounds.** Wyrd writes each part's bytes to disk as they arrive,
spread across many machines. A background housekeeper continuously reclaims bytes that no
finished object refers to — that is how overwritten and deleted data is freed. But a part that
arrived twenty minutes ago belongs to no finished object *yet*, so to the housekeeper it looks
exactly like garbage. Until now the answer was a short-lived ticket — "a write is in progress,
keep these bytes" — good for thirty seconds and renewed for as long as one request keeps
streaming. A multipart upload lasts hours and outlives the server holding it, so that ticket
cannot be what keeps its bytes alive. Everything below follows from replacing it.

### How it works

- **Receipts, not tickets.** Every part that finishes leaves a small durable record saying
  "these bytes belong to upload X, part 7", and every part still streaming leaves one per chunk
  saying the same thing. The housekeeping passes are taught to read those receipts: bytes with a
  receipt are protected, and the protection lasts as long as the receipt does rather than as long
  as a timer does. A part hands off from the streaming receipt to the finished-part receipt in a
  single indivisible step, so there is never an instant where the bytes are unclaimed.

- **A fixed number of parking spaces.** Each upload has a small, fixed set of numbered slots —
  say sixteen — and a part must take one before it may start streaming. Because the slots are
  *keys that either exist or do not*, no amount of concurrency can create a seventeenth: when
  they are all taken, the client is told "slow down, try again", which is a normal, expected S3
  answer. This is what stops one client from leaving unbounded half-finished debris behind, and
  it is why a crashed part keeps holding its slot — debris is counted, not invisible.

- **A lock at assembly time, not a stopwatch.** When the client asks to assemble, the upload is
  first **fenced**: the upload's own record is flipped to "completing" in one atomic step, and
  every part operation carries a check against that record. After the fence, no part can be
  added, replaced, or removed. That single check is what proves the assembler is looking at
  exactly the set of parts that exists — the guarantee the thirty-second ticket used to provide,
  now provided by a lock that no clock can expire.

- **Publication is one flip.** The assembled table of contents is written first — as several
  records if the object is too large for one, see below — and only then is a single atomic
  update made that points the object's name at it. Before that update nothing is visible; after
  it, everything is. There is no half-published object, and if the flip loses a race to someone
  else writing the same key at the same moment, the upload is unlocked and the client may retry
  rather than being stranded.

- **Big objects get chapters.** The list of "where each piece of this object lives" is itself
  data, and a 50 GB object's list does not fit in the single record the store allows. So the list
  is split into **segments** — chapters written ahead of time — with the object's record naming
  the set. This is what lets objects exceed the size a single ordinary upload can reach.

- **Cleaning up is a to-do list, not a big bang.** Overwriting or deleting a very large object
  would otherwise mean millions of small updates in one database transaction — more than any
  transaction is allowed to hold, so it would fail permanently rather than slowly. Instead the
  operation records a single durable **obligation** ("these bytes are no longer referenced") in
  the same instant it makes the change, and a background worker drains it in small batches. A
  crash mid-drain loses nothing: the to-do item is still there and the work resumes.

- **Evidence before removal, and the clock is never restarted.** Before anything stops protecting
  a byte, a mark is written saying "this is reclaimable, as of now"; the housekeeper only frees
  it after a grace period. Marks are written once and never overwritten — a second worker that
  finds a mark already present leaves it alone, because rewriting it would restart the grace
  period and could postpone reclamation indefinitely.

- **A janitor for uploads nobody will finish.** Someone presses Ctrl-C; a server dies holding a
  half-assembled upload. A background pass finds uploads that have shown no progress for a while,
  or that have simply been open too long, cancels them, and reclaims their bytes. It decides
  using durable records rather than trusting timestamps from machines whose clocks it does not
  own, and every state an upload can be in has some actor able to move it out — nothing can get
  permanently stuck.

- **After a metadata restore, in-flight uploads are cancelled.** If the metadata is rolled back
  to an earlier backup, an upload that was in progress at that moment may refer to bytes that
  have since been freed. Rather than guess, the restore cancels every open upload; clients start
  those uploads again. This is a deliberate simplification, chosen over a resumption protocol
  that could not prove the bytes still exist.

### What a client sees

Ordinary S3 behaviour, with three refusals that are all normal protocol answers rather than
errors in the usual sense: **slow down** when too many parts are in flight for one upload or too
many uploads are open across the system; **no such upload** when an upload was cancelled or
already finished; and **too large** when a part, or an upload's accumulated parts, exceeds what
this deployment is configured to publish. A cancelled upload is never a partially visible object
— the object either appears complete or does not appear at all.

### What the design promises, and what it costs

Four promises, each of which the rest of the document tests against concrete failure scenarios:
an object is never published over bytes the housekeeper is free to delete; no durable byte is
left with nothing that will ever clean it up; no upload can reach a state that nothing can move
it out of; and no operation ever needs a database transaction larger than the store allows.

The costs are chosen and stated rather than discovered later. Deleted or overwritten data is
retained slightly longer, because evidence is written before protection is lifted. An upload left
open beyond the configured ceiling is cancelled and must be restarted. A client cannot park more
staged data in one upload than that upload could ever publish. And the number of uploads the
system will accept at once follows arithmetically from two operator choices — how much memory the
housekeeping host has, and how large parts are allowed to be — rather than being a number someone
picks: large parts buy fewer simultaneous uploads, small parts buy more.

## Motivation

`CompleteMultipartUpload` is not a new verb over the existing write path. It is the first
consumer of the metadata layer's safety contracts that violates the shape those contracts
were written for — a single streaming upload, publishing inside one 30-second lease window —
and it violates all four of them at once:

- **Publication is lease-conditional and TTL-timed.** "These bytes are still safe to publish
  over" is proved today by *lease liveness*: every chunk must hold an unexpired `pending:`
  lease, checked by compare-and-set preconditions riding in the publishing batch
  (`live_lease_guards`, `crates/core/src/metadata.rs:763-796`, issue #490 — an absent **or**
  lapsed entry fails closed with `Conflict`). The TTL is 30 s
  (`crates/server/src/lib.rs:53`) and is renewed only while a single `stream_write_data`
  call is in flight (`crates/core/src/write.rs:474-500`; a renewal that finds a lapsed lease
  aborts the upload rather than resurrect dead authority). A multipart Complete assembles a
  chunk map from parts committed minutes, hours, or days apart. **Lease liveness cannot be
  the proof**, and no *correctness* timer may be put on an inherently long-lived operation.
- **The maintenance planes gate on committed state only.** The reference set that protects
  bytes from reclamation is built from *committed* inode chunk maps and nothing else — a
  pending inode's provisional map is excluded **by design**
  (`crates/custodian/src/gc.rs:217-228`, built at `:249-260`). GC's safety gate
  (`gc.rs:162`, via `ReferenceSet::protects`, `:244-246`), the restore pass's identical gate
  (`crates/custodian/src/restore.rs:218-224`), scrub's walk of `referenced.placed`
  (`crates/custodian/src/scrub.rs:95-110`) and drain's `genuinely_holds`
  (`crates/custodian/src/desired_state.rs:157-164`) share that one set; reconstruction
  (`crates/custodian/src/reconstruction.rs:313-325`, scan at `:607`), rebalance evacuation
  planning (`crates/custodian/src/rebalance.rs:147-151`) and backfill
  (`crates/custodian/src/backfill.rs:79`) scan committed `inode:` records independently.
  Multipart introduces a **third class** of durable byte — committed to disk, referenced by
  something that is not an inode, not yet published — and today's two-class world has no
  place for it.
- **The metadata model assumes small records.** The store trait inherits its backend's
  limits — "FoundationDB's are the tightest in play and are therefore the de-facto ceiling:
  10 KB key, 100 KB value, 10 MB and 5 s per transaction" — and states that the model in
  `core` "writes small records and stays far inside them"
  (`crates/traits/src/lib.rs:744-758`). Multipart is the feature that makes a *large* object
  routine, and then permits overwriting it, at which point publication owes one `put` per
  prior fragment **in a single batch** (`commit_chunk_map_superseding`,
  `crates/core/src/metadata.rs:582-619`). It also makes the *value* ceiling bite: a chunk map
  large enough to describe a multi-gigabyte object does not fit in one 100 KB value at all
  (the arithmetic is in decision 4 and decision 7).
- **Batches are explicitly non-idempotent.** "A batch is not guaranteed idempotent … A
  caller that wants replay safety must build that safety into the batch itself"
  (`crates/traits/src/lib.rs:833-843`), and a commit whose outcome is unknown has exactly
  one remedy: re-read and establish what happened (`:738-745`). A Complete that a client
  retries after a timeout must be distinguishable from a first Complete by **durable
  evidence** — and once publication is *staged* (decision 7), that evidence must survive a
  crash in the middle of it.

The sharpest consequence needs no crash. Fence a Complete `Open → Completing`; let a
concurrent `PutObject` to the same key win the publication compare-and-set — `create`'s
`require_absent` on inode and dirent (`crates/core/src/metadata.rs:366-382`) or the
superseding CAS on the prior inode's bytes (`:582-619`). Complete's publish returns
`Conflict`. With no defined exit from the fenced state, the session is stuck forever and
every staged byte it references is protected forever. That is not a bug in an
implementation; it is a protocol that was never written down.

Rounds of adversarial plan review on #508 each closed the previous round's findings and
surfaced a defect one layer deeper, because a slice is only briefable when **the negations
of its success criterion are enumerable** — for every way to implement it wrong, an
observable that fails. This proposal exists to make that enumeration possible: seven
decisions, each with its invariant and its failure-mode table, plus a register of concrete
executions (crash points, lost races, operator actions, clock mismatches, segment-write
crashes) and what disposes of each.

### The honest object ceiling — why segmentation is in scope

The launch requirement is objects **over 10 GiB**. A published chunk map is one JSON value,
and one value cannot hold an unbounded map. Working the arithmetic through (decision 4): a
`ChunkRef` encodes to ~131 B (small D-server ids) to ~302 B (worst-case `u64` ids), and the
100 KB value ceiling with 2× headroom holds only **165–381 chunks** — about **165–381 MiB**
of object at the 1 MiB default chunk size (`crates/server/src/lib.rs:51`). That is *below*
the 5 GB single-PUT ceiling, not above it; even 13–32 MiB chunks reach only ~5–12 GiB,
traded against gateway memory (`chunk_size × max_concurrent_encodes`, a chunk is buffered to
erasure-code it). A design that stops there cannot deliver the feature's own promise.
Therefore **chunk-map segmentation is settled here** (decision 7), designed *with* the
staged-obligation machinery of decision 4 as one pattern family, not beside it — a segmented
map cannot be published in one batch, so it needs its own staged publication (write segments,
then flip the root). The computed ceilings, flat and segmented, are stated as real numbers in
the accepted-costs register.

### Implementation order — normative

**[#625][i625] (the reaper) MUST land with or before [#508][i508] (multipart).** This is a
requirement, not a preference, and it follows from the design rather than from taste:

1. The protocol has states whose **only** exit is the reaper — a session whose client walked
   away (no verb will ever arrive), a session fenced into `Completing` by a gateway that then
   crashed, a teardown obligation whose draining gateway died mid-drain, a session that has
   outlived its `W_session` residency ceiling (decision 6). Sweeper-only exits are permitted
   *because* the sweeper is designed here (decision 6) and its absence would make those states
   absorbing, violating invariant (3).
2. Without the reaper, staged records only accumulate. The admission counter (decision 6)
   bounds the live-session namespace even with the reaper down, but a bound with no drain is a
   permanent `503 SlowDown` — the service stops accepting uploads rather than losing data, but
   it stops. `MetadataStore::scan` fails loud above `SCAN_CAP = 1 << 20` with no partial
   result (`crates/traits/src/lib.rs:275-292`; backends clamp to it,
   `crates/metadata-redb/src/lib.rs:73-78`), and a reference-set build that crossed the cap
   would abort the whole reconcile step before GC, scrub, reconstruction and rebalance run
   (each leg `?`-propagates, `crates/custodian/src/reconciliation.rs:75-112`). The admission
   counter is what keeps every namespace under the cap; the reaper is what keeps the service
   accepting work.
3. **The operator session-abort verb (FU-6) MUST ship with the reaper, not after the first
   report** (iteration-8 finding 3). The reaper's clock guard (decision 6) deliberately *skips*
   a session whose `clock_source` it does not recognise — declining to judge is the safe
   direction — so for that session the operator verb is the **only** exit in the whole design.
   Ship the guard without the verb and a deployed producer mismatch or a legacy record creates a
   session that is absorbing by construction: its records, its owned residue and its admission
   slot are held forever, and `MAX_SESSIONS` of them is a permanent `503 SlowDown` that no
   in-system actor can clear. The same argument as point 1, one state further out.

All three are required, and they are required in the same release as the S3 verbs that create
sessions. A deployment that exposes `CreateMultipartUpload` without a running reaper is
misconfigured, and the implementing slices MUST make that state visible (a startup refusal or
an operator-visible alarm — the mechanism is #508's and #625's, the requirement is this
proposal's).

## Design

### Settled directions this design applies (sign-off, 2026-07-23)

Eight directions were settled by the maintainer at sign-off and are **applied** below as
decisions taken, not re-opened. They are referenced by label (`D-A`…`D-H`) at each point of
use; the design **honours all eight** — none is dropped or re-derived differently. Two further
calls were taken by the maintainer on **2026-07-24**, when the document moved from the automated
cycle to hand refinement; both are recorded here as settled and applied below (`D-I`, `D-J`).

- **D-A — Session lifetime.** No *correctness* timer; one *administrative* residency ceiling
  `W_session` (from initiation, deployment default, per-bucket tighten-only) bounds residency
  (decisions 1, 2, 6). The reaper stays record-only.
- **D-B — Restore.** No resumption across a metadata restore; a restore fences/aborts every
  session open in the restored image — `Open` by the ordinary abort fence, `Completing` by the
  dedicated restore-fence transition that also retires that attempt's segments (decisions 1.4, 2,
  3; F13, X57).
- **D-C — Admission is guaranteed.** A serialized slot reservation — a counter CAS'd in the
  create batch, released in the terminal delete; contention is the `503 SlowDown` (decision 6;
  F12). Create only — part commits stay counter-free.
- **D-D — Namespace cardinality.** Owned staging entries are the disjoint per-session `sidx:`
  record class (no global `pending:` scan enumerates them, finding 3), cursor-keyed bounded
  `retire:` walks, an in-flight-part cap, all-records admission accounting, bounded tombstone
  retention, and drain-health alarms (decisions 4, 5, 6; F11).
- **D-E — Mechanical repairs.** `W_completing` from the fence instant; best-effort `UploadPart`
  refusal with the authoritative check at Complete; the clock table owns the reaper's owned-lease
  read; the reaper stale-snapshot rule (decisions 3, 4, 6; F15, F16, F17, F10).
- **D-F — Honest arithmetic.** Computed ceilings as real numbers in the accepted-costs register
  (decisions 4, 7).
- **D-G — Segmentation in scope.** Staged publication (write segments, then root flip) designed
  with decision 4's machinery as one pattern family (decision 7).
- **D-H — Structure.** One document — 0016 expanded in place.
- **D-I — The in-flight cap is a key space, not a counter (2026-07-24).** The per-session
  `sinf:` integer is replaced by per-slot records `slot:<upload-id>:<index>` over
  `[0, MAX_INFLIGHT_PARTS)`: a start claims one index under `require_absent`, its owner releases
  it with a keyed delete inside its own commit batch. The cap becomes structural, the part path
  keeps no shared writable key, and the reserve stamp gives the reaper the liveness evidence a
  pre-first-chunk request previously lacked (decisions 5, 6; iteration-7 findings 1, 2, 4). This
  also **closes** the flagged part-boundary-serialization sign-off question rather than answering
  it (*Open questions*).
- **D-J — `U_ref` charges what a session may publish, not what it could stage (2026-07-24).**
  A per-session ceiling `MAX_STAGED_CHUNKS` (settled value: the publishable segmented ceiling) is
  enforced at part commit by a cumulative check over the bounded `psum:` summary range, with an
  overshoot bounded by the enforced in-flight cap. `U_ref` takes the smaller of the raw
  part-number space and that ceiling plus its headroom, which is what raises the derived
  `MAX_SESSIONS` from ≈1 to ≈19 at maximal in-range parts (decisions 2, 4, 6; the iteration-7
  launch-capacity finding).

### 0. Vocabulary

- **Assembled write** — a write whose durable staging outlives any single request or lease
  window. Multipart today; server-side copy (#504 step 2) and resumable writes tomorrow.
- **Staged bytes** — fragments durably written to D servers, referenced by a staging record,
  not referenced by any committed inode.
- **Session** — one multipart upload, from `CreateMultipartUpload` to its terminal state.
- **Fence** — a compare-and-set on the session record that both changes its state and bumps
  its **epoch**, so every operation that preconditions on the old epoch bytes fails closed.
- **Retirement** — the bounded, durable, idempotent draining of an obligation installed at
  publication or teardown.
- **Segmented map** — a published chunk map too large for one value, split into a bounded set
  of segment records named by a root (decision 7). A **flat map** is the inline chunk list an
  object small enough for one value carries today.

### 1. The records (ADR-0046: real records, not synthesized encodings)

[ADR-0046][a46] settled that a new namespace concept gets first-class records under a
disjoint prefix, not encodings smuggled into an existing namespace, and that such a record
states its key shape, who writes it, who deletes it, and which scans see it. It also flagged
that a **scan-then-commit** emptiness/population check is race-prone (`0046` §Consequences on
`DeleteBucket`) — the reason admission here is a serialized reservation, not a scan
(decision 6). The existing namespaces are `inode:` / `dirent:` / `pending:` / `bucket:` /
`orphan:` (`crates/core/src/metadata.rs:30-70`) and `desired:dserver:`
(`crates/custodian/src/desired_state.rs:33-38`). This protocol adds the following, all
disjoint from each other and from the above (no prefix is a prefix of another, so no `scan`
returns a neighbour's records):

| Key | Value (JSON, as every record today) | Written by | Deleted by | Scanned / read by |
|---|---|---|---|---|
| `mpuctl` | **one record, two fields: `{ count, max_sessions }`** (the admission record, decision 6). `count` is the number of `mpu:` records that exist in any state; `max_sessions` is the **governing limit those increments were admitted against**, stored rather than derived per gateway so a rolling configuration change cannot leave two gateways enforcing different bounds (iteration-9 finding 5, X64). Both fields live in **one** record and every mutation CASes it **whole** (`require(mpuctl == prior)`), so the count and the limit it was checked against can never be read apart. A gateway whose locally derived `MAX_SESSIONS` differs from `prior.max_sessions` **refuses to admit and alarms**; changing the limit is an explicit operator CAS of this field | **absent reads as `{ count: 0 }`** — the **first** Create on a fresh or upgraded store initializes it in the create batch with `require_absent(mpuctl)` + `put mpuctl = { count: 1, max_sessions: <its derived value> }` (no migration/init step), so a fresh store adopts the first admitting gateway's derivation and every later gateway is checked against it; every later Create CASes the whole record with `count + 1`; CAS'd `-1` in the **terminal** batch that deletes the last session record — that batch also `require`s the session record's **exact prior bytes** (`require(mpu:<id> == prior)`), so the `-1` is **exactly-once** even when a gateway drain and the reaper both attempt the teardown (one wins the CAS, the other's precondition fails and its batch is a no-op) | never deleted (a fixed singleton) | Create (read-then-CAS reservation) |
| `slot:<upload-id>:<index>` | one **in-flight part slot**: `{ part_number, attempt_id, reserved_at_millis, lease_expiry_millis }` — the stamp records when the slot was claimed, and the lease is **renewed in flight by the same half-TTL loop that renews the owned `sidx:` leases** (`crates/core/src/write.rs:474-500`), so a request is observably alive from the instant it reserves, *before* it has written a byte. `<index>` is a fixed-width decimal in `[0, MAX_INFLIGHT_PARTS)`, so **the key space *is* the cap** — the per-session in-flight admission bound is *structural*, enforced by which keys can exist, not by an integer every writer must CAS correctly. This replaces the iteration-6 `sinf:` counter, whose shared per-session CAS gave a part commit no termination bound and put a benign counter collision on the same classification path as a genuine losing writer (iteration-7 findings 1 and 4) | `UploadPart` **before it streams any chunk**: it reads the bounded range `slot:<upload-id>:` (≤ `MAX_INFLIGHT_PARTS` records), picks a free index `k`, and commits `require_absent(slot:<id>:<k>)` **and** `require(mpu == Open@E)` with the put. The authorization is the **per-key `require_absent`**, not the population read, so concurrent reservers cannot overshoot the cap ([ADR-0046][a46]'s scan-then-commit warning does not bite: the range read only *chooses* an index, it does not admit); a reserver that loses an index retries the next free one and answers `503 SlowDown` once all `MAX_INFLIGHT_PARTS` are taken — **a bound on probes, not a retry loop against a contended key**. No slot can be reserved once the session leaves `Open` | the part commit or the live-session compensation that owns it, by `require(slot:<id>:<k> == prior)` + delete **in that same batch** — its **own** key, so the release contends with nothing and is **exactly-once** (a retry after a landed release fails its precondition and is a no-op); otherwise deleted with the session at the terminal delete. While the session is `Open`, a part that **crashes mid-stream never releases its slot**, so its residue stays counted against the cap (F11a) | the reserving `UploadPart`, and the reaper via the bounded per-session range `scan("slot:<upload-id>:")` (≤ `MAX_INFLIGHT_PARTS`), which reads `reserved_at_millis` as a progress instant and `lease_expiry_millis` as liveness for a part that has reserved but not yet written its first chunk (iteration-7 finding 2, decision 6) |
| `mpu:<upload-id>` | session: target bucket/key, `content_type`, `created_at_millis`, `clock_source`, `state`, `epoch`; `attempts` (Complete fences so far, capped at `MAX_COMPLETE_ATTEMPTS`, decision 3); on `Completing` also `fenced_at_millis`, `segments_written` (the segment-write cursor, §3) and `publish_target` (the **dirent identity** — parent bucket inode + object name — **and** the `Completing` fence epoch `E`, so segments are written under a deterministic segment-group id, decision 7). **`publish_target` names the key, never an inode id:** an inode id is minted per `create` and both keys are `require_absent`-guarded (`metadata.rs:366-382`), so a concurrent delete-and-recreate of the same object key rebinds it to a **different** inode. A flip that reused a frozen inode id would then address a *deleted* generation — recreating an unlinked inode and CASing a dirent that now names another one. Each flip attempt therefore re-resolves the dirent: absent ⇒ `create` with a freshly minted id; present ⇒ CAS the inode it names, with `version = prior.version + 1` from that re-read prior. **The published version is `prior.version + 1` computed from the re-read prior at each flip attempt** (matching `commit_chunk_map_superseding`, `crates/core/src/metadata.rs:551`,`:595`,`:656`), never frozen at fence time — so a lost-CAS retry against a newly-superseded prior records the correct next version, not a stale one (the iteration-4 finding). On `Completed` the published `{inode, version, etag, completed_at_millis}` | `CreateMultipartUpload` (`require_absent` + counter CAS); every later state change is a fenced CAS | the retirement drain, last of all its records, in a batch preconditioned on its exact bytes (with the `count` `-1` and the surviving `slot:` deletes) | the staged reference build; the reaper (one bounded `scan("mpu:")`, ≤ `MAX_SESSIONS`); `ListMultipartUploads` |
| `part:<upload-id>:<part-number>` | part: `chunks: Vec<ChunkRef>`, `len`, `digest`, `committed_at_millis`, `session_epoch` | the fenced part commit (`UploadPart`), and only it — **no maintenance pass ever writes a part record except under the session fence** (decision 2, reconstruction row) | the retirement drain | the staged reference build; the reaper and every resolver via the **bounded per-session range** `scan("part:<upload-id>:")` (≤ `MAX_PARTS_PER_SESSION`); Complete (which needs the chunk lists) |
| `psum:<upload-id>:<part-number>` | the part's **summary**, a few tens of bytes: `{ chunks: <count>, len, digest, committed_at_millis }` — everything about a committed part except its chunk list. Written in the **same batch** as the `part:` record it summarizes (its own key, no contention), so the pair is always consistent | the fenced part commit, with the `part:` record | the retirement drain, with the `part:` record it names (`retire:records:{parts}` names the pair) | the part commit's **cumulative staged-chunk check** and the reaper's progress read, via the bounded per-session range `scan("psum:<upload-id>:")` (≤ `MAX_PARTS_PER_SESSION` records ≈ 400 KB at the cap, so the check reads *summaries*, never the fat chunk lists); `ListParts`, which needs exactly these fields |
| `sidx:<upload-id>:<part-number>:<chunk-id>` | the **owned staging entry itself** — a `PendingEntry { owner: Some(<upload-id>), lease_expiry_millis, staged: Some(StagedPlacement{ scheme, placement }) }` (§ below), the lease-carrying protection record for one in-flight chunk, **carrying the chunk's planned EC placement** so the record-only reaper can compute its `orphan:<dserver>:<chunk>:<index>` keys and drain can count its fragments as held on a specific server (the iteration-4 finding fix). It lives under a **prefix disjoint from `pending:`** so that **no global `scan("pending:")` — the restore pass's `pending_chunks` (`crates/custodian/src/restore.rs:417-429`) or the expiry sweep (`crates/custodian/src/gc.rs:296-313`) — ever enumerates an owned entry** (the iteration-3 finding-3 fix; the `<part-number>` in the key lets the reaper attribute residue to the part attempt that staged it) | `write::intent` for a staging write, writing the `WritePlan` placement into `staged`, in a batch preconditioned `require(mpu == Open@E)` — so **none can be created after the session leaves `Open`** (the iteration-3 finding-1 serialization edge) — **and `require_absent(desired:dserver:<S>)` for every server in the placement it records**, so none can be created naming a server whose drain has already been recorded (the drain fence, decision 2, iteration-8 finding 1) | the part commit or compensation (a live-session loser) that removes its own chunk; else the reaper, which walks it for **both** `Aborting` **and** `Completed` teardown before the terminal delete (the iteration-3 finding-2 fix) | the reaper **and** the staged reference build (decision 2 — in-flight owned fragments count as held for drain via their `staged` placement, the iteration-3 finding-4 fix), each via the **bounded per-session range** `scan("sidx:<upload-id>:")` (≤ `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`) — there is **no** global scan of owned entries anywhere |
| `seg:<upload-id>:<epoch>:<index>` | one segment of a published map: `chunks: Vec<ChunkRef>`, `byte_offset`, `byte_len` (decision 7). `<epoch>` is the `Completing` **fence epoch** that wrote it, so the segment-group is **per-attempt** — a rolled-back attempt's stale segments live in a key range disjoint from any later attempt's (the F18 fix) | (1) the Completing session's segment-write phase at that epoch (byte-budgeted batches, before the root flip); (2) for a **committed** segmented object, **reconstruction re-place and rebalance evacuation** rewrite a segment's `ChunkRef.placement` from a source position `P_old` to a destination `P_new` by (i) **pre-marking the destination** `orphan:<P_new>:<chunk>:<i>` *before* writing the fragment there, (ii) writing the fragment, then (iii) an **exact-bytes CAS** `require(seg:<id>:<E>:<i> == prior)` **and** `require(inode == prior)` (the committed generation is still current) that, **on win**, updates the placement, **deletes the `P_new` pre-mark** (adopting the destination) and **writes `orphan:<P_old>`** (evidencing the vacated source), and, **on loss** (a concurrent supersede/delete advanced the inode, or another repoint won the seg), is a no-op that **leaves the `P_new` pre-mark standing** so GC reclaims the abandoned destination — so no repoint outcome strands a fragment, and the retirement drain that supersedes/deletes the generation re-reads each `seg:` record's **current** placement at drain time (never a frozen one) and can no longer race a repoint, since post-supersede `require(inode == prior)` fails (decision 2; X47, adversary C) | the retirement drain — on abort/rollback (`retire:records:{seg:<upload-id>:<epoch>}`, naming exactly that epoch's segments), or when the object that owns them is superseded/deleted | every maintenance consumer resolving a **segmented** committed inode, via the bounded per-object range `scan("seg:<upload-id>:<epoch>:")` (≤ `MAX_ROOT_SEGMENTS`), the epoch read from the root |
| `retire:bytes:<token>` | one of `{session, parts}` / `{chunks}` / `{generation: {inode, version, chunks, segments?: <upload-id>:<epoch>}}` — bytes to orphan-mark, then the naming records to delete | the batch that removed the last reference to those bytes, atomically with it | the retirement drain, when the obligation is fully drained | the reaper, via cursor-keyed **bounded key ranges** (never one unbounded scan) |
| `retire:records:<token>` | `{session, parts}` and/or `{seg: <upload-id>:<epoch>}` — staging records (part records **with their `psum:` summaries** and/or the dangling segments of **one** rolled-back Completing attempt) to delete whose bytes are protected by something else | the publication batch (part records), or a Completing rollback/abort (that attempt's dangling segments) | the retirement drain | the reaper, via cursor-keyed **bounded key ranges** |

**The `<token>` grammar, and why a token can never be reused (iteration-7 review).** A
retirement token is
`s:<upload-id>:<epoch>[:<part-number>:<attempt-id>]` for a **session-scoped** obligation — the
optional suffix present for the per-part obligations (a re-uploaded part's superseded chunks, a
losing writer's compensation), absent for the one session-wide teardown obligation an epoch's
fence installs — or `g:<inode-id>:<version>` for a **superseded or deleted generation**. Every
component is minted once: an epoch is bumped by every fence, a `(part, attempt)` pair belongs to
one `UploadPart`, and a `(inode, version)` pair is produced by exactly one publication
(`version = prior.version + 1` under the inode CAS, `metadata.rs:551`,`:595`). Two properties
follow, and the design leans on both:

- **Installation is `require_absent(retire:<mode>:<token>)`**, never a blind put. A token
  collision is therefore a `Conflict` the installer re-reads and classifies — never a silent
  overwrite, which would replace one obligation's payload with another's and **permanently lose
  the reclamation evidence** for every fragment the overwritten one named (outcome (a), and
  invisible: the bytes stay on disk with no record naming them).
- **A session's obligations are addressable by its own prefix.** The terminal delete's "no
  obligation left" gate is two **bounded** reads — `scan_page("retire:bytes:s:<upload-id>:", …, 1)`
  and the same for `retire:records:` — each asking only whether the range is empty, never a walk
  of the whole `retire:` namespace (which is deliberately not cardinality-bounded, decision 6). A
  `g:` obligation belongs to the object generation rather than to the session that published it,
  so the session's gate ignores it: a completed session may be torn down while the generation it
  superseded is still draining.

`parts` is a **range-encoded part-number set** (`[[1, 400]]` for a contiguous run), so the
common "every staged part was published" case is a few bytes and the worst case — 10,000
alternating numbers — stays inside one value. It is what distinguishes the two obligations a
Complete can install: S3 publishes **only the parts the client names**, so the named parts'
records are deleted (`retire:records:`) and any staged part the client did *not* name has its
bytes orphaned and then its record deleted (`retire:bytes:`). Deriving that set later from
the published map instead would mean reading an inode that may already have been superseded.

**Structural validity is checked at decode, never defaulted ([ADR-0045][a45]) — against stable
FORMAT maxima, never against live deployment knobs (iteration-9 finding 9).** The two are easy to
conflate and must not be: `MAX_INFLIGHT_PARTS`, `MAX_ROOT_SEGMENTS`, `MAX_PART_CHUNKS` and the rest
are *mutable operator caps*, and this document already permits lowering one — the rollout note for
`MAX_INFLIGHT_PARTS` says live sessions keep holding indices above the new cap until their parts
finish. If decode enforced the *current* knob, those slots would stop decoding the moment the knob
dropped, so they could no longer be renewed, committed **or torn down** — the records would become
unreadable exactly when the teardown path needs to read them, and lowering `MAX_ROOT_SEGMENTS`
would likewise make already-published roots unreadable. Decode therefore validates each record
against a **format maximum fixed by the record format** (a compile-time constant of the encoding,
changed only by a versioned format change), while the **current** knob is enforced where new work
is *admitted* — the slot reserve, the part commit, the Complete fence. Every record ever written
under a legal configuration stays decodable under every later one.
So: a violation is an **error**,
not a silently-corrected default: a `slot:` key whose `<index>` is not a fixed-width decimal in
`[0, SLOT_INDEX_FORMAT_MAX)` (the format bound; the *live* `MAX_INFLIGHT_PARTS` is enforced at
reservation, not at decode); a `slot:`, `psum:`, `sidx:` or `mpu:` value that fails to decode or
carries a field the state forbids (a `Completing`-only `fenced_at_millis` on an `Open` session); a
`retire:` key whose mode prefix is neither `bytes` nor `records`; and a segmented root whose
segment count exceeds the **format** segment maximum (not the live `MAX_ROOT_SEGMENTS`, which is
enforced when a Complete decides to segment) or whose `seg:` records' `byte_offset`/`byte_len` do not
tile `[0, size)` contiguously. The reader **fails closed** — a resolver that meets one returns an
error and, where a maintenance pass is the reader, alarms and skips that object rather than acting
on a half-understood record. This is the same boundary rule the `retire:` mode-in-the-key argument
below rests on, stated once for the whole record set.

**What is *not* a decode error: placement length (iteration-9 finding 2).** A `sidx:` value whose
`staged` placement length does not match its scheme's fragment count is a **contextual** check, and
the standing convention puts contextual checks on the other side of the boundary — *"structural
invariants are validated at decode and surface as errors, never as values; contextual checks (e.g.
placement length) are liberal on read and strict in maintenance paths"* ([ADR-0045][a45];
`AGENTS.md:146-149`, which names placement length as **the** example). Rejecting it at decode
would also defeat the handling the fleet already has: a malformed-placement chunk is not a fault to
propagate but a record to **quarantine** — GC's safety gate protects *every* fragment bearing its
id precisely because its true placement cannot be trusted (`crates/custodian/src/gc.rs:160-170`,
the `malformed-placement` skip reason), and the drain surfaces it as `PendingMalformed { chunks }`
rather than as an unexplained stall (`desired_state.rs:97-103`). So: a `sidx:` with a
length-mismatched `staged` placement **decodes**, and the staged reference build classifies it into
the existing `ReferenceSet.malformed` set, where it is fail-safe protected, attributed in the drain
answer, and repaired or reaped through the ordinary paths. Turning it into a decode error would
convert a quarantinable record into an error that aborts the whole reconcile step before GC, scrub,
reconstruction and rebalance run (each leg `?`-propagates, `reconciliation.rs:75-112`) — strictly
worse than the quarantine, and a contradiction of the boundary rule this section is applying.

**Why the mode lives in the key, not in a field.** `retire:bytes:` orphan-marks fragments;
`retire:records:` must never orphan anything, because its bytes are live object content or
still protected by a part record. A boolean field misread once is silent data loss; a
malformed key prefix is an error at decode, which is the boundary [ADR-0045][a45] puts
structural invariants at (the rubric's *Metadata validation boundaries* rule). The drain
dispatches on the prefix and treats a `retire:` key it cannot parse as an error, never as a
default.

**Two additive optional fields on `PendingEntry`, carried only by the disjoint owned record.**
`PendingEntry` (`crates/core/src/metadata.rs:344-350`, today `{ lease_expiry_millis }`) gains

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub owner: Option<String>,   // the upload id; Some(..) only on a `sidx:` owned entry, None on a `pending:` entry
#[serde(default, skip_serializing_if = "Option::is_none")]
pub staged: Option<StagedPlacement>,   // Some(..) only on a `sidx:` owned entry: the chunk's planned EC placement
```

where `StagedPlacement { scheme: EcScheme, placement: Vec<DServerId> }` is the same
`(scheme, per-fragment D-server vector)` a committed `ChunkRef` carries
(`crates/core/src/metadata.rs:124-136`, `placement` at `:135`). An **ordinary** streaming write's
`pending:<chunk-id>` entry keeps both `None`; an **owned** staging entry is the
`sidx:<upload-id>:<part-number>:<chunk-id>` record above, whose value is a `PendingEntry` with
`owner = Some(<upload-id>)` **and** `staged = Some(placement)`.

**Why the placement is on the record, not derivable later (the iteration-4 finding: `sidx:` value
carried no placement).** Orphan records are **placement-keyed** — `orphan:<dserver>:<chunk>:<index>`
(`crates/core/src/metadata.rs:60-70`) — and the drain/desired-state seam counts held fragments as
`(DServerId, FragmentId)` pairs (`crates/custodian/src/gc.rs:228-247`,
`crates/custodian/src/desired_state.rs:157-164`). A record-only reaper (decision 6) holding only a
chunk id in the key cannot compute those keys, and drain's `genuinely_holds` cannot count an
in-flight owned fragment as held **on a specific draining server**, without the chunk's placement.
`write::intent` today records no placement (it defaults to the identity vector, overwritten by
`WritePlan::place`, `crates/core/src/write.rs:45-54`); for an owned entry it therefore writes the
**planned** placement (`WritePlan`'s `placement`) into `staged` **at intent time**, before any
fragment reaches a D server. This gives the reaper exactly the addresses the committed `ChunkRef`
would have carried — so it can orphan-mark every fragment position `orphan:<placement[i]>:<chunk>:<i>`
(decision 5) and drain can count each `(placement[i], {chunk, i})` as held (decision 2, finding 4).
This is a record-shape point [ADR-0046][a46] requires stated; the key shape, writer, deleter and
scan visibility are in the §1 table.

`skip_serializing_if` on **both** fields is **load-bearing**, exactly as it is for [ADR-0047][a47]'s
optional inode fields: `renew_pending` and `live_lease_guards` compare the *re-encoded* prior entry
byte-for-byte against stored bytes (`crates/core/src/metadata.rs:748-758`, `:786-793`), so
`decode → encode` must be the identity on **both** a legacy `pending:` entry (both fields absent) and
an owned `sidx:` entry across its own lease renewals (both fields present, unchanged). Emitting
`"owner":null` / `"staged":null` would turn every renewal and every lease guard on a pre-upgrade
`pending:` entry into a permanent `Conflict`, and — because an owned lease is **renewed in flight**
(`crates/core/src/write.rs:474-500`) via the same `require(re-encode(prior))` path — would break the
renewal of a `sidx:` entry too. This is the *Serialization identity* class the review rubric calls
out (`AGENTS.md:170-174`), now covering `staged` as well as `owner`; both round-trip tests (legacy
`pending:`, owned `sidx:` with placement) are named in *Graduation criteria*. Because owned entries
live under `sidx:` and never under `pending:`, the global `pending:` scans (restore, expiry sweep)
see **only** ordinary entries — their bound is exactly today's, unchanged by multipart (the
iteration-3 finding-3 re-derivation, decision 5). The `owner` tag is retained on the record so any
code path that does encounter one recognises it in O(1); the forward direction — how the reaper
*enumerates* one session's owned entries without a global scan — is the `sidx:<upload-id>:` key range
itself.

**Upload ids** are 128-bit random tokens rendered as lowercase hex, minted by the gateway —
coordination-free in the same spirit as the chunk-id epoch (`crates/server/src/lib.rs:237-251`,
[ADR-0019][a19]). `require_absent` on the session key turns the (astronomically unlikely)
collision into a `Conflict`, never a shared session — the same 2^-128 basis as the chunk-id
epoch (execution X31).

### 2. The state machine

```text
                       CreateMultipartUpload
                                │  reserve: read mpuctl.count < mpuctl.max_sessions,
                                │  CAS count+1 && require_absent(mpu:<id>)  (one batch)
                                ▼
   ┌──────────────────────── Open@E ────────────────────────┐
   │  UploadPart: require(mpu == Open@E)  (no write to it)  │
   │                                                        │
   ├─ Complete fence ─────────► Completing@E+1              │  (stamps fenced_at_millis,
   │       (validate, then segment-write phase, then flip)  │   publish_target)
   │                             │                          │
   │                             ├─ root flip wins ──────► Completed@E+2 ──► (drain) ──► ∅ (count-1)
   │                             ├─ publish CAS lost ─────► Open@E+2   (fence released, seg cleanup)
   │                             ├─ request invalid ──────► Open@E+2   (fence released, 4xx, seg cleanup)
   │                             ├─ completer crashed ────► Open@E+2   (reaper rollback, seg cleanup)
   │                             └─ restore fence (D-B) ─► Aborting@E+2 ──► (drain) ─► ∅ (count-1)
   │                                                         (seg cleanup + session bytes, one batch)
   │                                                        │
   ├─ Abort fence ────────────► Aborting@E+1 ──► (drain) ──► ∅ (count-1)
   ├─ reaper fence (abandoned) ► Aborting@E+1 ──► (drain) ──► ∅ (count-1)
   └─ reaper fence (W_session) ► Aborting@E+1 ──► (drain) ──► ∅ (count-1)
```

Every transition is a compare-and-set on the **exact current bytes** of the session record,
so it both authorizes the transition and invalidates every operation that read the previous
state. The epoch is a monotone counter inside the record; "exact bytes" is what
`WriteBatch::require` compares (`crates/traits/src/lib.rs:825-843`), and the epoch makes two
successive states textually distinct even when nothing else changes. Two stamps are added to
the record the moment it fences into `Completing`: `fenced_at_millis` (so `W_completing` is
measured from the fence, decision 6) and `publish_target` (the target **dirent identity** — parent + name, never a
frozen inode id, §1 — and the fence epoch `E`, so a resumed completer writes the same
deterministic segment-group id, decision 7). The
published **version** is *not* frozen here — it is `prior.version + 1` recomputed from the re-read
prior at each flip, so a lost-CAS retry records the correct next version (decision 3, finding 2).

**No state is absorbing** (invariant 3):

| State | Exits | Driver |
|---|---|---|
| `Open` | `Completing` (Complete), `Aborting` (Abort, idle abandonment, or `W_session` ceiling) | client; reaper after `W_open` idle **or** `W_session` from initiation |
| `Completing` | `Completed` (root flip won), `Open` (publish lost, request rejected as invalid, or reaper rollback after `W_completing` from `fenced_at`), `Aborting` (**restore fence only**, **D-B** — the one transition that leaves `Completing` without passing through `Open`, decision 3) | client; reaper; the restore pass |
| `Completed` | record deleted after its `retire:records:` obligation drains, its owned `sidx:` range is walked empty (finding 2), and the tombstone window elapses | retirement drain (reaper, or the completing gateway inline) |
| `Aborting` | record deleted after its `retire:bytes:` obligation drains and its owned `sidx:` range is walked empty | retirement drain (reaper, or the aborting gateway inline) |
| a `retire:` obligation | deleted when drained | retirement drain; re-entrant after any crash |
| a session-owned `sidx:` staging entry | orphan-marked and deleted when its session leaves `Open`, **including on the `Completed` path** (a crashed in-flight part's residue, finding 2) or when the session vanishes | retirement drain via the per-session `sidx:` range (decision 5) |
| a session's `slot:` in-flight reservation | deleted by the part commit or compensation that owns it (`require(slot:<id>:<k> == prior)`, its own key — exactly-once); else discarded with the session at the terminal delete | client; retirement drain |
| a dangling `seg:` record | deleted by `retire:records:{seg}` on rollback/abort; adopted by the inode on a winning flip | retirement drain (decision 7) |

The session record is deleted **last**, together with any surviving `slot:<id>:` records and the
`mpuctl.count` decrement, in a batch **preconditioned on the session record's exact bytes**
(`require(mpu:<id> == prior)`) and issued only once the reaper has **observed the session's
`sidx:` range empty in this pass**. The gate is the **empty `sidx:` range**, *not* an empty
`slot:` range: a slot is held by a part that crashed mid-stream and never released it (its residue
is deliberately kept counted against the cap, F11a), so a reaped session may hold slots forever —
gating on them would deadlock teardown. The `sidx:` range, by contrast, is the exact set of
byte-bearing residue (intent precedes every fragment), so *empty `sidx:`* is the true "no residue
remains" condition; the leftover `slot:` records carry no bytes and are simply discarded with the
session (≤ `MAX_INFLIGHT_PARTS` small deletes, inside the batch's byte budget). The exact-bytes
precondition makes the teardown a
single-winner operation: a gateway draining inline and the reaper may both reach it, but exactly
one CAS succeeds and the loser's whole batch is a no-op — so the counter can never **under-count**
a live session nor **double-decrement** a torn-down one (the concurrent-drainer race the
iteration-2 review found). The empty-`sidx:` gate is the teardown side of the iteration-3
finding-1/-2 fix, and it holds **because owned residue can no longer be created once the session
leaves `Open`**: every `sidx:` intent carries `require(mpu == Open@E)` (finding 1), so after the
fence the reaper's single `sidx:` walk (run for **both** the `Aborting` and the `Completed` path,
finding 2) empties a *frozen* set that nothing can refill — no owned entry can be stranded after
the teardown. A partially drained teardown is always discoverable, and the counter stays exact,
which is what `MAX_SESSIONS` enforcement (decision 6, F12) rests on.

### 3. Batch inventory — every batch this protocol commits, and its bound

This table is the proof obligation for invariant (4) and for refutation outcome (d): no batch
is proportional to object size. `V` is the backend value ceiling (100 KB by the inherited
envelope) and `E_tx` is the per-**transaction** byte ceiling (10 MB, both
`crates/traits/src/lib.rs:744-758`; distinct from the fence epoch `E` of `Completing@E`);
`C_part`, `C_seg` and `C_object` are the per-part,
per-segment and per-object chunk bounds of decisions 4 and 7.

**Where the cursors live (no cursor record class).** Two rows below advance a cursor. Neither
introduces a record: the **drain** cursor is a field *inside* the `retire:` obligation it walks —
that batch already CASes the obligation with `require(retire:… == prior)`, so the cursor advance is
part of the single-winner mutation that authorizes the step — and the **segment-write** cursor is a
field inside the session record (beside `publish_target`), written by the completing gateway under
`require(mpu == Completing@E)`, which is the same fence that authorizes the segment writes. A "1
cursor put" in the table below is therefore always the same record the row already preconditions
on, never a new key whose shape, owner and scan visibility [ADR-0046][a46] would need stated.

**Batch size is bounded by BYTES, not by a fixed mutation count.** Every drain and
segment-write batch commits at most `E_tx/2 = 5 MB` of mutations — so the *number* of mutations
per batch is `B = ⌊(E_tx/2) / (bytes per mutation)⌋`, which is **~1,000 for small `orphan:`
marks** (a mark is a key plus a few-byte value — this is the `MARK_BATCH = 1_000` precedent,
`crates/custodian/src/restore.rs:93-100`, whose own comment ties the count to
FoundationDB's transaction cap) but only **`⌊5 MB / V⌋ = 50` for `seg:` writes** whose values
reach `V = 100 KB`. A *count*-derived `B = 1_000` applied to segment writes would put
`1_000 × 100 KB = 100 MB` in one batch — **10× the envelope**, a permanent commit failure
(this was the iteration-2 defect: the segment-write and drain rows claimed "`B` inside the
envelope" while `B` was a fixed count). The byte budget is what keeps every row below the
envelope regardless of value size; `B` denotes that byte-derived count throughout.

| Batch | Preconditions | Mutations | Size bound |
|---|---|---|---|
| Create session | **record present:** `require(mpuctl == prior)` with `prior.count < prior.max_sessions` — the **stored** limit, and the gateway refuses+alarms if its own derivation disagrees with it (iteration-9 finding 5); **record absent (fresh/upgraded store, reads as `{count: 0}`):** `require_absent(mpuctl)`; plus `require_absent(mpu:<id>)`, bucket-existence per [ADR-0046][a46] §4 | 1 put session, and either 1 CAS `mpuctl` `{count: c} → {count: c+1}` **or** (bootstrap) 1 put `mpuctl = { count: 1, max_sessions }` | O(1), < 1 KB |
| Part slot reserve (`UploadPart` start, decision 5) | `require(mpu == Open@E)` **and** `require_absent(slot:<id>:<k>)` for the chosen index `k` | 1 put `slot:<id>:<k>` | O(1); the cap is the key space `[0, MAX_INFLIGHT_PARTS)`, so no overshoot is representable; a taken index is retried against the next free one (≤ `MAX_INFLIGHT_PARTS` probes) and a full range refuses `503`, **and `404` once the session is not `Open`** (no slot after fence, finding 1) |
| Part intent | `require(mpu == Open@E)` (the finding-1 serialization edge — a read precondition, not a write, so concurrent intents never conflict) **and `require_absent(desired:dserver:<S>)` for every server `S` in the planned placement** (the drain fence, decision 2, iteration-8 finding 1 — also a read precondition; a failure re-plans against the fresh `Topology::excluding(draining)`) | one owned `sidx:<id>:<part>:<chunk>` staging put per chunk, each its own commit | O(1) per commit; `n ≤ 9` extra read preconditions |
| Part commit | `require(mpu == Open@E)`, `require_absent(part:…)` or `require(part:… == prior)`, `require(slot:<id>:<k> == prior)` (**its own** slot — never another part's, so concurrent commits of *different* part numbers share no writable key and cannot conflict with each other, iteration-7 findings 1/4) | 1 put (part record ≤ `V`), 1 put `psum:<id>:<n>` (the summary, tens of bytes), 1 delete `slot:<id>:<k>` (slot release), ≤ `C_part` owned `sidx:` deletes, ≤ 1 put (`retire:bytes:` on re-upload, ≤ `V`) | ≤ 2·V + O(chunks in part)·(key) |
| Complete fence | `require(mpu == Open@E)` **and** `attempts < MAX_COMPLETE_ATTEMPTS` (read from the same record) | 1 put (state, `fenced_at_millis`, `publish_target`, `attempts+1`) | O(1); at the cap the fence is refused and the session is only abortable, so rollback→re-Complete cannot mint `seg:` epochs without bound |
| Segment write (one, decision 7) | `require(mpu == Completing@E)` | `B_seg` `seg:<id>:<E>:` puts (each ≤ `V`), 1 cursor put | ≤ `E_tx/2 = 5 MB`; `B_seg = ⌊(E_tx/2)/V⌋ = 50` seg puts (byte-budgeted, **not** a fixed count) |
| Root flip (publish) | `require(mpu == Completing@E)`; the bucket-existence pair of [ADR-0046][a46] §4; **the target dirent is pinned on BOTH branches** — `require_absent(dirent)`+`require_absent(inode)` for a fresh key, **`require(dirent == prior)` *and* `require(inode == prior)`** for an overwrite (iteration-9 finding 6: publication is defined against the *dirent identity*, so guarding only the inode lets a `metadata::rename` move that dirent after Complete resolved it and the flip then overwrites an inode now bound at another name, leaving the multipart target absent or rebound while the client is told the Complete succeeded). Either binding changing is a `Conflict` that re-resolves and retries within `R_publish` | 1 put session→`Completed`, 1 put inode root (≤ `V`, records the segment-group `<id>:<E>` if segmented), ≤ 1 put dirent, 1 put `retire:records:{parts}`, ≤ 1 put `retire:bytes:` for unnamed staged parts, ≤ 1 put `retire:bytes:{generation}` for the superseded generation | ≤ 4·V + O(1) |
| Fence release (publish lost / invalid) | `require(mpu == Completing@E)` | 1 put session→`Open`, ≤ 1 put `retire:records:{seg:<id>:<E>}` for this attempt's segments | O(1) |
| Abort / reap fence | `require(mpu == Open@E)` (or `Completing@E` for the rollback, and for the **restore fence** below). **The reaper's *idle* arm additionally pins the liveness evidence it judged: `require(slot:<id>:<k> == prior)` for every slot index observed present and `require_absent(slot:<id>:<k>)` for every one observed free** (≤ `MAX_INFLIGHT_PARTS`, iteration-9 finding 4) — a renewal, release or new reservation in the read→fence window makes the batch `Conflict` and the pass re-derives. A **client** Abort and the `W_session` arm carry no slot preconditions | 1 put, 1 put `retire:bytes:{session}`, ≤ 1 put `retire:records:{seg:<id>:<E>}` | O(1) |
| **Restore fence of a `Completing` session** (**D-B**, iteration-7 adversary) | `require(mpu == Completing@E)` | 1 put session→`Aborting@E+1`, 1 put `retire:bytes:{session, parts}`, **1 put `retire:records:{seg:<id>:<E>}`** for the segments that attempt already wrote | O(1) — one batch, so no interleaving leaves a `Completing` session whose segments have no reclaimer |
| Retirement drain step | `require(retire:… == prior)` (single-winner on the obligation) **and**, per mark written, the decision-4.2 three-arm guard: `require_absent(orphan:<pos>)` for a position observed absent, `require(orphan:<pos> == prior)` for a stale-evidence re-stamp, none for a same-identity skip | `B` orphan puts **or** `B` record deletes, plus 1 cursor put | ≤ `E_tx/2`; `B = ⌊(E_tx/2)/(bytes per mutation)⌋` (~1,000 small orphan marks; fewer for larger record deletes) |
| Object delete (unlink) — retirement (decision 4, finding 5) | `require(dirent == prior)`, `require(inode == prior)` | 1 delete dirent, 1 delete inode, 1 put `retire:bytes:{generation}` (the removed map's chunks and, if segmented, its `seg:` range) | O(1) — the fan-out (up to ~1.78 M fragment orphans for a max segmented object) is drained in `B`-batches, **never inline** |
| Segment repoint (reconstruction / rebalance of a committed segmented object, decision 2/7) | `require(seg:<id>:<E>:<i> == prior)`, `require(inode == prior)`, **`require(orphan:<P_new> == prior)`** (the pre-mark must still stand — see below), **`require_absent(desired:dserver:<S_new>)`** (the drain fence — a repoint must not adopt a destination on a server that started draining in the write→CAS window; a loss leaves the `P_new` pre-mark for GC as usual) | pre-step: 1 put `orphan:<P_new>` pre-mark under the decision-4.2 guard for what it observed (own batch, before the fragment write); then the CAS batch: 1 CAS `seg:` (placement `P_old→P_new`) + 1 delete `orphan:<P_new>` (adopt) + 1 put `orphan:<P_old>` under `require_absent` (evidence the vacated source) | O(1); a lost CAS is a no-op that leaves the `P_new` pre-mark standing for GC — neither branch strands a fragment (X47, adversary C) |
| Staged part re-place (reconstruction of an in-flight owned chunk, decision 2/5) | `require(mpu == Open@E)`, `require(part:<id>:<n> == prior)`, **`require(orphan:<P_new> == prior)`**, **`require_absent(desired:dserver:<S_new>)`** (the drain fence, as for the segment repoint) | pre-step: 1 put `orphan:<P_new>` pre-mark under the decision-4.2 guard for what it observed (own batch, before the fragment write); then the CAS batch: 1 CAS `part:` (chunk placement `P_old→P_new`) + 1 delete `orphan:<P_new>` (adopt) + 1 put `orphan:<P_old>` under `require_absent` (evidence the vacated source) | O(1); a lost CAS (the session left `Open@E`, or the part moved) is a no-op that leaves the `P_new` pre-mark standing for GC — the rebuilt staged fragment is never stranded (X29, finding 1) |
| Owned-`sidx:` drain step (Aborting **and** Completed) | the decision-4.2 three-arm guard per mark — `require_absent(orphan:<pos>)` for a position observed absent, `require(orphan:<pos> == prior)` for a stale-evidence re-stamp under this walk's `<upload-id>:<epoch>` identity, none for a same-identity skip (iteration-7 finding 3 as corrected by iteration-9 finding 3); the walk itself is reference-based (decision 5), so there is no obligation record to serialize two drainers on | `B` orphan puts + owned `sidx:` deletes, 1 cursor put | ≤ `E_tx/2` (byte-budgeted, ~1,000 small marks); a guard loss re-reads and re-splits, marking strictly fewer positions — it never re-stamps a live grace clock |
| Losing-writer compensation (live `Open` session only) | `require(mpu == Open@E)`, `require(slot:<id>:<k> == prior)` | 1 put `retire:bytes:{chunks}` (≤ `V`), 1 delete `slot:<id>:<k>` (slot release), ≤ `C_part` owned `sidx:` deletes | ≤ V + O(chunks in part)·(key) |
| Terminal session delete | `require(mpu:<id> == prior)`, **and** no **session-scoped** `retire:` obligation left (two bounded emptiness reads over `retire:bytes:s:<id>:` and `retire:records:s:<id>:`, §1), **and** the session's `sidx:` range observed empty in this pass | 1 delete `mpu:<id>`, ≤ `MAX_INFLIGHT_PARTS` deletes of surviving `slot:<id>:` records, 1 CAS `mpuctl` `count -1` | O(1) in object size (`MAX_INFLIGHT_PARTS` small keys, inside the byte budget) — **the exactly-once decrement point**; the session-record precondition serializes a gateway drain against the reaper (double-decrement fix), and the empty-`sidx:` gate holds because fenced intents (finding 1) let nothing refill the walked-empty range (findings 1/2) |

Every row that installs a `retire:` obligation additionally carries
`require_absent(retire:<mode>:<token>)` (§1's token grammar), and every row that writes an
`orphan:` mark carries the **per-position** guard decision 4.2 defines — `require_absent(orphan:<pos>)`
for a position observed absent, an exact-value `require(orphan:<pos> == prior)` for one being
re-stamped because it carries a *different* unreference-event identity, and no mutation at all for
one carrying the same identity (iteration-9 finding 3); both are omitted from the
Preconditions column only to keep it readable.

No row's *byte size* exceeds the `E_tx/2` transaction budget (every batch is byte-budgeted, not
count-budgeted); no row is proportional to total object size except through `V`, which is
capped by admission (decision 4). What scales with object size is only the *number* of
segment-write and drain batches, which is what makes the work bounded-per-batch rather than
bounded-per-object. The root flip in particular is **O(1) in the number of parts and
segments** — the fence, not per-part or per-segment preconditions, is what proves the set did
not move (decision 1); the segments are already durable when the flip commits (decision 7).

---

### Decision 1 — What replaces lease liveness as the publication-time proof

**Decision.** Publication of an assembled write is conditional on **durable protection
evidence plus a fence**, never on a timer:

1. Staged bytes are protected by their `part:` record once the part commits, and by their owned
   `sidx:` staging entry before it. The part commit atomically writes the part record and deletes
   the chunks' owned `sidx:` entries, so protection is continuous — the entry that protects the
   bytes is replaced by a record that protects them in the same batch, never with a gap. Both the
   in-flight `sidx:` entries and the committed `part:` records are members of the staged reference
   set (decision 2), so protection is *explicit*, not a reliance on GC's conservative arm.
2. `CompleteMultipartUpload` **fences first** (`Open@E → Completing@E+1`, stamping
   `fenced_at_millis` and `publish_target`), **then** reads the part records, **then**
   validates the client's named-part list against them (every named part exists and its
   recorded digest matches; part numbers ascending), **then** writes any segments (decision 7),
   **then** flips the root with `require(mpu == Completing@E+1)` in the same batch as the inode
   create/CAS. A validation failure releases the fence back to `Open` and answers
   `400 InvalidPart` — a rejected request never wedges a session.
3. Because every part commit carries `require(mpu == Open@E)` and every teardown begins with a
   fence, no `part:` record can be created, replaced, or deleted after the Complete fence
   lands. The single session precondition therefore proves *the entire part set is exactly
   what Complete read* — the same guarantee per-part preconditions would give, at O(1) size
   and with no serialization of concurrent part uploads (preconditions do not write, so N
   concurrent part commits sharing one session precondition do not conflict with each other).
4. **The records-only proof is scoped to records that have not been rewound.** The proof is a
   statement about which records exist *now* in the live store. A metadata restore can rewind
   the store to an image in which a torn-down session and its parts are resurrected while their
   fragments are physically gone; a records-only proof cannot see that the restore falsified
   its premises. Decision 3 and **D-B** close this: a restore **fences/aborts every session
   open in the restored image**, so no resurrected session can be Completed — the proof holds
   only over records the restore left standing. **The restore fence generation MUST complete
   before any gateway serves multipart verbs on the restored image** (the iteration-4
   restore-then-serve-before-fence gap): otherwise a client's retried Complete could arrive
   *between* the image going live and the fence pass and publish over reclaimed bytes. The
   ordering is normative — a gateway waits for, or refuses multipart until it observes, the
   restore-fence generation (X17, X17b).
5. Publication stamps [ADR-0047][a47] metadata (`etag`, `content_type`, `modified`) as any
   content publication does; `modified` is the Complete instant, matching the existing rule
   that publication time is the instant the object became atomically visible
   (`crates/server/src/lib.rs:169-181`).

**Invariant preserved.** (1) A chunk map is never published over bytes a maintenance pass is
free to reclaim — the #490 obligation, restated for a class of write whose staging window is
unbounded. The proof changes from *the lease has not lapsed* to *the protecting record is
still exactly as read, its session has not been torn down, and the store has not been rewound
under it*, which is a statement about references rather than about clocks — so it holds for a
session that was open for a week, and it does **not** hold across a restore that falsifies the
records (decision 3).

**Residency, not correctness, is what `W_session` bounds.** There is no *correctness* timer:
publication is proved by records. There is one **administrative** ceiling, `W_session`
(**D-A**), measured from `created_at_millis`, that bounds how long a session may hold staged
bytes before the reaper fences it — an operator policy modelled on Amazon's
`AbortIncompleteMultipartUpload` lifecycle rule (days-after-initiation, deployment-side, not
client-opt-in). It never falsifies a publication proof: a session at the ceiling is *reaped*
(fenced to `Aborting`), and a fenced session's Complete fails the fence — it does not publish
a stale map.

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Reuse `create_leased` / `commit_chunk_map_superseding_leased` and keep multipart parts on renewed `pending:` leases | A test that stages a part, advances the clock past `2 × lease TTL` with no renewal, and Completes: publication MUST succeed. Under a lease-conditional publish it returns `Conflict` (`metadata.rs:763-796`). |
| Publish without the session precondition (read parts, then commit) | DST: interleave a reaper fence between Complete's part read and its flip. The flip MUST fail with `Conflict`. Without the precondition it publishes a map over bytes the teardown will orphan — refutation outcome (c). |
| Fence *after* reading the part set instead of before | DST: a part re-upload commits between the read and the fence. Complete MUST NOT publish the superseded chunk list; assert the published map equals the part set as of the fence. |
| Delete owned `sidx:` entries in a batch separate from the part-record write | Kill the process between the two commits; assert no fragment is ever left with neither a `sidx:` entry nor a `part:` record naming it (the classification sweep in *Graduation criteria*). |
| Precondition on the part records instead of the session (O(N) exact-value preconditions) | A 10,000-part Complete against the FoundationDB backend: the flip batch MUST commit. With per-part preconditions the batch carries N part values and exceeds the envelope (`traits/src/lib.rs:744-758`). |
| Put a **correctness** timer on the session (make publication conditional on completing within `T`) | A session that stages a part, idles, then resumes within `W_open` of its last progress (or holding a live owned lease) and within its remaining `W_session` budget, and Completes: publication MUST succeed. Publication is proved by records, never by a clock; the only timer is the administrative `W_session` ceiling, which reaps an over-age session but never falsifies a publication proof. |
| Keep a records-only proof valid across a restore | The F13 trace (execution register): snapshot an `Open` session with staged parts, abort/reap it, let GC reclaim the fragments, then restore to the snapshot and retry Complete. The retried Complete MUST NOT publish (the restore fenced the session to `Aborting`, **D-B**); a design without the restore fence publishes over reclaimed bytes — outcome (c). |

---

### Decision 2 — A protection class for durable-but-unpublished bytes, per consumer

**Decision.** Staged bytes are a **first-class protection class**, distinct from committed
references and from leased garbage. `ReferenceSet` (`crates/custodian/src/gc.rs:228-247`)
gains a second, *disjoint* member — the staged set — and `protects()` returns true for either.
The staged set has **two sources**, both read through bounded per-session ranges (never a global
scan): the committed `part:` records of sessions that still exist, **and** the in-flight owned
`sidx:` staging entries of `Open` sessions (the iteration-3 finding-4 fix — a still-streaming
part's fragments must count as protected/held, or an operator can drain-then-wipe a server under
them and a later Complete publishes a map naming wiped fragments, outcome (c)). The two sources
serve different consumers: **`protects()` (GC), the restore protection gate, and drain/desired-state
count both** (an in-flight owned fragment is protected and drain-held), but **scrub and
reconstruction act only on the committed-`part:` subset**, because verification and re-placement
need the fragment's committed EC scheme, which an in-flight chunk does not carry until its part
commits (a chunk's redundancy is untended only for the bounded streaming window of its own part,
after which its `part:` record makes it scrub/reconstruct-visible). Keeping the set disjoint from
`placed` (rather than merging staged fragments in) is what lets each consumer make its own decision
below rather than inherit one. **The two ranges are read in a fixed order — owned `sidx:` first, then `part:` (normative).** A
part commit atomically deletes a chunk's `sidx:` entries and writes the `part:` record that
protects the same bytes (decision 1.1). A build that read `part:` first and `sidx:` second could
therefore observe a chunk in **neither** set — absent from the part snapshot because it had not
committed yet, absent from the owned snapshot because it had by then — and a drain that misses it
reports `Satisfied` while those fragments sit on the draining server (outcome (c), the F6 trace).
Reading the **source** class before the **destination** class makes the same interleaving observe
it in *both* or in `part:` alone, never in neither, without needing a snapshot the store does not
offer. **The rule is general, not local to this build:** wherever one batch atomically moves a fact
from one key range to another, every reader of both ranges reads the source first — the reaper's
`slot:` → `psum:` progress reads obey it for the same reason (decision 6, X60). **Publication is a
third instance of the same handoff, and it extends the order to three classes (iteration-9
finding 7): `sidx:` → `part:` → committed inodes (normative).** The root flip moves protection from
the `part:` records to the published inode and installs `retire:records:{parts}`, whose drain then
deletes those part records. A build that read committed inodes **first** could miss a flip, read
`part:` **after** that drain deleted them, and observe the published object's chunks in *neither*
set — so `genuinely_holds` misses them, `reconciliation_status` answers `Satisfied`, and the server
is wiped while a live object references it (the F6 outcome (c), one handoff further along than the
staged case). Reading source-before-destination at both seams makes every interleaving observe each
chunk in at least one class. Building the staged set costs a **product of chunks**, not
a single scan: the build iterates the `mpu:` scan (≤ `MAX_SESSIONS`) and, per session, the bounded
`part:<id>:` range (≤ `MAX_PARTS_PER_SESSION` records, **each expanding to ≤ `MAX_PART_CHUNKS`
chunk-refs held in memory**) **and** the bounded `sidx:<id>:` range
(≤ `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` owned chunks). Removing every global `part:`/`pending:`
scan means no *single* scan can cross `SCAN_CAP`, but the *aggregate* chunk-reference work per
reconcile pass is `≤ MAX_SESSIONS × U_ref` where
`U_ref = min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) × MAX_PART_CHUNKS ,
MAX_STAGED_CHUNKS + 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS )` — **each committed part
charged its full `MAX_PART_CHUNKS`, not one unit** (the iteration-4 fix: charging a part as one
record under-bounded the in-memory footprint by up to `MAX_PART_CHUNKS×`), and the second term is
the **enforced staged ceiling plus its bounded overshoot** (decision 4.4, the 2026-07-24 call: a
session may not stage more chunk-refs than it could publish, so charging the raw part-number space
over-bounds by up to ≈19× at maximal parts). So `MAX_SESSIONS` is
**derived** from a per-pass memory budget `W_ref` as `⌊W_ref / U_ref⌋`, and the serialized counter
enforcing `MAX_SESSIONS` bounds the aggregate `≤ W_ref` mechanically, for every part-size
distribution — because every session is charged its worst-case `U_ref` (decision 6). The honest
number is computed in decision 6 and the accepted-costs register (**D-F**).

| Consumer | Sees staged? | Why, and what it costs |
|---|---|---|
| **GC** — `gc.rs:162` via `protects`, `:244-246` | **Yes — protect** | Both a committed part's fragments and an `Open` session's in-flight owned (`sidx:`) fragments are in the staged set, so `protects()` returns true for them. Under `Defer` GC's conservative arm would keep an owned fragment anyway (no `orphan:` record), but relying on that is relying on an absence; explicit membership makes the guarantee auditable. Cost: none. |
| **Restore** — the identical gate, `restore.rs:218-224`, its `pending_chunks` scan `:417-429`, and the mark at `:266-269` | **Yes — protect via the staged set, then fence sessions** | The pass marks every unreferenced, non-pending on-disk fragment `orphan:`; a committed part's or an in-flight owned session's fragments are now protected because they are in the **staged set** (`sidx:` + `part:`, per-session bounded ranges), not because a global `pending:` scan saw them. That matters for scale: the restore pass's own `pending_chunks` does a **global `scan("pending:")`**, and the fleet-wide owned population `MAX_OWNED_FLEET = MAX_SESSIONS × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` can exceed what any single scan holds — so if owned entries lived under `pending:` this scan could fail `ScanCapExceeded` and the restore command could never make progress (the iteration-3 finding-3 contradiction). Owned entries live under the **disjoint `sidx:` prefix** (decision 5), so `scan("pending:")` sees only *ordinary* streaming-write pending, whose bound is exactly today's — the re-derived restore scan bound; the owned population is read only through per-session `sidx:<id>:` ranges (each `≤ SCAN_CAP/2`), whose sum is charged to the `W_ref` memory budget. And by **D-B** the pass MUST additionally **fence every restored `Open`/`Completing` session to `Aborting`** — a restore rewinds records to an image whose bytes may be gone, so no resurrected session may Complete (decision 1.4, F13). For an `Open` session that is the ordinary abort fence; for a **`Completing`** one it is the dedicated **restore-fence transition** of §2 and §3 — `require(mpu == Completing@E)` in **one** batch that installs `retire:bytes:{session, parts}` **and `retire:records:{seg:<id>:<E>}`**, because a `Completing` session may already have written that attempt's segments before the snapshot (iteration-7 adversary). Fencing it as if it were `Open` would leave those `seg:<id>:<E>:*` records with **no deleter at all**: their only deleters are a `retire:records:{seg}` drain or the supersede/delete of the *committed* inode that adopted them, and a restore-fenced attempt never flips a root, so nothing would ever adopt or retire them (their fragments would be orphan-marked by `retire:bytes:{session}` and reclaimed, leaving records pointing at deleted fragments accumulating in `seg:`). Routing the session through a rollback to `Open` first would also work, but it is two batches where one suffices, and a crash between them re-opens a session the restore has just declared dead. **The restore fence generation MUST complete before any gateway serves multipart verbs on the restored image** (the iteration-4 restore-then-serve-before-fence gap): a gateway either waits for `reconcile_after_restore`'s fence pass or refuses multipart until it observes the restore-fence generation complete, so a client's retried Complete can never fence a *still-resurrected* `Open` session and publish over reclaimed bytes before the restore fence lands (F13, X17). The `pending_skipped` counter (`restore.rs:111-113`) gains `staged_skipped` and `sessions_fenced` siblings. Cost: an upload open at restore time is aborted (KISS: no resumption across a restore, **D-B**). |
| **Scrub** — walks `referenced.placed`, `scrub.rs:95-110` | **Yes — verify** | A staged fragment can rot during a staging window measured in hours. Scrub verifies it exactly as it verifies committed fragments, using the scheme recorded in the part record, and enqueues the ordinary `repair:` obligation. Cost: scrub's per-pass work grows by the staged population, which admission bounds. |
| **Reconstruction** — `assess` / `find_chunk`, `reconstruction.rs:313-325`, `:601-620` | **Yes — resolve and repair, under the session fence (staged) or the exact-bytes CAS (committed)** | Without it, a repair obligation for a staged chunk resolves to no committed map and is silently drained (`Assessment::Drain`, `reconstruction.rs:188-191`), so staged redundancy would decay untended. For a **staged** chunk it reads the `part:` record and re-places by the **same destination-pre-mark rule** as the committed path (finding 1 — writing the fragment then hoping the CAS wins strands it on a loss): it **pre-marks `orphan:<P_new>` before writing the destination fragment**, then CASes the `part:` record's `ChunkRef.placement` (`P_old→P_new`) under `require(mpu == Open@E)` **and** `require(part:<id>:<n> == prior)`, fenced exactly like a part upload — **on win** it adopts `P_new` (deletes the pre-mark) and orphans the vacated `P_old`; **on loss** (a Complete/Abort/reaper fence advanced the session out of `Open@E` in the write→CAS window, or the part record moved) it is a **no-op that leaves the `P_new` pre-mark standing** so GC reclaims the pre-written destination fragment. So it can never race a Complete that has already fenced **and never strands the rebuilt fragment**; a repair blocked by an in-progress Complete stays queued and is retried after publication. For a **committed segmented** object it resolves via the bounded `seg:<upload-id>:<epoch>:` range (decision 7e) and the re-place **pre-marks the destination** `orphan:<P_new>` before writing the fragment, then CASes the **`seg:` record's** `ChunkRef.placement` under `require(seg:<id>:<E>:<i> == prior)` **and** `require(inode == prior)`, deleting the `P_new` pre-mark and orphaning `P_old` on a win, or leaving the `P_new` pre-mark for GC on a loss (a concurrent supersede/delete advanced the inode → Conflict, obligation dropped) — so the repoint neither races the retirement drain deleting the same record nor strands the moved fragment on either branch (X47, adversary C). |
| **Rebalance evacuation** — `plan_evacuations` scans `inode:`, `rebalance.rs:147-151` | **Staged: no. Committed segmented: yes** | *Staged* fragments are not evacuated — they empty themselves within `W_session` (published, aborted, or reaped), and mutating part records outside the fence buys no durability. But a **committed segmented** object's fragments are ordinary committed content: rebalance evacuates them by rewriting the owning `seg:<id>:<E>:<i>` record's `ChunkRef.placement` under the **same** destination-pre-mark + `require(seg == prior)` + `require(inode == prior)` rule as reconstruction (win adopts `P_new` and orphans `P_old`; loss leaves the `P_new` pre-mark for GC — so a supersede/delete race strands nothing, X47). Cost: a drain waits for *staged* fragments (next row); committed ones evacuate. |
| **Drain / desired-state** — `genuinely_holds` from `referenced.placed`, `desired_state.rs:157-164` | **Yes — count committed-part *and* in-flight owned staged as held** | Otherwise the F6 trace runs, and the sharper iteration-3 finding-4 variant of it: even a **still-streaming** part's fragments (owned `sidx:`, no `part:` record yet) must count as held, or a drain reports `Satisfied` while they sit on the server, the operator wipes the disk, and the part commits then a Complete publishes a map naming wiped bytes (outcome (c)). `genuinely_holds` therefore tests the union — `referenced.placed` **and** the staged set: committed `part:` fragments (from their `ChunkRef.placement`) plus `Open`-session `sidx:` fragments (from each owned entry's `staged` placement, §1), each contributing its `(DServerId, FragmentId)` pairs exactly as a committed reference does. Cost: the drain stays `Pending` while any staged byte lives there — **bounded by `W_session`**, see below. |
| **Backfill** — `backfill.rs:79-99`, `:161-172` | **No — nothing to do** | Backfill fills *empty* placement vectors on pre-M3 records. Part and segment records are born with an explicit full-length placement written by the current write path (`crates/core/src/write.rs:108-114`), so the population it drains cannot contain one. |
| **Allocator recovery** — `high_water_marks`, `metadata.rs:847-881` | **No — unaffected** | It recovers the `< 2^64` in-process chunk-id space; gateway-minted ids carry a random epoch and are `>= 2^127` (`crates/server/src/lib.rs:237-251`), so a staged chunk id is never re-minted. Inode ids are unaffected: a session holds no inode until publication. |

**Bounding the drain stall (the cost of the "yes" in the drain row).** **Both** committed-part
placement **and** in-flight owned (`sidx:`) placement MUST select against
`Topology::excluding(draining)` (`crates/core/src/placement.rs:141-152`) rather than the plain
selector the write path uses today (`write.rs:108-114`), so **no new staged fragment — committed
or in-flight — lands on a draining server**.

**Filtering the selector is not enough on its own: the placement MUST be *fenced*, not merely
filtered (iteration-8 finding 1).** Selection reads a topology snapshot; the `sidx:` intent that
makes the placement *observable* is committed later. A drain recorded inside that window escapes
the filter entirely: the upload selects `S`, the operator records `desired:dserver:S`,
`reconciliation_status(S)` finds no staging record naming `S` and answers `Satisfied`, the
operator wipes `S` — and only then does the intent land, the part commit, and a Complete publish
a map naming wiped fragments (outcome (c) again, one interleaving further out than the F6 trace).
The remedy is a keyed precondition, not a new record: **every batch that installs a reference to a
fragment on server `S` — the `sidx:` intent, and the destination side of a staged re-place or a
committed segment repoint — carries `require_absent(desired:dserver:<S>)` for every server in the
placement it installs** (the desired-state ledger key, `crates/custodian/src/desired_state.rs:33-38`;
one `require_absent` per selected server, `n ≤ 9`). That makes intent creation and the drain
request a single-winner race on the desired-state key rather than two independent writes ordered
by luck:

- the intent commits **before** the drain record ⇒ the `sidx:` entry exists before any reconcile
  pass can read the drain, so the staged set (decision 2) counts those fragments as held and the
  status is `Pending`, never `Satisfied`; or
- the drain record commits first ⇒ the intent's `require_absent` fails, the write path re-plans
  against the fresh `Topology::excluding(draining)` and retries (bounded by the ordinary placement
  retry; a fleet with no admissible server refuses the part rather than staging onto a drain).

There is no third outcome, so **no fragment can land on `S` after `S` has been reported
`Satisfied`**. A returning server clears its `desired:dserver:` record, which re-admits placement
with no further coordination. The staged population on a draining server is
therefore fixed at the instant the drain is requested, and every member of it exits within
**`W_session`**
(published, aborted, or reaped at the ceiling) — because even a *live, progressing* session is
capped at `W_session` (**D-A**; the earlier `W_open` claim was wrong, a live session is
bounded by `W_open` by nothing). The stall is an availability cost with a stated bound; the
urgent-drain remedy for when `W_session` is too long to wait is **FU-2**, not the bound.

**Invariant preserved.** (2) Every durable byte is, at every instant, classifiable as
committed-referenced, staged-with-an-exit, or garbage-with-a-sound-reclamation-path. Staged is
now a named class with a named exit (bounded by `W_session`), and the fourth category —
"protected forever by residue" — is closed by decisions 5, 6 and 7.

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Leave the reference set committed-only (implement staging records but not the protection) | Stage a part, run one `reconcile_step` with an `orphan:` record present for one of its fragments, assert the fragment survives; and run `reconcile_after_restore` with the part staged and assert `stranded_marked == 0`, `staged_skipped > 0`. |
| Restore without fencing resurrected sessions (**D-B**) | The F13 trace: assert `sessions_fenced > 0` and that a Complete retried against a restored session returns `4xx`, never a publication. |
| Fence a resurrected **`Completing`** session as if it were `Open` (iteration-7 adversary, X57) | Snapshot a session in `Completing@E` **after** its segment writes and **before** its root flip, restore, and run the fence pass: the session MUST take the `Completing → Aborting` restore transition, whose batch installs `retire:records:{seg:<id>:<E>}`, and that range MUST drain empty. An abort fence that only installs `retire:bytes:{session}` leaves those `seg:` records with no deleter in the whole design — no root flip means no inode ever adopts or supersedes them, and no rollback means no obligation names them. |
| Merge staged fragments into `placed` instead of a disjoint set | Assert `ReferenceSet` exposes staged separately: rebalance's evacuation plan for a draining server holding **only** staged fragments MUST be empty, while `reconciliation_status` for that server MUST be `Pending`. A merged set makes those two answers inconsistent. |
| Make the drain ignore staged bytes (the "availability first" temptation) | The F6 wipe trace as a test: drain a server holding a *committed-part* staged fragment; `reconciliation_status` MUST be `Pending`, never `Satisfied`. |
| Count only committed-part staged bytes, not in-flight owned (`sidx:`) fragments (the iteration-3 finding-4 hole) | Drain a server, start a part upload whose fragments land there but **do not commit it**, and query `reconciliation_status`: it MUST be `Pending`. A design that only counts `part:` fragments reports `Satisfied`; wiping the server then lets the part commit and a Complete publish over wiped bytes (outcome (c)). |
| Let staged placement — committed OR in-flight owned — land on a draining server | Request a drain, then start a part upload; assert no fragment of the new part (in-flight *or* committed) is placed on the draining server (`Topology::excluding`). Without it, the drain stall is unbounded even under `W_session`. |
| Filter placement against the draining set but leave intent creation **unfenced** (iteration-8 finding 1) | DST: select a placement naming server `S` from a pre-drain topology snapshot, then interleave the drain request and a `reconciliation_status(S)` read **before** the `sidx:` intent commits. The status MAY be `Satisfied` at that instant, but the intent MUST then fail `require_absent(desired:dserver:S)` and re-plan — assert no fragment of that part ever lands on `S`. A design that only filters the snapshot lets the intent land after `Satisfied`, and wiping `S` then lets the part commit and Complete publish a map naming wiped fragments (outcome (c)). The same fence applies to the destination of a staged re-place and a committed segment repoint. |
| Build the staged/restore reference set with a global `pending:` scan that owned entries inflate (the iteration-3 finding-3 hole) | Strand fleet-wide owned entries past `SCAN_CAP` across many `Open` sessions, then run `reconcile_after_restore`: it MUST succeed — owned entries live under the disjoint `sidx:` prefix, so `scan("pending:")` sees only ordinary pending; a design that keeps owned entries under `pending:` fails `ScanCapExceeded` and the restore never progresses. |
| Adopt a pre-marked destination on the ledger precondition alone, with no `W_repoint` deadline (iteration-9 finding 1) | Seeded DST over GC's delete-before-cleanup ordering: let a repoint's pre-mark age past `G_orphan`, run a GC pass that calls `delete_fragment(P_new)` but has not yet committed its `cleanup` batch, then run the adoption CAS. It MUST refuse (its pre-mark is older than `W_repoint`), and the object MUST NOT end up naming `P_new`. A design that trusts `require(orphan:<P_new> == prior)` alone commits — the key still holds its exact pre-mark bytes while the fragment is gone — and publishes a placement over deleted bytes (outcome (c)). |
| Repair a staged chunk by rewriting its part record without the session precondition, **or without pre-marking the destination fragment** (finding 1) | DST: interleave a reconstruction re-place **after** it has written the destination fragment `P_new` but **before** its CAS, then fence the session (Complete/Abort/reap); the re-place MUST return `Conflict` **and** leave `P_new` covered by its `orphan:<P_new>` pre-mark so GC reclaims it (assert the pre-mark is present and the fragment is not stranded), and the obligation MUST stay queued. A design that writes `P_new` without pre-marking strands it unreferenced-and-unevidenced under `Defer` (outcome (a)). |
| Scrub staged fragments but leave reconstruction committed-only | Assert a repair obligation enqueued for a staged chunk is *resolved*, not drained: after one reconstruction pass the fragment is rebuilt and the part record's placement updated (never `Assessment::Drain`). |
| Build the staged reference set with a global `part:` scan | Create sessions past `SCAN_CAP / MAX_PARTS_PER_SESSION` (≈ 104) and assert `reconcile_step` still succeeds — the build MUST iterate sessions and use bounded `part:<id>:` ranges, so it never issues a scan that can cross `SCAN_CAP`. |

---

### Decision 3 — Lifecycle states and failure semantics

**Decision.** The state machine of §2, with these normative answers:

- **Publication loses the CAS race.** Complete re-reads the target key and retries the flip
  against the new prior at most `R_publish` times (retrying a `Conflict` is what `Conflict` is
  for, `crates/traits/src/lib.rs:738-745`). **Each retry recomputes the published version from the
  re-read prior** — `prior.version + 1` for an existing key, a fresh create for an absent one
  (`commit_chunk_map_superseding`, `crates/core/src/metadata.rs:551`; `create`'s `require_absent`,
  `:366-382`) — so the version is *never* the fence-time value: a retry against a prior that a
  concurrent `PutObject` just advanced records `newprior.version + 1`, not a stale number (the
  iteration-4 finding — `publish_target` fixes the target *key* and the fence epoch, not the
  version). This is a retry **within one attempt** (the fence stays at `Completing@E`): segment
  records are keyed by upload-id **and this fence epoch `E`** and their content is fixed by the
  frozen part set, so a publish-CAS-loss retry **reuses the same epoch-`E` `seg:` records** — there
  is no per-retry segment churn. (A retry that follows a
  *rollback* is a new attempt at a new epoch, §7c — a disjoint segment generation, never a
  double-write.) On exhaustion Complete **releases the fence**
  (`Completing@E → Open@E+1`) and answers `409 OperationAborted`; the session stays usable and
  the client may retry Complete without re-uploading a part.
- **Retry after an unknown outcome (F5).** The root flip changes the session record to
  `Completed{inode, version, etag, completed_at_millis}` **in the same batch** as the inode
  commit. There is therefore no state in which the object is published and the session is not
  `Completed`, or the reverse. When the gateway **that is running a Complete** receives
  `CommitUnknownResult` (`traits/src/lib.rs:730-745`) on its own publishing batch, it re-reads
  the session record to recover **its own in-flight operation** — this is internal recovery of
  a single ongoing Complete, *not* a second client verb: `Completed` → answer success with the
  recorded ETag; `Completing@E` → its own fence still stands and the flip's outcome is unknown,
  so it re-runs any missing segment writes **at epoch `E`** (idempotent by deterministic
  `seg:<id>:<E>:<i>` key and frozen content, decision 7d) and retries the flip; `Open@E+1` →
  the reaper rolled its fence back (it was too slow), so a fresh attempt must re-fence from
  `Open`. **A *separate* client `CompleteMultipartUpload` that arrives while the session is
  `Completing` is a different thing entirely — it returns `409 OperationAborted` (the verb ×
  state table below), never a concurrent resume.** The only party that re-runs a `Completing`
  session's publication is the one gateway that already owns the fence, recovering its own
  unknown result; a gateway that has *crashed* (gone, not merely slow) is handled by the
  reaper's rollback to `Open` after `W_completing`, after which a fresh client Complete
  succeeds. Choosing `409`-then-rollback over a concurrent two-gateway resume is a KISS
  decision: it guarantees **at most one publisher at a time** and is S3-conforming (a
  concurrent Complete is a conflict); its cost — the client waits up to `W_completing` for the
  rollback before re-Completing after a completer crash — is registered in *Accepted costs*.
  This is what makes Complete idempotent over a non-idempotent batch primitive, and the
  evidence now spans the whole segment-write phase, not just the flip (decision 7d).
- **The completed session is a tombstone before it is nothing.** After its `retire:records:`
  obligation drains, the session record survives in `Completed` for `W_tombstone`, so a client
  retry inside that window gets `200` with the same ETag rather than `404`. Tombstones are
  **counted by the admission counter** (they still hold an `mpu:` record) and their retention is
  bounded by `W_tombstone` (**D-D**), so they cannot silently inflate any namespace.
- **Client-visible answers by state** (normative for #508; the exact XML/status encoding is
  #508's):

| Verb | `Open` | `Completing` | `Aborting` | `Completed` (tombstone) | record absent |
|---|---|---|---|---|---|
| `UploadPart` | accepted | `404 NoSuchUpload` | `404 NoSuchUpload` | `404 NoSuchUpload` | `404 NoSuchUpload` |
| `CompleteMultipartUpload` | fences, publishes | `409 OperationAborted` | `404 NoSuchUpload` | `200` + recorded ETag | `404 NoSuchUpload` |
| `AbortMultipartUpload` | fences, `204` at once | `409 OperationAborted` | `204` (idempotent) | `404 NoSuchUpload` | `404 NoSuchUpload` |
| `ListParts` | the part set | the frozen part set | `404 NoSuchUpload` | `404 NoSuchUpload` | `404 NoSuchUpload` |
| `ListMultipartUploads` | listed | listed | not listed | not listed | not listed |

  The `Completing`/`Complete` cell is `409` for **every** client verb, including a client's
  own timeout-retry of Complete: the table is the wire contract, and there is **no** client
  path that "resumes" a `Completing` session. The only resume is the F5 internal recovery
  above (the owning gateway re-reading its own unknown result at the same epoch); a crashed
  completer is recovered by the reaper's rollback to `Open`, not by a client verb. This
  resolves the iteration-2 contradiction (the table said `409` while decisions 3 and 7 read as
  "resume") in favour of `409`.

  A session the reaper fences at `W_session` becomes `Aborting`, so a subsequent client verb
  sees the `Aborting` column (`404 NoSuchUpload`) — the S3-visible signal that the upload
  expired (FU-4 surfaces the reason in the error text). A restore that fences a session
  (**D-B**) has the identical client-visible effect.

- **A rejected Complete releases the fence.** An invalid named-part list (an absent part, a
  digest mismatch, part numbers out of order), a bucket that was deleted mid-session (the
  [ADR-0046][a46] §4 existence precondition), or an assembled map past the segmented ceiling
  (decision 7) all answer `4xx` **and** CAS the session back to `Open` (cleaning up any segment
  records the phase wrote). The session remains abortable and retriable; the one thing a
  rejected request may never do is leave the session fenced, which would make a client error
  indistinguishable from F1.
- **Abort is bounded-latency by construction (F9).** Abort's HTTP response is the *fence
  commit* — one O(1) batch that installs the `retire:bytes:{session}` obligation. Byte
  reclamation is the drain's, asynchronously and in bounded batches; teardown of a
  10,000-part session never rides inside one request.
- **`W_completing` is measured from the fence instant.** The reaper rolls a `Completing`
  session back to `Open` when `now - fenced_at_millis > W_completing` (**D-E**) — **not** from
  last part progress, which would make a healthy Complete begun long after its last part
  immediately rollback-eligible (F16). `fenced_at_millis` is stamped into the record by the
  fence batch itself.
- **Who drives resumption after a crash.** The retirement drain is re-entrant: obligations are
  durable, orphan marking is idempotent (a plain put, `gc.rs:106-122`) and already-marked
  fragments are skipped so the original grace clock is preserved (the restore precedent,
  `restore.rs:93-100`). Whichever party runs next — the gateway inline, or the reaper —
  re-derives the remaining work from the durable records. The session record is deleted last.

**Invariant preserved.** (3) No lifecycle state is absorbing, and (1) again: because the root
flip and the session transition are one batch, "published" and "about to publish" are never
both true.

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Treat a lost publication CAS as terminal (dead session) | The F1 trace: fence a Complete, land a concurrent `PutObject` on the key, retry Complete — the second Complete MUST succeed (publishing over the interloper) or, on exhaustion, leave the session `Open`. Assert the session is never left in `Completing`. |
| Freeze the published version in `publish_target` at fence time (finding 2) | Fence a Complete against key `K` at inode version `v`; land a concurrent `PutObject` that supersedes `K` to `v+1`; let Complete's flip lose the CAS and retry. The retry MUST publish `v+2` (recomputed from the re-read prior), never re-attempt `v+1`; assert the inode version advances monotonically and no publish records a stale version. |
| Commit the inode and the session transition in two batches | Kill between them (DST, both orderings). Assert: never an object published with the session still `Completing`, and never a `Completed` session with no object. |
| Measure `W_completing` from part progress instead of the fence instant (F16) | Fence a Complete; advance the clock past `W_completing` measured from the *last part* but not from `fenced_at`; the reaper MUST NOT roll it back. A progress-relative measure rolls back a healthy late Complete. |
| Distinguish a retried Complete by anything but the session record (client token, part digests) | Retry Complete after a `CommitUnknownResult`; assert exactly one publication (inode `version` advanced once) and one set of `retire:` obligations, including when the crash landed mid segment-write phase. |
| Delete the session record in the flip batch | Retry Complete immediately: it MUST answer `200` with the recorded ETag, not `404`, inside `W_tombstone`. |
| Answer Abort synchronously by tearing down inline | Abort a 10,000-part session against the FoundationDB backend and assert the response is returned in one batch and that no single commit exceeds the envelope. |
| Let `UploadPart` succeed while `Completing` | Fence a Complete, then issue `UploadPart`; it MUST fail. A part accepted after the fence is invisible to the publication that already read the set — a silently lost part. |
| Allow two Completes to both fence | Two concurrent Completes: exactly one fences; assert the other returns `409` and that only one publication occurs. |
| Leave the session fenced when the request is rejected | Complete with a bad named-part list, then Abort: the Abort MUST succeed (session back in `Open`), not `409`. Otherwise a client typo wedges an upload until the reaper's `W_completing`. |
| Publish every staged part instead of the ones the client named | Stage parts 1–3, Complete naming 1 and 3: the object MUST be parts 1+3, and part 2's **bytes** MUST end up orphan-marked while parts 1 and 3's bytes must not. |
| Publish without the bucket-existence precondition | Delete the target bucket while a session is open, then Complete: it MUST answer `NoSuchBucket` and MUST NOT strand an object in a deleted bucket ([ADR-0046][a46] §4). |

---

### Decision 4 — Bounded work for unbounded objects

**The arithmetic first (real numbers, not "well under S3's 5 TiB").** At
`DEFAULT_CHUNK_SIZE = 1 MiB` (`crates/server/src/lib.rs:51`) and RS(6,3) (`:49`, 9 fragments
per chunk), a 5 GiB *generation* is 5,120 chunks and 46,080 fragments. Three ceilings bite, and
the **same value ceiling binds three records** — the inode map, a segment, and a part — because
each is one JSON value:

- **Transaction ceiling.** Superseding a 5 GiB generation with one `put` per prior fragment in a
  single batch (`commit_chunk_map_superseding`, `metadata.rs:582-619`) owes 46,080 puts — a
  *permanent* failure to publish, not a slow one. This bites on any large committed object,
  multipart or single-PUT.
- **Map value ceiling.** `InodeRecord.chunk_map` is one JSON value (`metadata.rs:262-275`, encoded
  at `:352-356`). A `ChunkRef` (`metadata.rs:124-136`) encodes to ~131 B (small D-server ids)
  to ~302 B (worst-case `u64` ids), so a 100 KB value at V/2 headroom holds
  `MAX_MAP_CHUNKS = ⌊50000 / b_ref⌋` = **165–381 chunks** — about **165–381 MiB** of object at
  1 MiB chunks. Reaching 5 GiB flat needs 13–32 MiB chunks; 5–12 GiB is the flat ceiling even
  at large chunks (traded against gateway memory). This is *pre-existing* (a 5 GiB single PUT
  at 1 MiB chunks already crosses it), routine under multipart, and it is why the flat map alone
  cannot meet the >10 GiB launch requirement — decision 7 lifts it **for multipart**, while a
  single `PutObject` stays flat by choosing a chunk size that fits its declared `Content-Length`
  inside `MAX_MAP_CHUNKS` (or is refused past `chunk_size_max`), because segmentation's staged
  publication needs a session/epoch a single PUT has not got (decision 7, the finding-4 carve-out).
- **Part value ceiling — the same rule, one record class down.** A `part:` record's `chunks:
  Vec<ChunkRef>` is *also* one value, so `MAX_PART_CHUNKS` obeys the **identical** rule:
  `MAX_PART_CHUNKS = ⌊50000 / b_ref⌋` = **165–381 chunks**, i.e. a maximum part of
  `max_part_bytes = MAX_PART_CHUNKS × chunk_size` = **165–381 MiB at the default 1 MiB chunk**.
  A single S3-legal 5 GiB part is **5,120 chunks** and does **not** fit one `part:` value at
  default chunks — accepting it requires either a larger `chunk_size` (a 5 GiB part fits once
  `chunk_size ≥ 5 GiB / 165 ≈ 31 MiB`, traded against gateway RAM) or part-record segmentation.
  This proposal keeps the part record **flat** (one value) and **refuses a part above
  `max_part_bytes` at `UploadPart`** with `400 EntityTooLarge`, leaving the session usable; the
  S3-conformance consequence (parts above ~165–381 MiB refused at default chunks, versus S3's
  5 GiB part maximum) is stated as a real number and registered as an **accepted cost** (below).
  It is *never* an over-`V` commit — the iteration-1 class of hidden ceiling, here surfaced and
  bounded rather than hit at the backend. This is why the concurrency arithmetic below charges
  `MAX_PART_CHUNKS` per committed part, and why every "5 GiB part" scenario in earlier drafts is
  recomputed at the in-range `MAX_PART_CHUNKS ≤ 381`.

**Decision.** Three rules, one pattern.

1. **The retirement ledger is the general bounded-work pattern.** An obligation proportional
   to object size is installed as **one small record**, atomically with the transition that
   created it, and drained in **byte-budgeted batches** (`≤ E_tx/2` of mutations per commit, the
   batch inventory of §3), idempotently. This replaces inline fan-out on *every* operation that
   removes the last reference to a large map:
   - **every superseding publication** — including the ordinary single-PUT overwrite path,
     because a 5 MiB `PutObject` over a large object has exactly the same fan-out.
     `commit_chunk_map_superseding{,_leased}` therefore stops expanding the prior map inline and
     instead writes `retire:bytes:{generation}` carrying the superseded map (its chunks, and its
     `seg:` range if it was segmented, decision 7f);
   - **and object delete** — `DeleteObject` and the bulk `DeleteObjects` ([#509][i509]). Today
     `metadata::unlink` orphan-marks **every** fragment the removed object placed **inline, in one
     batch** (`crates/core/src/metadata.rs:514-531`). For a max segmented object that is
     `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS × 9` fragment orphans in one commit — **463 K to 1.78 M
     puts** (the segmented ceilings, decision 7g), permanently over `E_tx` (the iteration-3
     finding-5 hole, outcome (d)). `unlink` therefore also stops expanding inline: it removes the
     dirent + inode and installs **one** `retire:bytes:{generation}` (the removed map's chunks and
     its `seg:` range), which the drain orphan-marks in `B`-batches. The reader-safe grace clock
     (`orphaned_at`) is stamped by the **drain** when it writes each `orphan:` mark — i.e. grace
     starts *at drain*, **later** than today's inline mark at the unlink commit
     (`crates/core/src/metadata.rs:470-486` stamps it in the delete batch today). Because the drain
     mark is strictly *later*, reclamation is never *earlier* than today, so a GET-during-DELETE is
     untorn exactly as today (issue #364) — the retention window is if anything slightly longer, the
     bounded cost registered in *Accepted costs*; only the *fan-out* moves off the request path. A small flat object may keep the inline path (its fan-out fits one batch); the
     retirement route is **mandatory above one batch's worth of fragments** and is the single path
     for both flat-large and segmented.
     **The multi-object case is byte-budgeted too — the `DeleteObjects` adjudication (normative).**
     A single `DeleteObject` installs one O(1) obligation, but a **bulk `DeleteObjects`**
     ([#509][i509]) removes up to **1,000 keys per request** (the S3 limit), each installing its own
     `retire:bytes:{generation}` record (≤ `V`): committed in **one** transaction that is
     `1,000 × V = 100 MB` of *obligation-installation* mutations — **10× `E_tx`**, a permanent commit
     failure, outcome (d). So the obligation-installation is **itself byte-budgeted, not only the
     fan-out drain it schedules**: a bulk delete commits its per-object (dirent + inode) unlink +
     `retire:bytes:{generation}` installs in **`B`-batches of `≤ E_tx/2` mutation bytes**
     (≈ `⌊(E_tx/2)/V⌋ = 50` max-generation obligations per transaction, more for smaller records),
     each transaction preconditioned per-object (`require(dirent == prior)`, `require(inode ==
     prior)`), **never all 1,000 in one transaction** — the same byte-budget rule the fan-out drain
     obeys, applied one level up to the *installation*. This proposal **settles the `≤ E_tx/2`
     per-transaction bound as normative**; the per-request batching mechanics of the multi-delete
     verb (and its per-key S3 response, so a transaction that fails a precondition reports exactly
     those keys as undeleted) are **#509's to implement inside that bound** — not a contract this
     proposal leaves open (X54).
2. **Evidence precedes the lifting of protection, always.** The drain writes `orphan:` records
   for a batch of fragments *before* deleting the record that protected them. There is
   therefore no instant at which a durable byte is both unprotected and unevidenced —
   invariant (2) restated as an ordering rule. It also means the drain can never reclaim
   earlier than today's inline orphaning, only later: a strictly safe direction, at the cost of
   a longer retention window (registered as an accepted cost).
   **An `orphan:` mark is written only when absent — never re-stamped (normative; iteration-7
   finding 3).** `orphaned_at_millis` **is** the grace clock GC evaluates
   (`crates/core/src/metadata.rs:485`, `gc.rs:171-176`), so overwriting an existing mark silently
   restarts that fragment's grace window. Every mark this protocol writes — the retirement drain,
   the owned-`sidx:` walk, the repoint pre-marks — therefore carries a **per-position precondition
   chosen by what the writer read** (the three-arm rule below, iteration-9 finding 3): a position
   the writer observed **absent** is written under `require_absent(orphan:<pos>)`; a position it
   observed carrying a **different** unreference-event identity is *replaced* under an exact-value
   **`require(orphan:<pos> == prior)`**, since `require_absent` would make the very replacement the
   stale-evidence rule demands impossible; and a position carrying the **same** identity is skipped
   with no mutation at all. The blanket "`require_absent` for every mark" of the iteration-7 text was
   right for the concurrency property and wrong for the reader-safety one — it is stated per arm
   here so the two cannot be conflated again.
   A writer that finds the mark present **with its own event identity** treats the fragment as
   **already evidenced**:
   it skips that position, keeps the original stamp, and proceeds with the rest of its batch. This
   is exactly the restore pass's rule, whose `already_marked` counter exists to report it
   ("re-stamping would reset the grace clock and delay their reclamation",
   `crates/custodian/src/restore.rs:108-113`), made normative here because this protocol admits
   **two concurrent drainers by design** — a gateway draining its own teardown inline and the
   reaper draining the same session (§2's exit table). A `retire:`-driven step is already
   single-winner through its obligation CAS (`require(retire:… == prior)`), but the owned-`sidx:`
   walk is reference-derived and has no such record to serialize on, so without the per-mark guard
   the two drainers could each mark the same fragment and push its reclamation out by a full grace
   window per collision — unbounded under a persistent racer, and a contradiction of X12's
   "re-entry skips already-marked fragments, grace clock intact". A batch that loses the guard
   re-reads and re-splits, marking strictly fewer positions each round, so it converges.
   **The skip is scoped to ONE retirement: a mark that predates the reference being removed is
   stale evidence, not a duplicate, and MUST be re-stamped (normative; iteration-9 finding 3).**
   An `orphan:` mark can legitimately coexist with a *live* reference, because GC's safety gate
   tests the reference set **first** and lets a live reference override orphan evidence
   (`gc.rs:160-170`) — the rollout-skew case is exactly this (an old custodian without decision 2's
   staged awareness marks live staged fragments via the restore pass, "Backward compatibility"),
   and those marks survive the upgrade that stops new ones. Given such a mark, already aged past
   `G_orphan`, an unconditional "present ⇒ skip" would be a **reader-safety** defect rather than a
   leak: a later supersede/delete removes the last reference, the drain finds the ancient mark and
   keeps its ancient stamp, and GC — now seeing the fragment unreferenced with grace long elapsed —
   reclaims it on the very next pass, giving a GET that overlapped the deletion **no grace window
   at all**, which is the one thing `G_orphan` exists to provide. So the mark **carries the
   identity of the unreference event that wrote it** (the `retire:` token for a drained obligation,
   the `<upload-id>:<epoch>` for an owned-`sidx:` walk, the `{inode, version}` for a superseded
   generation), and the guard reads:
   - the mark is **absent** ⇒ write it, stamped now;
   - present with the **same** event identity ⇒ a duplicate of *this* retirement (the second
     concurrent drainer, X56) ⇒ skip it, original stamp intact — the iteration-7 property, unchanged;
   - present with a **different** event identity ⇒ **stale evidence predating the reference this
     event just removed** ⇒ re-stamp it with the new event's identity and a fresh `orphaned_at`,
     starting a full `G_orphan` for the deletion that is actually happening now.

   Re-stamping is bounded to **once per unreference event per position**, so it cannot be driven in
   a loop by a racing pair (they share an identity and take the skip arm) — the two properties are
   independent, and conflating them is what made the earlier rule wrong in one direction while
   fixing it in the other.
   **A pre-mark must still stand when the move that relies on it commits.** The repoint and
   re-place moves (decision 2, X47/X29) write `orphan:<P_new>` *before* the destination fragment,
   then adopt it in a later CAS batch. A `orphan:` mark is a **grace-limited** promise: if the
   window between the pre-mark and the adoption CAS exceeds `G_orphan` — a paused reconstruction,
   a requeued obligation — GC may reclaim the pre-marked destination fragment, and a CAS landing
   afterwards would publish a placement pointing at bytes that are gone (outcome (c)). The
   adoption CAS therefore also carries **`require(orphan:<P_new> == prior)`**, so a repoint that
   lost its window fails and restarts from a fresh pre-mark and a fresh fragment write.
   **But that precondition alone is not sufficient, and the window IS load-bearing (iteration-9
   finding 1).** The `orphan:` key outlives the bytes it evidences: GC deletes the *fragment*
   inside its per-server loop and only accumulates the matching `delete(orphan_key(..))` into a
   `cleanup` batch committed **after the whole fleet sweep** (`crates/custodian/src/gc.rs:189-207`).
   So between `delete_fragment` and that commit there is a window — as long as the rest of a fleet
   sweep — in which `orphan:<P_new>` still holds **exactly** its pre-mark bytes while `P_new`'s
   fragment is already gone. A repoint whose pre-mark had aged past `G_orphan` would pass
   `require(orphan:<P_new> == prior)` in that window and publish a placement naming deleted bytes
   (outcome (c)) — the precondition proves the *ledger* entry survives, never the *fragment*.
   Proving fragment presence from metadata is not available (and a D-server existence probe is
   TOCTOU against the same sweep), so the bound is restored to the timing, where it can be made
   sound: the pre-mark → adoption window is a **deadline `W_repoint`**, the adoption CAS
   **refuses fail-closed if its own pre-mark is older than `W_repoint`** (read from the
   pre-mark's own `orphaned_at_millis`, so the check needs no extra record), and
   **`G_orphan > W_repoint + δ_clock`** holds **strictly** under the one deployment wall clock.
   Then GC's reclamation test (`now ≥ orphaned_at + G_orphan`, `gc.rs:171-176`) cannot have fired
   for any pre-mark an adoption is still permitted to use, so the deleted-fragment window is
   unreachable rather than merely narrow. This is the identical construction to
   `G_orphan > W_write + δ_clock` for the late-landing straggler (finding 3, X49), applied to the
   other end of the same grace window; the earlier claim that "safety rests on the precondition,
   not on the timing" was wrong, and both bounds are now stated in the knob table.
3. **Admission bounds what cannot be drained: the map, the part, the segment, the session's
   staged total, and the session population.** `MAX_MAP_CHUNKS` caps a flat map, `MAX_SEG_CHUNKS`
   a segment record, `MAX_PART_CHUNKS` a part record, `MAX_ROOT_SEGMENTS` the segments a root may
   name (decision 7), `MAX_STAGED_CHUNKS` the chunk-refs one session may hold in committed part
   records (below), and `MAX_SESSIONS` / `MAX_INFLIGHT_PARTS` the namespaces (decision 6). Each is
   enforced by a **refusal**, never an over-envelope commit. For the *map* ceiling, `UploadPart`
   refuses **best-effort** early (a cumulative read-only check that concurrent commits can race,
   **D-E**, F17) and Complete refuses **authoritatively** at the fence, where the part set is
   frozen — the authoritative check is what that bound rests on; the early one is a courtesy.
4. **A session may not stage more chunk-refs than it could ever publish — `MAX_STAGED_CHUNKS`,
   and the overshoot is bounded (the 2026-07-24 sign-off call).** The reference build charges every
   committed part's chunks against the reconcile memory budget (decision 2), so what a session may
   *stage* — not what it may publish — is what sets `U_ref` and therefore `MAX_SESSIONS`. Charging
   the raw staging capacity `MAX_PARTS_PER_SESSION × MAX_PART_CHUNKS` charges ≈ 3.7 TiB of parts
   per session at maximal in-range parts, roughly **19× more than the ~193 GiB segmented ceiling
   Complete would let that session publish**, and it collapsed the derived `MAX_SESSIONS` to ≈ 1.
   The ceiling closes that gap: a part commit is refused with `400 EntityTooLarge` (session left
   usable and abortable) once the session's committed parts would exceed `MAX_STAGED_CHUNKS`,
   whose settled value is the publishable ceiling `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS`. The check
   is a **read-only cumulative check** over the bounded `psum:<id>:` range of *summaries* (§1 —
   tens of bytes per part, so it never reads the fat chunk lists), and like every read-then-commit
   check concurrent commits can race it. **Here that race is bounded, and that is what makes the
   ceiling usable in a derived bound:** at most `MAX_INFLIGHT_PARTS` part commits can be in flight
   at once — enforced by the `slot:` key space, not observed — each adding at most
   `MAX_PART_CHUNKS`, so the true staged total never exceeds
   `MAX_STAGED_CHUNKS + MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`. That headroom term is charged in
   `U_ref`, so the bound holds **for every interleaving**, not on average. This is *not* the
   headroom argument iteration 2 rejected for session admission: there the racing population was
   fleet-wide client creates, which nothing capped, so the overshoot was unbounded; here the racing
   population is itself capped by an enforced per-session key space one level down.
   The same ceiling bounds **Complete's own assembly read**: the completer holds at most
   `MAX_STAGED_CHUNKS` chunk-refs (≈ 60 MB at `b_ref ≈ 302 B`) rather than the raw part-number
   space's ≈ 1.1 GB — a gateway-memory bound that fell out of the same call.
   **What it costs:** S3 permits staging parts you never name at Complete, and a client that stages
   more than one publishable object's worth is now refused. Registered in *Accepted costs*.

**Correctness-relevant knobs — valid ranges settled here, values chosen by the implementing
slices.** A knob a safety property depends on has its range and bounding invariant settled in
this proposal; only a knob whose entire range is safe is the implementer's freely.

| Knob | Valid range | Bounding invariant | Value chosen by |
|---|---|---|---|
| `MAX_MAP_CHUNKS` | `> 0`, and `max_chunkref_bytes × MAX_MAP_CHUNKS ≤ V / 2` | a flat inode value stays inside the backend value ceiling with 2× headroom | #508 |
| `MAX_SEG_CHUNKS` | same rule against a `seg:` record | ditto for segment values | #508 |
| `MAX_PART_CHUNKS` | `> 0`, and `max_chunkref_bytes × MAX_PART_CHUNKS ≤ V / 2` (**identical** to `MAX_MAP_CHUNKS`: a `part:` record is one value) ⇒ **165–381** | a `part:` value stays inside the ceiling; sets `max_part_bytes = MAX_PART_CHUNKS × chunk_size` (**165–381 MiB at 1 MiB**), the enforced per-part refusal at `UploadPart` (decision 4) | #508 |
| `MAX_ROOT_SEGMENTS` | `> 0`, and `max_segref_bytes × MAX_ROOT_SEGMENTS ≤ V / 2` | a segmented root inode value stays inside the ceiling (decision 7) | #508 |
| `MAX_STAGED_CHUNKS` (chunk-refs a session may hold in committed `part:` records) | `[MAX_PART_CHUNKS, MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS]`; settled value = the upper end, the publishable segmented ceiling | a session cannot stage more than it could publish, so `U_ref` charges a real ceiling instead of the raw part-number space. Enforced by a `400 EntityTooLarge` refusal at part commit against the bounded `psum:<id>:` summary range, with the overshoot bounded by the in-flight cap (decision 4.4) — at least one maximal part must remain stageable, hence the lower end | #508 |
| `U_ref` (worst-case per-session staged-reference footprint, **chunk-refs**) | `= min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) × MAX_PART_CHUNKS ,  MAX_STAGED_CHUNKS + 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS )` | the reference build holds, per session, **every** chunk of its committed `part:` records **and** its `MAX_INFLIGHT_PARTS` in-flight owned parts — each part expanding to up to `MAX_PART_CHUNKS` chunk-refs (the iteration-4 finding: a part is **not** one unit). The first term is the raw part-number space; the second is the enforced staged ceiling **plus** the bounded commit overshoot (`MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`) **plus** the in-flight owned entries (the same term again). Whichever binds first is the worst legal footprint (the 2026-07-24 tightening: at maximal parts the raw term charges ≈19× what Complete would let the session publish) | derived from `MAX_PARTS_PER_SESSION`, `MAX_INFLIGHT_PARTS`, `MAX_PART_CHUNKS`, `MAX_STAGED_CHUNKS` |
| `MAX_SESSIONS` | `= min( ⌊W_ref / U_ref⌋ , SCAN_CAP/2 )` — **DERIVED, not a chosen knob**, and the `SCAN_CAP/2` term is a **clamp the implementation applies**, not a range check left to the operator: `W_ref` is sized from host RAM and `U_ref` from the caps, so a legal pairing (a large `W_ref` with small parts) can make `⌊W_ref/U_ref⌋` exceed `SCAN_CAP` and break the reaper's `scan("mpu:")` — the clamp is what makes the two bounds compose | the aggregate staged-reference work is `Σ_sessions (actual footprint) ≤ MAX_SESSIONS × U_ref ≤ W_ref` **by construction, for every part-size distribution**: every session is charged its *worst-case* `U_ref`, so an implementer cannot pick a small-part-derived value that a later large-part session overruns (the iteration-4 C5/T2/T4 defect). Enforced exactly by the serialized counter (decision 6). Also `< SCAN_CAP` so the reaper's `scan("mpu:")` never fails | **derived** (from `W_ref` and `U_ref`) — *not* chosen by a slice |
| `MAX_INFLIGHT_PARTS` | `[1, ⌊SCAN_CAP / (2·MAX_PART_CHUNKS)⌋]` | owned `sidx:` per session `≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS ≤ SCAN_CAP/2` — **enforced by the `slot:<id>:` key space** (indices `[0, MAX_INFLIGHT_PARTS)`, each claimed by `require_absent`), residue counted (F11a), so the per-session teardown scan (and each per-session reference-build range) is always safe. Because the value defines the key space, **raising it is unconditionally safe; lowering it** leaves live sessions holding indices above the new cap until those parts finish — a transient over-cap bounded by the *old* value, never an unbounded one (a rollout note, not a correctness case) | #508 |
| `MAX_OWNED_FLEET` (fleet-wide in-flight owned entries) | `= MAX_SESSIONS × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, and since `MAX_SESSIONS = ⌊W_ref/U_ref⌋` while **either** branch of `U_ref` is `≥ 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` (the second branch charges that term twice by construction; the first does whenever `MAX_INFLIGHT_PARTS ≤ MAX_PARTS_PER_SESSION`), this is `≤ W_ref / 2` — at most half the reference budget, under both the raw and the ceiling-bound derivation | owned entries live under the disjoint `sidx:` prefix, so **no global `scan("pending:")` (restore/sweep) enumerates them** (finding 3); they are read only through per-session `sidx:<id>:` ranges each `≤ SCAN_CAP/2`, whose *sum* is already inside the `W_ref` charge above | derived (not a free knob) |
| `W_ref` (staged-reference per-pass **memory** budget, chunk-refs) | `[U_ref, deployment RAM budget]` — a memory ceiling, **not** `SCAN_CAP` (that bounds a single scan; `W_ref` bounds the whole in-memory staged reference set, ×9 fragment pairs) | `MAX_SESSIONS × U_ref ≤ W_ref`: the reconcile pass's staged-reference read+memory budget, charging **every** committed `part:` chunk and in-flight owned `sidx:` chunk (finding 4 and the iteration-4 per-chunk fix; corrects the ~10^10-read and the part-as-one-unit claims). Raising it is more RAM or a smaller `U_ref` | #625 (sized to the reconcile host's RAM) |
| `chunk_size` (assembled writes) | `[1 MiB, chunk_size_max]` | `ceil(max_object_bytes / chunk_size) ≤ MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS`, **and** `chunk_size × max_concurrent_encodes ≤ the gateway memory budget` | #508 |
| `B` (drain / segment batch) | **byte-budgeted**: mutation bytes per commit `≤ E_tx/2 = 5 MB`, so the count is *derived* (`⌊(E_tx/2)/(bytes per mutation)⌋`) — **not** a fixed number | one commit stays inside the `E_tx = 10 MB / 5 s` envelope *regardless of value size*: ~1,000 small `orphan:` marks (the `MARK_BATCH` precedent, `restore.rs:93-100`) but only ≤50 `seg:` writes at `V = 100 KB` | #625 |
| `R_publish` (publish retries) | `[1, small]` | bounded so Complete terminates; exceeding it releases the fence rather than looping | #508 |
| `MAX_COMPLETE_ATTEMPTS` (fences per session) | `[1, small]` | each Complete fence mints a new `Completing@E` epoch whose rolled-back segments become one `retire:records:{seg:<id>:<E>}` obligation, so an unbounded fence/rollback cycle would let **one** session install obligations and `seg:` generations without bound (`W_session` alone does not bound it — a client can loop faster than the drain). The counter lives in the session record, which every fence CASes anyway; at the cap Complete is refused and the session's only exit is Abort | #508 |
| `W_write` / `G_orphan` (fragment-write deadline / orphan grace window) | `W_write > 0`, enforced **at both ends** (a fail-closed caller await **and** a D-server refusal of a write past its deadline), and **`G_orphan > W_write + δ_clock`** (**strict**, with a clock-resolution/skew margin `δ_clock`) under the one deployment wall clock | a fragment authorized before a fence lands within `W_write` of its authorization (the write is deadline-bounded, not merely un-renewed), which `G_orphan > W_write + δ_clock` keeps **strictly** inside its landing position's orphan grace — the boundary `G_orphan == W_write` would let GC's inclusive `≥` grace check reclaim the evidence in the same tick the straggler lands, so no straggler is ever unevidenced (finding 3, clock-lifecycle table) | write path (`W_write`) / #625 (`G_orphan`, today's grace window) |
| `W_repoint` (pre-mark → adoption deadline, iteration-9 finding 1) | `W_repoint > 0`, enforced fail-closed at the adoption CAS (which reads its own pre-mark's `orphaned_at_millis` and refuses past the deadline), and **`G_orphan > W_repoint + δ_clock`** (**strict**, same `δ_clock`, same deployment wall clock) | the other end of the same grace window as `W_write`. GC deletes a fragment inside its fleet loop but commits the matching `orphan:` key delete only after the whole sweep (`gc.rs:189-207`), so the ledger key **outlives the bytes** and `require(orphan:<P_new> == prior)` cannot witness fragment presence. `G_orphan > W_repoint + δ_clock` makes GC's `now ≥ orphaned_at + G_orphan` test unable to have fired for any pre-mark an adoption may still use, so a repoint can never adopt a reclaimed destination (X61) | #625 (`G_orphan`) / reconstruction + rebalance (`W_repoint`) |

**Invariant preserved.** (4) Every obligation installed at publication or teardown is drained
in bounded work, and replay safety is built into each batch (each drain step carries
`require(retire:… == prior)`; each orphan put is idempotent) rather than assumed — the
explicit demand of `crates/traits/src/lib.rs:833-843`. And **every namespace the protocol
grows is bounded by an enforced admission formula or accessed only through bounded key ranges**
(decision 6).

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Keep inline orphan expansion for supersede "because large objects are rare" | Publish an object of `MAX_MAP_CHUNKS` chunks, then overwrite it with a 1-byte `PutObject` against FoundationDB; the overwrite MUST commit. Inline expansion exceeds the envelope and fails permanently. |
| Keep inline orphan expansion for **`DeleteObject`** of a large/segmented object (the iteration-3 finding-5 hole) | `DeleteObject` (and one member of a bulk `DeleteObjects`) on a max segmented object against FoundationDB MUST return in one O(1) batch, its fan-out (up to ~1.78 M fragment orphans) drained asynchronously. Inline `unlink` expansion (`metadata.rs:514-531`) puts that fan-out in one commit — permanently over `E_tx`, outcome (d). |
| Install the obligation in a batch *after* the publication | Kill between publication and obligation. Assert no fragment is ever unreferenced *and* unevidenced: the classification sweep (below) finds it in neither the reference set, nor `pending:`, nor `orphan:`, nor a `retire:` payload. |
| Drain by deleting the staging record first, then marking orphans | Kill mid-drain after the deletes; the fragments are unprotected and unevidenced — same sweep, same failure. |
| Choose `B` as a fixed count rather than a byte budget (e.g. 1,000 for both orphan marks and `seg:` writes) | A segment-write phase for a `MAX_ROOT_SEGMENTS`-segment object against FoundationDB: assert every commit's **total mutation bytes** ≤ `E_tx/2`, so a `seg:` batch never carries more than `⌊(E_tx/2)/V⌋` records; a fixed count of 1,000 × `V` = 100 MB exceeds the envelope and fails permanently. A 10,000-part teardown likewise: assert the maximum observed batch *bytes*, not just count. |
| Derive `U_ref` from the raw part-number space while `MAX_STAGED_CHUNKS` refuses long before it (the 2026-07-24 tightening), or enforce the staged ceiling with an *unbounded* overshoot | Two observables. (a) Arithmetic: assert the deployed `U_ref` equals `min(raw, MAX_STAGED_CHUNKS + 2·MAX_INFLIGHT_PARTS·MAX_PART_CHUNKS)` and that `MAX_SESSIONS = ⌊W_ref/U_ref⌋` follows — a build charging the raw term at maximal parts admits ≈1 session where 19 are safe, and one charging *only* the ceiling (no headroom) under-bounds. (b) Race: fire `MAX_INFLIGHT_PARTS` concurrent part commits at a session already just below `MAX_STAGED_CHUNKS`; the resulting staged total MUST stay `≤ MAX_STAGED_CHUNKS + MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, and the next commit MUST be refused `400 EntityTooLarge` with the session still abortable. A design that admits part commits without the in-flight cap has no bound on that overshoot at all. |
| Enforce `MAX_MAP_CHUNKS` / the segmented ceiling only at `UploadPart` (treat the early check as authoritative) | Concurrent `UploadPart`s that each read sub-cap and collectively overshoot (F17): they MAY all be accepted (best-effort), but Complete MUST refuse authoritatively at the fence; assert Complete rejects the over-ceiling assembled map with a defined S3 error and leaves the session abortable. |
| Enforce it nowhere and let the backend error | Complete an object past the ceiling: the answer MUST be a defined S3 error, and the session MUST remain abortable (never a wedged session whose Complete always errors). |
| Accept a part whose chunk list exceeds one `part:` value (`> MAX_PART_CHUNKS`, i.e. `> max_part_bytes`) and let it fail at the `part:` commit | `UploadPart` of a part larger than `max_part_bytes = MAX_PART_CHUNKS × chunk_size` MUST be refused with `400 EntityTooLarge` **before** the over-`V` `part:` commit; assert the session stays usable and no over-`V` value is ever committed (the hidden-ceiling class, surfaced not hit). |
| Let `retire:records:` orphan bytes (mode confusion) | Complete a session, drain it fully, then assert **no** `orphan:` record exists for any chunk in the published map. |

---

### Decision 5 — Reclamation evidence for failed in-flight work

**The problem, precisely.** A part upload that is fenced or refused mid-stream leaves owned
staging entries. The deployed GC runs `ExpiredPendingPolicy::Defer` — correctly, because
reclaiming on a producer's stamp is unsound when producers do not share the reconciler's clock
(`crates/custodian/src/gc.rs:77-104`, the #557 class; the deployed loop takes the policy as a
parameter whose contract names `Defer` as the deployed default,
`crates/server/src/custodian.rs:456-464`, and the CLI passes `Defer` unless the operator
attests `--gc-expired-pending`, `crates/server/src/cli.rs:975-979`). So that residue is never
reclaimed by expiry in deployment. The everyday case is Ctrl-C during `aws s3 cp` with parts in
flight.

**Decision — reclaim by reference, not by expiry; a fast path and a per-session backstop.**

1. **Owned staging entries are a disjoint record class, not `pending:` entries, and they carry
   the chunk's placement.** Every staged chunk of an in-flight part is registered by
   `write::intent` as its owned entry
   `sidx:<upload-id>:<part-number>:<chunk-id> → PendingEntry{ owner: Some(<upload-id>), lease,
   staged: Some(placement) }` (§1), written *before* any fragment reaches a D server
   (`crates/server/src/lib.rs:167-168`, `crates/core/src/write.rs:198-214`) **and in a batch
   preconditioned `require(mpu == Open@E)`** (the finding-1 serialization edge). The `staged`
   placement is the `WritePlan`'s per-fragment D-server vector (`crates/core/src/write.rs:45-54`),
   the same addresses a committed `ChunkRef` would carry. Three properties follow, all load-bearing:
   - **no staged byte can exist without a `sidx:` entry naming it** — the intent precedes the
     fragment, so the backstop that walks `sidx:` finds every in-flight fragment; and
   - **owned entries never share the `pending:` prefix**, so the global `scan("pending:")` of the
     restore pass (`restore.rs:417-429`) and the expiry sweep (`gc.rs:296-313`) never enumerates
     one — which keeps the doc's "no global scan touches owned entries" claim true and preserves
     the ordinary `pending:` scans' existing bound (owned entries are read only through per-session
     `sidx:` ranges; the fleet-wide owned population `MAX_OWNED_FLEET` is bounded by the
     staged-reference work budget `W_ref`, decision 6, not by any single scan — the iteration-3
     finding-3 fix); and
   - **every fragment's address is on its `sidx:` record**, so the reclaimer can orphan-mark the
     chunk's *entire* placement (`orphan:<placement[i]>:<chunk>:<i>` for every `i`) from the record
     alone, and drain can count each `(placement[i], {chunk, i})` as held — the iteration-4 fix.
   Because the intent is fenced on `Open@E`, once a session leaves `Open` (Complete's or Abort's
   or the reaper's fence) **no further owned entry can be created for it** — so the backstop's
   single per-session `sidx:` walk sees a *frozen*, complete set, and nothing can appear after it
   (the finding-1 teardown/fence race, closed at creation rather than patched at teardown).
   **A fragment write authorized *before* the fence but landing *after* it is still evidenced**
   (the iteration-4 late-fragment race, findings 3/5/6). Two properties make this hold, and the
   second is a *bound*, not the renewal-loop hand-wave iteration 5 relied on:
   - **Position coverage.** The reclaimer orphan-marks the chunk's full `staged` placement — every
     position the fragment *could* land on — so a late-landing fragment lands on a position an
     `orphan:` record already covers. The placement is fixed at intent (§1), so the reaper marks
     exactly the straggler's landing positions regardless of *when* it lands.
   - **A landing deadline the grace window covers.** Refusing to *renew* does not *cancel* an
     already-authorized in-flight fragment write, so the renewal refusal alone bounds nothing (the
     finding-3 hole in the "≈15 s" claim). The bound is instead the **fragment-write deadline
     `W_write`**: every fragment write to a D server is a bounded, fail-closed await (the rubric's
     *await discipline* MUST — every await on external work is timeout-bounded, `AGENTS.md:181-183`),
     so an authorized write either lands within `W_write` of its authorization or is abandoned; it
     can **not** land arbitrarily late. **`W_write` is an end-to-end deadline, not merely a caller
     timeout** — a caller-side `await` timeout bounds how long the *writer waits*, not when an
     already-accepted fragment write *takes effect* on the D server, so the bound is only real if
     the D server enforces it too: the fragment write carries its authorization instant, and the D
     server **refuses** (rather than queues) a write whose deadline has passed, so no accepted
     write can be applied more than `W_write` after it was authorized. That refusal is the
     implementing slices' obligation and its DST observable is in the failure-mode table below;
     without it `G_orphan > W_write + δ_clock` bounds nothing, because a write parked in a server
     queue can land arbitrarily late. The reaper's `orphan:` grace `G_orphan` is required to
     satisfy **`G_orphan > W_write + δ_clock`** — a **strict** margin, where `δ_clock` bounds the
     deployment wall clock's resolution and any skew between the two evaluation sites (the write
     path's deadline check and GC's grace check) — under the **one** deployment wall clock that owns
     both (the grace is evaluated at `gc.rs:171-176`; the clock-lifecycle table records the
     coupling). The strictness is load-bearing: at the boundary `G_orphan == W_write`, GC's
     `now − orphaned_at ≥ G_orphan` test (`gc.rs:171-176`, an inclusive `≥`) could fire at the
     **exact** tick a fragment authorized at its deadline lands, reclaiming the evidence in the same
     instant it is needed (the finding-3 boundary race). With the strict margin `G_orphan` exceeds
     the worst-case landing time `W_write` by at least one clock tick, so a fragment authorized under
     the last live lease lands **strictly before** its position's orphan grace can elapse — the mark
     is still present, and GC never reclaims the evidence before the late fragment is covered by it.
     The renewal loop's *refuse-rather-than-resurrect* (`crates/core/src/write.rs:474-478`)
     is a *supporting* property (it stops *new* authorizations after the fence), not the bound.
   This is the same coverage-plus-deadline mechanism an ordinary abandoned streaming write already
   relies on; multipart adds only that the placement is read from the `sidx:` record instead of a
   committed `ChunkRef`.
2. **Fast path — a live-session loser compensates; nothing else can make it lose
   (iteration-7 findings 1 and 4).** A part-commit batch carries three preconditions —
   `require(mpu == Open@E)`, the part-key precondition (`require_absent(part:…)` or
   `require(part:… == prior)`), and `require(slot:<id>:<k> == prior)` on the committer's **own**
   slot record. Two of the three cannot move under an unrelated part at all — the slot record is
   written by **this attempt alone**, and the session record is a *read* no concurrent part writes —
   and the third, the part key, moves only when a rival attempt for the **same part number** wins
   (X10). So **a commit can no longer be defeated by an unrelated part's
   progress**: the iteration-6 design put a shared `sinf:` counter CAS in this batch, which made a
   durable part's commit fail on a *different* part's slot release, gave the retry no real
   termination bound (fresh part starts kept moving the counter), and needed a third
   classification branch to keep that benign collision away from the compensation path. With the
   slot table there is no shared writable key and no such branch. The store still reports a
   `Conflict` without naming which precondition failed, so the committer **re-reads to classify**
   before it acts (the rubric's *re-read to establish what happened*,
   `traits/src/lib.rs:738-745`):
   - **the `mpu:` record left `Open@E`** — the session fenced under it; the part is genuinely no
     longer acceptable, so the committer answers `404 NoSuchUpload` and does **not** self-compensate
     (that avoids a double slot-release racing the reaper; its residue and slot are the reaper's to
     reclaim, below);
   - **the part key moved** (a concurrent commit of the **same** part number won, X10) — a genuine
     losing writer: it commits one `retire:bytes:{chunks}` obligation for its **own** distinct
     chunks, releases its own slot (`require(slot:<id>:<k> == prior)` + delete, under
     `require(mpu == Open@E)`), and deletes its own `sidx:` entries in one batch, then drains it;
   - **its own slot record moved or vanished** while the session is still `Open@E` and the part key
     is unchanged — while a session is `Open` the only writers of that key are this attempt (its
     release) and, *once released*, whichever later attempt claims the freed index; a reaper
     teardown cannot touch an `Open` session. So this branch is reachable **only after this
     attempt's own release already landed** — it is a retry of a batch that in fact committed
     (X38's re-read case). The committer re-reads the **part record**, finds its own commit
     present, and returns success without compensating. It never re-releases the slot, so no
     double release is representable (iteration-7 finding 4).

   There is deliberately **no third branch** for "someone else's part moved a counter I share",
   because the slot table leaves no such key. A part commit therefore either lands, or loses to a
   same-part rival (compensate, X10), or finds its session fenced (walk away, X7) — an enumeration
   with no starvation state in it.

   A `CommitUnknownResult` is **not** a conflict (the rubric's *Transactions* rule,
   `AGENTS.md:178-180`): the committer re-reads first, and only a settled *not-committed* outcome
   enters the classification above (X38). Compensation, when it does fire, is sound for the same
   reason the orphan ledger is: the `retire:bytes:{chunks}` record is written only once nothing
   references the bytes, so it is clock-independent (`gc.rs:91-94`).
3. **Backstop — reference-based, per-session reclamation, for every non-`Open` state.** When a
   session leaves `Open` (aborted, reaped, fenced by a restore) **or reaches `Completed`** (a
   crashed in-flight part's residue that never became a `part:` record, the iteration-3 finding-2
   hole) or vanishes, the reaper walks that session's owned entries through the **bounded** range
   `scan("sidx:<upload-id>:")` (≤ `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`) and orphan-marks + deletes
   them in `B`-batches, until the range is **empty** — which is the gate the terminal delete waits
   on. Because every `sidx:` intent is fenced on `Open@E` (rule 1), nothing can refill the range
   once the session left `Open`, so the walk terminates in a bounded number of steps and stays
   empty; the surviving `slot:` records, whose only job was to enforce the in-flight cap *while
   `Open`*, are then deleted with the session (a reserved-but-unwritten slot leaves no residue,
   because intent precedes any fragment). The reaper is the **sole** reclaimer for a non-`Open` session, so
   no live-session compensation races it. The judgment uses **no clock at all** — it is a statement
   about which records exist — so it is immune to the #557 trap by construction and needs no
   operator attestation. There is **no global `pending:` scan** of owned entries anywhere in this
   protocol.
4. **Hands off everything else.** `ExpiredPendingPolicy` and the expiry-based sweep
   (`gc.rs:296-313`, `write::sweep_expired_leases`, `write.rs:623-648`) scan only `pending:` and
   so **never touch an owned `sidx:` entry** — their exit is by reference, and expiry-based
   reclamation of them would resurrect exactly the mid-flight-deletion hazard the `Defer` posture
   exists to avoid. The non-multipart `pending:` residue keeps its current posture; this proposal
   does not change it, and multipart does not add to it (owned entries are disjoint).

**In-flight concurrency is admission-capped — enforced by the key space, and residue counts
(**D-D**, F11a).** The bound is *reserved*, not merely *observed* — the distinction the
iteration-2 review made load-bearing. `UploadPart` reserves an in-flight slot **before it streams
any chunk** by claiming one index of the fixed per-session range
`slot:<upload-id>:[0, MAX_INFLIGHT_PARTS)`: it reads that bounded range, picks a free index `k`,
and commits `require_absent(slot:<id>:<k>)` **and** `require(mpu == Open@E)` with the put. A slot
is released by the **delete** in the batch of the part that **commits** or is **compensated** by a
live-session loser, under `require(slot:<id>:<k> == prior)`; once the session leaves `Open` the
slots stop mattering (no new parts) and are deleted with the session.

**Why the key space rather than a counter (iteration-7 findings 1, 2 and 4).** Iteration 6 spent a
per-session integer `sinf:<upload-id>`, CAS'd `+1` at every part start and `-1` at every part end.
Three properties are strictly better here, and the third is what the iteration-7 review demanded:

- **The cap is structural, so overshoot is unrepresentable.** At most `MAX_INFLIGHT_PARTS` keys
  can exist in the range, whatever the concurrency, because each is claimed by its own
  `require_absent`. A counter enforced the same bound only for as long as every writer CAS'd it
  correctly. This is [ADR-0046][a46]'s scan-then-commit warning respected rather than dodged: the
  range read *chooses* an index, the per-key precondition *authorizes* the reservation.
- **Reservation terminates on a bound anyone can state.** A reserver that finds index `k` taken
  probes the next free one — at most `MAX_INFLIGHT_PARTS` probes, each against a *different* key —
  and answers `503 SlowDown` when the range is full. That refusal is the designed backpressure, not
  a failure; contention resolves into a refusal instead of a retry loop against one hot key.
- **An unknown reserve outcome is settled by re-reading, never by re-reserving.** This is why the
  slot value carries an `attempt_id`: on a `CommitUnknownResult` for the reserve (the rubric's
  *Transactions* rule, `AGENTS.md:178-180`) the reserver re-reads `slot:<id>:<k>` and compares —
  **its own** `attempt_id` means the reserve landed and it *adopts* that slot. What the *other*
  outcomes prove depends on `CommitUnknownResult::may_still_commit`, and conflating them is a
  defect (iteration-9 finding 8): with `may_still_commit == false` the transaction is already out
  of flight, so one re-read settles it for good (FoundationDB's 1021) — another attempt's value or
  absence both mean this reserve did not land, and the reserver probes the next free index. With
  **`may_still_commit == true`** (FoundationDB's 1031, and *every* TiKV case) **absence proves
  nothing** — the original batch may still be applied afterwards (`crates/traits/src/lib.rs:241-247`).
  A reserver that probed a different index on that evidence could end up owning **two** durable
  slots, and a run of metadata timeouts would exhaust `MAX_INFLIGHT_PARTS` and wedge the session at
  a permanent `503` with no crash to blame. So on an ambiguous outcome the reserver **retries the
  same index with the same `attempt_id`**, never a different one: the two batches are mutually
  exclusive through `require_absent(slot:<id>:<k>)`, so at most one lands and both would carry the
  same `attempt_id`, making either outcome the same observable slot. It probes onward only once it
  reads that index carrying *another* attempt's id (its own reserve definitively lost), and refuses
  `503` after a bounded number of ambiguous retries — leaving any slot the ambiguous batch may yet
  install as ordinary residue the reaper reclaims with the session (F11a). Blindly probing a fresh
  index after an unknown outcome would leak the landed one for the session's whole life — the
  same accounting hazard the release side avoids with `require(slot == prior)`.
- **A durable part commit cannot be starved or discarded.** The counter's `-1` rode in the
  part-commit batch, so a commit failed whenever *another* part released its slot; the retry was
  claimed to terminate in ≤ `MAX_INFLIGHT_PARTS` rounds, which is **false** — fresh part starts
  keep incrementing the same counter, so the round argument has no fixed point and a valid part's
  commit can be starved indefinitely (iteration-7 finding 1). The slot delete touches only the
  committer's **own** key, so concurrent commits of different part numbers now share **no writable
  key at all** and cannot conflict; and because release is a keyed delete under
  `require(slot == prior)` it is exactly-once by construction, which removes the double-release
  hazard X52 was written to police (iteration-7 finding 4).

Crucially, **while the session is `Open` a part upload that crashes mid-stream never releases its
slot**, so its owned `sidx:` residue is *counted against the cap*, not invisible to it: a client
cannot accumulate an unbounded pile of crashed-part residue while its session stays `Open`,
because each crashed attempt holds a slot, and every owned entry is reclaimed per-session when the
session is aborted, reaped, or completed (finding 2). That is the iteration-2 F11a hole closed: the per-session owned
population is `≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` **by enforcement, residue included**, and
the knob range is constrained so that product is `≤ SCAN_CAP / 2` (the knob table), so
`scan("sidx:<upload-id>:")` — the per-session teardown range and the *only* way any pass reads
owned entries — can **never itself cross `SCAN_CAP`** and fail. This is **D-D**'s mandated
in-flight-part cap, distinct from **D-C**'s fleet counter, and it now costs **no shared-key
serialization on the part path at all**: what remains is one `require_absent` on a private key per
part start, one keyed delete per part end, and the per-chunk `require(mpu == Open@E)` **and**
`require_absent(desired:dserver:<S>)` (drain-fence) read
preconditions on each `sidx:` intent — reads, which do not serialize concurrent writers. That is
why the iteration-3/iteration-6 flagged sign-off question about part-boundary serialization is
**resolved by construction** rather than left for the human to bless (Open questions). The fleet-wide
owned population is `≤ MAX_SESSIONS × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` (`MAX_OWNED_FLEET`,
the knob table), reclaimed per-session in byte-budgeted batches and **read only through
per-session `sidx:` ranges** — never touched by any single global scan (finding 3). A session
wedged at `MAX_INFLIGHT_PARTS` by crash residue is a **bounded availability cost** (the client
aborts and restarts; registered in *Accepted costs*), never a safety hole.

**Invariant preserved.** (2) — the residue of a failed in-flight part is
garbage-with-a-sound-reclamation-path within `W_session`, not a fourth, forever-protected class.

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Keep owned entries under `pending:` (so a global `scan("pending:")` enumerates them — the iteration-1 hole and the iteration-3 finding-3 contradiction) | Strand `> SCAN_CAP` owned entries across many `Open` sessions, then run `reconcile_after_restore` **and** the reaper: both MUST proceed. Owned entries live under the disjoint `sidx:` prefix, so `scan("pending:")` never sees them and the reaper walks bounded `sidx:<id>:` ranges per session; a design that keeps them under `pending:` fails `ScanCapExceeded` in *both* passes. |
| Create owned residue after the fence-and-scan, or tear a session down while an in-flight part can still create it (the iteration-3 finding-1 race) | DST: fence a session to `Aborting`, then have a pre-fence in-flight part attempt an intent put; the put MUST fail `require(mpu == Open@E)` (no residue after the fence), and the terminal delete MUST NOT fire while the `sidx:` range is non-empty. A design whose intents carry no session precondition leaves an owned entry nothing can ever discover. |
| Omit the placement from the `sidx:` value, so the reaper cannot compute orphan keys and drain cannot count in-flight fragments (the iteration-4 finding) | The reaper MUST orphan-mark an in-flight owned chunk's fragments from its `sidx:` value alone: assert the exact `orphan:<placement[i]>:<chunk>:<i>` keys are written for each `i`, and that `reconciliation_status` for a drained server holding an in-flight owned fragment is `Pending` (counted via the `staged` placement). A value with no placement can do neither. |
| Delete the `sidx:` entry on teardown without covering a fragment that lands afterward, or bound the landing by the renewal loop instead of a write deadline (findings 3/5/6 late fragment) | DST: authorize a part's fragment write, then fence + reaper-walk the session (orphan-marking the full `staged` placement and deleting the `sidx:` entry), then let the authorized fragment land **after a delay approaching `G_orphan`**. Assert (a) the late fragment lands on an `orphan:`-covered position, and (b) the fragment write is `W_write`-bounded with `G_orphan > W_write + δ_clock` (**strict**), so it cannot land at or after its position's grace elapses even at the boundary tick; a design that sets `G_orphan == W_write` (or bounds the landing only by the renewal refusal, which does not cancel an in-flight write) can leave a straggler at/past grace, unevidenced (X49). |
| Complete a session without reclaiming a crashed in-flight part's owned residue (the iteration-3 finding-2 hole) | Crash one part upload mid-stream (owned `sidx:` residue, no `part:` record), then Complete the session naming other parts and let it reach `Completed` and terminal-delete: the reaper's **Completed-path** `sidx:` walk MUST reclaim the residue and observe the `sidx:<id>:` range **empty** before the terminal delete (which then discards the surviving `slot:` records outright — the crashed attempt still holds its slot; the teardown gate is the empty `sidx:` range, **never** an empty `slot:` range, §2). A Completed teardown that skips the `sidx:` walk strands the residue forever (outcome (a)). |
| Cap `MAX_INFLIGHT_PARTS` on *live* parts only, letting crashed-part residue accumulate uncounted while the session stays `Open` (the iteration-2 F11a hole) | Crash a part upload mid-stream repeatedly against one `Open` session (no commit, no compensation, so no slot release): `UploadPart` MUST start refusing with `503` once all `MAX_INFLIGHT_PARTS` indices are held **by residue**, so that session's owned `sidx:` population never approaches `SCAN_CAP` and its per-session teardown scan cannot fail. A design that counts only live parts admits unbounded residue and `scan("sidx:<id>:")` eventually fails `ScanCapExceeded`. |
| Treat a `CommitUnknownResult` on the **reserve** as a failure and claim a different index | DST: inject an unknown result on a slot reserve whose batch lands, then let the request continue. It MUST re-read `slot:<id>:<k>`, recognise its own `attempt_id`, and adopt that slot — the session's held-slot count MUST equal the number of live attempts, and repeated unknown outcomes MUST NOT walk the session up to `MAX_INFLIGHT_PARTS`. A design that probes a fresh index instead leaks the landed slot until teardown. |
| Release a slot on a `CommitUnknownResult` before the re-read settles (double-release, or release for a part that did commit) | DST: inject an unknown result on a part commit whose batch lands; the writer MUST re-read first — a committed part releases exactly once (the keyed delete in its commit batch), a settled-not-committed part releases in its compensation batch, and a replayed release MUST fail `require(slot:<id>:<k> == prior)` and be a no-op. |
| Put the in-flight cap in a **shared** per-session counter CAS'd inside the part-commit batch (the iteration-6 `sinf:` design, finding 1) | DST: keep `MAX_INFLIGHT_PARTS` part uploads starting and finishing continuously against one `Open` session while one designated part commits. That commit MUST land without depending on any "≤ `MAX_INFLIGHT_PARTS` rounds" argument — with per-attempt slot keys it shares no writable key with the churn, so it cannot lose at all. A shared counter makes it lose to *unrelated* parts' releases and starts, and no round bound holds because fresh starts keep moving the counter: the observable is a valid, durable part whose commit never lands. |
| Admit a slot from the population read rather than a per-key `require_absent` | Fire `MAX_INFLIGHT_PARTS + N` concurrent `UploadPart` starts at one session from distinct gateways: the number of `slot:<id>:` records MUST never exceed `MAX_INFLIGHT_PARTS` and the surplus MUST get `503`. A design that reads the range and then writes an index it merely *believes* free admits every racing reserver (the [ADR-0046][a46] scan-then-commit class), and the owned-`sidx:` bound it underwrites is then only observed. |
| Let two drainers re-stamp the same fragment's `orphan:` mark (iteration-7 finding 3) | DST: run the completing gateway's inline drain and a reaper pass concurrently over one session's owned `sidx:` range with a seeded interleaving that has both reach the same fragment. Assert the fragment's `orphaned_at_millis` after both passes equals the **first** mark's value, and that GC reclaims it on the original grace window. A drain that writes marks unguarded (no `require_absent`) re-stamps the clock, and a repeated race postpones reclamation without bound. |
| Build the staged reference set `part:`-first, `sidx:`-second (no shared snapshot) | DST: begin a reference build, let it read a session's `part:` range, then commit an in-flight part (which deletes its `sidx:` entries and writes the part record), then let the build read `sidx:`. Every chunk of that part MUST still appear in the staged set — with the normative `sidx:`-then-`part:` order it is captured by the owned read; the reverse order captures it in neither, and `reconciliation_status` for the server holding those fragments wrongly reports `Satisfied`. |
| Rely on the expiry sweep for staged residue | Run one deployed-configuration reconcile (`Defer`) after killing a part upload; the residue MUST still be reclaimed — by the reaper's reference arm, with no `--gc-expired-pending` attestation anywhere in the test. |
| Bound the fragment write with a caller-side timeout only (no server-enforced deadline) | DST: authorize a fragment write, park it in the D server's accept queue past `W_write` (the caller has long since timed out), fence and reap the session, and let the parked write proceed. The D server MUST refuse it as past its deadline. A design that bounds only the caller's wait lets an accepted write land after its position's `orphan:` grace elapsed — the straggler is then unevidenced, which is what `G_orphan > W_write + δ_clock` was supposed to prevent (X49). |
| Let the expiry sweep touch owned entries | Stage a part with a *lapsed* lease under `ExpiredPendingPolicy::Reclaim` while its session is `Open`; assert the entry and its fragments survive. Deleting them is the #557 mid-flight-deletion defect with a new name. |
| Register the owned entry after the fragments are written | Assert the **order** directly (a recording store fake: the `sidx:` owned-entry commit precedes the first fragment put), not merely that no un-indexed fragment was observed — the absence is what a passing test would show either way, the order is what makes it true (the rubric's *Absent or unsupported entries* rule against count-only assertions). |
| Omit `skip_serializing_if` on the new `owner` field | Round-trip a legacy `pending:` entry (`owner = None`): `decode → encode` MUST be byte-identical, and a `renew_pending` against it MUST return `Committed`, not `Conflict` (`metadata.rs:748-758`); and round-trip a `sidx:` owned entry (`owner = Some`) through the conformance suite. |
| Compensate with per-fragment orphan puts in one batch | Kill a `MAX_PART_CHUNKS`-chunk (max in-range, ≈381 MiB) part upload; assert every compensation commit is inside the envelope (it goes through the retirement ledger, decision 4). |
| Make the backstop judge by lease expiry rather than session state | A DST run with a logical-clock producer and a wall-clocked reaper: no owned entry of an `Open` session may be reclaimed regardless of stamp skew. |
| Treat a `CommitUnknownResult` part commit as a failure and compensate at once | DST: inject an unknown result on a part commit whose batch *does* land. The writer MUST re-read the part record first; compensating blind would orphan-mark the chunks of a part record that exists — a live staged part pointed at reclaimable bytes, refutation outcome (c). |

---

### Decision 6 — The abandoned-upload reaper (designed here, implemented in #625)

**(a) The protocol-facing half.**

- **The liveness observable, derived only from records the protocol durably writes.** A session's
  *last progress instant* is
  `max(mpu.created_at_millis, max over its part records of committed_at_millis,
  max over its slot records of reserved_at_millis)`, read via the bounded `slot:<id>:` and
  `psum:<id>:` ranges — **summaries, not the fat part records** (§1) — and a session additionally counts as live while **any session-owned `sidx:`
  entry holds an unexpired lease** — which is exactly what a single long `UploadPart` produces,
  because the streaming write renews its owned leases every half-TTL while the call is in flight
  (`crates/core/src/write.rs:474-500`). A large in-range part (up to `max_part_bytes` ≈ 165–381 MiB)
  streaming for a long time is therefore *observably* progressing without inventing a new record or
  a new heartbeat.
- **The progress ranges are read in a fixed order — `slot:` first, then `psum:` (normative,
  iteration-8 finding 2).** This is decision 2's source-before-destination rule applied to the
  reaper's own reads, and for the same reason: a part commit **atomically** deletes its
  `slot:<id>:<k>` and writes its `psum:<id>:<n>` summary (§1, the part-commit batch). A reaper
  that read the **destination** first would see a part that commits between its two reads in
  **neither** range — absent from `psum:` because it had not committed yet, absent from `slot:`
  because it had by then — so `progress` would collapse to `created_at`, and against a session
  whose earlier progress is older than `W_open` with no other live lease the reaper would fence a
  session that had *just* committed a part, discarding durable staged bytes. Reading the
  **source** (`slot:`) first makes the same interleaving observe the part in **both** ranges or in
  `psum:` alone, never in neither. The mirror-image staleness — a `slot:` seen in the snapshot and
  deleted before the `psum:` read — is a **false negative** bounded by one pass: the reaper credits
  a `reserved_at` stamp that is at most one pass old and re-derives the truth next pass, which is
  the safe direction (a session survives one pass too long; it is never wrongly killed).
- **Why the slot record is in that observable (iteration-7 finding 2).** A slot is reserved
  *before* the request streams its first chunk, so between the reserve and the first `sidx:` intent
  a genuinely active `UploadPart` owned **no** lease and had committed **no** part — it was
  invisible to both of the other terms, and a request that spent longer than `W_open` in its
  pre-first-chunk phase (a slow client body, a stalled EC encode, a retried placement) against an
  otherwise idle session was falsely idle-abandoned and reaped mid-request. The slot record closes
  it with **two** distinct pieces of evidence, and the distinction is load-bearing:
  - `reserved_at_millis` is a **fixed** stamp that joins the progress maximum, so a session is
    never idle-abandoned within `W_open` of a reservation; and
  - `lease_expiry_millis` is **renewed in flight** by the same half-TTL loop that renews the owned
    `sidx:` leases, so a request whose pre-first-chunk phase runs *longer* than `W_open` still
    counts as live — a fixed stamp alone could not do that, and claiming it could would simply
    move the false reap from `W_open` after the *session's* last progress to `W_open` after the
    *reservation*.

  A **crashed** attempt renews nothing, so its lease lapses and it stops conferring liveness — its
  slot record stays (residue is deliberately counted against the cap, F11a) but it can no longer
  keep the session alive. Liveness and the cap therefore read different fields of the same record:
  the reaper reads the lease, admission reads the key's existence.
- **Renewal is conditional but NOT atomic with its own expiry test, so the idle fence carries a
  serialization edge to the evidence it judged (normative; iteration-9 finding 4).** Renewal
  refuses a lease it finds lapsed — *"a lapsed lease is dead — resurrecting it would let this
  upload commit an inode over fragments the GC is free to reclaim"* (`write.rs:485-504`, issue
  #490), and the `slot:` lease renewal inherits exactly those semantics from the owned-`sidx:`
  renewal it shares a loop with. **But that test is evaluated against the renewer's own sampled
  `now_millis`, in the gateway, before the commit** — `renew_pending` compares
  `existing.lease_expiry_millis <= now_millis` at *read* time and then commits under
  `require(key == prior_bytes)` (`crates/core/src/metadata.rs:746-760`). The store enforces the
  bytes, never the expiry. So a renewal that read a **live** lease can still commit *after* the
  reaper has read that same lease as expired, and the reaper does not touch lease keys, so the
  renewal's precondition holds and it succeeds. The fence's `require(mpu == Open@E)` then also
  succeeds, and an upload that is progressing again is aborted before `W_session` — a false
  positive of exactly the class decision 6's failure-mode table forbids. Wall-clock agreement does
  not close it; this is a read-then-act TOCTOU across two different keys.

  **The fence therefore preconditions on the liveness evidence it read.** The idle arm's fence
  batch carries, for the session's whole `slot:<id>:` range as observed in this pass:
  `require(slot:<id>:<k> == prior)` for every index it saw **present**, and
  `require_absent(slot:<id>:<k>)` for every index it saw **free**. That is `≤ MAX_INFLIGHT_PARTS`
  preconditions — the same bounded key space the terminal delete already enumerates, so it costs no
  new scan and no new record. It is sufficient because **every live renewer holds a slot**: a slot
  is reserved before the first chunk and released only at part commit or compensation, and the
  half-TTL loop renews the slot lease together with that request's owned `sidx:` leases, so the
  slot record is a per-request liveness witness that any renewal necessarily rewrites. A renewal,
  a release, or a *new* reservation between the reads and the fence therefore turns the fence into
  a `Conflict`, and the reaper simply re-derives its decision next pass — the safe direction, and
  one that converges because a genuinely abandoned session stops rewriting slots. Preconditioning
  on the owned-`sidx:` range instead would be unbounded (`≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`
  preconditions); the slot range is the summary that makes the edge cheap (X62).

  Arm (ii), the `W_session` residency ceiling, deliberately fences a *demonstrably live* session
  and carries **no** such precondition — that is the administrative bound (**D-A**), not a race,
  and making it conflict on renewal would defeat the ceiling it exists to enforce.
- **The clock guard gates JUDGMENTS, not the whole pass (normative; iteration-9 finding 11).** The
  guard exists because reaping on a producer's stamp when producers do not share the reconciler's
  clock is the #557 defect class — so it must suppress every arm that *compares a foreign
  timestamp*: the `W_open` idle arm, the `W_session` ceiling, the `W_completing` rollback, and the
  `W_tombstone` expiry. It must **not** suppress the work that reads no timestamp at all. Skipping
  the session wholesale (the earlier text) left a foreign-clocked session that had already reached
  a **terminal** state with no driver whatsoever: a client can Abort it, or it can Complete, before
  the reaper ever sees it, and if the gateway then dies the owned-`sidx:` walk and the terminal
  delete never run — records and the admission slot resident forever, and the operator abort verb
  cannot help because a `Completed` session is not abortable (it answers `404`). That is an
  absorbing state reached without any clock disagreement being *acted* on, which is precisely what
  the guard was supposed to prevent. So for a foreign-clocked session the reaper still performs the
  **clock-free** teardown: the owned-`sidx:` walk for `Aborting`/`Completed`, the `retire:` drain,
  and the terminal delete for `Aborting` — none of which reads a session timestamp. The single
  residue is a **`Completed`** session's tombstone window, which is measured from that session's own
  `completed_at_millis` and therefore genuinely unjudgeable under a foreign clock; it holds its
  records until an operator authorizes expiry, which **FU-6 gains as a second verb beside the
  abort** (an authorized expiry for a terminal foreign-clocked session, since abort does not apply).
  The alarm still fires on every foreign-clocked session the pass meets.
- **Abandonment rule (two arms).** A session in `Open` is fenced to `Aborting` when **either**:
  (i) it is **idle-abandoned** — its last progress instant is older than `W_open` *and* it
  owns no unexpired lease on any `sidx:` entry **or `slot:` record**; **or** (ii) it has reached the **administrative residency
  ceiling** — `now - created_at_millis > W_session` (**D-A**), regardless of liveness. Arm (i)
  reclaims the walked-away client; arm (ii) bounds *every* session's residency (F14) and is what
  re-derives the drain bound (F6) and models Amazon's `AbortIncompleteMultipartUpload`. Both
  arms read only records and the deployment wall clock — the reaper stays **record-only**, with
  no topology coupling (**D-A**). `Completing` sessions use the separate, shorter
  `W_completing`, measured from `fenced_at_millis` (**D-E**, F16), and are **rolled back**
  (`Completing → Open`), never reaped directly — rollback is always safe because the epoch bump
  invalidates any publication batch the crashed completer might still land. The one
  `Completing → Aborting` edge in the state machine belongs to the **restore fence** (**D-B**,
  X57), not to the reaper: a restore has declared every open session dead, so there is no later
  attempt to preserve.
- **What the reaper may exit** — every state in §2's exit table: idle-abandoned or over-age
  `Open` (fence to `Aborting`), stale `Completing` (roll back to `Open`, cleaning up its
  `seg:` records), any `retire:` obligation (drain), any session-owned `sidx:` entry of a
  **non-`Open`** session — `Aborting` **and `Completed`** alike (decision 5, finding 2) — and
  finally the session record itself (once its `sidx:` range is walked empty) with the counter
  decrement. **Once the reaper exists, no protocol state is unexitable** — this is what closes F1
  globally rather than case by case.
- **The stale-snapshot rule (**D-E**, F15).** The reaper reclaims a session's owned
  `sidx:` entries **only after it has fenced that session** (CAS to `Aborting`, reached
  `Completed`, or confirmed the record absent) **in the same pass** — the step-5 judgment must be
  **no staler than the entry it condemns**. It never filters a snapshot of owned entries against an
  older-than-the-scan session list; it walks each session's `sidx:` range only after that session
  is fenced/terminal, so it can never reclaim the in-flight entries of a session *created mid-pass*
  (which would orphan-mark a live streaming part's fragments and kill its renewals,
  `crates/core/src/write.rs:474-478`, "refuse rather than resurrect"). Since the intent is fenced
  on `Open@E` (finding 1), a fenced session's owned set is *frozen*, so fence-then-walk sees a
  complete set and the stale-snapshot mode is unconstructible; the DST observable is in the
  failure-mode table.
- **The clock lifecycle and its owner (F10, **D-E**).** The abandonment judgment reads the
  **deployment wall clock**, stamped by the gateway into `created_at_millis` /
  `committed_at_millis` / `fenced_at_millis` and evaluated by the reaper — the same
  cross-component lifecycle the overwrite path already uses (`orphaned_at_millis` stamped at
  `crates/core/src/write.rs:305-313`, evaluated against the grace window at `gc.rs:171-176`). It
  is `SystemTime::now()`, which madsim virtualises, so DST determinism holds ([ADR-0009][a9];
  the [ADR-0047][a47] publication timestamp is the worked precedent). **The clock guard is
  derived from the session record, not from the owned entries (the iteration-3 finding-6b fix).**
  The `mpu:` record carries `clock_source`; the reaper reads it **first**, per session, and
  **skips (and alarms on) any session whose `clock_source` it does not own — before it reads that
  session's owned leases at all**. So the owned-lease liveness read of abandonment condition (i)
  only ever happens for a session the reaper's clock owns; the owned `sidx:` entries need **no**
  `clock_source` field of their own (they inherit the session's, transitively, by never being
  evaluated except under an owned session). The fail direction is safe (skew → a false-positive
  reap → the fenced, bounded FU-4 cost). No logical-clock producer may open a session; the CLI,
  the one such producer in tree (`crates/server/src/cli.rs:629-637`, the fixed logical
  `NOW_MILLIS`), has no multipart verb, and the tag keeps that true by evidence rather than by
  assumption.
- **A skipped foreign-clocked session is not an absorbing state — its exit is an operator verb
  (iteration-7 review).** The guard above makes the reaper *decline* to judge such a session, so
  neither abandonment arm can retire it: it keeps its records, its staged bytes and — the part
  that matters — its **admission slot**, and the `W_session` residency ceiling does not apply to
  it, because the ceiling is evaluated against a clock the reaper has just declared foreign.
  Left there it would violate invariant (3). Two things resolve it, and both are normative: the
  skip **MUST** alarm naming the session id and its `clock_source` (never a silent skip), and the
  management surface **MUST** expose an operator-driven abort that fences such a session to
  `Aborting` on the operator's authority — the operator, not the reaper, supplies the judgment the
  clock cannot. Teardown then proceeds by the ordinary, **clock-free** reference-based path
  (decision 5), so nothing downstream needs the foreign clock either. The residual cost is
  bounded and registered: at worst `MAX_SESSIONS` slots parked behind an alarm awaiting an
  operator, which is a capacity loss with a name and a remedy, not silent unbounded growth. In
  deployment this is a misconfiguration signal — no in-tree producer with a logical clock has a
  multipart verb (above).
- **Admission control — the enforceable, exact bound (**D-C**, F7/F12).** `CreateMultipartUpload`
  makes a **serialized slot reservation**: in the create batch it reads the singleton
  `mpuctl` — **an absent record on a fresh or upgraded store reads as `{ count: 0 }`**, and the first
  Create initializes it in the same batch with `require_absent(mpuctl)` + `put` rather
  than a CAS, so the counter is **self-bootstrapping with no migration/init step** and no
  first-create dead-end — refuses with `503 SlowDown` if it is at `MAX_SESSIONS`, and otherwise
  CASes it `+1` together with `require_absent(mpu:<id>)`. A concurrent create that initialized or
  advanced the counter first makes the losing create's precondition (`require_absent` or the value
  CAS) fail, so it retries against the re-read value — the bootstrap is race-safe by the same CAS
  discipline as steady state, and the counter never double-initializes. The terminal delete CASes
  it `-1` (it is never deleted — a fixed singleton, so after the first Create the absent branch is
  never taken again). This is
  the reserve/CAS pattern ([ADR-0007][a7]); the counter counts **all** session records in any
  state (Open/Completing/Aborting/Completed tombstones), so it bounds the whole `mpu:` namespace
  and every namespace derived from it. Contention at the counter under a create storm **is** the
  `503 SlowDown` backpressure — that is by design, and it costs nothing on the hot path because
  **only Create touches the counter; part commits never do** (the retry-storm objection that
  ruled out a per-part counter, decision 1, does not apply to a per-*create* counter). This
  reverses iteration 1's scan-then-create / "no hot counter" stance for **Create only**:
  scan-then-create is race-prone (ADR-0046 flagged the same shape for `DeleteBucket`) and admits
  unbounded overshoot; a cap overrun would halt the maintenance plane, which is data-loss-class,
  so the bound must be **enforced, not observed**.
- **The governing limit is stored with the counter, not derived per gateway (iteration-9
  finding 5).** `MAX_SESSIONS` is **derived** from `W_ref` and `U_ref` (above), which are *local
  configuration*, so during a rolling change two gateways can hold different values while the
  counter only orders their increments — it records how many sessions exist, never which threshold
  admitted them. A gateway still running the larger value would keep admitting after the newer
  gateways and the custodian consider the smaller limit reached, and the claimed
  `MAX_SESSIONS × U_ref ≤ W_ref` bound — the thing that keeps the reconcile pass inside its RAM
  budget — would simply not hold, with the overrun landing on the maintenance plane rather than on
  the gateway that caused it. So `mpuctl` is not a bare integer but
  **`{ count, max_sessions }`**, one record, and:
  - Create CASes the **whole record** (`require(mpuctl == prior)`), admitting only while
    `count < prior.max_sessions` — so **every reserver enforces the value in the ledger**,
    whatever its local derivation says;
  - a gateway whose locally derived `MAX_SESSIONS` **differs** from `prior.max_sessions` **refuses
    to admit and alarms** rather than silently deferring to either value — a fail-closed
    configuration-skew signal, since the disagreement means one of the two hosts is sized for a
    budget it is not getting (this is the same "misconfiguration is visible, not absorbed" stance
    as the reaper-absent refusal in the implementation order);
  - changing the limit is an **explicit operator act** that CASes `max_sessions` on the record.
    Raising it is safe only once the reconcile host actually has the RAM the new `W_ref` claims;
    lowering it below the live `count` needs no drain — admission simply refuses until the count
    falls under the new value, which is the self-correcting direction.

  The bootstrap path is unchanged in shape: the first Create initializes the record under
  `require_absent(mpuctl)` with `{ count: 1, max_sessions: <its derived value> }`, so a fresh store
  adopts the first admitting gateway's derivation and every later one is checked against it.

**The bounding formula, and every namespace bounded.** With the reworked access patterns, the
**only** global scan multipart adds is one bounded `scan("mpu:")`; everything else is a bounded
key range or reference-based:

| Namespace | Bound | How it is accessed (never an unbounded scan) |
|---|---|---|
| `mpu:` | `≤ MAX_SESSIONS`, where `MAX_SESSIONS = ⌊W_ref / U_ref⌋` is **derived** (not chosen) from the staged-reference memory budget `W_ref` and the worst-case per-session footprint `U_ref` (see below and the accepted-costs register), and `< SCAN_CAP` | exact serialized counter (**D-C**); one bounded `scan("mpu:")` in the reaper |
| `slot:` | `≤ MAX_INFLIGHT_PARTS` per session **by key space** (indices `[0, MAX_INFLIGHT_PARTS)`), so `≤ MAX_SESSIONS × MAX_INFLIGHT_PARTS` fleet-wide | per-session range `scan("slot:<id>:")` at part starts and in the reaper; each index claimed by `require_absent`, released by a keyed delete; discarded with the session |
| `part:` | `≤ MAX_PARTS_PER_SESSION` per session, and their chunk-refs `≤ MAX_STAGED_CHUNKS` + bounded overshoot (decision 4.4) | per-session range `scan("part:<id>:")` |
| `psum:` | one per committed part, `≤ MAX_PARTS_PER_SESSION` per session (tens of bytes each) | per-session range `scan("psum:<id>:")` — the cumulative staged-chunk check and `ListParts` |
| owned `sidx:` (disjoint from `pending:`) | `≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS ≤ SCAN_CAP/2` per session — **ENFORCED** by the `slot:` key space, **residue counted** (F11a); fleet-wide `MAX_OWNED_FLEET`, `W_ref`-coupled not `SCAN_CAP`-coupled | per-session range `scan("sidx:<id>:")`, fence-then-walk (**D-E**); the enforced bound keeps this single scan below `SCAN_CAP`, and **no global `pending:` scan enumerates it** (finding 3) |
| `seg:` | `≤ MAX_ROOT_SEGMENTS` per **attempt** (epoch-scoped, decision 7) | per-object range `scan("seg:<id>:<epoch>:")`, the epoch read from the root |
| `retire:` | grows with overwrites; **not** session-bounded | the **paginated `scan_page("retire:", cursor, limit)` seam** (named in *What the implementing slices change* — today's `scan` is prefix-only and complete-or-fail, `traits/src/lib.rs:772-776`), walked in cursor-keyed pages each `< SCAN_CAP`, `B` mutations per drain step; growth is bounded-or-alarmed by drain-health (oldest-obligation-age alarm, **D-D**) — it is never read by one unbounded `scan`, so it cannot halt the plane |

**`MAX_SESSIONS` is DERIVED from a memory budget, and the derivation is the enforcement.** Removing
every global `part:`/`pending:` scan (decisions 2, 5) means no *single* scan can cross `SCAN_CAP` —
but it does **not** make `MAX_SESSIONS` free. The staged reference-set build reads, per reconcile
pass, per session, **every chunk** of its committed `part:` records **and** its in-flight owned
`sidx:` entries (finding 4), holding the resulting `(server, fragment)` pairs in memory; that
**aggregate memory** is the binding constraint. The worst-case per-session footprint is

```text
U_ref = min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) × MAX_PART_CHUNKS,   # raw part-number space
             MAX_STAGED_CHUNKS + 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS )    # enforced staged ceiling
                                 └─ bounded commit overshoot ─┘└─ in-flight owned entries ─┘
```

chunk-refs — **each committed part charged its full `MAX_PART_CHUNKS`** (the iteration-4 C5/T2/T4
defect was charging a part as one unit, under-bounding by up to `MAX_PART_CHUNKS×`), and the second
term applying the enforced staged ceiling of decision 4.4 with its bounded overshoot (the
2026-07-24 call). `MAX_SESSIONS` is therefore **not a free knob**: it
is `min(⌊W_ref / U_ref⌋, SCAN_CAP/2)` — the memory derivation, **clamped** so the reaper's one
global `scan("mpu:")` can never cross `SCAN_CAP` at any legal `W_ref` — and the serialized counter enforcing that value bounds the aggregate
`Σ_sessions (actual footprint) ≤ MAX_SESSIONS × U_ref ≤ W_ref` **mechanically, for every part-size
distribution** — an implementer cannot set it high from a small-part assumption and have a later
large-part session overrun the maintenance bound, because the derivation already charges the worst
legal part.

The honest numbers, computed at **in-range** `MAX_PART_CHUNKS ≤ 381` (a `part:` record is one value,
so a "5 GiB part" of 5,120 chunks is **not** admissible — decision 4; the earlier `5,120` figure was
the forbidden value the iteration-4 adversary flagged). Take a reconcile host sized for
`W_ref = 4,000,000` chunk-refs (≈ 36 M `(server, fragment)` pairs at RS(6,3), a few GB of set):

- **small parts** (`MAX_PART_CHUNKS = 5`, a 5 MiB part; `MAX_INFLIGHT_PARTS = 16`): the raw term
  binds — `U_ref = min(10,016 × 5, 198,120 + 160) = 50,080` ⇒ **`MAX_SESSIONS ≈ 79`** concurrent
  uploads, each up to `10,000 × 5 MiB = 50 GiB` (segmented). The staged ceiling is not reachable at
  this part size, so it costs such a client nothing;
- **max in-range parts** (`MAX_PART_CHUNKS = 381`, a 381 MiB part; `MAX_INFLIGHT_PARTS = 16`): the
  ceiling term binds — `U_ref = min(3.82 M, 198,120 + 12,192) = 210,312` ⇒
  **`MAX_SESSIONS ≈ 19`**. The raw term would charge each session ≈3.7 TiB of stageable parts,
  ≈19× the ~193 GiB segmented ceiling Complete would let it publish (decision 7), and collapsed
  `MAX_SESSIONS` to **≈ 1** — the iteration-7 adversary's launch-capacity finding. Charging the
  enforced ceiling instead is what buys back the order of magnitude, and hosting *many* maximal
  sessions is now affordable: 32 concurrent ⇒ `W_ref ≈ 6.7 M` chunk-refs (a few GB), where the raw
  charge demanded ≈122 M (tens of GB).

This is the honest capacity trade, stated as a real number: **a deployment picks `MAX_PART_CHUNKS`
(hence per-part size) and `W_ref` (hence RAM), and `MAX_SESSIONS` falls out** — small parts buy many
concurrent sessions, large parts buy fewer, but no longer *one*. It corrects the iteration-2 register
(which conflated the scan bound with the memory bound), the iteration-3 finding-4 addition (owned
reads now charged), the iteration-4 defect (each part now charged its chunks, `MAX_SESSIONS` derived
not chosen, arithmetic in-range), and the iteration-7 launch-capacity finding (the raw part-number
space charged what a session could never publish). Raising concurrency without more RAM needs a
smaller `MAX_PART_CHUNKS` or a smaller `MAX_STAGED_CHUNKS`, or the future
incremental/cached staged-reference build (**FU-3**) that would let admission charge *actual* rather
than worst-case footprint.

**Admission bounds the namespaces when the reaper is down; the reaper makes the system make
progress.** An asynchronous collector alone establishes no cardinality bound, which is why both
are required and why neither alone is offered as F7's disposal. The `retire:` namespace is the
one that can *grow* under sustained overwrites even with admission (ordinary `PutObject` traffic,
not session-bounded), so its safety rests not on a cardinality cap but on **never being read by
a single scan**: the drain walks it in cursor-keyed bounded ranges, and a drain that falls behind
raises the oldest-obligation-age alarm (**D-D**) rather than silently crossing a cap.

**(b) The algorithm.**

```text
one reaper pass (dispatched from the fenced control point, like every custodian loop):
  1. sessions = scan("mpu:")                      # bounded by MAX_SESSIONS (the counter)
  2. for each session, in id order:               # deterministic order, resumable
       foreign = (clock_source not ours)          # FIRST — guard from the session record (F10/6b)
       if foreign: alarm                          #   but the guard gates JUDGMENTS, not the pass:
                                                  #   it suppresses only the arms that compare a
                                                  #   foreign timestamp (W_open / W_session /
                                                  #   W_completing / W_tombstone). The clock-FREE
                                                  #   teardown of a terminal session still runs
                                                  #   below (iteration-9 finding 11)
       slots = scan("slot:<id>:")                  # SOURCE first  — <= MAX_INFLIGHT (key space)
       psums = scan("psum:<id>:")                  # DESTINATION second — <= MAX_PARTS
                                                   #   the part commit deletes the slot and writes
                                                   #   the summary in ONE batch, so this order sees
                                                   #   a just-committed part in BOTH or in psum:
                                                   #   alone — never in neither (iteration-8 f.2)
       progress = max(created_at,
                      max psum committed_at,
                      max slot reserved_at)        #   pre-first-chunk parts (finding 2)
       if not foreign and state == Open and ((now - progress > W_open and no live sidx:/slot: lease)
                                             or (now - created_at > W_session)):
             CAS Open@E -> Aborting@E+1 + put retire:bytes:{session, all}  # one O(1) batch
       if not foreign and state == Completing and now - fenced_at > W_completing:
             CAS Completing@E -> Open@E+1 + put retire:records:{seg:<id>:E}  # rollback; names
                                                                             #   only THIS epoch
       # reclaim owned residue for EVERY non-Open state — Aborting AND Completed (finding 2):
       if state in {Aborting, Completed}:          # fence/terminal already stands
             for k in scan("sidx:<id>:"):
                 # orphan-mark each entry's FULL `staged` placement (orphan:<placement[i]>:<chunk>:<i>),
                 # so a fragment authorized before the fence but landing after it is covered too,
                 # then delete the sidx: entries — in B-batches until the range is empty
       # terminal delete — BOTH terminal states, and the tombstone window applies only to
       # Completed (an Aborting session has no completed_at and needs no client-visible tombstone):
       #   Aborting needs NO timestamp, so it is terminal for a foreign-clocked session too;
       #   Completed's tombstone window reads the session's OWN completed_at, which a foreign
       #   clock makes unjudgeable — that one case waits for the operator expiry verb (FU-6).
       terminal = (state == Completed and not foreign and now - completed_at > W_tombstone)
               or (state == Completed and foreign and operator_expiry_authorized)
               or (state == Aborting)
       if terminal and no retire: obligation for this session (scan("retire:*:<id>:") empty)
              and sidx:<id>: observed empty:
             require(mpu:<id> == prior)
                 -> delete mpu:<id> + delete slot:<id>:* + CAS count -1  # exactly-once (mpu bytes)
  3. for page in scan_page("retire:", cursor, limit):   # paginated seam; each page < SCAN_CAP
       drain up to E_tx/2 BYTES of mutations, then commit; repeat next range / next pass
         retire:bytes:   orphan-mark this batch of fragments (idempotent; never re-stamp)
                         -> when all marked: delete the records the payload names
                         -> when all deleted: delete the obligation; then, once the session has
                            session-scoped obligation left, sidx:<id>: empty, the state terminal (with
                            the tombstone window elapsed if Completed), and
                            require(mpu:<id> == prior),
                            delete the session record (+ delete slot:<id>:*, + count -1)
         retire:records: delete this batch of the records the payload names (parts and/or
                         seg:<id>:<epoch>) -> when all deleted: delete the obligation (never
                         orphan-mark)
```

Every step is one preconditioned, idempotent batch; a pass may stop anywhere and the next pass
re-derives its work from durable records. The sweep is byte-budgeted per commit (each commit
`≤ E_tx/2`, decision 3's inventory) and **never scans an unbounded namespace**: the only
whole-namespace scan is `scan("mpu:")`, bounded by the counter; `part:` (`≤
MAX_PARTS_PER_SESSION`), `slot:` (`≤ MAX_INFLIGHT_PARTS` by key space), owned `sidx:` (`≤
MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, enforced by the slot range) and `seg:`
(`≤ MAX_ROOT_SEGMENTS`) are per-session/per-object ranges each held below
`SCAN_CAP` by an enforced bound; `retire:` is walked in cursor-keyed bounded ranges; **no global
`pending:` scan of owned entries exists** (finding 3). Owned-`sidx:` reclamation is
**fence-then-walk** (**D-E**) for both the `Aborting` and the `Completed` path — a session's
entries are touched only after that session is fenced or terminal in this pass, so a session
created mid-pass is never condemned (F15), and because intent is fenced on `Open@E` (finding 1)
the walked set is frozen and complete. The terminal `mpu:` delete is preconditioned on the session
record's exact bytes, so its `mpuctl.count` decrement is exactly-once even when a gateway drains
inline and the reaper runs concurrently (the double-decrement fix); it fires only once that pass
has **observed the session's `sidx:` range empty**, which fenced intents keep true (nothing can
refill it), so no in-flight part could still be creating owned residue (findings 1/2).

**Invariant preserved.** (3) globally — with the reaper deployed, every state in §2 has a
driver, and no residency is unbounded (`W_session`); and (2) — every staged byte's exit is the
reaper's when no client provides one.

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| **False positive: reap a progressing upload.** Judge liveness by session age, or by "no part committed within W" alone | A single `UploadPart` streaming longer than `W_open` (DST: a slow body with lease renewals): the session MUST survive the idle-abandonment arm, and the part MUST commit afterwards. (It is still bounded by `W_session`, arm ii.) |
| **False positive: reap a request that holds a slot but has not written its first chunk (iteration-7 finding 2).** Derive progress from `created_at`, part `committed_at` and owned-`sidx:`-lease liveness only | DST, both halves. (a) **Live:** against a session whose last part committed longer ago than `W_open`, start an `UploadPart` that reserves its slot and stalls before its first chunk for longer than `W_open` **while its renewal loop runs**; the session MUST NOT be fenced and the part MUST commit afterwards. A design that reads only the other terms reaps a live request mid-flight, and the window is reachable by construction because the reserve deliberately precedes the first `sidx:` intent (§1); a design that carries only the *fixed* `reserved_at` stamp merely moves the false reap to `W_open` after the reservation. (b) **Crashed:** kill the same request before its first chunk and stop renewing; once its slot lease lapses and the progress maximum ages past `W_open`, the session MUST become reapable — a lapsed slot confers no liveness, though its key keeps holding the cap slot (F11a). |
| **False positive: read the progress ranges destination-first (iteration-8 finding 2).** Scan `psum:<id>:` before `slot:<id>:` | Seeded DST: against a session whose earlier progress is older than `W_open` and which owns no other live lease, commit a part **between** the reaper's `psum:` read and its `slot:` read (the commit deletes the slot and writes the summary in one batch). The session MUST NOT be fenced and the committed part MUST survive. A destination-first reader sees the part in neither range, collapses `progress` to `created_at`, and reaps a session that just made durable progress. Source-first (`slot:` then `psum:`) makes the interleaving unconstructible; the mirror staleness is a one-pass false negative, which is the safe direction. |
| **Unbounded residency: never bound a live session (F14).** Omit the `W_session` arm | A client that uploads one part every `W_open/2` for a week while a server drains: the session MUST be reaped at `W_session` and the drain MUST clear. Without arm (ii) the drain stalls indefinitely — an unbounded availability cost wearing a bounded label. |
| **Stale-snapshot reclamation (F15).** Filter a snapshot of owned entries against an older session list | DST: create a session mid-pass; the reaper MUST NOT orphan-mark its in-flight part's fragments or kill its renewals. Fence-then-walk per session makes this unconstructible; a stale-snapshot implementation reaps the live part (a DST observable on the renewal refusal, `write.rs:474-478`). |
| **Completed-path residue stranded (finding 2).** Walk `sidx:` only for `Aborting`, not `Completed` | Crash an in-flight part (owned `sidx:` residue, no `part:` record), then Complete naming other parts: after the `Completed` teardown, `scan("sidx:<id>:")` MUST be empty before the terminal delete (which discards the surviving `slot:` records outright); the gate is the empty `sidx:` range, **never** an empty `slot:` range (the crashed attempt keeps its slot, §2). A design that skips the `Completed` walk strands the residue forever (outcome (a)). |
| **Terminal delete before the `sidx:` range is walked empty (findings 1/2).** Delete the session while owned residue remains | DST: leave a crashed in-flight part's owned `sidx:` residue, fence the session, and attempt the terminal delete in the same pass before the `sidx:` walk: it MUST NOT fire until the range is observed empty. Because fenced intents (finding 1) let nothing refill the range, the walk always converges; deleting early would strand residue nothing can discover. |
| **Clock guard read from the owned entry, not the session (finding 6b).** Reap using an owned-lease stamp before checking the session's `clock_source` | Write a session with a foreign `clock_source` and an owned entry with a *local*-looking lease; the reaper MUST skip the whole session (guard read from `mpu:` first) and never evaluate its owned lease. |
| **`W_completing` from the wrong instant (F16).** Measure staleness from last part progress | Fence a Complete long after its last part; advance the clock past `W_completing` from that part but not from `fenced_at`; the reaper MUST NOT roll it back. |
| **The Complete race.** Reap a session concurrently with its Complete | DST both interleavings: (i) reaper fences first → Complete's fence returns `Conflict`, client sees `404 NoSuchUpload`, **no object is published**; (ii) Complete fences first → the reaper's CAS fails and it MUST NOT touch the session, and the published object's fragments MUST carry no `orphan:` record after the drain. |
| **Roll back a `Completing` session unsafely** | DST: reaper rolls back while the completer's flip batch is in flight; assert the flip returns `Conflict` and that a subsequent Complete publishes exactly once, and that the rolled-back session's stale `seg:` records are reclaimed. |
| **Crash mid-teardown** | Kill the reaper at each drain step (seeded DST): re-running MUST converge to zero staged records with no fragment left unevidenced, no orphan record re-stamped (grace clock preserved), and the counter back to its true value. |
| **False negative: never reap** | A session abandoned for `> W_open`, or over-age past `W_session`, MUST be gone (records and bytes reclaimed, counter decremented) within a bounded number of passes; assert the staged population and the counter return to baseline. |
| **The sweep breaks the bounded-work rule** | Reap a 10,000-part session and assert every commit is inside the envelope and that the pass makes durable partial progress when interrupted. |
| **Tombstone / retire accumulation (F11c/F11b).** Bound only the open population | Drive sustained creates+completes and overwrites; assert `scan("mpu:")` stays `≤ MAX_SESSIONS` (tombstones counted by the counter, expired at `W_tombstone`) and that `retire:` never fails a scan (walked in bounded ranges), while the oldest-obligation-age alarm fires if the drain falls behind. |
| **Arrival outruns drain** | Drive session creation faster than the reaper reclaims: creation MUST start refusing at `MAX_SESSIONS` (`503`) via the serialized counter, and every namespace MUST stay accessible without `ScanCapExceeded`. |
| **Derive the admission limit per gateway instead of reading it from the ledger (iteration-9 finding 5)** | Run two gateways whose local `W_ref` yields different `MAX_SESSIONS` against one store. The one whose derivation disagrees with `mpuctl.max_sessions` MUST refuse to admit and alarm — never admit on its own larger value. Assert the live session count never exceeds the ledger's `max_sessions`, so `MAX_SESSIONS × U_ref ≤ W_ref` holds through the rolling change; a design that keeps the limit in local config admits past the smaller bound and blows the reconcile host's RAM budget, which the gateway that caused it never observes. |
| **Fence the idle arm without pinning the liveness evidence (iteration-9 finding 4)** | Seeded DST: start an `UploadPart` whose renewal reads a live lease; hold its commit; let the reaper read the now-expired lease and decide to fence; let the renewal commit; then let the fence commit. The fence MUST `Conflict` on the slot preconditions and the upload MUST go on to commit its part. A design whose fence preconditions only `mpu:` aborts a progressing upload before `W_session` — renewal's expiry test is evaluated in the gateway before the commit, not atomically at the store (`metadata.rs:746-760`), so wall-clock agreement does not close it. Assert too that the `W_session` arm still fences a *renewing* session (the ceiling is not a race). |
| **Reaper unavailable** | Stop the reaper entirely, then drive creation: `reconcile_step` MUST keep succeeding indefinitely (no `ScanCapExceeded`), because the counter refuses new sessions before `mpu:` can approach the cap and no other namespace is read by an unbounded scan. |
| **Clock-epoch mismatch** | Write a session record with a foreign `clock_source`; the reaper MUST skip it and raise an operator signal, never reap it. |
| **Reap by expiry of owned leases** | An `Open` session whose owned leases lapsed (a stalled but live client) MUST NOT have its bytes reclaimed until the session itself is abandoned (arm i) or hits `W_session` (arm ii) and is fenced. |

---

### Decision 7 — Chunk-map segmentation (the >10 GiB launch requirement)

**Decision.** A published map larger than one value is split into **segment records** named by
the root inode, and the object is published by **staged publication** — write the segments in
bounded batches, then flip the root in one batch — reusing decision 4's retirement machinery as
one pattern family, never a new unbounded or unevidenced namespace.

**Segmentation is a multipart-only mechanism (the finding-4 / adversary-A carve-out).** Staged
publication needs the three things only a *session* provides — an **upload id**, a fenced
**`Completing@E` state** to write segments under, and the **fence epoch `E`** that scopes the
segment-group key `seg:<upload-id>:<epoch>:<i>` and its crash-evidence/rollback machinery (§c).
A **single `PutObject`** has none of these: no session record to fence, no epoch to key segments
by, and so no anchor for the staged-publication protocol or the reaper that reclaims a crashed
staged publication. Iteration 5's "segmentation applied *uniformly* to multipart Complete *and*
single-PUT" was therefore underspecified — a single PUT cannot create or recover the `Completing`
epoch the protocol requires. Instead:

- **A multipart Complete** whose assembled map exceeds `MAX_MAP_CHUNKS` is **segmented** (this
  decision).
- **A single `PutObject`** publishes a **flat** map, and reaches large sizes by **choosing a
  chunk size that fits its declared `Content-Length` inside `MAX_MAP_CHUNKS`**:
  `chunk_size_effective = max(DEFAULT_CHUNK_SIZE, ⌈Content-Length / MAX_MAP_CHUNKS⌉)`. `PutObject`
  carries a declared `Content-Length` (enforced, the rubric's *Protocol input* rule), so the
  gateway knows the size up front and picks the chunk size deterministically. S3's 5 GiB single-PUT
  maximum fits a flat map once `chunk_size ≥ 5 GiB / MAX_MAP_CHUNKS ≈ 13.4–31 MiB` — reachable, and
  the only cost is gateway memory (`chunk_size × max_concurrent_encodes`), registered honestly.
- **A single `PutObject` that cannot fit `MAX_MAP_CHUNKS` even at `chunk_size_max`** is **refused**
  with `400 EntityTooLarge` (the client must use multipart) — never silently segmented against a
  session that does not exist. In the S3 range (`Content-Length ≤ 5 GiB`) a sane `chunk_size_max`
  always fits, so the refusal is a guard, not a routine path (X50).

A published map of `≤ MAX_MAP_CHUNKS` chunks stays a flat inline map (unchanged, today's shape);
a larger map is produced **only** by a multipart session, and is segmented.

**(a) The record shape ([ADR-0046][a46]).** The segmented `InodeRecord.chunk_map` becomes a
two-variant value: `Flat(Vec<ChunkRef>)` as today, or
`Segmented { group: (<upload-id>, <epoch>), segment_count, segments: Vec<SegmentRef> }` where a
`SegmentRef` is `{ index, byte_offset, byte_len }` and the chunks of segment *i* live in the
record `seg:<upload-id>:<epoch>:<i>` (§1). The **segment-group id** `group` is the minting
upload-id paired with the **`Completing` fence epoch of the attempt that wrote the segments** —
an opaque token that outlives the session (2^-128 reuse on the upload-id, the X31 basis). The
per-attempt (epoch) scoping is **load-bearing** (F18, below): a rolled-back attempt's segment
keys carry its own epoch and no later attempt ever writes them, so a stale rollback obligation
can never delete a *later* attempt's published segments. Key shape, writer, deleter and scan
visibility are in the §1 table; a maintenance consumer resolves the map by reading the root,
then the bounded range `scan("seg:<upload-id>:<epoch>:")` (≤ `MAX_ROOT_SEGMENTS`), the epoch
taken from the root's `group`. Because this changes the shape of `InodeRecord` for **every**
consumer of `.chunk_map` (the read path, GC's reference build, reconstruction's `find_chunk`,
rebalance, backfill, and every backend's conformance expectations), it is the strongest
ADR-graduation candidate here — the successor to [ADR-0047][a47]'s record shape (*Graduation
criteria*).

**(b) Staged publication (write segments, then flip).** Complete, after fencing to
`Completing@E` and validating (decision 1.2), takes its segment-group id `group = (<upload-id>,
E)` — `E` is the current fence epoch, recorded in `publish_target` — computes the full ordered
chunk list from the frozen part set, and splits it into `⌈total_chunks / MAX_SEG_CHUNKS⌉ ≤
MAX_ROOT_SEGMENTS` segments. It writes the `seg:<upload-id>:<E>:<i>` records in **byte-budgeted**
batches (`≤ E_tx/2` per commit, `B_seg = ⌊(E_tx/2)/V⌋ = 50` segment puts each — **not** a fixed count,
the iteration-2 envelope fix; each `require(mpu == Completing@E)`), then flips the root in
**one** batch: `require(mpu == Completing@E)` + the inode CAS (the root records `group`) +
session→`Completed` + `retire:records:{parts}` + any `retire:bytes:`. **The flip is the
publication instant**, carrying exactly decision 1's fence/epoch proof; the segments are already
durable when it commits, so the flip is O(1) and cannot exceed the envelope. A segmented map
thus needs its own staged publication precisely because it cannot be published in one batch (the
transaction envelope, `crates/traits/src/lib.rs:744-758`) — the mirror image of decision 4's
staged *retirement* (commit-then-drain vs. write-then-commit), one pattern family per **D-G**.

**(c) The crash story — one pattern family with decisions 4/5, and why segment keys are
epoch-scoped (F18).** A completer that dies between segment writes and the flip leaves
`seg:<upload-id>:<E>:*` records whose bytes are **still protected by the `part:` records** (the
flip has not installed `retire:records:{parts}` yet) and whose session is still `Completing@E`.
Two exits, both bounded:
- **Same-gateway recovery** (the completer received `CommitUnknownResult` and re-reads its own
  session, decision 3's F5 — *not* a second client verb, which gets `409`): sees `Completing@E`,
  re-runs the segment-write phase **at the same epoch `E`** — **idempotent because the keys
  (`seg:<upload-id>:<E>:<i>`) and their content are fixed by the frozen part set at that
  epoch** — then flips.
- **Rollback / abort** (reaper after `W_completing`, or a rejected Complete): the transition
  installs `retire:records:{seg:<upload-id>:<E>}`, naming **exactly epoch `E`'s** segment keys,
  which the drain deletes in byte-budgeted batches (never orphan-marking — the fragments remain
  part-protected). A subsequent client Complete fences from `Open` to a **new** epoch `E' > E`
  and writes the **disjoint** range `seg:<upload-id>:<E'>:*` (the part set may have changed;
  segments are recomputed fresh under the new group).

**This is the iteration-2 F18 refutation, closed by construction.** The refutation was: roll
back a Completing attempt (installing `retire:records:{seg}`), then re-Complete and publish
*while that obligation is still pending*, so the drain later deletes the published object's
segment records — outcome (c). With **per-attempt (epoch) scoping**, the stale obligation names
only epoch `E`'s keys, and the republished object's segments live at epoch `E'`; the two key
ranges are disjoint, so the pending obligation may drain **concurrently with or after** the new
publication and can **never** touch the published epoch-`E'` segments (execution X40). A
dangling segment set is therefore **evidenced** (by `publish_target` while `Completing@E`, and
by the epoch-scoped `retire:records:{seg:<id>:<E>}` on rollback) and **bounded** (≤
`MAX_ROOT_SEGMENTS` per attempt). Segment keys are **per-attempt**, *not* shared across
attempts: a resume of the *same* attempt (same `Completing@E`) reuses epoch-`E` keys
idempotently, while a rollback-then-retry starts a fresh, disjoint epoch — reversing
iteration-2's "upload-id-scoped, not per-attempt" claim, which was exactly the hole. The number
of rolled-back attempts a session can accumulate is itself bounded (the reaper rolls back at
most once per `W_completing` over the session's `W_session` life), each leaving one bounded,
drained `retire:records:{seg:<id>:<E>}` obligation under the ordinary `retire:` bounded-range +
drain-health treatment (F11b). It is never a new unbounded or unevidenced namespace.

**(d) Complete idempotency over the segment-write phase (F5).** The idempotency proof of
decision 3 now spans the whole phase, not just the flip: within one attempt (epoch `E`) segment
keys are deterministic from `(upload-id, E)` and the frozen part set; segment content is
deterministic; the flip is a single fenced batch. The **same-gateway recovery** therefore
**never double-writes** (identical epoch-`E` keys+bytes) and **never half-flips** (the flip is
atomic and fenced). A **concurrent second client Complete cannot even enter the segment-write
phase**: it fails to fence a `Completing` session and returns `409 OperationAborted` (decision
3, fix 4) — so there is never a second publisher writing segments in parallel; the only re-run
is the owning gateway's own unknown-result recovery at the same epoch. A rollback-then-retry is
a *new* epoch (§c), a fresh disjoint generation, not a double-write.

**(e) Every maintenance consumer resolves the segmented shape in bounded work, and that work is
charged.** Each consumer of decision 2's list that resolves a committed inode's map (GC reference
build, restore's identical gate, scrub, reconstruction, rebalance, backfill) reads the root and, if
`Segmented`, the bounded range `scan("seg:<upload-id>:<epoch>:")` (≤ `MAX_ROOT_SEGMENTS`), the epoch
read from the root's `group` — **never a global `seg:` scan**. Reference-set construction therefore
stays under `SCAN_CAP` per scan: it already iterates objects (bounded per object), and segment
resolution adds a bounded per-object range, not a new whole-namespace scan.

But *no scan crossing `SCAN_CAP`* is not the whole cost: a **committed** segmented object adds up to
`MAX_ROOT_SEGMENTS` extra record reads **and** up to `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS`
(≈ 51,480–198,120) chunk-refs — ~463 K–1.78 M `(server, fragment)` pairs — to the in-memory
reference set, **per max segmented object**. This is the *committed*-side analogue of the staged
`W_ref` charge, and unlike sessions it is **not** admission-bounded (durable data cannot be refused).
It is therefore charged honestly as an **operational capacity cost**, not a by-construction bound:
the committed reference build's memory scales with total committed fragments (flat or segmented,
exactly as today — segmentation only lets a *single* object contribute up to ~1.78 M pairs), and the
implementing slices carry a **reference-build size telemetry + alarm** (`W_ref_committed`, decision 6
/ FU-3) so an operator sees the reconcile host approaching its RAM ceiling before it exhausts. It is
**not** a refutation outcome — nothing is stranded, absorbing, published over reclaimable bytes, or
over the transaction envelope; it is memory pressure, registered as a capacity cost (X48). Segment
records are **counted** in the cardinality reasoning (decision 6's formula) as ≤ `MAX_ROOT_SEGMENTS`
per committed object, resolved per-object.

**(f) Supersede/overwrite of a segmented object.** Publishing over a segmented generation
installs `retire:bytes:{generation: {inode, version, chunks?, segments: <upload-id>:<epoch>}}`
(decision 4), which the drain uses to orphan-mark the prior generation's fragments **and** delete
its `seg:<upload-id>:<epoch>:*` records in byte-budgeted batches. The committed inode carried the
`group` (upload-id + epoch), so the supersede finds the exact segment range without reading the
(possibly already superseded) prior map inline, and only ever names the superseded generation's
own epoch. The obligation names the segments **by their `seg:` keys, not by frozen placements**:
the drain re-reads each `seg:<upload-id>:<epoch>:<i>` record's **current** `ChunkRef.placement` at
drain time and orphan-marks that, so a fragment a reconstruction/rebalance repoint moved *before*
the supersede won the inode CAS is orphaned at its current position — and once the supersede's
inode CAS commits, no further repoint can win (a repoint requires `require(inode == prior)`), so the
placements the drain reads are stable. This is the drain side of the repoint pre-evidence rule (§e
below / decision 2): the two mechanisms — destination pre-mark on the repoint, key-range (not
frozen-placement) retirement on the supersede — close the repoint-vs-supersede race on every
interleaving (X47, adversary C).

**(g) The arithmetic (**D-F**).** With a `SegmentRef` encoding to ~96 B (compact) to ~160 B
(worst case), the root holds `MAX_ROOT_SEGMENTS = ⌊50000 / b_segref⌋` = **312–520** segments, each
holding `MAX_SEG_CHUNKS = MAX_MAP_CHUNKS` = **165–381** chunks. The segmented object ceiling is
`MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS × chunk_size`:

- at the **default 1 MiB chunk**: 312 × 165 × 1 MiB = **50.3 GiB** (worst-case encoding) to
  520 × 381 × 1 MiB = **193.5 GiB** (best case) — **comfortably over the 10 GiB launch
  requirement even in the worst case**, and at the default chunk size, so no gateway-memory
  trade is forced to reach it.
- reaching S3's **5 TiB** object cap needs the map to hold `5 TiB / chunk_size` chunks; with a
  map capacity of 51,480–198,120 chunks that is a **26.5–101.8 MiB** chunk size — reachable, but
  traded against the gateway memory budget (`chunk_size × max_concurrent_encodes`). The design
  does **not** claim 5 TiB at default chunks; it claims >10 GiB at default chunks and up to S3's
  ceiling with larger chunks.
- `MAX_PARTS_PER_SESSION = 10_000` (S3's limit) is **not** the binding ceiling in this range: a
  50 GiB object over 10,000 parts is 5.12 MiB/part (≥ S3's 5 MiB minimum) and a 5 TiB object is
  524 MiB/part (≤ S3's 5 GiB maximum).

The full computed register is in *Accepted costs*.

**(h) Reader safety when a generation's `seg:` records are deleted — the resolve-retry rule
(adversary B).** Retiring a superseded/deleted generation (§f) or rolling back a Completing attempt
(§c) **deletes `seg:` records**. The fragment orphan grace protects the *bytes* a concurrent GET is
reading, but the *record* deletion is new to segmentation and could tear a GET that has read the
root (pointing at `group = (id, E)`) and is midway through reading `seg:<id>:<E>:*`. Rather than add
a second grace clock, the map resolution is made **retry-tolerant**, using the ordering the protocol
already guarantees — **the root is flipped away from a generation *before* that generation's `seg:`
records are ever deleted** (supersede/delete flips or removes the root and installs the
`retire:bytes:{generation}` obligation in one batch; the drain deletes the `seg:` records only
afterward; rollback CASes the session out of `Completing@E` before installing
`retire:records:{seg:<id>:<E>}`):
- A consumer resolving a segmented map reads the root's `group`, then the bounded range
  `scan("seg:<id>:<E>:")`. If a segment read returns **absent**, it **re-reads the root**. A root
  now pointing at a **different `group`** (superseded) or **absent** (deleted) means the generation
  was concurrently retired: a reader restarts against the current root (or answers `NoSuchKey`); a
  maintenance pass drops the stale resolution (the generation it was reading is being reclaimed
  anyway). A root **unchanged** yet a segment **absent** is an invariant violation (a live
  generation never loses a segment — §f deletes only *retired* generations, whose root no longer
  names them), so it is **fail-closed** (an error, never a torn success — the rubric's *Absent or
  unsupported entries* rule), not silently accepted.
This is clock-free, needs no reader grace on the records, and closes the segmented-GET tear on
every interleaving (X51). It applies uniformly to the S3 read path and every maintenance consumer
of decision 2's list.

**Invariant preserved.** (1)/(2)/(4) at object scale: the segmented map is published by the same
fence proof (1), its dangling segments are an evidenced, bounded, reclaimable class (2), and both
its publication and its retirement are bounded-per-batch (4).

**Failure-mode table.**

| Way to implement it wrong | Observable that fails |
|---|---|
| Publish a segmented map in one batch | Complete a `> MAX_MAP_CHUNKS` object against FoundationDB in a single publication batch: it MUST be staged (segment writes, then flip); a one-batch publish exceeds the envelope and fails permanently. |
| Put a fixed-count `B` (e.g. 1,000) of `seg:` puts in one batch (the iteration-2 envelope defect) | A segment-write phase against FoundationDB: assert every commit's mutation **bytes** ≤ `E_tx/2`, so a `seg:` batch carries ≤ `⌊(E_tx/2)/V⌋ = 50` records; 1,000 × `V` = 100 MB exceeds the 10 MB envelope and fails permanently. |
| **Scope segment keys per-session, not per-attempt, so a stale rollback obligation deletes a later attempt's published segments (the iteration-2 F18 refutation, outcome c)** | DST: fence `Completing@E`, write segments, roll back (installing `retire:records:{seg:<id>:<E>}`), re-Complete to `Completing@E'`, publish, and drain the *still-pending* epoch-`E` obligation **after** publication; assert the **published** object's `seg:<id>:<E'>:*` records survive and the object resolves. Per-attempt (epoch) keys make the ranges disjoint; a session-scoped key deletes the live segments (X40). |
| Leave dangling `seg:` records after a completer crash (F18) | Kill a completer between segment writes and the flip, then roll the session back: assert every `seg:<id>:<E>:` record of that epoch is reclaimed by `retire:records:{seg:<id>:<E>}` and **no** fragment is orphaned (the parts still protect them). |
| Non-deterministic segment keys/content across a same-epoch recovery (F18) | DST: inject `CommitUnknownResult` mid segment-write phase and let the owning gateway recover at the same `Completing@E`; assert exactly one published object, no double-written segment, no half-flip (identical epoch-`E` keys+bytes; one fenced flip). (A *separate* client Complete meanwhile gets `409`, not a parallel writer, fix 4.) |
| Resolve a segmented map with a global `seg:` scan | Publish enough segmented objects that a global `seg:` scan would cross `SCAN_CAP`; assert `reconcile_step` still succeeds — resolution MUST be per-object bounded `seg:<id>:<epoch>:` ranges. |
| Orphan-mark a superseded segmented generation's fragments but leak its `seg:` records | Overwrite a segmented object; after the drain, assert no `seg:<id>:<epoch>:` record of the prior generation survives and its fragments are orphan-marked. |
| Count segment records outside the cardinality formula | Assert the reference-set build over `MAX_SESSIONS` segmented objects stays under `SCAN_CAP` and that segment resolution is bounded per object. |
| Repoint a committed segmented object's `seg:` fragment without pre-evidencing the destination (the iteration-4 / adversary-C repoint-vs-supersede race) | DST: reconstruction/rebalance re-places a fragment (`P_old→P_new`) of a committed segmented object's `seg:<id>:<E>:<i>` record while a supersede/delete of that generation is in flight, in **both** interleavings — repoint wins the CAS before the supersede's inode CAS, and repoint loses after it. The re-place MUST pre-mark `orphan:<P_new>` before writing the destination, then `require(seg == prior)` **and** `require(inode == prior)`, deleting the pre-mark and orphaning `P_old` on a win, leaving the pre-mark on a loss; and the supersede's retirement MUST re-read each seg's **current** placement at drain time. Assert **no** moved fragment is left unreferenced-and-unevidenced on either interleaving (X47). A repoint that writes the destination without the pre-mark strands `P_new` when the CAS loses (outcome (a)); one whose supersede orphans a frozen pre-repoint placement strands `P_new` when the repoint won first. |
| Delete a retired generation's `seg:` records without a reader-safe rule (adversary B — the reader-transparency claim) | DST: a GET reads a segmented object's root (`group = (id, E)`), then a concurrent supersede/delete retires that generation and the drain deletes its `seg:<id>:<E>:*` records before the GET finishes resolving. The GET MUST NOT tear: finding a `seg:` record absent, it re-reads the root and either restarts against the current generation or answers `NoSuchKey` (§h resolve-retry); assert no torn/half-resolved map is ever returned, and that an *unchanged* root with a missing segment is a fail-closed error, never a silent partial read (X51). |

---

### Clock lifecycles this protocol introduces (F10, one table)

`AGENTS.md:132-142` makes it a MUST that a new clock read states which source owns its
lifecycle, and that one lifecycle never mixes sources.

| Lifecycle | Stamp | Written by | Evaluated by | Owner |
|---|---|---|---|---|
| Abandonment (idle + residency) | `mpu.created_at_millis`, `part.committed_at_millis` (read via `psum:`), `slot.reserved_at_millis` | gateway | reaper (arms i and ii) | deployment wall clock (`SystemTime::now`, madsim-virtualised); guarded by `clock_source` — an unrecognized source is skipped, never reaped |
| Completing rollback | `mpu.fenced_at_millis` (stamped at the fence) | gateway (fence batch) | reaper (`W_completing`) | deployment wall clock |
| In-flight part slot | `slot:` record `reserved_at_millis` (fixed, a progress instant) and `lease_expiry_millis` (renewed in flight by the same loop as the owned leases, `write.rs:474-500`) | write path / gateway (reserve, then renewals) | **the reaper**, as abandonment condition (i) — the stamp joins the progress maximum, the lease confers liveness before the first chunk exists (iteration-7 finding 2). The **cap** reads neither: it reads the key's existence | deployment wall clock |
| Owned staging lease | `sidx:` entry `lease_expiry_millis` (owned entries) | write path, renewed in flight (`write.rs:474-500`) | **the reaper**, as abandonment condition (i)'s liveness input (an unexpired owned lease ⇒ live), *and* the in-flight renewal | deployment wall clock; **the clock guard is the session record's `clock_source`, read first** — the reaper skips a foreign-clocked session before ever reading its owned leases, so owned entries need no `clock_source` field of their own (the iteration-3 finding-6b fix); fail direction is safe (skew → false-positive reap → FU-4's fenced, bounded cost) |
| Orphan grace | `orphan:` value written by the retirement/reaper drain | whoever drains (gateway inline or reaper) | GC (`gc.rs:171-176`, an inclusive `now − orphaned_at ≥ G_orphan`) | the same deployment wall clock, exactly as the overwrite path's `orphaned_at_millis` (`write.rs:305-313`). **Constraint: `G_orphan > W_write + δ_clock`** (strict, below), so a fragment authorized before a fence lands *strictly before* its position's grace elapses — never at the boundary tick GC's inclusive `≥` would already reclaim (finding 3) |
| Fragment-write deadline | `W_write` — the fail-closed timeout on a fragment write to a D server (the rubric's *await discipline*, `AGENTS.md:181-183`) | write path | the write path itself (abandons the write past the deadline) | the same deployment wall clock; **coupled to orphan grace by the strict `G_orphan > W_write + δ_clock`** (`δ_clock` = the clock's resolution/skew margin) so no authorized fragment can land at or after its landing position's `orphan:` evidence is reclaimable (finding 3) |
| Tombstone | `mpu.completed_at_millis` | gateway (flip batch) | reaper (`W_tombstone`) | deployment wall clock |

No new clock *source* is introduced — every lifecycle is the deployment wall clock the overwrite
path already uses. The one lifecycle that could have mixed sources — reclamation of staged
residue — is made **clock-free** instead (decision 5, reference-based). The owned-lease row now
states honestly that the reaper reads the lease as a liveness input (**D-E**, correcting
iteration 1's "nothing that decides reclamation").

### Execution register — the enumeration, in one place

The refutation test for this proposal is a concrete execution — a crash point, a lost CAS, a
race, an operator action, a clock mismatch, a segment-write crash — that (a) strands bytes or
metadata with no bounded reclamation path, (b) leaves a state nothing can exit, (c) publishes or
preserves a chunk map over bytes a maintenance pass may reclaim or has reclaimed, or (d) installs
an obligation past the transaction envelope. This table is the enumeration those outcomes are
tested against; every row states what actually happens and what makes it so.

| # | Execution | Outcome under this protocol | Disposed by |
|---|---|---|---|
| X1 | Concurrent `PutObject` wins the publication CAS (no crash) | Flip returns `Conflict`; bounded retry **recomputing the version from the re-read prior** (`newprior.version + 1`, not the fence-time value — finding 2) and reusing the same epoch-`E` segments; else fence released to `Open`, `409` | D3 (finding 2) |
| X2 | Completer crashes after fencing, before flipping | Session sits in `Completing`; reaper rolls it back to `Open` after `W_completing` from `fenced_at`, cleaning its `seg:` records | D3, D6, D7 |
| X3 | Completer crashes after the flip commit, before answering | Session is `Completed`; the client's retry answers `200` with the recorded ETag | D3 (F5) |
| X4 | Publication returns `CommitUnknownResult` | Re-read the session record; it is the single authority on what happened | D3, `traits/src/lib.rs:730-745` |
| X5 | Reaper fences an abandoned session while a Complete fences it | One CAS wins; loser gets `Conflict`. If the reaper won, nothing is published and the bytes are retired; if Complete won, the reaper does not touch the session | D6 |
| X6 | Reaper rolls back a `Completing` session whose completer is merely slow | The slow flip fails its `require(mpu == Completing@E)` and publishes nothing; the client retries | D6 (F16 — measured from `fenced_at`, not progress) |
| X7 | A part commit lands after a Complete fence | `require(mpu == Open@E)` fails; `404 NoSuchUpload`; the part does **not** self-compensate (the session left `Open`) — its residue and slot are reclaimed by the session teardown, avoiding a double slot-release | D1, D5 |
| X8 | Gateway crashes mid part-upload (fragments on disk, owned `sidx:` entries) | Every fragment was preceded by a `sidx:` entry (intent precedes bytes, fenced on `Open@E`); reclaimed per-session via the bounded `sidx:` range when the session leaves `Open`, **Completes**, or vanishes | D5 |
| X9 | Client re-uploads a part number that already has a record | The part commit replaces the record and installs `retire:bytes:{chunks}` for the superseded chunk list in the same batch | D4, D5 |
| X10 | Two concurrent `UploadPart`s for the same part number | Exact-value precondition on the part key: one wins, the loser gets `Conflict` and compensates | D1, D5 |
| X11 | Client aborts a 10,000-part session | One O(1) fence batch answers `204`; teardown drains asynchronously in `B`-sized batches; counter decremented last | D3, D4 (F9) |
| X12 | Drain crashes after orphan-marking, before deleting the part records | Bytes are both protected and evidenced; re-entry skips already-marked fragments, grace clock intact | D4, `restore.rs:93-100` |
| X13 | Drain crashes after deleting records, before deleting the obligation | Bytes carry orphan evidence; GC reclaims after grace; the obligation is re-drained as a no-op | D4 |
| X14 | Operator drains a D server holding staged fragments — committed-part **or in-flight owned** | `reconciliation_status` stays `Pending` for either (the staged set counts committed `part:` **and** `Open`-session `sidx:` fragments, finding 4); no new staged fragment — committed or in-flight — lands there; clears within `W_session` | D2 (F6, finding 4) |
| X15 | Operator wipes that server anyway | The pre-wipe status was `Pending`, so the wipe is an operator override, not a silent `Satisfied` — the F6 trace and its in-flight-owned variant are closed at the reporting step | D2 |
| X16 | Metadata restored to a point before the session's part records existed | Those fragments are genuinely unreferenced (the session record is rewound too), so marking them is correct; a *surviving* session's staged fragments are protected by the staged reference set | D2, *Backward compatibility* |
| X17 (F13) | Snapshot an `Open` session with staged parts → abort/reap → GC reclaims fragments → restore to the snapshot → retry Complete | The restore **fences every resurrected session to `Aborting`** (**D-B**), so the retried Complete returns `4xx` and **cannot** publish over the reclaimed bytes; the client re-uploads from the start. A session resurrected in `Completing` takes the restore-fence transition, not this one — X57 | D1.4, D2, D3 |
| X17b (F13, iteration-4) | The restored image goes live and a client's retried Complete arrives **before** `reconcile_after_restore` fences the resurrected session | The restore-fence generation MUST complete before any gateway serves multipart verbs on the restored image (decision 2 restore row): the gateway waits for or refuses multipart until the restore fence lands, so Complete can never fence a still-`Open` resurrected session and publish over reclaimed bytes in the pre-fence window | D1.4, D2, D3 |
| X18 (F14) | A live client uploads one part every `W_open/2` for a week while a server drains | Idle-abandonment (arm i) never fires (the session is live), but the residency ceiling (arm ii) reaps it at `W_session`; the drain clears within `W_session` | D6 (F14), D2 |
| X19 (F15) | A session is created mid reaper pass | Fence-then-walk reclamation touches a session's owned entries only after fencing it, so the mid-pass session is never condemned; its live part's renewals survive | D6 (F15) |
| X20 | 1-byte `PutObject` overwrites a max-size (flat or segmented) object | Publication installs `retire:bytes:{generation}` (with the `seg:` range if segmented); no inline fan-out; the commit fits the envelope | D4, D7 (F4) |
| X21 | Multipart Complete overwrites an existing multipart object | Same, plus `retire:records:` for its own staging records | D4, D7 |
| X22 | Abandoned/tombstoned sessions accumulate while the reaper is down | The serialized counter refuses new sessions at `MAX_SESSIONS`; every namespace stays accessible (bounded ranges); the custodian plane keeps reconciling | D6 (F7) |
| X23 | Valid uploads arrive faster than the reaper drains | Same counter: `503 SlowDown` at the cap — contention at the counter is the backpressure | D6 (F7/F12) |
| X24 | Fleet-wide concurrent creates race the cap | The serialized counter CAS admits exactly `MAX_SESSIONS`; losers retry or get `503` — no overshoot (F12) | D6 (**D-C**) |
| X25 (finding 3) | Fleet-wide owned entries `MAX_OWNED_FLEET` exceed what any single scan holds; `reconcile_after_restore` runs its `pending_chunks` scan | Owned entries live under the **disjoint `sidx:` prefix**, so `scan("pending:")` sees only ordinary pending (bounded as today) and the restore command progresses; owned entries are reclaimed per-session via bounded `sidx:` ranges each `≤ SCAN_CAP/2` (enforced by the `slot:` key space, residue counted), their sum charged to the `W_ref` memory budget (decision 6). A design that keeps owned entries under `pending:` risks `ScanCapExceeded` in the restore *and* the sweep (the iteration-3 finding-3 contradiction) | D5, D2 (finding 3) |
| X26 | A session record carries a `clock_source` the reaper does not own | The reaper reads the session's `clock_source` **first** and skips (and alarms) the whole session before evaluating any owned lease — never reaped on a foreign stamp (the #557 class, finding 6b). The skip is **not** an absorbing state: neither abandonment arm can retire the session (both would read the foreign clock), so its exit is the **operator-driven abort** on the management surface, and the alarm is what summons it — worst case `MAX_SESSIONS` slots parked behind a named alarm, a bounded capacity cost with a remedy (FU-6) | D6 (F10), FU-6 |
| X27 | Expired-pending sweep runs with `Reclaim` while a session is `Open` | The sweep scans only `pending:`; owned entries are under the disjoint `sidx:` prefix, so it never touches one — only the reference-based arm may reclaim them | D5 |
| X28 | A D server holding staged fragments loses a disk | Scrub detects, reconstruction rebuilds and re-places under the session fence (segmented map resolved by bounded `seg:` range) | D2, D7 |
| X29 (finding 1) | Reconstruction re-places a **staged** chunk — writes the destination fragment `P_new`, then CASes the `part:` record — while a Complete/Abort/reaper fence advances the session out of `Open@E` in that window | The staged re-place uses the **same destination-pre-mark rule** as the committed repoint (X47): it **pre-marks `orphan:<P_new>` before writing the fragment**, so the losing CAS (`require(mpu == Open@E)` fails) is a **no-op that leaves the `P_new` pre-mark standing** and GC reclaims the abandoned destination — the rebuilt fragment is never left unreferenced-and-unevidenced (outcome (a) closed, permanent under `Defer` otherwise). On a win it adopts `P_new` (deletes the pre-mark) and orphans `P_old`; the blocked obligation stays queued and is served after publication. The prior register row treated the bare CAS failure as safe and missed the pre-written fragment — the finding-1 correction | D2, D7f (§1 staged/`seg:` writers) |
| X30 | Complete names only a subset of the staged parts | Named parts publish; unnamed parts' bytes are orphan-marked and their records deleted | D3, §1 |
| X31 | The target bucket is deleted mid-session | The flip's bucket-existence precondition fails; `NoSuchBucket`; fence released | D3, [ADR-0046][a46] §4 |
| X32 | The assembled map would exceed the segmented ceiling | Refused best-effort at `UploadPart` and authoritatively at Complete (F17); never an over-envelope commit. Distinct from the *staged* ceiling `MAX_STAGED_CHUNKS` (X55), which bounds what the session may hold, not what it may publish | D4, D7 |
| X33 | Complete retried after the tombstone window | `404 NoSuchUpload`, with the object present and readable by its key | D3 (bounded, registered) |
| X34 | An `orphan:` record exists for a chunk that is committed-referenced | GC's reference gate is evaluated **first** (`gc.rs:159-176`), so the record is inert while the reference stands | GC precedence, defence in depth |
| X35 | Two sessions Complete on the same key concurrently | Both flip through the inode CAS; one supersedes the other; the superseded generation (its `seg:` range too) is retired | D3, D4, D7 |
| X36 | Gateway restarts (fresh random chunk epoch) with sessions open | Chunk ids never collide across epochs and are `>= 2^127`, clear of allocator recovery; sessions are unaffected — a second gateway can complete them | `crates/server/src/lib.rs:237-251` |
| X37 (F18) | Completer dies between segment writes and the root flip | Segments are evidenced (`publish_target` while `Completing@E`) and the fragments stay part-protected; the owning gateway's same-epoch recovery re-runs idempotently (deterministic `seg:<id>:<E>:` keys) or a rollback installs `retire:records:{seg:<id>:<E>}` naming only epoch `E` | D7 |
| X58 (iteration-7 review) | A client loops Complete → publish-CAS-lost → fence release → Complete, faster than the drain retires each attempt's segments | Every fence increments `attempts` in the session record it already CASes, and a fence past `MAX_COMPLETE_ATTEMPTS` is refused — the session's only remaining exit is Abort. So one session can mint at most `MAX_COMPLETE_ATTEMPTS` segment epochs and at most that many `retire:records:{seg}` obligations, whatever the client's retry rate; `W_session` bounds *residency*, not attempt *rate*, and so could not have bounded this alone | D3, D7 (F18), §3 |
| X40 (F18) | Rollback → re-Complete → **publish while the prior attempt's `retire:records:{seg}` obligation is still pending**, then that obligation drains | The stale obligation names epoch-`E` keys `seg:<id>:<E>:*`; the re-Complete wrote a **new** epoch `seg:<id>:<E'>:*` and published `group = (<id>, E')`; draining the stale obligation deletes only epoch-`E` keys, so the published object's epoch-`E'` segments survive — no published object loses its map (the iteration-2 F18 refutation, closed by per-attempt scoping) | D7 (F18) |
| X38 | A part commit returns `CommitUnknownResult` and the batch later lands | The writer re-reads before compensating; only a settled not-committed outcome triggers compensation, so a live part's chunks are never orphan-marked | D5, `traits/src/lib.rs:730-745` |
| X39 | Sustained `PutObject` overwrites grow `retire:` past `SCAN_CAP` | `retire:` is walked in cursor-keyed bounded ranges, never one scan, so it never fails `ScanCapExceeded`; a lagging drain raises the oldest-obligation-age alarm (**D-D**) | D4, D6 (F11b) |
| X41 (F11a) | A client crash-loops part uploads against one `Open` session (residue never released) | `UploadPart` refuses `503` once all `MAX_INFLIGHT_PARTS` indices of `slot:<id>:` are held by crashed residue (a crashed attempt never deletes its slot, so residue occupies the key space exactly as a live part does), so the session's owned `sidx:` population stays `≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS ≤ SCAN_CAP/2` and its teardown scan cannot fail; the client aborts to recover (bounded availability cost) | D5, D6 (F11a) |
| X42 (F12) | A gateway drains a session inline while the reaper tears down the same session | The terminal batch `require`s the session record's exact bytes, so one delete-and-`count-1` wins and the other's precondition fails (no-op); the counter decrements **exactly once** — no low drift, so `MAX_SESSIONS` stays exact (the iteration-2 double-decrement fix) | D6 (**D-C**), §2 |
| X43 (finding 1) | An in-flight part attempts to create owned residue **after** the session has been fenced and its `sidx:` range walked | The part's slot reserve and every intent put carry `require(mpu == Open@E)`, so both fail once the session left `Open` — **no owned entry can be created after the fence**, so the walked-empty `sidx:` range stays empty and the terminal delete's empty-`sidx:` gate holds. A reserved-but-unwritten slot leaves no residue (intent precedes any fragment). Nothing is stranded | D5, §2 (finding 1) |
| X44 (finding 2) | A crashed in-flight part's owned residue outlives a session that **Completes** | The reaper's teardown walks `sidx:<id>:` for the `Completed` state too (not only `Aborting`), reclaiming the residue; the terminal delete waits for the `sidx:` range observed empty. Skipping the Completed walk would strand it forever — outcome (a) | D5, D6 (finding 2) |
| X45 (finding 5) | `DeleteObject` (or one member of a bulk `DeleteObjects`) removes a max **segmented** object | `unlink` installs one `retire:bytes:{generation}` (its chunks + `seg:` range) and returns O(1); the ~1.78 M fragment orphans drain in `B`-batches — never inline, so never over `E_tx`. Reader-safe grace starts at the drain's orphan mark (later than today's inline mark, so reclamation is never *earlier*; outcome (d) closed) | D4 (finding 5), [#509][i509] |
| X46 (finding 6b) | A foreign-clocked session carries an owned entry whose lease *looks* local | The reaper reads the **session's** `clock_source` first and skips the session before evaluating the owned lease, so a skewed owned stamp is never trusted; owned entries need no clock field of their own | D6 (F10, finding 6b) |
| X47 (iteration-4, adversary C) | Reconstruction/rebalance **repoints a committed segmented object's `seg:` fragment** (`P_old→P_new`) while a supersede/delete of that generation is in flight | The repoint **pre-marks `orphan:<P_new>` before writing the destination fragment**, then CASes `require(seg == prior)` **and** `require(inode == prior)`. **Repoint wins** (inode not yet superseded): it adopts `P_new` (deletes the pre-mark) and orphans the vacated `P_old`; a later supersede's key-range retirement re-reads the seg's **current** placement (`P_new`) at drain time and orphans it. **Repoint loses** (supersede advanced the inode first): the CAS is a no-op, the `P_new` pre-mark **stays**, so GC reclaims the abandoned destination fragment, and the drain orphans the seg's current placement (still `P_old`). On **either** branch every position is seg-referenced, pre-marked, or orphaned — no moved fragment is left unreferenced-and-unevidenced (outcome (a) closed; the earlier claim that the drain orphans the *pre-repoint* placement was the over-claim adversary C refuted, since a repoint could win before the supersede read) | D2, D7f (§1 `seg:` writers) |
| X48 (iteration-4) | The committed reference build resolves many **segmented** objects; one max object alone adds ~1.78 M `(server, fragment)` pairs | Per-object resolution is a bounded `seg:<id>:<E>:` range (no scan crosses `SCAN_CAP`); the aggregate in-memory reference set grows with total committed fragments (as today — segmentation only lets one object contribute up to ~1.78 M pairs). It is **not** admission-bounded (durable data is not refusable) and is charged as a **capacity cost** with a reference-build-size telemetry + alarm (`W_ref_committed`), never a stranding/absorbing/over-map/over-envelope outcome | D7(e), D6 (FU-3) |
| X49 (findings 3/5/6) | A part fragment authorized **before** the fence lands on disk **after** the reaper has orphan-marked and deleted its `sidx:` entry | Two properties close it: (1) the reaper orphan-marked the chunk's **full `staged` placement** (§1, fixed at intent), so the late fragment lands on an `orphan:`-covered position wherever it lands; and (2) the fragment write is a **fail-closed, `W_write`-bounded** await (`AGENTS.md:181-183`) — refusing to *renew* does not *cancel* it, so the deadline, not the renewal loop, is the bound — and the orphan grace satisfies the **strict** **`G_orphan > W_write + δ_clock`** under one clock, so the fragment lands **strictly before** its position's grace elapses (never at the boundary tick GC's inclusive `≥` check would already reclaim) and GC never reclaims the evidence before the late fragment is covered by it (outcome (a) closed). The renewal refusal (`write.rs:474-478`) is a supporting property (no *new* authorizations after the fence), not the bound | D5 (rule 1), D6, clock table |
| X50 (finding 4) | A single `PutObject` whose object cannot fit `MAX_MAP_CHUNKS` even at `chunk_size_max` | Refused with `400 EntityTooLarge` (the client must use multipart) — **never** silently segmented, because segmentation's staged publication needs an upload-id / `Completing@E` / epoch a single PUT has not got; in the S3 range (`≤ 5 GiB`) chunk-size selection always fits, so this is a guard, not a routine path | D7 (finding 4 carve-out) |
| X51 (adversary B) | A GET reads a segmented object's root, then a concurrent supersede/delete retires that generation and the drain deletes its `seg:<id>:<E>:*` records mid-resolution | The **resolve-retry rule** (D7h): a segment read that returns absent re-reads the root; a changed/absent root ⇒ restart against the current root or `NoSuchKey`; an *unchanged* root with an absent segment ⇒ fail-closed (a live generation never loses a segment, since `seg:` deletion follows the root flip). No torn map is ever returned — the reader-transparency claim holds without a record-grace clock | D7h, D2 |
| X52 (iteration-7 findings 1/4) | Two concurrent part commits for **different** part numbers race while a third part starts, all against one `Open` session | They **cannot conflict**: each commit's only writable session-scoped key is its **own** `slot:<id>:<k>` (deleted under `require(slot == prior)`), each start claims a *different* index under `require_absent`, and the shared session record is a **read** precondition. So there is no "benign collision" class to classify and no retry loop to bound — the iteration-6 shared-`sinf:`-counter design had both, with a false ≤ `MAX_INFLIGHT_PARTS`-round termination claim (fresh starts keep moving the counter) and a compensation branch that could discard a durable part. Compensation (`retire:bytes:{chunks}`) fires **only** for a genuine same-part loss (X10); a session that left `Open` is **never** self-compensated — its residue and slots are the reaper's (X7), which is what keeps the release exactly-once | D5 (rule 2), §1 `slot:` |
| X53 (finding 2) | The **first** `CreateMultipartUpload` on a **fresh or upgraded** store, where `mpuctl` does not yet exist | The create reads `mpuctl` **absent-as-`{count: 0}`** and initializes it in the same batch with `require_absent(mpuctl)` + `put { count: 1, max_sessions }` (rather than the CAS the steady-state path uses), so the first session is admissible with **no migration/init step**; a concurrent first-create that initialized it first makes the loser's `require_absent` fail and it retries against the re-read `1`. Without this the Create CAS `require(mpuctl == prior)` can never be satisfied and multipart is dead on a new store | D6 (**D-C**, F12, finding 2), §1 |
| X54 (DeleteObjects adjudication) | A bulk `DeleteObjects` removes 1,000 large/segmented objects in one request | The obligation-**installation** is byte-budgeted, not just the fan-out drain: the verb commits the per-object unlink + `retire:bytes:{generation}` installs in `B`-batches of `≤ E_tx/2` mutation bytes (≈ 50 max-generation obligations per transaction), **never** `1,000 × V ≈ 100 MB` in one transaction (over-envelope, outcome (d)); each transaction is per-object preconditioned, and every installed obligation's fan-out drains in `B`-batches thereafter. This proposal settles the `≤ E_tx/2` bound; the per-request batching is #509's within it | D4 (finding 5 / adjudication), [#509][i509] |
| X55 (iteration-7 capacity call) | A client stages parts past `MAX_STAGED_CHUNKS`, and `MAX_INFLIGHT_PARTS` commits race the cumulative check right at the ceiling | The part commit reads the bounded `psum:<id>:` **summary** range (never the fat chunk lists) and refuses `400 EntityTooLarge` past the ceiling, leaving the session usable and abortable. Racing commits can each observe sub-ceiling, but at most `MAX_INFLIGHT_PARTS` are in flight — enforced by the `slot:` key space, not observed — and each adds ≤ `MAX_PART_CHUNKS`, so the staged total never exceeds `MAX_STAGED_CHUNKS + MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, exactly the headroom `U_ref` charges. The bound therefore holds for **every** interleaving, which is what lets `MAX_SESSIONS` be derived from it (≈19 rather than ≈1 at maximal parts) | D4.4, D2 (`U_ref`), D6 |
| X56 (iteration-7 finding 3) | A gateway drains its own teardown inline while the reaper drains the **same** session's owned `sidx:` range, and both reach the same fragment | Each mark carries `require_absent(orphan:<pos>)` (decision 4.2), so the second writer's batch loses the guard, re-reads, and **skips** the already-evidenced position with its **original** `orphaned_at` intact; the `sidx:` deletes are idempotent, so both drainers converge on an empty range. Without the guard both marks land, the fragment's grace window restarts on every collision, and a persistently racing pair can postpone its reclamation indefinitely — the drain is then *not* the grace-preserving idempotent step X12 and `restore.rs:108-113` describe (a slow leak, not a stranding, but a contradiction of the stated property) | D4.2, D5 (rule 3), §3 |
| X57 (iteration-7 adversary) | A session is snapshotted in `Completing@E` **after** its segment-write phase (`seg:<id>:<E>:*` durable) but **before** its root flip; the image is restored and the restore pass fences it | The **restore-fence transition** `Completing@E → Aborting@E+1` (§2, §3) commits, in one batch, the state change, `retire:bytes:{session, parts}` **and** `retire:records:{seg:<id>:<E>}` — so that attempt's segments have a named deleter. Fencing it through the `Open` abort row instead (which the pre-iteration-7 text implied, since no `Completing → Aborting` edge existed) leaves those `seg:` records unreachable by **every** deleter the design defines: no root flip ⇒ no committed inode to supersede, and no rollback ⇒ no `retire:records:{seg}` obligation. Their fragments are still orphan-marked by `retire:bytes:{session}`, so this is record residue rather than lost bytes — but it is unbounded residue in a namespace the design otherwise bounds, and it was absent from the register | D-B, D2 (restore), D3, D7 |
| X59 (iteration-8 finding 1) | A part upload selects a placement naming server `S` from a topology snapshot; the operator records `S` draining, `reconciliation_status(S)` answers `Satisfied` (no record yet names `S`) and `S` is wiped — **then** the intent lands, the part commits, and Complete publishes | The **drain fence**: the `sidx:` intent batch carries `require_absent(desired:dserver:<S>)` for every server in the placement it records (D2). The two writes therefore race on one key with only two outcomes — intent first (the entry exists before any reconcile can read the drain, so the staged set makes the status `Pending`, never `Satisfied`) or drain first (the intent fails and re-plans against `Topology::excluding(draining)`). Filtering the *selector* alone leaves the select→commit window unfenced, and that window is exactly where outcome (c) reappears one interleaving beyond the F6 trace. The destination side of a staged re-place and a committed segment repoint carries the same fence | D2, §1 (`sidx:`), §3 |
| X60 (iteration-8 finding 2) | A part commit interleaves **between** the reaper's two progress reads, on a session whose earlier progress is older than `W_open` and which owns no other live lease | The reaper reads **source before destination** — `slot:<id>:` then `psum:<id>:` (D6, normative) — mirroring D2's `sidx:`→`part:` build order, because the commit deletes the slot and writes the summary in one batch. Destination-first sees the part in *neither* range, collapses `progress` to `created_at`, and fences a session that has just made durable progress (a false positive that discards committed staged bytes). Source-first observes it in both ranges or in `psum:` alone; the mirror staleness (a slot seen, then deleted before the `psum:` read) credits a stamp at most one pass old — a one-pass false *negative*, the safe direction | D6, D2 (read order) |
| X61 (iteration-9 finding 1) | A repoint's pre-mark ages past `G_orphan`; GC deletes the destination fragment and, in the window before its `cleanup` batch commits, the adoption CAS runs against an `orphan:<P_new>` key that still holds exactly its pre-mark bytes | `require(orphan:<P_new> == prior)` would **pass** here — GC deletes the fragment inside its per-server loop but commits the matching `orphan:` key delete only after the whole fleet sweep (`gc.rs:189-207`), so the ledger key outlives the bytes and the precondition proves entry survival, never fragment presence. Closed by making the window load-bearing: the adoption CAS refuses fail-closed when its own pre-mark is older than **`W_repoint`**, and **`G_orphan > W_repoint + δ_clock`** (strict) means GC's `now ≥ orphaned_at + G_orphan` cannot have fired for any pre-mark an adoption may still use — the deleted-fragment window is unreachable, not merely narrow. Same construction as `G_orphan > W_write + δ_clock` at the other end of the grace window (X49) | D4.2, D2 (repoint/re-place), knob table |
| X62 (iteration-9 finding 4, **corrected**) | An `UploadPart` renewal reads a **live** lease, the reaper then reads that same lease as **expired**, and the renewal commits before the reaper's fence | Real, and the first-round rejection of it was wrong. Renewal's `existing.lease_expiry_millis <= now_millis` test is evaluated against the **renewer's sampled clock in the gateway**, then committed under `require(key == prior_bytes)` (`metadata.rs:746-760`) — the store enforces bytes, not expiry — so "a lapsed lease can never be renewed" does not hold across the commit window, and a single wall clock does not close a read-then-act TOCTOU spanning two keys. Closed by giving the **idle** fence a serialization edge to the evidence it judged: `require(slot:<id>:<k> == prior)` for every slot observed present and `require_absent(...)` for every one observed free (≤ `MAX_INFLIGHT_PARTS`, no new scan or record). Sufficient because every live renewer holds a slot and the half-TTL loop rewrites its slot lease with its `sidx:` leases, so any renewal invalidates the fence; the reaper re-derives next pass. The `W_session` arm carries no such precondition by design (**D-A**) | D6, D5 (`slot:`), D-A |
| X63 (iteration-9 finding 3) | A fragment carries a **stale** `orphan:` mark aged past `G_orphan` while still referenced (the rollout-skew case: an old custodian marked live staged fragments); a later supersede/delete then removes the last reference | GC's safety gate tests the reference set first, so a live reference legitimately overrides orphan evidence (`gc.rs:160-170`) and such marks outlive the upgrade that stops new ones. An unconditional "present ⇒ skip" would keep the ancient stamp, and GC — now seeing the fragment unreferenced with grace long elapsed — reclaims on the next pass, leaving a GET that overlapped the deletion **no grace at all**. Closed by scoping the skip to one retirement: the mark carries its **unreference-event identity**, the same identity skips (the concurrent-drainer property, X56), a **different** identity is stale evidence and is re-stamped with a fresh `orphaned_at` — bounded to once per event per position, so no racing pair can loop it | D4.2, X56, X12 |
| X64 (iteration-9 finding 5) | A rolling configuration change leaves two gateways with different locally derived `MAX_SESSIONS` while both admit against one counter | The counter orders increments but records no threshold, so the larger-valued gateway would keep admitting past the smaller limit and break `MAX_SESSIONS × U_ref ≤ W_ref` — with the overrun landing on the maintenance plane, not the gateway that caused it. Closed by storing the governing limit **with** the counter (`mpuctl = { count, max_sessions }`, CAS'd as one record), so every reserver enforces the ledger's value; a gateway whose local derivation disagrees **refuses and alarms** (fail-closed configuration skew) rather than deferring to either value, and changing the limit is an explicit operator CAS | D6 (**D-C**), D2 (`W_ref`) |
| X65 (iteration-9 finding 2) | A `sidx:` value's `staged` placement length does not match its scheme's fragment count | **Not a decode error** — placement length is the standing convention's named example of a *contextual* check ("liberal on read, strict in maintenance paths", `AGENTS.md:146-149`, [ADR-0045][a45]). The record decodes and the staged reference build classifies it into `ReferenceSet.malformed`, where GC's safety gate protects **every** fragment bearing that chunk id (`gc.rs:160-170`, the `malformed-placement` skip) and the drain answers `PendingMalformed { chunks }` instead of an unexplained stall. Failing it at decode would convert a quarantinable record into an error that aborts the whole reconcile step before GC, scrub, reconstruction and rebalance run (`reconciliation.rs:75-112`) | §1 (validation boundary), D2 |
| X66 (iteration-9 finding 6) | A `metadata::rename` moves the target dirent **after** Complete resolved it and **before** the root flip commits | The overwrite branch guarded only `require(inode == prior)`, so the flip could succeed by overwriting an inode now bound at *another* name, leaving the multipart target absent or rebound while the client is told the Complete succeeded — publication is defined against the **dirent identity** (§1's `publish_target`), so guarding the inode alone does not pin what was published. Both branches now pin the dirent: `require_absent(dirent)`+`require_absent(inode)` for a fresh key, `require(dirent == prior)`+`require(inode == prior)` for an overwrite. Either binding moving is a `Conflict` that re-resolves and retries within `R_publish` | D3, §3 (root flip) |
| X67 (iteration-9 finding 7) | A reference build reads committed inodes **before** a root flip and reads `part:` **after** the publication's `retire:records:{parts}` drain deleted them | The published object's chunks appear in **neither** class, so `genuinely_holds` misses them and a drain answers `Satisfied` while a live object references the server (outcome (c), one handoff past the staged case). Publication is a third instance of the same source→destination move, so the normative read order extends to three classes — **`sidx:` → `part:` → committed inodes** — and every interleaving then observes each chunk in at least one of them | D2 (read order), D3 |
| X68 (iteration-9 finding 8) | A slot reserve returns `CommitUnknownResult` with `may_still_commit == true`, and the re-read observes the index **absent** | Absence proves nothing in that case — the batch may be applied afterwards (`traits/src/lib.rs:241-247`, FDB 1031 and every TiKV case). Probing a different index on that evidence can leave one request owning **two** durable slots, and a run of timeouts exhausts `MAX_INFLIGHT_PARTS` into a permanent `503` with no crash to blame. The reserver instead **retries the same index with the same `attempt_id`** — the batches are mutually exclusive through `require_absent`, so at most one lands and either outcome is the same observable slot — probing on only once it reads another attempt's id, and refusing `503` after bounded ambiguous retries. With `may_still_commit == false` one re-read still settles it (FDB 1021) | D5 (`slot:`), rubric *Transactions* |
| X69 (iteration-9 finding 9) | An operator lowers `MAX_INFLIGHT_PARTS` (or `MAX_ROOT_SEGMENTS`) while records written under the higher value are still live | Decode validated against the **live knob**, so those records would stop decoding the instant the knob dropped — and the slots this document explicitly permits to outlive a lowering could then be neither renewed, committed **nor torn down**, while already-published roots above a lowered segment cap would become unreadable. Decode now validates **format maxima** (constants of the encoding, changed only by a versioned format change); the current knob is enforced where new work is *admitted* — slot reserve, part commit, Complete fence — so every record written under a legal configuration stays decodable under every later one | §1 (validation boundary), ADR-0045 |
| X70 (iteration-9 finding 10) | A `scan_page` continuation skips a `retire:` key under concurrent mutation | Today's `scan` leaves ordering unspecified, so a conforming `scan_page` could paginate in an order whose cursor silently skips a key — and a skipped obligation retains its bytes and records **forever**, the exact failure pagination exists to prevent. The trait now fixes raw byte-lexicographic order, exclusive `after`, `next = Some(last key)` / `None` only at exhaustion, and **no-skip for stable keys** under concurrent mutation (inserts behind the cursor and mid-walk deletes may be missed or duplicated). Duplicates are harmless — each drain step is idempotent under `require(retire:… == prior)` — so no backend needs snapshot isolation, which is what keeps it implementable on redb, FDB and TiKV alike; `metadata-conformance` asserts all four | §3, D4, traits seam |
| X71 (iteration-9 finding 11) | A **foreign-clocked** session is client-aborted, or Completes, and its gateway then dies before the teardown drains | The clock guard skipped the session *wholesale*, so the owned-`sidx:` walk and the terminal delete — neither of which reads a session timestamp — never ran, and the operator abort verb could not help because a `Completed` session is not abortable (`404`): records and the admission slot resident forever, an absorbing state reached without any clock disagreement being acted on. The guard now gates **judgments, not the pass**: it suppresses only the arms comparing a foreign timestamp (`W_open`, `W_session`, `W_completing`, `W_tombstone`) while the clock-free teardown always runs. The one residue is a `Completed` session's tombstone window, measured from its own `completed_at_millis` and genuinely unjudgeable — that waits on FU-6's second verb, an operator-authorized terminal expiry | D6 (F10), FU-6 |
| X72 (iteration-9 finding 5, schema) | An implementation follows §1's record table rather than decision 6's prose and stores a bare integer at the admission key | The two disagreed after the previous round: decision 6 required `{ count, max_sessions }` while §1 still defined one integer, and an implementation following the table cannot detect gateways enforcing different derived limits — the larger-valued one admits past the smaller bound and breaks `MAX_SESSIONS × U_ref ≤ W_ref`. One key, one schema, stated identically in both places: `mpuctl = { count, max_sessions }`, CAS'd whole so the count and the limit it was checked against can never be read apart | §1, D6 (**D-C**), X64 |
| X73 (iteration-9 finding 3, guard) | A drain must **replace** a stale orphan mark, but the blanket rule required `require_absent` for every mark written | `require_absent` makes the very replacement the stale-evidence rule demands impossible — the batch conflicts, the writer falls back to treating the ancient mark as already-evidenced, and GC reclaims immediately after the later delete, tearing a concurrent reader. The guard is now stated **per arm**: `require_absent(orphan:<pos>)` for a position observed absent, an exact-value `require(orphan:<pos> == prior)` for a stale-evidence replacement carrying a *different* unreference-event identity, and no mutation for a same-identity skip. The iteration-7 blanket rule was right for the concurrency property and wrong for the reader-safety one | D4.2, X63, X56 |

### What the implementing slices change (summary)

- `crates/traits/src/lib.rs` — **the one narrow-seam change this protocol requires** (ADR-0010 /
  ADR-0016 narrow-seam rule): `MetadataStore` gains a **bounded/paginated range scan**
  (`scan_page(prefix, after: Option<&[u8]>, limit) -> (Vec<(key, val)>, next: Option<Vec<u8>>)`) so
  the retirement drain can walk the `retire:` namespace in cursor-keyed pages, none exceeding
  `SCAN_CAP`. **The signature alone is not the contract, and the semantics are normative
  (iteration-9 finding 10):** today's `scan` leaves ordering *unspecified*, so a `scan_page` that
  inherited that freedom could return pages whose continuation silently skips a key — and a skipped
  `retire:` obligation retains its bytes and records **forever**, which is the exact failure the
  paginated walk exists to prevent. The trait therefore fixes, and the `metadata-conformance` suite
  asserts on every backend: (a) results are ordered by **raw byte-lexicographic key**, identically
  across backends; (b) `after` is **exclusive** — a page starts strictly after that key; (c) `next`
  is `Some(last_key_returned)` when more may remain and `None` only when the prefix is exhausted at
  that instant; and (d) under concurrent mutation the walk is **no-skip for stable keys**: a key
  present throughout the walk and not lexicographically before the cursor is returned exactly once,
  while keys inserted before the cursor after it passed, or deleted mid-walk, may be missed or
  duplicated. (d) is what the drain actually needs — it is idempotent and re-entrant per obligation
  (`require(retire:… == prior)`), so a duplicate is a no-op, whereas a skip is unbounded retention;
  a fresh walk each pass re-derives anything a mutation raced. No snapshot isolation is required of
  any backend, which is what keeps this implementable on all three. Today's `scan(prefix)` is prefix-only and complete-or-`ScanCapExceeded`
  (`crates/traits/src/lib.rs:772-776`, `SCAN_CAP` at `:275-292`), so a `retire:` population that
  grows past `SCAN_CAP` under sustained overwrites cannot be enumerated by any single `scan` — the
  drain-health-alarmed, unbounded `retire:` namespace (decision 6) is walkable only through a
  paginated primitive. Every backend (`metadata-redb`, `metadata-fdb`, `metadata-tikv`), the DST
  sim store, and the `metadata-conformance` suite implement it. No other namespace needs it — all
  the rest are bounded prefixes or reference-based. (The alternative — packing `retire:` into
  fixed shards under `scan(prefix)` — only divides an *unbounded* population by a constant, so a
  shard can still cross `SCAN_CAP`; the paginated seam is the honest primitive.)
- `crates/core/src/metadata.rs` — the new key helpers and parsers (`mpuctl:`, `slot:`, `mpu:`,
  `part:` with its small `psum:` summary sibling written in the same batch, `sidx:` — the **disjoint owned-staging record** `sidx:<id>:<part>:<chunk>` carrying a
  `PendingEntry` with `owner` **and** `staged` (the chunk's EC placement), **not** under `pending:`
  — `seg:` (epoch-scoped, `seg:<id>:<epoch>:<i>`) and `retire:`); `PendingEntry.owner` and
  `PendingEntry.staged` (both additive, `skip_serializing_if`, `Some` only on a `sidx:` value); the
  `Flat | Segmented` chunk-map variant carrying the `group = (upload-id, epoch)` (decision 7 — the
  ADR-scale change); session-fenced publication committers beside the lease-guarded ones, computing
  the published version from the re-read prior at each flip attempt (`prior.version + 1`, not a
  fence-frozen version — finding 2); supersede **and `unlink`** stop expanding orphans inline,
  routing through `retire:bytes:{generation}` (decision 4, finding 5); the serialized fleet
  admission counter and the **per-session `slot:` in-flight key space** — claim by `require_absent`
  on one index, release by a keyed delete inside the owning batch (**D-C**/**D-D**); the
  terminal delete preconditioned on the session record's exact bytes and gated on the session's
  `sidx:` range observed empty (exactly-once `mpuctl.count` decrement, no stranded residue);
  byte-budgeted (not count-budgeted) drain/segment batches.
- `crates/core/src/write.rs` — staged placement (committed **and** in-flight owned) selects against
  `Topology::excluding(draining)` (`placement.rs:141-152`); `intent` writes the owned `sidx:`
  staging entry with the `WritePlan` placement in `staged` (finding 4) **preconditioned
  `require(mpu == Open@E)`** (finding 1) **and `require_absent(desired:dserver:<S>)` per selected
  server** — the drain fence that makes the filter atomic with the drain request, with a re-plan
  on failure (decision 2, iteration-8 finding 1); `UploadPart` claims its `slot:<id>:<k>` index before the
  first chunk (also fenced, stamping `reserved_at_millis` for the reaper's progress maximum,
  refusing `503` when the range is full) and its commit runs the cumulative `MAX_STAGED_CHUNKS`
  check over the bounded `psum:<id>:` range, refusing `400 EntityTooLarge` past the ceiling
  (decision 4.4); the renewal loop renews the owned `sidx:` leases and
  **refuses rather than resurrects** once the session leaves `Open` (`write.rs:474-478`) — a
  *supporting* property that stops *new* fragment authorizations after the fence, **not** the
  late-fragment bound. A fragment authorized *before* the fence is bounded by the fail-closed
  **`W_write`** write deadline (the *await discipline* MUST, `AGENTS.md:181-183`; decision 5, not
  the renewal loop), coupled to the orphan grace by the strict `G_orphan > W_write + δ_clock` so no
  straggler lands unevidenced (findings 3/5/6); the live-session losing-writer compensation path
  releases the slot.
- `crates/custodian/src/gc.rs` — `ReferenceSet` gains the disjoint staged set (committed `part:`
  **and** in-flight owned `sidx:` fragments, built by bounded per-session ranges, finding 4) and
  resolves segmented maps by bounded `seg:` ranges; the expiry arm is unchanged (it scans only
  `pending:`, which no longer holds owned entries).
- `crates/custodian/{restore,scrub,reconstruction,desired_state}.rs` — per decision 2's table;
  restore's `pending_chunks` scan is bounded again (owned entries are disjoint, finding 3), and
  restore additionally fences resurrected sessions (**D-B**); `desired_state` counts in-flight
  owned fragments as held (finding 4).
- `crates/custodian/` — the reaper loop (#625), dispatched from `reconcile_step` like every other
  pass (`reconciliation.rs:75-112`); admission counter, `sidx:` reclamation, cursor-keyed
  `retire:` drain, drain-health alarms.
- `crates/gateway-s3`, `crates/server` — the verbs, the state/verb answer table, serialized
  admission control (#508); the subresource denylist that refuses multipart today
  (`crates/gateway-s3/src/lib.rs:335-346`, object-route guard `:1696-1709`) loses its multipart
  entries in that slice.
- The **living architecture** documents ([runtime view][arch6], [crosscutting concepts][arch8])
  gain the new record classes, the segmented map, and the reaper loop **in the slice that lands
  them** — the docs-currency rule for a change that adds persisted fields and API operations
  (`AGENTS.md:154-157`). This proposal, which lands no code, is not that slice.

## Alternatives considered

- **Keep lease liveness and renew the leases of staged parts.** Rejected: it puts a correctness
  timer on an inherently long-lived operation, and it makes the *renewal loop* a correctness
  dependency for data whose client may be offline for hours. It also leaves the staged bytes
  inside `pending:`, where the deployed `Defer` posture makes their residue unreclaimable (F3).
- **A per-session mutation counter as the *publication proof*, CAS'd by every part commit.**
  Rejected *as a proof*: it would give Complete a one-precondition proof exactly as the fence
  does, but it puts that correctness dependency on a per-session key every part commit must CAS,
  serializing the 4–16 parts an S3 client uploads concurrently. The fence buys the same proof
  with a CAS only on *state transitions*, which are rare — so publication correctness never
  depends on a counter. **This is not the `slot:` in-flight cap (decision 5).** The slot table is
  an *admission cap* (**D-D**'s mandated in-flight-part bound), **not** a proof, and it is not a
  counter at all: each part attempt claims and releases its **own** key, so the part path has no
  shared writable key, nothing preconditions publication on it, and there is no CAS to lose to an
  unrelated part. That is also why iteration 6's per-session counter was replaced rather than
  merely re-argued: a shared `sinf:` CAS inside the part-commit batch made a durable part's commit
  fail on another part's progress with no true termination bound (iteration-7 finding 1). The
  remaining asymmetry is admission only: the *per-create* fleet counter (**D-C**) is a hot key for
  `CreateMultipartUpload` alone, and the chunk-write path touches no counter.
- **Scan-then-create admission (iteration 1's "no hot counter").** Rejected on rework: a
  population read followed by a create is race-prone (ADR-0046 flagged the identical shape for
  `DeleteBucket`), admits unbounded fleet-wide overshoot, and cannot enforce the `SCAN_CAP`
  invariant that a cap overrun would violate — and that overrun halts the maintenance plane,
  which is data-loss-class. The serialized counter (**D-C**) makes the bound exact. The cost
  shown: a single hot key on the Create path only; part commits stay counter-free.
- **Owned residue under `pending:` — a global `scan("pending:")` backstop (iteration 1's F11a
  hole) or even a per-session sub-scan of a shared `pending:` namespace (iteration 3's finding-3
  hole).** Rejected: owned cardinality is per-chunk of in-flight parts, so fleet-wide
  `MAX_OWNED_FLEET` can exceed what a single scan holds, and *any* code that does `scan("pending:")` —
  the reaper backstop, the restore pass's `pending_chunks`, the expiry sweep — would then risk
  `ScanCapExceeded` and halt, the exact failure it was meant to prevent; even below the cap it would
  inflate the ordinary `pending:` scans and make the expiry sweep a consumer of owned entries (the
  #557 hazard). Owned entries are therefore a **record class disjoint from `pending:`**
  (`sidx:<id>:<part>:<chunk>`, **D-D**/finding 3), read only through per-session bounded ranges: no
  global scan of owned entries exists anywhere, and the ordinary `pending:` scans keep exactly their
  present bound.
- **Per-part exact-value preconditions at Complete.** Correct, and it is the shape a first
  implementation reaches for; rejected on the envelope: 10,000 part values in one batch cannot
  fit inside 10 MB / 5 s (`traits/src/lib.rs:744-758`), so the largest legal upload would be the
  one that cannot complete.
- **Staging inside `pending:` with a longer TTL.** Rejected: it is exactly the "synthesized
  encoding into an existing namespace" [ADR-0046][a46] rejected for buckets, it inherits the
  expiry-based reclamation hazard (#557), and it makes every consumer of `pending:` — GC, restore,
  the sweep, allocator recovery — implicitly a consumer of multipart semantics.
- **Inline orphan expansion with a "large object" special case.** Two code paths, two correctness
  stories, and the boundary between them is exactly where the bug hides. The retirement ledger is
  one path for both.
- **Synchronous Abort teardown.** Rejected on the same envelope arithmetic as above, and it would
  make Abort's latency proportional to the session's size — an unbounded HTTP response time for
  the verb clients call *to recover*.
- **A smaller reference-set scan bound.** Rejected: choosing a smaller cap only makes the halt
  arrive earlier (`SCAN_CAP` is a correctness constraint, not a tuning knob,
  `traits/src/lib.rs:275-292`). The bound has to come from admission and bounded key ranges.
- **Bounding session life with `W_open` alone (no `W_session`).** Rejected on rework: `W_open` is
  the *idle* window; a live, progressing session is bounded by it by nothing, so a drain behind
  such a session stalls indefinitely (F14) — an unbounded availability cost. The administrative
  `W_session` ceiling (**D-A**) is what bounds residency; it never falsifies a publication proof.
- **Resumable uploads across a metadata restore.** Rejected on KISS (**D-B**): resumption would
  require the restore to prove the staged bytes still exist, which a records-only image cannot do.
  Fencing every resurrected session to `Aborting` is the simple, safe rule; the cost is that an
  in-flight upload at restore time restarts from the beginning.
- **Deferring chunk-map segmentation (iteration 1's FU-1).** Rejected on rework: the honest flat
  ceiling is ~165–381 MiB at default chunks and ~5–12 GiB even at large chunks, below the >10 GiB
  launch requirement — so segmentation is not a follow-up, it is decision 7. It is designed *with*
  decision 4's staged-obligation machinery (one pattern family, **D-G**), not beside it. The
  cost shown: it changes the `InodeRecord.chunk_map` shape for the ~19 `.chunk_map` read sites, so
  it is recommended for ADR graduation (successor to ADR-0047's record shape) — but deferring it
  would ship a feature that cannot meet its own requirement.
- **Letting staged bytes go unscrubbed and unrepaired** (the smaller diff for decision 2).
  Rejected because the cost class is durability, not availability: over a staging window measured
  in hours, unrepaired fragment loss accumulates and Complete would publish a map over fewer than
  `k` survivors. Only availability, latency, capacity and operational costs are acceptable
  trade-offs here.

## Graduation criteria

### The F1–F18 disposition list

Exactly one disposition per row: **eliminated** by the design (with the observable that would
catch a regression), a **bounded non-safety cost** (with bound, rationale, and follow-up), or
**flagged NEEDS-HUMAN** (never self-accepted). No refutation-standard outcome — (a) stranded with
no bounded reclamation, (b) a state nothing can exit, (c) a map over reclaimable bytes, (d) an
obligation past the envelope — is disposed of as an accepted cost.

| # | Disposition | Mechanism | Binding observable |
|---|---|---|---|
| **F1** absorbing state on publication CAS loss | **Eliminated** | D3: bounded retry, then fence release to `Open`; reaper rollback for a crashed completer; §2's exit table | After a concurrent `PutObject` wins, the session is never left in `Completing`; a state-machine test asserts every state has a taken exit |
| **F2** staging-record disposal at publication | **Eliminated** | `retire:records:` installed in the flip batch; drained in bounded batches; the mode lives in the key | After Complete + full drain: `scan("part:<id>:")` is empty, and **no** `orphan:` record exists for any published chunk |
| **F3** abort-race residue unreclaimable under `Defer` | **Eliminated** | D5: disjoint `sidx:` owned record, live-session loser compensation, reference-based per-session backstop; the expiry sweep scans only `pending:` and so never touches an owned entry | Kill a part upload, run the deployed configuration (`Defer`, no attestation): residue reclaimed within `W_session`; owned entries survive an expiry sweep while `Open` (they are not under `pending:`) |
| **F4** obligation fan-out vs. the envelope | **Eliminated** | D4: retirement ledger for every superseding publication **and for `DeleteObject`/`DeleteObjects` of a large/segmented object** (finding 5 — `unlink` stops orphaning inline); D7 staged publication for the map itself | Overwrite **and delete** a max-size segmented object on FoundationDB: both return in one O(1) batch, and every commit in the batch inventory (including the drain) stays inside the envelope; a bulk `DeleteObjects` of 1,000 large/segmented objects installs its obligations in `≤ E_tx/2` byte-budgeted transactions (≈ 50 per commit), never one `1,000 × V ≈ 100 MB` over-envelope commit (X54) |
| **F5** non-idempotent Complete / retry after unknown outcome (incl. the segment-write phase) | **Eliminated** | Flip + `Completed` in one batch; deterministic segment keys/content; the session record is the durable evidence; re-read protocol `traits/src/lib.rs:730-745`; tombstone window | Retry Complete after an injected `CommitUnknownResult` mid segment-write phase: exactly one publication, one obligation set, identical ETag, no double-written segment |
| **F6** maintenance-pass visibility split | **Eliminated** (safety half); **bounded availability cost** (drain half) | D2's per-consumer table; staged placement excludes draining servers; drain bound re-derived as `W_session` | The wipe trace: drain a server holding staged fragments → `Pending`, never `Satisfied`. **Cost:** a drain waits up to `W_session`. **Follow-up:** FU-2 (urgent operator-forced staged evacuation) |
| **F7** unbounded staged state halts the custodian plane | **Eliminated** | Serialized admission counter (exact `MAX_SESSIONS`) + bounded key-range access for every derived namespace + the reaper (progress) | Reaper stopped + sustained creation/overwrites: `reconcile_step` keeps succeeding and creation refuses with `503`; assert no namespace ever fails `ScanCapExceeded` |
| **F8** vacuous or unsound reaper design | **Eliminated** | D6: liveness from records the protocol writes (part stamps + owned leases), clock-sound detection, the full false-positive / false-negative / race / crash / stale-snapshot table, and the normative `#625` ordering | D6's failure-mode table in full, run as seeded DST |
| **F9** client-visible semantics of fenced states | **Eliminated** | D3's verb × state table (incl. `W_session`-fenced and restore-fenced sessions); Abort answers from the fence commit alone | A wire-level test per cell of the table (owned by #508), plus: Abort of a 10,000-part session answers from one O(1) batch |
| **F10** clock-lifecycle ownership | **Eliminated** | The clock-lifecycle table: one source (deployment wall clock), the `clock_source` guard **read from the session record first** (so the owned-lease liveness read only ever runs for a session the reaper's clock owns — finding 6b, no per-owned-entry clock field), and staged reclamation made clock-free | A foreign `clock_source` session is skipped and alarmed **before any owned lease is read** (X46), never reaped; DST with skewed producers reclaims nothing it should not |
| **F11** namespace cardinality escapes the admission formula (3 surfaces) | **Eliminated** | (a) owned entries are the **disjoint `sidx:` record class** (never under `pending:`, so no global `pending:` scan enumerates them — finding 3) + **enforced** `MAX_INFLIGHT_PARTS` slot reservation (the `slot:<id>:` key space) with **crashed residue counted against the cap**, so per-session owned `≤ MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS ≤ SCAN_CAP/2` by construction (not merely observed), and the fleet-wide `MAX_OWNED_FLEET` is `W_ref`-coupled, read only per-session; (b) `retire:` walked in cursor-keyed bounded ranges + drain-health alarm; (c) tombstones counted by the serialized counter, retention bounded by `W_tombstone` | Crash-loop part uploads against one `Open` session and assert `503` at the cap and its teardown scan never fails (X41); stage a fleet-wide owned population larger than any single global scan could hold and run restore + reaper + overwrites; assert no scan fails `ScanCapExceeded` (X25) and `scan("mpu:") ≤ MAX_SESSIONS` |
| **F12** admission race: scan-then-create is not a bound | **Eliminated** | **D-C**: serialized slot reservation (counter CAS'd in the create batch, released in the terminal delete **preconditioned on the session record's exact bytes**, so the decrement is exactly-once even under concurrent gateway+reaper drainers); contention is the `503` | Race fleet-wide creates against the cap: exactly `MAX_SESSIONS` admitted, no overshoot; part commits never touch the counter; race an inline drain against the reaper for one session and assert the counter decrements exactly once (X42); and the **first** `CreateMultipartUpload` on a fresh/empty store succeeds and leaves `mpuctl == { count: 1, max_sessions }` (absent-as-zero bootstrap, X53), a create+abort round-trip returning it to 0 |
| **F13** restore resurrection mints a publication over reclaimed bytes | **Eliminated** | **D-B**: restore fences/aborts every session open in the restored image — `Open` by the abort fence, `Completing` by the restore-fence transition that retires that attempt's `seg:` records (X57, the iteration-7 adversary's enumeration gap); the records-only proof scoped to unrewound records (D1.4); executions X17, X17b, X57 | X17 as a test: `sessions_fenced > 0`; a Complete retried against a restored session returns `4xx`, never a publication; **X57 as a test**: fence a `Completing` session that already wrote segments and assert its `seg:<id>:<E>:*` range drains empty |
| **F14** unbounded session residency | **Eliminated** | **D-A**: `W_session` administrative ceiling from initiation, deployment default, tighten-only per bucket; the reaper's arm (ii); drain bound and F6 re-derived from it | X18 as a test: a live one-part-per-window session is reaped at `W_session` and a drain behind it clears |
| **F15** reaper stale-snapshot judgment | **Eliminated** | **D-E**: per-session fence-then-walk reclamation (no global filtered `pending:` snapshot); the no-staler rule with its DST observable | X19 as a test: a session created mid-pass is never condemned; its live part's renewals survive |
| **F16** `W_completing` measured from the wrong instant | **Eliminated** | **D-E**: `fenced_at_millis` stamped in the session record at the fence; rollback measured from it | X6 as a test: a healthy Complete begun long after its last part is not rolled back |
| **F17** cumulative admission at `UploadPart` overstated | **Eliminated** | **D-E**: best-effort early refusal, authoritative check at Complete (frozen part set); **D-D**'s in-flight cap for the hard namespace bound | Concurrent `UploadPart`s overshoot the early check (allowed); Complete refuses the over-ceiling assembled map authoritatively |
| **F18** segmentation's staged publication leaves residue | **Eliminated** | D7: one pattern family with D4/D5 — **per-attempt (epoch-scoped) segment keys** `seg:<id>:<E>:*` so a stale rollback obligation names only its own epoch and can never delete a later attempt's published segments; evidenced, bounded, reclaimable segments; deterministic idempotent same-epoch recovery; per-object bounded resolution; byte-budgeted segment-write batches | X37 + **X40** (rollback → re-Complete → publish while the prior `retire:records:{seg}` is still pending → drain it; the published segments survive) + D7's failure-mode table as seeded DST: crash between segment writes and flip, same-epoch recovery, rollback-to-new-epoch, supersede |

### Accepted costs register (computed numbers, **D-F**)

Each is a bounded availability / latency / capacity / operational trade-off. None is a
refutation outcome. `V = 100 KB`; headroom keeps an encoded value `≤ V/2 = 50 KB`;
`SCAN_CAP = 1 << 20 = 1,048,576`.

| Cost | Bound (computed) | Rationale | Follow-up |
|---|---|---|---|
| **Flat object ceiling** (no segmentation) | `MAX_MAP_CHUNKS = ⌊50000/b_ref⌋` = **165–381 chunks** (`b_ref` = 131–302 B); **165–381 MiB** at 1 MiB chunks, ~5–12 GiB at 13–32 MiB chunks | the value ceiling is inherited and pre-existing (a 5 GiB single PUT already crosses it); refusing past it is safe, committing is not | **subsumed** — decision 7 lifts it (segmentation is in scope, not deferred) |
| **Segmented object ceiling** | `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS × chunk_size` = 312–520 × 165–381 chunks = 51,480–198,120 chunks ⇒ **50.3–193.5 GiB at 1 MiB chunks**; S3's 5 TiB reachable only at **26.5–102 MiB chunks** (traded against gateway RAM); `MAX_PARTS_PER_SESSION = 10,000` is not binding in `[10 GiB, 5 TiB]` (5.1 MiB–524 MiB/part) | >10 GiB is met at default chunks with worst-case margin; the full S3 5 TiB is a chunk-size/RAM trade, stated honestly | **FU-1** — record-shape ADR (successor to ADR-0047); larger-object tuning |
| **Max part size** (flat `part:` record) | `max_part_bytes = MAX_PART_CHUNKS × chunk_size` = **165–381 MiB at the default 1 MiB chunk** (`MAX_PART_CHUNKS = ⌊50000/b_ref⌋` = 165–381, the same value rule as the map). A part above it is refused at `UploadPart` (`400 EntityTooLarge`), session left usable. To accept **S3's 5 GiB part maximum** a deployment raises `chunk_size` (a 5 GiB part fits once `chunk_size ≥ 5 GiB/165 ≈ 31 MiB`, traded against gateway RAM `chunk_size × max_concurrent_encodes`) | a `part:` record is one value; refusing past it is safe and bounded (never an over-`V` commit — the iteration-1/iteration-4 hidden-ceiling class, here surfaced). Objects >10 GiB are met with many in-range parts (10,000 × 165 MiB ≈ 1.6 TiB), so the *object* requirement does not need 5 GiB parts | **FU-5** — part-record segmentation (if a deployment needs 5 GiB parts at small chunks); or operator `chunk_size` tuning |
| **Concurrent-session capacity** | `MAX_SESSIONS = ⌊W_ref / U_ref⌋` — **derived, not chosen**, enforced exactly by the serialized counter, so `Σ_sessions footprint ≤ W_ref` for **every** part-size distribution. `U_ref = min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) × MAX_PART_CHUNKS, MAX_STAGED_CHUNKS + 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS )` charges **each committed part its full `MAX_PART_CHUNKS`** (the iteration-4 fix — a part is not one unit) but no more than the **enforced staged ceiling plus its bounded overshoot** (decision 4.4, the 2026-07-24 call). Computed at in-range `MAX_PART_CHUNKS ≤ 381` (a "5 GiB part" of 5,120 chunks is inadmissible — decision 4). With `W_ref = 4,000,000` chunk-refs (≈ 36 M `(server,fragment)` pairs, a few GB): **small parts** (`MAX_PART_CHUNKS = 5`, `MAX_INFLIGHT_PARTS = 16` ⇒ `U_ref = 50,080`, the raw term binding) ⇒ **`MAX_SESSIONS ≈ 79`**; **max in-range parts** (`MAX_PART_CHUNKS = 381` ⇒ `U_ref = 210,312`, the ceiling term binding) ⇒ **`MAX_SESSIONS ≈ 19`**, where charging the raw 3.82 M — ≈19× what Complete would publish — gave **≈ 1** (the iteration-7 launch-capacity finding). Hosting *N* maximal sessions costs `W_ref ≈ N × U_ref`: 32 concurrent ⇒ ≈6.7 M chunk-refs, not ≈122 M. The honest trade: **small parts buy many concurrent sessions, large parts buy fewer; `MAX_SESSIONS` falls out of the caps** — it is not `~10^6` and not a free knob | a cap overrun halts the maintenance plane (data-loss-class), so it is derived and enforced exactly; `503 SlowDown` is the backpressure | **FU-3** — admission/drain telemetry; a future incremental/cached staged-reference build would charge *actual* not worst-case footprint, raising `MAX_SESSIONS` at fixed `W_ref` |
| **A session may not stage more chunk-refs than it could publish** (`MAX_STAGED_CHUNKS`, decision 4.4): a part commit past the ceiling is refused `400 EntityTooLarge`, session still usable and abortable. S3 permits staging parts that Complete never names; a client that stages more than one publishable object's worth is refused here | `MAX_STAGED_CHUNKS = MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS` ⇒ **~193 GiB of staged parts at 1 MiB chunks**, with a bounded overshoot of `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` (≈6,096 refs) | the alternative is charging `U_ref` for staging capacity no Complete can ever publish, which costs a **19×** capacity factor at maximal parts (`MAX_SESSIONS ≈ 1`); the refused traffic is a client staging beyond any publishable object | **FU-3** — admission-refusal telemetry (which ceiling refused, and how close sessions run to it) |
| A client's Complete waits for the reaper's rollback after a completer crash (no concurrent two-gateway resume of a `Completing` session; a fresh client Complete meanwhile gets `409`) | **`W_completing`** (reaper rollback window, measured from `fenced_at`) | `409`-then-rollback keeps Complete simple and S3-conforming (a concurrent Complete is a conflict, never a silent second publisher) and needs no two-gateway resume protocol; the fence makes the wait safe (no partial publication) — the fix-4 decision | **FU-4** (surface the reason) / **FU-3** (alert on stuck `Completing`) |
| A session wedges at `MAX_INFLIGHT_PARTS` when crashed-part residue holds every slot (the client must Abort to recover) | `MAX_INFLIGHT_PARTS` slot indices, each released by the keyed delete in its owner's commit/compensation batch while `Open`, the survivors discarded with the session at teardown | residue must be *counted* to keep the bound enforced (F11a); a healthy client whose parts commit never hits it; the alternative (uncounted residue) is the unbounded-`sidx:` safety hole | **FU-3** — in-flight-slot / admission-refusal telemetry |
| A drain waits for staged uploads on the draining server | **`W_session`** (deployment default; per-bucket tighten-only) | the alternative is the F6 wipe trace, a safety outcome; `W_open` was the wrong (unbounded) label | **FU-2** — urgent operator-forced staged evacuation |
| Superseded-**or-deleted** large/segmented bytes retained slightly longer than today (grace starts at drain, not at the supersede/delete commit — finding 5 routes `DeleteObject`/`DeleteObjects` through the same ledger) | one reaper cadence plus the grace window | evidence must precede the lifting of protection; the direction is strictly safe, and the fan-out moving off the request path is what keeps a segmented-object delete inside `E_tx` | **FU-3** — drain-rate telemetry and obligation-age alert |
| A retried Complete after `W_tombstone` answers `404` although the object exists | `W_tombstone` | unbounded tombstones are an unbounded namespace (they are counted by the admission counter) | none required (S3-conforming) |
| A reap of a *progressing but silent* upload (idle arm) or an *over-age* upload (`W_session` arm) costs the client its staged parts | `W_open` / `W_session` (operator-chosen inside the settled ranges) | reaping is safe (the fence prevents any partial publication); the cost is re-upload | **FU-4** — surface the abandonment reason in the S3 error text |
| A foreign-clocked session is skipped by the reaper and holds its admission slot until an operator aborts it | at worst `MAX_SESSIONS` slots, each behind an alarm naming the session and its `clock_source` | reaping on a producer's stamp when producers do not share the reconciler's clock is the #557 defect class; declining to judge is the safe direction, and the operator verb is the exit that keeps the state non-absorbing — **which is why the verb ships with the guard, not after the first report** (iteration-8 finding 3): a deployed producer mismatch or a legacy record can create these sessions in bulk, and `MAX_SESSIONS` of them is a permanent `503 SlowDown` with no in-system exit at all | **FU-6** — the operator-driven session abort, and the alarm that summons it, **both with #625** |
| An upload open at metadata-restore time is aborted (no resumption across a restore) | one restore event | KISS (**D-B**): a records-only image cannot prove the staged bytes still exist | none required (documented operator behaviour) |
| **Single-PUT chunk size scales with object size** (segmentation is multipart-only, finding 4): a large single `PutObject` uses `chunk_size_effective = max(DEFAULT_CHUNK_SIZE, ⌈Content-Length/MAX_MAP_CHUNKS⌉)` to stay flat; a `PutObject` that cannot fit `MAX_MAP_CHUNKS` at `chunk_size_max` is refused | S3's 5 GiB single-PUT max fits once `chunk_size ≥ 5 GiB/MAX_MAP_CHUNKS ≈ **13.4–31 MiB**`; the gateway-memory cost is `chunk_size × max_concurrent_encodes` (bounded, and only for large single PUTs) | segmentation needs a session/epoch a single PUT has not got; chunk-size selection is the simple, sessionless way to reach S3's single-PUT ceiling, and refusal past it is a safe guard, never a silent unanchored segmentation (X50) | **FU-5**/operator `chunk_size` tuning (same trade as the max-part-size row) |
| **A lost segment repoint OR staged re-place leaves a pre-marked destination fragment for GC** (adversary C, X47; the staged path per iteration-6 T4 finding 1, X29): when a reconstruction/rebalance repoint of a **committed** segment loses its CAS to a concurrent supersede/delete, **or** a reconstruction re-place of a **staged** chunk loses its CAS to a session fence, the destination fragment it pre-wrote stays `orphan:`-pre-marked and is reclaimed by GC | one orphan grace window (`G_orphan`), for **one** fragment per lost repoint/re-place; the number of lost repoints/re-places is bounded by the reconstruction/rebalance work rate on segmented and staged objects | pre-evidencing the destination is what makes both moves safe on **both** CAS branches (X47, X29); the alternative (write-then-hope) strands the destination on a loss (outcome (a)) — reclaiming it via the pre-mark is the named, bounded reclamation | **FU-3** — orphan-rate telemetry (the pre-mark reclamations are ordinary `orphan:` traffic) |

### Follow-ups this proposal requires (filed on acceptance)

FU-1 (chunk-map segmentation) is **dissolved into decision 7** — it is in scope, not a
follow-up; what remains as FU-1 is the **record-shape ADR graduation** it triggers. Each accepted
cost above names one of these; filing them is part of accepting this proposal.

| Tag | Issue to file | Owner / milestone | Trigger |
|---|---|---|---|
| **FU-1** | *Segmented chunk-map record-shape ADR* — graduate decision 7's flat-or-segmented `InodeRecord.chunk_map` and the `seg:` namespace to an ADR (successor to [ADR-0047][a47]'s record shape), since it changes the object model for every `.chunk_map` consumer | metadata / object model | with #508 (the shape lands there); the ADR records the settled decision |
| **FU-2** | *Urgent operator-forced staged evacuation* — a management verb that fences sessions holding fragments on a decommissioning server so a drain need not wait out `W_session` | management surface (proposal 0008) | an operator hits the drain wait in practice |
| **FU-3** | *Retirement-drain and admission telemetry/alerting* — open sessions, obligation count, oldest obligation age, drain rate, admission refusals, **reference-build size** (staged `W_ref` and committed `W_ref_committed`, X48), with alert thresholds; and a future **incremental/cached staged-reference build** so admission can charge *actual* not worst-case footprint (raising `MAX_SESSIONS` at fixed `W_ref`) | observability floor (proposal 0010) | with #625 |
| **FU-4** | *Surface the abandonment reason in the multipart error text* — so a client reaped after `W_open`/`W_session` learns why | S3 gateway (#508 follow-up) | after the first operator report |
| **FU-6** | *Operator-driven session abort **and terminal expiry*** — a management verb that fences any `Open`/`Completing` session to `Aborting` on the operator's authority, **plus a second verb that authorizes expiry of a `Completed` foreign-clocked session whose tombstone window cannot be judged** (abort does not apply to a terminal session, iteration-9 finding 11), plus the alarm that names a skipped foreign-clocked session; the exit for a session the reaper declines to judge (X26) | management surface (proposal 0008) | **with #625 — the verb *and* the alarm, not the verb on first report** (iteration-8 finding 3): the reaper's clock guard makes this verb the **only** exit for a foreign-clocked session, so shipping the guard without the verb makes that state absorbing (implementation order, point 3) |
| **FU-5** | *Part-record segmentation* — split a `part:` record's chunk list across segment records (reusing decision 7's machinery, one record class down) so a deployment can accept S3's 5 GiB part maximum at the default chunk size, instead of raising `chunk_size` | metadata / #508 follow-up | a deployment needs 5 GiB parts at small chunks |

### Tests and telemetry the implementing slices owe

- **Tier-0 DST ([ADR-0009][a9]) is the correctness authority** for every interleaving named in
  the failure-mode tables: the Complete/reap race in both orders, publication CAS loss, crash at
  each step of the publication, segment-write, and drain sequences, restore-fence (X17) **and the
  `Completing`-with-segments restore fence (X57)**, the `W_session` residency reap (X18), the
  stale-snapshot mid-pass creation (X19), **continuous part churn against one designated commit
  (X52), the slot-reserve race at the cap (X41/X55), the pre-first-chunk liveness window (decision
  6's finding-2 row), and two concurrent drainers over one owned `sidx:` range (X56)**, **the
  drain-request-versus-intent race (X59) and the commit-between-the-reaper's-two-progress-reads
  race (X60)**, **the adoption-CAS-against-a-GC-deleted-destination window (X61, the
  delete-before-cleanup interleaving of `gc.rs:189-207`), the stale-orphan-mark-at-a-new-unreference
  delete/read regression (X63), and the split-`MAX_SESSIONS` rolling-change admission case (X64)**,
  **the renewal-commits-after-the-reaper's-expired-read fence race (X62), the dirent-moved-under-a
  -flip overwrite (X66), the inode-before-`part:` publication handoff (X67), the ambiguous slot
  reserve with `may_still_commit` (X68), and the foreign-clock terminal-teardown case (X71)**,
  and arrival-outruns-drain. Each lands as a seeded regression. The `scan_page` ordering and
  continuation rules (X70) land in `metadata-conformance` on every backend rather than as DST.
- **The classification sweep** — one test helper this protocol earns: given a store and a fleet,
  assert every on-disk fragment is in exactly one class (committed-referenced,
  staged-with-a-session, evidenced-for-reclamation). It is the mechanical form of invariant (2)
  and the single strongest regression net for this protocol; run it after every DST scenario.
- **Conformance across backends** (`metadata-conformance`) for the new record round-trips
  (including `slot:` and `psum:`), the segmented map, and the `PendingEntry` decode→encode identity
  on **both** a legacy value (both optional fields absent) and an owned `sidx:` value (`owner` +
  `staged` placement present).
- **Telemetry** ([ADR-0011][a11]): open sessions (vs. `MAX_SESSIONS`), staged bytes/records,
  oldest session age, obligation count and oldest obligation age, reaper pass outcome counts,
  admission refusals **by ceiling** (session cap, in-flight slots, `MAX_STAGED_CHUNKS`), and the
  **reference-build size** (staged `W_ref` and committed
  `W_ref_committed`, the in-memory `(server, fragment)` count each reconcile pass builds) with an
  alarm as it approaches the reconcile host's RAM ceiling (X48 — committed segmented data is not
  admission-bounded, so it is *monitored*). The three that matter operationally are *oldest
  obligation age* (the drain is falling behind, **D-D**), *admission refusals* (backpressure
  engaged), and *open sessions* (capacity headroom); *reference-build size* is the fourth once
  segmented objects are common.

### Definition of done

- **#625 (reaper)** — decision 6 implemented as a custodian loop with its knob values inside the
  settled ranges, decision 5's per-session reference-based reclamation, the serialized admission
  counter, the cursor-keyed retirement drain, the `W_session` arm, and the DST rows above. Lands
  **with or before** #508.
- **#508 (multipart)** — the S3 verbs over decisions 1–4 and 7, serialized admission control, the
  verb × state table, and the wire surface that was already settled.

### ADR graduation recommendation

Three of these decisions outlive multipart and should graduate to ADRs after this proposal is
accepted (authoring them is follow-up work under [ADR-0037][a37], not part of this document):

1. **The segmented chunk-map record shape** (decision 7) — it changes `InodeRecord` for every
   consumer of `.chunk_map`, which is precisely the class of decision an ADR records; the
   strongest candidate here, successor to [ADR-0047][a47]'s shape (FU-1).
2. **The retirement ledger — durable obligations installed atomically, drained in bounded,
   idempotent batches** (decision 4). It applies to every superseding publication, to
   server-side copy (#504 step 2), and to any future resumable write, and it changes an existing
   committed contract (`commit_chunk_map_superseding`).
3. **The staging protection class and per-consumer visibility rule** (decision 2). It changes
   what the maintenance plane's reference set *means* — a contract every custodian pass and every
   future maintenance pass inherits.

Decisions 1, 3, 5 and 6 are multipart-shaped enough to live in this proposal; if server-side copy
adopts decision 1's fence verbatim, that is the moment to reconsider.

## Backward compatibility

- **On-disk / record format.** Additive prefixes (`mpuctl:`, `mpu:`, `part:`, `sidx:`, `seg:`,
  `retire:`), each **disjoint from every existing prefix and from each other** — in particular the
  owned staging entry is `sidx:<id>:<part>:<chunk>`, **not** under `pending:`, so the existing
  global `pending:` scans (restore's `pending_chunks`, the expiry sweep) keep exactly their present
  bound and never enumerate an owned entry (finding 3). **Two** optional fields on `PendingEntry`
  (`owner` and `staged`, the chunk's EC placement — finding 4), each `#[serde(default)]` **and**
  `skip_serializing_if = "Option::is_none"`, so a legacy `pending:` entry (both `None`) decodes
  unchanged and re-encodes byte-identically, **and** an owned `sidx:` entry (both `Some`) re-encodes
  byte-identically across its own lease renewals — the CAS-identity rule [ADR-0047][a47] established
  for `InodeRecord`, extended to cover `staged` (both round-trip tests owed). The one non-additive change is
  `InodeRecord.chunk_map` becoming a `Flat | Segmented` variant (decision 7): a legacy inode is
  `Flat` and MUST decode and re-encode byte-identically (the same serialization-identity rule; the
  round-trip test is owed), so existing objects are unaffected and only newly published large
  objects are `Segmented`. This is the change FU-1 graduates to an ADR.
- **Version skew during rollout — a hard ordering requirement.** A custodian build **without**
  decision 2's staged awareness, run against a store where a newer gateway is staging parts,
  would mark live staged fragments `orphan:` via the restore pass (`restore.rs:266-269`) and GC
  would reclaim them after grace (owned entries no longer sit under `pending:`, so the old
  `pending_chunks` skip does not cover them — the staged set is what protects them, which the old
  build lacks). GC under `Defer` is still safe (its conservative arm, `gc.rs:183-187`), as is
  scrub. A custodian without decision 7's `Segmented` awareness cannot resolve a segmented map at
  all, and one without decision 4's retirement drain cannot bound a segmented-object delete's
  fan-out (finding 5). Therefore: **the custodian plane, including the restore tool, MUST be
  upgraded before any gateway is allowed to create multipart sessions or publish a segmented
  object.** The implementing slices should make this checkable rather than documentary (a refusal or
  an alarm when the staging/segment namespaces are non-empty and staged/segment awareness is
  absent).
- **Metadata restore.** The restore tool MUST fence every restored `Open`/`Completing` session to
  `Aborting` (**D-B**) — an operational change: after a restore, in-flight uploads are aborted,
  not resumed. An `Open` session takes the ordinary abort fence; a **`Completing`** session takes
  the **restore-fence transition** (§2, §3, decision 2's restore row), whose single batch installs
  `retire:bytes:{session, parts}` **and** `retire:records:{seg:<id>:<E>}` so that any segments that
  attempt had already written are reclaimed — the one `Completing → Aborting` edge in the state
  machine, and the reason it exists (iteration-7 adversary: a `Completing` session snapshotted
  after its segment-write phase but before its root flip has `seg:` records whose only other
  deleters — a rollback obligation, or the supersede of a committed inode — never fire on this
  path). This is the F13 remedy and is part of the custodian upgrade above. **The restore
  fence generation MUST complete before any gateway serves multipart verbs on the restored image**
  (X17b): the deployment either sequences the restore-fence pass ahead of re-enabling the gateways,
  or the gateways refuse multipart until they observe the restore-fence generation complete — so no
  retried Complete can publish over reclaimed bytes in the pre-fence window.
- **Deployments.** No new external dependency, no new service, no new port. The reaper is another
  pass in the existing custodian loop, dispatched from the same fenced control point
  (`reconciliation.rs:75-112`). `W_session` is a new deployment default (per-bucket tighten-only).
- **Public API.** Additive: the multipart verbs (#508). Existing PUT/GET/DELETE semantics are
  unchanged, with three deliberate internal changes — a superseding publication installs a
  retirement obligation instead of expanding orphan records inline (decision 4); a `DeleteObject`
  or bulk `DeleteObjects` of a large/segmented object does the same instead of orphaning every
  fragment inline (decision 4, finding 5); and a large single PUT is published as a **flat** map
  with an object-sized chunk size (segmentation is **multipart-only**; a single PUT that cannot fit
  `MAX_MAP_CHUNKS` even at `chunk_size_max` is refused with `EntityTooLarge`, never segmented
  against a session it has not got — decision 7, finding 4). Reclamation of a superseded or deleted
  generation is asynchronous, and the reader-safe grace guarantee is unchanged (reclamation is
  never *earlier* than today, and a GET during a DELETE is untorn exactly as before). A GET of a
  **segmented** object is reader-safe by the **resolve-retry rule** (decision 7h): a reader that
  finds a `seg:` record absent mid-resolution re-reads the root — a changed or absent root means
  the generation was concurrently superseded/deleted and the reader restarts against the current
  root (or returns `NoSuchKey`), so a segmented GET is never torn by a concurrent retirement
  deleting `seg:` records (adversary B).
- **Rollback.** Disabling the multipart verbs after sessions exist leaves records the reaper still
  drains; the reaper must therefore keep running (and keep its rows compiled in) even when the
  verbs are disabled. Removing the reaper while sessions exist is the one unsupported downgrade —
  it re-creates F1, F7 and F14 by construction. A store that has published a segmented object
  cannot be served by a build without `Segmented` support.

## Open questions

Each of these is explicitly **non-gating for #508** and carries its reason and its owner. None of
the seven decisions and none of F1–F18 is parked here.

**Closed on 2026-07-24 — the part-boundary serialization question (was ⚑ NEEDS-HUMAN, finding 6a).**
Iterations 3 and 6 flagged a *cost* question for sign-off: the enforced in-flight cap made
`sinf:<upload-id>` a per-session key CAS'd at **every part start and end**, serializing same-session
part boundaries, and the human was asked to bless that cost or ask for a cheaper enforcement. The
question is **resolved by construction rather than blessed**: the cap is now the `slot:<id>:`
**key space** (decision 5), where a start claims one index under `require_absent` and an end deletes
**its own** key, so the part path holds no shared writable key at all and there is nothing left to
serialize. What remains is the per-chunk `require(mpu == Open@E)` and `require_absent(desired:dserver:<S>)`
**read** preconditions on each
`sidx:` intent — reads do not serialize concurrent writers — and the cap stays *enforced*, not
observed: no concurrency can produce a `MAX_INFLIGHT_PARTS + 1`-th key. This also removed the
starvation hole the counter carried (iteration-7 finding 1) and the double-release branch its
classification needed (finding 4). Recorded here rather than deleted, because the rejected shape is
the load-bearing part of the rationale.
- **Multipart ETag composition.** [ADR-0047][a47] closed the ETag *basis* (lowercase-hex SHA-256
  as an opaque change-token; MD5 rejected) and deferred only the multipart *composition* to the
  implementing slice. Non-gating for the commit protocol because the protocol needs exactly one
  property from it — the published ETag must be a **pure function of the part records' recorded
  digests and their order**, so a re-derived value on a retried Complete is identical, and the
  value is in any case recorded in the `Completed` session record. Owner: **#508**.
- **Knob values.** Every correctness-relevant knob has its range and bounding invariant settled
  above; choosing the values inside those ranges, and wiring the metrics, alerts and CLI flags, is
  non-gating because no value inside a settled range can break an invariant. Owners: **#625**
  (`B`, `W_open`, `W_completing`, `W_session`, `W_tombstone`, `W_ref` (reconcile RAM budget),
  cadence), **#508** (`MAX_MAP_CHUNKS`, `MAX_SEG_CHUNKS`, `MAX_PART_CHUNKS`, `MAX_ROOT_SEGMENTS`,
  `MAX_STAGED_CHUNKS`, `MAX_INFLIGHT_PARTS`, `chunk_size`, `R_publish`,
  `MAX_COMPLETE_ATTEMPTS`). **`MAX_SESSIONS` is not on either list — it is
  `⌊W_ref / U_ref⌋`, *derived* from `W_ref` and the caps, never independently chosen** (decision 6,
  the iteration-4 mechanical-enforcement fix).
- **Whether the single-PUT flat-map committer shares code with the multipart flat-map path.**
  Decision 7 **settles the design** (it is not parked): segmentation is **multipart-only**, and a
  single `PutObject` publishes a **flat** map sized by chunk-size selection
  (`chunk_size_effective = max(DEFAULT_CHUNK_SIZE, ⌈Content-Length / MAX_MAP_CHUNKS⌉)`), or is
  refused with `400 EntityTooLarge` past `chunk_size_max` — it never segments, because staged
  publication needs a session/epoch it has not got (finding 4). What remains is only the code
  factoring (whether one flat-map committer serves both callers), an implementation choice that
  gates nothing. Owner: **#508**.
- **Whether `ExpiredPendingPolicy::Defer` can be retired once every producer stamps live leases**
  (#490 / #557). Untouched by this proposal — staged reclamation is clock-free precisely so it does
  not depend on that question being answered. Owner: **#557**.
- **Per-bucket lifecycle expiry of incomplete uploads** (S3's `AbortIncompleteMultipartUpload`).
  Non-gating: the deployment-default `W_session` already bounds the namespace; a per-bucket policy
  is a tighten-only refinement that belongs with lifecycle configuration. Owner: proposal 0006's
  adoption slice.

[arch6]: ../../architecture/06-runtime-view.md
[arch8]: ../../architecture/08-crosscutting-concepts.md
[a7]: ../../adr/0007-reserve-append-cas-watch.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a19]: ../../adr/0019-chunk-format-layout.md
[a37]: ../../adr/0037-proposal-and-spec-process.md
[a45]: ../../adr/0045-metadata-validation-boundaries.md
[a46]: ../../adr/0046-bucket-model-real-namespace.md
[a47]: ../../adr/0047-object-metadata-model.md
[i504]: https://github.com/getwyrd/wyrd/issues/504
[i508]: https://github.com/getwyrd/wyrd/issues/508
[i509]: https://github.com/getwyrd/wyrd/issues/509
[i625]: https://github.com/getwyrd/wyrd/issues/625
