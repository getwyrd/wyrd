---
created: 01.07.2026
type: proposal
status: draft
author: Eduard Ralph
tracking-issue: "#368"
tags:
  - proposal
  - milestone-7
  - implementation-plan
  - failover
  - disaster-recovery
  - operations
---
# Proposal: Milestone 7 — failover and disaster recovery, single-datacenter (implementation plan)

> The implementation plan for the seventh step of the [implementation arc][p2]
> (proposal 0013, which supersedes proposal 0002). M5 secured the wire
> ([0011][p11]) and M6 sealed the bytes at rest ([0012][p12]); **M7 proves the
> resulting system survives** — node, disk, and rack loss *within one
> datacenter*, **drilled rather than asserted** ([p2][p2] M7). It is the last
> hardening milestone before the **★ release point at M8**: M8 explicitly *needs*
> M7, because "failure recovery is an operator workflow" ([p2][p2] M8), and a
> workflow must exist and be exercised before a management surface can wrap it.
> M7 extends the custodian reconstruction plane M3 built ([0005][p5]) and runs it
> against the secured, encrypted store M5/M6 produced — the arc's stated *needs*
> edge. Unlike its Step-2 siblings, M7 is **verification-led**: it ships less new
> plane than any of them and instead converts standing architectural decisions —
> the [§6.5][s6] restore ordering, the [§7.3][s7] failure-domain model, the
> [§8.2][s8] out-of-band backup rule, the [blueprint][bp]'s per-tier backup model
> — into **exercised fact**, plus the one production mechanism those decisions
> presuppose but nothing yet implements: holistic failure *detection*. **No new
> spec, and no ratification of the milestone's technical contracts, is
> required** — the trust, key, and consistency ADRs stand, and M7 is where they
> stop being paper; the one *process* decision M7 introduces, the runbooks
> document class, lands as a lightweight ADR with M7.5 (the [design README][rd]
> ADR-first habit for a settled process choice). The *cross-zone*, zone-loss DR
> drill —
> home-zone failover via the version high-water mark ([ADR-0015][a15]) — is
> **explicitly M11**, the Step-3 ★, not this milestone ([p2][p2]).

## Motivation

M7 retires the risk that **single-zone durability holds in steady state but not
through local failure and recovery** ([p2][p2] M7). The arc's ordering principle
is **risk retired, not features delivered**, and M7's risk decomposes into three
falsifiable propositions — each one a thing the M0–M6 system, audited below,
demonstrably does *not* yet establish:

- **Detection is production-shaped, or repair never starts.** M3's repair loop is
  the best-proven machinery in the system (the audit below lists the suites), but
  every production *trigger* for it is **per-fragment**: a read-time checksum
  failure, a scrub-time checksum failure, and — since issue #330 closed the
  missing-fragment gap — a scrub-time *absence* of a placed fragment (scrub now
  walks the committed reference set per placed server; absence is the same
  durable obligation as rot). What no production path does is detect a **dead
  D server** *holistically*: the L5 lease signal exists by construction —
  `Coordination::register` is leased "so a crashed member's registration
  lapses," and `discover` returns "the current (unexpired) members"
  (`crates/traits/src/lib.rs`) — but **nothing consumes it**. A killed server
  today is discovered fragment-by-fragment at scrub cadence. And the half of
  the durability equation detection owns is **unmeasured**: §6.3 frames
  durability as "the probability that more than *m* fragments fail within one
  **repair** window" ([§6.3][s6]) — but repair cannot begin before loss is
  *detected*, so the true at-risk interval is **detect + repair**, and M7 makes
  that first half explicit (an extension of §6.3's framing, not a quote of it).
  M3 already emits four of ADR-0011 §1's five durability metrics
  (under-replicated count, repair-queue depth, time-to-repair, scrub
  coverage/corruption; replication-lag-per-zone-pair is single-zone-moot) — but
  **time-to-detect is not among them** ([ADR-0011][a11]), because no loop yet
  owns detection to time it.
- **The system of systems recovers, or only the data plane does.** Every fault
  M3 injected was a *data-plane* fault. The production single zone that M5/M6
  completed is five stateful or control tiers — the D fleet, TiKV+PD, the L5
  etcd ensemble, the step-ca trust plane, the OpenBao key plane — with
  per-tier loss semantics the architecture already fixes: coordination loss
  "loses no data … what is lost is the ability to *react*" (`Coordination`
  doc-comment); an unreachable CA under **fail-closed mTLS** "halts *every new
  dial and certificate rotation* in the zone" ([§7.3][s7], the fail-closed
  consequence of [ADR-0025][a25]); a KMS outage makes
  encrypted data "temporarily *unreadable*, never *lost*" ([0012][p12];
  [ADR-0026][a26]); metadata loss "orphans all chunks" ([§8.2][s8]). Control-,
  trust-, and key-plane loss is a **failure class M3 never touched**, and the
  restore ordering that stitches the tiers back together — trust plane first of
  all, then L5, then L4 from out-of-band backup, then re-protect the bytes
  ([§6.5][s6]) — is decided, documented, and **never once executed**.
- **The runbook is drilled, or it is a hypothesis.** The blueprint is blunt: the
  single-zone DR sequence "is a **runbook that must be written and drilled
  before it is needed**, not improvised during an incident — and that runbook,
  plus backup cadence/format/retention, is still an open item in proposal 0008,
  not yet specified" ([bp][bp]). The backup model itself is asymmetric by design
  ([bp][bp]: "replication is not backup"; Raft and EC "faithfully replicate
  logical disasters" like a bad migration or errant delete, [§8.2][s8]) and
  **fallible in exactly the place that matters**: TiKV restore has real-world
  inconsistency history (TiKV issue #13281, cited by the blueprint), and
  whether log-backup PITR works against Wyrd's standalone `txnkv` (no TiDB) is
  an **open verification item the blueprint records** ([bp][bp]) — so today the
  mandatory metadata backup is configured on faith. The arc's definition-of-done verb is
  *drilled*; nothing weaker retires this risk.

A second, quieter motivation: M7 is the **dress rehearsal for the arc's final
gate**. M11 — the Step-3 ★ — is this milestone re-instantiated at zone scale
("the DR ordering … written as a runbook and exercised in a drill", [p2][p2]
M11). The drill automation, the runbook shape, the drill-record discipline, and
the detection telemetry M7 builds are precisely the machinery M11 inherits; and
because M11 is "the final and least-forgiving risk", paying for that machinery
now, at single-DC scale where a failed drill costs a re-run rather than a
reputation, is the cheap time to pay ([p2][p2]).

## Design

### Scope boundary

**In scope** — exactly what retires the survives-local-failure risk:

- **The liveness reconciler** — the one new production mechanism: a fifth
  custodian loop, dispatched from the same fenced `reconcile_step` control point
  as GC / scrub / reconstruction / rebalance
  (`crates/custodian/src/reconciliation.rs`), that consumes the **existing** L5
  membership view (`Coordination::discover`; registrations lapse with their
  lease), diffs it against the D-server ids named by committed placement
  records, and — after a **suspicion window**, and never for a server under a
  `desired:dserver:` drain/decommission record (an evacuation is not a failure,
  `crates/custodian/src/desired_state.rs`) — enqueues repair obligations for
  **every fragment placed on the departed server**, on the same shared, durable
  repair queue scrub and the read path already feed (`wyrd_core::repair`).
- **Time-to-detect telemetry** — a first-class durability-plane metric joining
  ADR-0011's five on the same `tracing`→OTel seam
  (`crates/custodian/src/telemetry.rs`), so the *detect* half of the durability
  window is measured like the *repair* half already is ([ADR-0011][a11],
  [ADR-0012][a12]).
- **The failure-detection matrix** — the documented table of *failure class ×
  detecting signal × detection bound × responding loop*, covering: a dead
  sector (`EIO` read-around, exists), bit rot (scrub checksum, exists), a
  missing placed fragment (scrub reference-set walk, #330), a dead disk — which
  **is** a dead D server under the one-D-server-per-disk model
  ([ADR-0034][a34]) — a dead host (several D servers, correlated), a dead rack
  (all D servers sharing one failure-domain label), custodian-leader loss
  (re-election behind the fence, `leadership.rs`), and the quorum / trust / key
  tiers below. The matrix is the runbook's first section.
- **Rack-scale and beyond-tolerance fault legs** — extending the M3 campaign
  entry points (`cargo xtask disk-faults | jepsen | kill-reconstruct`) from
  their single-victim shape to **whole-failure-domain kills** (every container
  sharing one `fd` label), asserting the [§7.3][s7] tolerance boundary from
  *both* sides: within tolerance → reads keep serving and the rebuild lands in
  surviving distinct domains; **beyond tolerance → the loss is surfaced, never
  silent** (the reconstruction loop's `Unrepairable` assessment leaves the
  obligation queued for re-assessment, `reconstruction.rs:145`). One honest gap
  M7.2 must close first: today `emit_under_replicated` counts only *repairable*
  plans (`reconstruction.rs:166`), so a beyond-tolerance chunk sits in the
  repair-queue-depth metric but is **absent from the under-replicated count** —
  the honest-refusal half is not yet surfaced there. M7.2 either extends the
  emission to count unrepairable obligations or asserts the drill against queue
  depth; either way the loss is made visible — the half no current tier
  exercises.
- **Quorum-loss recovery, per tier, per the blueprint's backup model** ([bp][bp];
  [§8.2][s8]): **etcd is rebuilt, not restored** (a stale snapshot rolls the
  mvcc revision backward and "can regress lock fencing tokens, re-admitting a
  stale lock holder — a correctness hazard worse than the soft state you'd
  recover"); **TiKV is restored from the mandatory out-of-band backup** (online
  MVCC snapshots to independent storage), with PD riding the cluster backup;
  **fragments are never backed up** — EC + custodian reconstruction *is* their
  mechanism; **gateway/custodian are config-only** (stateless). Each recovery is
  executed, not configured: the custodian **verify pass** after a metadata
  restore reconciles the restored map against surviving fragments
  (post-snapshot fragments become collectable orphans; referenced-but-lost
  chunks rebuild from ≥ *k* survivors), and the measured RPO/RTO is recorded.
- **Trust- and key-plane outage drills** — the CA and KMS killed and recovered
  as tiers, with the *what-breaks-when* timeline measured (below): fail-closed
  must be **bounded, observable, and recoverable**, never a mystery outage.
- **The runbook artifact** — `docs/design/runbooks/` (a new document class,
  justified below): the detection matrix, per-failure-class procedures, the
  single-DC restore ordering instantiated from [§6.5][s6], backup verification,
  and the drill protocol with a drill-record template.
- **The graduation drill** — the runbook executed end to end on the
  **first-deployment substrate** (issue #367's gate: the deploy tree, the etcd
  `Coordination` backend, the observability floor, the S3 wire floor), with a
  dated drill record committed and every surprise promoted to a seeded DST
  regression ([ADR-0009][a9]; the blueprint's step-7 discipline).

**Out of scope** — deferred to the milestone or artifact that owns it:

- **Cross-zone / zone-loss DR** — **M11**, the Step-3 ★ ([p2][p2]). There is one
  zone at M7; "rack" is the largest failure domain this milestone drills, and
  home-zone failover via the version high-water mark (ADR-0015) has no second
  zone to fail over to. M7's runbook and drill machinery are written to be
  re-instantiated there, not to anticipate its content.
- **The backup/restore *operator workflow*** — **M8 / [0008][p8]**. 0008 makes
  backup/restore, drain/decommission, and upgrades first-class, safe, resumable
  management-plane operations behind ADR-0013's API. M7 hands M8 a *drilled
  procedure*; M8 wraps it in the operator surface. The boundary is
  mechanism-and-proof (M7) vs workflow-and-API (M8). **0008, authored before
  this proposal, today lists the DR runbook, the drill, and single-zone
  backup/restore among its *own* graduation criteria and open items**;
  reconciling that — narrowing 0008's criterion to "wrap the M7-drilled
  procedure and backup config in the management API", so the runbook and the
  backup cadence/format/retention are authored once, here — is a task of M7.5
  and of 0008's slicing (#369), not a second independent plan for the same
  artifacts.
- **The Tier-1 `deploy/` performance bundle** — **[0009][p9]**. Kernel pins,
  udev/sysctl tuning, and the OS image are the performance program's; the drill
  runs on whatever bundle the substrate carries and neither depends on nor
  alters it.
- **The observability floor itself** — **[0010][p10] (#366)**. 0010 verified
  that the durability telemetry seam is library-only today and the `server`
  binary runs no custodian loop; wiring telemetry and the custodian role into
  the deployable binary is 0010's job and a **named dependency** of the drill
  (a drill against an unobservable system proves nothing an operator can use).
- **Key-compromise emergency response** — **[ADR-0029][a29]**. A leaked KEK is a
  *security incident* (revocation + forced re-encryption), not an availability
  failure; its runbook is a sibling artifact of the same new class, reserved,
  not written here.
- **Alerting policy and dashboards** — the floor's metrics are consumed raw at
  M7 (Prometheus/OTLP); alert thresholds and the polished operator view are
  M8 / 0008 territory (one threshold question is flagged in Open questions).

### What carries over from M0–M6 — and what the existing suites already prove

M7 builds **on** the M3 verification estate, not over it. The audit, against the
working tree, of what already exists and must not be re-claimed:

- **Tier-0 DST proves the repair loop deterministically**
  (`crates/dst/tests/custodian.rs`): seeded storage faults model a killed D
  server / disk loss and bit rot; two crash injectors — a metadata store that
  drops the version-conditional repoint commit, and a D server that fails
  `put_fragment` — **bracket the whole "fragment writes → commit" window**,
  proving a crashed repair leaves collectable garbage, never a torn chunk;
  assertions pin full redundancy restored across **distinct failure domains**
  and the exact durability-plane metric values emitted.
- **Tier-1 disk faults are real** (`cargo xtask disk-faults`, `WYRD_TIER1=1`):
  device-mapper `dm-error` block-layer faults drive the **production**
  `FsChunkStore` / `reconcile_step` / scrub / reconstruction path
  (`crates/custodian/tests/tier1_disk_faults.rs`) — real `EIO`, not modelled.
- **Tier-1 consistency-over-repair is real** (`cargo xtask jepsen`,
  [ADR-0039][a39]): a ten-container RS(6,3) cluster (nine for placement, one
  spare), a `docker kill` crash **and** a `docker pause`/`unpause` partition
  injected mid-repair, asserting the ADR-0015 contract over the repair path —
  commit-point-atomic repair, read-after-commit, exactly-once convergence, no
  stale/torn reads (`tier1_jepsen_consistency`).
- **Tier-2 kill-and-reconstruct is real** (`cargo xtask kill-reconstruct`,
  `WYRD_TIER2=1`): one victim killed in a live containerized cluster, the
  production `custodian::reconcile_step` → `reconstruction::reconcile` path
  driven against it, rebuilt fragments asserted onto healthy servers in
  distinct failure domains (`tier2_kill_reconstruct`).
- **The repair machinery itself** carries over verbatim: assessment verifies
  every survivor before decode and reads around permanent faults
  (`IntegrityFault` / `EIO`); priority is slack-ordered (`repair_priority =
  survivors − k`, nearest-the-floor first); placement excludes survivors'
  domains (`select_distinct_domains_excluding`); the repoint is **one
  version-conditional commit** that also drains the obligation and orphans
  displaced fragments; conflicts and aborts are offset on their own counters
  ([ADR-0011][a11] §2). The custodian rebuilds **ciphertext** and never needs
  tenant keys ([ADR-0021][a21]; `reconstruction.rs` module doc) — the fact M7's
  restore ordering leans on below.
- **The fence** carries over: one active custodian per zone via
  `Coordination::elect_leader`, a monotonic fencing token, a deposed leader's
  step rejected (`leadership.rs`), with the version-conditional commit as the
  decisive **second fence** on every location mutation (ADR-0015).

What M7 adds around this estate is therefore narrow and nameable: **a detection
loop the estate lacks, fault legs at scales the estate never injects (rack,
quorum, trust, keys), the recovery procedures the estate presupposes, and the
drill that runs all of it on real independent hardware.**

### Detection: the liveness reconciler and the time-to-detect bound

The reconciler's shape follows the four existing loops exactly — a context
struct of injected seams, one `reconcile` pass, dispatched only from the fenced
`reconcile_step` (the anti-parallel-entry discipline the scaffold enforces):

- **Inputs:** the authoritative metadata store (placement records + the repair
  queue), the L5 membership view (the decoded `DServerRegistration { id,
  endpoint, failure_domain }` records `discover` returns — the registration
  record M3.1 already defined, `crates/server/src/dserver.rs`), the desired-state
  ledger, and the clock seam ([ADR-0024][a24]): the suspicion window is
  deterministic under DST **and sits inside ADR-0024's single shared skew
  budget** — an implausible custodian clock (a forward jump that would make live
  registrations look lapsed) **fails the pass closed** (no mass enqueue) rather
  than fabricating loss, as ADR-0024 requires of every time-dependent check.
- **The pass:** compute the set of D-server ids referenced by committed
  placement records but **absent from the unexpired membership**; for a server
  absent longer than the suspicion window, enqueue a repair obligation for every
  fragment the placement records put on it — with its own source label on the
  shared queue (the ledger records the trigger, as scrub's `"scrub"` label
  does), so the audit trail distinguishes liveness-detected loss from
  scrub-detected loss. A recorded drain/decommission lifecycle changes the
  *audit disposition*, **not** whether the loss is repaired. A draining server
  is excluded only once its evacuation is **satisfied** (`reconciliation_status`
  = *satisfied*: no committed placement record still points at it — at which
  point the pass has nothing to enqueue for it anyway, `desired_state.rs`). A
  draining server whose evacuation is **unsatisfied** — fragments still placed
  on it — that goes absent is a genuine loss the rest of the plane cannot
  recover: rebalance clean-copies only an *intact fragment from the live source*
  and a departed source is off-fleet (`rebalance.rs`), and scrub walks only the
  present fleet (`scrub.rs`), so a fully-departed server's fragments fall to no
  other loop. The reconciler therefore enqueues reconstruction for its
  still-referenced fragments exactly as for any other departed server; excluding
  by lifecycle *alone* would suppress the one holistic detector precisely when
  nothing else can repair.
- **Why a suspicion window:** a lease lapse can be a blip — an etcd restart, a
  GC pause, a network flap — and the false-positive cost is a **whole server's
  re-placement** (mass repair traffic that then contends with foreground reads,
  [§6.3][s6]). The window is deliberate hysteresis; its floor is structural
  (a lapse is only *observable* after the lease TTL plus a reconcile cadence),
  its value is a measurement (Open questions). A flapping server must not
  oscillate the queue.
- **Why the custodian, not a new service:** the custodian is already the
  durability-plane authority — single-active, fenced, leader-elected — and
  detection that *enqueues repair* is a durability decision. A separate failure
  detector would be a second writer to the repair queue with its own election
  and fence; the reconciler is one more loop on the existing control point
  ([0005][p5]; the reconciliation scaffold).
- **Time-to-detect:** emitted on the durability plane per detected loss — the
  interval from last-known-alive (lease expiry) to obligation-enqueued —
  alongside the existing time-to-repair, so the operator sees both halves of
  the durability window and the drill can assert a stated detection bound.
  Emission follows the assessment-frame discipline reconstruction established
  (metrics emitted where the assessment is authoritative).

Poll-based `discover` on the reconcile cadence is the honest first shape; the
trait itself records push watches as a later refinement of the seam, and the
detection bound the matrix states accounts for the poll.

### Recovery per tier: the single-DC restore ordering, executed

[§6.5][s6] fixes the dependency order after a real disaster; the blueprint's
per-tier backup model fixes what each step restores *from*. M7 instantiates
both for the single-DC case and — the point of the milestone — **executes**
each leg. The single-DC ordering the runbook writes:

0. **Trust plane (step-ca) first of all** — every step below dials over
   fail-closed mTLS and "cannot even complete a handshake without it"
   ([§6.5][s6]; [ADR-0025][a25], [ADR-0036][a36]). Recovery restores the CA's
   HA shape (N instances behind a load balancer sharing one SQL backend and
   identical signing material — the blueprint's documented step-ca HA pattern)
   and re-distributes the trust bundle.
1. **L5 coordination: rebuild, don't restore.** Nearly all L5 state
   reconstructs from the running fleet — D servers re-register, leases renew,
   the leader re-elects — which is exactly why §6.5 *stands etcd up* rather
   than restoring it, and why a stale snapshot restore is a **correctness
   hazard** (mvcc rollback → regressed fencing tokens → a stale lock holder
   re-admitted, [bp][bp]). Zone config lives in declarative IaC so etcd is a
   cache, not the sole source of truth. The drill destroys the ensemble,
   re-bootstraps fresh, and asserts the fleet re-registers, a leader re-elects,
   and — the sharp edge — **no placement mutation is corrupted across the
   rebuild**: a custodian deposed before the loss must still lose its
   version-conditional commit after it (the ADR-0015 second fence carries
   safety while coordination-issued tokens restart; whether a persisted
   token-epoch floor is also needed is an Open question this drill answers
   with evidence).
2. **L4 metadata: restore from the out-of-band backup.** TiKV is the one
   mandatory backup ([bp][bp]; [§8.2][s8]: tiny but total blast radius —
   "losing it orphans all chunks"): online consistent MVCC snapshots to
   independent storage, restored onto a fresh TiKV+PD tier. M7 also executes
   the blueprint's named verification: whether log-backup **PITR works against
   standalone `txnkv`** (a TiDB-cluster-documented feature, unverified for
   Wyrd's no-TiDB deployment — the blueprint's open item, [bp][bp]) is settled
   against the
   pinned TiKV-BR version, and the achievable **RPO floor is recorded either
   way** (the snapshot interval if PITR is out). Restore is treated as fallible
   and drilled, not configured (TiKV #13281).
3. **Key plane before encrypted readability — but not before repair.** The
   custodian rebuilds ciphertext without tenant keys (ADR-0021), so
   **durability recovery does not wait for the KMS**: the verify/reconstruct
   pass starts as soon as L4 is back. Encrypted *readability* returns when the
   KMS tier is restored (HA OpenBao, KEK backups held independent of Wyrd —
   [0012][p12]; [ADR-0026][a26]). The runbook states this split explicitly; the
   drill asserts it (repair converges while the KMS is still down).
4. **Custodian verify and re-protect.** The scrub reference-set walk plus the
   liveness diff reconcile the restored map against reality: fragments written
   after the snapshot are unreferenced → collectable orphans (GC's grace-window
   discipline holds); referenced-but-lost fragments rebuild from ≥ *k*
   survivors; chunks beyond tolerance surface as unrepairable — **the RPO made
   visible on the durability plane rather than discovered by users**.

Rack-scale loss slots into the same frame *without* the restore machinery: the
[§7.3][s7] math says a full-strength RS(6,3) layout (9 domains, one fragment
each) tolerates **any 3 domains**, and the reduced 6-domain layout any 1 — so
the drill kills an entire `fd`-label group and asserts reads keep serving,
detection fires once per member server, and the rebuild lands in surviving
distinct domains; then kills one more server than the layout tolerates and
asserts the **honest refusal** (unrepairable surfaced, nothing torn, obligations
retained for when capacity returns).

### Trust- and key-plane outages: fail-closed is bounded, observable, recoverable

Fail-closed is the decided posture for both planes (ADR-0025 §1; ADR-0026 §6);
M7's contribution is the **outage timeline** — what breaks *when*, measured, so
the runbook can state each plane's fuse instead of "it fails closed":

- **CA unreachable.** Established mTLS connections keep serving (the wire's
  fail-closed rule bites at *handshake* time); **new dials refuse immediately**
  — "a misconfigured CA is an outage, not a silent plaintext downgrade — the
  intended trade" ([ADR-0025][a25]) — and **rotation stalls**, which arms the
  real fuse: as short-lived certs expire un-renewed, components fall out one by
  one, ending in a zone-wide halt ([§7.3][s7]). The drill kills the CA tier,
  measures the timeline (immediate: new dials; deferred: expiry-ordered
  fallout), restores it per step 0, and verifies renewal resumes **without
  process restarts** (the M5 rotation-aware acquisition seam, [0011][p11]).
  The runbook's stated invariant: **CA MTTR must be well inside the minimum
  remaining cert lifetime**, which couples this drill to M5's lifetime/renewal
  policy (Open questions).
- **KMS unreachable.** M6 already proved the data-path contract at its own
  Tier-1 — an uncached encrypted read errors, a write refuses, ciphertext
  durability is untouched, recovery on heal ([0012][p12]) — and M7 does **not**
  re-claim it. What M7 adds is the **tier drill at deployment scale**: an HA
  OpenBao member loss (no client-visible effect), a full-tier outage (the
  bounded DEK cache defines the read fade-out window — cached DEKs keep
  serving until their bound expires, then fail-closed), a restore from the
  KEK backups that are deliberately **independent of Wyrd** ([ADR-0026][a26]),
  and the restore-ordering assertion above (repair proceeds on ciphertext
  while the KMS is down). Both outages must be *observable* while in progress —
  visible on the request/durability planes the floor exports — or the drill
  fails its operability half.

### The runbook artifact and the drill discipline

**Where it lives — `docs/design/runbooks/`, a new document class.** The
existing four classes ([design README][rd]) each have the wrong lifecycle:
specs are normative and version-immutable; ADRs and accepted proposals are
immutable records; the architecture overview is *descriptive* — it says what
the system is, not what an operator must do at 03:00. A runbook is
**prescriptive and living** — it MUST be amended after every drill that
falsifies a step, which is the opposite of the immutability the decision
classes carry, and the [blueprint][bp] (an "operational note, not a normative
spec" already stretching the architecture class) shows the need for an
operational home. So: `docs/design/runbooks/single-dc-failover-and-recovery.md`
(frontmatter `type: runbook, status: living`), with **dated, append-only drill
records** under `docs/design/runbooks/drills/` — the record immutable like an
ADR (it is evidence), the procedure living like architecture. The README's
class table gains the fifth row, and the class plus its append-only-drill-record
change process are minted in a **lightweight ADR landed with M7.5** — the
ADR-first habit for a settled process decision ([design README][rd]), not a new
document class smuggled in under a milestone plan.

**What it contains:** the failure-detection matrix (signal, bound, responding
loop, per class); per-failure-class response procedures (node / disk / rack /
custodian-leader / etcd quorum / TiKV+PD / CA / KMS); the single-DC restore
ordering above, as numbered operator steps with verification gates between
them (the day-one-runbook pattern the blueprint set: each step gates the
next); backup configuration and **backup verification** (cadence, independence
per [§8.2][s8], the restore-test rule — a backup is only real once restored);
and the **drill protocol**: which legs constitute a full drill, the
drill-record template (scenario, timeline, measured time-to-detect /
time-to-repair / RPO / RTO, deviations, resulting regressions and runbook
amendments), and the standing rule that **every surprise becomes a seeded DST
regression** ([ADR-0009][a9]).

**Cadence:** graduation requires **one full drill executed** on the
first-deployment substrate. The runbook prescribes the standing cadence —
at minimum before each release and after any change to topology, backup
tooling, or the restore ordering — with the long-term calendar cadence
recorded as an operational open item rather than invented here.

### DST and tests — what each tier proves

> **Numbering note.** As in [0012][p12]: this document uses the architecture
> **realism ladder** ([§13][s10]: Tier 0 DST · Tier 1 local software faults ·
> Tier 2 first real hardware · Tier 3 multi-region), whose M7 row already reads
> "Tier 0–2; zone-internal node/disk/rack fault injection + local recovery
> drill" ([§13.4][s10]). The *code/CI* taxonomy's `Tier-1`/`Tier-2` labels
> (`cargo xtask disk-faults` / `jepsen` / `kill-reconstruct`) are proposal
> 0004/0005's scheme and run within ladder Tiers 0–1; new M7 legs keep those
> CLI names and the deferred-by-default opt-in gating (`WYRD_TIER1` /
> `WYRD_TIER2`, the `Plan` gate in `xtask/src/faults.rs`).

**Tier 0 — deterministic simulation (the logic).** Everything decision-shaped
is madsim-provable against the in-memory seams, seed-reproducible:

- **Detection properties:** a lapsed registration inside the suspicion window
  enqueues nothing; past it, exactly the departed server's placed fragments are
  enqueued (no over- or under-enqueue); a drained/decommissioning server is
  never treated as dead; a flapping registration does not oscillate the queue;
  time-to-detect is measured on the simulated clock (ADR-0024's injected-clock
  seam is what makes the window deterministic).
- **Rack-scale placement properties:** kill all servers of one domain in the
  model → rebuild lands in surviving distinct domains; kill past tolerance →
  `Unrepairable` surfaced, obligations retained, nothing torn — both sides of
  the [§7.3][s7] boundary as properties.
- **Restore-reconcile properties:** run the custodian verify pass over a
  deliberately *older* metadata state plus the surviving fragment population —
  post-snapshot fragments become orphans, referenced-but-lost chunks rebuild,
  convergence is exact. This is the restore drill's core semantics, provable
  deterministically because it is pure logic over the seams.
- **Fencing-across-rebuild property:** reset the coordination state under a
  live deposed custodian; its location mutation still loses the
  version-conditional commit — the second fence carries safety while tokens
  restart.

**Tier 1 — local software-defined faults (the real cluster).** The new legs
extend the existing compose plumbing: whole-`fd`-group container kills
(rack-scale, both sides of tolerance); a **whole-device** `dm-error` leg
(a dead disk, vs the existing dead-sector leg — under ADR-0034 equivalent to a
D-server death and thus also a *detection* test); etcd-ensemble kill +
fresh-rebuild with the re-registration/re-election/fence assertions; TiKV+PD
kill + BR restore + verify-pass assertions; CA-tier kill (against the M5
`deploy/` step-ca) with the dial/rotation timeline; KMS-tier kill (against the
M6 `deploy/` OpenBao) with the repair-proceeds/readability-waits split. Every
behaviour the real tiers surface that the models did not is **promoted to a
seeded DST regression wherever it manifests through the seams** — the
compounding loop ([ADR-0009][a9]).

**Tier 2 — first real hardware.** The single-machine tier gives the honest
numbers the drill's bounds are stated against: real-fsync repair throughput,
real BR snapshot/restore duration on NVMe (the RTO term), real lease-expiry
timing — a single failure domain, so it calibrates bounds, never claims
independence.

**The live drill (the graduation leg).** Failure-domain independence "is a
topology property, not a process count" ([§7.3][s7]) — the one thing no
container cluster on one host can prove — and the runbook's operator loop is
human. So graduation is a drill on the **first-deployment substrate** (#367;
the blueprint's Hetzner shape, where `hcloud server delete` of an `fd`-group is
an honest rack-loss and the homelab variant can pull a literal power strip):
node kill, `fd`-group kill, etcd rebuild, TiKV restore, CA and KMS outage
windows, each against the runbook's stated bounds, timed, recorded.

### Crate touch-points

Building on the workspace as the preceding plans leave it (the M0–M3 crates
present today — `chunk-format`, `chunkstore-fs`, `chunkstore-grpc`,
`coordination-mem`, `core`, `custodian`, `dst`, `metadata-redb`, `proto`,
`server`, `testkit`, `traits`, `xtask` — plus the backend and `deploy/`
additions 0007/0011/0012 make):

- **`traits`** — **unchanged.** The audit's strongest finding: detection needs
  `Coordination::register`'s leases and `Coordination::discover`, and both
  exist; the repair queue and the metadata scan exist. A trait edit here is a
  failure signal for M7's composition thesis.
- **`custodian`** — the liveness reconciler (a new module beside
  `gc`/`scrub`/`reconstruction`/`rebalance`, dispatched from `reconcile_step`
  with its own context struct); time-to-detect emission in `telemetry.rs`'s
  scope. No new authority: same fence, same queue, same commit discipline.
- **`core`** — the shared repair-queue helpers gain the liveness source label;
  no read/write-path change (detection produces obligations; reconstruction
  already consumes them).
- **`server`** — wire the discovered-membership view (decode
  `DServerRegistration` off `discover`) into the custodian role's contexts at
  the composition root — contingent on 0010/#366 having wired the custodian
  loop into the binary at all (the named dependency).
- **`dst` / `testkit`** — the four Tier-0 property families above; fault seams
  for registration lapse/flap, coordination reset, and stale-metadata restore.
- **`xtask`** — new deferred-by-default legs (rack-scale kill, quorum
  kill/rebuild, restore-verify, CA/KMS outage), reusing the compose plumbing,
  log-capture-before-teardown, and `Plan` gating the M3 runners established.
- **`deploy/`** — the backup jobs (TiKV BR schedule to independent storage;
  the etcd-config-in-IaC pattern), and drill compose profiles (multi-`fd`
  clusters; the control/trust/key tiers killable as groups), extending the
  0007/0011/0012 stacks. No crate imports an orchestrator API
  ([ADR-0010][a10]).
- **`docs/design/runbooks/`** (**new**) — the runbook + `drills/` records; the
  design README's class table gains the row.

## Alternatives considered

- **Assert via DST only — no live drill:** **rejected.** DST proves logic, and
  M7's Tier-0 additions carry the logic — but failure-domain independence is a
  *topology* property no simulator or single-host container cluster possesses
  ([§7.3][s7]), restore tooling against real TiKV/etcd/step-ca/OpenBao is
  exactly the "real environments are complementary" tier ADR-0009 reserves, and
  the runbook's operator loop cannot be simulated at all. The arc's DoD verb is
  *drilled*; "asserted" is the failure mode this milestone exists to retire.
- **Fold M7 into M8:** **rejected.** M8 *needs* M7 — "failure recovery is an
  operator workflow" ([p2][p2] M8) presupposes a recovery that exists and is
  proven before the management plane wraps it. Folding them gates the ★ on both
  at once, doubles the release-blocking surface, and inverts the risk order:
  recovery correctness is a prerequisite of operability, not a feature of it.
- **Run M7 before M6 (drill against dev-CA / dev-keys — cheaper, earlier):**
  **rejected, with the tension recorded.** The arc orders M7 after M6
  deliberately, "so recovery operates on the secured, encrypted store"
  ([p2][p2] M7): M5/M6 *change failure semantics* — fail-closed mTLS makes the
  CA restore step 0 of the ordering, and encryption splits recovery into
  durability (keyless, immediate) vs readability (KMS-gated) — so a recovery
  proven on the plaintext store would need re-proving anyway. The honest
  tension: a dev-CA drill *is* cheaper and earlier, and the first-deployment
  gate (#367) already contemplates a dev-CA-first bring-up. Resolution: M7.1–
  M7.2 (detection, rack legs) can begin against the tail of M6 — they touch
  neither keys nor certs — while the graduation drill waits for the secured
  store.
- **Continuous chaos engineering instead of scheduled drills:** **deferred.**
  Randomized fault injection in the production deployment presupposes the M8
  operator surface, mature alerting, and fleet headroom; at first-deployment
  scale it is indistinguishable from self-inflicted outage. The Tier-1/Tier-2
  campaigns *are* the continuous automated arm — in CI, where they belong —
  and the drill is the deliberate, human-in-the-loop complement.
- **Restore etcd from snapshot (symmetric with TiKV):** **rejected** — the
  blueprint's correctness hazard: an mvcc rollback can regress fencing tokens
  and re-admit a stale lock holder. Rebuild-fresh is the documented default; a
  periodic snapshot remains cheap insurance for config, and durable zone config
  belongs in IaC so etcd stays a cache ([bp][bp]; [§6.5][s6]).
- **Back up D-server fragments ("simpler recovery"):** **rejected** — [§8.2][s8]
  row 1: EC + custodian reconstruction *is* the fragments' durability
  mechanism; a fragment backup is redundant with it, roughly doubles storage
  cost, and would tempt the drill into exercising the wrong (restore-bytes)
  path when the correct one is rebuild-from-survivors ([bp][bp]).
- **A standalone failure-detector service:** **rejected** — a second writer to
  the repair queue with its own election, fence, and deployment footprint. The
  custodian is already the single-active, fenced durability authority;
  detection is one more reconciler on the existing control point, preserving
  the one-entry discipline the scaffold enforces.
- **Detect via a gateway-side read-failure heuristic instead of the lease:**
  **rejected** — read-path failures already feed the repair queue per-fragment
  and cannot distinguish a dead server from a slow one; the lease *is* the
  designed liveness signal ("a crashed member's registration lapses",
  `traits`). The read path stays a complementary trigger, not the holistic one.
- **Mint a new ADR:** **not minted** — §6.5/§7.3/§8.2, ADR-0011, ADR-0015, and
  the blueprint's backup model already carry every decision M7 implements;
  M7 is their first execution, not a new decision. (The one candidate — the
  runbooks document class — is a documentation-process question, flagged in
  Open questions rather than smuggled in as architecture.)

## Graduation criteria (definition of done)

- **A dead D server is detected by a production path** — lease lapse →
  suspicion window (a *satisfied* drain excluded; a departed server whose drain
  is unsatisfied is **not** excluded) → repair obligations for every placed
  fragment on the departed server, on the shared queue, from the fenced control
  point; proven at Tier 0 (seeded, incl. the implausible-clock leg) and against
  a real cluster kill (the M7.2 leg asserts detection fires per killed member).
- **Time-to-detect is on the durability plane** alongside ADR-0011's five, and
  the **failure-detection matrix** (class × signal × bound × responder) is
  written into the runbook.
- **Node, disk, and rack loss recover within tolerance and refuse honestly
  beyond it** — whole-`fd`-group kill: reads keep serving, rebuilds land in
  surviving distinct domains, under-replicated returns to zero; past
  tolerance: unrepairable is surfaced, never silent, never torn.
- **etcd quorum loss recovers by rebuild-don't-restore** — fleet re-registers,
  a leader re-elects, and no placement mutation is corrupted across the
  rebuild (the second fence holds; the token-epoch question answered with
  evidence).
- **TiKV is restored from the out-of-band backup and verified** — the custodian
  verify pass reconciles the restored map (post-snapshot orphans collected,
  referenced-but-lost rebuilt, beyond-tolerance surfaced); measured RPO/RTO
  recorded; **PITR-vs-`txnkv` settled** against the pinned TiKV-BR, or the
  snapshot-interval RPO floor recorded as the standing answer.
- **CA outage is bounded and recoverable** — established traffic unaffected,
  new dials refused, rotation stalls; the what-breaks-when timeline measured;
  recovery per §6.5 step 0; renewal resumes without restarts.
- **KMS outage is bounded and recoverable at tier scale** — HA member loss
  invisible; full outage fail-closed within the DEK-cache bound with ciphertext
  durability intact (M6's contract, reconfirmed not re-proven); **repair
  converges while the KMS is down**; readability returns on restore from
  Wyrd-independent KEK backups.
- **The runbook exists in `docs/design/runbooks/`** — matrix, procedures, the
  single-DC restore ordering with verification gates, backup verification, and
  the drill protocol with record template; the README class table updated.
- **One full drill executed on the first-deployment substrate (#367)** — all
  legs, against the runbook as written, its dated record committed under
  `runbooks/drills/`, and **every surprise promoted to a seeded DST
  regression**.
- `fmt`/`clippy` clean; new xtask legs deferred-by-default with hard-fail
  opt-in; `cargo-deny` unaffected or deliberately updated ([ADR-0003][a3]).

### Suggested PR sequence (each with its own definition of done)

Each step is one PR, tracked under the **M7** milestone (branch
`feat/m7.<n>-<slug>`, commit subject `feat(<crate>): … (M7.<n>, #<issue>)`):

1. **M7.1 — the liveness reconciler + time-to-detect**
   (`feat/m7.1-liveness-detection`, `feat(custodian): …`). The fifth loop:
   membership diff over `Coordination::discover`, suspicion window on the clock
   seam, drain/decommission exclusion, per-fragment obligations with a liveness
   source label; time-to-detect on the telemetry seam. *DoD:* Tier-0 detection
   property family green and seed-reproducible (exact enqueue set, window
   honored, **satisfied-drain excluded but an unsatisfied drain that departs
   enqueues its remaining fragments**, flap-stable, and an **implausible-clock
   leg that fails closed with no enqueue**, [ADR-0024][a24]); `traits`
   unchanged; the metric observable on both export surfaces.
2. **M7.2 — rack-scale and beyond-tolerance fault legs**
   (`feat/m7.2-rack-scale-faults`, `feat(xtask): …`). Whole-`fd`-group kill and
   whole-device `dm-error` legs extending the M3 runners (same compose
   plumbing, `Plan` gating, log-capture-before-teardown); Tier-0 rack-loss
   placement properties. *DoD:* within-tolerance group kill rebuilds into
   surviving distinct domains with reads unbroken; **liveness detection fires
   once per killed member** (the real-cluster-kill half of graduation criterion
   1, against the M7.1 loop); past-tolerance kill surfaces unrepairable (the
   `emit_under_replicated` gap above closed as part of this leg), retains
   obligations, tears nothing; legs inert without opt-in.
3. **M7.3 — quorum-loss recovery: etcd rebuild, TiKV restore-verify**
   (`feat/m7.3-quorum-recovery`, `feat(xtask): …` / `feat(deploy): …`). The
   etcd destroy → fresh-rebuild leg with re-registration / re-election / fence
   assertions; the TiKV BR backup job in `deploy/` + the restore leg feeding
   the custodian verify pass; the PITR-vs-`txnkv` verification against the
   pinned version; Tier-0 restore-reconcile and fencing-across-rebuild
   properties. *DoD:* both recoveries executed by automation; verify pass
   converges (orphans collected, lost rebuilt); RPO/RTO measured; the PITR
   answer recorded; no fence regression observed.
4. **M7.4 — trust- and key-plane outage drills**
   (`feat/m7.4-trust-key-outage`, `feat(xtask): …`). CA-tier kill/restore with
   the dial/rotation/expiry timeline; KMS-tier kill/restore with the
   repair-proceeds/readability-waits assertion and the KEK-backup restore path.
   *DoD:* both timelines measured and bounded; renewal resumes without
   restarts; repair converges under KMS outage; both outages visible on the
   exported planes.
5. **M7.5 — the runbook + drill protocol** (`feat/m7.5-runbook`,
   `docs(runbooks): …`). `docs/design/runbooks/single-dc-failover-and-recovery.md`
   (matrix, procedures, restore ordering with gates, backup verification, drill
   protocol + record template); `drills/` scaffold; README class-table row; a
   **lightweight ADR minting the runbooks document class** and its
   append-only-drill-record change process. *DoD:* every procedure traces to an
   automated leg or a manual step with a verification gate; the M8 boundary
   (workflow-and-API, incl. narrowing 0008's backup/runbook criterion — #369)
   and M11 boundary (zone-scale) stated in the artifact itself.
6. **M7.6 — the graduation drill** (`feat/m7.6-graduation-drill`). Execute the
   runbook end to end on the #367 substrate (the blueprint topology): node,
   rack, etcd, TiKV, CA, KMS legs, timed against the stated bounds. *DoD:* the
   dated drill record committed; every deviation either amends the runbook or
   lands as a seeded DST regression (or both); the arc's M7 done-when sentence
   is checkably true.

(M7 is sized *unlike* M4–M6: one new custodian loop and one new metric are the
only production-code growth; the weight is drill automation, recovery
execution, and the runbook — verification-led by design. M7.1–M7.2 can begin
against the tail of M6, touching neither keys nor certs; M7.3 needs the 0007
`deploy/` TiKV stack; M7.4 needs M5's step-ca and M6's OpenBao tiers; M7.6
needs the #367 first-deployment gate and 0010's observability floor.)

## Backward compatibility

M7 lands last before the M8 ★, and its compatibility surface is deliberately
the narrowest in Step 2:

- **The seam traits** — byte-for-byte **unchanged**: detection composes
  `Coordination` and `MetadataStore` surfaces that already exist. (The one
  internal growth: `reconcile_step` gains an optional fifth context, the same
  additive shape rebalance took — an internal, pre-1.0 signature.)
- **The metadata model** — **additive only**: liveness reuses the existing
  durable repair ledger with a new source label; no new keyspace shape, no
  migration; the version-conditional commit discipline is untouched.
- **On-disk formats** — **untouched**. M7 moves no byte format; fragments,
  headers, and conformance vectors are exactly M6's.
- **Telemetry** — additive: time-to-detect joins the existing scope on the
  existing dual-export surfaces (ADR-0012); no existing metric is renamed.
- **The deployment surface** — extended, not broken: backup jobs and drill
  profiles are new `deploy/` artifacts; the runbook becomes part of the
  documented production shape. There are no public deployments to migrate
  before the M8 release point ([p2][p2]).
- **Reserved seats honored** — **M11** inherits the drill machinery, runbook
  shape, and record discipline at zone scale (the full §6.5 ordering with L3);
  **M8** wraps the drilled procedures in the management plane ([0008][p8]);
  **ADR-0029**'s key-compromise emergency runbook is the reserved sibling in
  the new class; a push membership watch remains the seam's reserved
  refinement over today's polled `discover`.

## Open questions

- **The suspicion window and the detection signal set.** How long after lease
  lapse before mass re-placement starts — trading a false positive's cost (a
  whole server's repair traffic, contending with foreground reads) against a
  longer at-risk window — and whether the lease signal should be corroborated
  (e.g. by the existing `ChunkStore::health` probe) before declaring death, or
  a reachable-but-sick server ("gray failure") handled by a separate policy.
  The floor is structural (lease TTL + reconcile cadence); the value is a
  Tier-1/live **measurement**, shipped as a bounded default plus a knob.
- **Fencing across a fresh etcd rebuild.** Coordination-issued fencing tokens
  restart when the ensemble is rebuilt rather than restored. Is the ADR-0015
  second fence (the version-conditional commit) sufficient on its own for
  every custodian action — or does the rebuild need a persisted token-epoch
  floor (e.g. in L4 config) so L5-issued tokens never regress? M7.3's drill
  answers with evidence and pins the answer as a standing test.
- **TiKV PITR against standalone `txnkv`.** The blueprint's open verification
  item ([bp][bp]): log-backup PITR is documented for TiDB clusters and RawKV,
  not Wyrd's bare `txnkv`. Verify against the pinned TiKV-BR/`tikv-client`; if
  unsupported, the RPO floor is the snapshot interval and the runbook says so
  plainly.
- **The CA outage fuse: cert lifetime and renewal margin.** Fail-closed mTLS
  makes minimum-remaining-cert-lifetime the zone's outage fuse under CA loss.
  What lifetime/renewal-margin policy balances that fuse against short-lived-
  cert security — and how it interacts with M5's provisioner choice — is
  measured by M7.4 and settled with the M5 pins, not assumed here.
- **Drill cadence.** Per-release + post-change is this proposal's floor;
  whether a standing calendar cadence is warranted pre-M8 is an operational
  question to settle with the first drill's experience, not invented here. (The
  runbooks document class and its change-process rule are *decided*, not
  deferred — minted in the M7.5 ADR above.)
- **Which durability-plane alerts the drill requires of the floor.** A drill is
  only operator-real if the outage is *alerting*, not merely log-visible:
  sustained under-replicated > 0, a time-to-detect bound breach, a stalled
  repair queue. Which of these the [0010][p10] floor must carry before M7.6 —
  versus what waits for M8's alerting policy — is coordinated with 0010 at
  M7.5.

[p2]: ../accepted/0013-implementation-arc-rescoped.md
[p5]: ../accepted/0005-milestone-3-custodians.md
[p7]: ../accepted/0007-milestone-4-production-metadata-backend.md
[p8]: ./0008-management-and-administration.md
[p9]: ./0009-d-server-performance.md
[p10]: ./0010-observability-floor-for-first-deployment.md
[p11]: ../accepted/0011-milestone-5-internal-ca-step-ca.md
[p12]: ../accepted/0012-milestone-6-encryption-at-rest-kms.md
[bp]: ../../architecture/m4-first-deployment-blueprint.md
[rd]: ../../README.md
[s6]: ../../architecture/06-runtime-view.md
[s7]: ../../architecture/07-deployment-view.md
[s8]: ../../architecture/08-crosscutting-concepts.md
[s10]: ../../architecture/10-quality-risks-glossary.md
[a3]: ../../adr/0003-apache-2-license-and-dco.md
[a9]: ../../adr/0009-deterministic-simulation-testing.md
[a10]: ../../adr/0010-pluggable-deployment-substrate.md
[a11]: ../../adr/0011-durability-telemetry-and-declarative-management.md
[a12]: ../../adr/0012-opentelemetry-instrumentation.md
[a15]: ../../adr/0015-consistency-contract.md
[a21]: ../../adr/0021-encryption-at-rest-and-key-management.md
[a24]: ../../adr/0024-clock-and-time-source-trust.md
[a25]: ../../adr/0025-internal-service-to-service-trust.md
[a26]: ../../adr/0026-key-service-and-kms-backend-selection.md
[a29]: ../../adr/0029-key-compromise-emergency-response.md
[a34]: ../../adr/0034-d-server-disk-model.md
[a36]: ../../adr/0036-internal-ca-step-ca-spire.md
[a39]: ../../adr/0039-tier1-consistency-in-repo-scenario.md
