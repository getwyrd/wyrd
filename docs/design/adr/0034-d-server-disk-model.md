---
created: 23.06.2026 14:30
type: adr
status: Proposed
tags:
  - adr
  - storage
  - d-server
  - failure-domain
  - durability
  - deployment
---
# 0034. D server disk model: one-per-disk now, multi-disk reserved

## Context

A D server stores erasure-coded fragments on local disk. A real storage host
typically has many disks, which raises a structural question with two coherent
answers:

- **Model A — one D server per disk.** Each physical disk runs its own D server
  process. A disk failure is a whole-D-server failure: the process is lost, the
  custodian observes a failure-domain loss, and reconstruction proceeds via the
  existing "D server died" path (the reconstruction custodian, proposal 0005;
  runtime view section 6). The disk *is* the failure domain.
- **Model B — one D server managing multiple disks.** A single D server process
  owns several disks and places fragments across them internally. A single disk
  failure is now a *partial* failure of a still-running D server, and the failure
  domain is no longer flat: it is a disk-within-server (within-rack) hierarchy.

The decision matters because Wyrd's entire durability guarantee rests on
fragments landing in **independent failure domains** (RS(6,3): 9 fragments, any 6
reconstruct, up to 3 losses tolerated). The choice of disk model is therefore a
choice about *what the unit of independent failure is*, not merely an
implementation convenience.

Model A is the simpler and lower-risk model, and it matches the Colossus lineage
(D servers are network-attached disks; placement intelligence lives above them in
the client and custodians). Model B's only advantages — fewer processes, lower
per-process overhead, denser nodes — matter at an operational scale and density
the project does not yet have, and Model B carries real cost: it introduces a
"D server up but lost a subset of its fragments" failure mode the custodians must
handle explicitly, and it forces the placement engine to understand a two-level
failure-domain hierarchy (otherwise it may place two fragments of one chunk on two
disks of the *same* server, silently under-spreading the RS(6,3) redundancy a
whole-server failure would then violate).

Nothing in the single-zone milestones (through M4) requires Model B.

## Decision

Adopt **Model A — one D server per disk** for now. Each physical disk is served
by its own D server process and is treated as one independent failure domain.
Reconstruction reuses the existing whole-D-server-loss machinery (proposal 0005);
no partial-failure path and no hierarchical failure-domain model are introduced.

**Model B is reserved as a future option**, not built. The reserved seat is kept
open by one load-bearing architectural requirement, which the implementation MUST
honor from now on:

> **The failure domain MUST be a first-class, explicitly-labeled concept,
> distinct from D-server identity — never an implicit identity equivalence.**

Under Model A the failure-domain label is 1:1 with the D server, which makes it
tempting to collapse the two in code ("the D server id *is* the failure domain").
That collapse MUST be resisted. Concretely:

- A D server registers (via L5 coordination) with an explicit **failure-domain
  label**, carried as its own field, even though under Model A it is currently
  redundant with the D server's identity.
- The placement / fragment-selection logic (the zone-local failure-domain-aware
  selector, proposal 0005) spreads fragments **by failure-domain label**, not by
  D-server identity.
- No component may assume `D-server == failure domain == disk` as a hardcoded
  equivalence.

With this seam preserved, the later move to Model B is a *contained extension*
(allow many disks to map to one server; let the label express a disk-within-server
hierarchy; add the partial-failure custodian path) rather than a cross-cutting
refactor that must untangle a conflated identity everywhere it leaked.

This is the same "reserve the seat cheaply" discipline applied elsewhere (e.g. the
reserved `FLAG_ENCRYPTED` bit in the chunk format, ADR-0019/ADR-0021): the seam
costs almost nothing to preserve now and is expensive to retrofit.

## Architecture and implementation requirements

To follow this decision, the architecture and code MUST:

1. **Keep `failure_domain` a distinct, first-class field** on D-server
   registration and in the placement model — separate from D-server id/endpoint —
   and document it as such in the building-block view (section 5, the L4 D server
   and the placement/selection description).
2. **Place fragments by failure-domain label**, so the RS(6,3) spread is
   expressed against domains, not server identities. This is already the correct
   behavior under Model A (1:1) and is what makes Model B a non-breaking change.
3. **Treat each disk as one D server / one failure domain** operationally:
   deployments run one D-server process per disk (JBOD-style), not one process
   over a RAID set. RAID under a single D server is explicitly discouraged — it is
   redundant with the cross-server erasure coding and it hides disk failures from
   the custodian that the failure-domain model needs to see and react to.
4. **Document the reserved Model B** in the deployment view (section 7) and the
   relevant roadmap/proposal, with its trigger conditions (node-density /
   process-overhead pressure at scale) and its two prerequisites (the
   partial-failure custodian path and the hierarchical failure-domain model), so a
   future implementer knows it was a deliberate deferral with a defined upgrade
   path, not an unconsidered default.

Hardware guidance follows from Model A (recorded here so it is not relitigated):
because erasure coding and reconstruction happen in the client and custodians —
not on the D server — a D server is I/O- and network-bound, not CPU-bound. Provision
it like a disk with a network port: capacity-optimized storage with honest fsync,
≥10GbE on the data path, modest CPU and memory, one D server per disk, no RAID.

## Consequences

- The failure-domain model stays **flat and honest** while the durability story is
  still being established: disk = D server = failure domain, and "disk failed" is
  identical to the already-handled "D server failed".
- Reconstruction, placement, and scrub logic carry **no partial-failure special
  case** and no two-level hierarchy — less code and less correctness risk at this
  stage.
- More, smaller D servers (one per disk) give the placement engine **more
  independent failure domains** to spread across, strengthening the RS(6,3) math,
  at the cost of more processes to run and coordinate — accepted, because process
  density is not a constraint at single-zone scale.
- **Model B remains a cheap future option** *provided* the first-class
  failure-domain-label requirement is honored. If that abstraction is ever
  collapsed into D-server identity, this consequence is lost and Model B becomes a
  refactor — so the label discipline is the thing reviewers must protect.
- A latent risk to watch: if any component starts treating D-server identity as
  the failure domain (because it is 1:1 today), the reserved seat silently closes.
  A code-review check and, ideally, a test that fragment placement is keyed on the
  label (not the id) guards against this.
- Adjacent to ADR-0032 (the FsChunkStore on-disk layout these fragments land in)
  and ADR-0033 (fragment durability via redundancy): ADR-0033's soundness
  condition — that no correlated power event drops more than `m` of a chunk's
  fragments — relies on exactly the distinct-failure-domain placement this ADR
  keeps first-class.

## Revisit when

Node-density or per-process overhead becomes a measured operational problem at
fleet scale, *and* the two Model B prerequisites (partial-failure custodian path,
hierarchical failure-domain placement) are worth building. Until both hold, Model
A stands.
