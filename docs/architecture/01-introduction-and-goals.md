# 1. Introduction and goals

> Status: living document. Reflects decisions through the initial design phase.
> Open questions are marked **[OPEN]** inline.

## 1.1 What this is

**Wyrd** is an open-source foundation for globally scalable, distributed file storage —
the substrate on which a cloud provider can build a worldwide, horizontally
scalable storage product (a Drive-like service, an object store, or the backend
for collaborative document editing).

The design follows the architecture of Google's Colossus stack rather than its
predecessor GFS: metadata in a scalable transactional store, dumb storage
servers, intelligence concentrated in the client library and background
maintenance services, and a separate global control plane for multi-region
behavior.

## 1.2 The headline guarantee

**Writes are atomic from the user's perspective.** A write is invisible until a
single linearizable operation makes it fully visible; that operation either
happens completely or not at all. There is never a torn or half-written state
observable by any reader.

Crucially, this does *not* require co-locating metadata and blob data. Atomicity
requires a single linearization point, not physical co-location. Data fragments
are written first (unreferenced and invisible), and a single metadata commit
makes the file exist. This is the central design insight and the project's
primary differentiator: a **specified, provably-atomic commit protocol**.

## 1.3 Quality goals

In priority order:

1. **Correctness / durability.** The system must not lose or corrupt data, and
   its atomicity guarantee must be specifiable and testable. This is the
   project's reason to exist; it outranks performance and features.
2. **Horizontal scalability.** Aggregate throughput scales close to linearly
   with hardware on the data path, because bulk data never crosses a shared
   component. Both the data tier and the metadata tier scale independently.
3. **Operability.** The durability state of the system must be observable, and
   the dangerous-but-routine operations (draining, upgrades, recovery) must be
   safe, resumable, and observable.
4. **Scale range.** The same codebase runs as a single binary for development
   and spans a multi-region provider fleet in production.
5. **Replaceability.** Underlying storage, metadata store, and coordination
   service are all pluggable behind narrow interfaces.

## 1.4 Stakeholders and target users

- **Primary:** mid-sized cloud providers deploying the system as the foundation
  of a storage product across their own regions.
- **Secondary:** application teams building on top — Drive-like document storage
  today, collaborative editors (Google-Docs-style) later.
- **Tertiary:** developers and evaluators running the single-binary profile to
  learn, develop against, or assess the system.

## 1.5 Scope boundaries

- **Single-provider federation only.** Regions and nodes cooperate *within one
  provider*. There is no cross-provider or untrusted-operator operation. This is
  a deliberate exclusion that removes a large amount of protocol surface (see
  ADR-0005); it is reversible if cross-provider federation is ever required.
- **The single-binary / NAS-class profile is for development and evaluation
  only**, not a supported production tier (ADR-0014). Production durability
  begins at the real multi-node backends with proper failure-domain separation.
- The system provides storage primitives. Application-level concerns
  (collaborative-editing merge logic, OT/CRDT engines) are out of scope, though
  the storage primitives needed to support them (atomic append, compare-and-set,
  change notification) are reserved from the start (ADR-0007).
