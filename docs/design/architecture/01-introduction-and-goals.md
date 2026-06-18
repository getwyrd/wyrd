---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - goals
  - requirements
---
# 1. Introduction and goals

> Status: living document. Reflects decisions through the initial design phase. Open questions are marked **[OPEN]** inline.

## 1.1 What this is

Cloud computing has become the norm for providing solutions in business and for consumers: streaming platforms for consuming entertainment, social media platforms to connect to other people and business software to produce, share and communicate - they all use infrastructure at scale. 

Cloud computing has opened up a gap though: the control of larger infrastructures and solutions that use them has become the sole provenance of an elite few with the money and time to not only build the infrastructure but the *custom software* to control such scaling as well. The providers of such scaling solutions never released the code and if you want to use it for your own solution, you have to use their service . 

Not all is bleak though: Distributed SQL, Analytics, Streaming and other OSS solutions close many gaps and are the de-facto standards used by companies to protect themselves from vendor lock-in. There is one area that still is a genuine gap:  an exabyte capable, provably atomic object storage.  And this has consequences for all  open source based systems that target potentially global scaled solutions:  they have to either implement it themselves or accept the limitation in scaling.

**Wyrd** is an open-source foundation for globally scalable, distributed file storage — the substrate on which any provider can build a worldwide, horizontally scalable storage product (a Drive-like service, an object store, or the backend for collaborative document editing) and any open source software can scale with.

The design is inspired by the architecture of Google's Colossus stack rather than its predecessor GFS: metadata in a scalable transactional store, dumb storage servers, intelligence concentrated in the client library and background maintenance services, and a separate global control plane for multi-region behavior.

## 1.2 The headline guarantee

**Writes are atomic from the user's perspective.** A write is invisible until a single linearizable operation makes it fully visible; that operation either happens completely or not at all. There is never a torn or half-written state observable by any reader.

Crucially, this does *not* require co-locating metadata and blob data. Atomicity requires a single linearization point, not physical co-location. Data fragments are written first (unreferenced and invisible), and a single metadata commit makes the file exist. This is the central design insight and the project's primary differentiator: a **specified, provably-atomic commit protocol**.

## 1.3 Quality goals

These are the forces the architecture is optimized for, in priority order: when two of them pull against each other, the lower-numbered one wins. Each goal is made measurable in section 10 (quality scenarios) and traced to a concrete strategy in section 4 (solution strategy); this section is the *why* behind it.

1. **Correctness / durability.** The system must not lose or corrupt data, and its atomicity guarantee must be specifiable and testable. This is the project's reason to exist; it outranks performance and features.

   Data outlives the software that wrote it: a provider holding petabytes cannot accept silent corruption, and here the corruption risk *is* silent — wrong reads, not crashes — concentrated in the thick client and the background custodians (repair, scrub, GC), the paths hardest to exercise by hand. That is why correctness is gate-zero and why it is pursued structurally — a specified commit protocol, conformance vectors, deterministic simulation testing (ADR-0009) — rather than treated as a feature to harden later.

2. **Horizontal scalability.** Aggregate throughput scales close to linearly with hardware on the data path, because bulk data never crosses a shared component. Both the data tier and the metadata tier scale independently.

   The target is a provider fleet, where the economics depend on throughput rising with the hardware you add rather than flattening at a shared bottleneck. The Colossus-class split — dumb storage servers, bulk bytes flowing directly between client and D server, metadata on its own scalable tier — exists precisely so neither path becomes the chokepoint. A Drive-style small-file workload shifts the pressure onto the metadata tier instead, which is why that tier has to scale on its own (section 10, Q6–Q7).

3. **Operability.** The durability state of the system must be observable, and the dangerous-but-routine operations (draining, upgrades, recovery) must be safe, resumable, and observable.

   At fleet scale these operations are not exceptional — draining a server, rolling an upgrade, recovering a zone happen continuously, and they are where data loss and outages actually originate, not steady-state serving. An operator must be able to answer "is my data meeting its durability policy right now?" (ADR-0011) and to run these procedures without crossing their fingers: a half-finished drain must resume, not fall off a cliff (section 10, Q8–Q9).

4. **Scale range.** The same codebase runs as a single binary for development and spans a multi-region provider fleet in production.

   One system configured differently, not two implementations kept in sync. A developer or evaluator gets the whole thing from a single binary on a laptop (embeddable backends), and the same code spans regions with distributed backends — so what you test locally is what ships. The single-binary profile is deliberately a dev/eval convenience, not a production durability tier (ADR-0014); this goal buys ergonomics and fidelity, not a cheaper way to run production.

5. **Replaceability.** Underlying storage, metadata store, and coordination service are all pluggable behind narrow interfaces.

   The heavy external dependencies are the parts most likely to change with scale, operational taste, or licensing — and the most damaging to be locked into. Narrow interfaces keep each one swappable (embedded store for dev, distributed for production — ADR-0008, ADR-0006) and keep its failure modes contained, e.g. the rule that nothing data-proportional ever lives in the coordination tier (section 11). It is also what makes goal 4 possible: scale range is just two backend choices behind one interface.

## 1.4 Stakeholders and target users

- **Primary:** mid-sized cloud providers deploying the system as the foundation of a storage product across their own regions.
- **Secondary:** application teams building on top — Drive-like document storage today, collaborative editors (Google-Docs-style) later.
- **Tertiary:** developers and evaluators running the single-binary profile to learn, develop against, or assess the system.

## 1.5 Scope boundaries

- **Single-provider federation only.** Regions and nodes cooperate *within one provider*. There is no cross-provider or untrusted-operator operation. This is a deliberate exclusion that removes a large amount of protocol surface (see ADR-0005); it is reversible if cross-provider federation is ever required.
- **The single-binary / NAS-class profile is for development and evaluation only**, not a supported production tier (ADR-0014). Production durability begins at the real multi-node backends with proper failure-domain separation.
- The system provides storage primitives. Application-level concerns (collaborative-editing merge logic, OT/CRDT engines) are out of scope, though the storage primitives needed to support them (atomic append, compare-and-set, change notification) are reserved from the start (ADR-0007).
