# 0018. Reserve hooks for a hyperscale identity consumer

Date: design phase
Status: Accepted

## Context

Wyrd was designed around one major consumer: a Drive-like storage product and
its object-store and (later) collaborative-editor surfaces. A second major
consumer is now anticipated — a globally scalable identity system, built as a
**separate project** that depends on Wyrd through its existing traits, not by
modifying Wyrd's core.

That consumer stresses Wyrd very differently from storage. Identity objects
(principals, credentials, attributes, relationship tuples) are all kilobytes:
they never chunk or erasure-code, and so exercise *none* of the data path
(client EC, D servers, direct fragment writes). They exercise only the metadata
tier, the commit point, the consistency contract, the placement service, and the
watch hook. Identity is also read-dominated and globally distributed, where the
storage workload is write-once and home-region-centric.

The identity system keeps the bulk of its scale off Wyrd by design: most
authentication and authorization is stateless cryptographic verification at the
edge, never a read of any shared store. Wyrd's role is therefore the small,
strongly-consistent core (issuance, uniqueness, freshness-critical state) plus
the durable event truth — not the hot path. Keeping verification off the core is
the identity layer's responsibility and is recorded in *that* project's decision
records, not here.

Several hooks this consumer needs are cheap to reserve in Wyrd's design now and
expensive to retrofit once the metadata schema and consistency contract have set
— the same "reserve the seat now, build later" reasoning as ADR-0007 (append /
CAS / watch) and ADR-0015 (the version-fence). This ADR records those
reservations. It **builds on and references ADR-0007 and ADR-0015; it does not
edit or supersede them.**

## Decision

Reserve the following in Wyrd's design as accommodations for a future identity
consumer. None adds anything to the data path; each is a general improvement that
stands on its own merits.

1. **The version-fence is specified as an externally-carryable token.** ADR-0015
   reserves `meta:version` and Option C (version-fenced reads), framed there
   chiefly as a failover mechanism. When specified, the fence MUST also be a
   first-class value in the read API: issued to a caller, carried by that caller
   across services, and honored by any replica as "serve me only if you are
   caught up to here" — the consistency-token ("zookie") shape an external
   authorization plane needs, not solely an internal failover detail.

2. **Placement policy is expressible over a namespace subtree, not only
   per-file.** The placement service (L2) keeps a single mechanism, but its
   policy language MUST be able to scope a whole subtree (a tenant) to a region
   or failure-domain set, so "pin this tenant's data to eu-west" is one policy
   and home-zone authority for everything beneath it follows. This also serves
   the data-sovereignty goal directly.

3. **The reserved watch primitive is not capped at Drive-sync magnitudes.** When
   the ADR-0007 watch/notify hook is specified, its interface MUST NOT bake in
   per-subscription cost or fan-out-topology assumptions that prevent scaling to
   large fan-out (e.g. an urgent change pushed to many edge verification nodes
   within a bounded window). The division of labour is left open: urgent
   high-fan-out propagation MAY ride a dedicated transport (NATS, already present
   for L3) with Wyrd's watch as the durable-truth backstop.

4. **Metadata-op rate is a first-class scaling target.** For the identity
   workload the metadata tier's small-object op-rate *is* the entire scaling
   dimension (cf. quality scenario Q7, where for storage it is one workload among
   several). The metadata tier's sharding strategy MUST be able to scale on
   per-tenant op-rate, not only on dataset size, and inline-path latency is a
   benchmarked target rather than an afterthought.

### Explicitly not decided here

- The boundary between Wyrd's append-only audit log and an identity CQRS event
  stream (ordered, replayable, projected). These are different primitives; which
  side owns ordering and replay is **open**, and is settled when the identity
  layer is designed.
- Whether authorization is a Wyrd trait with multiple implementations or lives
  entirely in the identity project. **Open.**
- The build-order consequence below (whether Option C is pulled forward) is
  noted, not scheduled here.

## Consequences

- The version-fence reservation gains read-API surface when built (a token in
  the read path). Accepted; the carryable token is useful to Wyrd independent of
  identity.
- The identity workload will want fenced local-replica reads (Option C) far
  sooner than the storage workload does, because it is read-dominated and
  globally distributed. This **may pull Option C forward in the build order**
  (section 9); recorded here so the reprioritisation is not a surprise.
- These reservations are justified only because each is general and cheap. Any
  accommodation that makes sense *only* for identity belongs in the identity
  project's crates as a consumer of Wyrd's traits — not in Wyrd's core. Wyrd must
  not grow toward a "god component" to serve a second consumer (ADR-0017).
- Nothing here touches the chunk / EC / D-server data path, confirming that Wyrd
  serves the identity system entirely through its metadata tier, commit point,
  consistency contract, placement policy, and watch hook.
