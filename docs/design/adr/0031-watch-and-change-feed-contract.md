---
created: 22.06.2026 20:25
type: adr
status: Proposed
tags:
  - adr
  - watch
  - change-feed
  - sync
  - consistency
  - control-plane
---
# 0031. Watch / change-feed contract

## Context

ADR-0007 reserved three primitives — append, CAS, and watch — without specifying them, so the cost
of supporting them later was kept low without committing to a shape. The Drive-product feature
review made the watch primitive the most pressing of the three: an efficient **change feed** is the
engine of every sync client. A desktop or mobile client must learn "what changed since I last
synced" by reading a bounded delta, not by re-listing the namespace; the WebDAV / Drive surface
already advertises "sync" (section 5, L1) with no mechanism behind it. ADR-0018 §3 further
constrained the eventual watch: its interface MUST NOT bake in per-subscription cost or fan-out caps
(a hyperscale identity consumer may push an urgent change to many edge nodes), and it noted that
urgent high-fan-out propagation MAY ride a dedicated transport (NATS, the L3 transport of ADR-0027)
with Wyrd's watch as the durable-truth backstop. The shape is thus partly constrained but never
specified. This ADR de-reserves watch and pins its contract; append and CAS remain reserved.

## Decision

1. **The change feed is a versioned delta over the namespace.** A client presents a namespace
   version — the `meta:version` fence already reserved by ADR-0015 — and receives the *ordered* set
   of changes since that version (create / rename / delete / ACL-change entries) up to the current
   version. The feed rides the existing global commit version; it introduces **no new ordering
   authority**, so it inherits the consistency contract rather than competing with it.

2. **Two delivery modes; the pull feed is the source of truth.** A **pull** mode answers "changes
   since version V" and is durable and authoritative. A **push** mode streams a subscription for low
   latency, fanned out over the NATS transport (ADR-0027, ADR-0018 §3); push is an optimisation, and
   a client may always fall back to the pull feed, which is the correctness backstop. The system MUST
   NOT depend on push delivery for correctness.

3. **Scoped and authorised.** A watch is scoped to a subtree (a tenant, a directory) and authorised
   against the L2 ACLs, so a client sees only changes to entries it may read — tenant isolation
   (ADR-0022) holds on the change feed as on direct reads.

4. **A bounded history window.** The change feed retains history for a bounded window (the
   retention/compaction model of ADR-0018 §1's snapshot mode); a client that has fallen further
   behind than the window must perform a full re-list to re-sync, then resume from the current
   version. The window is a per-deployment policy bound, not unbounded history.

5. **No per-subscription cost cap baked in (ADR-0018 §3).** The contract expresses fan-out as a
   transport concern (push over NATS), so scaling to a large number of subscribers is a deployment
   property of the transport, not a limit frozen into the API.

This refines ADR-0007 for the watch primitive specifically; append and CAS stay reserved under
ADR-0007 until their own consumers force the same specification.

## Consequences

- Sync clients become efficient: a delta keyed to the version fence replaces full-namespace
  re-listing, which is what a Drive-class product needs from the foundation.
- The high-fan-out identity-consumer requirement (ADR-0018) is satisfied without baking limits into
  the contract — fan-out lives in the transport (ADR-0027), the durable truth in the pull feed.
- Tying the feed to the existing `meta:version` fence means no new consistency machinery: the change
  feed is "the version counter, made enumerable", and is simulatable under DST (ADR-0009) against the
  consistency contract (ADR-0015).
- Cost: L2 must retain a bounded change history (a compaction/retention obligation on the namespace
  store, ADR-0020), and the push path depends on the NATS transport (ADR-0027); a client beyond the
  window pays a full re-list.
- Refines ADR-0007 (watch only); depends on ADR-0015 (the version fence), ADR-0018 (fan-out and
  snapshot retention), ADR-0020 (namespace-store history), and ADR-0027 (push transport). Status
  Proposed.
