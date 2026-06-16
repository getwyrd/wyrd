---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - spec
  - on-disk-format
---
# 0002. Spec-first for the on-disk format only

## Context

A spec-first posture (write normative specifications, make implementations conform) adds rigor but costs weeks of design work producing documents and conformance vectors before there is runnable code — hard on momentum for a project that needs early contributors. The strongest argument for spec-first was enabling *independent implementations by other organizations*. That requirement was removed by the decision to support only single-provider, closed federation (ADR-0005): one codebase serves one provider's zones.

However, one concern survives that change: **data outlives software**. A provider with petabytes written under format `v1` must be able to read it with software written years later, and the requirement to be able to replace the underlying storage system makes format-level stability the migration anchor.

## Decision

Spec-first applies to the **on-disk chunk/fragment format only**. It is normative (RFC 2119 language), versioned, and ships with conformance test vectors in `docs/specs/conformance/`. The `chunk-format` crate is verified against those vectors in CI.

Everything else — wire protocols, component interfaces — is implementation-first, evolving behind versioned protobuf with disciplined interface versioning.

## Consequences

- Best momentum profile: runnable code early, rigor where it is permanent.
- The format is the one place an independent reader (e.g. a future migration tool or a non-Rust client) can rely on a written contract.
- The format must record the EC scheme per chunk (ADR-0008) so mixed-era data remains readable.
- If cross-provider federation is ever added (reversing ADR-0005), the wire protocols would need promotion to spec-first; that cost is noted in ADR-0005.
