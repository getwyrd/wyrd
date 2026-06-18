---
created: 13.06.2026 11:57
type: spec
status: DRAFT
version: v1
tags:
  - spec
  - conformance
  - testing
---
# Conformance vectors

> **Status: DRAFT — normative; tracks chunk-format v1 (v0, unstable).** Uses [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords: MUST, SHOULD, MAY.

Test vectors that any implementation of the on-disk format (`../chunk-format/v1.md`) MUST satisfy. They live *with* the spec, in the repository, so the spec and the reference implementation stay honest with each other mechanically: the `chunk-format` crate is run against these vectors in CI (`cargo xtask conformance`).

## Layout (to be populated during Milestone 0–1)

```
conformance/
  vectors/
    v1/
      <name>.fragment      # raw bytes of a valid fragment
      <name>.expected.json # the parsed, verified interpretation a conforming reader must produce
  invalid/
    v1/
      <name>.fragment      # malformed fragments a conforming reader MUST reject
      <name>.reason.txt    # why it must be rejected
```

## Rules

- A conforming **reader** MUST parse and verify every vector under `vectors/` to match its `.expected.json`, and MUST reject every fragment under `invalid/`.
- A conforming **writer** MUST produce fragments the reference reader accepts.
- Adding or changing a vector is part of the strict spec change process (a format version bump). Vectors are append-only within a format version.

These vectors are also how a future non-Rust client (ADR-0002, architecture section 8.6) proves it reads and writes data compatibly, rather than relying on reverse-engineering the reference code.
