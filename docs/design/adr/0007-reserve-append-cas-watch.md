# 0007. Reserve append / CAS / watch primitives

Date: design phase
Status: Accepted

## Context

A Drive-like product reads and writes whole files. A future collaborative editor
streams small operations against shared documents and needs change notification.
Building the collaboration layer is out of scope, but three storage primitives
decide whether one *can* be built on top, and they are cheap to design in now and
expensive to retrofit.

## Decision

Reserve three primitives in the commit protocol and metadata schema from the
start, as non-goals-with-reserved-seats (not built early, but accommodated):

1. **Atomic append** — commit "extend file with this data iff current length is
   L" (a conditional mutation the commit protocol already supports).
2. **Compare-and-set** — commit iff version matches (the `meta:version` counter
   already present for optimistic concurrency).
3. **Watch / notify** — "tell me when anything under this directory changes," as
   a metadata-layer event stream. The hook must exist in v1; notification
   fan-out at scale can come later.

## Consequences

- The collaborative-editor future is buildable without re-architecting storage.
- Watch also serves Drive sync clients, so it earns its place even before any
  editor exists.
- The commit protocol and schema must accommodate these from Milestone 0
  (section 9), even though the features ship later.
