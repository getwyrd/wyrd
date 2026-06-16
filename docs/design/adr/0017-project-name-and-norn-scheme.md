---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - naming
---
# 0017. Project name and the Norn component scheme

## Context

The project needed a name. The constraints that emerged: a Germanic root (for etymological consistency across any component-naming scheme), a meaning that fits a durable storage system, a short and hard-edged word that types cleanly as a binary/crate/identifier, and — the binding practical constraint — availability across the namespaces that matter (GitHub org, crates.io, a domain).

A long search ruled out many evocative candidates precisely because the good ones were already claimed (Aleph → Aleph Alpha; Vellum → vellum.ai; Argus → multiple monitoring projects; Yggdrasil → the IPv6 mesh network; Tholos → a crypto-custody company; Angreal → an existing Rust/Python tool). The bare word `wyrd` itself was taken on the GitHub org, the `.io` domain, and crates.io.

## Decision

The project is named **Wyrd** (Old English, "fate — the woven web of what has been, what is, and what is owed"; same Proto-Germanic root *wurðiz* as Old Norse *Urðr*). Wyrd names the system as a whole *and* the concept of its total woven state: committed data + in-flight writes + outstanding obligations, seen as one consistent picture. *The Norns weave the Wyrd.*

The conceptually load-bearing components are named for the three Norns, mapped onto the system's relationship with time:

- **Urth** (*Urðr*, "what has become") — the durable committed record: the metadata store and on-disk truth.
- **Verdandi** (*Verðandi*, "what is becoming") — the write and commit path: the client library and commit protocol.
- **Skuld** (*Skuld*, "what is owed / what shall be") — pending work and reconciliation: the replication queue, repair backlog, and unsatisfied policy. Skuld's name literally means *debt*, which is exactly what this layer tracks — the system's outstanding obligations.

Lower-level mechanical parts (D servers, gateways, etc.) keep plain descriptive names. The Norn scheme marks the conceptually load-bearing parts only; it is a clarifying map, not decoration, and **Wyrd must not become a single "god" component** — it is the name of the system and its total state, while the actual work is done by the Norns and the mechanical layers.

### Namespace identity

`wyrd` was unavailable as a bare GitHub org, `.io` domain, and crate. The resolved identity keeps the project name "Wyrd" everywhere it is spoken or read, using the product-style `get-` handle only in the URL:

- Project name: **Wyrd**
- GitHub org: `github.com/getwyrd`
- Repository: `github.com/getwyrd/wyrd` (the repo itself is named `wyrd`)
- Domain: `getwyrd.dev`
- Primary crate: `wyrd-core` (the bare `wyrd` crate being taken); additional crates follow `wyrd-*` per the workspace split in ADR-0016.

## Consequences

- The name satisfies every constraint: Germanic root, fitting meaning (a keeper of what has been woven and laid down), short and hard-edged, and now available across org/domain/crate via the `getwyrd` handle and `wyrd-*` crate names.
- The Norn scheme gives the codebase a coherent, self-explaining naming convention: each component name states its relationship to time and the data lifecycle, and the scheme is etymologically consistent (all Germanic; Wyrd and Urth are the same root down the English and Norse branches).
- The name is, deliberately, a homophone of *weird* — embraced, given the failure modes one encounters operating distributed storage at scale.
- "Wyrd" reads as a typo of "weird" to those who do not know the etymology; the README origin note addresses this directly rather than hiding it.
- The `get-` prefix appears only in the URL; the binary, the documentation, and everyday reference all use "Wyrd," so the single-word identity is preserved.
