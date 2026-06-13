# Wyrd — documentation

> **Wyrd** is a globally scalable, atomically consistent distributed storage
> foundation. Home: `github.com/getwyrd` · repo: `getwyrd/wyrd` · site:
> `getwyrd.com`. The name and the component-naming scheme are explained in
> [ADR-0017](adr/0017-project-name-and-norn-scheme.md).

This project's documentation is organized into four distinct classes, each with
its own purpose, audience, and change process. The distinction matters because
parts of this documentation are not merely *descriptive* — they are *normative*.
An independent implementation of the on-disk format must conform to a written
specification, so that class of document carries a stricter change process than
the rest.

## The four document classes

| Class | Location | Nature | Change process |
|-------|----------|--------|----------------|
| 1. Specifications | `specs/` | Normative, versioned | Strict; a version bump is an ecosystem event |
| 2. Architecture overview | `architecture/` | Descriptive, living | Edited continuously; always describes the current system |
| 3. Decision records (ADRs) | `adr/` | Immutable history | Append-only; superseded, never edited |
| 4. Enhancement proposals | `proposals/` | The change process | Draft → accepted → implemented |

### 1. Specifications (`specs/`)

The normative core. Currently this is the on-disk chunk/fragment format — the
one artifact that must remain stable because **data outlives software**. A
provider with petabytes written under format `v1` must be able to read it with
software written years later. Specs use RFC 2119 language (MUST / SHOULD / MAY)
and ship with conformance test vectors in `specs/conformance/`.

Everything else (wire protocols, component interfaces) is implementation-first
with versioned protobuf, not spec-first. See ADR-0002 for the reasoning.

### 2. Architecture overview (`architecture/`)

A trimmed [arc42](https://arc42.org/)-derived structure plus
[C4](https://c4model.com/) diagrams. This is the living description of the
system as it currently is. It is edited continuously and should never lag the
code; "update the architecture doc" is a legitimate merge requirement on a PR
that changes structure.

### 3. Architecture Decision Records (`adr/`)

Short, numbered, immutable records of *why* a decision was made. They exist so
the same debate ("why not Ceph?", "why client-side erasure coding?") is not
relitigated in every new issue. An ADR is never edited after acceptance; it is
superseded by a later ADR that references it. Format: a lightweight
[Nygard-style](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
template. See `adr/0001-record-architecture-decisions.md`.

### 4. Enhancement proposals (`proposals/`)

Once something is implemented, architectural change flows through proposals
(modeled on Kubernetes KEPs and Rust RFCs). A proposal moves from `draft/` to
`accepted/` and carries motivation, design, alternatives, and graduation
criteria. Use `proposals/template.md` to start one. Early in the project this is
deliberately lightweight; the discipline matters more than the ceremony.

## Conventions

- **Diagrams as code.** All diagrams live in `architecture/diagrams/` as
  Mermaid or D2 source, never as binary exports. Diagrams must be diffable and
  changeable in a PR. Exported images from drawing tools rot within months.
- **Docs live with code.** Documentation is in the same repository as the code
  it describes and is reviewed in the same pull requests.
- **ADR-first habit.** When a design question is settled, the output artifact is
  an ADR, not a wiki paragraph or a Slack message.

## Reading order for newcomers

1. `architecture/01-introduction-and-goals.md` — what this is and why.
2. `architecture/05-building-block-view.md` — the layer model (L1–L5).
3. `adr/` — skim the index below; the early ADRs capture the foundational
   decisions and are the fastest way to understand the *why* behind the system.
4. `architecture/09-build-order-and-roadmap.md` — where to start contributing.

See `adr/README.md` for the full ADR index.
