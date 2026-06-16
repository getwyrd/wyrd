---
created: 16.06.2026 23:23
type: index
tags:
  - index
---
# Templates

Starting points for the three *authored* design-document classes. **Copy** the
relevant file out of this folder — do not edit these in place — rename it, and
fill it in. This folder is authoring scaffolding; it is not published to the
site.

| Template | Use it for | Copy it to |
|----------|------------|-----------|
| `adr.md` | An Architecture Decision Record | `../adr/NNNN-short-title.md` (next free number; add a row to `../adr/README.md`) |
| `proposal.md` | An enhancement proposal | `../proposals/draft/NNNN-short-title.md` |
| `architecture.md` | An arc42 architecture section | `../architecture/NN-section-name.md` |

The fourth class, specifications (`../specs/`), is normative and versioned and
does not use a fill-in template; see `../README.md` for how the classes differ
and which one a given change belongs in.
