# Documentation publishing

> **Status: pipeline in place.** This is *descriptive tooling documentation*, not a normative specification — it does not belong in `design/specs/`. It describes how the Markdown under `docs/` is rendered to the getwyrd.dev site.

## Overview

This repository (`getwyrd/wyrd`, the `docs/` tree) is the **single source of truth** for project documentation, authored in Markdown and editable in Obsidian, Git, or any editor. The pipeline renders that Markdown into a bespoke, hand-rolled static site — the paper/Norn aesthetic — and CI publishes the whole site to the **getwyrd.dev** repo.

```
docs/**/*.md   →   render_site.py + page.html + style.css   →   static HTML   →   getwyrd.dev
(source of truth)  (render + chrome, themed)                    (./build)         (published)
```

**getwyrd.dev is a generated mirror.** It is no longer hand-edited: the landing page is authored as structured content (`docs/index.yml` — words only, no markup), and the stylesheet and page templates live here in `publishing/site/` and `publishing/templates/`; CI overwrites getwyrd.dev's `main` branch with the built site. There is a single web property — the apex domain `getwyrd.dev` — serving both the landing page (`/`) and the rendered docs (`/architecture/`, `/adr/`, `/specs/`, `/proposals/`, `/name.html`).

## Layout

```
<repo root>/
├── .github/workflows/
│   └── docs.yml              ← CI: lint → render → publish to getwyrd.dev (GitHub reads it here)
└── docs/
    ├── README.md             ← GitHub-facing index of the docs/ folder (not published)
    ├── index.yml             ← the landing page as structured content (no markup)  → /
    ├── NAME.md               ← the name essay              → /name.html
    ├── design/               ← the four document classes   → /architecture/ /adr/ /specs/ /proposals/
    │   ├── README.md         ← reader-facing docs hub       → /docs/
    │   ├── architecture/
    │   │   └── diagrams/*.mermaid  ← diagrams as code       → /architecture/diagrams/<stem>.html
    │   ├── specs/
    │   ├── adr/
    │   └── proposals/
    └── publishing/           ← build tooling + site inputs (excluded from the published docs)
        ├── site/
        │   ├── assets/style.css
        │   ├── CNAME         ← getwyrd.dev
        │   └── .nojekyll
        ├── templates/
        │   ├── page.html     ← chrome wrapped around every rendered doc ({{TITLE}}, {{CONTENT}}, …)
        │   └── home.html     ← full-bleed landing template, filled from index.yml ({{TITLE}}, {{CONTENT}}, …)
        └── tools/
            ├── render_site.py   ← renders docs/ → ./build, copies site/, audits links
            └── lint_docs.py     ← guards: no Obsidian [[wikilinks]]; specs carry status markers
```

## How a page is built

`render_site.py` walks the four document classes plus `NAME.md` and the design hub, computes each source's output URL, renders Markdown to HTML, **rewrites relative `.md`/`.mermaid` links to their output URLs**, and wraps the result in `templates/page.html`. The landing page is different: it renders to `/` from the structured content in `docs/index.yml` (hero, sections, props, Norns — only the words) through the full-bleed `templates/home.html`; inline Markdown in its text fields is supported. Section index pages (`/architecture/`, `/specs/`, …) are generated; `adr/` and `proposals/` use their `README.md`. Each `architecture/diagrams/*.mermaid` becomes a page that renders client-side via a vendored `mermaid.min.js` (fetched at build time into `./build`, never committed).

A `--check` pass then fails the build on any dangling internal link — the correctness guarantee that justifies the heavy cross-linking in the docs (ADR references, spec back-links, proposal links).

See [tools/README.md](tools/README.md) for build and deploy instructions.

## Two properties, do not conflate

| Property | What it is | Source |
|----------|-----------|--------|
| `getwyrd.dev` | The whole site: landing + docs | this `docs/` tree via `render_site.py` (generated mirror) |
| `github.com/getwyrd/wyrd` | The repository | source of truth for everything, including the site |

## Open items

- [x] Renderer (`render_site.py`) + lint (`lint_docs.py`) in `publishing/tools/`.
- [x] Site inputs (landing, stylesheet, page template) under `publishing/`.
- [x] `docs.yml` renders and publishes the whole site to getwyrd.dev `main`.
- [ ] CI secret: `DOCS_DEPLOY_KEY` (write-enabled deploy key on getwyrd.dev).
- [ ] Optional: richer per-section navigation / a sidebar in `page.html`.
