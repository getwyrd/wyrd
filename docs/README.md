# Wyrd — documentation

> **Wyrd** is a globally scalable, atomically consistent distributed storage foundation. Home: `github.com/getwyrd` · repo: `getwyrd/wyrd` · site: `getwyrd.dev`. The name and the component-naming scheme are explained in [ADR-0017](design/adr/0017-project-name-and-norn-scheme.md).

This repository is the **single source of truth** for Wyrd's documentation. It is authored in Markdown — editable in Obsidian, Git, or any editor — and published as a static site to `getwyrd.dev`.

## Where things live

| Path | What it is |
|------|------------|
| [`design/`](design/README.md) | **Start here.** The documentation itself — architecture, specifications, decision records, and proposals — organized into four document classes with different change processes. |
| [`index.yml`](index.yml) | The `getwyrd.dev` landing page as structured content (hero, sections, props, Norns) — no markup, rendered to the site root `/`. |
| [`publishing/`](https://github.com/getwyrd/wyrd/tree/main/docs/publishing) | The build/publish tooling (`render_site.py`) that renders this tree to the static site, plus the site's stylesheet and page templates. Not part of the published documentation. |
| [`NAME.md`](https://github.com/getwyrd/wyrd/blob/main/docs/NAME.md) | Why the project is called "Wyrd." |

New here? Read [the design documentation overview](design/README.md) for how the docs are organized and the recommended reading order for newcomers.
