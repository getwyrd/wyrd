# docs/publishing/tools — documentation publishing

The repository is the single source of truth for documentation. These tools
render the Markdown under `docs/` into the bespoke hand-rolled getwyrd.dev site
and CI publishes it. Nothing here adds or rewrites content; it renders, wires up
navigation, and guards conventions. You author the same Markdown files in
Obsidian, in Git, or in any editor.

## What each piece does

| File | Purpose |
|------|---------|
| `render_site.py` | Renders `docs/**/*.md` → static HTML in `./build`, using `../templates/page.html` for chrome and `../site/` for assets. Renders the landing page (`/`) from `docs/index.yml` through `../templates/home.html`. Rewrites relative `.md`/`.mermaid` links to output URLs, generates section indexes, renders each `architecture/diagrams/*.mermaid` as a client-side Mermaid page, and (`--check`) fails on any dangling internal link. |
| `lint_docs.py` | Fails the build on Obsidian-only `[[wikilink]]` / `![[embed]]` syntax, and on a normative spec missing its status/stability marker. |
| `../site/` | The stylesheet (`assets/style.css`), `CNAME`, and `.nojekyll`. Copied verbatim into the output. |
| `../templates/page.html` | The page chrome wrapped around every rendered doc: `{{TITLE}}`, `{{DESCRIPTION}}`, `{{DOC_HEADER}}`, `{{CONTENT}}`, `{{SCRIPTS}}`. |
| `../templates/home.html` | The full-bleed landing template (chrome + `{{CONTENT}}`); `render_site.py` fills `{{CONTENT}}` from `docs/index.yml`. |
| `../../../.github/workflows/docs.yml` | Lints, renders, and deploys the whole site to getwyrd.dev `main` on every push to `main` that touches docs. (Lives at the repo root under `.github/workflows/`.) |

## Local build

Two Python dependencies — `markdown-it-py` (the `[linkify]` extra enables
bare-URL autolinking, as on GitHub/Obsidian; the renderer also runs without it,
just no autolinking) and `PyYAML` (reads the landing page's `index.yml`):

```sh
pip install "markdown-it-py[linkify]" PyYAML   # CI pins ==3.0.0 / ==6.0.2
```

Then, from the repo root:

```sh
python3 docs/publishing/tools/lint_docs.py             # optional; CI runs it anyway
python3 docs/publishing/tools/render_site.py --check   # builds ./build, audits links
python3 -m http.server -d build 8000                   # preview at http://localhost:8000
```

`render_site.py` fetches a pinned `mermaid.min.js` once into `./.cache/` (set
`MERMAID_JS` to a local file to skip the download) and copies it into the build;
the published site serves it from `/assets/`, with no runtime CDN. Both scripts
are standalone (no Rust toolchain) so documentation can be published before the
workspace compiles.

## Editing in Obsidian

Open the **`docs/` folder itself** as the vault, so what you edit is what ships —
there is no separate vault to keep in sync. Two settings keep Obsidian output
portable (now persisted in `.obsidian/app.json`):

- **Settings → Files & Links → Use [[Wikilinks]]: off.** Write standard
  `[label](relative/path.md)` links, as the existing docs do. `lint_docs.py`
  will fail the build if a wikilink slips through.
- **New link format: Relative path to file.**

Templater/templates, local graph, and backlinks are all fine — they are
editor-side only and never reach the published site.

The vault was authored with two community plugins:

- **[Templater](https://github.com/SilentVoid13/Templater)** (`templater-obsidian`) — note templating.
- **realclaudian** (`realclaudian`) — Claude integration inside Obsidian.

Their enabled state is tracked in `.obsidian/community-plugins.json`, but the
plugin **code** is git-ignored (`.obsidian/plugins/` — see the root `.gitignore`):
it is large, third-party, and re-installable. Install both from Obsidian's
community-plugins browser to reproduce the authoring setup; neither affects the
published site.

## Generated / build files (git-ignore these)

`render_site.py` regenerates the site on every build, and CI does too, so they
are not committed (already in the root `.gitignore`):

```gitignore
# Generated docs-site output + build cache
/build/
/.cache/
```

The diagram `.mermaid` sources stay tracked — they are the authority; their
rendered HTML pages are produced into `./build`.

## CI secret and target

`docs.yml` deploys to the getwyrd.dev repo named in its `env:` block
(`DOCS_REPO`, `SITE_DOMAIN`). Deploy keys must be enabled for the org (Settings →
Repository → Deploy keys → Enabled); then add a repository secret
`DOCS_DEPLOY_KEY` — the private half of an ed25519 keypair whose public half is
registered, with write access, as a deploy key on the getwyrd.dev repo. The
workflow overwrites getwyrd.dev's `main` branch (its Pages source) with the built
site; getwyrd.dev is a generated mirror, not hand-edited.
