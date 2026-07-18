# Agent instructions — Wyrd

This repository is the source of truth for Wyrd code, design docs, CI, and the
published documentation site. Treat repo policy as part of the work: make
changes in an isolated worktree, keep commits signed, link PRs to issues, and
run the checks that match the surface you changed.

## Worktree discipline

- Do implementation work in a dedicated Git worktree, not in the user's active
  checkout, unless the user explicitly asks otherwise.
- Name worktrees by task or branch, for example `../wyrd-adr-0037` or
  `../wyrd-fix-dserver-timeout`.
- Before editing, inspect `git status -sb` in both the active checkout and the
  worktree you plan to use. Do not overwrite unrelated user changes.
- Keep each worktree scoped to one PR-sized change. If a task splits, create a
  second worktree/branch rather than mixing unrelated files.
- When a dependent PR has landed, fetch `origin main`, rebase the worktree branch
  onto it, resolve conflicts locally, rerun checks, and push with
  `--force-with-lease`.

## Publishing defaults

- Open draft PRs unless the user explicitly asks for ready-for-review.
- Every non-Dependabot PR must reference a real issue in the title or body. Use
  `Closes #N`, `Fixes #N`, or `Refs #N`; prefer closing keywords when the PR
  fully resolves the issue.
- Every commit must carry a DCO sign-off trailer. Use `git commit -s` for new
  commits. If a commit is missing the trailer, fix it before pushing with
  `git commit --amend -s --no-edit`.
- Verify sign-off before final push with `git log -1 --format=full`.
- After a rebase or amend on a published branch, push with
  `git push --force-with-lease`.

## Required PR gates and local actions

- **DCO (`dco`)** — every commit must include a `Signed-off-by:` trailer. This
  applies to docs-only changes too.
- **Issue linkage (`require-issue`)** — every non-Dependabot PR must reference a
  real issue, not only another PR.
- **Rust gate (`ci` / required job: `gate`)** — for code or workflow changes that
  can affect the Rust build, run `cargo xtask ci`. This is the local equivalent
  of the required Rust gate: fmt, clippy with warnings denied, build, tests,
  cargo-deny, cargo-machete, and conformance. Docs-only changes may skip this
  locally because CI's `gate` job handles the docs-only skip.
- **Spell check (`ci` / job: `typos`)** — `cargo xtask ci` runs `typos` as its
  first step (#598), so running the gate covers this. If `typos` is not
  installed locally the gate warns and skips — install it
  (`cargo install typos-cli --locked`) or run `typos` yourself; never pretend
  it ran. This gate is always-on in CI and fires on prose, identifiers,
  comments, and docs alike.
- **Docs check (`docs-check`)** — `cargo xtask ci` runs both steps (#598):
  - `python3 docs/publishing/tools/lint_docs.py`
  - `python3 docs/publishing/tools/render_site.py --check --out <scratch dir>`
    (the gate renders into a per-process temp directory it removes afterwards;
    skipped locally with a warning if the pinned renderer deps are absent).
  Running the render manually, use any scratch path, e.g.
  `--out /tmp/wyrd-docs-build`.
  If the renderer needs the pinned Mermaid asset and network is unavailable,
  rerun with network access or report the exact blocker.
- **Document immutability (`adr-immutability`, renamed to `docs-immutability`
  once ADR-0037 lands)** — never edit an already Accepted ADR. Under ADR-0037,
  also never edit accepted/implemented/superseded/withdrawn proposals or stable
  specs/spec-governed assets; supersede or version instead. Draft/Proposed ->
  Accepted/Stable transitions are allowed because the guard reads base status.
- **CodeQL / security analysis** — GitHub may run CodeQL independently of repo
  workflow files. If it fails, inspect the alert/logs and address the finding or
  document why it needs human/security review.

## Non-gating or scheduled workflows

- **`mutants-pr` / `mutants-full`** — mutation testing is currently
  experimental/non-gating (`continue-on-error`). Do not block ordinary PRs on it,
  but inspect surviving mutants when the change touches correctness logic.
- **`integration-nightly`** — nightly/manual Tier-2 Docker integration. It is not
  a merge gate. For changes to networked D servers or integration harnesses,
  mention whether it needs follow-up observation in the nightly run.
- **`tier1-disk-faults`** — nightly/manual privileged device-mapper fault
  campaign. It is not a merge gate. For disk-fault, scrub, reconstruction, or
  custodian changes, mention whether a Tier-1 follow-up is warranted.
- **`tier2-kill-reconstruct`** — nightly/manual Docker kill-and-reconstruct
  campaign. It is not a merge gate. For reconstruction/custodian durability
  changes, mention whether this scenario should be watched or triggered.
- **`docs`** — publish-on-main workflow for `getwyrd.dev`. For docs publishing
  changes, run the same lint/render commands as `docs-check`; deployment itself
  happens only after merge to `main` or manual dispatch.

## Design documents — pick the right class, change it the right way

- **Pick the class by intent** (`docs/design/README.md` is the map):
  - **ADR** (`docs/design/adr/`) — a settled decision + rationale (ADR-0001).
  - **Proposal** (`docs/design/proposals/`) — an implementation plan / milestone
    scope; ratified draft → accepted (ADR-0037).
  - **Spec** (`docs/design/specs/`) — a normative, versioned compatibility
    contract (bytes / wire / conformance). Spec-first is used **only** for the
    on-disk format; everything else is implementation-first behind versioned
    protobuf (ADR-0002).
  - **Architecture overview** (`docs/design/architecture/`) — living description
    of the current system; update it whenever the system changes.
- **Changing a ratified doc = supersede, never rewrite.** Once an ADR is Accepted,
  a proposal accepted, or a spec stable, its file is frozen (`docs-immutability`
  enforces this). To change it: write a **new** doc carrying `supersedes: <old>`,
  **leave the old file untouched** (do *not* add a `superseded-by:` banner — the
  gate rejects any edit to a frozen file), and record the supersession in the
  **index/README** instead (ADR-0001). Small implementation facts go in living
  architecture or code comments, not back-patched into an accepted proposal.

## Architecture invariants (respect these in code changes)

- **Narrow trait seams** (ADR-0010, ADR-0016): a backend implementation depends
  only on the `traits` crate, never on another concrete; only `server` wires
  concrete backends. New backend = a `<domain>-<backend>` crate.
- **DST is the correctness authority** (ADR-0009): prove correctness in
  deterministic simulation (Tier 0); real-environment tiers complement, never
  replace it. Don't run real external services inside the simulator; promote any
  real-world finding back into a seeded DST regression.
- **Dependency doctrine** (ADR-0003): linked crates must pass the `deny.toml`
  license wall (permissive only; AGPL/BSL/SSPL denied). Deployed *services* are
  judged by control-resilience (forkable, self-hostable, foundation-governed) —
  e.g. OpenBao, not BSL-licensed Vault.
