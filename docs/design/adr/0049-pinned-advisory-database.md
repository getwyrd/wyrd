---
created: 29.10.2026 13:08
type: adr
status: Proposed
tags:
  - adr
  - ci
  - dependencies
  - security
---
# 0049. Pinned advisory database: the PR gate audits a RustSec database commit recorded in the repository, Dependabot moves it daily, and release and a daily run on `main` audit the newest one

## Context

The dependency wall (ADR-0003 §2, `deny.toml`, `deny-all-features.toml`) fails a pull request when a crate in the dependency graph has a RustSec advisory. Until now every `cargo deny` run downloaded the newest advisory database first. The result of the `gate` check therefore depended on the day it ran, not only on the change under review.

When an advisory is published for a crate that is already on `main`, the `gate` check of every open pull request turns red at once, including pull requests that touch no dependencies. That happened on #979 with RUSTSEC-2026-0414 (`pulldown-cmark`): unrelated work stopped until #981 bumped the crate. The same run on a laptop could also disagree with CI, depending on when each copy of the database was last fetched, which breaks the promise of ADR-0009 that `cargo xtask ci` is the gate's local equivalent.

The advisory check itself must stay. A pull request that adds or changes a dependency onto a version with a known advisory must still fail, and a release must not ship one.

## Decision

### 1. The PR gate audits a pinned database

The commit of the RustSec advisory database (<https://github.com/RustSec/advisory-db>) that the wall audits against is recorded in the repository as the gitlink of the `.github/advisory-db` submodule. Only the gitlink is used; nothing needs to check the submodule out.

Both deny configs set `db-path = "target/advisory-dbs"`. Before the wall runs, `cargo xtask ci` moves cargo-deny's clone there to the pinned commit (fetching only that commit), downloads the crates in `Cargo.lock` with `cargo fetch --locked`, and runs every wall invocation with `--offline`. A pull request now fails the advisory check only when the dependencies it brings, or the pin it moves, carry an advisory. CI and a laptop audit the same database.

`cargo xtask advisories` runs the advisory part of the wall alone, against the pin.

### 2. Dependabot moves the pin

A `gitsubmodule` entry in `.github/dependabot.yml` opens a pull request every day that moves `.github/advisory-db` to the newest database commit. The full gate runs on it. When a new advisory affects Wyrd, that one pull request is red and names the crate, and every other pull request stays green. That pull request is where the fix lands: a bump of the crate, or a justified exception in the deny config, as before. The entry has no cooldown, because the database has no releases to let settle.

### 3. Release and a daily run audit the newest database

`cargo xtask advisories --latest` audits the newest database instead of the pin.

- `release.yml` runs it in an `advisories` job that the `release` job needs, so a release never ships with a known advisory just because the pin has not moved yet.
- `advisories.yml` runs it on `main` every day. When it fails, it opens an issue titled "RustSec advisory affects main", or comments on the open one when it finds an advisory the issue does not name yet. A red pin update therefore cannot sit unnoticed.

## Consequences

- A newly published advisory no longer stops unrelated work. It shows up in one place, the pin update, plus the daily issue.
- An advisory reaches the PR gate up to a day later than before, and only once the pin update merges. Dependabot's security updates are unaffected: they open as soon as an advisory names a crate Wyrd uses.
- The gate's result depends only on the commit under review, so re-running an old pull request gives the same answer.
- `cargo xtask ci` refuses a pin whose commit is more than 90 days old. If the pin updates are left unmerged that long, the gate fails until one merges, which is the intended pressure. xtask checks the commit's date itself: cargo-deny's `maximum-db-staleness` dates the database by its last fetch, and a fresh CI runner fetches the pin every time, so that check alone would never fire.
- `git clone --recursive` or `git submodule update --init` downloads the advisory database into `.github/advisory-db`. Nothing in the build needs it there.
- `xtask/tests/advisory_db.rs` holds the pieces in place: the gitlink and its `.gitmodules` entry, `db-path` in both configs, `--offline` on every wall invocation, and the `--latest` runs in `release.yml` and `advisories.yml`.
