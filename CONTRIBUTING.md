# Contributing to Wyrd

Thank you for your interest in contributing to Wyrd.

Before contributing, please read the project's Governance documentation and Security Policy.

- Governance: docs/governance/
- Security Policy: SECURITY.md

## Development workflow

1. Fork the repository.
2. Create a feature branch for your changes.
3. Make your changes and test them locally.
4. Push the branch to your fork.
5. Open a pull request against the main branch.

## Pull request requirements

### Reference an issue

Every pull request must reference a real issue in this repository.

Examples:

* Closes #123
* Refs #123

Pull requests that do not reference an issue will fail the required checks.

### DCO sign-off

Every commit must contain a Developer Certificate of Origin (DCO) sign-off.

The easiest way is to create commits using:

```bash
git commit -s -m "your commit message"
```

This automatically adds the required Signed-off-by line.

## Running checks

Before opening a pull request, run:

```bash
cargo xtask ci
```

This is the same gate that runs in CI, including the prose gates (`typos` and
the docs lint/render check); external tools it needs (`typos`,
`cargo-machete`, `cargo-deny`, the docs renderer deps) are warn-and-skip when
not installed locally, so install them for full parity.

Optional Tier-2 integration tests:

```bash
cargo xtask integration
```

Note: Tier-2 integration tests require Docker.

## Subsystems with development stood down

A subsystem here is either **actively developed** or **stood down**. "Stood down" means
the core team is no longer adding to or hardening it, but it is **kept in the tree,
kept compiling, and left open for anyone to continue** — the opposite of deleted. If you
want a substantial, self-contained piece of work with a clear backlog, this is where to
look.

### TiKV metadata backend — retained fallback, stood down (#443)

FoundationDB is the production `MetadataStore` backend (ADR-0042, which supersedes
ADR-0008; it passed the M4 fault + contention battery in #442). TiKV is **retained as a
fallback and is not going away**: `crates/metadata-tikv`, the `tikv` feature, the `Tikv`
CLI backend variant and the `deploy/tikv-*` stacks all stay, and CI keeps the `tikv`
feature compiling (the `tikv` job — that build-only bar is what makes "continuable" true
rather than aspirational).

What that means for a contributor:

- **The backlog is open.** The *Metadata Store TiKV* milestone holds the continuation
  work — client pin / API shape (#260), optimistic-vs-pessimistic transactions (#259),
  async-commit/1PC parity (#418), the nightly conformance workflow + deny-audited graph
  (#420), the `tikv-client` go/no-go umbrella (#435), the `wyrd:tikv` image (#471), and
  the red Tier-1 fault battery (#537). None of these are closed, and nobody is working
  them. Pick one up.
- **Know what you are picking up.** `tikv-client` 0.4.0 is abandoned upstream and carries
  unpatched advisories in its TLS stack, including a live DoS in CRL parsing
  (RUSTSEC-2026-0104, high). The exposure boundary — why the shipped artifact is
  unaffected, and why that is *not* a claim the TiKV backend is safe — is recorded in
  `deny-all-features.toml` (#543). The Tier-1 fault battery is also currently red (#537), for a
  pre-existing reason unrelated to the FoundationDB work.
- **The bar for changes is unchanged.** The shared `metadata-conformance` suite (ADR-0016)
  and the same review gates apply. Standing down is not a licence to lower the bar; it is
  a statement about who is doing the work.

## Reporting bugs

Please use the issue templates provided by the repository.

## Security issues

Do not report security vulnerabilities through public issues.

See SECURITY.md for responsible disclosure instructions.
