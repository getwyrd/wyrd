---
created: 22.06.2026 20:20
type: adr
status: Proposed
tags:
  - adr
  - security
  - supply-chain
  - build
  - release
---
# 0030. Build and release integrity

## Context

The project guards its **inbound** supply chain well: permissive-license and RUSTSEC-advisory walls
via `cargo-deny`, unused-dependency checks, and DCO sign-off on every commit (ADR-0003), all
enforced in CI. The **outbound** supply chain — what a user actually downloads and runs — is
unguarded. There are no signed release artifacts, no build provenance, and GitHub Actions are
referenced by mutable tags rather than pinned commits. A compromised CI run, a hijacked Action tag,
or a malicious release would ship a backdoored static binary or OCI image that a user has no way to
detect. The threat model lists "Build / release-pipeline compromise" (section 14.5) and marks it
open (section 14.9). For a system whose entire pitch is verifiable correctness, an unverifiable
build is a conspicuous gap.

## Decision

1. **Signed releases.** Every release artifact — the static binary and the OCI image — is signed,
   using **Sigstore / cosign keyless** (OIDC-based, no long-lived signing key to guard), and the
   verification steps are documented so a user can confirm an artifact came from this project's
   release pipeline before running it.

2. **Build provenance (SLSA).** The release workflow emits a **SLSA provenance attestation** tying
   each artifact to the exact source commit and build environment, so the path from tagged source to
   shipped binary is independently verifiable — the outbound complement to the inbound advisory wall.

3. **Pinned Actions.** All GitHub Actions are pinned by full commit **SHA**, not by tag, so a
   compromised or retagged Action cannot inject code into a build; Dependabot (already configured)
   keeps the pins current as a tracked change rather than a silent drift.

4. **Protected, signed source.** The existing DCO (ADR-0003), `require-issue`, and `adr-immutability`
   gates, plus branch protection on `main`, are the source-integrity baseline; commit signing MAY be
   added for release-tagging commits.

5. **Reproducibility as a goal.** The pinned Rust toolchain (`rust-toolchain.toml`) and committed
   `Cargo.lock` already push toward reproducible builds; provenance is most valuable when a third
   party can rebuild and match, so reproducibility is a tracked goal, not yet a hard gate.

These land with the release machinery, not before there is something to release; the decision is
recorded now so the release workflow is built signed from the start rather than retrofitted.

## Consequences

- The outbound supply chain reaches parity with the inbound rigor: users can verify provenance and
  signature of exactly what they run, consistent with correctness-as-the-headline-feature.
- Keyless signing avoids a long-lived key as a new secret to protect; the cost is a dependency on the
  Sigstore transparency infrastructure (itself open and widely used).
- Pinning Actions by SHA trades a little manual/Dependabot upkeep for removing the mutable-tag attack
  surface — the same "tracked change, not surprise" discipline as the dependency wall.
- Closes the section 14.9 build/release-integrity open item and complements ADR-0003 (inbound
  supply chain). Status Proposed; lands with the first real release artifacts.
