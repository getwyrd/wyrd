# 0003. Apache 2.0 license and DCO sign-off

Date: design phase
Status: Accepted

## Context

The target users are mid-sized cloud providers — commercial entities that must
be legally able to deploy, modify, and build products on the system. AGPL (the
MinIO path) squeezes exactly these users and triggers fork-and-flee dynamics. An
infrastructure foundation intended for broad adoption needs a permissive license.

Contribution provenance must be tracked, but a full CLA imposes friction that
deters drive-by contributors.

## Decision

License under **Apache 2.0**. Track provenance with **DCO** (Developer
Certificate of Origin) sign-offs on every commit, not a CLA. Ship `LICENSE`,
`NOTICE`, and a `SECURITY.md` from the first commit.

## Consequences

- Commercial providers can adopt without legal friction; patent grant included.
- Lower contribution friction than a CLA, sufficient provenance for an
  Apache-2.0 project.
- The MinIO community (wounded by AGPL + feature-stripping) becomes an
  addressable audience.
- Trademark/governance (project vs. foundation trajectory) is deferred; the
  license is not.
