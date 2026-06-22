# Security policy

Wyrd is pre-release software (early implementation) and is **not yet deployable**
— it carries no durability or security promise at this stage. We still want to
hear about vulnerabilities early, including in the design itself.

The system's adversarial **threat model** — assets, trust boundaries, the
D-server-compromise blast radius, and the storage attack catalog — is maintained
with the design documentation: see [architecture §14](docs/design/architecture/14-threat-model.md),
and the reliability decisions it leans on (clock and time-source trust,
[ADR-0024](docs/design/adr/0024-clock-and-time-source-trust.md); internal
service-to-service trust, [ADR-0025](docs/design/adr/0025-internal-service-to-service-trust.md)).

As an open-source project, Wyrd relies on **coordinated disclosure** and free,
continuous tooling rather than commissioned audits: `cargo-deny` (license +
RUSTSEC advisory wall), Dependabot, and `cargo-mutants` (mutation testing) run
today, with CodeQL static analysis and coverage-guided fuzzing of the
on-disk-format reader planned (architecture §14.9). Security researchers are
welcome: reporters are credited, and we will work with you on a
coordinated-disclosure timeline.

**Safe harbor.** We will not pursue or support legal action against researchers
acting in good faith under this policy — accessing only their own data, avoiding
privacy violations, service disruption, and data destruction, and giving us
reasonable time to remediate before public disclosure.

## Reporting a vulnerability

Please report suspected vulnerabilities **privately**, not via a public issue or
pull request. Email **policy@getwyrd.dev** with:

- a description of the issue and its impact,
- the steps or proof-of-concept needed to reproduce it, and
- the affected version or commit.

We will acknowledge your report and keep you informed as we investigate. Please
give us a reasonable opportunity to address the issue before any public
disclosure.

## Scope

Until the format and protocol are stamped stable (the `chunk-format` spec is
explicitly `v0/unstable`), behavioural and on-disk-format changes are expected
and are not in themselves security issues.
