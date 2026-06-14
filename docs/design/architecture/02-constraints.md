# 2. Constraints

> Living document.

## 2.1 Technical constraints

| Constraint | Source | Consequence |
|------------|--------|-------------|
| Implementation language is Rust | ADR-0004 | Correctness-as-differentiator; access to deterministic simulation testing; no embedded-etcd convenience |
| License is Apache 2.0 | ADR-0003 | Permissive; suitable for an infrastructure standard adopted by commercial providers |
| Single binary must run on NAS-class hardware (e.g. Synology) | Requirement | Metadata store, chunk store, and coordination must all have embeddable, single-process backends |
| Must scale to a multi-region provider fleet | Requirement | Same components must have distributed backends behind the same interfaces |
| On-disk format must be stable across software versions | ADR-0002 | The chunk/fragment format is spec-first and versioned; data carries its format version |
| Components must tolerate version skew with neighbors | Operability | A half-upgraded fleet is the normal state during a rolling upgrade; wire interfaces are versioned protobuf |

## 2.2 Organizational constraints

- **Small founding team.** Favors a monorepo, a single language, off-the-shelf
  components for the genuinely hard distributed-systems primitives (consensus,
  transactions, coordination), and a build order that produces a working system
  early.
- **Open-source, contributor-dependent.** Favors excellent onboarding docs,
  well-marked good-first-issues in non-hot-path code, and a clear contribution
  roadmap (the build order in section 9).
- **Provenance via DCO**, not a CLA (ADR-0003). Every commit is signed off.

## 2.3 Conventions

- Diagrams as code (Mermaid / D2), reviewed in PRs.
- Documentation lives in-repo and is reviewed alongside code.
- Architectural decisions are recorded as ADRs at the time they are made.
- The deterministic-simulation-testing harness is a first-class dependency, not
  a test-time afterthought (see ADR-0009).
