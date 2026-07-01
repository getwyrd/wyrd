---
created: 13.06.2026 11:57
type: index
tags:
  - proposal
---
# Enhancement proposals

KEP/RFC-style proposals for architectural change after initial implementation. Start one by copying `../templates/proposal.md` into `draft/`. Accepted proposals move to `accepted/`. See `../README.md` for how this class relates to ADRs and the living architecture overview.

## Index

This index is the authoritative record of each proposal's current status and any supersession link (ADR-0038): an accepted proposal's own file is never edited, so a later supersession is recorded **here**, not on the frozen file. The lifecycle and immutability rules are ADR-0037.

| # | Title | Status |
|---|-------|--------|
| [0001](accepted/0001-milestone-0-walking-skeleton.md) | Milestone 0 — the walking skeleton | accepted¹ |
| [0002](accepted/0002-implementation-arc.md) | The implementation arc | **superseded by 0013** |
| [0003](accepted/0003-milestone-1-erasure-coding.md) | Milestone 1 — erasure coding | accepted¹ |
| [0004](accepted/0004-milestone-2-networked-d-servers.md) | Milestone 2 — networked D servers | accepted¹ |
| [0005](accepted/0005-milestone-3-custodians.md) | Milestone 3 — custodians | accepted¹ |
| [0006](draft/0006-object-lifecycle-and-retention.md) | Object lifecycle and retention (versioning, trash, WORM) | draft (adoption: [#370](https://github.com/getwyrd/wyrd/issues/370)) |
| [0007](accepted/0007-milestone-4-production-metadata-backend.md) | Milestone 4 — production metadata backend (TiKV) | accepted (tracker [#201](https://github.com/getwyrd/wyrd/issues/201)) |
| [0008](draft/0008-management-and-administration.md) | Management and administration (Milestone 8) | draft (slicing: [#369](https://github.com/getwyrd/wyrd/issues/369)) |
| [0009](draft/0009-d-server-performance.md) | D-server performance | draft — standing program, attaches from M4 |
| [0010](draft/0010-observability-floor-for-first-deployment.md) | The observability floor | draft — gates the M4 campaign ([#366](https://github.com/getwyrd/wyrd/issues/366)) |
| [0011](accepted/0011-milestone-5-internal-ca-step-ca.md) | Milestone 5 — internal CA (step-ca) | accepted (tracker [#374](https://github.com/getwyrd/wyrd/issues/374)) |
| [0012](accepted/0012-milestone-6-encryption-at-rest-kms.md) | Milestone 6 — encryption at rest (KeyService / KMS) | accepted (tracker [#375](https://github.com/getwyrd/wyrd/issues/375)) |
| [0013](accepted/0013-implementation-arc-rescoped.md) | The implementation arc (rescoped) | accepted (supersedes 0002; tracker [#298](https://github.com/getwyrd/wyrd/issues/298)) |

¹ Frozen pre-rescope: uses proposal 0002's original milestone numbering (old M5/M6/M7 = cross-zone replication / global control plane / cross-zone DR → **M9/M10/M11** today). Reconcile through [0013's old→new mapping table](accepted/0013-implementation-arc-rescoped.md).
