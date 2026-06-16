---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - governance
  - licensing
  - sovereignty
  - dependencies
---
# 0003. Licensing, and dependency-selection criteria

## Context

The target users are mid-sized cloud providers — commercial entities that must be legally able to deploy, modify, and build products on the system. AGPL (the MinIO path) squeezes exactly these users and triggers fork-and-flee dynamics. An infrastructure foundation intended for broad adoption needs a permissive license. Contribution provenance must be tracked, but a full CLA imposes friction that deters drive-by contributors.

The same lens applies *inward*. Wyrd is sovereignty-first and built on pluggable components at every seam (coordination, metadata store, chunk store, deployment substrate, telemetry). **A sovereignty-first system is only as sovereign as its weakest pluggable component**, so dependency choices cannot rest on technical merit alone. The original design applied the license/sovereignty lens *unevenly* — etcd and TiKV happen to pass it; CockroachDB did not (source-available, single US vendor, revenue-capped, forced telemetry) and slipped through until audited. The durable fix is not swapping one database; it is encoding the test so the next CockroachDB is caught at selection time.

## Decision

### 1. Outbound license

License Wyrd under **Apache 2.0**. Track provenance with **DCO** (Developer Certificate of Origin) sign-offs on every commit, not a CLA. Ship `LICENSE`, `NOTICE`, and `SECURITY.md` from the first commit.

### 2. Dependency-selection criteria (inbound)

Every external dependency at a pluggable seam — and every component recommended as a *default* — is evaluated against three tests:

- **License.** Permissive and clearly-licensed (MIT / Apache-2.0 / BSD-class) is preferred. Adopter-infecting copyleft (AGPL) and source-available "paywall" licenses (BSL, SSPL, vendor community licenses) are **disqualifying for a default** and must be explicitly justified if used at all.
- **Governance.** Foundation- or multi-stakeholder-governed (e.g. CNCF) is preferred over a single vendor who can unilaterally relicense. A permissive license is the safety net: permissively-licensed code is forkable, so a relicense *trap* is impossible even if the lead vendor changes course.
- **Sovereignty.** **"Sovereign" here means not captured by US hyperscalers and not single-vendor rug-pullable — not "EU-only."** A component passes if it is (a) not a US-controlled hosted service and (b) not relicenseable out from under us (permissive + ideally foundation-governed). Corporate *origin* outside the US (including non-EU) does **not** fail the test, but a non-EU lead vendor is a **wrinkle to state explicitly**, not hide. No component may force a phone-home.

### 3. Principle

**Prefer foundation-governed, permissively-licensed components at every pluggable seam.** When a chosen default carries a sovereignty wrinkle, name it; when a maximally-sovereign alternative exists, offer it. This makes the audit a standing rule, not a one-time cleanup.

## Consequences

- Commercial providers can adopt Wyrd without legal friction; patent grant included. Lower contribution friction than a CLA, with sufficient provenance. The MinIO community (wounded by AGPL + feature-stripping) becomes an addressable audience.
- The three tests turn "we got lucky on most dependencies" into "we have a rule." This is the higher-value output — more than any single component swap.
- Trademark/governance (project-vs-foundation trajectory for Wyrd itself) is deferred; the license is not.

### Audit of currently-named dependencies (the three tests applied)

| Component | Seam | Verdict |
|-----------|------|---------|
| **etcd** | L5 coordination | **Keep** — Apache-2.0, CNCF-graduated. (Auth CVEs in non-k8s contexts handled as defence-in-depth behind mTLS — ADR-0006.) |
| **TiKV** | L4 metadata; **new L2 default** | **Keep** — Apache-2.0, CNCF-graduated. The answer to the CockroachDB problem. |
| **TiDB** | L2 default (SQL surface on TiKV) | **Default, with a stated wrinkle** — Apache-2.0; storage core (TiKV) is CNCF-graduated; TiFlash is also Apache-2.0. PingCAP-led (China-HQ). Passes the not-US / not-rug-pullable test; the non-EU lead vendor is the wrinkle, defused by CNCF governance + a permissive, forkable license. |
| **YugabyteDB** | L2 alternative | **Alternative** — core is **100% Apache-2.0** (former enterprise features now included; verified 2025). Only the *management platform* (YugabyteDB Anywhere) is source-available, which Wyrd does not depend on. US-vendor-led but forkable. |
| **PostgreSQL** | L2 alternative (smaller tier) | **Keep — gold-standard governance / max sovereignty**: PostgreSQL License (permissive), foundation-governed, *un-relicenseable*, no controlling vendor. Not natively Spanner-class, so it fits L2 deployments that don't need geo-distributed consensus. |
| **CockroachDB** | former L2 | **Rejected** — source-available (2024), single US vendor, revenue-capped, forced telemetry. Fails all three tests. Replaced by TiDB/TiKV; kept only as a caveated mention (ADR-0008, building-block view). |
| **NATS JetStream** | L3 transport | **Keep — and a live cautionary example.** Apache-2.0, CNCF. In 2025 the lead vendor (Synadia) attempted to pull NATS out of CNCF and relicense the server to BSL; it stayed Apache-2.0/CNCF *only* because CNCF held the trademark and the code was forkable — precisely the protection the principle above buys. |
| **OpenTelemetry; SPIFFE/SPIRE; Prometheus** | telemetry / PKI | **Keep** — Apache-2.0, CNCF, vendor-neutral. |
| **Rust toolchain, reed-solomon-simd, redb, prost/protobuf, madsim/turmoil** | implementation | **Keep** — permissive (MIT/Apache-2.0); forkable, so no relicense trap even where single-maintainer (redb). |
| **Deployment substrate (Kubernetes)** | orchestration | **Audit wording (follow-up).** Kubernetes itself is CNCF/Apache and clean, but *managed* k8s (EKS/GKE/AKS) is US-hyperscaler-controlled — the sovereignty trap one layer down. ADR-0010's "no coupling to orchestrator APIs" protects architecturally; the docs should *recommend* self-hosted k8s / systemd / EU substrates (e.g. Sovereign Cloud Stack) and flag the managed-hyperscaler path as convenient-but-sovereignty-compromising. |
| **Reference observability (Loki/Tempo/Grafana)** | optional reference only | **Note, don't change.** AGPLv3 since 2021, but shipped as an *optional reference* Wyrd depends on none of (OTel-native, backend-agnostic — ADR-0012), so the AGPL does not reach Wyrd. Note that an operator avoiding AGPL can swap it. |

### Follow-ups (separate changes)

- Audit the **deployment-substrate wording** (architecture section 7 / ADR-0010) to recommend sovereignty-safe orchestration and flag the managed-hyperscaler path.
- Add the **AGPL note** on the reference observability stack (near ADR-0012).
- Consider naming **YugabyteDB / PostgreSQL** in ADR-0008 itself (it currently names only `redb` + `TiKV` concretely).
