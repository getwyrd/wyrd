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

The same lens applies *inward*. Wyrd is sovereignty-first and built on pluggable components at every seam (coordination, metadata store, chunk store, deployment substrate, telemetry). **A sovereignty-first system is only as sovereign as its weakest pluggable component**, so dependency choices cannot rest on technical merit alone. An early draft design applied the license/sovereignty lens *unevenly* — etcd and TiKV happen to pass it; CockroachDB did not (source-available, single vendor, revenue-capped, forced telemetry). The durable fix is not swapping one database; it is encoding the test so the next CockroachDB is caught at selection time.

## Decision

### 1. Outbound license and contribution provenance

License Wyrd under **Apache 2.0** — permissive, with an explicit patent grant — so commercial providers can deploy, modify, and build products on it without legal friction. Ship `LICENSE`, `NOTICE`, and `SECURITY.md` from the first commit.

Track contribution provenance with the **Developer Certificate of Origin (DCO)**, *not* a **Contributor License Agreement (CLA)**. Both establish that the project has the right to use a contribution, but they differ sharply in cost and in where they put power:

- A **CLA** is a legal agreement each contributor signs — typically once, via a signing bot such as CLA Assistant — granting the project's steward an explicit copyright license, a patent grant, and, in many CLAs, the right to **relicense** the contribution later. (A stronger variant, a Copyright Assignment Agreement, transfers copyright outright.) It gives the steward the cleanest legal footing and the freedom to change the license unilaterally — but at two costs. First, **friction**: a contributor acting for an employer usually has to clear the agreement through legal or management before even a one-line fix can merge, which is exactly what deters the drive-by contributors an open-infrastructure project wants. Second, **concentration of power**: the relicensing right a CLA grants is precisely the lever a single vendor later pulls to go source-available.
- The **DCO** is a lightweight, per-commit attestation originated by the Linux kernel: the contributor adds a `Signed-off-by:` line certifying they have the right to submit the patch under the project's license. No separate document, no signing ceremony, no legal review for routine work. Contributions are licensed inbound under Apache 2.0, identical to the outbound license ("inbound = outbound").

We choose the DCO because the CLA's friction is paid by exactly the contributors we most want, and because the one thing a CLA would buy us — the unilateral right to relicense — is something a sovereignty-first, no-rug-pulls project should deliberately **not** hold. The trade-off is conscious: under the DCO every contributor retains copyright under Apache 2.0, so Wyrd *cannot* later relicense the project out from under its community. That is the same guarantee this ADR demands of its dependencies (§2: foundation-governed, permissive, un-rug-pullable), turned on Wyrd itself.

### 2. Dependency-selection criteria (inbound)

Every external dependency at a pluggable seam — and every component recommended as a *default* — is evaluated against three tests:

- **License.** Permissive and clearly-licensed (MIT / Apache-2.0 / BSD-class) is preferred. Adopter-infecting copyleft (AGPL) and source-available "paywall" licenses (BSL, SSPL, vendor community licenses) are **disqualifying for a default** and must be explicitly justified if used at all.
- **Governance.** Foundation- or multi-stakeholder-governed (e.g. CNCF) is preferred over a single vendor who can unilaterally relicense. A permissive license is the safety net: permissively-licensed code is forkable, so a relicense *trap* is impossible even if the lead vendor changes course.
- **Control resilience.** *(Sovereignty, framed around control rather than jurisdiction.)* A component passes if control over it cannot be withdrawn and you can run it yourself: it must be **forkable** (permissive, ideally foundation-governed, so no single party can relicense it away), **self-hostable** with no forced phone-home and no *required* managed/hosted service, and — for components that hold data — keep **data residency** under the operator's control. **Vendor nationality is explicitly not a test;** the test is whether control can be withdrawn — a managed, hosted service you cannot self-run fails it regardless of who operates it.

*Inspired by the EU Cloud Sovereignty Framework's dimensional decomposition, minus the politics.*

### 3. Principle

**Prefer foundation-governed, permissively-licensed components at every pluggable seam.** When a chosen default carries a control-resilience wrinkle, name it; when a more resilient alternative exists, offer it. This makes the audit a standing rule, not a one-time cleanup.

## Consequences

- Commercial providers can adopt Wyrd without legal friction; patent grant included. Lower contribution friction than a CLA, with sufficient provenance. The MinIO community (wounded by AGPL + feature-stripping) becomes an addressable audience.
- The three tests turn "we got lucky on most dependencies" into "we have a rule." This is the higher-value output — more than any single component swap.
- Trademark/governance (project-vs-foundation trajectory for Wyrd itself) is deferred; the license is not.

### Audit of currently-named dependencies (the three tests applied)

| Component                                                                   | Seam                             | Verdict                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------------------------------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **etcd**                                                                    | L5 coordination                  | **Keep** — Apache-2.0, CNCF-graduated. (Auth CVEs in non-k8s contexts handled as defence-in-depth behind mTLS — ADR-0006.)                                                                                                                                                                                                                                                                                                                  |
| **TiKV**                                                                    | L4 metadata; **new L2 default**  | **Keep** — Apache-2.0, CNCF-graduated. The answer to the CockroachDB problem.                                                                                                                                                                                                                                                                                                                                                               |
| **TiDB**                                                                    | L2 default (SQL surface on TiKV) | **Default, with a stated wrinkle** — Apache-2.0; storage core (TiKV) is CNCF-graduated; TiFlash is also Apache-2.0. PingCAP-led (China-HQ). Passes the control-resilience test (Apache-2.0, CNCF-graduated core, forkable, self-hostable); vendor nationality is not a criterion.                                                                                                                                                  |
| **YugabyteDB**                                                              | L2 alternative                   | **Alternative** — core is **100% Apache-2.0** (former enterprise features now included; verified 2025). Only the *management platform* (YugabyteDB Anywhere) is source-available, which Wyrd does not depend on. Single-vendor-led but forkable (Apache-2.0 core).                                                                                                                                                                                                |
| **PostgreSQL**                                                              | L2 alternative (smaller tier)    | **Keep — gold-standard governance / maximal control resilience**: PostgreSQL License (permissive), foundation-governed, *un-relicenseable*, no controlling vendor. Not natively Spanner-class, so it fits L2 deployments that don't need geo-distributed consensus.                                                                                                                                                                                    |
| **CockroachDB**                                                             | former L2                        | **Rejected** — source-available (2024), single-vendor-controlled, revenue-capped, forced telemetry. Fails all three tests. Replaced by TiDB/TiKV; kept only as a caveated mention (ADR-0008, building-block view).                                                                                                                                                                                                                                  |
| **NATS JetStream**                                                          | L3 transport                     | **Keep — and a live cautionary example.** Apache-2.0, CNCF. In 2025 the lead vendor (Synadia) attempted to pull NATS out of CNCF and relicense the server to BSL; it stayed Apache-2.0/CNCF *only* because CNCF held the trademark and the code was forkable — precisely the protection the principle above buys.                                                                                                                           |
| **OpenTelemetry; SPIFFE/SPIRE; Prometheus**                                 | telemetry / PKI                  | **Keep** — Apache-2.0, CNCF, vendor-neutral.                                                                                                                                                                                                                                                                                                                                                                                                |
| **Rust toolchain, reed-solomon-simd, redb, prost/protobuf, madsim/turmoil** | implementation                   | **Keep** — permissive (MIT/Apache-2.0); forkable, so no relicense trap even where single-maintainer (redb).                                                                                                                                                                                                                                                                                                                                 |
| **Deployment substrate (Kubernetes)**                                       | orchestration                    | **Audit wording (follow-up).** Kubernetes itself is CNCF/Apache and clean, but *managed* k8s (EKS/GKE/AKS) is operator-captured — you cannot self-run it — the control trap one layer down. ADR-0010's "no coupling to orchestrator APIs" protects architecturally; the docs should *recommend* self-hosted k8s, systemd, or any operator-controlled substrate, and flag the managed path as convenient-but-control-compromising. |
| **Reference observability (Loki/Tempo/Grafana)**                            | optional reference only          | **Note, don't change.** AGPLv3 since 2021, but shipped as an *optional reference* Wyrd depends on none of (OTel-native, backend-agnostic — ADR-0012), so the AGPL does not reach Wyrd. Note that an operator avoiding AGPL can swap it.                                                                                                                                                                                                     |

### Follow-ups (separate changes)

- Audit the **deployment-substrate wording** (architecture section 7 / ADR-0010) to recommend sovereignty-safe orchestration and flag the managed-hyperscaler path.
- Add the **AGPL note** on the reference observability stack (near ADR-0012).
- Consider naming **YugabyteDB / PostgreSQL** in ADR-0008 itself (it currently names only `redb` + `TiKV` concretely).
