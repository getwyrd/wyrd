---
created: 13.06.2026 11:57
type: architecture
status: living
tags:
  - architecture
  - context
  - scope
---
# 3. Context and scope

> Living document.

## 3.1 System context (C4 level 1)

The system sits between the applications that consume storage and the physical infrastructure a provider operates. It depends on external operational tooling (an observability backend, a coordination service in production) but couples tightly to none of them.

See `diagrams/c4-context.mermaid` for the diagram. In prose:

- **Applications** (a Drive-like product, an S3 client, a future collaborative editor) talk to the system through the **access layer** — primarily an S3-compatible API, with FUSE/WebDAV/native-SDK surfaces.
- **Operators** manage the system through an **API-first management surface** (ADR-0013) and observe it through an **OpenTelemetry**-based telemetry pipeline (ADR-0012).
- The system runs on the provider's **compute and storage hardware**, organized into **zones** (one per datacenter/region), and uses a **coordination service** (etcd in production) and a **global control-plane database** (a Spanner-class SQL store) as dependencies.

## 3.2 External interfaces

| Interface | Direction | Protocol | Notes |
|-----------|-----------|----------|-------|
| S3-compatible API | inbound | HTTP/S3 | Primary integration surface |
| FUSE / NFS | inbound | filesystem | Second-class; full POSIX semantics not a goal |
| WebDAV / Drive API | inbound | HTTP | The consumer Drive surface |
| Native SDK | inbound | gRPC | The "real" interface; exposes streaming, append, CAS |
| Management API | inbound | gRPC/REST | Operator control; desired-state, declarative (ADR-0013) |
| OTLP telemetry | outbound | OTLP | Push to operator's collector; also a Prometheus scrape endpoint (ADR-0012) |
| Coordination | internal | etcd gRPC | Production; in-memory in dev (ADR-0006) |

All inbound interfaces authenticate at the access layer (section 8.5): OIDC for the Drive / WebDAV / SDK surfaces, S3 Signature V4 for the S3 API, OIDC + mTLS for management. Internal service-to-service traffic is mTLS under the provider CA (ADR-0005).

## 3.3 What is explicitly out of context

- Cross-provider or untrusted-operator federation (ADR-0005).
- Application-level collaborative-editing logic (storage primitives only).
- The observability storage/visualization stack — recommended and shipped as a reference, but not a dependency the binary is aware of (ADR-0012).
