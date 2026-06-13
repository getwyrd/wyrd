# 0013. API-first management surface

Date: design phase
Status: Accepted

## Context

The management surface could be CLI-first, API-first, or UI-first. Given the
declarative-reconciliation model (ADR-0011) and a Kubernetes-shaped audience, the
authoritative interface should be a clean programmatic one, with other surfaces
as consumers.

## Decision

**API-first.** A clean gRPC/REST management API is the source of truth for
desired state and reconciliation status. Ship a thin CLI over it for v1. Defer a
web UI. The API is the authoritative interface; CLI, dashboards, and any future
UI are consumers of it.

## Consequences

- Automation-friendly and scriptable from day one; meets the target audience
  where they are.
- The web UI can be added later without re-architecting management.
- Aligns with OTel-first observability (ADR-0012): two open-standard seams the
  project controls, with vendor tools as replaceable consumers.
