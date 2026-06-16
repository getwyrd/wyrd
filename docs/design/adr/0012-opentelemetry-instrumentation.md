---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - observability
  - telemetry
---
# 0012. OpenTelemetry instrumentation; storage/visualization agnostic

## Context

OpenTelemetry, Prometheus, and Grafana are not alternatives — they are three layers (instrumentation, storage, visualization). The only true lock-in decision is the instrumentation layer, because it touches the code. The target audience already runs the CNCF observability stack.

## Decision

- **Instrument with OpenTelemetry (OTLP)** via `tracing` + `tracing-opentelemetry`. Expose **both** a Prometheus-scrapeable endpoint (zero-dependency, great for the dev profile) **and** OTLP push (scales in production). The binary hardcodes no storage backend.
- **Recommend and ship as reference** Prometheus + Loki + Tempo for storage, but depend on none of them.
- **Provide curated Grafana dashboards** (durability, capacity, request) as version-controlled artifacts in `deploy/grafana/` — a gift, not a gate.

## Consequences

- Same instrumentation decision yields the dev profile (curl a metrics endpoint), the production profile (full OTLP pipeline), and the bring-your-own profile (provider's existing stack).
- OTel is the only telemetry commitment; everything downstream is the operator's swappable choice — the same pluggability principle as the storage backends.
- Consistent with API-first management (ADR-0013): the management API is the system of record; Grafana is one consumer.
