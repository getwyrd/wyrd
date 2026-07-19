//! The **shared telemetry seam** (observability floor, proposal 0010 §"Scope
//! boundary" item 1; ADR-0011 telemetry-from-first-commit, ADR-0012 backend-agnostic
//! OpenTelemetry).
//!
//! This crate is the *extraction* of the custodian's M3 durability-plane seam
//! (proposal 0005 §"The durability plane") into a **backend-agnostic** crate that any
//! role can own — the custodian's durability plane today, and the server's
//! request-plane RED / capacity-plane admission signals (0010 items 4–5) next, without
//! forking a second export path (0010 invariant: *reuse, don't rebuild* the seam;
//! *no concrete telemetry backend leaks into a leaf crate*). It lives outside
//! `custodian` precisely so it is not anchored to one consumer.
//!
//! Telemetry is emitted through `tracing` bridged to OpenTelemetry
//! ([`tracing_opentelemetry::MetricsLayer`]) and **dual-exported** — over BOTH a
//! **Prometheus-scrapeable registry** (zero-dependency, the dev profile) **and**
//! **OTLP push** (production) — with **no backend hardcoded** (`0005:338-340`,
//! ADR-0012). The export surface is chosen by [`ExporterConfig`]; the SDK meter
//! provider fans the same meters out to whichever readers are wired.
//!
//! The dual-export *surfaces* are BINDING (ADR-0012); the **in-process assertion**
//! mechanism is ILLUSTRATIVE — at C4-verify the emitted metric is read back from the
//! Prometheus registry in-process ([`DurabilityTelemetry::gather_prometheus`]); a
//! live Prometheus scrape / OTLP collector run is supplementary evidence off-Check.

#![forbid(unsafe_code)]

use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use prometheus::Encoder;
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::layer::SubscriberExt;

/// The instrumentation scope name the durability-plane meters live under. Other
/// planes (request / capacity) attach their own scope through the same provider.
pub const SCOPE: &str = "wyrd.custodian";

/// Which export surface(s) the telemetry seam is wired to. **No backend is
/// hardcoded** (ADR-0012): the caller selects, and a deployment can run Prometheus
/// for dev, OTLP for production, or both.
#[derive(Debug, Clone)]
pub enum ExporterConfig {
    /// A Prometheus-scrapeable registry only (the zero-dependency dev profile).
    Prometheus,
    /// OTLP push to `endpoint` only (production).
    Otlp {
        /// The OTLP collector endpoint (e.g. `http://127.0.0.1:4317`).
        endpoint: String,
    },
    /// Both surfaces at once — a Prometheus registry **and** OTLP push.
    Both {
        /// The OTLP collector endpoint for the push surface.
        otlp_endpoint: String,
    },
}

impl ExporterConfig {
    fn wants_prometheus(&self) -> bool {
        matches!(
            self,
            ExporterConfig::Prometheus | ExporterConfig::Both { .. }
        )
    }

    fn otlp_endpoint(&self) -> Option<&str> {
        match self {
            ExporterConfig::Otlp { endpoint } => Some(endpoint),
            ExporterConfig::Both { otlp_endpoint } => Some(otlp_endpoint),
            ExporterConfig::Prometheus => None,
        }
    }
}

/// The telemetry handle: an OpenTelemetry [`SdkMeterProvider`] wired to the configured
/// export surface(s), plus the Prometheus registry (when a Prometheus surface is
/// configured) for in-process read-back. Named `DurabilityTelemetry` for the M3
/// consumer it was extracted from; the request/capacity planes share the same handle.
#[derive(Clone)]
pub struct DurabilityTelemetry {
    provider: SdkMeterProvider,
    registry: Option<prometheus::Registry>,
}

impl DurabilityTelemetry {
    /// Build the telemetry seam against `config`, wiring the selected export surfaces.
    /// Constructing the OTLP exporter must run inside a Tokio runtime (the tonic
    /// transport is built there).
    pub fn new(config: ExporterConfig) -> Result<Self, TelemetryError> {
        let mut builder = SdkMeterProvider::builder();
        let mut registry = None;

        if config.wants_prometheus() {
            let reg = prometheus::Registry::new();
            let exporter = opentelemetry_prometheus::exporter()
                .with_registry(reg.clone())
                .build()
                .map_err(|e| TelemetryError::Prometheus(e.to_string()))?;
            builder = builder.with_reader(exporter);
            registry = Some(reg);
        }

        if let Some(endpoint) = config.otlp_endpoint() {
            let exporter = MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.to_owned())
                .build()
                .map_err(|e| TelemetryError::Otlp(e.to_string()))?;
            let reader = PeriodicReader::builder(exporter).build();
            builder = builder.with_reader(reader);
        }

        Ok(Self {
            provider: builder.build(),
            registry,
        })
    }

    /// The OpenTelemetry meter — the instrument factory for the durability metrics.
    pub fn meter(&self) -> Meter {
        self.provider.meter(SCOPE)
    }

    /// The `tracing` → OpenTelemetry **metrics** bridge layer. Install it on a
    /// `tracing` subscriber so a `tracing` metric event (e.g.
    /// `monotonic_counter.custodian_active = 1`) becomes an OTel counter, dual-
    /// exported through this provider's readers.
    pub fn metrics_layer<S>(&self) -> MetricsLayer<S, SdkMeterProvider>
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        MetricsLayer::new(self.provider.clone())
    }

    /// A **metrics-only** `tracing` [`Dispatch`](tracing::Dispatch) over this handle's
    /// bridge — the sink a *server* role hands to instrumentation running in tasks the
    /// role does not own.
    ///
    /// [`metrics_layer`](Self::metrics_layer) is composed into a role's own subscriber when
    /// the role drives the instrumented code itself: the custodian wraps each reconciliation
    /// pass in one scoped dispatch (`crates/server/src/custodian.rs:310`), and a scoped
    /// dispatch is a thread-local that a pass inherits because the pass *is* the task. A
    /// server role cannot do that. `tonic` and `axum` `tokio::spawn` a task **per
    /// connection** (`tonic-0.14.6` `src/transport/server/mod.rs:925`), and a spawned task
    /// does not inherit the spawner's scoped dispatch — so a `with_subscriber` around the
    /// serve future would reach no handler, and the request/capacity planes would emit into
    /// whatever global subscriber happened to be installed. Instead they **carry** this
    /// dispatch and enter it around each (synchronous) metric emission, which works from any
    /// task and keeps each server's telemetry its own — the same per-provider isolation the
    /// custodian's read-back tests rely on.
    ///
    /// It is metrics-**only** on purpose, and that is the difference from
    /// `CustodianService::with_logging`'s log+metrics dispatch. That one is *current* for a
    /// whole pass, so it must carry the log layers or it would swallow the pass's audit
    /// lines. This one is current for exactly one `tracing` metric event, so composing a
    /// `fmt` layer in would only echo every metric onto stderr as a second log row — one
    /// extra line per request on a hot path. Everything else a role logs keeps going to the
    /// process-global subscriber (#527), untouched.
    pub fn metrics_dispatch(&self) -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(self.metrics_layer()))
    }

    /// Force the meter provider to flush its readers (collect + export). Returns the
    /// first flush error, if any.
    pub fn flush(&self) -> Result<(), TelemetryError> {
        self.provider
            .force_flush()
            .map_err(|e| TelemetryError::Flush(e.to_string()))
    }

    /// Read the Prometheus surface back **in-process** — the metric families encoded
    /// in the Prometheus text exposition format, or `None` when no Prometheus surface
    /// is configured. This is the ILLUSTRATIVE in-process assertion seam; a live
    /// scrape is supplementary off-Check evidence.
    pub fn gather_prometheus(&self) -> Option<String> {
        let registry = self.registry.as_ref()?;
        let metric_families = registry.gather();
        let mut buf = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metric_families, &mut buf).ok()?;
        String::from_utf8(buf).ok()
    }
}

/// Errors raised while wiring or flushing the telemetry seam.
#[derive(Debug, Clone)]
pub enum TelemetryError {
    /// The Prometheus exporter could not be built.
    Prometheus(String),
    /// The OTLP exporter could not be built.
    Otlp(String),
    /// A force-flush of the readers failed.
    Flush(String),
}

impl std::fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TelemetryError::Prometheus(e) => write!(f, "prometheus exporter: {e}"),
            TelemetryError::Otlp(e) => write!(f, "otlp exporter: {e}"),
            TelemetryError::Flush(e) => write!(f, "telemetry flush: {e}"),
        }
    }
}

impl std::error::Error for TelemetryError {}
