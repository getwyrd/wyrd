//! The **operational log subscriber** (observability floor, proposal 0010 items 1 + 3;
//! issue #527) — the sink every `tracing` event in the process finally lands in.
//!
//! Before this module the workspace declared the whole `tracing` stack and the custodian
//! was fully instrumented, yet **no `fmt` layer and no `EnvFilter` was installed in any
//! binary**: every event was emitted into a subscriber with no log layer and discarded.
//! That included [`wyrd_custodian`]'s audit plane — the `action = "data-loss"` error line
//! that says *the data is gone, NEEDS-HUMAN*. The instrumentation was written; only the
//! sink was missing. This module is that sink.
//!
//! ## Discipline
//!
//! - **Logs go to stderr.** `wyrd get` writes the object's raw bytes to **stdout**, so a
//!   redirect (`wyrd get k > out.bin`) must never be corrupted by a diagnostic. That is
//!   the stream discipline `cli.rs` has documented since M0; the subscriber honours it.
//! - **JSON by default.** The field-experiment collection story is `docker compose logs |
//!   jq` — the structured fields (`dserver`, `chunk`, `index`, `action`) are the whole
//!   point, and prose throws them away. `--log-format text` restores a human-readable
//!   line for interactive use.
//! - **Level precedence: `--log-level` > `RUST_LOG` > `info`.** An explicit flag beats the
//!   ambient environment; an invalid directive is rejected at parse time
//!   ([`LogConfig::new`]) rather than silently ignored.
//!
//! ## Why a [`Dispatch`] and not just a global
//!
//! [`init_global`] installs the subscriber process-wide, which is what the binary roles
//! want. But the custodian role additionally routes its *metric* events into a
//! per-role [`DurabilityTelemetry`](wyrd_telemetry::DurabilityTelemetry) provider through
//! a **scoped** dispatch (`custodian.rs`), because the durability tests each need their
//! own provider to read back in-process — a single process-global provider could not give
//! twelve parallel tests twelve isolated registries. So this module exposes [`dispatch`]
//! (build a subscriber, don't install it) and lets the custodian compose the log layers
//! *and* its metrics layer into one dispatch. Both stacks carry the same `fmt` layer, and
//! only one dispatch is current for any given event, so nothing is logged twice.

use std::io;

use tracing::Dispatch;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::{Identity, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, Registry};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The level applied when neither `--log-level` nor `RUST_LOG` says otherwise.
pub const DEFAULT_LEVEL: &str = "info";

/// The line format the `fmt` layer writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// One JSON object per event, carrying every structured field. The default: this is
    /// what a log collector (or `jq`) ingests, and what makes `dserver` / `chunk` /
    /// `index` greppable rather than embedded in prose.
    #[default]
    Json,
    /// A human-readable single line — for interactive use, not for collection.
    Text,
}

impl LogFormat {
    fn parse(value: &str) -> Result<Self, BoxError> {
        match value {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            other => {
                Err(format!("invalid --log-format `{other}` (expected `json` or `text`)").into())
            }
        }
    }
}

/// The resolved logging configuration for a role.
#[derive(Debug, Clone, Default)]
pub struct LogConfig {
    /// The explicit `--log-level` directive, already validated. `None` defers to
    /// `RUST_LOG`, then to [`DEFAULT_LEVEL`].
    level: Option<String>,
    format: LogFormat,
}

impl LogConfig {
    /// Resolve from the `--log-level` / `--log-format` flag values. Both are validated
    /// here so a typo fails the role at startup instead of silently degrading it to
    /// no logging — the failure mode this whole module exists to end.
    pub fn new(level: Option<&str>, format: Option<&str>) -> Result<Self, BoxError> {
        if let Some(level) = level {
            EnvFilter::builder()
                .parse(level)
                .map_err(|e| format!("invalid --log-level `{level}`: {e}"))?;
        }
        Ok(Self {
            level: level.map(str::to_owned),
            format: format
                .map(LogFormat::parse)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    /// The filter for this config: the explicit level if given, else `RUST_LOG`, else
    /// [`DEFAULT_LEVEL`].
    fn filter(&self) -> EnvFilter {
        match &self.level {
            // Validated in `new`, so this cannot be a lossy parse.
            Some(level) => EnvFilter::new(level),
            None => {
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LEVEL))
            }
        }
    }

    /// The `fmt` layer, boxed so the JSON and text formatters (different concrete types)
    /// share one return type.
    fn fmt_layer<S, W>(&self, writer: W) -> Box<dyn Layer<S> + Send + Sync + 'static>
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
        W: for<'w> fmt::MakeWriter<'w> + Send + Sync + 'static,
    {
        match self.format {
            // `with_current_span` carries the enclosing span's fields (the request id, once
            // #529 lands) onto every event emitted under it — that is what turns a client's
            // `x-amz-request-id` into a `jq` selector over the whole server-side trail.
            LogFormat::Json => Box::new(
                fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_current_span(true)
                    .with_span_list(false),
            ),
            LogFormat::Text => Box::new(fmt::layer().with_writer(writer).with_ansi(false)),
        }
    }
}

/// Build a subscriber over `writer`, with `extra` composed in beneath the log layers.
///
/// `extra` is the seam the custodian role hangs its
/// [`MetricsLayer`](wyrd_telemetry::DurabilityTelemetry::metrics_layer) on, so one dispatch
/// carries both the metric bridge and the log sink. Pass `()` for a log-only subscriber.
pub fn dispatch<W, L>(config: &LogConfig, writer: W, extra: L) -> Dispatch
where
    W: for<'w> fmt::MakeWriter<'w> + Send + Sync + 'static,
    L: Layer<Registry> + Send + Sync + 'static,
{
    Dispatch::new(
        Registry::default()
            .with(extra)
            .with(config.filter())
            .with(config.fmt_layer(writer)),
    )
}

/// Install the log subscriber as the process-global default, writing to **stderr**.
///
/// Called once at CLI entry, before any role runs, so a callsite hit early in startup
/// registers its interest against a real subscriber rather than latching
/// `Interest::never` against an empty one.
pub fn init_global(config: &LogConfig) -> Result<(), BoxError> {
    tracing::dispatcher::set_global_default(dispatch(config, io::stderr, Identity::new()))
        .map_err(|e| format!("could not install the log subscriber: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A `MakeWriter` that appends every line into a shared buffer, so a test can read
    /// back exactly what the subscriber emitted.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'w> fmt::MakeWriter<'w> for Capture {
        type Writer = Self;
        fn make_writer(&'w self) -> Self::Writer {
            self.clone()
        }
    }

    /// The keystone assertion, in miniature: an event with structured fields reaches a
    /// sink and its fields survive as JSON. Before #527 this produced nothing at all.
    #[test]
    fn a_structured_event_reaches_the_sink_as_json_with_its_fields() {
        let capture = Capture::default();
        let dispatch = dispatch(
            &LogConfig::new(None, None).unwrap(),
            capture.clone(),
            Identity::new(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::error!(
                target: "wyrd.custodian.reconstruction.audit",
                action = "data-loss",
                chunk = "00000000000000000000000000c0ffee",
                "un-reconstructable",
            );
        });
        let out = capture.contents();
        assert!(
            out.contains(r#""action":"data-loss""#)
                && out.contains(r#""chunk":"00000000000000000000000000c0ffee""#)
                && out.contains(r#""target":"wyrd.custodian.reconstruction.audit""#),
            "the audit fields must survive to the sink as JSON, not be flattened into prose; got: {out}"
        );
    }

    /// The level gate is real, not decorative: a `debug!` is dropped at the default level.
    #[test]
    fn the_default_level_drops_debug_and_keeps_info() {
        let capture = Capture::default();
        let dispatch = dispatch(
            &LogConfig::new(None, None).unwrap(),
            capture.clone(),
            Identity::new(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::debug!(target: "wyrd.test", "chatter");
            tracing::info!(target: "wyrd.test", "signal");
        });
        let out = capture.contents();
        assert!(
            !out.contains("chatter"),
            "debug is below the default `info`"
        );
        assert!(out.contains("signal"), "info is at the default level");
    }

    /// `--log-level` raises the gate that `RUST_LOG` would otherwise set.
    #[test]
    fn an_explicit_level_admits_debug() {
        let capture = Capture::default();
        let dispatch = dispatch(
            &LogConfig::new(Some("debug"), None).unwrap(),
            capture.clone(),
            Identity::new(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::debug!(target: "wyrd.test", "chatter");
        });
        assert!(capture.contents().contains("chatter"));
    }

    /// A typo in the level fails the role at startup rather than silently leaving it mute.
    #[test]
    fn an_invalid_level_is_rejected_not_ignored() {
        let err = LogConfig::new(Some("nonsense=;;"), None).unwrap_err();
        assert!(err.to_string().contains("invalid --log-level"), "{err}");
    }

    #[test]
    fn an_invalid_format_is_rejected() {
        let err = LogConfig::new(None, Some("yaml")).unwrap_err();
        assert!(err.to_string().contains("invalid --log-format"), "{err}");
    }

    /// The text format is a real alternative, not an alias of JSON.
    #[test]
    fn the_text_format_writes_prose_not_json() {
        let capture = Capture::default();
        let dispatch = dispatch(
            &LogConfig::new(None, Some("text")).unwrap(),
            capture.clone(),
            Identity::new(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "wyrd.test", action = "repair", "did a thing");
        });
        let out = capture.contents();
        assert!(
            out.contains("did a thing") && !out.contains(r#""action":"repair""#),
            "{out}"
        );
    }
}
