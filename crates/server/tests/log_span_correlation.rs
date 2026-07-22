//! **A span is context, not verbosity — the correlation id must survive a raised log level.**
//! (#531/#532 review.)
//!
//! Its own test binary, and that is load-bearing. `tracing` caches each callsite's `Interest` in
//! a **process-global** table the first time it is hit. A sibling test that touches the same
//! `info_span!` callsite under a permissive subscriber latches it *enabled* for the whole
//! process — after which these tests pass no matter what the filter does, and the regression
//! they exist to catch walks straight through them. (Verified: as unit tests inside
//! `logging.rs` they survived a mutation that reintroduced the bug. The same hazard is why
//! `custodian_day_one.rs` is its own binary and why `scrub.rs` has `enable_metric_callsites`.)
//!
//! A separate binary is a separate process, so the callsites below are hit only by these tests.

#![forbid(unsafe_code)]

use std::io;
use std::sync::{Arc, Mutex};

use wyrd_server::logging::{dispatch, LogConfig};

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

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// The S3 gateway carries the `request_id` (#529) on an `info`-level `s3.request` span, and
/// every event under it inherits the field — that is what makes the id a **join key** rather
/// than a decoration.
///
/// Under a plain `EnvFilter` at `--log-level warn` — the obvious setting for an operator who
/// finds `info` noisy — the *span itself* was filtered out. The fmt layer never recorded its
/// fields, so `with_current_span` had nothing to attach, and the `ERROR` line reporting the
/// failure came out with **no `request_id`**:
///
/// ```text
/// {"level":"ERROR","fields":{"message":"the gateway failed the request"},"target":"wyrd.gateway.s3.error"}
/// ```
///
/// The client is handed an `x-amz-request-id` that joins to nothing, in exactly the
/// configuration where correlation matters most. Pre-fix this test is RED.
#[test]
fn a_request_span_still_carries_its_id_onto_an_error_at_log_level_warn() {
    let capture = Capture::default();
    let dispatch = dispatch(
        &LogConfig::new(Some("warn"), None).unwrap(),
        capture.clone(),
        tracing_subscriber::layer::Identity::new(),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("s3.request", request_id = "deadbeefcafe");
        let _entered = span.enter();
        tracing::error!(target: "wyrd.gateway.s3.error", "the gateway failed the request");
    });
    let out = capture.contents();
    assert!(
        out.contains("deadbeefcafe"),
        "the ERROR event must carry the request_id from its enclosing info-level span at \
         --log-level warn. Pre-fix the span is filtered out and the id is LOST — the client's \
         x-amz-request-id joins to nothing, and #529's entire purpose evaporates. got: {out}"
    );
}

/// The other half, which the fix must not trade away: throttling *spans* is wrong, but
/// throttling *events* is the entire job of a log level. A "fix" that made the level a no-op
/// would pass the test above and be worse than the bug.
#[test]
fn events_are_still_throttled_even_though_spans_are_not() {
    let capture = Capture::default();
    let dispatch = dispatch(
        &LogConfig::new(Some("warn"), None).unwrap(),
        capture.clone(),
        tracing_subscriber::layer::Identity::new(),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("s3.request", request_id = "deadbeefcafe");
        let _entered = span.enter();
        tracing::info!(target: "wyrd.gateway.s3.access", "request served");
        tracing::warn!(target: "wyrd.gateway.s3.auth", "refused an unauthenticated request");
    });
    let out = capture.contents();
    assert!(
        !out.contains("request served"),
        "an INFO event must STILL be dropped at --log-level warn — the level is not a no-op. \
         got: {out}"
    );
    assert!(
        out.contains("refused an unauthenticated request") && out.contains("deadbeefcafe"),
        "the WARN event passes, and inherits the span's request_id. got: {out}"
    );
}
