//! **The request and capacity planes, observed through their own export surface**
//! (observability floor, proposal 0010 §"Scope boundary" items 4–5; issue #575).
//!
//! The custodian's *durability* plane has been watchable since #450/#527. These two were
//! dark: nothing reported a failing or slow request, and a load-shed was silent — visible
//! only as a client-side `RESOURCE_EXHAUSTED`, which is to say visible only to whoever kept
//! the client's logs. This file closes that gap by driving the **real roles** and reading the
//! metrics back off `DurabilityTelemetry::gather_prometheus` (`crates/telemetry/src/lib.rs`),
//! the in-process assertion seam 0010 §DST names.
//!
//! **Both planes are driven over a real loopback listener, and that is load-bearing rather
//! than thoroughness.** The instrumentation emits from tonic's and axum's *per-connection
//! spawned tasks* (`tonic-0.14.6` `src/transport/server/mod.rs:925`), and a spawned task does
//! not inherit a scoped `tracing` dispatch. A test that drove the router in-process, on its
//! own task, would pass against wiring that emits **nothing** in production. So each test
//! stands the role up exactly as its CLI entry does — `S3Gateway::with_metrics_dispatch` +
//! `serve` for `wyrd s3` (`cli::serve_s3`), `DServer::with_metrics_dispatch` + `serve` for
//! `wyrd d-server` (`cli::run_d_server`) — and reads back from the same handle the role wired.
//!
//! What is asserted, each separately:
//!
//! 1. [`the_request_plane_records_per_op_latency_for_a_put_and_a_get`] — a PUT and a GET
//!    through the real router over a real `Gateway` backend each land a latency sample under
//!    their own `op` label. The GET's body is **fully drained** first: op completion is
//!    deliberately deferred to body completion (`crates/gateway-s3/src/lib.rs`), because a
//!    duration measured when the head was built would report ~0ms for a transfer that has not
//!    happened yet.
//! 2. [`the_request_plane_counts_a_failing_op_by_op_and_by_the_typed_class`] — a GET whose
//!    backend raises a `wyrd_traits::TransientFault` is counted on
//!    `s3_request_errors{op="get",class="transient"}`. The class is #577's exported value,
//!    consumed — the fault is a real seam type and the gateway runs the seam's own
//!    `classify` over it.
//! 3. [`the_request_plane_classes_a_mid_stream_fault_by_the_seam_not_the_head`] — the same
//!    counter, on the path where the class is hardest to get right: a GET whose head is
//!    already `200` when the fault hits. A class captured at head time can only be the
//!    `Terminal` default there (a streaming head carries no seam error yet), which is the
//!    counter's transient-vs-terminal distinction inverted for exactly the long streaming
//!    reads a real fleet fails on. Test 2 cannot catch it: its fault IS decided at head time.
//! 4. [`the_capacity_plane_reports_admission_and_the_in_flight_gauge_returns_to_zero`] — two
//!    requests held open in the handler raise the admitted counter and drive the in-flight
//!    gauge to 2; releasing them returns it to **0**.
//! 5. [`the_capacity_plane_reports_a_forced_load_shed`] — a request over the server-wide
//!    admission bound is shed, and the shed is an EVENT on the server, not just a status on
//!    the client (0010 PR-sequence item 5 DoD).
//! 6. [`the_capacity_plane_reports_a_request_cut_by_the_request_timeout`] — a handler past
//!    `request_timeout` raises the timed-out counter.
//!
//! RED on the base: none of these metric families exist — no per-op or admission metric is
//! emitted anywhere, and the `s3` / `d-server` roles have no metrics provider at all — so
//! every read-back below finds nothing and every assertion fails.
//!
//! Its own test binary, deliberately: `tracing` caches per-callsite interest in process-global
//! state, so these callsites must not be raced by a no-subscriber sibling (the
//! `custodian_day_one.rs` discipline).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::GrpcChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_core::{ContentHash, ObjectGateway, ObjectMeta, ObjectRead};
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::dserver::{AdmissionControl, DServer, DSERVER_GROUP};
use wyrd_server::Gateway;
use wyrd_telemetry::{DurabilityTelemetry, ExporterConfig};
use wyrd_traits::{BoxError, ChunkId, ChunkStore, FragmentId, Health, Result, TransientFault};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

// ---- callsite interest ---------------------------------------------------------------

/// Install a permissive global `tracing` default **once**, before any metric callsite is hit.
///
/// `tracing` caches each callsite's interest in a process-global table the first time it is
/// hit, and a first hit that races a no-subscriber default can latch the callsite
/// `Interest::never` — after which the read-back silently sees nothing and the test is flaky
/// rather than wrong. Registering against an always-enabling default first makes every
/// registration agree; each server's own metrics dispatch still routes its events into its own
/// provider. (The proven pattern from `crates/server/tests/custodian_day_one.rs:338`.)
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

// ---- reading the export surface back -------------------------------------------------

/// Every exported sample of `metric`, as `(label block, value)`.
///
/// Splits the value off the **last** whitespace rather than taking the second field, so a
/// label value containing a space cannot shift the parse. Robust to the exporter's own scope
/// decoration (`otel_scope_name`, …), which rides in the same label block.
fn samples<'a>(exposed: &'a str, metric: &str) -> Vec<(&'a str, f64)> {
    exposed
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (key, value) = line.rsplit_once(char::is_whitespace)?;
            let value: f64 = value.trim().parse().ok()?;
            let (name, labels) = match key.split_once('{') {
                Some((name, rest)) => (name, rest.strip_suffix('}').unwrap_or(rest)),
                None => (key, ""),
            };
            (name == metric).then_some((labels, value))
        })
        .collect()
}

/// The summed value of every sample of `metric` whose label block carries all of `labels`.
fn metric_sum(exposed: &str, metric: &str, labels: &[(&str, &str)]) -> f64 {
    samples(exposed, metric)
        .into_iter()
        .filter(|(block, _)| {
            labels
                .iter()
                .all(|(k, v)| block.contains(&format!("{k}=\"{v}\"")))
        })
        .map(|(_, value)| value)
        .sum()
}

/// A `monotonic_counter`'s total. The OTel→Prometheus exporter suffixes a counter `_total`;
/// accept either spelling (the `custodian_day_one.rs:441` convention).
fn counter(exposed: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    metric_sum(exposed, name, labels) + metric_sum(exposed, &format!("{name}_total"), labels)
}

/// A gauge's current value — the last sample exported for it, or `None` if it was never
/// emitted.
fn gauge(exposed: &str, name: &str) -> Option<f64> {
    samples(exposed, name).last().map(|(_, value)| *value)
}

/// Gather the export surface, retrying until `settled` holds or a budget elapses.
///
/// The metric is raised on the SERVER's connection task, which tonic/axum spawn: the client's
/// last byte can land a scheduling tick before that task finishes emitting. Polling the real
/// surface — rather than sleeping a guessed interval — keeps a green run fast and keeps a RED
/// run honest: with the production reverted `settled` never holds, the budget elapses, and the
/// caller's assertion fails on the (empty) text this returns.
async fn gather_until(telemetry: &DurabilityTelemetry, settled: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        telemetry.flush().expect("flush the meter provider");
        let exposed = telemetry
            .gather_prometheus()
            .expect("Prometheus surface configured");
        if settled(&exposed) || Instant::now() >= deadline {
            return exposed;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn prometheus_telemetry() -> DurabilityTelemetry {
    DurabilityTelemetry::new(ExporterConfig::Prometheus).expect("build the telemetry seam")
}

// ---- (a) the request plane -----------------------------------------------------------

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// A gateway whose GET always fails with a **transient** seam fault — the shape an
/// unreachable D-server fleet raises. It is a real `wyrd_traits::TransientFault`, so the
/// gateway's own `wyrd_traits::classify` (#577) classifies it exactly as it would in
/// production: the test injects the fault, it does not assert the classification.
struct FaultyGateway;

impl ObjectGateway for FaultyGateway {
    async fn put_object_streaming<S>(
        &self,
        _key: &str,
        _source: S,
        _expected: ContentHash,
        _content_type: Option<String>,
    ) -> Result<String>
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        Err(transient_fault())
    }

    async fn get_object_streaming(self: Arc<Self>, _key: &str) -> Result<Option<ObjectRead>> {
        Err(transient_fault())
    }

    async fn head_object(&self, _key: &str) -> Result<Option<ObjectMeta>> {
        Err(transient_fault())
    }

    async fn delete_object(&self, _key: &str) -> Result<bool> {
        Err(transient_fault())
    }
}

/// The bytes a mid-stream-faulting GET manages to deliver before the fault, and the size its
/// head declares. The declared size is deliberately larger: the transfer is cut short, which
/// is what makes the `200` head a promise the body then breaks.
const DELIVERED_PREFIX: &[u8] = b"the first chunk lands, and then the fleet dies";
const DECLARED_SIZE: u64 = (DELIVERED_PREFIX.len() + 4096) as u64;

/// A gateway whose GET **succeeds at the head and then fails inside the body** — a
/// `200 content-length: N`, a first chunk, and then a `TransientFault` raised mid-transfer,
/// on the test's cue.
///
/// This is the shape of a D-server that dies (or a fragment read that faults) *mid-read*, and
/// it is a genuinely different code path from [`FaultyGateway`]: there the failure is decided
/// before the head exists, so the response carries the seam's verdict out of
/// `gateway_error_response`. Here the head is built while the read is still healthy, and the
/// fault surfaces later — in the body stream, which axum polls only after the handler has
/// already returned `200` to the client.
///
/// **The fault waits for the test to release it**, which is what makes the shape deterministic
/// rather than a race. The body stream parks (`Pending`) on the release channel after its first
/// chunk; hyper flushes the head and that chunk when the body pends, so the client is genuinely
/// holding a `200` before the fault exists. A stream that errored immediately would be torn
/// down with the head still in hyper's write buffer — the client would get *nothing*, which is
/// a head-time failure wearing a mid-stream costume (verified: it puts 0 bytes on the wire).
///
/// The fault is a real `wyrd_traits::TransientFault` wrapping a real cause, so the gateway's
/// own `wyrd_traits::classify` (#577) has the same chain to walk that it would in production —
/// the test injects the fault, it never asserts the classification.
struct MidStreamFaultGateway {
    /// Fires the mid-transfer fault. Taken by the one GET this fixture serves.
    release: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

impl MidStreamFaultGateway {
    fn holding(release: oneshot::Receiver<()>) -> Self {
        Self {
            release: std::sync::Mutex::new(Some(release)),
        }
    }
}

impl ObjectGateway for MidStreamFaultGateway {
    async fn put_object_streaming<S>(
        &self,
        _key: &str,
        _source: S,
        _expected: ContentHash,
        _content_type: Option<String>,
    ) -> Result<String>
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        Err(transient_fault())
    }

    async fn get_object_streaming(self: Arc<Self>, _key: &str) -> Result<Option<ObjectRead>> {
        let release = self
            .release
            .lock()
            .expect("release channel")
            .take()
            .expect("this fixture serves exactly one GET");

        // The states of a read that starts healthy and dies partway through.
        enum Read {
            Prefix(oneshot::Receiver<()>),
            Fault(oneshot::Receiver<()>),
            Done,
        }

        let stream = futures_util::stream::unfold(Read::Prefix(release), |state| async move {
            match state {
                // Delivered while the fleet is still alive.
                Read::Prefix(release) => Some((
                    Ok(Bytes::from_static(DELIVERED_PREFIX)),
                    Read::Fault(release),
                )),
                // Parks here — hyper flushes the head + prefix to the client — until the test
                // kills the fleet under the transfer.
                Read::Fault(release) => {
                    let _ = release.await;
                    Some((Err(transient_fault()), Read::Done))
                }
                Read::Done => None,
            }
        });

        // `Ok` — the read starts healthy, so the router builds a real 200 head declaring the
        // full object length. The body then delivers only part of it.
        Ok(Some(ObjectRead {
            size: DECLARED_SIZE,
            stream: Box::pin(stream),
            // The metadata fields are additive (#503) and irrelevant to this fixture's
            // mid-stream-fault shape; a record with none behaves like a pre-#503 object.
            etag: None,
            content_type: None,
            modified: None,
        }))
    }

    async fn head_object(&self, _key: &str) -> Result<Option<ObjectMeta>> {
        Err(transient_fault())
    }

    async fn delete_object(&self, _key: &str) -> Result<bool> {
        Err(transient_fault())
    }
}

/// A transient fault wrapping a backend error, as a real backend raises it (#577: the seam
/// class WRAPS the producer's error rather than replacing it, so `classify` walks the chain).
fn transient_fault() -> BoxError {
    Box::new(TransientFault::with_source(
        "the D-server fleet is unreachable",
        Box::<dyn std::error::Error + Send + Sync>::from("connection refused"),
    ))
}

fn build_gateway(dir: &std::path::Path) -> Arc<Backend> {
    Arc::new(
        Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            FsChunkStore::open(dir).expect("fs store"),
            MemCoordination::new(),
        )
        // A small chunk size, so a modest object still spans several chunks and the GET
        // really streams (the case whose latency a head-time measurement would misreport).
        .with_chunk_size(8),
    )
}

/// Serve `gateway` over an ephemeral loopback port with `telemetry` wired as the role's
/// metrics sink — the composition `cli::serve_s3` builds for `wyrd s3`.
async fn start_s3<G>(gateway: Arc<G>, telemetry: &DurabilityTelemetry) -> SocketAddr
where
    G: ObjectGateway,
{
    let config = S3Config::new(vec![Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server =
        S3Gateway::new(gateway, config).with_metrics_dispatch(telemetry.metrics_dispatch());
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    addr
}

fn signed_headers(method: &str, path: &str, host: &str, body: &[u8]) -> Vec<(String, String)> {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, "", host, &amz_date, body, &creds, REGION, "s3",
    );
    vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ]
}

/// Write one signed HTTP/1.1 request over a fresh connection, and hand the connection back
/// ready to be read from.
async fn dispatch_request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> TcpStream {
    let host = addr.to_string();
    let headers = signed_headers(method, path, &host, body);
    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    for (name, value) in &headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n", body.len()));
    request.push_str("connection: close\r\n\r\n");

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write head");
    stream.write_all(body).await.expect("write body");
    stream.flush().await.expect("flush");
    stream
}

/// The status code off a raw response's status line.
fn status_of(raw: &[u8]) -> u16 {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    String::from_utf8_lossy(&raw[..split])
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status")
}

/// Send one signed HTTP/1.1 request over a fresh connection and **read the response to EOF**
/// — which fully drains a streaming GET's body. That is required, not incidental: the op's
/// latency sample is raised when the transfer actually ends, so a caller that read only the
/// head would read the metric back before it existed.
async fn send(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> u16 {
    let mut stream = dispatch_request(addr, method, path, body).await;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    status_of(&raw)
}

/// Read from `conn` until the response head **and** `want` body bytes have arrived, and hand
/// back the status plus those body bytes.
///
/// Reading the head off the wire before the test releases its fault is what makes the
/// mid-stream fixture the real thing rather than a hopeful one. Hyper buffers the head and the
/// body's first frames together and only flushes when the body pends (or the buffer fills), so
/// a body that errors *immediately* is torn down with the head still unflushed — the client
/// gets nothing at all, and the response never was a `200` anyone received. That is a different
/// failure shape from the one under test. Blocking here until the promise is genuinely on the
/// wire pins the fixture to the field case: a client that has been told `200 content-length: N`
/// and is reading, when the fleet dies under it.
async fn read_head_and_body_prefix(conn: &mut TcpStream, want: usize) -> (u16, Vec<u8>) {
    let mut raw = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        if let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let body = &raw[split + 4..];
            if body.len() >= want {
                return (status_of(&raw), body[..want].to_vec());
            }
        }
        let read = conn.read(&mut buf).await.expect("read the response head");
        assert!(
            read > 0,
            "the connection closed before the head and {want} body bytes arrived — the fixture \
             must put the 200 on the wire BEFORE the fault, or it is testing a head-time \
             failure; got:\n{}",
            String::from_utf8_lossy(&raw)
        );
        raw.extend_from_slice(&buf[..read]);
    }
}

/// 0010 item 4: each S3 op records a **latency measurement keyed by op**, off the role's own
/// export surface. Pre-fix no `s3_request_duration_ms` family exists at all (and the `s3` role
/// has no metrics provider to carry one), so both reads are `0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_plane_records_per_op_latency_for_a_put_and_a_get() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    let dir = tempfile::tempdir().expect("temp dir");
    let addr = start_s3(build_gateway(dir.path()), &telemetry).await;

    let object = b"observe this object's request plane, every byte of it".to_vec();
    assert_eq!(
        send(addr, "PUT", "/bucket/red-object", &object).await,
        200,
        "the signed PUT is stored"
    );
    // The GET's body is read to EOF — the op is not complete until it is.
    assert_eq!(
        send(addr, "GET", "/bucket/red-object", b"").await,
        200,
        "the signed GET returns the object"
    );

    let exposed = gather_until(&telemetry, |exposed| {
        metric_sum(exposed, "s3_request_duration_ms_count", &[("op", "get")]) >= 1.0
    })
    .await;

    // EXACTLY one sample per op — one PUT was sent, one GET was sent. An exact count is what
    // makes this bind the measurement rather than the metric's mere existence: a `>= 1` here
    // would also be satisfied by a series minted at registration time, which is precisely how
    // a latency assertion passes against a gateway that measures nothing.
    assert_eq!(
        metric_sum(&exposed, "s3_request_duration_ms_count", &[("op", "put")]),
        1.0,
        "the PUT records exactly one latency sample under op=\"put\"; got:\n{exposed}"
    );
    assert_eq!(
        metric_sum(&exposed, "s3_request_duration_ms_count", &[("op", "get")]),
        1.0,
        "the GET records exactly one latency sample under op=\"get\", once its body has \
         drained; got:\n{exposed}"
    );
    // The ops are keyed apart, not summed into one series — "which op is slow" is the whole
    // point of a per-op measurement.
    assert_eq!(
        metric_sum(
            &exposed,
            "s3_request_duration_ms_count",
            &[("op", "delete")]
        ),
        0.0,
        "no DELETE was sent, so op=\"delete\" measures nothing — the op label is a real key, \
         not decoration; got:\n{exposed}"
    );
}

/// 0010 item 4: a failing op is counted on an error counter keyed by **op** and by the
/// **typed failure class** #577 exports. Pre-fix no `s3_request_errors` family exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_plane_counts_a_failing_op_by_op_and_by_the_typed_class() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    let addr = start_s3(Arc::new(FaultyGateway), &telemetry).await;

    // A signed, well-formed GET whose BACKEND fails transiently — a real fault through the
    // real router, not a hand-made status code.
    assert_eq!(
        send(addr, "GET", "/bucket/unreachable", b"").await,
        500,
        "a transient backend fault is answered 500 (the S3 contract has no `unknown` code)"
    );

    let exposed = gather_until(&telemetry, |exposed| {
        counter(
            exposed,
            "s3_request_errors",
            &[("op", "get"), ("class", "transient")],
        ) >= 1.0
    })
    .await;

    assert!(
        counter(
            &exposed,
            "s3_request_errors",
            &[("op", "get"), ("class", "transient")]
        ) >= 1.0,
        "the failing GET is counted under op=\"get\" AND the stable class label #577 exports \
         for a TransientFault; got:\n{exposed}"
    );
    // The class must be the SEAM's verdict, not the wire status. A 500 maps from both a
    // transient fault and a may-have-landed commit, so a counter keyed off the status could
    // never have told these apart — which is why item 4 keys on item 6's typed class.
    assert_eq!(
        counter(
            &exposed,
            "s3_request_errors",
            &[("op", "get"), ("class", "terminal")]
        ),
        0.0,
        "a transient fault is NOT counted as terminal — the class comes from wyrd_traits::classify \
         walking the error's source chain, not from the 500 on the wire; got:\n{exposed}"
    );
}

/// 0010 item 4, on the path where the class is **hardest to get right**: a GET whose head
/// already went out `200` and whose body then fails.
///
/// The class is not decoration on this counter — it is the counter's reason to exist. An
/// operator pages on `class="transient"` (the fleet is wobbling, retry and it may pass) and
/// files a bug on `class="terminal"` (retrying cannot help). A mid-stream fault reported
/// `terminal` is therefore not a cosmetic mislabel: it is the counter giving its most
/// confident wrong answer about the failure shape it most needs to name, and it does so for
/// **exactly** the transfers a real fleet fails on — long streaming reads, where a D-server has
/// time to die after the head is built.
///
/// The trap is structural. A streaming GET's head is built before a byte is read, so it carries
/// no seam error to stamp, and a class captured at head time can only be the `Terminal`
/// fail-safe default. The fault arrives later, in the body stream. Nothing about a head-time
/// read is *wrong* — head time is simply too early to know, so the verdict must be refined
/// where the error actually surfaces.
///
/// Note what this asserts against: [`FaultyGateway`]'s test above passes with the class read
/// only at head time, because its fault IS decided at head time. This one cannot — which is
/// why it is here and not folded into that test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_request_plane_classes_a_mid_stream_fault_by_the_seam_not_the_head() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    let (kill_the_fleet, fault) = oneshot::channel();
    let addr = start_s3(Arc::new(MidStreamFaultGateway::holding(fault)), &telemetry).await;

    let mut conn = dispatch_request(addr, "GET", "/bucket/dies-mid-read", b"").await;

    // The client is holding a `200` and a partial body BEFORE the fault exists. If this ever
    // read 500, the fixture would have degenerated into `FaultyGateway` — whose class is
    // already right at head time — and everything below would prove nothing about the path
    // this test exists for.
    let (status, prefix) = read_head_and_body_prefix(&mut conn, DELIVERED_PREFIX.len()).await;
    assert_eq!(
        status, 200,
        "the head must succeed: this test is about a fault raised AFTER the response is \
         already promised"
    );
    assert_eq!(
        prefix, DELIVERED_PREFIX,
        "the transfer is genuinely under way — real bytes of the object reached the client"
    );

    // The fleet dies, mid-transfer.
    kill_the_fleet
        .send(())
        .expect("the body stream is still reading — nothing else can have failed yet");

    // Drain what is left, tolerating the tear-down: the head promised a `content-length` that
    // will now never be met, so hyper's only truthful move is to drop the connection
    // unterminated, and a client's read may surface that as a reset rather than a clean EOF.
    // Insisting on a clean read here would flake on the very fault being injected.
    let mut rest = Vec::new();
    let _ = conn.read_to_end(&mut rest).await;
    assert!(
        ((prefix.len() + rest.len()) as u64) < DECLARED_SIZE,
        "the body must stop short of the {DECLARED_SIZE} bytes the head declared — a complete \
         body would mean the fault never landed"
    );

    let exposed = gather_until(&telemetry, |exposed| {
        counter(exposed, "s3_request_errors", &[("op", "get")]) >= 1.0
    })
    .await;

    // The transfer failed, so it is an error at all: a 200 head is not a served request when
    // the client never got the object.
    assert!(
        counter(
            &exposed,
            "s3_request_errors",
            &[("op", "get"), ("class", "transient")]
        ) >= 1.0,
        "a GET torn down by a mid-stream TransientFault is counted under class=\"transient\" — \
         the class of the error that ACTUALLY ended the transfer, which is the only one an \
         operator can act on; got:\n{exposed}"
    );
    // The head-time default must not survive to the counter. This is the assertion that fails
    // when the class is captured once, at head time: the extension is absent on a 200, so the
    // sample lands on `terminal` and tells the operator to stop retrying a transient fault.
    assert_eq!(
        counter(
            &exposed,
            "s3_request_errors",
            &[("op", "get"), ("class", "terminal")]
        ),
        0.0,
        "the mid-stream fault must NOT be counted terminal: the `Terminal` the head defaulted to \
         is what `classify` returns for an error it cannot see, and at head time it could not \
         see this one; got:\n{exposed}"
    );
}

// ---- (b) the capacity plane ----------------------------------------------------------

/// A `ChunkStore` whose `get_fragment` gates: it signals that a request was admitted into the
/// handler, then parks until the test releases it — so an admitted request holds its
/// admission slot for exactly as long as the test needs. (The `dserver.rs:176` fixture.)
struct GateStore {
    inner: FsChunkStore,
    entered: mpsc::UnboundedSender<()>,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl ChunkStore for GateStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        let _ = self.entered.send(());
        let _permit = self.gate.acquire().await.expect("gate not closed");
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
    }
}

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Bind, register and serve one D server over a gated store with `admission` in force and
/// `telemetry` wired as the role's metrics sink — the composition `cli::run_d_server` builds
/// for `wyrd d-server`.
async fn serve_gated(
    admission: AdmissionControl,
    telemetry: &DurabilityTelemetry,
) -> (
    String,
    Arc<Semaphore>,
    mpsc::UnboundedReceiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    let (entered_tx, entered_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0)); // closed: handlers park until released
    let gate_store = GateStore {
        inner: store,
        entered: entered_tx,
        gate: gate.clone(),
    };

    let coord = Arc::new(MemCoordination::new());
    let server = DServer::bind(gate_store, "127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind")
        .with_admission_control(admission)
        .with_metrics_dispatch(telemetry.metrics_dispatch());
    let endpoint = server.endpoint().to_string();
    let lease = server
        .register(&*coord, DSERVER_GROUP, Duration::from_secs(3600))
        .await
        .expect("register");
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(
        server.serve(coord, lease, Duration::from_secs(3600), async move {
            let _ = rx.await;
        }),
    );
    (endpoint, gate, entered_rx, tx, handle, dir)
}

/// 0010 item 5: the admitted event fires on the happy path, and the **in-flight RPC gauge
/// rises while requests are held open and RETURNS TO ZERO** once they complete.
///
/// A gauge is load-bearing here for the reason the durability backlog needed one: in-flight is
/// a *level*, and only a gauge comes back down through an accumulating Prometheus registry — a
/// monotonic counter would export `..._total` and stay pinned at its peak forever.
///
/// Pre-fix no `capacity_requests_*` family exists, so the gauge reads `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_capacity_plane_reports_admission_and_the_in_flight_gauge_returns_to_zero() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    // A roomy bound: nothing is shed here, so the ONLY thing under test is admission itself.
    let admission = AdmissionControl {
        max_concurrent_requests: 8,
        request_timeout: Duration::from_secs(60),
        ..AdmissionControl::default()
    };
    let (endpoint, gate, mut entered_rx, shutdown, handle, _dir) =
        serve_gated(admission, &telemetry).await;

    let client_a = GrpcChunkStore::connect(endpoint.clone())
        .await
        .expect("connect A");
    let client_b = GrpcChunkStore::connect(endpoint).await.expect("connect B");
    let id = fid(0x1_F1E, 0);

    // TWO requests admitted and held open in the handler — each holds a real admission slot.
    let held_a = tokio::spawn(async move { client_a.get_fragment(id).await });
    let held_b = tokio::spawn(async move { client_b.get_fragment(id).await });
    entered_rx.recv().await.expect("first request admitted");
    entered_rx.recv().await.expect("second request admitted");

    let exposed = gather_until(&telemetry, |exposed| {
        gauge(exposed, "capacity_requests_in_flight") == Some(2.0)
    })
    .await;
    assert!(
        counter(&exposed, "capacity_requests_admitted", &[]) >= 2.0,
        "each admitted request raises an admitted event; got:\n{exposed}"
    );
    assert_eq!(
        gauge(&exposed, "capacity_requests_in_flight"),
        Some(2.0),
        "the in-flight gauge RISES to 2 while both requests are held open in the handler; \
         got:\n{exposed}"
    );

    // Release the gate: both requests complete and give their slots back.
    gate.add_permits(8);
    let _ = held_a.await.expect("join A");
    let _ = held_b.await.expect("join B");

    let exposed = gather_until(&telemetry, |exposed| {
        gauge(exposed, "capacity_requests_in_flight") == Some(0.0)
    })
    .await;
    assert_eq!(
        gauge(&exposed, "capacity_requests_in_flight"),
        Some(0.0),
        "and it RETURNS TO ZERO once they complete — the level comes back down, which is the \
         whole reason it is a gauge and not a counter; got:\n{exposed}"
    );
    assert_eq!(
        counter(&exposed, "capacity_requests_shed", &[]),
        0.0,
        "nothing was shed: the bound was never reached, so admission behaviour is unchanged"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
}

/// 0010 item 5 DoD: a **forced load-shed is observable as an EVENT**, not merely as a status
/// code the client happened to keep. This is the signal the d-server never had — it sheds
/// today with no server-side record at all.
///
/// The overload is driven across two SEPARATE connections against a server-wide bound of 1,
/// so what is shed is the binding server-wide limit (`AdmissionControl::max_concurrent_requests`)
/// rather than a per-connection cap. Pre-fix no `capacity_requests_shed` family exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_capacity_plane_reports_a_forced_load_shed() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    // Server-wide bound of 1, a roomy per-connection cap, and a long timeout — so the shed is
    // the only thing that can answer the excess request.
    let admission = AdmissionControl {
        max_concurrent_requests: 1,
        max_concurrent_requests_per_connection: 64,
        request_timeout: Duration::from_secs(60),
        ..AdmissionControl::default()
    };
    let (endpoint, gate, mut entered_rx, shutdown, handle, _dir) =
        serve_gated(admission, &telemetry).await;

    let client_a = GrpcChunkStore::connect(endpoint.clone())
        .await
        .expect("connect A");
    let client_b = GrpcChunkStore::connect(endpoint).await.expect("connect B");
    let id = fid(0x5_1ED, 0);

    // A holds the one slot.
    let admitted = tokio::spawn(async move { client_a.get_fragment(id).await });
    entered_rx.recv().await.expect("A holds the single slot");

    // B is over the server-wide bound: it is shed, promptly.
    let excess = tokio::time::timeout(Duration::from_secs(5), client_b.get_fragment(id))
        .await
        .expect("the over-limit request is answered (shed) within the budget");
    excess.expect_err("an over-limit request is shed, not served a value");

    let exposed = gather_until(&telemetry, |exposed| {
        counter(exposed, "capacity_requests_shed", &[]) >= 1.0
    })
    .await;
    assert!(
        counter(&exposed, "capacity_requests_shed", &[]) >= 1.0,
        "the forced load-shed is raised as a server-side EVENT — pre-fix it was visible only \
         as the client's RESOURCE_EXHAUSTED; got:\n{exposed}"
    );
    // The shed request was never admitted, so it must never have touched the in-flight
    // gauge: the observer sits INSIDE the concurrency limit precisely so a rejected request
    // cannot show up as load the server accepted.
    assert_eq!(
        counter(&exposed, "capacity_requests_admitted", &[]),
        1.0,
        "exactly ONE request was admitted (A); the shed request B was not; got:\n{exposed}"
    );

    gate.add_permits(8);
    let _ = admitted.await;
    let _ = shutdown.send(());
    let _ = handle.await;
}

/// 0010 item 5: a request that exceeds `request_timeout` raises a **timed-out** event.
///
/// The handler parks forever (the gate is never opened) under a short, operator-tunable
/// timeout, so the cut is deterministic rather than a race. Pre-fix no
/// `capacity_requests_timed_out` family exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_capacity_plane_reports_a_request_cut_by_the_request_timeout() {
    enable_metric_callsites();
    let telemetry = prometheus_telemetry();
    // A short request timeout; a wide admission bound so nothing is shed — the deadline cut
    // is the only thing that can answer.
    let admission = AdmissionControl {
        max_concurrent_requests: 64,
        request_timeout: Duration::from_millis(200),
        ..AdmissionControl::default()
    };
    let (endpoint, _gate, mut entered_rx, shutdown, handle, _dir) =
        serve_gated(admission, &telemetry).await;

    let client = GrpcChunkStore::connect(endpoint).await.expect("connect");
    let outcome = tokio::time::timeout(Duration::from_secs(5), client.get_fragment(fid(0xDEAD, 0)))
        .await
        .expect("a hung handler is cut by the request timeout within the budget");
    outcome.expect_err("a timed-out request returns an error status, not a value");
    // It really was in-flight when the deadline cut it — it reached the handler and parked.
    entered_rx
        .recv()
        .await
        .expect("the request reached the handler before being cut");

    let exposed = gather_until(&telemetry, |exposed| {
        counter(exposed, "capacity_requests_timed_out", &[]) >= 1.0
    })
    .await;
    assert!(
        counter(&exposed, "capacity_requests_timed_out", &[]) >= 1.0,
        "a handler past request_timeout raises a timed-out event; got:\n{exposed}"
    );
    // A request the SERVER's deadline cut is a timeout, not a client walking away — the two
    // are separate series because the operator response differs.
    assert_eq!(
        counter(&exposed, "capacity_requests_cancelled", &[]),
        0.0,
        "the cut is attributed to the server's request timeout, not to a cancellation; \
         got:\n{exposed}"
    );
    // The cut request released its slot: a timeout that leaked the gauge would defeat the
    // point of a bound whose whole job is that a hung handler cannot pin one.
    let exposed = gather_until(&telemetry, |exposed| {
        gauge(exposed, "capacity_requests_in_flight") == Some(0.0)
    })
    .await;
    assert_eq!(
        gauge(&exposed, "capacity_requests_in_flight"),
        Some(0.0),
        "the cut request freed its admission slot, so the in-flight gauge returns to zero; \
         got:\n{exposed}"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
}
