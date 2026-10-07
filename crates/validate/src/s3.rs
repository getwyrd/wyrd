//! The S3 client every validator scenario calls through (proposal 0017 §6-7).
//!
//! It is `aws-sdk-s3` pointed at an arbitrary `--endpoint`: path-style addressing, static
//! SigV4 credentials, a plain-HTTP connector (TLS is terminated in front of the gateway,
//! proposal 0017 §10), and retries and stalled-stream protection **off**, so every request
//! the validator reports on is exactly one request on the wire. On top of the SDK it adds
//! four things:
//!
//! * a typed error, [`S3Error`], whose every fact is the server's or the client's own
//!   measurement — in particular the request id is read from the `x-amz-request-id` header
//!   itself, because the SDK's generic accessor prefers `x-amzn-requestid` when both are
//!   present;
//! * bounded waits ([`Deadlines`]): connect, operation, body-idle, and whole body, each
//!   expiring as a typed timeout naming its phase;
//! * bodies that stream both ways ([`PutSource`], [`ObjectBody`]), so what the client holds
//!   of an object is bounded independently of the object's size;
//! * PUTs that end with their call ([`S3Client::put_object`]): a receipt only when the source
//!   had ended before the answer, and neither the source nor the connection left behind when
//!   the call returns.

mod body;
mod error;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    ConfigBag, Credentials as SdkCredentials, Intercept, Region, RuntimeComponents,
    StalledStreamProtectionConfig,
};
use aws_sdk_s3::error::{BoxError, DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use futures_util::future::{self, Either};

pub use body::{ObjectBody, PutSource};
pub use error::{BodyError, ErrorCode, Phase, S3Error};

use body::Upload;

use crate::ResolvedConfig;

/// The response header the request id is read from. The name is AWS's; the Wyrd gateway
/// stamps it on every response.
const REQUEST_ID_HEADER: &str = "x-amz-request-id";

/// The bound on every wait for the endpoint. All four run on tokio's runtime clock: the
/// SDK's connect and operation timeouts sleep on its default tokio sleep, and the body-idle
/// and whole-body deadlines are a `tokio::time::timeout`. A PUT's two deadlines sleep on the
/// timer of the PUT's own runtime ([`S3Client::put_object`]), which reads the same monotonic
/// clock, so every deadline of one request still has one source. The SDK also reads the wall
/// clock, to date SigV4 signatures; that stamp belongs to the server's freshness check, not to any
/// deadline here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    /// Establishing a TCP connection.
    pub connect: Duration,
    /// One whole request until its response head is read and deserialized. For a PUT that
    /// covers the upload, so the default is sized for a large object rather than a small
    /// one.
    pub operation: Duration,
    /// The wait for each next piece of a GET body.
    pub body_idle: Duration,
    /// A whole GET body, from the moment its response head is read to its end. The operation
    /// deadline ends with the response head, and the idle deadline restarts at every piece,
    /// so without this a peer that trickles a byte inside every idle window could hold a
    /// worker for as long as its declared length lasts. The default matches `operation`:
    /// the same object read back gets the budget its upload had.
    pub body: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            operation: Duration::from_secs(15 * 60),
            body_idle: Duration::from_secs(60),
            body: Duration::from_secs(15 * 60),
        }
    }
}

/// What a successful PUT reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// The `ETag` the server returned, if it returned one.
    pub etag: Option<String>,
}

/// An S3 client for the bucket and endpoint of one resolved configuration.
#[derive(Debug, Clone)]
pub struct S3Client {
    sdk: aws_sdk_s3::Client,
    /// The same configuration without an idle-connection pool, for PUTs: a PUT's connection
    /// is closed when the PUT returns ([`S3Client::put_object`]), so none is kept for reuse.
    uploads: aws_sdk_s3::Client,
    bucket: String,
    deadlines: Deadlines,
}

impl S3Client {
    /// A client for `config`'s endpoint, region, bucket and credentials, with the default
    /// [`Deadlines`].
    pub fn new(config: &ResolvedConfig) -> Self {
        Self::with_deadlines(config, Deadlines::default())
    }

    /// As [`S3Client::new`], with explicit deadlines.
    pub fn with_deadlines(config: &ResolvedConfig, deadlines: Deadlines) -> Self {
        // Every timeout is set or disabled explicitly, so none comes from the SDK's
        // behaviour-version defaults. The connector carries only the connect deadline, which
        // is what lets a connector-level timeout be reported as the connect phase.
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(deadlines.connect)
            .disable_read_timeout()
            .operation_timeout(deadlines.operation)
            .disable_operation_attempt_timeout()
            .build();
        let credentials = SdkCredentials::new(
            config.credentials.access_key_id(),
            config.credentials.secret_access_key(),
            None,
            None,
            "wyrd-validate",
        );
        let sdk_config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new(config.args.region.clone()))
            .endpoint_url(config.args.endpoint.as_str())
            .credentials_provider(credentials)
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .timeout_config(timeouts);
        let sdk = sdk_config
            .clone()
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build();
        let uploads = sdk_config
            .http_client(
                aws_smithy_http_client::Builder::new()
                    .pool_max_idle_per_host(0)
                    .build_http(),
            )
            .build();
        Self {
            sdk: aws_sdk_s3::Client::from_conf(sdk),
            uploads: aws_sdk_s3::Client::from_conf(uploads),
            bucket: config.args.bucket.clone(),
            deadlines,
        }
    }

    /// PUT `key` from `source`, streaming it: the source is pulled only as fast as the
    /// connection drains.
    ///
    /// **A receipt.** The PUT is a receipt only if the source had **ended** when the response
    /// head arrived: it had given its whole declared length and then reported its end. For
    /// the client the response arrives at the moment hyper reads it off the socket and hands
    /// it over. A success that arrives sooner is [`BodyError::AcknowledgedEarly`], carrying
    /// the answer's request id. Having given the whole declared length is not enough: until
    /// the source reports its end, the SDK has not written the body's final chunk and
    /// checksum trailer and may still be holding the last bytes back, and the source may
    /// still run past its declared length. From the moment hyper hands the response over the
    /// source is not polled again, so whatever it would have done after that moment (reported
    /// its end, given another piece, failed, run past its declared length) counts for nothing.
    /// A source that runs past its declared length before the response arrives is still
    /// [`BodyError::SourceLength`].
    ///
    /// **The limit.** The client sees two things: what it handed to the HTTP stack, and the
    /// moment hyper reads the answer off the socket. It cannot see whether the server read
    /// the bytes it was handed: when the source ended first, a success is a receipt even if
    /// some of those bytes were still in the client's or the kernel's buffers as the server
    /// answered, or the server read them and threw them away. And it cannot see an answer
    /// that has reached its own kernel's receive buffer but that hyper has not read yet: a
    /// source that ends inside that window turns an answer the server sent early into a
    /// receipt. Closing the window needs the socket, which the SDK's connector does not
    /// expose.
    ///
    /// Nothing detects a false receipt of either kind today: no scenario drives this client
    /// yet. Once scenarios do, the read-back check of proposal 0017's oracle will catch one
    /// only on a single-writer key that is read back before its next successful overwrite.
    /// That read hashes to a prior generation, or is a `404`: a fatal `integrity` or `oracle`
    /// failure. It will not catch every one. On the contention pool a read may return any
    /// value written to the key, and the key may settle on any of them, so a concurrent PUT
    /// the server never got can leave another permitted value and no mismatch. A
    /// single-writer write that is overwritten before anything reads it back leaves no trace
    /// either.
    ///
    /// **Nothing outlives the call.** When this returns, the source has been dropped and the
    /// connection closed, whatever the server did: answered early, stopped reading, or both.
    /// If the future is dropped instead, both are released promptly after. The request runs
    /// on a current-thread runtime of its own, on a thread of its own, and that runtime is
    /// shut down before the outcome is handed back. Shutting it down drops every task on it,
    /// hyper's connection task among them, and with that task its socket and the request
    /// body. Nothing else can release a connection blocked mid-write: it waits only for its
    /// socket to drain, never polls the body again, and the SDK's connector exposes neither
    /// the socket nor the task.
    ///
    /// **The cost, per PUT.** One thread for its length, and a fresh TCP connection that the
    /// client closes: no connection is reused across PUTs, and each one leaves a client
    /// socket in `TIME_WAIT`. That caps sustained PUTs between one client address and one
    /// gateway address at about the number of ephemeral ports divided by the `TIME_WAIT`
    /// time: about 470 a second with Linux's defaults (28,232 ports, 60 s). Past that,
    /// `connect` fails and the PUT is [`S3Error::NoResponse`], which proposal 0017's taxonomy
    /// would count against the server as `availability`. The cap is a default of the host,
    /// not a hard ceiling. It holds per pair of client address and gateway address, shared by
    /// every worker on that client, and an operator raises it with more client or gateway
    /// addresses, a wider `net.ipv4.ip_local_port_range`, or `net.ipv4.tcp_tw_reuse = 1`
    /// (Linux's default, `2`, reuses ports on loopback only).
    pub async fn put_object(&self, key: &str, source: PutSource) -> Result<PutOutcome, S3Error> {
        let length = i64::try_from(source.length()).map_err(|_| S3Error::RequestNotBuilt {
            detail: format!(
                "declared length {} does not fit a Content-Length",
                source.length()
            ),
        })?;
        let (body, upload) = source.into_body();
        let request = self
            .uploads
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(length)
            .body(ByteStream::from_body_1_x(body))
            .customize()
            .interceptor(ResponseArrival(Arc::clone(&upload)))
            .send();
        let sent = on_own_runtime(upload.request_first(request)).await?;
        // The source has been dropped and the connection closed by here.
        if sent.is_ok() {
            if let Some(early) = upload.acknowledged_early() {
                return Err(S3Error::Body(early));
            }
        }
        // A failure of the caller's source outranks however the SDK and hyper reported the
        // aborted request: it is the cause, and the server never saw a complete body.
        if let Some(failure) = upload.failure() {
            return Err(S3Error::Body(failure.clone()));
        }
        match sent {
            Ok(output) => Ok(PutOutcome {
                etag: output.e_tag().map(str::to_owned),
            }),
            Err(e) => Err(self.classify(e)),
        }
    }

    /// GET `key`. The returned body is read piece by piece with
    /// [`ObjectBody::next_piece`]; nothing is read ahead of the caller.
    pub async fn get_object(&self, key: &str) -> Result<ObjectBody, S3Error> {
        let output = self
            .sdk
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| self.classify(e))?;
        // deferred: #853 — close-delimited and chunked GET framing. Without a declared
        // length a cut connection is indistinguishable from the end of the object, so such a
        // body is refused rather than trusted.
        let Some(declared) = output.content_length().and_then(|n| u64::try_from(n).ok()) else {
            return Err(S3Error::Body(BodyError::LengthUndeclared));
        };
        Ok(ObjectBody::new(
            output.body,
            declared,
            self.deadlines.body_idle,
            self.deadlines.body,
        ))
    }

    /// DELETE `key`.
    pub async fn delete_object(&self, key: &str) -> Result<(), S3Error> {
        self.sdk
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| self.classify(e))
    }

    /// Map an SDK failure onto the one [`S3Error`] variant for where it happened.
    fn classify<E>(&self, err: SdkError<E, HttpResponse>) -> S3Error
    where
        E: ProvideErrorMetadata + std::error::Error + 'static,
    {
        let detail = DisplayErrorContext(&err).to_string();
        match &err {
            SdkError::ConstructionFailure(_) => S3Error::RequestNotBuilt { detail },
            SdkError::TimeoutError(_) => S3Error::Timeout {
                phase: Phase::Operation,
                limit: self.deadlines.operation,
            },
            // The connector's only timeout is the connect deadline (`with_deadlines`). Every
            // other dispatch failure (a refused or reset connection, a scheme the plain-HTTP
            // connector cannot speak) has no response and falls through to the last arm.
            SdkError::DispatchFailure(failure) if failure.is_timeout() => S3Error::Timeout {
                phase: Phase::Connect,
                limit: self.deadlines.connect,
            },
            SdkError::ServiceError(context) => {
                let raw = context.raw();
                let status = raw.status().as_u16();
                let request_id = request_id(raw);
                // deferred: #853 — non-conforming error responses. The SDK reads the error
                // body whole, with no byte budget. An empty error body is classified below
                // (`NoBody`) but exercised only there. An `<Error>` document that names no
                // `<Code>` is not yet told apart from a body that is not an `<Error>` document
                // at all; both are reported as unreadable until then.
                match (raw.body().bytes(), context.err().code()) {
                    // Checked before the SDK's code: it fills in `NotFound` for a bodiless
                    // 404, which the server never sent.
                    (Some([]), _) => S3Error::Service {
                        status,
                        code: ErrorCode::NoBody,
                        message: None,
                        request_id,
                    },
                    (_, Some(code)) => S3Error::Service {
                        status,
                        code: ErrorCode::Code(code.to_owned()),
                        message: context.err().message().map(str::to_owned),
                        request_id,
                    },
                    (_, None) => S3Error::Unreadable {
                        status,
                        request_id,
                        detail,
                    },
                }
            }
            // Everything else is classified by whether a response arrived: a dispatch failure
            // has none; a response the SDK could not deserialize (`ResponseError`) has one.
            // `SdkError` is non-exhaustive, so a future variant lands here too.
            // deferred: #853 — a conforming server sends no unreadable response, so only
            // #853's non-conforming responses reach the `Unreadable` side of this arm.
            _ => match err.raw_response() {
                Some(raw) => S3Error::Unreadable {
                    status: raw.status().as_u16(),
                    request_id: request_id(raw),
                    detail,
                },
                None => S3Error::NoResponse { detail },
            },
        }
    }
}

/// The `x-amz-request-id` header, read directly.
fn request_id(raw: &HttpResponse) -> Option<String> {
    raw.headers().get(REQUEST_ID_HEADER).map(str::to_owned)
}

/// Run `request` on a current-thread runtime of its own, on a thread of its own, and shut
/// that runtime down before handing back the outcome. hyper spawns each connection's task
/// onto the runtime it is called from (the SDK's connector hands it tokio's executor), so the
/// task lands on this runtime, and shutting it down drops the task, its socket and whatever
/// request body it holds, on this thread, before the outcome is sent.
///
/// If this future is dropped first, the thread sees its abandon signal close, stops polling
/// `request` and shuts the runtime down all the same.
///
/// On every path `request`, and the body it owns, is dropped before the outcome is handed
/// back: if the runtime cannot be built, the thread drops `request` before it reports that;
/// if the thread cannot be started, `std` has dropped the thread's closure, `request` with
/// it, by the time `spawn` returns the error.
///
/// The runtime is built on that thread, never here: dropping a runtime that never ran, on
/// the error path of a thread that failed to start, would happen inside the caller's async
/// context, where tokio refuses to drop one.
async fn on_own_runtime<F>(request: F) -> Result<F::Output, S3Error>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    // Never sent on: the receiver resolves when this future, and the sender with it, drops.
    let (_abandon, abandoned) = tokio::sync::oneshot::channel::<()>();
    std::thread::Builder::new()
        .name("wyrd-validate-put".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    // Released first: the caller returns as soon as the error arrives.
                    drop(request);
                    let _ = outcome_tx.send(Err(format!("cannot start the PUT's runtime: {e}")));
                    return;
                }
            };
            let outcome = runtime.block_on(async move {
                match future::select(std::pin::pin!(request), abandoned).await {
                    Either::Left((outcome, _)) => Some(outcome),
                    Either::Right(_) => None,
                }
            });
            // Drops every task on the runtime before returning. Blocking work (a DNS lookup
            // still in `getaddrinfo`) is left to finish on its own rather than waited for.
            runtime.shutdown_background();
            if let Some(outcome) = outcome {
                let _ = outcome_tx.send(Ok(outcome));
            }
        })
        .map_err(|e| S3Error::RequestNotBuilt {
            detail: format!("cannot start the PUT's thread: {e}"),
        })?;
    // The thread sends unless it was abandoned, which needs `_abandon` dropped, or it
    // panicked, in which case the panic has been reported on that thread already.
    outcome_rx
        .await
        .expect("the PUT's thread panicked before it had an outcome")
        .map_err(|detail| S3Error::RequestNotBuilt { detail })
}

/// Records on a PUT's [`Upload`] that its request has seen the response head. The SDK calls
/// `read_after_transmit` in the same poll in which the connector's future yields the
/// response, with no await in between, and before it reads the response body
/// (`aws-smithy-runtime` 1.15.0, `client/orchestrator.rs:504-511`). The body takes nothing
/// from hyper's handing the response over until this poll ends ([`Upload::request_first`]),
/// so what is recorded here, whether the source had ended, is how it stood at the response's
/// arrival.
#[derive(Debug)]
struct ResponseArrival(Arc<Upload>);

impl Intercept for ResponseArrival {
    fn name(&self) -> &'static str {
        "wyrd-validate PUT response arrival"
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.0.answered(request_id(context.response()));
        Ok(())
    }
}
