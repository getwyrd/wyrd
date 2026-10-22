//! The S3 client every validator scenario calls through (proposal 0017 §6-7).
//!
//! It is `aws-sdk-s3` pointed at an arbitrary `--endpoint`: path-style addressing, static
//! SigV4 credentials, and retries and stalled-stream protection **off**, so every request the
//! validator reports on is exactly one request on the wire. On top of the SDK it adds seven
//! things:
//!
//! * a plain-HTTP connector of its own (TLS is terminated in front of the gateway, proposal
//!   0017 §10), with one connection per request, which counts what the endpoint sent after
//!   each response's end: anything at all fails the response (`s3/wire.rs`);
//! * a typed error, [`S3Error`], whose every fact is the server's or the client's own
//!   measurement — in particular the request id is read from the `x-amz-request-id` header
//!   itself, because the SDK's generic accessor prefers `x-amzn-requestid` when both are
//!   present;
//! * a strict reading of every response: one that is not a whole, well-formed, correctly
//!   framed S3 response is never reported as one — not as a clean S3 error, not as a
//!   success, not as a shorter object. An error body is read by the client itself, not the
//!   SDK: empty is "no body", an `<Error>` without a `<Code>`, or with an empty one, is "no
//!   code in body", and anything but one whole `<Error>` document whose `<Code>` and
//!   `<Message>` hold only text is [`S3Error::Unreadable`]. Framing that cannot mark where a
//!   body ends is refused: both `Transfer-Encoding` and `Content-Length`, a transfer coding
//!   other than exactly `chunked`, a `204` that frames a body, a `Transfer-Encoding` on a
//!   `304` to a PUT or a DELETE, or neither header on a GET object. So are bytes after the
//!   end the framing marks, a PUT or DELETE success that carries a body, and a success the
//!   SDK cannot read, with the SDK's diagnostic kept;
//! * a byte budget, [`BUFFERED_BODY_BUDGET`] (64 KiB), on every body the SDK buffers — every
//!   error response and every PUT or DELETE response — counted as the body arrives, so a body
//!   with no declared length is cut off at the budget rather than read whole;
//! * bounded waits ([`Deadlines`]): connect, operation, body-idle, and whole body, each
//!   expiring as a typed timeout naming its phase;
//! * bodies that stream both ways ([`PutSource`], [`ObjectBody`]), so what the client holds
//!   of an object is bounded independently of the object's size. A GET object is outside the
//!   budget: it streams, whether it is framed by a `Content-Length` or chunked;
//! * PUTs that end with their call ([`S3Client::put_object`]): a receipt only when the source
//!   had ended before the answer, and neither the source nor the connection left behind when
//!   the call returns.

mod body;
mod error;
mod response;
mod wire;

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
use aws_sdk_s3::error::{BoxError, DisplayErrorContext, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use futures_util::future::{self, Either};

pub use body::{ObjectBody, PutSource};
pub use error::{BodyError, ErrorCode, Phase, S3Error};
pub use response::BUFFERED_BODY_BUDGET;

use body::Upload;
use response::{error_document, Framing, ResponseGuard};
use wire::WireClient;

use crate::ResolvedConfig;

/// The response header the request id is read from. The name is AWS's; the Wyrd gateway
/// stamps it on every response.
const REQUEST_ID_HEADER: &str = "x-amz-request-id";

/// The bound on every wait for the endpoint. All four run on tokio's runtime clock: the
/// SDK's operation timeout sleeps on its default tokio sleep, and the connect, body-idle and
/// whole-body deadlines are a `tokio::time::timeout`. A PUT's two deadlines sleep on the
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
    /// The SDK client, on the client's own connector: no connection is kept for reuse
    /// (`s3/wire.rs`), and a PUT's is closed when the PUT returns ([`S3Client::put_object`]).
    sdk: aws_sdk_s3::Client,
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
        let sdk = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new(config.args.region.clone()))
            .endpoint_url(config.args.endpoint.as_str())
            .credentials_provider(credentials)
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .timeout_config(timeouts)
            .http_client(WireClient)
            .build();
        Self {
            sdk: aws_sdk_s3::Client::from_conf(sdk),
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
    /// the answer's request id, whether or not its body is then read whole. Having given the
    /// whole declared length is not enough: until the source reports its end, the SDK has not
    /// written the body's final chunk and checksum trailer and may still be holding the last
    /// bytes back, and the source may still run past its declared length. From the moment
    /// hyper hands the response over the
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
    /// receipt. Closing the window would need the connector to read the socket ahead of
    /// hyper, which it does not: it looks at the socket only once a response has ended
    /// (`s3/wire.rs`).
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
    /// the connection's task among them, and with that task its socket and the request body.
    /// A connection blocked mid-write waits only for its socket to drain and never polls the
    /// body again. The connector also ends its task when the response is read to its end or
    /// dropped (`s3/wire.rs`), but only the runtime's shutdown covers every path.
    ///
    /// **The cost, per PUT.** One thread for its length, and, as for every request the
    /// client makes (`s3/wire.rs`), a fresh TCP connection that the client closes: no
    /// connection is reused, and each one leaves a client socket in `TIME_WAIT`. That caps
    /// sustained requests (PUT, GET and DELETE requests together) between one client address
    /// and one gateway address at about the number of ephemeral ports divided by the
    /// `TIME_WAIT` time: about 470 a second with Linux's defaults (28,232 ports, 60 s). Past
    /// that, `connect` fails and the request is [`S3Error::NoResponse`], which proposal 0017's
    /// taxonomy would count against the server as `availability`. The cap is a default of the
    /// host, not a hard ceiling. It holds per pair of client address and gateway address, shared by
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
            .sdk
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(length)
            .body(ByteStream::from_body_1_x(body))
            .customize()
            .interceptor(ResponseArrival(Arc::clone(&upload)))
            .interceptor(ResponseGuard::buffered())
            .send();
        let sent = on_own_runtime(upload.request_first(request)).await?;
        // The source has been dropped and the connection closed by here.
        //
        // Whether an answer came early is settled by its head: a success head that arrived
        // before the source had ended is the early acknowledgement, whether its body is then
        // read whole or refused while being read (a PUT success carries no body, so any is
        // refused), since that body came after the head. A head the SDK cannot read is no
        // success head, and stays `Unreadable` with the SDK's diagnostic.
        let success_head = match &sent {
            Ok(_) => true,
            Err(SdkError::ResponseError(context)) => context.raw().status().is_success(),
            Err(_) => false,
        };
        if success_head {
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
        let (guard, framing) = ResponseGuard::streaming_success();
        let output = self
            .sdk
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .customize()
            .interceptor(guard)
            .send()
            .await
            .map_err(|e| self.classify(e))?;
        // The guard records the framing of every success it lets through, and the SDK
        // returns an output only for a success it has let through.
        let framing = framing
            .get()
            .cloned()
            .expect("the response guard records the framing of every GET success");
        let declared = match framing {
            Framing::Length(n) => Some(n),
            // Chunked framing marks its own end, so the body is accepted without a length.
            Framing::Chunked => None,
            // Without either a cut connection is indistinguishable from the end of the
            // object, so the body is refused rather than trusted.
            Framing::Undeclared => return Err(S3Error::Body(BodyError::LengthUndeclared)),
            Framing::Untrusted {
                transfer_encoding,
                content_length,
            } => {
                return Err(S3Error::Body(BodyError::Framing {
                    transfer_encoding,
                    content_length,
                }))
            }
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
            .customize()
            .interceptor(ResponseGuard::buffered())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| self.classify(e))
    }

    /// Map an SDK failure onto the one [`S3Error`] variant for where it happened.
    fn classify<E>(&self, err: SdkError<E, HttpResponse>) -> S3Error
    where
        E: std::error::Error + 'static,
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
                // A success status here is one the SDK could not read, a header that fails its
                // grammar for one: an error only in the SDK's account, which is kept whole.
                if raw.status().is_success() {
                    return S3Error::Unreadable {
                        status,
                        request_id,
                        detail,
                    };
                }
                // The body is read here, never through the SDK's error metadata: the SDK
                // makes up `NotFound` for a bodiless 404 and reads a cut-off or trailed
                // document as a clean one. The response guard has capped it at the budget.
                match raw.body().bytes().map(error_document) {
                    Some(Ok((code, message))) => S3Error::Service {
                        status,
                        code,
                        message,
                        request_id,
                    },
                    Some(Err(reading)) => S3Error::Unreadable {
                        status,
                        request_id,
                        detail: reading,
                    },
                    // The SDK reads every error body before it deserializes one
                    // (`aws-smithy-runtime` 1.16.0 `client/orchestrator.rs:526-536`), so this
                    // is not expected; reported as what it would be, a body nobody read.
                    None => S3Error::Unreadable {
                        status,
                        request_id,
                        detail,
                    },
                }
            }
            // Everything else is classified by whether a response arrived: a dispatch failure
            // has none; a response that was refused or could not be read (`ResponseError`:
            // the response guard's framing and budget refusals, a body that failed mid-read)
            // has one. `SdkError` is non-exhaustive, so a future variant lands here too.
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
/// that runtime down before handing back the outcome. The client's connector spawns each
/// connection's task onto the runtime it is called from (`tokio::spawn`, `s3/wire.rs`), so the
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
