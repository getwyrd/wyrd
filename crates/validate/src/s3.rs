//! The S3 client every validator scenario calls through (proposal 0017 §6-7).
//!
//! It is `aws-sdk-s3` pointed at an arbitrary `--endpoint`: path-style addressing, static
//! SigV4 credentials, a plain-HTTP connector (TLS is terminated in front of the gateway,
//! proposal 0017 §10), and retries and stalled-stream protection **off**, so every request
//! the validator reports on is exactly one request on the wire. On top of the SDK it adds
//! three things:
//!
//! * a typed error, [`S3Error`], whose every fact is the server's or the client's own
//!   measurement — in particular the request id is read from the `x-amz-request-id` header
//!   itself, because the SDK's generic accessor prefers `x-amzn-requestid` when both are
//!   present;
//! * bounded waits ([`Deadlines`]): connect, operation, and body-idle, each expiring as a
//!   typed timeout naming its phase;
//! * bodies that stream both ways ([`PutSource`], [`ObjectBody`]), so what the client holds
//!   of an object is bounded independently of the object's size.

mod body;
mod error;

use std::time::Duration;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Credentials as SdkCredentials, Region, StalledStreamProtectionConfig};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;

pub use body::{ObjectBody, PutSource};
pub use error::{BodyError, ErrorCode, Phase, S3Error};

use crate::ResolvedConfig;

/// The response header the request id is read from. The name is AWS's; the Wyrd gateway
/// stamps it on every response.
const REQUEST_ID_HEADER: &str = "x-amz-request-id";

/// The bound on every wait for the endpoint. All three run on tokio's runtime clock: the
/// SDK's connect and operation timeouts sleep on its default tokio sleep, and the body-idle
/// deadline is a `tokio::time::timeout`. The SDK also reads the wall clock, to date SigV4
/// signatures; that stamp belongs to the server's freshness check, not to any deadline here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    /// Establishing a TCP connection.
    pub connect: Duration,
    /// One whole request until its response head is read and deserialized. For a PUT that
    /// covers the upload, so the default is sized for a large object rather than a small
    /// one.
    pub operation: Duration,
    /// The wait for each next piece of a GET body. The body as a whole is bounded by its
    /// declared length times this.
    pub body_idle: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            operation: Duration::from_secs(15 * 60),
            body_idle: Duration::from_secs(60),
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
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .timeout_config(timeouts)
            .build();
        Self {
            sdk: aws_sdk_s3::Client::from_conf(sdk_config),
            bucket: config.args.bucket.clone(),
            deadlines,
        }
    }

    /// PUT `key` from `source`, streaming it: the source is pulled only as fast as the
    /// connection drains.
    pub async fn put_object(&self, key: &str, source: PutSource) -> Result<PutOutcome, S3Error> {
        let length = i64::try_from(source.length()).map_err(|_| S3Error::RequestNotBuilt {
            detail: format!(
                "declared length {} does not fit a Content-Length",
                source.length()
            ),
        })?;
        let (body, source_failure) = source.into_body();
        let sent = self
            .sdk
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(length)
            .body(ByteStream::from_body_1_x(body))
            .send()
            .await;
        // A failure of the caller's source outranks however the SDK and hyper reported the
        // aborted request: it is the cause, and the server never saw a complete body.
        if let Some(failure) = source_failure.get() {
            return Err(S3Error::Body(failure.clone()));
        }
        // deferred: #854 — a peer that answers before it has read the whole body (or stops
        // reading) is not detected here; an early success is reported as success.
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
