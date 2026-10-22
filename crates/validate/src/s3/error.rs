//! The typed error every S3 call returns.
//!
//! One variant per place a call can fail, so the failure taxonomy (proposal 0017 §7) can
//! classify a failure by matching on it, never by reading a message. Every fact a variant
//! carries is either one the server sent (status, `<Code>`, `<Message>`, the
//! `x-amz-request-id` header) or one the client measured itself (byte counts, the deadline
//! that expired). Nothing is filled in on the server's behalf.

use std::fmt;
use std::time::Duration;

/// Why an S3 call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Error {
    /// The endpoint answered with an S3 error response.
    Service {
        /// The response's HTTP status.
        status: u16,
        /// The `<Code>` the response carried, or why it carried none.
        code: ErrorCode,
        /// The `<Message>` element, when the document had one.
        message: Option<String>,
        /// The `x-amz-request-id` response header, read from that header itself.
        request_id: Option<String>,
    },
    /// A response arrived but cannot be read as S3: a success the SDK could not deserialize,
    /// an error response whose body is not one whole S3 `<Error>` document, a response whose
    /// framing cannot mark where its body ends, bytes after the end its framing marks, a PUT
    /// or DELETE success that carries a body, or a body the client buffers that runs past
    /// [`super::BUFFERED_BODY_BUDGET`].
    Unreadable {
        /// The response's HTTP status.
        status: u16,
        /// The `x-amz-request-id` response header, when present.
        request_id: Option<String>,
        /// What went wrong: the SDK's report with its source chain, or the client's own
        /// reading of the response.
        detail: String,
    },
    /// No response: the connection could not be made, or was lost before a response head
    /// arrived.
    NoResponse {
        /// What the SDK reported, with its source chain.
        detail: String,
    },
    /// A deadline expired.
    Timeout {
        /// Which deadline.
        phase: Phase,
        /// The limit that was exceeded.
        limit: Duration,
    },
    /// The request was never started: the SDK refused to build it (an empty key, for one),
    /// or the client could not start the thread or the runtime a PUT runs on. Nothing was
    /// sent.
    RequestNotBuilt {
        /// What the SDK reported, with its source chain.
        detail: String,
    },
    /// An object body failed or cannot be trusted.
    Body(BodyError),
}

/// The `<Code>` of an S3 error response. The three cases stay apart: a code the server
/// named, an `<Error>` document that names none, and a response with no body at all (a
/// `HEAD` error has none, and the SDK fills in `NotFound` for a bodiless 404; that code is
/// the SDK's, not the server's, so it is never reported here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// The `<Code>` element's text.
    Code(String),
    /// A well-formed `<Error>` document without a `<Code>` element.
    MissingInXml,
    /// The error response had an empty body.
    NoBody,
}

/// The deadline that expired. See [`super::Deadlines`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Establishing the TCP connection.
    Connect,
    /// The whole request up to a deserialized response head: for a PUT, that includes the
    /// upload.
    Operation,
    /// Waiting for the next piece of a GET body.
    BodyIdle,
    /// Reading a whole GET body, from its response head to its end.
    Body,
}

/// Why an object body failed or cannot be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    /// PUT: the caller's source yielded `produced` bytes against its declared length. Short
    /// is reported when the source ends; long at the first piece that crosses the declared
    /// length, before any of that piece is sent.
    SourceLength { declared: u64, produced: u64 },
    /// PUT: the caller's source failed after yielding `produced` bytes.
    SourceFailed { produced: u64, detail: String },
    /// PUT: the endpoint answered with success before the caller's source had ended, that is,
    /// before it had yielded its whole declared length and then reported its end. It had
    /// yielded `produced` of its `declared` bytes by then. Never a receipt, even when
    /// `produced` equals `declared`: until the source reports its end the body's final chunk
    /// has not been written, so the server cannot hold the object the caller sent. The source
    /// is not polled again once the client has the answer, so a source that would have
    /// reported its end, given its last piece, failed, or run past its declared length after
    /// that is reported here too.
    AcknowledgedEarly {
        declared: u64,
        produced: u64,
        /// The answer's `x-amz-request-id` header.
        request_id: Option<String>,
    },
    /// GET: reading the body failed after `received` bytes (a cut connection, a framing
    /// error such as chunked framing that ends before its terminal chunk). `declared` is the
    /// `Content-Length`, or `None` for a chunked body, which declares none.
    Transport {
        declared: Option<u64>,
        received: u64,
        detail: String,
    },
    /// GET: the body ended short of, or ran past, its declared `Content-Length`.
    Length { declared: u64, received: u64 },
    /// GET: the body reached the end its framing marks after `received` bytes, but the
    /// endpoint had sent at least `after` more bytes past that end: a body longer than its
    /// `Content-Length`, or bytes after a chunked body's terminal chunk and trailer section.
    /// `declared` is the `Content-Length`, or `None` for a chunked body.
    Overrun {
        declared: Option<u64>,
        received: u64,
        after: u64,
    },
    /// GET: the response declared neither a `Content-Length` nor chunked framing, so a cut
    /// connection could not be told apart from the end of the object.
    LengthUndeclared,
    /// GET: the response's framing cannot mark where the object ends, so its body is not read:
    /// a `Transfer-Encoding` together with a `Content-Length` (RFC 9112 §6.2 forbids sending
    /// both), a `Transfer-Encoding` other than exactly `chunked`, a `Content-Length` that is
    /// not one decimal length, or a `204` that frames a body. The two header values as sent,
    /// joined when repeated.
    Framing {
        transfer_encoding: Option<String>,
        content_length: Option<String>,
    },
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Service {
                status,
                code,
                message,
                request_id,
            } => {
                write!(f, "S3 error response: HTTP {status}, {code}")?;
                if let Some(message) = message {
                    write!(f, ", message {message:?}")?;
                }
                write_request_id(f, request_id.as_deref())
            }
            Self::Unreadable {
                status,
                request_id,
                detail,
            } => {
                write!(f, "unreadable S3 response: HTTP {status}")?;
                write_request_id(f, request_id.as_deref())?;
                write!(f, ": {detail}")
            }
            Self::NoResponse { detail } => write!(f, "no response from the endpoint: {detail}"),
            Self::Timeout { phase, limit } => {
                write!(f, "the {phase} deadline of {limit:?} expired")
            }
            Self::RequestNotBuilt { detail } => {
                write!(f, "the request was not built, nothing was sent: {detail}")
            }
            Self::Body(e) => write!(f, "object body: {e}"),
        }
    }
}

fn write_request_id(f: &mut fmt::Formatter<'_>, request_id: Option<&str>) -> fmt::Result {
    match request_id {
        Some(id) => write!(f, ", x-amz-request-id {id}"),
        None => write!(f, ", no x-amz-request-id"),
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Code(code) => write!(f, "code {code}"),
            Self::MissingInXml => write!(f, "an <Error> document without a <Code>"),
            Self::NoBody => write!(f, "no body"),
        }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connect => "connect",
            Self::Operation => "operation",
            Self::BodyIdle => "body-idle",
            Self::Body => "body",
        })
    }
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceLength { declared, produced } => write!(
                f,
                "the PUT source yielded {produced} bytes against a declared length of {declared}"
            ),
            Self::SourceFailed { produced, detail } => write!(
                f,
                "the PUT source failed after yielding {produced} bytes: {detail}"
            ),
            Self::AcknowledgedEarly {
                declared,
                produced,
                request_id,
            } => {
                write!(
                    f,
                    "the endpoint acknowledged the PUT before its source had ended, when it had \
                     yielded {produced} of {declared} bytes"
                )?;
                write_request_id(f, request_id.as_deref())
            }
            Self::Transport {
                declared: Some(declared),
                received,
                detail,
            } => write!(
                f,
                "reading the body failed after {received} of {declared} bytes: {detail}"
            ),
            Self::Transport {
                declared: None,
                received,
                detail,
            } => write!(
                f,
                "reading the chunked body failed after {received} bytes: {detail}"
            ),
            Self::Length { declared, received } => write!(
                f,
                "the body carried {received} bytes against a declared Content-Length of \
                 {declared}"
            ),
            Self::Overrun {
                declared,
                received,
                after,
            } => {
                write!(
                    f,
                    "the body ended where its framing says, after {received} bytes"
                )?;
                if let Some(declared) = declared {
                    write!(f, " of a declared Content-Length of {declared}")?;
                }
                write!(
                    f,
                    ", but the endpoint sent at least {after} more bytes after that end"
                )
            }
            Self::LengthUndeclared => write!(
                f,
                "the response declared neither a Content-Length nor chunked framing, so its end \
                 cannot be told from a cut"
            ),
            Self::Framing {
                transfer_encoding,
                content_length,
            } => write!(
                f,
                "the response's framing cannot mark where the object ends: Transfer-Encoding \
                 {transfer_encoding:?}, Content-Length {content_length:?}"
            ),
        }
    }
}

impl std::error::Error for S3Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(e) => Some(e),
            _ => None,
        }
    }
}

impl std::error::Error for BodyError {}
