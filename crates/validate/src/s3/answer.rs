//! Answers kept whole (#938): what the server sent in answer to one request, its head as it
//! arrived and its body as far as it could be read (invariant: what the client reports is what
//! the server sent; no answer is upgraded, none downgraded, nothing is filled in on the
//! server's behalf, and a failure after a 2xx head never erases the 2xx).
//!
//! The verbs that report an [`Answer`] (`super::verbs`) do not let the SDK read the response.
//! A [`Capture`] sits on the call, as the response guard does on the older verbs
//! (`super::response`), at two hooks the SDK runs after the head arrives and before it
//! touches the body:
//!
//! * `read_after_transmit`, which every interceptor runs in the poll that hands the response
//!   over, before any of them runs the hook that may change it (`aws-smithy-runtime` 1.16.0
//!   `client/orchestrator.rs:510-514`): the status, every header field line as sent, and the
//!   framing as [`framing_of_no_body`] reads it. Not later: the SDK's own interceptor for HEAD and
//!   GET object drops an `Expires` header it cannot parse and adds an `ExpiresString` of its
//!   own at the next hook (`aws-sdk-s3` 1.152.0 `s3_expires_interceptor.rs:30-56`);
//! * `modify_before_deserialization`: the body itself, taken away and replaced with an empty
//!   one for the SDK to deserialize. What the SDK makes of that empty body is never consulted.
//!
//! The SDK's own interceptors run that second hook first, and two of them wrap a GET's body
//! there. Neither changes a byte, but each can turn the end of a body into a failure of its
//! own. The checksum validator is turned off for the call (`super::verbs`). The `Content-Length`
//! check cannot be (`aws-smithy-runtime` 1.16.2 `client/http/body/content_length_enforcement.rs`:
//! its types are private): hyper finds every short body first, so only a `304` that declares a
//! length, which has no body at all, can reach it, and [`read_whole`] reads that refusal as the
//! empty body it is.
//!
//! The client then reads the body itself ([`read_whole`]), under [`BUFFERED_BODY_BUDGET`] and
//! the body-idle and body deadlines, so a body that stalls, is cut, overruns its framing or
//! runs past the budget is a [`BodyFailure`] beside the head it came after, never a timeout or
//! a transport error in its place. A 2xx GET body is not read here: it streams, as
//! [`super::S3Client::get_object`]'s does ([`Fetched`]).
//!
//! A call whose response the SDK never saw (no connection, a deadline before the head, a
//! request the SDK would not build) has no head to keep, and is the [`S3Error`] it always was,
//! with one sharpening: a third hook, `read_before_transmit`, the last the SDK runs before it
//! calls the connector (`aws-smithy-runtime` 1.16.0 `client/orchestrator.rs:468-500`), records
//! that it got that far, and whether the request is a HEAD (the only hook of the three that sees
//! the request). A call that failed with no response and never got that far sent
//! nothing, and is [`S3Error::RequestNotBuilt`] rather than the [`S3Error::NoResponse`] the
//! SDK's dispatch-failure report reads as (`super::verbs`). The converse does not hold: a
//! call that got that far may still have sent nothing, for the connector can fail before it
//! writes a byte (a connection that cannot be opened, an `https` endpoint the plain-HTTP
//! connector does not speak), and that call stays [`S3Error::NoResponse`], as for the older
//! verbs.

use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::interceptors::{
    BeforeDeserializationInterceptorContextMut, BeforeDeserializationInterceptorContextRef,
    BeforeTransmitInterceptorContextRef,
};
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::SdkBody;
use bytes::Bytes;
use http_body::Body as _;
use tokio::time::Instant;

use super::body::{error_chain, source_of, ObjectBody};
use super::error::{BodyError, ErrorCode, Phase, S3Error};
use super::response::{error_document, Framing, BUFFERED_BODY_BUDGET};
use super::wire::Overrun;
use super::REQUEST_ID_HEADER;

/// A response head as the server sent it: the status and every header field line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    /// The HTTP status.
    pub status: u16,
    /// Every header field line, in the order hyper keeps them (grouped by name, each name's
    /// lines in the order sent): the name lowercased, the value's bytes as sent.
    fields: Vec<(String, Vec<u8>)>,
}

impl Head {
    /// The head of `response`, read from the raw header bytes: the SDK's UTF-8 view skips a
    /// value it cannot decode.
    fn of(response: &HttpResponse) -> Self {
        Self {
            status: response.status().as_u16(),
            fields: response
                .headers()
                .iter_bytes()
                .map(|(name, value)| (name.to_owned(), value.to_vec()))
                .collect(),
        }
    }

    /// Whether the status is a 2xx.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The value of header `name` (any case) as text, when the head carries exactly one field
    /// line of that name and its value is UTF-8. `None` when it carries none, more than one,
    /// or one that is not text: a repeated header is never read as its first line.
    /// [`Head::header_lines`] gives every line as sent.
    pub fn header(&self, name: &str) -> Option<&str> {
        let mut lines = self
            .fields
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice());
        let only = lines.next()?;
        if lines.next().is_some() {
            return None;
        }
        std::str::from_utf8(only).ok()
    }

    /// Every field line of header `name` (any case), its value as sent, in order.
    pub fn header_lines<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.fields
            .iter()
            .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice())
    }

    /// Every header field line: its name, lowercased, and its value as sent.
    pub fn fields(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }

    /// The `x-amz-request-id` header, read as [`Head::header`] reads one.
    pub fn request_id(&self) -> Option<&str> {
        self.header(REQUEST_ID_HEADER)
    }
}

/// One answer, whole: its head as sent, and what its body turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub head: Head,
    pub outcome: Outcome,
}

/// What an answer's body turned out to be, read against its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A 2xx read whole. The body as sent: a HEAD's is empty, since the connector reports any
    /// byte after a HEAD's head as [`BodyFailure::Overrun`]. A listing page's 2xx is read into
    /// its [`super::Listing`] and a GET's streams ([`Fetched`]), so neither is one of these.
    Success { body: Bytes },
    /// An S3 error, read by the rules every error response is: its `<Code>` and `<Message>`.
    /// Either a status other than 2xx whose body is empty ([`ErrorCode::NoBody`]) or one whole
    /// `<Error>` document (a `304` to a conditional GET is one, with no body), or a 2xx whose
    /// body is one whole `<Error>` document in place of the operation's success body: S3 sends
    /// some failures that way, after a `200` head (CopyObject, UploadPartCopy and
    /// CompleteMultipartUpload document it). The status stays in the head as sent, so a 2xx
    /// here is still a 2xx; never one whose body is empty, which names no error.
    Error {
        code: ErrorCode,
        message: Option<String>,
    },
    /// The body was read whole and is not what its status calls for: for a 2xx, neither the
    /// document the operation's success carries (a document cut short, followed by junk, or
    /// under another root; no body at all) nor an `<Error>` document; for any other status, not
    /// an S3 error document. The body as sent, and why.
    Unreadable { body: Bytes, detail: String },
    /// The body could not be read whole.
    BodyFailed(BodyFailure),
}

/// Why the body of an [`Answer`] could not be read whole. The head it came after stays in the
/// answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyFailure {
    /// The head's framing cannot mark where the body ends (as [`super::S3Client::get_object`]
    /// refuses it): a `Transfer-Encoding` together with a `Content-Length`, a coding other than
    /// exactly `chunked`, a `Content-Length` that is not one decimal length, a `204` that frames
    /// a body, a `Transfer-Encoding` on a `304` to anything but a GET or a HEAD. Not a
    /// `Transfer-Encoding` alone, of any coding, on an answer with no body for it to frame: a
    /// `304` to a GET, or a response to a HEAD (RFC 9112 §6.1). The header values as sent, joined
    /// when repeated. The body is not read at all.
    Framing {
        transfer_encoding: Option<String>,
        content_length: Option<String>,
    },
    /// A deadline expired after `received` bytes of the body: [`Phase::BodyIdle`] or
    /// [`Phase::Body`].
    Timeout {
        phase: Phase,
        limit: Duration,
        received: u64,
    },
    /// Reading failed after `received` bytes: a connection cut mid-body, a body short of its
    /// `Content-Length`, chunked framing that ends before its terminal chunk.
    Transport { received: u64, detail: String },
    /// The body ended where its framing says, after `received` bytes, and the endpoint had sent
    /// at least `after` more bytes past that end.
    Overrun { received: u64, after: u64 },
    /// The body ran past [`BUFFERED_BODY_BUDGET`]: `received` counts the piece that crossed it,
    /// and the client read nothing after that piece.
    OverBudget { received: u64 },
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success { body } => write!(f, "a success, with a {}-byte body", body.len()),
            Self::Error { code, message } => {
                write!(f, "an S3 error, {code}")?;
                match message {
                    Some(message) => write!(f, ", message {message:?}"),
                    None => Ok(()),
                }
            }
            Self::Unreadable { body, detail } => {
                write!(f, "an unreadable {}-byte body: {detail}", body.len())
            }
            Self::BodyFailed(failure) => write!(f, "a body that failed: {failure}"),
        }
    }
}

impl fmt::Display for BodyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Framing {
                transfer_encoding,
                content_length,
            } => write!(
                f,
                "the response's framing cannot mark where its body ends: Transfer-Encoding \
                 {transfer_encoding:?}, Content-Length {content_length:?}"
            ),
            Self::Timeout {
                phase,
                limit,
                received,
            } => write!(
                f,
                "the {phase} deadline of {limit:?} expired after {received} bytes of the body"
            ),
            Self::Transport { received, detail } => {
                write!(
                    f,
                    "reading the body failed after {received} bytes: {detail}"
                )
            }
            Self::Overrun { received, after } => write!(
                f,
                "the body ended where its framing says, after {received} bytes, but the \
                 endpoint sent at least {after} more bytes after that end"
            ),
            Self::OverBudget { received } => write!(
                f,
                "the body ran past the client's {BUFFERED_BODY_BUDGET}-byte budget for a body it \
                 buffers ({received} bytes received when it stopped reading)"
            ),
        }
    }
}

impl fmt::Display for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP {}", self.head.status)?;
        match self.head.request_id() {
            Some(id) => write!(f, ", x-amz-request-id {id}")?,
            None => write!(f, ", no single x-amz-request-id")?,
        }
        write!(f, ": {}", self.outcome)
    }
}

/// What a ranged or conditional GET returned.
pub enum Fetched {
    /// A 2xx: its head, and its body to read piece by piece with [`ObjectBody::next_piece`]
    /// under the rules [`super::S3Client::get_object`]'s body is read by, or why that body
    /// cannot be read (framing that cannot mark the object's end). Boxed, as the body is
    /// several times the size of an answer.
    Object {
        head: Head,
        body: Result<Box<ObjectBody>, BodyError>,
    },
    /// Any other status, a `304` included: the whole answer, its body read under the budget.
    Other(Answer),
}

impl fmt::Debug for Fetched {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object { head, body } => f
                .debug_struct("Object")
                .field("head", head)
                .field(
                    "body",
                    &body
                        .as_ref()
                        .map(|body| ("streaming", body.content_length())),
                )
                .finish(),
            Self::Other(answer) => f.debug_tuple("Other").field(answer).finish(),
        }
    }
}

/// Why a verb that reads a document from its answer (a listing page) has no result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoResult {
    /// No answer arrived: the request was not built, no response head arrived, or a deadline
    /// expired before one did.
    NoAnswer(S3Error),
    /// An answer arrived and holds no result: any status other than 2xx, or a 2xx whose body
    /// failed or is not one whole document of the kind the verb reads. Kept whole; its
    /// [`Outcome`] is never [`Outcome::Success`].
    Answered(Answer),
}

impl fmt::Display for NoResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAnswer(e) => write!(f, "no answer: {e}"),
            Self::Answered(answer) => write!(f, "an answer with no result: {answer}"),
        }
    }
}

impl std::error::Error for NoResult {}

/// `body`, read against `head`'s status: for a 2xx, what `success` reads from it; otherwise,
/// and when `success` refuses it or the body failed, the [`Outcome`] that stands in its place.
/// A 2xx body `success` refuses is [`Outcome::Error`] when it is one whole `<Error>` document,
/// read as an error response's is, and [`Outcome::Unreadable`] otherwise.
pub(super) fn settle<T>(
    head: &Head,
    body: Result<Bytes, BodyFailure>,
    success: impl FnOnce(&Bytes) -> Result<T, String>,
) -> Result<T, Outcome> {
    let body = body.map_err(Outcome::BodyFailed)?;
    if head.is_success() {
        return success(&body).map_err(|detail| match error_document(&body) {
            // An empty body is no `<Error>` document: it names no error.
            Ok((code, message)) if !body.is_empty() => Outcome::Error { code, message },
            _ => Outcome::Unreadable { body, detail },
        });
    }
    Err(match error_document(&body) {
        Ok((code, message)) => Outcome::Error { code, message },
        Err(detail) => Outcome::Unreadable { body, detail },
    })
}

/// The answer of `head` and `body`, whose 2xx is a success with the body as sent.
pub(super) fn answer(head: Head, body: Result<Bytes, BodyFailure>) -> Answer {
    let outcome = match settle(&head, body, |body| Ok::<_, String>(body.clone())) {
        Ok(body) => Outcome::Success { body },
        Err(outcome) => outcome,
    };
    Answer { head, outcome }
}

/// What a [`Capture`] took of one response.
#[derive(Debug)]
pub(super) struct Captured {
    pub(super) head: Head,
    pub(super) framing: Framing,
    /// The body, or, when the SDK stopped between the two hooks and the body never reached
    /// the client, what the SDK reported.
    pub(super) body: Result<SdkBody, String>,
}

/// What a [`Capture`] has recorded so far.
#[derive(Debug, Default)]
struct Record {
    /// Whether the SDK reached the last hook before it calls the connector.
    transmitted: bool,
    /// Whether the request it was about to send there is a HEAD.
    head_request: bool,
    head: Option<(Head, Framing)>,
    body: Option<SdkBody>,
}

/// The per-call interceptor described in the module docs. One response per call (retries are
/// off), so each hook runs at most once.
#[derive(Debug)]
pub(super) struct Capture {
    record: Arc<Mutex<Record>>,
    /// Whether the request is a GET or a HEAD, for [`Framing::of`].
    get: bool,
}

/// A verb's handle on what its [`Capture`] recorded.
#[derive(Debug)]
pub(super) struct Taken(Arc<Mutex<Record>>);

impl Capture {
    /// A capture for a GET or HEAD request (`get`), or for any other, and the handle to take
    /// what it records.
    pub(super) fn new(get: bool) -> (Self, Taken) {
        let record = Arc::new(Mutex::new(Record::default()));
        (
            Self {
                record: Arc::clone(&record),
                get,
            },
            Taken(record),
        )
    }

    fn record(&self) -> std::sync::MutexGuard<'_, Record> {
        // Nothing panics while holding the lock, so a poisoned one still holds a whole record.
        self.record.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Taken {
    /// Whether the SDK got as far as calling the connector. `false` means nothing was sent;
    /// `true` does not mean anything was, since the connector can fail before it writes a
    /// byte (module docs).
    pub(super) fn transmitted(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .transmitted
    }

    /// The response's head, framing and body, or `None` if no response reached the hooks.
    /// `sdk_detail` is what the SDK reported, kept for a body that never reached the client.
    pub(super) fn take(&self, sdk_detail: impl FnOnce() -> String) -> Option<Captured> {
        let mut record = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (head, framing) = record.head.take()?;
        let body = record.body.take().ok_or_else(sdk_detail);
        Some(Captured {
            head,
            framing,
            body,
        })
    }
}

impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "wyrd-validate answer capture"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut record = self.record();
        record.transmitted = true;
        record.head_request = context.request().method() == "HEAD";
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let mut record = self.record();
        let framing = framing_of_no_body(
            Framing::of(response, self.get),
            response.status().as_u16(),
            self.get,
            record.head_request,
        );
        record.head = Some((Head::of(response), framing));
        Ok(())
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let body = std::mem::replace(context.response_mut().body_mut(), SdkBody::empty());
        self.record().body = Some(body);
        Ok(())
    }
}

/// `framing` as read for an answer the client reads itself, where an answer with no body has
/// nothing for a `Transfer-Encoding` to frame.
///
/// RFC 9112 §6.1 lets a server send that header on a response to a HEAD and on a `304` to a GET,
/// "to indicate that the origin server would have applied a transfer coding": any coding, so
/// `gzip, chunked` too, which [`Framing::of`] does not trust because for the older verbs it
/// would have to mark a body's end. hyper gives both of these answers no body whatever their
/// headers say (`hyper` 1.12.0 `proto/h1/role.rs:1301-1305`), so the header is reported in the head
/// and no refusal stands beside it; the connector still reports any byte after the head
/// (`super::wire`). Only that header alone: a `Content-Length` that is not one decimal length, or
/// one beside a `Transfer-Encoding` (RFC 9112 §6.2 forbids both in any message), stays refused,
/// and so does a `204` that frames a body, which §6.1 forbids however it was asked for.
fn framing_of_no_body(framing: Framing, status: u16, get: bool, head_request: bool) -> Framing {
    let no_body = match status {
        204 => false,
        304 => get,
        _ => head_request,
    };
    match framing {
        Framing::Untrusted {
            transfer_encoding: Some(_),
            content_length: None,
        } if no_body => Framing::Undeclared,
        framing => framing,
    }
}

/// Read `body` whole, as the head's `framing` marks it, under [`BUFFERED_BODY_BUDGET`]: each
/// wait for the next piece bounded by `idle`, the whole body by `limit` from now, and reported
/// as whichever deadline comes first (on a tie, the whole-body one: waiting longer would not
/// have helped). A body past the budget is not read further; dropping it closes the
/// connection (`super::wire`).
///
/// The end is hyper's to find, as for a GET body (`super::body`): a `Content-Length` body is
/// read to exactly that length, a chunked one to its terminal chunk, and a connection that
/// closes short of either is a transport error; at that end the connector checks that nothing
/// followed it. A body that declares neither ends where the connection does, so a cut cannot
/// be told from its end, as for every error body the SDK buffers: a cut document is still
/// refused by its reading. A `304` has no body, whatever its `Content-Length` says
/// ([`refused_by_the_sdks_length_check`]).
pub(super) async fn read_whole(
    mut body: SdkBody,
    status: u16,
    framing: Framing,
    idle: Duration,
    limit: Duration,
) -> Result<Bytes, BodyFailure> {
    let declared = match framing {
        Framing::Untrusted {
            transfer_encoding,
            content_length,
        } => {
            return Err(BodyFailure::Framing {
                transfer_encoding,
                content_length,
            })
        }
        Framing::Length(declared) => Some(declared),
        _ => None,
    };
    // Clock: tokio's runtime clock, the one the SDK's connect and operation deadlines and a GET
    // body's own deadlines (`super::body`) sleep on, so every deadline of one request reads the
    // same source.
    let deadline = Instant::now().checked_add(limit);
    let mut held = Vec::new();
    loop {
        let received = held.len() as u64;
        let left = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let (wait, phase, phase_limit) = match left {
            Some(left) if left <= idle => (left, Phase::Body, limit),
            _ => (idle, Phase::BodyIdle, idle),
        };
        let frame = tokio::time::timeout(wait, poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)))
            .await
            .map_err(|_| BodyFailure::Timeout {
                phase,
                limit: phase_limit,
                received,
            })?;
        match frame {
            None => return Ok(Bytes::from(held)),
            Some(Err(e)) => {
                let detail = match source_of::<Overrun>(e.as_ref()) {
                    Some(overrun) => {
                        return Err(BodyFailure::Overrun {
                            received,
                            after: overrun.after,
                        })
                    }
                    None => error_chain(e.as_ref()),
                };
                if refused_by_the_sdks_length_check(status, declared, received, &detail) {
                    return Ok(Bytes::new());
                }
                return Err(BodyFailure::Transport { received, detail });
            }
            // A trailer section never gets here: the connector drops it (`super::wire`).
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    let total = received.saturating_add(data.len() as u64);
                    if total > BUFFERED_BODY_BUDGET {
                        return Err(BodyFailure::OverBudget { received: total });
                    }
                    held.extend_from_slice(&data);
                }
            }
        }
    }
}

/// Whether a read that failed with `detail` after `received` bytes is the SDK's `Content-Length`
/// check refusing a `304`, which may declare a length and has no body (RFC 9110 §8.6;
/// [`Framing::of`] lets it through). The check runs only after the body beneath it has ended
/// cleanly, the connection's say included, so nothing is lost by reading it as that end.
///
/// The check's error type is not reachable from here (`aws-smithy-runtime` is not a dependency
/// of this crate), so its words are all there is to know it by, matched whole against the length
/// the head declared. If an SDK update changes them, this stops matching: the `304` reads as the
/// failure it was reported as, never as more than it is, and the test of this case goes red.
fn refused_by_the_sdks_length_check(
    status: u16,
    declared: Option<u64>,
    received: u64,
    detail: &str,
) -> bool {
    let (304, Some(declared), 0) = (status, declared, received) else {
        return false;
    };
    detail == format!("Invalid Content-Length: Expected {declared} bytes but 0 bytes were received")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name is the only thing of this interceptor's a reader sees of it: the SDK prints it
    /// when an interceptor fails, and no test can make this one fail, since every hook returns
    /// `Ok`.
    #[test]
    fn the_capture_names_itself_as_this_clients() {
        let (capture, _taken) = Capture::new(true);
        assert_eq!(capture.name(), "wyrd-validate answer capture");
    }

    /// `crates/validate/tests/s3_client_smoke_verbs.rs` drives every case an endpoint can make
    /// of these verbs, all of which are GETs or HEADs. This is the one it cannot: a `304` to any
    /// other request, whose `Transfer-Encoding` RFC 9112 §6.1 does not allow.
    #[test]
    fn a_304_to_anything_but_a_get_or_a_head_keeps_its_refusal() {
        let untrusted = || Framing::Untrusted {
            transfer_encoding: Some("gzip, chunked".to_string()),
            content_length: None,
        };
        assert_eq!(
            framing_of_no_body(untrusted(), 304, false, false),
            untrusted()
        );
        assert_eq!(
            framing_of_no_body(untrusted(), 304, true, false),
            Framing::Undeclared
        );
    }

    /// `crates/validate/tests/s3_client_smoke_verbs.rs` drives the case that matches through the
    /// SDK. These are the ones it cannot make: no endpoint can make the connector fail a `304`
    /// in other words.
    #[test]
    fn only_the_sdks_own_refusal_of_a_304_reads_as_no_body() {
        let refusal = "Invalid Content-Length: Expected 5 bytes but 0 bytes were received";
        let read = refused_by_the_sdks_length_check;
        assert!(read(304, Some(5), 0, refusal));
        assert!(!read(404, Some(5), 0, refusal), "not a 304");
        assert!(!read(304, None, 0, refusal), "declares no length");
        assert!(!read(304, Some(6), 0, refusal), "not the length declared");
        assert!(!read(304, Some(5), 1, refusal), "a byte arrived");
        assert!(!read(304, Some(5), 0, "connection lost"), "other words");
    }
}
