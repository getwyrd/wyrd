//! What the client accepts of a response before the SDK reads it (invariant: a response that
//! is not a whole, well-formed, correctly framed S3 response is never reported as one).
//!
//! The SDK reads every body it does not stream whole before it deserializes anything: every
//! error response, and every PUT or DELETE response, success included
//! (`aws-smithy-runtime` 1.16.0 `client/orchestrator.rs:526-536`). Only a successful GET
//! streams (`aws-sdk-s3` 1.152.0 `operation/get_object.rs:338-341`). So a [`ResponseGuard`]
//! sits on every call, at the one hook that runs after the response head arrives and before
//! the SDK touches the body, and:
//!
//! * for a body the SDK will buffer, refuses framing that cannot mark the body's end and caps
//!   the body as it arrives: an error response's at [`BUFFERED_BODY_BUDGET`], so a body with
//!   no declared length is cut off at the budget rather than read whole, and a PUT's or a
//!   DELETE's success at nothing, since S3 sends neither with a body;
//! * for a GET success, which streams, records the framing for the caller to enforce piece by
//!   piece ([`super::ObjectBody`]), and leaves the body uncapped.
//!
//! The buffered body of an error response is then read by [`error_document`], strictly.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{ready, Context, Poll};

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextMut;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::SdkBody;
use bytes::Bytes;
use http_body::Frame;

use super::error::ErrorCode;

/// The most bytes of a response body the client buffers: 64 KiB.
///
/// It applies to every body the SDK reads whole (every error response, and every PUT or
/// DELETE response) and to no successful GET body, which streams. A PUT or DELETE success
/// has no body at all, so one that sends a byte is refused at that byte, well inside the
/// budget. S3 error documents are small: a few hundred bytes, except
/// `SignatureDoesNotMatch`, which echoes the canonical request twice (as text, and as hex
/// bytes at three characters a byte) and the string to sign. For this client's requests that
/// is at most about 20 KiB, even with a 1024-byte key whose every byte is percent-encoded,
/// so a conforming server never comes near the budget. A body of exactly the budget is read;
/// one byte more is reported as unreadable, and the client stops reading it there: what the
/// endpoint sends beyond the budget is never held.
pub const BUFFERED_BODY_BUDGET: u64 = 64 * 1024;

/// How a response marks where its body ends (RFC 9112 §6.3), as far as the client trusts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Framing {
    /// A `Content-Length` and no `Transfer-Encoding`: hyper reads exactly this many bytes,
    /// and a connection that closes short of them is a transport error.
    Length(u64),
    /// `Transfer-Encoding: chunked` and no `Content-Length`: the body ends at its terminal
    /// chunk, and hyper reports a connection that closes before it as a transport error.
    Chunked,
    /// Neither header: the response declares no length. Its body ends when the connection
    /// closes (or, for a `204`, there is none), so for a body that has one a cut connection
    /// cannot be told apart from the end.
    Undeclared,
    /// Framing no conforming server sends: a `Transfer-Encoding` together with a
    /// `Content-Length` (RFC 9112 §6.2 forbids sending both, and §6.3 says to treat it as an
    /// error), a `Transfer-Encoding` other than exactly `chunked`, a `Content-Length` that is
    /// not one decimal length, a `204` that frames a body (RFC 9112 §6.1, RFC 9110 §8.6), or
    /// a `Transfer-Encoding` on a `304` to anything but a GET (RFC 9112 §6.1). The header
    /// values as sent, joined when repeated.
    Untrusted {
        transfer_encoding: Option<String>,
        content_length: Option<String>,
    },
}

impl Framing {
    /// The framing of `response` to a request that was a GET or, with `get` false, a PUT or a
    /// DELETE, read from the raw header bytes: the SDK's UTF-8 view skips a value it cannot
    /// decode, and a skipped `Transfer-Encoding` would read as an absent one. Only what hyper
    /// itself reads as the same framing is trusted, so the body the SDK gets ends exactly
    /// where this says.
    pub(super) fn of(response: &HttpResponse, get: bool) -> Self {
        let headers = response.headers();
        let transfer_encoding: Vec<&[u8]> = headers.get_all_bytes("transfer-encoding").collect();
        let content_length: Vec<&[u8]> = headers.get_all_bytes("content-length").collect();
        let untrusted = || Self::Untrusted {
            transfer_encoding: joined(&transfer_encoding),
            content_length: joined(&content_length),
        };
        let framing = match (transfer_encoding.as_slice(), content_length.is_empty()) {
            ([], true) => Self::Undeclared,
            // One field line, the one token, case-insensitive, with only the optional
            // whitespace HTTP allows around a field value (SP and HTAB, RFC 9110 §5.6.3) and
            // no other: hyper reads the same line as chunked (`hyper` 1.10.1 `headers.rs:130-
            // 139`), and a value it cannot read as text (a UTF-8 non-breaking space, say) as
            // not chunked, which this refuses rather than trusts.
            ([coding], true) if ows_trimmed(coding).eq_ignore_ascii_case(b"chunked") => {
                Self::Chunked
            }
            ([], false) => decimal_length(&content_length).map_or_else(untrusted, Self::Length),
            _ => untrusted(),
        };
        // hyper gives a `204` and a `304` no body whatever their headers say (`hyper` 1.10.1
        // `proto/h1/role.rs:1268`); whatever bytes follow the head are caught as bytes after
        // the response (`super::wire`). The headers themselves are refused where no
        // conforming server sends them. A `204` carries neither (RFC 9112 §6.1, RFC 9110
        // §8.6), though `Content-Length: 0` agrees with the empty body and is let through. A
        // `304` may carry a `Content-Length` (RFC 9110 §8.6), and a `Transfer-Encoding` only
        // in answer to a GET (RFC 9112 §6.1).
        let refused = match response.status().as_u16() {
            204 => !matches!(framing, Self::Undeclared | Self::Length(0)),
            304 => !get && !transfer_encoding.is_empty(),
            _ => false,
        };
        if refused {
            return untrusted();
        }
        framing
    }
}

/// `value` without the optional whitespace around it: SP and HTAB only (RFC 9110 §5.6.3).
fn ows_trimmed(mut value: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = value {
        value = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = value {
        value = rest;
    }
    value
}

/// The `Content-Length` field lines: one or more comma-separated copies of one decimal length,
/// as hyper accepts (`hyper` 1.10.1 `headers.rs:45-70`). Digits only (no sign, no blank
/// element), and no value past `u64`.
fn decimal_length(values: &[&[u8]]) -> Option<u64> {
    let mut length = None;
    for element in values.iter().flat_map(|value| value.split(|&b| b == b',')) {
        let digits = ows_trimmed(element);
        if digits.is_empty() {
            return None;
        }
        let n = digits.iter().try_fold(0u64, |n, &b| {
            b.is_ascii_digit()
                .then(|| n.checked_mul(10)?.checked_add(u64::from(b - b'0')))
                .flatten()
        })?;
        if *length.get_or_insert(n) != n {
            return None;
        }
    }
    length
}

/// Header field lines as sent, joined with `", "`, or `None` when there are none.
fn joined(values: &[&[u8]]) -> Option<String> {
    (!values.is_empty()).then(|| {
        values
            .iter()
            .map(|value| String::from_utf8_lossy(value))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// Why a response was refused before the SDK read its body, or while it read it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Refusal {
    /// The response's framing cannot mark where its body ends.
    Framing {
        transfer_encoding: Option<String>,
        content_length: Option<String>,
    },
    /// A buffered body ran past the budget as it arrived; `received` counts the piece that
    /// crossed it.
    OverBudget { received: u64 },
    /// A PUT or DELETE success carried a body, which S3 never sends with one; `received`
    /// counts the first piece of it.
    BodyOnSuccess { received: u64 },
}

impl fmt::Display for Refusal {
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
            Self::OverBudget { received } => write!(
                f,
                "the response body ran past the client's {BUFFERED_BODY_BUDGET}-byte budget \
                 for a body it buffers ({received} bytes received when it stopped reading)"
            ),
            Self::BodyOnSuccess { received } => write!(
                f,
                "the success carries a body, which an S3 PUT or DELETE success never has \
                 ({received} bytes received when the client stopped reading)"
            ),
        }
    }
}

impl std::error::Error for Refusal {}

/// The per-call interceptor described in the module docs.
#[derive(Debug)]
pub(super) struct ResponseGuard {
    /// Where a streamed success's framing is recorded; `None` for a call whose every
    /// response body is buffered.
    streamed: Option<Arc<OnceLock<Framing>>>,
}

impl ResponseGuard {
    /// For a PUT or a DELETE: the SDK buffers every response body.
    pub(super) fn buffered() -> Self {
        Self { streamed: None }
    }

    /// For a GET: a success streams, and its framing is recorded in the returned slot.
    pub(super) fn streaming_success() -> (Self, Arc<OnceLock<Framing>>) {
        let slot = Arc::new(OnceLock::new());
        let guard = Self {
            streamed: Some(Arc::clone(&slot)),
        };
        (guard, slot)
    }
}

impl Intercept for ResponseGuard {
    fn name(&self) -> &'static str {
        "wyrd-validate response guard"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response_mut();
        let get = self.streamed.is_some();
        let success = response.status().is_success();
        let framing = Framing::of(response, get);
        // The SDK's own rule for which GET responses stream: a success status
        // (`aws-sdk-s3` 1.152.0 `operation/get_object.rs:338-341`).
        if let Some(slot) = self.streamed.as_ref().filter(|_| success) {
            // One response per call (retries are off), so the slot is empty here.
            let _ = slot.set(framing);
            return Ok(());
        }
        if let Framing::Untrusted {
            transfer_encoding,
            content_length,
        } = framing
        {
            return Err(Refusal::Framing {
                transfer_encoding,
                content_length,
            }
            .into());
        }
        // A declared length is not checked against the cap here: the count below refuses the
        // same body once more than the cap of it has arrived, declared or not. Every buffered
        // success is a PUT's or a DELETE's, and S3 sends neither a body, so its cap is
        // nothing at all.
        let cap = if success { Cap::NoBody } else { Cap::Budget };
        let body = response.take_body();
        *response.body_mut() = SdkBody::from_body_1_x(Budgeted {
            inner: body,
            cap,
            received: 0,
            exceeded: false,
        });
        Ok(())
    }
}

/// How much of a buffered body the client takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cap {
    /// An error response's body: up to [`BUFFERED_BODY_BUDGET`].
    Budget,
    /// A PUT or DELETE success: no byte.
    NoBody,
}

impl Cap {
    fn bytes(self) -> u64 {
        match self {
            Self::Budget => BUFFERED_BODY_BUDGET,
            Self::NoBody => 0,
        }
    }

    fn refusal(self, received: u64) -> Refusal {
        match self {
            Self::Budget => Refusal::OverBudget { received },
            Self::NoBody => Refusal::BodyOnSuccess { received },
        }
    }
}

/// A body the SDK buffers, failed as soon as more bytes of it than its [`Cap`] have arrived.
/// The piece that crosses the cap is never handed on, so the SDK holds at most
/// [`BUFFERED_BODY_BUDGET`]. The SDK drops the body at the first error (`aws-smithy-runtime`
/// 1.16.0 `client/orchestrator/http.rs:15-21`), and with it the connection (`super::wire`).
///
/// It keeps the default `is_end_stream` and `size_hint`, which promise nothing: the SDK's
/// collection reads neither, and an exact hint forwarded from a large declared length would
/// invite a consumer to reserve that much.
struct Budgeted {
    inner: SdkBody,
    cap: Cap,
    received: u64,
    exceeded: bool,
}

impl http_body::Body for Budgeted {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if this.exceeded {
            // Never read past the cap, however often the body is polled after it.
            return Poll::Ready(Some(Err(this.cap.refusal(this.received).into())));
        }
        let frame = ready!(Pin::new(&mut this.inner).poll_frame(cx));
        if let Some(data) = frame
            .as_ref()
            .and_then(|f| f.as_ref().ok())
            .and_then(Frame::data_ref)
        {
            this.received = this.received.saturating_add(data.len() as u64);
            if this.received > this.cap.bytes() {
                this.exceeded = true;
                return Poll::Ready(Some(Err(this.cap.refusal(this.received).into())));
            }
        }
        Poll::Ready(frame)
    }
}

/// The `<Code>` and `<Message>` of an error response's buffered body, or why it is not an
/// S3 error document.
///
/// An empty body is [`ErrorCode::NoBody`]. Otherwise the body must be one well-formed XML
/// document (UTF-8, every element closed, no DTD, elements nested at most
/// [`MAX_ERROR_DOCUMENT_DEPTH`] deep) whose root element is `<Error>`, with nothing but the
/// XML declaration, whitespace and comments before it and nothing but whitespace and
/// comments after it. `<Code>` and `<Message>` are the root's child elements of those names,
/// each at most once, each holding text only (character references and CDATA sections are
/// text; an element, a comment or a processing instruction is not); a document without a
/// `<Code>`, or whose `<Code>` holds only whitespace, is [`ErrorCode::MissingInXml`]. The
/// SDK's own reading is not used: it accepts a document that is cut off or followed by junk,
/// reads a field up to the first markup inside it, and makes up `NotFound` for a bodiless 404
/// (`aws-sdk-s3` 1.152.0 `protocol_serde.rs:25-30`).
pub(super) fn error_document(body: &[u8]) -> Result<(ErrorCode, Option<String>), String> {
    if body.is_empty() {
        return Ok((ErrorCode::NoBody, None));
    }
    let text = std::str::from_utf8(body)
        .map_err(|e| format!("the error response body is not UTF-8: {e}"))?;
    // Before the parser, which recurses once per level of nesting with no limit of its own
    // (`roxmltree` 0.21.1 `tokenizer.rs`, `parse_element` and `parse_content`): a body nested
    // a few thousand levels deep, well inside the budget, would overflow the stack, an abort
    // no caller can catch.
    if nests_deeper_than(body, MAX_ERROR_DOCUMENT_DEPTH) {
        return Err(format!(
            "the error response body nests elements more than {MAX_ERROR_DOCUMENT_DEPTH} \
             deep, and an S3 error document is two deep"
        ));
    }
    let document = roxmltree::Document::parse(text)
        .map_err(|e| format!("the error response body is not a well-formed XML document: {e}"))?;
    let root = document.root_element();
    if root.tag_name().name() != "Error" {
        return Err(format!(
            "the error response body is an XML document whose root is <{}>, not <Error>",
            root.tag_name().name()
        ));
    }
    // Outside the root the parser refuses text and elements and keeps no whitespace (the XML
    // declaration is not a node either), so what can sit there is comments and processing
    // instructions. A processing instruction is well-formed XML, but no part of an S3 error
    // document, before the root or after it.
    if document
        .root()
        .children()
        .any(|node| node != root && !node.is_comment())
    {
        return Err(
            "the <Error> document has something other than whitespace or comments around it"
                .to_string(),
        );
    }
    let mut code = None;
    let mut message = None;
    for child in root.children().filter(roxmltree::Node::is_element) {
        let name = child.tag_name().name();
        let slot = match name {
            "Code" => &mut code,
            "Message" => &mut message,
            _ => continue,
        };
        let text = child
            .children()
            .map(|node| {
                node.is_text()
                    .then(|| node.text())
                    .flatten()
                    .ok_or_else(|| {
                        format!("the <{name}> of the <Error> document holds markup, not only text")
                    })
            })
            .collect::<Result<String, String>>()?;
        if slot.replace(text).is_some() {
            return Err(format!("the <Error> document has more than one <{name}>"));
        }
    }
    // An empty `<Code>` names no code, any more than an absent one does. XML's whitespace
    // (`S`, XML 1.0 §2.3) is the only kind trimmed: a code is never read as anything else.
    let code = code
        .filter(|code| {
            !code
                .trim_matches(['\x20', '\x09', '\x0D', '\x0A'])
                .is_empty()
        })
        .map_or(ErrorCode::MissingInXml, ErrorCode::Code);
    Ok((code, message))
}

/// The deepest element nesting [`error_document`] parses, the root being level one. An S3
/// error document is two levels deep (`<Error>` and its fields); this leaves room for an
/// extension element nested inside a field, and keeps the parser's recursion to a few
/// kilobytes of stack per level.
const MAX_ERROR_DOCUMENT_DEPTH: usize = 16;

/// Whether `xml` has an element more than `limit` levels deep (the root is level one) before
/// anything the parser would stop at. Never less deep than the parser goes: comments, CDATA
/// sections and processing instructions are skipped to their first end, as the parser skips
/// them; inside a tag a quoted attribute value may hold `>` or `/>`, so quotes are honoured
/// there; every start tag is an element one level below the open ones, and opens that level
/// for what follows unless it ends with `/>`; every end tag closes one. Where the scan and the
/// parser could part ways (an unterminated construct, a `<!` that is neither a comment nor
/// CDATA, a tag that is not well formed) the document is malformed and the parser refuses it
/// there, so nothing past that point can deepen its recursion.
fn nests_deeper_than(xml: &[u8], limit: usize) -> bool {
    // The levels open here; never more than `limit`, since an element below them is refused.
    let mut open_levels = 0usize;
    let mut at = 0;
    while let Some(open) = xml[at..].iter().position(|&b| b == b'<') {
        let tag = &xml[at + open..];
        let len = if tag.starts_with(b"<!--") {
            end_of(tag, b"-->")
        } else if tag.starts_with(b"<![CDATA[") {
            end_of(tag, b"]]>")
        } else if tag.starts_with(b"<?") {
            end_of(tag, b"?>")
        } else if tag.starts_with(b"<!") {
            // A DTD (refused by the parser) or no XML at all.
            None
        } else if tag.starts_with(b"</") {
            open_levels = open_levels.saturating_sub(1);
            end_of(tag, b">")
        } else {
            // An element at level `open_levels + 1`.
            if open_levels >= limit {
                return true;
            }
            let end = start_tag_end(tag);
            if end.is_none_or(|end| tag[end - 2] != b'/') {
                open_levels += 1;
            }
            end
        };
        let Some(len) = len else {
            return false;
        };
        at += open + len;
    }
    false
}

/// The length of `xml` up to and including the first `end`, if there is one.
fn end_of(xml: &[u8], end: &[u8]) -> Option<usize> {
    xml.windows(end.len())
        .position(|window| window == end)
        .map(|at| at + end.len())
}

/// The length of the start tag `xml` opens with, up to and including its `>`: the first one
/// outside a quoted attribute value.
fn start_tag_end(xml: &[u8]) -> Option<usize> {
    let mut quote = None;
    for (at, &b) in xml.iter().enumerate().skip(1) {
        match (quote, b) {
            (None, b'"' | b'\'') => quote = Some(b),
            (Some(q), _) if b == q => quote = None,
            (None, b'>') => return Some(at + 1),
            _ => {}
        }
    }
    None
}
