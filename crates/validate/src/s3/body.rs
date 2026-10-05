//! Object bodies, streamed both ways (invariant "stream, don't buffer", the one the gateway
//! itself holds at `crates/gateway-s3/src/lib.rs:12-17`).
//!
//! Neither direction keeps a copy of bytes it has passed on. A PUT piece goes from the
//! caller's source to the SDK by value and nothing here retains it; a GET piece goes from the
//! SDK's body stream to the caller by value, and only its length is counted. There is no
//! `collect`, no aggregation, and no growing buffer in either path.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{ready, Context, Poll, Wake, Waker};
use std::time::Duration;

use aws_sdk_s3::error::{BoxError, DisplayErrorContext};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use futures_util::task::AtomicWaker;
use http_body::Frame;
use tokio::task::Unconstrained;

use super::error::{BodyError, Phase, S3Error};

/// The pieces of a PUT body.
type Pieces = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send + Sync>>;

/// A PUT body: a caller's source of pieces and the length it declares.
///
/// The declared length is the object's `Content-Length`, sent before the first byte. The
/// source must yield exactly that many bytes and then end. A source that ends short, runs
/// long, or fails makes the PUT fail with [`BodyError`]; the request is aborted mid-body, so
/// the server never sees a complete upload.
pub struct PutSource {
    length: u64,
    pieces: Pieces,
}

impl PutSource {
    /// A source of `length` bytes. `Sync` because the SDK's request body type is.
    pub fn new<S, E>(length: u64, pieces: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
        E: Into<BoxError>,
    {
        Self {
            length,
            pieces: Box::pin(pieces.map(|piece| piece.map_err(Into::into))),
        }
    }

    /// The declared length.
    pub fn length(&self) -> u64 {
        self.length
    }

    /// The request body handed to the SDK, plus the [`Upload`] record it keeps. The record is
    /// how a source failure reaches the caller as itself, whatever the SDK and hyper wrap it
    /// in on the way out, and how the caller learns whether the source had ended when the
    /// response arrived.
    pub(super) fn into_body(self) -> (DeclaredLengthBody, Arc<Upload>) {
        let upload = Arc::new(Upload {
            declared: self.length,
            produced: AtomicU64::new(0),
            ended: AtomicBool::new(false),
            failure: OnceLock::new(),
            answer: OnceLock::new(),
            turn: Arc::new(Turn::default()),
        });
        let body = DeclaredLengthBody {
            pieces: self.pieces,
            upload: Arc::clone(&upload),
        };
        (body, upload)
    }
}

/// What one PUT's body has done, shared between the body, the request that carries it
/// ([`Upload::request_first`]), the hook that sees the response arrive, and
/// [`super::S3Client::put_object`], which judges the outcome on it.
#[derive(Debug)]
pub(super) struct Upload {
    declared: u64,
    /// Bytes the source has given, all of them handed on to the SDK. Only the body writes it.
    produced: AtomicU64,
    /// The source has ended: it gave its whole declared length and then reported its end.
    /// Only the body writes it, and it never polls the source again after.
    ended: AtomicBool,
    /// The source's first failure; the body never polls the source again after one.
    failure: OnceLock<BodyError>,
    /// Set once, when the request sees the response head.
    answer: OnceLock<Answer>,
    /// Keeps the body from running ahead of the request.
    turn: Arc<Turn>,
}

/// The response head as the request first saw it.
#[derive(Debug)]
struct Answer {
    /// Whether the source had ended when hyper handed the response over.
    source_ended: bool,
    /// The response's `x-amz-request-id` header.
    request_id: Option<String>,
}

impl Upload {
    /// Wrap a PUT's request future so its body never runs ahead of it ([`Turn`]).
    pub(super) fn request_first<F: Future>(&self, request: F) -> RequestFirst<F> {
        RequestFirst {
            request: Box::pin(tokio::task::unconstrained(request)),
            waker: Waker::from(Arc::clone(&self.turn)),
            turn: Arc::clone(&self.turn),
        }
    }

    /// Record that the request has seen the response head. From here on the body takes
    /// nothing more from the source, so whether it has ended by now is final for judging the
    /// outcome.
    pub(super) fn answered(&self, request_id: Option<String>) {
        // This runs inside a poll of the request, and the body takes nothing until that poll
        // has ended ([`Turn`]), so `ended` still stands as it did when hyper handed the
        // response over.
        let _ = self.answer.set(Answer {
            source_ended: self.ended.load(Ordering::SeqCst),
            request_id,
        });
    }

    /// The source's own failure, if it failed.
    pub(super) fn failure(&self) -> Option<&BodyError> {
        self.failure.get()
    }

    /// The error for a success that arrived before the source had ended; `None` when the
    /// source had ended first, which makes the success a receipt. Having given its whole
    /// declared length is not enough: a source that has not reported its end may still run
    /// past that length, and until it does report it the SDK has not written the body's
    /// final chunk.
    ///
    /// A success with no record of its arrival is not a receipt either, since nothing then
    /// shows that the source had ended first. The SDK calls the hook that makes the record
    /// for every response it hands back ([`Upload::answered`]), so that is not expected.
    ///
    /// The byte count in the error is read now. It is still the count at the response's
    /// arrival: the body takes nothing from the source once the response has arrived
    /// ([`DeclaredLengthBody`]).
    pub(super) fn acknowledged_early(&self) -> Option<BodyError> {
        let answer = self.answer.get();
        if answer.is_some_and(|answer| answer.source_ended) {
            return None;
        }
        Some(BodyError::AcknowledgedEarly {
            declared: self.declared,
            produced: self.produced.load(Ordering::SeqCst),
            request_id: answer.and_then(|answer| answer.request_id.clone()),
        })
    }
}

/// The order between a PUT's request and its body.
///
/// hyper reads the response in its connection task. When the response head arrives it hands
/// the response to the request and wakes it, and then, in the same poll of the connection,
/// asks the body for more. Left alone, the body would take from the source after the
/// response had arrived and before the request had seen it, and a source that ended in that
/// gap would turn an early answer into a receipt.
///
/// So the request is polled with a waker that counts its wakes ([`RequestFirst`]). While the
/// request has a wake it has not finished a poll for, the body takes nothing: it waits for
/// that poll to end. The response's wake is one of them, so the request has always seen the
/// response, and recorded it ([`Upload::answered`]), before the body can take another piece.
/// A wake for anything else (a deadline, say) only makes the body wait one poll of the
/// request. The connection task is also the only one that polls the body, so a response
/// cannot arrive in the middle of a body poll.
#[derive(Debug, Default)]
struct Turn {
    /// How many times the request has been woken.
    wakes: AtomicU64,
    /// `wakes` as it stood when the request's latest finished poll began: that poll saw
    /// everything those wakes were for.
    seen: AtomicU64,
    /// The waker of the task that polls the request.
    request: AtomicWaker,
    /// The body's waker while it waits for the request.
    body: AtomicWaker,
}

impl Turn {
    /// Whether the request has finished a poll since its latest wake.
    fn caught_up(&self) -> bool {
        // `seen` first: it never runs ahead of `wakes`, so equal means caught up when `seen`
        // was read.
        let seen = self.seen.load(Ordering::SeqCst);
        seen == self.wakes.load(Ordering::SeqCst)
    }

    /// Whether the body has to wait for the request, in which case it is woken when the
    /// request's next poll ends.
    fn body_waits(&self, cx: &Context<'_>) -> bool {
        if self.caught_up() {
            return false;
        }
        self.body.register(cx.waker());
        // The request may have finished a poll between the check and the registration.
        !self.caught_up()
    }
}

impl Wake for Turn {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.request.wake();
    }
}

/// A PUT's request future, polled so its body never runs ahead of it ([`Turn`]).
///
/// The request is polled unconstrained by tokio's cooperative budget. An exhausted budget
/// makes a ready resource return pending and defers its wake-up, and the request would then
/// end a poll with the response in hand, unrecorded, and no wake counted yet.
///
/// A wake that comes after the request has finished is never caught up with, so the body
/// would wait for good. It never gets the chance: the request's runtime is shut down as soon
/// as the request finishes, before any task polls the body again (`put_object`).
pub(super) struct RequestFirst<F> {
    request: Pin<Box<Unconstrained<F>>>,
    /// Counts into `turn` and passes the wake on to the task that polls this.
    waker: Waker,
    turn: Arc<Turn>,
}

impl<F: Future> Future for RequestFirst<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        // Registered before the count is read, so a wake from here on reaches this task.
        this.turn.request.register(cx.waker());
        let wakes = this.turn.wakes.load(Ordering::SeqCst);
        let polled = this
            .request
            .as_mut()
            .poll(&mut Context::from_waker(&this.waker));
        this.turn.seen.store(wakes, Ordering::SeqCst);
        this.turn.body.wake();
        polled
    }
}

/// An `http_body::Body` over a [`PutSource`] that enforces the declared length.
///
/// Each piece is handed on by value as it arrives. After the declared length is reached the
/// source is polled once more, so excess bytes in a separate piece are caught before the
/// body reports its end. Only that poll, answered with the source's end, makes the source
/// ended ([`Upload::acknowledged_early`]). The SDK always asks for it: its aws-chunked layer,
/// which wraps this body, polls it until it reports its end, because only then can it write
/// the final chunk and the checksum trailer (`aws-runtime` 1.10.0,
/// `content_encoding/body/http_body_1_x.rs:56-83`).
///
/// From the moment hyper hands the response over, the source is not polled again and the
/// body sends nothing more: until the request has seen the response the body waits for it
/// ([`Turn`]), and once the request has recorded it ([`Upload::answered`]) the body stops for
/// good. The outcome is judged on whether the source had ended by then.
///
/// One poll handles at most one item of the source, so the work a poll does is bounded
/// whatever the source yields. That is what lets the operation deadline and a cancellation
/// reach the task that drives this body.
///
/// It keeps the default `size_hint` and `is_end_stream`: the SDK takes the decoded length
/// from the `Content-Length` that [`super::S3Client::put_object`] always sets, and reads the
/// body's own hint only when that header is absent.
pub(super) struct DeclaredLengthBody {
    pieces: Pieces,
    upload: Arc<Upload>,
}

impl http_body::Body for DeclaredLengthBody {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        let this = self.get_mut();
        let upload = &*this.upload;
        if let Some(failure) = upload.failure.get() {
            return Poll::Ready(Some(Err(failure.clone())));
        }
        if upload.ended.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        // The request has seen the response: take nothing more from the source and send
        // nothing more. Pending, not an error, because an error makes hyper fail the
        // connection, and with it a response body the SDK may still be reading. Nothing wakes
        // this poll again; the connection ends with the PUT's runtime (`put_object`).
        if upload.answer.get().is_some() {
            return Poll::Pending;
        }
        // The request has a wake it has not been polled for, perhaps the response: let it see
        // that first.
        if upload.turn.body_waits(cx) {
            return Poll::Pending;
        }
        let declared = upload.declared;
        let given = upload.produced.load(Ordering::SeqCst);
        let failure = match ready!(this.pieces.as_mut().poll_next(cx)) {
            // An empty piece carries nothing to send, and it is not skipped in a loop either:
            // a source that is always ready with empty pieces would then spin inside this one
            // poll forever, where no deadline and no cancellation can reach it (on a
            // current-thread runtime the deadline's timer could not even fire). The SDK's
            // aws-chunked layer loops the same way on an empty frame, so passing it on is no
            // better. Instead the poll ends here: wake this task and yield.
            Some(Ok(piece)) if piece.is_empty() => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Some(Ok(piece)) => {
                let produced = given.saturating_add(piece.len() as u64);
                if produced <= declared {
                    upload.produced.store(produced, Ordering::SeqCst);
                    return Poll::Ready(Some(Ok(Frame::data(piece))));
                }
                BodyError::SourceLength { declared, produced }
            }
            Some(Err(e)) => BodyError::SourceFailed {
                produced: given,
                detail: error_chain(e.as_ref()),
            },
            None if given == declared => {
                upload.ended.store(true, Ordering::SeqCst);
                return Poll::Ready(None);
            }
            None => BodyError::SourceLength {
                declared,
                produced: given,
            },
        };
        // First failure wins; the body never polls its source again after one.
        let failure = upload.failure.get_or_init(|| failure).clone();
        Poll::Ready(Some(Err(failure)))
    }
}

/// An error and its sources, joined with `": "`.
fn error_chain(err: &(dyn Error + 'static)) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(e) = source {
        text.push_str(": ");
        text.push_str(&e.to_string());
        source = e.source();
    }
    text
}

/// A GET body, read piece by piece.
///
/// Every piece is handed over as the SDK yields it. Each wait for the next piece is bounded
/// by the body-idle deadline. The body is trusted only up to its declared `Content-Length`:
/// a body that fails, ends short or runs past it is a [`BodyError`], never a shorter or
/// longer object. After the first error every later call returns that error again.
pub struct ObjectBody {
    stream: ByteStream,
    declared: u64,
    received: u64,
    idle: Duration,
    ended: bool,
    failed: Option<S3Error>,
}

impl ObjectBody {
    pub(super) fn new(stream: ByteStream, declared: u64, idle: Duration) -> Self {
        Self {
            stream,
            declared,
            received: 0,
            idle,
            ended: false,
            failed: None,
        }
    }

    /// The object's declared `Content-Length`.
    pub fn content_length(&self) -> u64 {
        self.declared
    }

    /// The next piece, or `None` once exactly `content_length` bytes have been handed over
    /// and the body has ended.
    pub async fn next_piece(&mut self) -> Result<Option<Bytes>, S3Error> {
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        if self.ended {
            return Ok(None);
        }
        let next = self.read().await;
        if let Err(e) = &next {
            self.failed = Some(e.clone());
        }
        next
    }

    async fn read(&mut self) -> Result<Option<Bytes>, S3Error> {
        // Clock: tokio's runtime clock, the one the SDK's connect and operation deadlines
        // also sleep on, so every deadline of one request reads the same source.
        let next = tokio::time::timeout(self.idle, self.stream.next())
            .await
            .map_err(|_| S3Error::Timeout {
                phase: Phase::BodyIdle,
                limit: self.idle,
            })?;
        // deferred: #853 — the two length checks below (a piece past the declared length, an
        // end short of it) can fire only when the response's framing disagrees with its
        // `Content-Length`. With a conforming response hyper reads exactly the declared
        // length and reports a cut connection as a transport error, so only #853's
        // non-conforming framings exercise them.
        match next {
            Some(Ok(piece)) => {
                let received = self.received.saturating_add(piece.len() as u64);
                if received > self.declared {
                    return Err(S3Error::Body(BodyError::Length {
                        declared: self.declared,
                        received,
                    }));
                }
                self.received = received;
                Ok(Some(piece))
            }
            Some(Err(e)) => Err(S3Error::Body(BodyError::Transport {
                declared: self.declared,
                received: self.received,
                detail: DisplayErrorContext(&e).to_string(),
            })),
            None if self.received == self.declared => {
                self.ended = true;
                Ok(None)
            }
            None => Err(S3Error::Body(BodyError::Length {
                declared: self.declared,
                received: self.received,
            })),
        }
    }
}
