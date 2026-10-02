//! Object bodies, streamed both ways (invariant "stream, don't buffer", the one the gateway
//! itself holds at `crates/gateway-s3/src/lib.rs:12-17`).
//!
//! Neither direction keeps a copy of bytes it has passed on. A PUT piece goes from the
//! caller's source to the SDK by value and nothing here retains it; a GET piece goes from the
//! SDK's body stream to the caller by value, and only its length is counted. There is no
//! `collect`, no aggregation, and no growing buffer in either path.

use std::error::Error;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{ready, Context, Poll};
use std::time::Duration;

use aws_sdk_s3::error::{BoxError, DisplayErrorContext};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use http_body::Frame;

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

    /// The request body handed to the SDK, plus the slot it records its own failure in.
    /// The slot is how a source failure reaches the caller as itself, whatever the SDK and
    /// hyper wrap it in on the way out.
    pub(super) fn into_body(self) -> (DeclaredLengthBody, Arc<OnceLock<BodyError>>) {
        let failure = Arc::new(OnceLock::new());
        let body = DeclaredLengthBody {
            pieces: self.pieces,
            declared: self.length,
            produced: 0,
            ended: false,
            failure: Arc::clone(&failure),
        };
        (body, failure)
    }
}

/// An `http_body::Body` over a [`PutSource`] that enforces the declared length.
///
/// Each piece is handed on by value as it arrives. After the declared length is reached the
/// source is polled once more, so excess bytes in a separate piece are caught before the
/// body reports its end.
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
    declared: u64,
    produced: u64,
    ended: bool,
    failure: Arc<OnceLock<BodyError>>,
}

impl http_body::Body for DeclaredLengthBody {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        let this = self.get_mut();
        if let Some(failure) = this.failure.get() {
            return Poll::Ready(Some(Err(failure.clone())));
        }
        if this.ended {
            return Poll::Ready(None);
        }
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
                let produced = this.produced.saturating_add(piece.len() as u64);
                if produced <= this.declared {
                    this.produced = produced;
                    return Poll::Ready(Some(Ok(Frame::data(piece))));
                }
                BodyError::SourceLength {
                    declared: this.declared,
                    produced,
                }
            }
            Some(Err(e)) => BodyError::SourceFailed {
                produced: this.produced,
                detail: error_chain(e.as_ref()),
            },
            None if this.produced == this.declared => {
                this.ended = true;
                return Poll::Ready(None);
            }
            None => BodyError::SourceLength {
                declared: this.declared,
                produced: this.produced,
            },
        };
        // First failure wins; the body never polls its source again after one.
        let failure = this.failure.get_or_init(|| failure).clone();
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
