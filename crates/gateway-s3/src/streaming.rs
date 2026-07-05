//! `aws-chunked` streaming-signature payload decoding — the wire form a **stock modern
//! SDK** (boto3, aws-sdk) sends an object PUT in (`Content-Encoding: aws-chunked`,
//! `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`). Without this the gateway
//! rejects a default SDK upload with `501` — so it is not actually S3-compatible for a real
//! client (issue #364 carry-forward: the recurring "real-SDK interop / streaming 501").
//!
//! # The framing
//! The body is a sequence of chunks, each
//! `<hex-length>;chunk-signature=<64-hex>\r\n<length bytes of data>\r\n`, terminated by a
//! final zero-length chunk `0;chunk-signature=<64-hex>\r\n` (a trailer variant may append
//! `<name>:<value>\r\n` trailer headers before the closing `\r\n`). The **signed** variants
//! chain a per-chunk signature off the request's seed signature; the
//! `STREAMING-UNSIGNED-PAYLOAD-TRAILER` variant carries framing only.
//!
//! # Fail-closed (invariant "auth is fail-closed")
//! For a signed stream every chunk's signature is recomputed and compared in constant time
//! ([`sign_chunk`]); a chunk whose signature does not verify aborts the decode with
//! [`StreamingError::ChunkSignature`] (→ HTTP 403) **before** its bytes reach the store, so
//! a body that was not signed by the credential holder is never published. The chunk-signing
//! algorithm is pinned to AWS's **published** streaming worked example (the
//! `aws_published_streaming_example` known-answer test), so this is AWS-correct — a genuine
//! independent oracle, not self-consistency.
//!
//! # Stream, don't buffer (invariant 0015:789)
//! Decoding is incremental over a bounded [`tokio::sync::mpsc`] channel: a chunk's data is
//! forwarded as it is parsed and the reader task blocks on the channel, so peak resident
//! bytes stay `O(chunk size)`, never `O(object)`. The decoded stream feeds straight into
//! the object-store's streaming PUT
//! ([`wyrd_gateway_core::ObjectGateway::put_object_streaming`]).
//!
//! # Bounded per-chunk allocation (issue #364 carry-forward, iter-6 item 2)
//! A chunk must be buffered whole to verify its signature (the signature is computed over
//! the chunk data), so an *unbounded* declared chunk size is a pre-auth memory-amplification
//! lever — the 0015:789 OOM cliff on the wire. The declared size is therefore bounded
//! ([`MAX_CHUNK_SIZE`]): a header claiming more is refused with a framing error (→ HTTP 400)
//! **before** any of its body is buffered, never read to a silent truncated `200 OK`.

use std::fmt;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use wyrd_traits::{BoxError, Result};

use crate::crypto;
use crate::sigv4::StreamingContext;

/// SHA-256 of the empty string — the fixed `Hash("")` line in every chunk string-to-sign.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The maximum a single `aws-chunked` chunk may **declare**. A chunk is buffered whole to
/// verify its signature, so this bounds per-chunk resident bytes — a header declaring a
/// gigabyte is refused on the declared size before a byte of it is buffered (issue #364
/// carry-forward, iter-6 item 2; 0015:789 OOM cliff). 16 MiB is far above any stock SDK's
/// aws-chunked chunk (KBs–low MBs) yet caps a hostile one; it also keeps `size + 2` (the
/// chunk + trailing CRLF) well clear of `usize` overflow.
pub(crate) const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// A refused / malformed `aws-chunked` streaming upload.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamingError {
    /// The chunk framing was malformed (bad size line, missing CRLF, truncated body).
    /// Maps to HTTP 400.
    Framing(String),
    /// A chunk's signature did not verify — the body was not signed by the credential
    /// holder. Maps to HTTP 403 (fail-closed); the object is never committed.
    ChunkSignature,
}

impl fmt::Display for StreamingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamingError::Framing(what) => write!(f, "malformed aws-chunked body: {what}"),
            StreamingError::ChunkSignature => {
                write!(f, "aws-chunked chunk signature does not verify")
            }
        }
    }
}

impl std::error::Error for StreamingError {}

fn framing(what: &str) -> BoxError {
    Box::new(StreamingError::Framing(what.to_string()))
}

/// The SigV4 **chunk signature** for `chunk_data`, chained from `previous_signature`
/// (AWS "Signature Calculations … Transferring Payload in Multiple Chunks"):
///
/// ```text
/// StringToSign = "AWS4-HMAC-SHA256-PAYLOAD" \n <date-time> \n <scope> \n
///                <previous-signature> \n Hash("") \n Hash(chunk-data)
/// chunk-signature = HexEncode(HMAC(signing-key, StringToSign))
/// ```
///
/// Shared by the decoder (to verify) and test harnesses (to produce), so the two can never
/// drift. Pinned to AWS's published values by [`tests::aws_published_streaming_example`].
pub fn sign_chunk(ctx: &StreamingContext, previous_signature: &str, chunk_data: &[u8]) -> String {
    let data_hash = crypto::hex(&crypto::sha256(chunk_data));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        ctx.date_time, ctx.scope, previous_signature, EMPTY_SHA256, data_hash
    );
    crypto::hex(&crypto::hmac_sha256(
        &ctx.signing_key,
        string_to_sign.as_bytes(),
    ))
}

/// Decode an `aws-chunked` body: strip the chunk framing, verify each chunk (for a signed
/// stream), and yield the raw object bytes as a stream — the same `Stream<Item = Result<Bytes>>`
/// shape [`crate::Gateway::put_object_streaming`] consumes. Runs on a spawned task feeding a
/// bounded channel, so the object is never resident whole.
pub fn decode<S>(source: S, ctx: StreamingContext) -> ReceiverStream<Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
{
    // A small bound so peak resident bytes stay O(chunk size): the decoder blocks here until
    // the write path drains a decoded chunk.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);
    tokio::spawn(async move {
        let mut decoder = Decoder::new(source, ctx);
        loop {
            match decoder.next_chunk().await {
                Ok(Some(data)) => {
                    if tx.send(Ok(data)).await.is_err() {
                        break; // receiver gone (write aborted) — stop decoding.
                    }
                }
                Ok(None) => break, // final (zero-length) chunk consumed cleanly.
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    break;
                }
            }
        }
    });
    ReceiverStream::new(rx)
}

/// A stateful `aws-chunked` parser over a rolling byte buffer. Pulls from the inner stream
/// only as far as it needs to satisfy the next header line / chunk body, so a chunk that
/// spans several transport frames (or several chunks in one frame) both parse correctly.
struct Decoder<S> {
    source: S,
    buf: Vec<u8>,
    eof: bool,
    ctx: StreamingContext,
    /// The previous chunk's signature (seed for the first chunk) — the chunk chain.
    previous_signature: String,
    done: bool,
}

impl<S> Decoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    fn new(source: S, ctx: StreamingContext) -> Self {
        let previous_signature = ctx.seed_signature.clone();
        Self {
            source,
            buf: Vec::new(),
            eof: false,
            ctx,
            previous_signature,
            done: false,
        }
    }

    /// Pull one more transport frame into `buf`. Returns whether more data arrived (`false`
    /// once the inner stream is exhausted). A source error propagates unchanged.
    async fn fill(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }
        match self.source.next().await {
            Some(Ok(bytes)) => {
                self.buf.extend_from_slice(&bytes);
                Ok(true)
            }
            Some(Err(err)) => Err(err),
            None => {
                self.eof = true;
                Ok(false)
            }
        }
    }

    /// Parse and de-frame the next chunk. `Ok(Some(data))` for a data chunk, `Ok(None)` when
    /// the terminating zero-length chunk has been consumed, `Err` on bad framing / a chunk
    /// signature that fails to verify.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.done {
            return Ok(None);
        }

        // 1. The chunk header line, up to CRLF.
        let header = loop {
            if let Some(pos) = find_crlf(&self.buf) {
                let line = self.buf[..pos].to_vec();
                self.buf.drain(..pos + 2);
                break line;
            }
            if !self.fill().await? {
                return Err(framing("body ended before a chunk header"));
            }
        };
        let (size, chunk_sig) = parse_chunk_header(&header)?;

        // Bound the DECLARED chunk size before buffering any of its body: an unbounded size
        // would force the gateway to buffer (and hash) up to that many bytes before the
        // signature — computed over the chunk data — could reject them, a pre-auth memory
        // amplification (issue #364 carry-forward, iter-6 item 2; 0015:789). Refuse on the
        // declared size with a framing error (→ 400), never a silent truncated 200 OK.
        if size > MAX_CHUNK_SIZE {
            return Err(framing(&format!(
                "declared chunk size {size} exceeds the {MAX_CHUNK_SIZE}-byte maximum"
            )));
        }

        // 2. The chunk data plus its trailing CRLF.
        while self.buf.len() < size + 2 {
            if !self.fill().await? {
                return Err(framing("body ended mid-chunk"));
            }
        }
        let data = self.buf[..size].to_vec();
        if &self.buf[size..size + 2] != b"\r\n" {
            return Err(framing("chunk not CRLF-terminated"));
        }
        self.buf.drain(..size + 2);

        // 3. Fail-closed verification of the chunk signature (signed variants).
        if self.ctx.signed {
            let presented =
                chunk_sig.ok_or_else(|| framing("signed chunk missing chunk-signature"))?;
            let expected = sign_chunk(&self.ctx, &self.previous_signature, &data);
            if !crypto::constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
                return Err(Box::new(StreamingError::ChunkSignature));
            }
            self.previous_signature = expected;
        }

        if size == 0 {
            // The terminating chunk: nothing more is object data (any trailer headers that
            // follow are consumed-and-ignored — the seed/chunk signatures already
            // authenticated the body).
            self.done = true;
            return Ok(None);
        }
        Ok(Some(Bytes::from(data)))
    }
}

/// Find the first `\r\n` in `buf`, returning the index of the `\r`.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Parse a chunk header line `<hex-size>[;chunk-signature=<sig>][;…]` into `(size, signature)`.
fn parse_chunk_header(line: &[u8]) -> Result<(usize, Option<String>)> {
    let line = std::str::from_utf8(line).map_err(|_| framing("non-UTF-8 chunk header"))?;
    let mut parts = line.split(';');
    let size_hex = parts.next().unwrap_or("").trim();
    let size = usize::from_str_radix(size_hex, 16)
        .map_err(|_| framing("chunk size is not hexadecimal"))?;
    let mut signature = None;
    for ext in parts {
        if let Some(v) = ext.trim().strip_prefix("chunk-signature=") {
            signature = Some(v.to_string());
        }
    }
    Ok((size, signature))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::signing_key_for;

    /// Build the streaming context for AWS's published streaming example.
    /// <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html>
    fn aws_example_ctx() -> StreamingContext {
        StreamingContext {
            seed_signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                .to_string(),
            signing_key: signing_key_for(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "20130524",
                "us-east-1",
                "s3",
            ),
            date_time: "20130524T000000Z".to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed: true,
        }
    }

    /// Known-answer test against AWS's **published** chunked-upload worked example — the
    /// independent oracle proving the chunk-signing algorithm is AWS-correct, not merely
    /// self-consistent with the decoder. AWS documents these exact chunk signatures for a
    /// 66560-byte object of `a`s split as 64 KiB + 1 KiB + a 0-byte terminator, seeded from
    /// signature `4f232c43…`.
    #[test]
    fn aws_published_streaming_example() {
        let ctx = aws_example_ctx();

        let chunk1 = vec![b'a'; 65536];
        let sig1 = sign_chunk(&ctx, &ctx.seed_signature, &chunk1);
        assert_eq!(
            sig1,
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        );

        let chunk2 = vec![b'a'; 1024];
        let sig2 = sign_chunk(&ctx, &sig1, &chunk2);
        assert_eq!(
            sig2,
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
        );

        // The terminating zero-length chunk, chained off chunk 2.
        let sig3 = sign_chunk(&ctx, &sig2, b"");
        assert_eq!(
            sig3,
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
        );
    }

    /// Frame `chunks` as a signed `aws-chunked` body (what a real SDK puts on the wire),
    /// chaining each chunk's signature off the seed exactly as [`sign_chunk`] / the decoder do.
    fn frame_signed(ctx: &StreamingContext, chunks: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut prev = ctx.seed_signature.clone();
        // The data chunks followed by the terminating zero-length chunk.
        let mut all: Vec<&[u8]> = chunks.to_vec();
        all.push(&[]);
        for data in all {
            let sig = sign_chunk(ctx, &prev, data);
            out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", data.len()).as_bytes());
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
            prev = sig;
        }
        out
    }

    /// Drive the async decoder over an in-memory framed body split into arbitrary transport
    /// pieces, and collect the decoded object bytes.
    async fn decode_to_vec(
        framed: Vec<u8>,
        piece: usize,
        ctx: StreamingContext,
    ) -> Result<Vec<u8>> {
        let pieces: Vec<Result<Bytes>> = framed
            .chunks(piece.max(1))
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let source = futures_util::stream::iter(pieces);
        let mut out = Vec::new();
        let mut decoded = decode(Box::pin(source), ctx);
        while let Some(item) = decoded.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn decodes_a_signed_chunked_body_byte_identically() {
        let ctx = aws_example_ctx();
        let object: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        // Frame it in three data chunks of uneven size + the terminator.
        let framed = frame_signed(
            &ctx,
            &[&object[..2048], &object[2048..4096], &object[4096..]],
        );

        // Feed it through the decoder at several transport-piece granularities (a chunk may
        // span pieces, or several may share one) — all must recover the object exactly.
        for piece in [1usize, 7, 64, 512, framed.len()] {
            let decoded = decode_to_vec(framed.clone(), piece, ctx.clone())
                .await
                .expect("a well-signed body decodes");
            assert_eq!(decoded, object, "piece size {piece}");
        }
    }

    #[tokio::test]
    async fn a_tampered_chunk_is_refused_fail_closed() {
        let ctx = aws_example_ctx();
        let object = vec![b'z'; 1000];
        let mut framed = frame_signed(&ctx, &[&object]);
        // Flip a byte of the FIRST chunk's data: its signature no longer verifies.
        let tamper_at = framed
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("first header")
            + 2;
        framed[tamper_at] ^= 0xff;

        let err = decode_to_vec(framed, 512, ctx)
            .await
            .expect_err("a tampered chunk must be refused");
        let streaming = err
            .downcast_ref::<StreamingError>()
            .expect("a streaming error");
        assert_eq!(
            *streaming,
            StreamingError::ChunkSignature,
            "a body the credential holder did not sign is refused fail-closed"
        );
    }

    /// Issue #364 carry-forward, iter-6 item 2 (fail-open in streaming): a chunk header that
    /// declares a size beyond [`MAX_CHUNK_SIZE`] must be refused **on the declared size**,
    /// before its body is buffered — a framing error (→ 400), not a read-to-EOF nor a huge
    /// allocation. The header alone (no body) is enough to reject: RED before the bound
    /// existed (the decoder would try to `fill()` toward the declared size and fail late with
    /// "body ended mid-chunk"); GREEN once the declared size is bounded up front.
    #[tokio::test]
    async fn an_oversized_chunk_header_is_refused_on_the_declared_size() {
        let ctx = aws_example_ctx();
        // Declare one byte over the cap; send no body at all.
        let framed = format!(
            "{:x};chunk-signature={}\r\n",
            MAX_CHUNK_SIZE + 1,
            "0".repeat(64)
        )
        .into_bytes();
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("an over-cap chunk must be refused");
        match err.downcast_ref::<StreamingError>() {
            Some(StreamingError::Framing(what)) => assert!(
                what.contains("exceeds"),
                "must be refused on the declared size bound, not read to EOF: {what}"
            ),
            other => panic!("expected a framing error citing the size bound, got {other:?}"),
        }
    }

    /// A malformed chunk header (a non-hexadecimal size) is refused with a framing error
    /// (→ 400), never hung on or silently accepted (issue #364 carry-forward, iter-6 item 2:
    /// "malformed … chunk header must return 400").
    #[tokio::test]
    async fn a_malformed_chunk_header_is_refused() {
        let ctx = aws_example_ctx();
        let framed = b"zzzz;chunk-signature=deadbeef\r\nignored".to_vec();
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("a non-hex size line must be refused");
        assert!(
            matches!(
                err.downcast_ref::<StreamingError>(),
                Some(StreamingError::Framing(_))
            ),
            "a malformed chunk header is a framing error (400), not a panic or a silent accept"
        );
    }
}
