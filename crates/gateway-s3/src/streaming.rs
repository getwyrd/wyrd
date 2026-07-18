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

use crate::checksum::{self, RunningChecksum};
use crate::crypto;
use crate::sigv4::{StreamingContext, TrailerDeclaration};

/// SHA-256 of the empty string — the fixed `Hash("")` line in every chunk string-to-sign.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The maximum a single `aws-chunked` chunk may **declare**. A chunk is buffered whole to
/// verify its signature, so this bounds per-chunk resident bytes — a header declaring a
/// gigabyte is refused on the declared size before a byte of it is buffered (issue #364
/// carry-forward, iter-6 item 2; 0015:789 OOM cliff). 16 MiB is far above any stock SDK's
/// aws-chunked chunk (KBs–low MBs) yet caps a hostile one; it also keeps `size + 2` (the
/// chunk + trailing CRLF) well clear of `usize` overflow.
pub(crate) const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// The maximum a `-TRAILER` framing's trailer SECTION (the lines after the terminating
/// zero-length chunk, up to the closing blank line) may occupy before it is refused — the
/// same bounded-buffering discipline as [`MAX_CHUNK_SIZE`] (issue #364 carry-forward,
/// iter-6 item 2), applied to the new trailer-consuming code path (issue #505): a checksum
/// trailer is at most a couple of short header lines, so 8 KiB is far above any real one
/// yet caps a client that never sends the closing CRLF.
const MAX_TRAILER_SIZE: usize = 8 * 1024;

/// A refused / malformed `aws-chunked` streaming upload.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamingError {
    /// The chunk framing was malformed (bad size line, missing CRLF, truncated body) — or,
    /// for a `-TRAILER` framing (issue #505), a malformed trailer section: bad base64, a
    /// trailer name not declared in `x-amz-trailer`, a declared trailer never sent, a
    /// `x-amz-decoded-content-length` mismatch, or bytes after the trailer block. Maps to
    /// HTTP 400.
    Framing(String),
    /// A chunk's signature did not verify — the body was not signed by the credential
    /// holder — or, for a signed `-TRAILER` framing, the trailer signature did not verify
    /// (the declared trailer headers were not signed by the credential holder). Maps to
    /// HTTP 403 (fail-closed); the object is never committed.
    ChunkSignature,
    /// The declared `x-amz-checksum-*` trailer value does not match the checksum of the
    /// bytes actually streamed (issue #505: a tampered — or simply wrong — declared
    /// checksum). Maps to HTTP 400; the object is never committed. Deliberately distinct
    /// from [`StreamingError::ChunkSignature`]: this is a content-integrity mismatch, not
    /// (necessarily) an authentication failure — the unsigned `-TRAILER` variant has no
    /// signature to fail, only a checksum to mismatch.
    ChecksumMismatch,
}

impl fmt::Display for StreamingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamingError::Framing(what) => write!(f, "malformed aws-chunked body: {what}"),
            StreamingError::ChunkSignature => {
                write!(f, "aws-chunked chunk signature does not verify")
            }
            StreamingError::ChecksumMismatch => {
                write!(
                    f,
                    "declared x-amz-checksum-* trailer does not match the streamed body"
                )
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

/// The SigV4 **trailer signature** for a signed `-TRAILER` framing's declared trailer
/// headers, chained from `previous_signature` — the terminating zero-length chunk's OWN
/// signature (AWS "Signing the trailing header"):
///
/// ```text
/// StringToSign = "AWS4-HMAC-SHA256-TRAILER" \n <date-time> \n <scope> \n
///                <previous-signature> \n Hash(canonical-trailer-headers)
/// trailer-signature = HexEncode(HMAC(signing-key, StringToSign))
/// ```
///
/// `canonical_trailer_headers` is the exact `name:value\n` block that was signed — one
/// line per declared trailer header, lower-cased name, in the order sent (mirrors the
/// canonical-header-block shape the header-based signature already builds, `sigv4.rs`'s
/// `canonical_request`). Shared by the decoder (to verify) and test harnesses (to
/// produce), so the two can never drift — the same discipline [`sign_chunk`] keeps.
/// Pinned to a **published** worked example (AWS's own `aws-c-auth` chunked-signing test
/// suite, `sigv4_trailing_headers_signing_test`, which chains off this module's own
/// `aws_published_streaming_example` seed/final-chunk signatures) by
/// `tests::aws_published_trailing_headers_example`.
pub fn sign_trailer(
    ctx: &StreamingContext,
    previous_signature: &str,
    canonical_trailer_headers: &str,
) -> String {
    let block_hash = crypto::hex(&crypto::sha256(canonical_trailer_headers.as_bytes()));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        ctx.date_time, ctx.scope, previous_signature, block_hash
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
    /// The previous chunk's signature (seed for the first chunk) — the chunk chain. Also
    /// the seed the trailer signature (a `-TRAILER` framing, signed variant) chains from,
    /// once the terminating zero-length chunk has updated it.
    previous_signature: String,
    done: bool,
    /// A running checksum over the DECODED object bytes, for a `-TRAILER` framing (`None`
    /// for the plain no-trailer sentinel) — checked against the declared trailer value once
    /// the trailer section is consumed (issue #505: validated, not consumed-and-trusted).
    checksum: Option<RunningChecksum>,
    /// De-framed object bytes seen so far, checked against a declared
    /// `x-amz-decoded-content-length` once the trailer is consumed.
    decoded_len: u64,
}

impl<S> Decoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    fn new(source: S, ctx: StreamingContext) -> Self {
        let previous_signature = ctx.seed_signature.clone();
        let checksum = ctx
            .trailer
            .as_ref()
            .map(|t| RunningChecksum::new(t.algorithm));
        Self {
            source,
            buf: Vec::new(),
            eof: false,
            ctx,
            previous_signature,
            done: false,
            checksum,
            decoded_len: 0,
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

        // 2. The chunk data plus its trailing CRLF — EXCEPT for the terminating chunk of a
        // `-TRAILER` framing (issue #505), which has no data/CRLF of its own: the trailer
        // section follows directly after the chunk header's own CRLF, exactly as HTTP/1.1
        // chunked-transfer trailers follow the closing `0\r\n` line. The plain, no-trailer
        // sentinel's terminator is unchanged: `0[;chunk-signature=…]\r\n\r\n` (the second
        // `\r\n` consumed here as the "empty data + its own CRLF").
        let is_trailer_terminator = size == 0 && self.ctx.trailer.is_some();
        let data = if is_trailer_terminator {
            Vec::new()
        } else {
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
            data
        };

        // 3. Fail-closed verification of the chunk signature (signed variants) — applies to
        // the terminating chunk too (over empty data), chaining `previous_signature` for a
        // signed `-TRAILER` framing's trailer signature below.
        if self.ctx.signed {
            let presented =
                chunk_sig.ok_or_else(|| framing("signed chunk missing chunk-signature"))?;
            let expected = sign_chunk(&self.ctx, &self.previous_signature, &data);
            if !crypto::constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
                return Err(Box::new(StreamingError::ChunkSignature));
            }
            self.previous_signature = expected;
        }

        // 4. Track the running checksum / decoded length over what is ACTUALLY streamed
        // (issue #505) — a no-op for the plain no-trailer sentinel (`checksum` is `None`)
        // and for the terminating chunk (`data` is always empty there).
        if let Some(checksum) = self.checksum.as_mut() {
            checksum.update(&data);
        }
        self.decoded_len += data.len() as u64;

        if size == 0 {
            if let Some(trailer) = self.ctx.trailer.clone() {
                self.consume_trailer(&trailer).await?;
            }
            self.done = true;
            return Ok(None);
        }
        Ok(Some(Bytes::from(data)))
    }

    /// Read the next CRLF-terminated line of the trailer section. `Ok(None)` marks the
    /// blank line that closes the block (already consumed). Bounded by [`MAX_TRAILER_SIZE`]
    /// so a client that never sends the closing CRLF cannot force unbounded buffering — the
    /// same discipline [`MAX_CHUNK_SIZE`] applies to a chunk's declared size (issue #364
    /// carry-forward, iter-6 item 2).
    async fn next_trailer_line(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(pos) = find_crlf(&self.buf) {
                if pos == 0 {
                    self.buf.drain(..2);
                    return Ok(None);
                }
                let line = self.buf[..pos].to_vec();
                self.buf.drain(..pos + 2);
                return Ok(Some(line));
            }
            if self.buf.len() > MAX_TRAILER_SIZE {
                return Err(framing(&format!(
                    "aws-chunked trailer section exceeds the {MAX_TRAILER_SIZE}-byte bound"
                )));
            }
            if !self.fill().await? {
                return Err(framing("body ended inside the aws-chunked trailer section"));
            }
        }
    }

    /// Consume, validate, and (for the signed variant) authenticate the `-TRAILER`
    /// framing's trailer section — the lines after the terminating zero-length chunk, up to
    /// the closing blank line. Fail-closed on every axis (issue #505): a trailer name not
    /// declared in `x-amz-trailer` is refused (400), a declared trailer never sent is
    /// refused (400), a malformed base64 checksum value is refused (400), a signed
    /// framing's trailer signature that does not verify is refused (403, the object is
    /// never committed), a mismatched checksum against what was ACTUALLY streamed is
    /// refused (400, the object is never committed — never consumed-and-trusted), a
    /// mismatched `x-amz-decoded-content-length` is refused (400), and any bytes left over
    /// after the closing blank line — garbage the client appended past the logical end of
    /// the body — are refused (400) rather than silently dropped.
    async fn consume_trailer(&mut self, trailer: &TrailerDeclaration) -> Result<()> {
        let mut declared_value: Option<(String, Vec<u8>)> = None; // (name, decoded checksum)
        let mut canonical_line: Option<String> = None; // the exact `name:value\n` that was signed
        let mut trailer_signature: Option<String> = None;

        while let Some(line) = self.next_trailer_line().await? {
            let line = std::str::from_utf8(&line)
                .map_err(|_| framing("non-UTF-8 aws-chunked trailer line"))?;
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| framing("malformed aws-chunked trailer line (missing `:`)"))?;
            let lower_name = name.trim().to_ascii_lowercase();
            let value = value.trim();

            if lower_name == "x-amz-trailer-signature" {
                if !self.ctx.signed {
                    return Err(framing(
                        "x-amz-trailer-signature on an unsigned streaming upload",
                    ));
                }
                if trailer_signature.is_some() {
                    return Err(framing("duplicate x-amz-trailer-signature"));
                }
                trailer_signature = Some(value.to_string());
                continue;
            }

            if lower_name != trailer.name {
                return Err(framing(&format!(
                    "trailer `{lower_name}` was not declared in x-amz-trailer"
                )));
            }
            if declared_value.is_some() {
                return Err(framing("duplicate declared trailer header"));
            }
            let decoded = checksum::base64_decode(value)
                .ok_or_else(|| framing("declared trailer checksum is not valid base64"))?;
            canonical_line = Some(format!("{lower_name}:{value}\n"));
            declared_value = Some((lower_name, decoded));
        }

        let (_, declared_bytes) = declared_value
            .ok_or_else(|| framing("declared x-amz-trailer checksum was never sent"))?;

        // Fail-closed authentication of the trailer headers themselves (signed variant
        // only): a tampered/forged trailer — including a tampered checksum value, since it
        // is part of what was signed — is refused BEFORE the checksum comparison below,
        // same "signature first" discipline as the header-based request.
        if self.ctx.signed {
            let presented = trailer_signature.ok_or_else(|| {
                framing("signed streaming upload missing x-amz-trailer-signature")
            })?;
            let canonical = canonical_line.expect("declared_value implies canonical_line");
            let expected = sign_trailer(&self.ctx, &self.previous_signature, &canonical);
            if !crypto::constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
                return Err(Box::new(StreamingError::ChunkSignature));
            }
        }

        // Content-integrity check: the declared checksum against what was ACTUALLY streamed
        // — never consumed-and-trusted (issue #505's invariant: a validated trailer, not a
        // half-accept in new clothes).
        let computed = self
            .checksum
            .take()
            .expect("trailer-mode Decoder always carries a running checksum")
            .finalize();
        if !crypto::constant_time_eq(&computed, &declared_bytes) {
            return Err(Box::new(StreamingError::ChecksumMismatch));
        }

        if let Some(expected_len) = trailer.decoded_content_length {
            if expected_len != self.decoded_len {
                return Err(framing(
                    "x-amz-decoded-content-length does not match the streamed body",
                ));
            }
        }

        // No half-accept on trailing garbage either: pull once more (or until EOF) and
        // refuse if the client sent anything past the logical end of the aws-chunked body,
        // rather than silently dropping it.
        while self.buf.is_empty() {
            if !self.fill().await? {
                break;
            }
        }
        if !self.buf.is_empty() {
            return Err(framing("bytes present after the aws-chunked trailer block"));
        }

        Ok(())
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
            trailer: None,
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

    // ---- issue #505: `-TRAILER` framing (checksum-trailer) tests ----

    /// AWS's published trailing-header (checksum-trailer) chunked-signing worked example —
    /// from AWS's OWN open-source SigV4 streaming conformance suite (`aws-c-auth`,
    /// `tests/test_chunked_signing.c`, `sigv4_trailing_headers_signing_test`), which chains
    /// off exactly the SAME credentials/date/scope AND the SAME seed + chunk signatures
    /// [`aws_published_streaming_example`] pins above: three trailer headers `first:1st`,
    /// `second:2nd`, `third:3rd`, the trailer signature chained off the terminating
    /// zero-length chunk's own signature (`b6c6ea8a…`, `sig3` above). The independent oracle
    /// proving [`sign_trailer`] is AWS-correct, not merely self-consistent with the decoder
    /// that also calls it.
    #[test]
    fn aws_published_trailing_headers_example() {
        let ctx = aws_example_ctx();
        let final_chunk_signature =
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9";
        let canonical_trailer_headers = "first:1st\nsecond:2nd\nthird:3rd\n";
        let trailer_signature =
            sign_trailer(&ctx, final_chunk_signature, canonical_trailer_headers);
        assert_eq!(
            trailer_signature,
            "df5735bd9f3295cd9386572292562fefc93ba94e80a0a1ddcbd652c4e0a75e6c"
        );
    }

    /// [`aws_example_ctx`], but declaring a `-TRAILER` framing's trailer — `signed` picks
    /// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (`true`) vs
    /// `STREAMING-UNSIGNED-PAYLOAD-TRAILER` (`false`).
    fn aws_example_ctx_with_trailer(
        signed: bool,
        name: &str,
        algorithm: checksum::ChecksumAlgorithm,
    ) -> StreamingContext {
        StreamingContext {
            signed,
            trailer: Some(TrailerDeclaration {
                name: name.to_string(),
                algorithm,
                decoded_content_length: None,
            }),
            ..aws_example_ctx()
        }
    }

    /// Standard base64 (RFC 4648, `+`/`/`, `=`-padded) — independent of
    /// [`checksum::base64_decode`] (this is an ENCODER, the production module has no
    /// encoder), used only to build test wire bytes.
    fn test_base64_encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0];
            let b1 = *chunk.get(1).unwrap_or(&0);
            let b2 = *chunk.get(2).unwrap_or(&0);
            let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
            out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[((n >> 6) & 0x3f) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[(n & 0x3f) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    /// Frame `chunks` + a ONE-header checksum trailer as a real SDK checksum-trailer PUT
    /// would: data chunks (signed per `ctx.signed`), the terminating zero-length chunk
    /// (`0[;chunk-signature=…]\r\n`) directly followed by `<trailer_name>:<trailer_value>\r\n`
    /// and, for a signed `ctx`, a computed `x-amz-trailer-signature:…\r\n`, closed by the
    /// blank-line terminator.
    fn frame_trailer_body(
        ctx: &StreamingContext,
        chunks: &[&[u8]],
        trailer_name: &str,
        trailer_value: &str,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let mut prev = ctx.seed_signature.clone();
        for data in chunks {
            if ctx.signed {
                let sig = sign_chunk(ctx, &prev, data);
                out.extend_from_slice(
                    format!("{:x};chunk-signature={sig}\r\n", data.len()).as_bytes(),
                );
                prev = sig;
            } else {
                out.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
            }
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        if ctx.signed {
            prev = sign_chunk(ctx, &prev, b"");
            out.extend_from_slice(format!("0;chunk-signature={prev}\r\n").as_bytes());
        } else {
            out.extend_from_slice(b"0\r\n");
        }
        out.extend_from_slice(format!("{trailer_name}:{trailer_value}\r\n").as_bytes());
        if ctx.signed {
            let canonical = format!("{trailer_name}:{trailer_value}\n");
            let trailer_sig = sign_trailer(ctx, &prev, &canonical);
            out.extend_from_slice(format!("x-amz-trailer-signature:{trailer_sig}\r\n").as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    #[tokio::test]
    async fn decodes_an_unsigned_trailer_body_and_validates_the_checksum() {
        let ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let mut running = checksum::RunningChecksum::new(checksum::ChecksumAlgorithm::Crc32);
        running.update(&object);
        let checksum_b64 = test_base64_encode(&running.finalize());
        let framed = frame_trailer_body(
            &ctx,
            &[&object[..1500], &object[1500..]],
            "x-amz-checksum-crc32",
            &checksum_b64,
        );
        for piece in [1usize, 97, 512, framed.len()] {
            let decoded = decode_to_vec(framed.clone(), piece, ctx.clone())
                .await
                .expect("a well-formed unsigned trailer body decodes");
            assert_eq!(decoded, object, "piece size {piece}");
        }
    }

    #[tokio::test]
    async fn decodes_a_signed_trailer_body_with_a_valid_trailer_signature() {
        let ctx = aws_example_ctx_with_trailer(
            true,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'q'; 2000];
        let mut running = checksum::RunningChecksum::new(checksum::ChecksumAlgorithm::Crc32);
        running.update(&object);
        let checksum_b64 = test_base64_encode(&running.finalize());
        let framed = frame_trailer_body(
            &ctx,
            &[&object[..1000], &object[1000..]],
            "x-amz-checksum-crc32",
            &checksum_b64,
        );
        let decoded = decode_to_vec(framed, 333, ctx)
            .await
            .expect("a well-signed trailer body with a valid trailer signature decodes");
        assert_eq!(decoded, object);
    }

    /// The signed trailer signature itself is fail-closed: tampering the checksum VALUE
    /// (part of what the trailer signature covers) without re-signing must be refused as an
    /// auth failure, not a checksum mismatch.
    #[tokio::test]
    async fn a_signed_trailer_whose_signature_does_not_verify_is_refused_fail_closed() {
        let ctx = aws_example_ctx_with_trailer(
            true,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'q'; 500];
        let mut running = checksum::RunningChecksum::new(checksum::ChecksumAlgorithm::Crc32);
        running.update(&object);
        let checksum_b64 = test_base64_encode(&running.finalize());
        let mut framed =
            frame_trailer_body(&ctx, &[&object], "x-amz-checksum-crc32", &checksum_b64);
        // Flip a byte of the trailer signature's own hex digits: it no longer verifies.
        let needle = b"x-amz-trailer-signature:";
        let sig_at = framed
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("trailer signature line")
            + needle.len();
        framed[sig_at] = if framed[sig_at] == b'0' { b'1' } else { b'0' };

        let err = decode_to_vec(framed, 128, ctx)
            .await
            .expect_err("a forged trailer signature must be refused");
        assert_eq!(
            err.downcast_ref::<StreamingError>(),
            Some(&StreamingError::ChunkSignature),
            "a trailer that was not signed by the credential holder is refused fail-closed"
        );
    }

    /// Issue #505's invariant: a declared checksum trailer that does not match what was
    /// ACTUALLY streamed is refused (never consumed-and-trusted). The unsigned variant has
    /// no trailer signature to catch this first, so it isolates the checksum-comparison
    /// path specifically.
    #[tokio::test]
    async fn a_tampered_trailer_checksum_is_refused_fail_closed() {
        let ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'z'; 500];
        // A checksum for bytes OTHER than what is actually streamed.
        let wrong_checksum_b64 = test_base64_encode(&[0u8; 4]);
        let framed = frame_trailer_body(
            &ctx,
            &[&object],
            "x-amz-checksum-crc32",
            &wrong_checksum_b64,
        );
        let err = decode_to_vec(framed, 128, ctx).await.expect_err(
            "a declared checksum that does not match the streamed body must be refused",
        );
        assert_eq!(
            err.downcast_ref::<StreamingError>(),
            Some(&StreamingError::ChecksumMismatch)
        );
    }

    /// A trailer name on the wire other than the one declared in `x-amz-trailer` is refused
    /// (400) — never silently consumed under the declared name, never silently ignored.
    #[tokio::test]
    async fn an_undeclared_trailer_name_is_refused() {
        let ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'z'; 100];
        let mut framed = format!("{:x}\r\n", object.len()).into_bytes();
        framed.extend_from_slice(&object);
        framed.extend_from_slice(b"\r\n0\r\nx-amz-checksum-crc32c:AAAAAA==\r\n\r\n");
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("an undeclared trailer name must be refused");
        assert!(matches!(
            err.downcast_ref::<StreamingError>(),
            Some(StreamingError::Framing(_))
        ));
    }

    /// A declared checksum value that is not valid base64 is refused (400), never
    /// half-parsed.
    #[tokio::test]
    async fn a_non_base64_trailer_checksum_is_refused() {
        let ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'z'; 100];
        let framed = frame_trailer_body(
            &ctx,
            &[&object],
            "x-amz-checksum-crc32",
            "not valid base64!!",
        );
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("a malformed base64 checksum must be refused");
        assert!(matches!(
            err.downcast_ref::<StreamingError>(),
            Some(StreamingError::Framing(_))
        ));
    }

    /// Bytes left over after the trailer block's closing blank line are refused (400), not
    /// silently dropped — the "garbage after the trailer block" edge (brief #505).
    #[tokio::test]
    async fn garbage_after_the_trailer_block_is_refused() {
        let ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        let object = vec![b'z'; 100];
        let mut running = checksum::RunningChecksum::new(checksum::ChecksumAlgorithm::Crc32);
        running.update(&object);
        let checksum_b64 = test_base64_encode(&running.finalize());
        let mut framed =
            frame_trailer_body(&ctx, &[&object], "x-amz-checksum-crc32", &checksum_b64);
        framed.extend_from_slice(b"GARBAGE-AFTER-THE-TRAILER-BLOCK");
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("bytes after the trailer block must be refused");
        assert!(matches!(
            err.downcast_ref::<StreamingError>(),
            Some(StreamingError::Framing(_))
        ));
    }

    /// A declared `x-amz-decoded-content-length` that does not match the actually-decoded
    /// byte count is refused (400).
    #[tokio::test]
    async fn a_decoded_content_length_mismatch_is_refused() {
        let mut ctx = aws_example_ctx_with_trailer(
            false,
            "x-amz-checksum-crc32",
            checksum::ChecksumAlgorithm::Crc32,
        );
        ctx.trailer.as_mut().unwrap().decoded_content_length = Some(9999);
        let object = vec![b'z'; 100];
        let mut running = checksum::RunningChecksum::new(checksum::ChecksumAlgorithm::Crc32);
        running.update(&object);
        let checksum_b64 = test_base64_encode(&running.finalize());
        let framed = frame_trailer_body(&ctx, &[&object], "x-amz-checksum-crc32", &checksum_b64);
        let err = decode_to_vec(framed, 64, ctx)
            .await
            .expect_err("a decoded-content-length mismatch must be refused");
        assert!(matches!(
            err.downcast_ref::<StreamingError>(),
            Some(StreamingError::Framing(_))
        ));
    }
}
