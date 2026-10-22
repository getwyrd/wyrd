//! Issue #853: the S3 client reports every response a conforming S3 server would not send as
//! what it is, and never buffers more of a response body than a byte budget the client fixes.
//!
//! The production [`S3Client`] runs against a **scripted endpoint**: a loopback TCP listener
//! that reads one whole request and answers with exact bytes, so each response below is the
//! one written here, byte for byte, with no server in between to normalise it. The request id
//! in every expectation is the `x-amz-request-id` the endpoint sent. Each case runs on GET,
//! PUT and DELETE wherever the response can occur, and every operation must classify the same
//! bytes the same way.
//!
//! * Error responses: an empty body is "no body", never a code the SDK makes up; an `<Error>`
//!   document without `<Code>`, or with an empty one, is "no code in body"; an HTML page, an
//!   `<Error>` that is unclosed, cut off, preceded or followed by anything but whitespace or
//!   comments, or nested past the client's depth limit, a `<Code>` given twice, a `<Code>` or
//!   `<Message>` that holds markup rather than text, a body whose length disagrees with its
//!   `Content-Length` (or that is chunked and also declares a length), anything the endpoint
//!   sends after the end the framing marks (a chunked body's trailer section included), a
//!   body in a transfer coding other than `chunked`, and a complete document whose chunked
//!   framing is not complete are all unreadable.
//! * A success the SDK cannot read is unreadable, with the SDK's diagnostic in the detail. A
//!   `204` that carries body framing (a `Transfer-Encoding`, or a `Content-Length` other than
//!   `0`) is refused on every operation: HTTP gives a `204` no body whatever its headers say,
//!   so whatever the endpoint sent after the head is not part of the response. So is a `304`
//!   with a `Transfer-Encoding` to a PUT or a DELETE, and a PUT or DELETE success that carries
//!   a body, which S3 never sends.
//! * The byte budget: an `<Error>` document of exactly the budget is read, field by field,
//!   with a `Content-Length` and chunked; one byte more is unreadable either way. An error
//!   body sent chunked with no `Content-Length`, an otherwise valid `<Error>` whose
//!   `<Message>` the endpoint generates until it is far larger than the budget plus every
//!   buffer between the two ends, is unreadable, and the endpoint has written at most the
//!   budget plus the slack in the table below when it sees the client close the connection.
//!   The same holds for a PUT or DELETE success body.
//! * GET success bodies are never cut by the budget: an object many times the budget, sent
//!   with a correct `Content-Length` or with complete chunked framing (with or without a
//!   trailer section), is accepted byte for byte. A close-delimited body, chunked framing cut
//!   short, a transfer coding other than exactly `chunked`, a body whose length disagrees
//!   with its declared length, and bytes after the end the framing marks (after a trailer
//!   section too) are body errors, never a shorter object.
//! * A trailer section goes no further than the client's connector. An `x-amz-checksum-*`
//!   header makes the SDK read every frame of a GET's body through its own checksum
//!   validator, whatever the status, and that validator takes every frame for data: a
//!   chunked GET object or error response whose terminal chunk carries a trailer section is
//!   still checked against the header and classified as itself, never crashed.
//!
//! Where a case pins why a response was refused, it looks for words only the client's own
//! report carries: every request id names its case, and the SDK's report quotes the
//! response's headers, request id included.
//!
//! # The budget and the slack
//!
//! [`BUDGET`] is the client's documented default (64 KiB, sized for S3 error documents). The
//! test names it by value: C4-verify's red leg compiles this file against #852's client, which
//! has no budget to name. The boundary cases pin the value exactly; the generated-body cases
//! pin that the client stops reading at it rather than after reading the whole body.
//!
//! When the client stops reading a buffered body at its budget, what the endpoint has written
//! can sit, legitimately, in these places on the way:
//!
//! | Term | Bound | Source |
//! |---|---|---|
//! | the client's own buffer | `BUDGET` | the budget |
//! | the piece that crossed it, one more in hyper's body channel | `2 × FILL_CHUNK` | hyper hands a chunked body over at most one chunk's data per piece, and the generator's chunks are `FILL_CHUNK` long |
//! | hyper's read buffer, twice | `2 × HYPER_READ_BUF` | at most `8192 + 4096 × 100` bytes (`hyper` 1.10.1 `proto/h1/io.rs:23`), read once more when hyper drains a dropped body before it closes (`proto/h1/conn.rs:849-865`) |
//! | the client socket's receive queue | `tcp_rmem[2] + LOOPBACK_SKB` | the client's socket autotunes its receive buffer up to `net.ipv4.tcp_rmem[2]` (read, never set) |
//! | the endpoint socket's send queue | `2 × ENDPOINT_SNDBUF + LOOPBACK_SKB` | set on the endpoint's socket; the kernel doubles `SO_SNDBUF`, and a queue overshoots by at most one loopback segment |
//!
//! The generated body is [`GENERATED_FACTOR`] times that sum, so a client that reads it whole
//! writes past the bound by the whole sum again.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;
use wyrd_validate::{
    resolve_config, BodyError, Deadlines, ErrorCode, PutSource, ResolvedConfig, S3Client, S3Error,
};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "validate";
const KEY: &str = "object";

/// The client's buffered-body budget: `wyrd_validate::s3::BUFFERED_BODY_BUDGET`, by value
/// (see the module docs).
const BUDGET: u64 = 64 * 1024;
/// The data length of each chunk the endpoint generates.
const FILL_CHUNK: usize = 16 * 1024;
/// hyper's default read-buffer ceiling (`hyper` 1.10.1 `proto/h1/io.rs:23`).
const HYPER_READ_BUF: u64 = 8192 + 4096 * 100;
/// The largest loopback segment: what a socket queue can overshoot its limit by.
const LOOPBACK_SKB: u64 = 64 * 1024;
/// `SO_SNDBUF` on the endpoint's socket.
const ENDPOINT_SNDBUF: u32 = 16 * 1024;
/// How many times the slack bound a generated body is.
const GENERATED_FACTOR: u64 = 2;

/// The bytes a PUT sends.
const PUT_BODY: &[u8] = b"sixteen bytes!!!";

/// The client's report of a buffered body that ran past the budget.
const OVER_BUDGET: &str = "-byte budget for a body it buffers";
/// The client's report of a PUT or DELETE success that carried a body.
const BODY_ON_SUCCESS: &str = "which an S3 PUT or DELETE success never has";
/// The client's report of bytes after the end of a response its framing marks.
const AFTER_THE_END: &str = "after the end of the response";
/// The client's report of a GET object followed by bytes after its end.
const OBJECT_OVERRUN: &str = "the body ended where its framing says";
/// The SDK's report of a GET body that does not match the response's `x-amz-checksum-*`
/// header, kept in the client's body error.
const CHECKSUM_MISMATCH: &str = "body checksum mismatch";
/// The client's report of an error document nested past its depth limit.
const TOO_DEEP: &str = "nests elements more than";
/// The client's depth limit for an error document (`<Error>` itself is level one), by value
/// for the reason [`BUDGET`] is.
const DEPTH_LIMIT: usize = 16;

/// The test's bound on one call. Longer than the client's operation deadline, so a
/// production deadline fires first where one applies.
const STALL: Duration = Duration::from_secs(90);
/// How long the endpoint waits for the client to close once it has written its response, and
/// how long the test waits for the endpoint's report after the call returned.
const CLOSE_WAIT: Duration = Duration::from_secs(10);

/// The ceiling of the client socket's autotuned receive buffer: `net.ipv4.tcp_rmem[2]`.
fn client_receive_queue_cap() -> u64 {
    std::fs::read_to_string("/proc/sys/net/ipv4/tcp_rmem")
        .ok()
        .and_then(|text| text.split_whitespace().nth(2)?.parse().ok())
        // The Linux default, where `/proc` cannot be read.
        .unwrap_or(6 * 1024 * 1024)
}

/// The most body bytes the endpoint may have written when the client closes a buffered body
/// at its budget: the table in the module docs.
fn budget_bound() -> u64 {
    let client = BUDGET;
    let pieces = 2 * FILL_CHUNK as u64;
    let hyper = 2 * HYPER_READ_BUF;
    let client_socket = client_receive_queue_cap() + LOOPBACK_SKB;
    let endpoint_socket = 2 * u64::from(ENDPOINT_SNDBUF) + LOOPBACK_SKB;
    client + pieces + hyper + client_socket + endpoint_socket
}

// --- objects -------------------------------------------------------------------------------

/// The byte at `offset` of every GET object: a function of the absolute offset, so a dropped,
/// repeated or reordered piece cannot compare equal.
fn payload_byte(offset: u64) -> u8 {
    let mixed = offset.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (mixed >> 56) as u8 ^ offset as u8
}

fn object(len: usize) -> Vec<u8> {
    (0..len as u64).map(payload_byte).collect()
}

/// `data` in chunked framing (RFC 9112 §7.1), in chunks of the given sizes cycled, without the
/// terminal chunk.
fn chunks(data: &[u8], sizes: &[usize]) -> Vec<u8> {
    let mut framed = Vec::new();
    let mut rest = data;
    for &size in sizes.iter().cycle() {
        if rest.is_empty() {
            break;
        }
        let (now, later) = rest.split_at(size.min(rest.len()));
        framed.extend_from_slice(format!("{:x}\r\n", now.len()).as_bytes());
        framed.extend_from_slice(now);
        framed.extend_from_slice(b"\r\n");
        rest = later;
    }
    framed
}

/// `data` as one whole chunked body, terminal chunk and final CRLF included.
fn chunked(data: &[u8]) -> Vec<u8> {
    let mut framed = chunks(data, &[data.len().max(1)]);
    framed.extend_from_slice(b"0\r\n\r\n");
    framed
}

/// `data` as one whole chunked body whose terminal chunk carries a trailer section of two
/// field lines, its closing empty line included (RFC 9112 §7.1.2). hyper hands the section
/// over as the body's last frame, and the SDK's body reader stops at that frame without
/// polling for the body's end (`aws-smithy-types` 1.8.1 `body.rs:220-226`).
fn chunked_with_trailers(data: &[u8]) -> Vec<u8> {
    let mut framed = chunks(data, &[data.len().max(1)]);
    framed.extend_from_slice(b"0\r\nx-trailer-one: value\r\nx-trailer-two: another\r\n\r\n");
    framed
}

/// What an endpoint may send after a whole response, each named for the case it is in: junk,
/// a whole second response, and more chunks as though the body went on.
const AFTER_A_TRAILER_SECTION: [(&str, &[u8]); 3] = [
    ("junk", b"junk"),
    (
        "a-second-response",
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ),
    ("more-chunks", b"5\r\nhello\r\n0\r\n\r\n"),
];

// --- S3 error documents --------------------------------------------------------------------

const XML_DECL: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const SLOW_DOWN: &str = "Please reduce your request rate.";

/// A complete, well-formed S3 `<Error>` document.
fn error_doc(code: &str, message: &str) -> String {
    format!(
        "{XML_DECL}<Error><Code>{code}</Code><Message>{message}</Message>\
         <RequestId>4442587FB7D0A2F9</RequestId></Error>"
    )
}

/// A complete, well-formed `SlowDown` document of exactly `len` bytes, and its `<Message>`:
/// the message is [`SLOW_DOWN`] repeated to fill the length.
fn error_doc_of_len(len: usize) -> (String, String) {
    let empty = error_doc("SlowDown", "");
    let fill = len
        .checked_sub(empty.len())
        .expect("the length leaves room for a message");
    let message: String = SLOW_DOWN.chars().cycle().take(fill).collect();
    let doc = error_doc("SlowDown", &message);
    assert_eq!(
        doc.len(),
        len,
        "the document is exactly the length asked for"
    );
    (doc, message)
}

// --- the scripted endpoint -----------------------------------------------------------------

/// A response head: the status line, the `x-amz-request-id`, then `headers`, in that order.
fn head(status: &str, request_id: &str, headers: &[(&str, String)]) -> Vec<u8> {
    let mut text = format!("HTTP/1.1 {status}\r\nx-amz-request-id: {request_id}\r\n");
    for (name, value) in headers {
        let _ = write!(text, "{name}: {value}\r\n");
    }
    text.push_str("\r\n");
    text.into_bytes()
}

fn length(n: usize) -> (&'static str, String) {
    ("Content-Length", n.to_string())
}

fn te_chunked() -> (&'static str, String) {
    te("chunked")
}

fn te(coding: &str) -> (&'static str, String) {
    ("Transfer-Encoding", coding.to_string())
}

fn xml() -> (&'static str, String) {
    ("Content-Type", "application/xml".to_string())
}

/// An `x-amz-checksum-sha256` header for `body`. On a GET the SDK checks the body against it,
/// whatever the status, through a validator of its own that reads every frame of the body.
fn checksum_sha256(body: &[u8]) -> (&'static str, String) {
    ("x-amz-checksum-sha256", base64(&Sha256::digest(body)))
}

/// `bytes` in base64 (RFC 4648 §4, padded): the encoding of an `x-amz-checksum-*` header.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::new();
    for group in bytes.chunks(3) {
        let bits = group
            .iter()
            .fold(0u32, |bits, &b| (bits << 8) | u32::from(b))
            << (8 * (3 - group.len()));
        for i in 0..=group.len() {
            text.push(ALPHABET[((bits >> (18 - 6 * i)) & 63) as usize] as char);
        }
        for _ in group.len()..3 {
            text.push('=');
        }
    }
    text
}

#[test]
fn base64_matches_the_rfc_4648_vectors() {
    for (input, want) in [
        (&b""[..], ""),
        (b"f", "Zg=="),
        (b"fo", "Zm8="),
        (b"foo", "Zm9v"),
        (b"foob", "Zm9vYg=="),
        (b"fooba", "Zm9vYmE="),
        (b"foobar", "Zm9vYmFy"),
    ] {
        assert_eq!(base64(input), want, "{input:?}");
    }
}

/// What the endpoint writes once it has read one whole request.
enum Body {
    /// Exactly these bytes after the head; then the endpoint closes its side.
    Exact(Vec<u8>),
    /// A chunked body generated as it is written, never held whole: `prefix`, then `filler`
    /// bytes of `a` in `FILL_CHUNK` chunks, then `suffix` and the terminal chunk. The endpoint
    /// stops at the first write the client's close makes fail.
    Generated {
        prefix: Vec<u8>,
        filler: u64,
        suffix: Vec<u8>,
    },
}

struct Script {
    head: Vec<u8>,
    body: Body,
}

impl Script {
    fn exact(head: Vec<u8>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            head,
            body: Body::Exact(body.into()),
        }
    }
}

/// What the endpoint saw of one exchange.
#[derive(Debug)]
struct Served {
    /// The request's method.
    method: String,
    /// Response-body bytes written (for a generated body, document bytes; chunk framing is
    /// not counted).
    body_written: u64,
    /// Whether a write failed because the client had closed the connection.
    closed_by_client: bool,
}

/// One scripted exchange on an ephemeral loopback port. The serving task is owned and
/// aborted on drop.
struct Endpoint {
    addr: SocketAddr,
    task: JoinSet<std::io::Result<Served>>,
}

impl Endpoint {
    fn start(script: Script) -> Self {
        let socket = TcpSocket::new_v4().expect("endpoint socket");
        // Set before `listen`, so the accepted socket inherits it.
        socket
            .set_send_buffer_size(ENDPOINT_SNDBUF)
            .expect("endpoint SO_SNDBUF");
        socket
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("endpoint bind");
        let listener = socket.listen(8).expect("endpoint listen");
        let addr = listener.local_addr().expect("endpoint addr");
        let mut task = JoinSet::new();
        task.spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let method = read_request(&mut stream).await?;
            serve(&mut stream, method, script).await
        });
        Self { addr, task }
    }

    /// The endpoint's record once it has finished, or `None` if it is still writing
    /// `CLOSE_WAIT` after the call returned: the client neither read the response nor closed
    /// the connection.
    async fn served(&mut self) -> Option<Served> {
        // Longer than the endpoint's own wait for the client to close an exact response.
        let joined = timeout(2 * CLOSE_WAIT, self.task.join_next()).await.ok()?;
        let served = joined
            .expect("the endpoint ran")
            .expect("the endpoint task did not panic")
            .expect("the endpoint read a whole request");
        Some(served)
    }
}

/// `text`, cut to a length a failure message can carry: an over-budget body read whole would
/// otherwise put tens of MiB into one line.
fn shown(text: String) -> String {
    const SHOWN: usize = 600;
    match text.char_indices().nth(SHOWN) {
        Some((at, _)) => format!("{}… ({} bytes in all)", &text[..at], text.len()),
        None => text,
    }
}

async fn read_more(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<()> {
    if stream.read_buf(buf).await? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// The index just past the next CRLF at or after `from`, reading more as needed.
async fn line_end(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    from: usize,
) -> std::io::Result<usize> {
    loop {
        if let Some(i) = find(&buf[from..], b"\r\n") {
            return Ok(from + i);
        }
        read_more(stream, buf).await?;
    }
}

/// Read one whole request, head and body, so the response is never written over unread
/// request bytes. Returns its method.
async fn read_request(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buf = Vec::with_capacity(8192);
    let head_end = loop {
        if let Some(i) = find(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        read_more(stream, &mut buf).await?;
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let method = head.split(' ').next().unwrap_or_default().to_string();
    let header = |name: &str| {
        head.split("\r\n").skip(1).find_map(|line| {
            let (field, value) = line.split_once(':')?;
            field
                .trim()
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    };
    if let Some(len) = header("content-length") {
        let len: usize = len.parse().expect("a numeric request Content-Length");
        while buf.len() < head_end + len {
            read_more(stream, &mut buf).await?;
        }
    } else if header("transfer-encoding").is_some_and(|te| te.eq_ignore_ascii_case("chunked")) {
        let mut pos = head_end;
        loop {
            let end = line_end(stream, &mut buf, pos).await?;
            let size_text = String::from_utf8_lossy(&buf[pos..end]).into_owned();
            let size_text = size_text.split(';').next().unwrap_or_default().trim();
            let size = usize::from_str_radix(size_text, 16).expect("a hex chunk size");
            if size == 0 {
                // The trailer section: field lines up to an empty one.
                let mut line = end + 2;
                loop {
                    let end = line_end(stream, &mut buf, line).await?;
                    if end == line {
                        return Ok(method);
                    }
                    line = end + 2;
                }
            }
            let next = end + 2 + size + 2;
            while buf.len() < next {
                read_more(stream, &mut buf).await?;
            }
            pos = next;
        }
    }
    Ok(method)
}

/// A write error that means the peer closed the connection.
fn closed_by_peer(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::{BrokenPipe, ConnectionAborted, ConnectionReset};
    matches!(e.kind(), BrokenPipe | ConnectionReset | ConnectionAborted)
}

async fn serve(stream: &mut TcpStream, method: String, script: Script) -> std::io::Result<Served> {
    let mut served = Served {
        method,
        body_written: 0,
        closed_by_client: false,
    };
    match script.body {
        Body::Exact(body) => {
            let mut response = script.head;
            response.extend_from_slice(&body);
            match stream.write_all(&response).await {
                Ok(()) => served.body_written = body.len() as u64,
                Err(e) if closed_by_peer(&e) => {
                    served.closed_by_client = true;
                    return Ok(served);
                }
                Err(e) => return Err(e),
            }
            // Close this side, then wait for the client to close its own, so the client reads
            // the end of the response as an end and never as a reset.
            let _ = stream.shutdown().await;
            let mut sink = [0u8; 1024];
            let _ = timeout(CLOSE_WAIT, async {
                while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
            })
            .await;
        }
        Body::Generated {
            prefix,
            filler,
            suffix,
        } => {
            if let Err(e) = stream.write_all(&script.head).await {
                served.closed_by_client = closed_by_peer(&e);
                return if served.closed_by_client {
                    Ok(served)
                } else {
                    Err(e)
                };
            }
            let fill = vec![b'a'; FILL_CHUNK];
            let mut left = filler;
            let mut pieces = std::iter::once(prefix.as_slice())
                .chain(std::iter::from_fn(|| {
                    (left > 0).then(|| {
                        let n = left.min(FILL_CHUNK as u64) as usize;
                        left -= n as u64;
                        &fill[..n]
                    })
                }))
                .chain(std::iter::once(suffix.as_slice()));
            let outcome = loop {
                let Some(data) = pieces.next() else {
                    break stream.write_all(b"0\r\n\r\n").await;
                };
                // An empty chunk is the terminal chunk: never write one early.
                if data.is_empty() {
                    continue;
                }
                if let Err(e) = write_chunk(stream, data, &mut served.body_written).await {
                    break Err(e);
                }
            };
            match outcome {
                Ok(()) => {
                    let _ = stream.shutdown().await;
                }
                Err(e) if closed_by_peer(&e) => served.closed_by_client = true,
                Err(e) => return Err(e),
            }
        }
    }
    Ok(served)
}

/// Write `data` as one chunk, crediting `written` with each data byte the socket accepted.
async fn write_chunk(
    stream: &mut TcpStream,
    data: &[u8],
    written: &mut u64,
) -> std::io::Result<()> {
    let mut framed = format!("{:x}\r\n", data.len()).into_bytes();
    let data_at = framed.len();
    framed.extend_from_slice(data);
    framed.extend_from_slice(b"\r\n");
    let data_end = data_at + data.len();
    let mut at = 0;
    while at < framed.len() {
        let n = stream.write(&framed[at..]).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        // Only the part of this write that lies inside the data is a document byte.
        let start = at.clamp(data_at, data_end);
        let end = (at + n).clamp(data_at, data_end);
        *written += (end - start) as u64;
        at += n;
    }
    Ok(())
}

// --- the client, built the way the binary builds it ----------------------------------------

/// A resolved configuration for `endpoint`, through the production argument parser and
/// credential resolution.
fn resolved(endpoint: SocketAddr) -> ResolvedConfig {
    let endpoint = format!("http://{endpoint}");
    let pairs = [
        ("--endpoint", endpoint.as_str()),
        ("--region", REGION),
        ("--bucket", BUCKET),
        ("--scenario", "smoke"),
        ("--duration", "1m"),
        ("--workers", "1"),
        ("--seed", "853"),
        ("--out", "unused"),
        ("--run-id", "s3-client-nonconforming-responses"),
        ("--driver-placement", "loopback"),
    ];
    let args: Vec<String> = pairs
        .iter()
        .flat_map(|(flag, value)| [flag.to_string(), value.to_string()])
        .collect();
    let lookup = |name: &str| -> Option<OsString> {
        match name {
            "AWS_ACCESS_KEY_ID" => Some(ACCESS_KEY.into()),
            "AWS_SECRET_ACCESS_KEY" => Some(SECRET_KEY.into()),
            _ => None,
        }
    };
    resolve_config(&args, &lookup).expect("the configuration resolves")
}

/// The client under test: production defaults except the deadlines, which are shortened so a
/// hung case fails fast. The byte budget is the client's default; it has no option.
fn client(endpoint: SocketAddr) -> S3Client {
    let deadlines = Deadlines {
        connect: Duration::from_secs(5),
        operation: Duration::from_secs(60),
        body_idle: Duration::from_secs(10),
        body: Duration::from_secs(60),
    };
    S3Client::with_deadlines(&resolved(endpoint), deadlines)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Put,
    Delete,
}

const OPS: [Op; 3] = [Op::Get, Op::Put, Op::Delete];

impl Op {
    fn method(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }
}

/// What one call returned. For a GET, `handed` is every byte the body handed over and
/// `result` how it ended; a GET that fails before its body is an error with nothing handed.
#[derive(Debug)]
struct Got {
    handed: Vec<u8>,
    result: Result<(), S3Error>,
}

async fn call(op: Op, client: &S3Client) -> Got {
    let mut handed = Vec::new();
    let result = match op {
        Op::Get => match client.get_object(KEY).await {
            Err(e) => Err(e),
            Ok(mut body) => loop {
                match body.next_piece().await {
                    Ok(Some(piece)) => handed.extend_from_slice(&piece),
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(e),
                }
            },
        },
        Op::Put => {
            let source = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(PUT_BODY))]);
            client
                .put_object(KEY, PutSource::new(PUT_BODY.len() as u64, source))
                .await
                .map(|_| ())
        }
        Op::Delete => client.delete_object(KEY).await,
    };
    Got { handed, result }
}

/// How a case must classify.
enum Expect {
    /// An S3 error response, field by field.
    Service {
        status: u16,
        code: ErrorCode,
        message: Option<String>,
    },
    /// Unreadable, with the response's status; `detail_has`, when set, must appear in the
    /// detail.
    Unreadable {
        status: u16,
        detail_has: Option<&'static str>,
    },
    /// GET: a body error after handing over at most a prefix of `object`, never its end.
    BodyError { object: Vec<u8> },
    /// As `BodyError`, and the error's text says `says`.
    BodyErrorSaying { object: Vec<u8>, says: &'static str },
    /// A success: for a GET, the whole object, byte for byte; for a PUT or a DELETE, `Ok`
    /// (nothing is handed over, so `object` is empty).
    Accepted { object: Vec<u8> },
}

/// Every case's verdict, kept so one failing case does not hide the others.
#[derive(Default)]
struct Verdicts {
    failures: Vec<String>,
}

impl Verdicts {
    fn fail(&mut self, case: &str, op: Op, why: String) {
        let why = shown(why);
        eprintln!("RED   {case} [{}]: {why}", op.method());
        self.failures
            .push(format!("{case} [{}]: {why}", op.method()));
    }

    fn pass(&self, case: &str, op: Op) {
        eprintln!("GREEN {case} [{}]", op.method());
    }

    fn finish(self) {
        assert!(
            self.failures.is_empty(),
            "{} case(s) misclassified:\n  {}",
            self.failures.len(),
            self.failures.join("\n  ")
        );
    }
}

/// `got` against `expect`, field by field. `None` when it matches; otherwise why not.
fn mismatch(got: &Got, expect: &Expect, request_id: &str) -> Option<String> {
    let rid = Some(request_id.to_string());
    match expect {
        Expect::Service {
            status,
            code,
            message,
        } => {
            let want = S3Error::Service {
                status: *status,
                code: code.clone(),
                message: message.clone(),
                request_id: rid,
            };
            (got.result.as_ref().err() != Some(&want))
                .then(|| format!("expected {want:?}, got {:?}", got.result))
        }
        Expect::Unreadable { status, detail_has } => match &got.result {
            Err(S3Error::Unreadable {
                status: s,
                request_id,
                detail,
            }) if s == status && *request_id == rid => match detail_has {
                Some(needle) if !detail.contains(needle) => Some(format!(
                    "unreadable as expected, but the detail lacks {needle:?}: {detail:?}"
                )),
                _ if detail.is_empty() => Some("unreadable with an empty detail".to_string()),
                _ => None,
            },
            other => Some(format!(
                "expected Unreadable {{ status: {status}, request_id: {rid:?} }}, got {other:?}"
            )),
        },
        Expect::BodyError { object } => {
            let prefix =
                got.handed.len() <= object.len() && got.handed[..] == object[..got.handed.len()];
            match &got.result {
                Err(S3Error::Body(BodyError::Transport { received, .. }))
                    if *received != got.handed.len() as u64 =>
                {
                    Some(format!(
                        "the body error counts {received} bytes but {} were handed over",
                        got.handed.len()
                    ))
                }
                Err(S3Error::Body(_)) if prefix => None,
                Err(S3Error::Body(_)) => Some(format!(
                    "a body error, but the {} bytes handed over are not a prefix of the object",
                    got.handed.len()
                )),
                Ok(()) => Some(format!(
                    "accepted as a {}-byte object of {}",
                    got.handed.len(),
                    object.len()
                )),
                Err(other) => Some(format!("expected a body error, got {other:?}")),
            }
        }
        Expect::BodyErrorSaying { object, says } => {
            let body_error = Expect::BodyError {
                object: object.clone(),
            };
            mismatch(got, &body_error, request_id).or_else(|| match &got.result {
                Err(e) if !e.to_string().contains(says) => {
                    Some(format!("a body error, but it does not say {says:?}: {e}"))
                }
                _ => None,
            })
        }
        Expect::Accepted { object } => match &got.result {
            Ok(()) if got.handed == *object => None,
            Ok(()) => Some(format!(
                "accepted, but {} bytes differ from the {}-byte object",
                got.handed.len(),
                object.len()
            )),
            Err(e) => Some(format!("expected success, got {e:?}")),
        },
    }
}

/// Run `op` against an endpoint serving `script` and record its verdict. Returns what the
/// endpoint saw, or `None` if it never finished (recorded as a failure).
async fn run(
    verdicts: &mut Verdicts,
    case: &str,
    op: Op,
    script: Script,
    request_id: &str,
    expect: &Expect,
) -> Option<Served> {
    let mut endpoint = Endpoint::start(script);
    let client = client(endpoint.addr);
    let got = timeout(STALL, call(op, &client))
        .await
        .unwrap_or_else(|_| panic!("{case} [{}]: no outcome within {STALL:?}", op.method()));
    // The client and every connection it holds go before the endpoint's record is read.
    drop(client);
    let served = endpoint.served().await;
    if let Some(served) = &served {
        assert_eq!(
            served.method,
            op.method(),
            "{case}: the endpoint served the operation under test"
        );
    }
    match mismatch(&got, expect, request_id) {
        Some(why) => verdicts.fail(case, op, why),
        None if served.is_none() => verdicts.fail(
            case,
            op,
            format!(
                "the endpoint was still writing {CLOSE_WAIT:?} after the call returned: the \
                 client neither read the response nor closed the connection"
            ),
        ),
        None => verdicts.pass(case, op),
    }
    served
}

/// Run one error response on all three operations: the same bytes, the same classification.
async fn on_every_operation(
    verdicts: &mut Verdicts,
    case: &str,
    script: impl Fn(&str) -> Script,
    expect: &Expect,
) {
    for op in OPS {
        let request_id = format!("853-{case}-{}", op.method());
        run(verdicts, case, op, script(&request_id), &request_id, expect).await;
    }
}

// --- error responses -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bodiless_error_response_is_no_body_never_a_code_the_body_did_not_carry() {
    let mut verdicts = Verdicts::default();
    on_every_operation(
        &mut verdicts,
        "404-content-length-0",
        |rid| Script::exact(head("404 Not Found", rid, &[length(0)]), ""),
        &Expect::Service {
            status: 404,
            code: ErrorCode::NoBody,
            message: None,
        },
    )
    .await;
    on_every_operation(
        &mut verdicts,
        "503-chunked-empty",
        |rid| {
            Script::exact(
                head("503 Service Unavailable", rid, &[te_chunked()]),
                "0\r\n\r\n",
            )
        },
        &Expect::Service {
            status: 503,
            code: ErrorCode::NoBody,
            message: None,
        },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_well_formed_error_document_without_a_code_is_no_code_in_body() {
    let mut verdicts = Verdicts::default();
    let doc = format!(
        "{XML_DECL}<Error><Message>the server named no code</Message>\
         <RequestId>4442587FB7D0A2F9</RequestId></Error>"
    );
    on_every_operation(
        &mut verdicts,
        "error-without-code",
        |rid| {
            Script::exact(
                head("400 Bad Request", rid, &[length(doc.len()), xml()]),
                doc.clone(),
            )
        },
        &Expect::Service {
            status: 400,
            code: ErrorCode::MissingInXml,
            message: Some("the server named no code".to_string()),
        },
    )
    .await;
    // An empty `<Code>` names no code either: never a code that is no code.
    for (case, code) in [
        ("empty-code-element", "<Code/>"),
        ("empty-code", "<Code></Code>"),
        ("whitespace-code", "<Code> \n\t </Code>"),
    ] {
        let doc = format!(
            "{XML_DECL}<Error>{code}<Message>the server named no code</Message>\
             <RequestId>4442587FB7D0A2F9</RequestId></Error>"
        );
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head("400 Bad Request", rid, &[length(doc.len()), xml()]),
                    doc.clone(),
                )
            },
            &Expect::Service {
                status: 400,
                code: ErrorCode::MissingInXml,
                message: Some("the server named no code".to_string()),
            },
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_xml_error_body_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let pages = [
        (
            "html-500",
            "<html><head><title>500 Internal Server Error</title></head>\
             <body><h1>Internal Server Error</h1></body></html>"
                .to_string(),
        ),
        (
            "html-doctype-500",
            "<!DOCTYPE html>\n<html><body><p>Something went wrong.<br></p></body></html>\n"
                .to_string(),
        ),
        ("plain-text-500", "Internal Server Error\n".to_string()),
    ];
    for (case, page) in &pages {
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                let content_type = ("Content-Type", "text/html".to_string());
                Script::exact(
                    head(
                        "500 Internal Server Error",
                        rid,
                        &[length(page.len()), content_type],
                    ),
                    page.clone(),
                )
            },
            &Expect::Unreadable {
                status: 500,
                detail_has: None,
            },
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unclosed_cut_or_trailed_error_document_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let whole = error_doc("SlowDown", SLOW_DOWN);
    let bodies = [
        (
            "unclosed",
            format!("{XML_DECL}<Error><Code>SlowDown</Code>"),
        ),
        (
            "cut-mid-message",
            format!("{XML_DECL}<Error><Code>SlowDown</Code><Message>Please reduce your req"),
        ),
        ("junk-after-error", format!("{whole}junk")),
        ("element-after-error", format!("{whole}<Error/>")),
        // Well-formed XML (a processing instruction is allowed after the root element), but no
        // part of an S3 error document.
        (
            "pi-after-error",
            format!("{whole}\n<?proxy served-by=\"x\"?>"),
        ),
        // The same before the root, after the XML declaration (which is no processing
        // instruction).
        (
            "pi-before-error",
            whole.replacen("<Error>", "<?proxy served-by=\"x\"?>\n<Error>", 1),
        ),
    ];
    for (case, body) in &bodies {
        // Each body is correctly sized: its `Content-Length` is its length.
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[length(body.len()), xml()]),
                    body.clone(),
                )
            },
            &Expect::Unreadable {
                status: 503,
                detail_has: None,
            },
        )
        .await;
    }
    // A guard on the strictness: whitespace and comments around the document are allowed.
    let trailed = format!("{whole}\n<!-- served by a proxy -->\n").replacen(
        "<Error>",
        "<!-- from a proxy -->\n<Error>",
        1,
    );
    on_every_operation(
        &mut verdicts,
        "comments-around-error",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[length(trailed.len()), xml()]),
                trailed.clone(),
            )
        },
        &Expect::Service {
            status: 503,
            code: ErrorCode::Code("SlowDown".to_string()),
            message: Some(SLOW_DOWN.to_string()),
        },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_code_or_message_that_is_not_one_text_field_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let rest = "<RequestId>4442587FB7D0A2F9</RequestId></Error>";
    let bodies = [
        // Two codes: nothing says which one the server meant.
        (
            "two-codes",
            format!(
                "{XML_DECL}<Error><Code>SlowDown</Code><Code>InternalError</Code>\
                 <Message>{SLOW_DOWN}</Message>{rest}"
            ),
        ),
        (
            "element-in-code",
            format!(
                "{XML_DECL}<Error><Code>Slow<Unexpected/>Down</Code>\
                 <Message>{SLOW_DOWN}</Message>{rest}"
            ),
        ),
        (
            "element-in-message",
            format!(
                "{XML_DECL}<Error><Code>SlowDown</Code>\
                 <Message>Please <b>reduce</b> your request rate.</Message>{rest}"
            ),
        ),
        (
            "comment-in-code",
            format!(
                "{XML_DECL}<Error><Code>Slow<!-- x -->Down</Code>\
                 <Message>{SLOW_DOWN}</Message>{rest}"
            ),
        ),
    ];
    for (case, body) in &bodies {
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[length(body.len()), xml()]),
                    body.clone(),
                )
            },
            &Expect::Unreadable {
                status: 503,
                detail_has: None,
            },
        )
        .await;
    }
    // A guard: character references and CDATA are text, decoded as XML decodes them.
    let escaped = format!(
        "{XML_DECL}<Error><Code>SlowDown</Code>\
         <Message>Rate &amp; size <![CDATA[<limits>]]> hit</Message>{rest}"
    );
    on_every_operation(
        &mut verdicts,
        "escaped-text",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[length(escaped.len()), xml()]),
                escaped.clone(),
            )
        },
        &Expect::Service {
            status: 503,
            code: ErrorCode::Code("SlowDown".to_string()),
            message: Some("Rate & size <limits> hit".to_string()),
        },
    )
    .await;
    verdicts.finish();
}

/// A whole `SlowDown` document with `extra` empty-text elements nested inside each other after
/// its `<Message>`: elements `1 + extra` deep, counting `<Error>` as one.
fn nested_error_doc(extra: usize) -> String {
    nested_error_doc_with(extra, "<a>", "")
}

/// As [`nested_error_doc`], each nested element opened with `open` and the deepest holding
/// `inner`.
fn nested_error_doc_with(extra: usize, open: &str, inner: &str) -> String {
    format!(
        "{XML_DECL}<Error><Code>SlowDown</Code><Message>{SLOW_DOWN}</Message>{}{inner}{}</Error>",
        open.repeat(extra),
        "</a>".repeat(extra)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_document_nested_past_the_depth_limit_is_unreadable() {
    let mut verdicts = Verdicts::default();
    // Well-formed XML well inside the budget, nested a few thousand deep: a parser that
    // recurses per level with no limit of its own aborts the whole process here, which no
    // classification survives. 4000 levels is 28 KB.
    let deep = nested_error_doc(4000);
    assert!(
        (deep.len() as u64) < BUDGET,
        "the deep document fits the budget"
    );
    // The limit exactly, and one level past it.
    let at_limit = nested_error_doc(DEPTH_LIMIT - 1);
    let past_limit = nested_error_doc(DEPTH_LIMIT);
    // Empty elements at the deepest level allowed: each is an element there, and opens no
    // level for the next.
    let at_limit_empty = nested_error_doc_with(DEPTH_LIMIT - 2, "<a>", "<Empty/><Empty x=\"1\"/>");
    // An empty element one level past the limit is past it all the same.
    let past_limit_empty = nested_error_doc_with(DEPTH_LIMIT - 1, "<a>", "<Empty/>");
    // Markup that holds end tags without closing anything, halfway down (each ending in `</`
    // just before its terminator, so a scan that stopped short of the terminator would read
    // an end tag), and attribute values that hold `/>` without ending their tag: still one
    // level past the limit.
    let half = DEPTH_LIMIT / 2;
    let past_limit_dressed = format!(
        "{XML_DECL}<Error><Code>SlowDown</Code><Message>{SLOW_DOWN}</Message>{}\
         <!-- </a></a></--><![CDATA[</a></a></]]><?note </a></a></?>{}{}</Error>",
        "<a x=\"/>\" y='/>'>".repeat(half),
        "<a x=\"/>\" y='/>'>".repeat(DEPTH_LIMIT - half),
        "</a>".repeat(DEPTH_LIMIT)
    );
    let too_deep = Expect::Unreadable {
        status: 503,
        detail_has: Some(TOO_DEEP),
    };
    let read = Expect::Service {
        status: 503,
        code: ErrorCode::Code("SlowDown".to_string()),
        message: Some(SLOW_DOWN.to_string()),
    };
    for (case, doc, expect) in [
        ("nested-4000-deep", &deep, &too_deep),
        ("nested-at-the-limit", &at_limit, &read),
        ("nested-past-the-limit", &past_limit, &too_deep),
        ("nested-at-the-limit-empty", &at_limit_empty, &read),
        ("nested-past-the-limit-empty", &past_limit_empty, &too_deep),
        (
            "nested-past-the-limit-dressed",
            &past_limit_dressed,
            &too_deep,
        ),
    ] {
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[length(doc.len()), xml()]),
                    doc.clone(),
                )
            },
            expect,
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_body_whose_length_disagrees_with_its_declaration_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let doc = error_doc("SlowDown", SLOW_DOWN);
    let unreadable = Expect::Unreadable {
        status: 503,
        detail_has: None,
    };
    // Declares more than it sends, then closes.
    on_every_operation(
        &mut verdicts,
        "length-over-sent",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[length(doc.len() + 40), xml()]),
                doc.clone(),
            )
        },
        &unreadable,
    )
    .await;
    // Declares less than the document: the declared length ends inside `<Message>`, and the
    // rest of the document follows past it. Unreadable for the length, not for the cut XML.
    let cut_at = doc.find("reduce").expect("the message is in the document");
    let bytes_after_the_end = Expect::Unreadable {
        status: 503,
        detail_has: Some(AFTER_THE_END),
    };
    on_every_operation(
        &mut verdicts,
        "length-under-sent",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[length(cut_at), xml()]),
                doc.clone(),
            )
        },
        &bytes_after_the_end,
    )
    .await;
    // Declares exactly one whole document, then sends more after it: the declared body alone
    // is a clean `SlowDown`, so only the bytes past it tell.
    let second = error_doc("InternalError", "We encountered an internal error.");
    for (case, after) in [
        ("whole-document-then-junk", "junk".to_string()),
        ("whole-document-then-a-second", second.clone()),
    ] {
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[length(doc.len()), xml()]),
                    format!("{doc}{after}"),
                )
            },
            &bytes_after_the_end,
        )
        .await;
    }
    // The same after a whole chunked body, terminal chunk and final CRLF included.
    on_every_operation(
        &mut verdicts,
        "chunked-document-then-junk",
        |rid| {
            let mut body = chunked(doc.as_bytes());
            body.extend_from_slice(b"junk");
            Script::exact(head("503 Slow Down", rid, &[te_chunked(), xml()]), body)
        },
        &bytes_after_the_end,
    )
    .await;
    // Chunked and also a `Content-Length`: agreeing, shorter, and longer than the chunked
    // data.
    for (case, declared) in [
        ("chunked-and-length-agreeing", doc.len()),
        ("chunked-and-length-over", doc.len() + 40),
        ("chunked-and-length-under", doc.len() - 40),
    ] {
        on_every_operation(
            &mut verdicts,
            case,
            |rid| {
                Script::exact(
                    head(
                        "503 Slow Down",
                        rid,
                        &[te_chunked(), length(declared), xml()],
                    ),
                    chunked(doc.as_bytes()),
                )
            },
            &unreadable,
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_body_with_bytes_after_its_trailer_section_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let doc = error_doc("SlowDown", SLOW_DOWN);
    // A whole chunked document whose terminal chunk carries a trailer section, then more:
    // the body hyper reads is a clean `SlowDown`, so only the bytes past it tell.
    for (after, sent) in AFTER_A_TRAILER_SECTION {
        let mut body = chunked_with_trailers(doc.as_bytes());
        body.extend_from_slice(sent);
        on_every_operation(
            &mut verdicts,
            &format!("chunked-trailer-then-{after}"),
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[te_chunked(), xml()]),
                    body.clone(),
                )
            },
            &Expect::Unreadable {
                status: 503,
                detail_has: Some(AFTER_THE_END),
            },
        )
        .await;
    }
    // Guard: the same trailer section with nothing after it is the `SlowDown` it says.
    on_every_operation(
        &mut verdicts,
        "chunked-trailer",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[te_chunked(), xml()]),
                chunked_with_trailers(doc.as_bytes()),
            )
        },
        &Expect::Service {
            status: 503,
            code: ErrorCode::Code("SlowDown".to_string()),
            message: Some(SLOW_DOWN.to_string()),
        },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_body_with_a_trailer_section_and_a_checksum_header_is_the_error_it_says() {
    let mut verdicts = Verdicts::default();
    let doc = error_doc("SlowDown", SLOW_DOWN);
    // The SDK's checksum validator wraps a GET's body whatever the status, so a `503` to a
    // GET is read through it; a PUT or DELETE has no such validator. It takes every frame of
    // the body for data, so a trailer section handed over as a frame would crash it instead
    // of letting the body end (`aws-smithy-checksums` 0.65.0 `body/validate.rs:57`). The
    // same bytes, with the document's own checksum: the same `SlowDown` on all three.
    on_every_operation(
        &mut verdicts,
        "chunked-trailer-checksum",
        |rid| {
            Script::exact(
                head(
                    "503 Slow Down",
                    rid,
                    &[te_chunked(), xml(), checksum_sha256(doc.as_bytes())],
                ),
                chunked_with_trailers(doc.as_bytes()),
            )
        },
        &Expect::Service {
            status: 503,
            code: ErrorCode::Code("SlowDown".to_string()),
            message: Some(SLOW_DOWN.to_string()),
        },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_error_document_in_incomplete_chunked_framing_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let doc = error_doc("SlowDown", SLOW_DOWN);
    let unreadable = Expect::Unreadable {
        status: 503,
        detail_has: None,
    };
    // The whole document in chunks, then the connection closes with no terminal chunk.
    on_every_operation(
        &mut verdicts,
        "chunked-no-terminal-chunk",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[te_chunked(), xml()]),
                chunks(doc.as_bytes(), &[7, 64]),
            )
        },
        &unreadable,
    )
    .await;
    // The terminal chunk arrives, but the connection closes before its final CRLF.
    on_every_operation(
        &mut verdicts,
        "chunked-no-final-crlf",
        |rid| {
            let mut body = chunks(doc.as_bytes(), &[7, 64]);
            body.extend_from_slice(b"0\r\n");
            Script::exact(head("503 Slow Down", rid, &[te_chunked(), xml()]), body)
        },
        &unreadable,
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_body_in_a_transfer_coding_other_than_chunked_is_unreadable() {
    let mut verdicts = Verdicts::default();
    let doc = error_doc("SlowDown", SLOW_DOWN);
    // A coding the client does not decode, so the bytes are not the document; and, with
    // `chunked` not the final coding, framed by the connection's close (RFC 9112 §6.3).
    on_every_operation(
        &mut verdicts,
        "gzip-coded-error",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[te("gzip"), xml()]),
                doc.clone(),
            )
        },
        // The detail names the header as the endpoint sent it.
        &Expect::Unreadable {
            status: 503,
            detail_has: Some("Transfer-Encoding Some(\"gzip\")"),
        },
    )
    .await;
    // `chunked` followed by a non-breaking space (UTF-8 `C2 A0`): not the token `chunked`, and
    // not text HTTP trims, so hyper reads the body to the close, chunk framing and all.
    on_every_operation(
        &mut verdicts,
        "chunked-nbsp-error",
        |rid| {
            Script::exact(
                head("503 Slow Down", rid, &[te("chunked\u{a0}"), xml()]),
                chunked(doc.as_bytes()),
            )
        },
        &Expect::Unreadable {
            status: 503,
            detail_has: None,
        },
    )
    .await;
    verdicts.finish();
}

// --- a success the SDK cannot read, and a bodiless success with body framing ---------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_success_the_sdk_cannot_read_is_unreadable_with_its_diagnostic() {
    let mut verdicts = Verdicts::default();
    // Each operation's response carries a header its output model parses and this value
    // fails; the expected detail is the SDK's own diagnostic for it.
    let cases = [
        (
            Op::Get,
            "200 OK",
            ("Last-Modified", "yesterday-ish"),
            "Failed to parse LastModified from header `Last-Modified",
        ),
        (
            Op::Put,
            "200 OK",
            ("x-amz-object-size", "sixteen"),
            "Failed to parse Size from header `x-amz-object-size",
        ),
        (
            Op::Delete,
            "204 No Content",
            ("x-amz-delete-marker", "perhaps"),
            "Failed to parse DeleteMarker from header `x-amz-delete-marker",
        ),
    ];
    for (op, status_line, (name, value), diagnostic) in cases {
        let request_id = format!("853-unreadable-success-{}", op.method());
        let status = status_line[..3].parse().expect("a status code");
        let body = if op == Op::Get { "hello" } else { "" };
        let mut headers = vec![(name, value.to_string())];
        if status != 204 {
            headers.push(length(body.len()));
        }
        let script = Script::exact(head(status_line, &request_id, &headers), body);
        run(
            &mut verdicts,
            "unreadable-success",
            op,
            script,
            &request_id,
            &Expect::Unreadable {
                status,
                detail_has: Some(diagnostic),
            },
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_204_with_body_framing_is_refused() {
    let mut verdicts = Verdicts::default();
    // HTTP gives a `204` no body whatever its headers say (RFC 9112 §6.3), and forbids it
    // either framing header (RFC 9112 §6.1, RFC 9110 §8.6). A server that sends one meant a
    // body the client never reads: the bytes after the head are not part of the response.
    let sent = b"hello".to_vec();
    let framed = [
        // Chunked framing with one chunk and no terminal chunk: read as chunked, a cut body.
        ("204-chunked", te_chunked(), chunks(&sent, &[5])),
        ("204-content-length-5", length(sent.len()), sent.clone()),
        // The same headers with nothing after the head: no bytes follow the response, so only
        // the headers themselves tell.
        ("204-chunked-nothing-sent", te_chunked(), Vec::new()),
        (
            "204-content-length-5-nothing-sent",
            length(sent.len()),
            Vec::new(),
        ),
    ];
    for (case, header, body) in &framed {
        for op in OPS {
            let request_id = format!("853-{case}-{}", op.method());
            let script = Script::exact(
                head("204 No Content", &request_id, std::slice::from_ref(header)),
                body.clone(),
            );
            let expect = match op {
                Op::Get => Expect::BodyError {
                    object: sent.clone(),
                },
                Op::Put | Op::Delete => Expect::Unreadable {
                    status: 204,
                    detail_has: None,
                },
            };
            run(&mut verdicts, case, op, script, &request_id, &expect).await;
        }
    }
    // Guards. A `204` with `Content-Length: 0` is the empty response it says it is, on every
    // operation.
    on_every_operation(
        &mut verdicts,
        "204-content-length-0",
        |rid| Script::exact(head("204 No Content", rid, &[length(0)]), ""),
        &Expect::Accepted { object: Vec::new() },
    )
    .await;
    // A `204` with no framing header is a PUT's or a DELETE's success. A GET object that
    // declares no length stays a body error, as for any other success status.
    for op in OPS {
        let request_id = format!("853-204-no-framing-{}", op.method());
        let script = Script::exact(head("204 No Content", &request_id, &[]), "");
        let expect = match op {
            Op::Get => Expect::BodyError { object: Vec::new() },
            Op::Put | Op::Delete => Expect::Accepted { object: Vec::new() },
        };
        run(
            &mut verdicts,
            "204-no-framing",
            op,
            script,
            &request_id,
            &expect,
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_or_delete_success_that_carries_a_body_is_unreadable() {
    let mut verdicts = Verdicts::default();
    // S3 answers a PUT or a DELETE with no body; one that sends a body is not that success.
    let with_body = [
        ("200-content-length-4", vec![length(4)], "junk"),
        // No framing header: the body runs to the close, here a cut-off document.
        ("200-close-delimited", Vec::new(), "<Err"),
    ];
    for (case, headers, body) in &with_body {
        for op in [Op::Put, Op::Delete] {
            let request_id = format!("853-{case}-{}", op.method());
            let script = Script::exact(head("200 OK", &request_id, headers), *body);
            let expect = Expect::Unreadable {
                status: 200,
                detail_has: Some(BODY_ON_SUCCESS),
            };
            run(&mut verdicts, case, op, script, &request_id, &expect).await;
        }
    }
    // A `204` has no body by rule, so bytes after its head follow the response's end.
    for op in [Op::Put, Op::Delete] {
        let request_id = format!("853-204-then-junk-{}", op.method());
        let script = Script::exact(head("204 No Content", &request_id, &[]), "junk");
        let expect = Expect::Unreadable {
            status: 204,
            detail_has: Some(AFTER_THE_END),
        };
        run(
            &mut verdicts,
            "204-then-junk",
            op,
            script,
            &request_id,
            &expect,
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_or_delete_success_with_bytes_after_its_trailer_section_is_unreadable() {
    let mut verdicts = Verdicts::default();
    // An empty chunked body, the terminal chunk alone, carrying a trailer section: no body
    // byte, so only what follows the section can tell.
    for (after, sent) in AFTER_A_TRAILER_SECTION {
        let case = format!("200-chunked-trailer-then-{after}");
        for op in [Op::Put, Op::Delete] {
            let request_id = format!("853-{case}-{}", op.method());
            let mut body = chunked_with_trailers(b"");
            body.extend_from_slice(sent);
            let script = Script::exact(head("200 OK", &request_id, &[te_chunked()]), body);
            let expect = Expect::Unreadable {
                status: 200,
                detail_has: Some(AFTER_THE_END),
            };
            run(&mut verdicts, &case, op, script, &request_id, &expect).await;
        }
    }
    // Guard: the same response with nothing after it carries no body, and is the success.
    for op in [Op::Put, Op::Delete] {
        let request_id = format!("853-200-chunked-trailer-{}", op.method());
        let script = Script::exact(
            head("200 OK", &request_id, &[te_chunked()]),
            chunked_with_trailers(b""),
        );
        run(
            &mut verdicts,
            "200-chunked-trailer",
            op,
            script,
            &request_id,
            &Expect::Accepted { object: Vec::new() },
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_304_with_a_transfer_encoding_to_a_put_or_delete_is_unreadable() {
    let mut verdicts = Verdicts::default();
    // RFC 9112 §6.1: a `Transfer-Encoding` on a `304` only in answer to a GET. A `304` has no
    // body either way, so nothing follows the head here.
    for op in OPS {
        let request_id = format!("853-304-transfer-encoding-{}", op.method());
        let script = Script::exact(head("304 Not Modified", &request_id, &[te_chunked()]), "");
        let expect = match op {
            Op::Get => Expect::Service {
                status: 304,
                code: ErrorCode::NoBody,
                message: None,
            },
            Op::Put | Op::Delete => Expect::Unreadable {
                status: 304,
                detail_has: Some("Transfer-Encoding Some(\"chunked\")"),
            },
        };
        run(
            &mut verdicts,
            "304-transfer-encoding",
            op,
            script,
            &request_id,
            &expect,
        )
        .await;
    }
    // A guard: a `304` may carry a `Content-Length` (RFC 9110 §8.6), which a PUT or a DELETE
    // takes as the empty response it is. A GET does not: the SDK checks every GET response's
    // body against its `Content-Length`, a bodiless status's included (`aws-smithy-runtime`
    // 1.16.0 `client/http/body/content_length_enforcement.rs:131-173`), so it reports this one
    // unreadable. The client sends no conditional GET, so a `304` is never its answer either
    // way; the case records the difference rather than hiding it.
    for op in OPS {
        let request_id = format!("853-304-content-length-{}", op.method());
        let script = Script::exact(head("304 Not Modified", &request_id, &[length(100)]), "");
        let expect = match op {
            Op::Get => Expect::Unreadable {
                status: 304,
                detail_has: Some("Expected 100 bytes but 0 bytes were received"),
            },
            Op::Put | Op::Delete => Expect::Service {
                status: 304,
                code: ErrorCode::NoBody,
                message: None,
            },
        };
        run(
            &mut verdicts,
            "304-content-length",
            op,
            script,
            &request_id,
            &expect,
        )
        .await;
    }
    verdicts.finish();
}

// --- the byte budget -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_document_at_the_budget_is_read_and_one_byte_more_is_unreadable() {
    let mut verdicts = Verdicts::default();
    for (size, name) in [
        (BUDGET - 1, "budget-less-1"),
        (BUDGET, "budget"),
        (BUDGET + 1, "budget-plus-1"),
    ] {
        let (doc, message) = error_doc_of_len(size as usize);
        let expect = if size <= BUDGET {
            Expect::Service {
                status: 503,
                code: ErrorCode::Code("SlowDown".to_string()),
                message: Some(message),
            }
        } else {
            // The detail says why: the budget, whether the length was declared or counted.
            Expect::Unreadable {
                status: 503,
                detail_has: Some(OVER_BUDGET),
            }
        };
        // Declared up front.
        on_every_operation(
            &mut verdicts,
            &format!("{name}-content-length"),
            |rid| {
                Script::exact(
                    head("503 Slow Down", rid, &[length(doc.len()), xml()]),
                    doc.clone(),
                )
            },
            &expect,
        )
        .await;
        // Chunked, no `Content-Length`, in uneven chunks: counted as it arrives.
        on_every_operation(
            &mut verdicts,
            &format!("{name}-chunked"),
            |rid| {
                let mut body = chunks(doc.as_bytes(), &[4093, 1, 8192]);
                body.extend_from_slice(b"0\r\n\r\n");
                Script::exact(head("503 Slow Down", rid, &[te_chunked(), xml()]), body)
            },
            &expect,
        )
        .await;
    }
    verdicts.finish();
}

/// Run `op` against a generated chunked body with no `Content-Length` and check both oracles:
/// the classification, and the bytes the endpoint wrote before the client closed.
async fn over_budget(verdicts: &mut Verdicts, case: &str, op: Op, status_line: &str) {
    let bound = budget_bound();
    let filler = GENERATED_FACTOR * bound;
    let request_id = format!("853-{case}-{}", op.method());
    let status = status_line[..3].parse().expect("a status code");
    let (prefix, suffix) = if status >= 300 {
        // An otherwise valid `<Error>` whose `<Message>` is the generated filler.
        (
            format!("{XML_DECL}<Error><Code>SlowDown</Code><Message>"),
            "</Message><RequestId>4442587FB7D0A2F9</RequestId></Error>".to_string(),
        )
    } else {
        // A success body: the filler alone. A PUT or DELETE output has no body member, so no
        // content would be read as anything; only its size matters here.
        (String::new(), String::new())
    };
    // An error body is refused at the budget; a PUT or DELETE success has no body to allow,
    // so it is refused at its first byte, well inside the same bound.
    let why = if status >= 300 {
        OVER_BUDGET
    } else {
        BODY_ON_SUCCESS
    };
    let script = Script {
        head: head(status_line, &request_id, &[te_chunked(), xml()]),
        body: Body::Generated {
            prefix: prefix.into_bytes(),
            filler,
            suffix: suffix.into_bytes(),
        },
    };
    let Some(served) = run(
        verdicts,
        case,
        op,
        script,
        &request_id,
        &Expect::Unreadable {
            status,
            detail_has: Some(why),
        },
    )
    .await
    else {
        // Already recorded: the client held the connection open without reading.
        return;
    };
    let written = served.body_written;
    if !served.closed_by_client {
        verdicts.fail(
            case,
            op,
            format!(
                "the endpoint wrote the whole {written}-byte body: the client read it to the end \
                 instead of closing at its {BUDGET}-byte budget"
            ),
        );
    } else if written > bound {
        verdicts.fail(
            case,
            op,
            format!(
                "the endpoint wrote {written} body bytes before the client closed, past the \
                 bound of {bound} (the {BUDGET}-byte budget plus the slack in the module docs)"
            ),
        );
    } else {
        eprintln!(
            "      {case} [{}]: {written} body bytes written before the close, bound {bound}",
            op.method()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_body_with_no_declared_length_is_cut_at_the_budget() {
    let mut verdicts = Verdicts::default();
    for op in OPS {
        over_budget(&mut verdicts, "error-over-budget", op, "503 Slow Down").await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_or_delete_success_body_is_cut_at_the_budget() {
    let mut verdicts = Verdicts::default();
    for op in [Op::Put, Op::Delete] {
        over_budget(&mut verdicts, "success-over-budget", op, "200 OK").await;
    }
    verdicts.finish();
}

// --- GET success bodies --------------------------------------------------------------------

/// A GET answered `200` with `headers` and exactly `body`.
async fn get_200(
    verdicts: &mut Verdicts,
    case: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
    expect: &Expect,
) {
    let request_id = format!("853-{case}-GET");
    let script = Script::exact(head("200 OK", &request_id, headers), body);
    run(verdicts, case, Op::Get, script, &request_id, expect).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_get_object_larger_than_the_budget_is_accepted_whole() {
    let mut verdicts = Verdicts::default();
    // Sixteen times the budget and a ragged tail: far past any cap applied to every body.
    let object = object(16 * BUDGET as usize + 7);
    get_200(
        &mut verdicts,
        "length-framed-16x-budget",
        &[length(object.len())],
        object.clone(),
        &Expect::Accepted { object },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_get_with_its_terminal_chunk_is_accepted_whole() {
    let mut verdicts = Verdicts::default();
    // Past the budget too, in uneven chunks, so neither the framing nor the size is refused.
    let object = object(4 * BUDGET as usize + 13);
    let mut body = chunks(&object, &[1, 4096, 777, 16 * 1024]);
    body.extend_from_slice(b"0\r\n\r\n");
    get_200(
        &mut verdicts,
        "chunked-4x-budget",
        &[te_chunked()],
        body,
        &Expect::Accepted { object },
    )
    .await;
    // An empty object, chunked: the terminal chunk alone.
    get_200(
        &mut verdicts,
        "chunked-empty",
        &[te_chunked()],
        b"0\r\n\r\n".to_vec(),
        &Expect::Accepted { object: Vec::new() },
    )
    .await;
    // A terminal chunk that carries a trailer section, with nothing after it: complete
    // framing, so the object is accepted byte for byte.
    let trailed = self::object(3000);
    get_200(
        &mut verdicts,
        "chunked-trailer",
        &[te_chunked()],
        chunked_with_trailers(&trailed),
        &Expect::Accepted { object: trailed },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_get_with_a_trailer_section_is_checked_against_its_checksum_header() {
    let mut verdicts = Verdicts::default();
    // An `x-amz-checksum-*` header makes the SDK wrap the body in its own checksum validator,
    // which reads every frame of the body and takes each for data: a trailer section handed
    // over as a frame would crash it instead of letting the body end
    // (`aws-smithy-checksums` 0.65.0 `body/validate.rs:57`). With the object's own checksum
    // the object is accepted byte for byte, the section dropped on the way.
    let object = object(3000);
    get_200(
        &mut verdicts,
        "chunked-trailer-checksum",
        &[te_chunked(), checksum_sha256(&object)],
        chunked_with_trailers(&object),
        &Expect::Accepted {
            object: object.clone(),
        },
    )
    .await;
    // With another object's checksum the validator reads to the body's end and reports the
    // mismatch: the check is real, and the object is never accepted on a header it fails.
    get_200(
        &mut verdicts,
        "chunked-trailer-wrong-checksum",
        &[te_chunked(), checksum_sha256(&self::object(3001))],
        chunked_with_trailers(&object),
        &Expect::BodyErrorSaying {
            object,
            says: CHECKSUM_MISMATCH,
        },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_delimited_get_is_a_body_error() {
    let mut verdicts = Verdicts::default();
    let object = object(3000);
    get_200(
        &mut verdicts,
        "close-delimited",
        &[],
        object.clone(),
        &Expect::BodyError { object },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_get_cut_short_is_a_body_error_never_a_shorter_object() {
    let mut verdicts = Verdicts::default();
    let object = object(3 * 4096 + 5);
    // Every chunk arrives whole, then the connection closes with no terminal chunk.
    get_200(
        &mut verdicts,
        "chunked-no-terminal-chunk",
        &[te_chunked()],
        chunks(&object, &[4096]),
        &Expect::BodyError {
            object: object.clone(),
        },
    )
    .await;
    // The terminal chunk arrives, but not its final CRLF.
    let mut body = chunks(&object, &[4096]);
    body.extend_from_slice(b"0\r\n");
    get_200(
        &mut verdicts,
        "chunked-no-final-crlf",
        &[te_chunked()],
        body,
        &Expect::BodyError {
            object: object.clone(),
        },
    )
    .await;
    // The connection closes inside a chunk's data.
    let mut body = chunks(&object, &[4096]);
    body.truncate(body.len() - 100);
    get_200(
        &mut verdicts,
        "chunked-cut-in-data",
        &[te_chunked()],
        body,
        &Expect::BodyError { object },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_get_in_a_transfer_coding_other_than_chunked_is_a_body_error() {
    let mut verdicts = Verdicts::default();
    let object = object(3000);
    // `chunked` followed by a non-breaking space: hyper does not read it as chunked, so the
    // body runs to the close. One chunk header announcing all 3000 bytes, then 1000 of them,
    // then the close: never an object.
    let mut cut = format!("{:x}\r\n", object.len()).into_bytes();
    cut.extend_from_slice(&object[..1000]);
    get_200(
        &mut verdicts,
        "chunked-nbsp-cut",
        &[te("chunked\u{a0}")],
        cut,
        &Expect::BodyError {
            object: object.clone(),
        },
    )
    .await;
    // A coding the client does not decode: the bytes are not the object.
    get_200(
        &mut verdicts,
        "gzip",
        &[te("gzip")],
        object.clone(),
        &Expect::BodyError {
            object: object.clone(),
        },
    )
    .await;
    // The same, with `chunked` the final coding: the framing is whole, the content still coded.
    get_200(
        &mut verdicts,
        "gzip-chunked",
        &[te("gzip, chunked")],
        chunked(&object),
        &Expect::BodyError { object },
    )
    .await;
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_get_whose_length_disagrees_with_its_declaration_is_a_body_error() {
    let mut verdicts = Verdicts::default();
    let object = object(3 * 4096 + 5);
    let len = object.len();
    // Declares more than it sends, then closes.
    get_200(
        &mut verdicts,
        "length-over-sent",
        &[length(len + 1000)],
        object.clone(),
        &Expect::BodyError {
            object: object.clone(),
        },
    )
    .await;
    // Chunked and also a `Content-Length`: shorter, longer, and agreeing with the chunked
    // data. Two framings for one body: neither can be trusted to mark its end.
    for (case, declared) in [
        ("chunked-and-length-over", len + 1000),
        ("chunked-and-length-under", len - 1000),
        ("chunked-and-length-agreeing", len),
    ] {
        get_200(
            &mut verdicts,
            case,
            &[te_chunked(), length(declared)],
            chunked(&object),
            &Expect::BodyError {
                object: object.clone(),
            },
        )
        .await;
    }
    verdicts.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_get_with_bytes_after_its_end_is_a_body_error() {
    let mut verdicts = Verdicts::default();
    let object = object(3000);
    let overrun = |object: &[u8]| Expect::BodyErrorSaying {
        object: object.to_vec(),
        says: OBJECT_OVERRUN,
    };
    // Declares 2000 bytes and sends 3000: hyper reads exactly the 2000, which alone look like
    // a whole object.
    get_200(
        &mut verdicts,
        "length-2000-sent-3000",
        &[length(2000)],
        object.clone(),
        &overrun(&object),
    )
    .await;
    // A whole chunked body, terminal chunk and final CRLF included, then more.
    let mut body = chunked(&object);
    body.extend_from_slice(b"junk");
    get_200(
        &mut verdicts,
        "chunked-then-junk",
        &[te_chunked()],
        body,
        &overrun(&object),
    )
    .await;
    // The same after a terminal chunk that carries a trailer section: the reader stops at the
    // section, so the check must have run before the section is handed over.
    for (after, sent) in AFTER_A_TRAILER_SECTION {
        let mut body = chunked_with_trailers(&object);
        body.extend_from_slice(sent);
        get_200(
            &mut verdicts,
            &format!("chunked-trailer-then-{after}"),
            &[te_chunked()],
            body,
            &overrun(&object),
        )
        .await;
    }
    verdicts.finish();
}
