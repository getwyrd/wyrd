//! A listing and a HEAD/GET report the **same instant** for one object's modification time
//! (issue #767), over the shipping S3 wire surface of the in-process gateway.
//!
//! The store keeps `modified` in epoch **milliseconds** (ADR-0047). The `Last-Modified`
//! header is an RFC-7231 IMF-fixdate, which has one-second resolution, so it drops the
//! millisecond part. Before #767 a listing's `<LastModified>` kept that part as a `.mmm`
//! fraction, so for any stored value not on a whole second (about 999 in 1000) the two
//! surfaces named different instants, and a sync tool that caches a listing's
//! `LastModified` and revalidates with HEAD saw a mismatch on every object, every pass.
//! S3's own listings are second-granular (`.000Z`) and agree with HEAD. This file pins
//! that rule:
//!
//! * an object stored at `1_700_000_000_750` lists as exactly `2023-11-14T22:13:20.000Z`
//!   under ListObjectsV2 **and** v1 ListObjects;
//! * HEAD **and** GET carry `Last-Modified: Tue, 14 Nov 2023 22:13:20 GMT`;
//! * the listing value and the header value, each parsed the way the AWS SDK parses that
//!   field (`aws-smithy-types`, independent of the gateway's formatters), name the same
//!   instant;
//! * an object stored exactly on a second (`1_700_000_000_000`) renders as it always did.
//!
//! Harness: the bucket marker and both objects are seeded on the store BEFORE it moves into
//! `Gateway::new` (the `s3_list_objects.rs` `seed_bucket` + `start_gateway` pattern). Each
//! object is a genuine flat write through `wyrd_core::write` (the `segmented_object_read.rs`
//! `write_flat` pattern) with the plan's `object_meta.modified` set, the field the gateway's
//! own PUT stamps, under the dirent name `"{bucket}/{key}"` beneath `ROOT`. Requests are raw
//! signed HTTP, so the exact wire strings are asserted first, before any date parsing.
//!
//! The raw-HTTP oracle is strict, so it cannot pass a response a conforming client would
//! reject: every socket exchange runs under a deadline, and the response reader refuses a
//! missing header terminator, a malformed status or header line, conflicting or absent
//! framing, a body shorter or longer than its declared `Content-Length`, a body on a HEAD
//! response, and any chunked body whose sizes, chunk terminators, terminal chunk or final
//! CRLF are invalid or missing. `the_response_reader_rejects_incomplete_or_malformed_framing`
//! proves each refusal.
//!
//! Every symbol used here predates #767, so on the pre-fix base this file compiles and fails
//! by assertion (`.750Z` against `:20 GMT`).

#![forbid(unsafe_code)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_smithy_types::date_time::Format;
use aws_smithy_types::DateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{EcScheme, ObjectMeta};
use wyrd_core::write;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials as GatewayCredentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// Upper bound on every await on the store or the socket. A stalled gateway, or one that
/// never closes the connection, fails the test instead of hanging it.
const DEADLINE: Duration = Duration::from_secs(30);

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 4;

const BUCKET: &str = "stamps";
/// Stored off a second boundary: the case that disagreed before #767.
const OFF_KEY: &str = "off-second";
const OFF_BODY: &[u8] = b"off-second body";
const OFF_MODIFIED: u64 = 1_700_000_000_750;
/// Stored exactly on a second: must render as it always did.
const ON_KEY: &str = "on-second";
const ON_BODY: &[u8] = b"on-second body";
const ON_MODIFIED: u64 = 1_700_000_000_000;

const EXPECTED_ISO: &str = "2023-11-14T22:13:20.000Z";
const EXPECTED_IMF: &str = "Tue, 14 Nov 2023 22:13:20 GMT";

/// Await `fut` for at most [`DEADLINE`], failing the test (not hanging it) past that.
async fn bounded<F: Future>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(DEADLINE, fut)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {DEADLINE:?}"))
}

/// Seed a `bucket:{name}` existence marker as raw key/value bytes (listing only reads it for
/// presence), as `s3_list_objects.rs::seed_bucket` does.
async fn seed_bucket(meta: &RedbMetadataStore, name: &str) {
    let key = format!("bucket:{name}").into_bytes();
    let value = format!("{{\"name\":\"{name}\",\"created_millis\":0}}").into_bytes();
    meta.commit(WriteBatch::new().put(key, value))
        .await
        .expect("commit bucket marker");
}

/// Write a genuine flat object through the ordinary write path (real fragments, the dirent the
/// gateway resolves the key through), stamping `modified` on the plan exactly where the
/// gateway's own PUT stamps it.
async fn write_flat(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    inode_id: u64,
    name: &str,
    data: &[u8],
    modified: u64,
    id_base: u128,
) {
    let mut next = id_base;
    let mut plan = write::plan_write(data, CHUNK, EcScheme::None, || {
        next += 1;
        next
    })
    .expect("plan");
    plan.object_meta = ObjectMeta {
        etag: None,
        content_type: None,
        modified: Some(modified),
    };
    write::intent(meta, &plan, NOW + TTL).await.expect("intent");
    write::write_fragments(chunks, &plan)
        .await
        .expect("fan out");
    assert_eq!(
        write::commit_create(meta, ROOT, name, inode_id, &plan, NOW)
            .await
            .expect("commit"),
        CommitOutcome::Committed
    );
    write::release(meta, &plan).await.expect("release");
}

/// The running gateway. Dropping it aborts the serve task, so no helper task outlives the
/// test; it also keeps the chunk store's temp dir alive for as long as the gateway runs.
struct Server {
    addr: SocketAddr,
    task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Seed the bucket and both objects, then start the S3 gateway on an ephemeral loopback port.
async fn start_gateway() -> Server {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    let chunks = FsChunkStore::open(dir.path()).expect("fs store");
    bounded("seeding the store", async {
        seed_bucket(&meta, BUCKET).await;
        write_flat(
            &meta,
            &chunks,
            1_001,
            &format!("{BUCKET}/{OFF_KEY}"),
            OFF_BODY,
            OFF_MODIFIED,
            10_000,
        )
        .await;
        write_flat(
            &meta,
            &chunks,
            1_002,
            &format!("{BUCKET}/{ON_KEY}"),
            ON_BODY,
            ON_MODIFIED,
            20_000,
        )
        .await;
    })
    .await;
    let gateway = Arc::new(Gateway::new(meta, chunks, MemCoordination::new()).with_chunk_size(8));
    let config = S3Config::new(vec![GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = bounded("binding the listener", TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    let task = tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    Server {
        addr,
        task,
        _dir: dir,
    }
}

/// The SigV4 `x-amz-date` for a request sent now.
// wall-clock exempt (test crate): the wall clock owns the SigV4 request-signing lifecycle
// against a live in-process server, and nothing else here reads a clock. The asserted
// `modified` values are fixed constants seeded on the store, so no asserted lifecycle mixes
// clock sources (#619).
#[allow(clippy::disallowed_methods)]
fn amz_date_now() -> String {
    format_amz_date(SystemTime::now())
}

/// A parsed response: status, the header fields (lowercased names, in order), and the body
/// with its transfer framing removed.
#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    /// The value of header `name` (lowercase), or `None` when absent. A repeated field fails
    /// the test: a surface that sends two `Last-Modified` values has no single answer.
    fn header(&self, name: &str) -> Option<&str> {
        let mut values = self.headers.iter().filter(|(n, _)| n == name);
        let first = values.next().map(|(_, v)| v.as_str());
        assert!(values.next().is_none(), "header {name} is repeated");
        first
    }

    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).expect("UTF-8 response body")
    }
}

/// Position of the first `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse one complete HTTP/1.1 response, read until the server closed the connection. Any
/// incomplete or malformed framing is an error, never a value: the body is only exposed when
/// the bytes on the wire are exactly what the framing declared. `head` says the request was a
/// HEAD, so the response must carry no body at all (RFC 9110 §9.3.2); its `Content-Length`,
/// if present, describes the GET representation, not this message.
fn parse_response(raw: &[u8], head: bool) -> Result<Response, String> {
    let split = find(raw, b"\r\n\r\n").ok_or("no end of the header block (truncated)")?;
    let block = std::str::from_utf8(&raw[..split]).map_err(|_| "header block is not UTF-8")?;
    let mut lines = block.split("\r\n");
    let status_line = lines.next().ok_or("no status line")?;
    if status_line.contains(['\r', '\n']) {
        return Err(format!("bare CR or LF in the status line: {status_line:?}"));
    }
    let (version, rest) = status_line
        .split_once(' ')
        .ok_or_else(|| format!("malformed status line: {status_line:?}"))?;
    if version != "HTTP/1.1" {
        return Err(format!("not an HTTP/1.1 response: {status_line:?}"));
    }
    let code = rest.split_once(' ').map_or(rest, |(code, _)| code);
    if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("status code is not three digits: {status_line:?}"));
    }
    let status: u16 = code.parse().map_err(|_| "status code")?;

    let mut headers = Vec::new();
    for line in lines {
        if line.starts_with([' ', '\t']) {
            return Err(format!("obsolete line folding: {line:?}"));
        }
        if line.contains(['\r', '\n']) {
            return Err(format!("bare CR or LF in a header line: {line:?}"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("header line without a colon: {line:?}"))?;
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(format!("malformed header name: {line:?}"));
        }
        headers.push((
            name.to_ascii_lowercase(),
            value.trim_matches([' ', '\t']).to_string(),
        ));
    }

    let single = |name: &str| -> Result<Option<String>, String> {
        let mut values = headers.iter().filter(|(n, _)| n == name);
        let first = values.next().map(|(_, v)| v.clone());
        if values.next().is_some() {
            return Err(format!("repeated {name} header"));
        }
        Ok(first)
    };
    let content_length = match single("content-length")? {
        Some(v) if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) => Some(
            v.parse::<usize>()
                .map_err(|_| format!("Content-Length {v:?}"))?,
        ),
        Some(v) => return Err(format!("Content-Length is not a decimal length: {v:?}")),
        None => None,
    };
    let transfer_encoding = single("transfer-encoding")?;
    if content_length.is_some() && transfer_encoding.is_some() {
        return Err("both Content-Length and Transfer-Encoding are present".to_string());
    }

    let after = &raw[split + 4..];
    let body = if head {
        if !after.is_empty() {
            return Err(format!(
                "a HEAD response carried {} body bytes",
                after.len()
            ));
        }
        Vec::new()
    } else if let Some(te) = transfer_encoding {
        if !te.eq_ignore_ascii_case("chunked") {
            return Err(format!("unsupported Transfer-Encoding: {te:?}"));
        }
        dechunk(after)?
    } else if let Some(len) = content_length {
        if after.len() != len {
            return Err(format!(
                "Content-Length {len} but {} body bytes arrived",
                after.len()
            ));
        }
        after.to_vec()
    } else {
        return Err("no framing: neither Content-Length nor chunked".to_string());
    };
    Ok(Response {
        status,
        headers,
        body,
    })
}

/// Decode an HTTP/1.1 chunked body (RFC 9112 §7.1) that must span ALL of `raw`: every size
/// line is hex digits (no sign, optional `;ext`), every chunk's data is followed by CRLF, the
/// zero-size terminal chunk is present, its trailer section ends with CRLF, and nothing
/// follows it.
fn dechunk(mut raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = find(raw, b"\r\n").ok_or("chunk-size line without CRLF (truncated)")?;
        let line = &raw[..line_end];
        let size_field = line.split(|&b| b == b';').next().unwrap_or(line);
        if size_field.is_empty() || !size_field.iter().all(u8::is_ascii_hexdigit) {
            return Err(format!(
                "invalid chunk size {:?}",
                String::from_utf8_lossy(line)
            ));
        }
        let size = std::str::from_utf8(size_field)
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or("chunk size overflows")?;
        raw = &raw[line_end + 2..];
        if size == 0 {
            loop {
                let end = find(raw, b"\r\n").ok_or("trailer section without final CRLF")?;
                if end != 0 && !raw[..end].contains(&b':') {
                    return Err("trailer field without a colon".to_string());
                }
                raw = &raw[end + 2..];
                if end == 0 {
                    break;
                }
            }
            if !raw.is_empty() {
                return Err(format!("{} bytes after the terminal chunk", raw.len()));
            }
            return Ok(out);
        }
        let framed = size.checked_add(2).ok_or("chunk size overflows")?;
        if raw.len() < framed {
            return Err(format!("chunk of {size} bytes truncated at {}", raw.len()));
        }
        if &raw[size..size + 2] != b"\r\n" {
            return Err("chunk data not followed by CRLF".to_string());
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
}

/// Send a raw SigV4-signed `{method} {path}[?{query}]` and parse the response strictly. A
/// listing request carries a query, so the canonical query is signed (an empty-query
/// signature would 403 — `s3_list_objects.rs::signed_headers_q`). The whole exchange
/// (connect, write, read to close) runs under [`DEADLINE`].
async fn send(addr: SocketAddr, method: &str, path: &str, query: &str) -> Response {
    let host = addr.to_string();
    let creds = GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let signed = sign(
        method,
        path,
        query,
        &host,
        &amz_date_now(),
        b"",
        &creds,
        REGION,
        "s3",
    );
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut request = format!("{method} {target} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    request.push_str(&format!("authorization: {}\r\n", signed.authorization));
    request.push_str(&format!("x-amz-date: {}\r\n", signed.amz_date));
    request.push_str(&format!(
        "x-amz-content-sha256: {}\r\n",
        signed.content_sha256
    ));
    request.push_str("content-length: 0\r\n");
    request.push_str("connection: close\r\n\r\n");

    let raw = bounded(&format!("{method} {target}"), async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        stream.flush().await.expect("flush");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        raw
    })
    .await;
    parse_response(&raw, method == "HEAD").unwrap_or_else(|e| {
        panic!(
            "{method} {target}: malformed response ({e}): {:?}",
            String::from_utf8_lossy(&raw)
        )
    })
}

/// The `<LastModified>` of the one `<Contents>` row whose `<Key>` is `key`. A missing row, a
/// repeated row, or a missing or repeated element fails loudly, so an omitted value can never
/// pass as agreement.
fn listed_last_modified(body: &str, key: &str) -> String {
    let wanted = format!("<Key>{key}</Key>");
    let rows: Vec<&str> = body
        .split("<Contents>")
        .skip(1)
        .map(|row| {
            row.split_once("</Contents>")
                .unwrap_or_else(|| panic!("unclosed <Contents> row in listing: {body}"))
                .0
        })
        .filter(|row| row.contains(&wanted))
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "exactly one <Contents> row for {key}: {body}"
    );
    let row = rows[0];
    let pieces: Vec<&str> = row.split("<LastModified>").skip(1).collect();
    assert_eq!(
        pieces.len(),
        1,
        "exactly one <LastModified> in the row for {key}: {body}"
    );
    pieces[0]
        .split_once("</LastModified>")
        .unwrap_or_else(|| panic!("unclosed <LastModified> for {key}: {body}"))
        .0
        .to_string()
}

/// Parse a listing's `<LastModified>` exactly as `aws-sdk-s3` decodes that element (its
/// generated `Object` deserializer uses `Format::DateTimeWithOffset`), keeping any fraction.
/// The parser is the SDK's, not the gateway's, so the instant check is not self-referential.
fn parse_listed(s: &str) -> DateTime {
    DateTime::from_str(s, Format::DateTimeWithOffset)
        .unwrap_or_else(|e| panic!("listing <LastModified> {s:?} is not RFC 3339: {e}"))
}

/// Parse a `Last-Modified` header exactly as `aws-sdk-s3` decodes it (`Format::HttpDate`).
fn parse_header(s: &str) -> DateTime {
    DateTime::from_str(s, Format::HttpDate)
        .unwrap_or_else(|e| panic!("Last-Modified {s:?} is not an HTTP-date: {e}"))
}

/// Epoch milliseconds of a parsed instant, for comparing against the stored `modified`.
fn millis(t: DateTime) -> u64 {
    let ms = t.to_millis().expect("instant fits in epoch milliseconds");
    u64::try_from(ms).expect("instant after the epoch")
}

#[test]
fn the_sdk_date_parsers_tell_a_fraction_from_a_whole_second() {
    // Check (3) is only as good as its parsers. They must recover the known instants, and the
    // listing parser must keep the fraction: if it dropped it, the pre-#767 `.750Z` would parse
    // to the same instant as `:20 GMT` and check (3) would pass on the unfixed gateway.
    let pre_fix = parse_listed("2023-11-14T22:13:20.750Z");
    assert_eq!(millis(pre_fix), OFF_MODIFIED);
    assert_eq!(millis(parse_listed(EXPECTED_ISO)), ON_MODIFIED);
    assert_eq!(millis(parse_header(EXPECTED_IMF)), ON_MODIFIED);
    assert_ne!(
        pre_fix,
        parse_header(EXPECTED_IMF),
        "the pre-#767 listing value and the header must parse to different instants"
    );
}

#[test]
fn the_response_reader_rejects_incomplete_or_malformed_framing() {
    const OK: &str = "HTTP/1.1 200 OK\r\n";
    const CHUNKED: &str = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
    /// A complete chunked body: one five-byte chunk, the terminal chunk, the final CRLF.
    const HELLO: &str = "5\r\nhello\r\n0\r\n\r\n";
    const TWO_LENGTHS: &str = "content-length: 5\r\ncontent-length: 5\r\n";
    const BOTH_FRAMINGS: &str = "content-length: 5\r\ntransfer-encoding: chunked\r\n";
    let get = |raw: &str| parse_response(raw.as_bytes(), false);
    let head = |raw: &str| parse_response(raw.as_bytes(), true);

    // Well-framed responses decode to exactly their body.
    let chunked = get(&format!(
        "{CHUNKED}5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\n\r\n"
    ))
    .expect("a complete chunked body");
    assert_eq!(
        (chunked.status, chunked.body.as_slice()),
        (200, &b"hello world"[..])
    );
    let trailer = get(&format!("{CHUNKED}2\r\nok\r\n0\r\nx-trailer: 1\r\n\r\n"))
        .expect("a chunked body with a trailer section");
    assert_eq!(trailer.body, b"ok");
    let sized = get(&format!("{OK}content-length: 5\r\n\r\nhello")).expect("a sized body");
    assert_eq!(sized.body, b"hello");
    let bodyless = head(&format!(
        "{OK}content-length: 15\r\nlast-modified: x\r\n\r\n"
    ))
    .expect("a HEAD response declares the GET length but carries no body");
    assert_eq!(
        (bodyless.body.len(), bodyless.header("last-modified")),
        (0, Some("x"))
    );

    // Everything below is incomplete or malformed. Each must be an error, never a body, and
    // for the stated reason, so no case can pass on some earlier, unrelated refusal.
    let refused = |what: &str, head: bool, raw: String, reason: &str| {
        let parsed = parse_response(raw.as_bytes(), head);
        match parsed {
            Err(e) => assert!(
                e.contains(reason),
                "{what}: refused for the wrong reason: {e}"
            ),
            Ok(r) => panic!("{what} must be refused, got {r:?}"),
        }
    };
    let (as_get, as_head) = (false, true);
    refused(
        "no header terminator",
        as_get,
        format!("{OK}content-length: 0\r\n"),
        "no end of the header block",
    );
    refused(
        "two-digit status",
        as_get,
        "HTTP/1.1 20 OK\r\ncontent-length: 0\r\n\r\n".into(),
        "not three digits",
    );
    refused(
        "not HTTP/1.1",
        as_get,
        "HTTP/1.0 200 OK\r\ncontent-length: 0\r\n\r\n".into(),
        "not an HTTP/1.1",
    );
    refused(
        "obs-fold",
        as_get,
        format!("{OK}content-length: 0\r\n x: y\r\n\r\n"),
        "obsolete line folding",
    );
    refused(
        "header without colon",
        as_get,
        format!("{OK}content-length 0\r\n\r\n"),
        "without a colon",
    );
    refused(
        "bare LF in a header",
        as_get,
        format!("{OK}a: b\nc: d\r\ncontent-length: 0\r\n\r\n"),
        "bare CR or LF",
    );
    refused(
        "signed Content-Length",
        as_get,
        format!("{OK}content-length: +5\r\n\r\nhello"),
        "not a decimal length",
    );
    refused(
        "repeated Content-Length",
        as_get,
        format!("{OK}{TWO_LENGTHS}\r\nhello"),
        "repeated content-length",
    );
    refused(
        "short body",
        as_get,
        format!("{OK}content-length: 6\r\n\r\nhello"),
        "Content-Length 6 but 5",
    );
    refused(
        "long body",
        as_get,
        format!("{OK}content-length: 4\r\n\r\nhello"),
        "Content-Length 4 but 5",
    );
    refused("no framing", as_get, format!("{OK}\r\nhello"), "no framing");
    refused(
        "both framings",
        as_get,
        format!("{OK}{BOTH_FRAMINGS}\r\n{HELLO}"),
        "both Content-Length and",
    );
    refused(
        "body on HEAD",
        as_head,
        format!("{OK}content-length: 5\r\n\r\nhello"),
        "HEAD response carried 5",
    );
    refused(
        "EOF after a data chunk",
        as_get,
        format!("{CHUNKED}5\r\nhello\r\n"),
        "chunk-size line without CRLF",
    );
    refused(
        "EOF inside a size line",
        as_get,
        format!("{CHUNKED}5"),
        "chunk-size line without CRLF",
    );
    refused(
        "invalid chunk size",
        as_get,
        format!("{CHUNKED}zz\r\nhello\r\n0\r\n\r\n"),
        "invalid chunk size",
    );
    refused(
        "signed chunk size",
        as_get,
        format!("{CHUNKED}+5\r\nhello\r\n0\r\n\r\n"),
        "invalid chunk size",
    );
    refused(
        "empty chunk size",
        as_get,
        format!("{CHUNKED}\r\nhello\r\n0\r\n\r\n"),
        "invalid chunk size",
    );
    refused(
        "chunk data truncated",
        as_get,
        format!("{CHUNKED}9\r\nhello\r\n"),
        "chunk of 9 bytes truncated",
    );
    refused(
        "chunk without CRLF",
        as_get,
        format!("{CHUNKED}5\r\nhelloXX0\r\n\r\n"),
        "not followed by CRLF",
    );
    refused(
        "terminal chunk without final CRLF",
        as_get,
        format!("{CHUNKED}5\r\nhello\r\n0\r\n"),
        "without final CRLF",
    );
    refused(
        "bytes after the terminal chunk",
        as_get,
        format!("{CHUNKED}{HELLO}extra"),
        "after the terminal chunk",
    );
    refused(
        "trailer field without a colon",
        as_get,
        format!("{CHUNKED}0\r\nnot-a-field\r\n\r\n"),
        "trailer field without",
    );
    refused(
        "chunk size that overflows",
        as_get,
        format!("{CHUNKED}ffffffffffffffff\r\nhello\r\n0\r\n\r\n"),
        "overflows",
    );
}

/// One object's modification time as every S3 surface reports it.
struct Surfaces {
    list_v2: String,
    list_v1: String,
    head: String,
    get: String,
}

/// A listing body must be a whole `<ListBucketResult>` document, not a cut-off one.
fn assert_whole_listing(what: &str, body: &str) {
    assert!(
        body.starts_with("<?xml") && body.trim_end().ends_with("</ListBucketResult>"),
        "{what} is not a complete <ListBucketResult> document: {body}"
    );
}

async fn surfaces(addr: SocketAddr, key: &str, stored: &[u8]) -> Surfaces {
    let bucket_path = format!("/{BUCKET}");
    let v2 = send(addr, "GET", &bucket_path, "list-type=2").await;
    let v2_body = v2.text();
    assert_eq!(v2.status, 200, "ListObjectsV2: {v2_body}");
    assert_whole_listing("ListObjectsV2", &v2_body);
    assert!(
        v2_body.contains("<KeyCount>"),
        "list-type=2 must be the v2 listing: {v2_body}"
    );
    let v1 = send(addr, "GET", &bucket_path, "").await;
    let v1_body = v1.text();
    assert_eq!(v1.status, 200, "v1 ListObjects: {v1_body}");
    assert_whole_listing("v1 ListObjects", &v1_body);
    assert!(
        !v1_body.contains("<KeyCount>"),
        "the bare bucket GET must be the v1 listing, not v2: {v1_body}"
    );

    let object_path = format!("/{BUCKET}/{key}");
    let head = send(addr, "HEAD", &object_path, "").await;
    assert_eq!(head.status, 200, "HEAD {key}");
    let get = send(addr, "GET", &object_path, "").await;
    assert_eq!(
        get.status,
        200,
        "GET {key}: {}",
        String::from_utf8_lossy(&get.body)
    );
    assert_eq!(get.body, stored, "GET {key} returns the stored bytes");

    Surfaces {
        list_v2: listed_last_modified(&v2_body, key),
        list_v1: listed_last_modified(&v1_body, key),
        head: head
            .header("last-modified")
            .unwrap_or_else(|| panic!("HEAD {key} carries no Last-Modified"))
            .to_string(),
        get: get
            .header("last-modified")
            .unwrap_or_else(|| panic!("GET {key} carries no Last-Modified"))
            .to_string(),
    }
}

fn assert_agreement(key: &str, s: &Surfaces) {
    // (1) Both listing forms render the whole second.
    assert_eq!(
        s.list_v2, EXPECTED_ISO,
        "ListObjectsV2 <LastModified> of {key}"
    );
    assert_eq!(
        s.list_v1, EXPECTED_ISO,
        "v1 ListObjects <LastModified> of {key}"
    );
    // (2) HEAD and GET carry the IMF-fixdate of that second.
    assert_eq!(s.head, EXPECTED_IMF, "HEAD Last-Modified of {key}");
    assert_eq!(s.get, EXPECTED_IMF, "GET Last-Modified of {key}");
    // (3) Parsed as the AWS SDK parses each field, every surface names the same instant.
    let header_instant = parse_header(&s.head);
    for (surface, listed) in [("v2", &s.list_v2), ("v1", &s.list_v1)] {
        assert_eq!(
            parse_listed(listed),
            header_instant,
            "{surface} listing ({listed}) and HEAD ({}) disagree on {key}'s instant",
            s.head
        );
    }
    assert_eq!(
        parse_header(&s.get),
        header_instant,
        "GET vs HEAD for {key}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_second_modified_lists_and_heads_as_the_same_instant() {
    let server = start_gateway().await;
    let s = surfaces(server.addr, OFF_KEY, OFF_BODY).await;
    assert_agreement(OFF_KEY, &s);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_second_modified_renders_unchanged_on_both_surfaces() {
    let server = start_gateway().await;
    let s = surfaces(server.addr, ON_KEY, ON_BODY).await;
    assert_agreement(ON_KEY, &s);
}
