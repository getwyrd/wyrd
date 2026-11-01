//! **S3 multipart routing** (issue #667, the first child of #508): every spelling of the three
//! multipart query keys — `uploads`, `uploadId`, `partNumber` — is classified, and none of them
//! reaches a plain object PUT, GET, HEAD or DELETE, or a listing.
//!
//! The defect: the object route refused those keys by matching the RAW query, while SigV4
//! canonicalisation decodes it once and re-encodes it. So a signed `PUT /b/k?part%4Eumber=1`
//! passed the refusal as an unknown key, reached the plain PUT and overwrote `k`; and a signed
//! `DELETE /b/k?upload%49d=…` reached the plain DELETE and deleted it.
//!
//! Every leg drives a fresh in-process gateway over the wire, served by the production entry
//! point `wyrd_server::cli::serve_s3` over a real `Gateway` (redb in memory, chunks on a temp
//! dir), with hand-signed raw requests, because an SDK will not spell these forms. Bucket `b`
//! holds an object `k` with known bytes. Every assertion is an exact status and an exact S3
//! error code; where a `501` must name what it refuses (an operation, a key), the `<Message>` is
//! checked for it too. After EVERY request each seeded object must still answer HEAD `200` with
//! the ETag its seeding PUT returned, and GET its original bytes:
//!
//! 1. [`percent_encoded_spellings_classify_as_their_decoded_key`]
//! 2. [`ill_formed_multipart_shapes_answer_400_invalid_argument`]
//! 3. [`well_formed_multipart_shapes_answer_501_naming_the_operation`]
//! 4. [`valid_non_multipart_forms_are_unaffected`]
//! 5. [`a_denylisted_subresource_wins_in_any_spelling`]
//! 6. [`a_bucket_alias_is_refused_on_every_multipart_route`]
//! 7. [`bulk_delete_keeps_refusing_a_multipart_selector`]
//! 8. [`an_encoded_bulk_delete_marker_runs_the_bulk_delete`]
//! 9. [`an_encoded_bulk_delete_marker_keeps_every_refusal`]
//!
//! Two more tests check the harness itself: the response parser refuses torn framing, and a
//! dropped fixture stops its server.
//!
//! A leg collects every failing row and fails once, listing them all, so one red run reports the
//! whole table rather than its first row; an object found changed is put back before the next
//! row, so one mishandled form does not fail the rows after it.
//!
//! On the base (before #667) legs 1, 2, 3, 5, 6, 8 and 9 fail by assertion, never by a build
//! error: this file uses only API the base already has. Legs 4 and 7 are regression guards, and
//! the two harness tests touch no production change, so those are green on both trees.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates against a live in-process server use
// real wall time; nothing here mixes clock sources within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never ships, so no production
// lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::cli::serve_s3;
use wyrd_server::Gateway;
use wyrd_traits::{MetadataStore, WriteBatch};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// How long one request/response exchange may take before the test fails instead of hanging.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The bucket every leg addresses.
const BUCKET: &str = "b";
/// The object every request must leave in place, and its known bytes.
const K: &str = "/b/k";
const K_BYTES: &[u8] = b"k: the bytes every request must leave in place";
/// The second object leg 7's bulk-delete body names beside `k`.
const K2: &str = "/b/k2";
const K2_BYTES: &[u8] = b"k2: the second object the bulk delete names";
/// What a request mishandled as a plain object PUT would overwrite its target with: every PUT
/// row carries it as its body.
const CLOBBER: &[u8] = b"CLOBBERED: a multipart form was served as a plain object PUT";

/// A well-formed upload id (32 lowercase hex), an unknown one of the same shape (never issued:
/// nothing in this slice can create an upload), and a malformed one.
const WELL_FORMED_ID: &str = "0123456789abcdef0123456789abcdef";
const UNKNOWN_ID: &str = "fedcba9876543210fedcba9876543210";
const MALFORMED_ID: &str = "zz";

const NOT_IMPLEMENTED: &str = "NotImplemented";
const INVALID_ARGUMENT: &str = "InvalidArgument";
const INVALID_BUCKET_NAME: &str = "InvalidBucketName";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

// ---- the harness ----------------------------------------------------------------------------

fn credentials() -> Credentials {
    Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }
}

/// Seed a `bucket:{name}` existence marker directly on the store, as raw key/value bytes
/// committed before the store moves into `Gateway::new` — the record CreateBucket (#511) will
/// write, seeded as the peer suite `s3_list_objects.rs` seeds it. Only the listing reads it.
async fn seed_bucket(meta: &RedbMetadataStore, name: &str) {
    let key = format!("bucket:{name}").into_bytes();
    let value = format!("{{\"name\":\"{name}\",\"created_millis\":0}}").into_bytes();
    tokio::time::timeout(HTTP_TIMEOUT, meta.commit(WriteBatch::new().put(key, value)))
        .await
        .expect("the bucket marker commits in time")
        .expect("commit bucket marker");
}

/// A gateway serving on loopback, and the objects every request must leave as they are.
struct Served {
    addr: SocketAddr,
    /// Each seeded object as `(path, bytes, the ETag its seeding PUT answered)`.
    objects: Vec<(&'static str, &'static [u8], String)>,
    /// The serving task, cancelled when this is dropped.
    server: JoinHandle<()>,
    /// The backend the serving task owns: a weak handle, so it goes when the task does.
    backend: Weak<Backend>,
    /// The chunk store's directory, which must outlive the gateway.
    _dir: tempfile::TempDir,
}

impl Drop for Served {
    /// Cancel the server on every exit path, an assertion failure included, before the fields
    /// drop and remove its directory. `#[tokio::test]` runs one thread, which is here, so the
    /// aborted task is never polled again: it serves nothing once its directory is going.
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Serve a fresh gateway through `serve_s3` — the entry point every `wyrd s3` composition runs —
/// on an ephemeral loopback port, with bucket `b` marked and object `k` stored.
async fn start() -> Served {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    seed_bucket(&meta, BUCKET).await;
    let gateway = Arc::new(Gateway::new(
        meta,
        FsChunkStore::open(dir.path()).expect("fs store"),
        MemCoordination::new(),
    ));
    let backend = Arc::downgrade(&gateway);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        serve_s3(
            gateway,
            vec![credentials()],
            REGION.to_string(),
            listener,
            None,
        )
        .await
        .expect("serve_s3");
    });
    let mut served = Served {
        addr,
        objects: Vec::new(),
        server,
        backend,
        _dir: dir,
    };
    served.seed(K, K_BYTES).await;
    served
}

impl Served {
    /// Store `bytes` at `path` with a plain PUT, and add it to the objects every request must
    /// leave in place.
    async fn seed(&mut self, path: &'static str, bytes: &'static [u8]) {
        let etag = self.put_back(path, bytes).await;
        self.objects.push((path, bytes, etag));
    }

    /// A plain PUT of `bytes` at `path`, which must store them: its ETag.
    async fn put_back(&self, path: &str, bytes: &[u8]) -> String {
        let reply = send(self.addr, "PUT", path, "", bytes).await;
        assert_eq!(reply.status, 200, "plain PUT {path}: {}", reply.text());
        reply.etag.expect("a stored PUT answers an ETag")
    }

    /// Check every seeded object is as it was: HEAD `200` with its seeding ETag, GET `200` with
    /// its bytes. Each failure is described after `context`; an object found changed is put
    /// back, so the next row starts from the same state.
    async fn intact(&self, context: &str) -> Vec<String> {
        let mut failures = Vec::new();
        for (path, bytes, etag) in &self.objects {
            let head = send(self.addr, "HEAD", path, "", b"").await;
            let get = send(self.addr, "GET", path, "", b"").await;
            let fine = head.status == 200
                && head.etag.as_deref() == Some(etag.as_str())
                && get.status == 200
                && get.body == *bytes;
            if !fine {
                failures.push(format!(
                    "{context}: then {path} answered HEAD {} (ETag {:?}, seeded {etag}) and GET {} \
                     with {} bytes{}",
                    head.status,
                    head.etag,
                    get.status,
                    get.body.len(),
                    if get.body == CLOBBER {
                        " — overwritten with the request's body"
                    } else {
                        ""
                    },
                ));
                self.put_back(path, bytes).await;
            }
        }
        failures
    }
}

/// One request and what it must answer.
struct Case {
    method: &'static str,
    path: &'static str,
    query: String,
    body: Vec<u8>,
    status: u16,
    /// The exact S3 error code; `None` for a HEAD, whose error carries no body.
    code: Option<&'static str>,
    /// Text the error's `<Message>` must contain, for an answer that must name what it refuses.
    names: Option<&'static str>,
}

/// A row: `method` on `path` with `query` exactly as it rides on the wire, answering `status`
/// and S3 error `code`. A PUT carries [`CLOBBER`], so a PUT served as a plain object PUT changes
/// its target; every other method carries no body.
fn case(
    method: &'static str,
    path: &'static str,
    query: impl Into<String>,
    status: u16,
    code: &'static str,
) -> Case {
    Case {
        method,
        path,
        query: query.into(),
        body: if method == "PUT" {
            CLOBBER.to_vec()
        } else {
            Vec::new()
        },
        status,
        code: (method != "HEAD").then_some(code),
        names: None,
    }
}

impl Case {
    /// The error message must contain `text`.
    fn naming(mut self, text: &'static str) -> Self {
        self.names = Some(text);
        self
    }

    /// Send `body` instead of the default.
    fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
}

/// Send each row, check its exact answer, then check every seeded object is intact. Fails once,
/// listing every failing row.
async fn run(served: &Served, cases: &[Case]) {
    let mut failures = Vec::new();
    for case in cases {
        let request = format!("{} {}?{}", case.method, case.path, case.query);
        let reply = send(served.addr, case.method, case.path, &case.query, &case.body).await;
        let code = reply.element("Code");
        let message = reply.element("Message");
        let named = match (case.names, &message) {
            (None, _) => true,
            (Some(text), Some(message)) => message.contains(text),
            (Some(_), None) => false,
        };
        if reply.status != case.status || (case.code.is_some() && code.as_deref() != case.code) {
            failures.push(format!(
                "{request}: expected {} {}, got {} {} — {}",
                case.status,
                case.code.unwrap_or("(no body)"),
                reply.status,
                code.as_deref().unwrap_or("(no code)"),
                message.as_deref().unwrap_or("(no message)"),
            ));
        } else if !named {
            failures.push(format!(
                "{request}: the {} must name {:?}, got message {message:?}",
                case.status,
                case.names.unwrap_or_default(),
            ));
        }
        failures.extend(served.intact(&request).await);
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Fail unless every seeded object is as it was.
async fn assert_intact(served: &Served, context: &str) {
    let failures = served.intact(context).await;
    assert!(failures.is_empty(), "{failures:#?}");
}

/// One HTTP response: its status, its `ETag` header, and its de-framed body.
struct Reply {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The text of the body's first `<name>` element, if any.
    fn element(&self, name: &str) -> Option<String> {
        let text = self.text();
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let start = text.find(&open)? + open.len();
        let end = start + text[start..].find(&close)?;
        Some(text[start..end].to_string())
    }
}

/// Send one SigV4-signed `method` on `path` with `query` and `body`, and read the whole
/// response. `path` and `query` go on the wire exactly as given (so `/b%2Fa/k` and
/// `?part%4Eumber=1` stay encoded) and are signed as given: `sign` canonicalises the query the
/// way the gateway does, so an encoded key passes authentication and reaches routing. Bounded: a
/// server that never answers fails the test instead of hanging it.
async fn send(addr: SocketAddr, method: &str, path: &str, query: &str, body: &[u8]) -> Reply {
    let host = addr.to_string();
    let signed = sign(
        method,
        path,
        query,
        &host,
        &format_amz_date(SystemTime::now()),
        body,
        &credentials(),
        REGION,
        "s3",
    );
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nhost: {host}\r\nauthorization: {}\r\nx-amz-date: {}\r\n\
         x-amz-content-sha256: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        signed.authorization,
        signed.amz_date,
        signed.content_sha256,
        body.len(),
    )
    .into_bytes();
    // Head and body in one write: the server holds the whole request before it answers.
    request.extend_from_slice(body);
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(&request).await.expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        raw
    };
    let raw = tokio::time::timeout(HTTP_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| panic!("{method} {target}: no response within {HTTP_TIMEOUT:?}"));
    parse_response(&raw, method == "HEAD").unwrap_or_else(|e| {
        panic!(
            "{method} {target}: {e}\nraw response: {}",
            String::from_utf8_lossy(&raw)
        )
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse one HTTP/1.1 response whose framing must prove the body whole, so a truncated response
/// fails here instead of reading as a short body: a `content-length` equal to the bytes received,
/// or a chunked body that ends exactly at its final CRLF. A response that declares neither has an
/// unprovable length and is refused, except a HEAD, `204` or `304`, which has no body at all.
fn parse_response(raw: &[u8], head_only: bool) -> Result<Reply, String> {
    let head_end = find(raw, b"\r\n\r\n").ok_or("no end of the header section")?;
    let head = std::str::from_utf8(&raw[..head_end]).map_err(|e| format!("head: {e}"))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or("empty response")?;
    let mut parts = status_line.splitn(3, ' ');
    if !matches!(parts.next(), Some("HTTP/1.1" | "HTTP/1.0")) {
        return Err(format!("status line `{status_line}`: bad version"));
    }
    let status = parts
        .next()
        .filter(|code| code.len() == 3 && code.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("status line `{status_line}`: bad status code"))?;
    let mut content_length = None;
    let mut chunked = false;
    let mut etag = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("header line `{line}` has no colon"))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some()
                || value.is_empty()
                || !value.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(format!("bad or repeated content-length `{value}`"));
            }
            content_length = Some(
                value
                    .parse::<usize>()
                    .map_err(|e| format!("content-length `{value}`: {e}"))?,
            );
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !value.eq_ignore_ascii_case("chunked") {
                return Err(format!(
                    "unsupported or repeated transfer-encoding `{value}`"
                ));
            }
            chunked = true;
        } else if name.eq_ignore_ascii_case("etag") {
            etag = Some(value.to_string());
        }
    }
    let rest = &raw[head_end + 4..];
    let bodyless = head_only || status == 204 || status == 304;
    let body = if bodyless {
        if !rest.is_empty() {
            return Err(format!("{} bytes after a bodyless response", rest.len()));
        }
        Vec::new()
    } else {
        match (chunked, content_length) {
            (true, Some(_)) => return Err("both chunked and content-length".to_string()),
            (true, None) => decode_chunked(rest)?,
            (false, Some(declared)) => {
                if rest.len() != declared {
                    return Err(format!(
                        "content-length declared {declared} bytes, received {}",
                        rest.len()
                    ));
                }
                rest.to_vec()
            }
            (false, None) => {
                return Err("neither content-length nor chunked: body length unprovable".into())
            }
        }
    };
    Ok(Reply { status, etag, body })
}

/// Decode a chunked body that must end exactly at the final CRLF: every chunk-size line
/// hexadecimal (no sign, no blank), every chunk's data whole and followed by CRLF, the zero-size
/// last chunk present, then the trailer section and its final CRLF, then nothing. The same rules
/// as `multipart_gateway_seam.rs`'s decoder; a test crate has no shared module to put it in.
fn decode_chunked(mut rest: &[u8]) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let line_end = find(rest, b"\r\n").ok_or("chunk-size line without CRLF (truncated)")?;
        let line =
            std::str::from_utf8(&rest[..line_end]).map_err(|e| format!("chunk size: {e}"))?;
        let size_hex = line.split(';').next().unwrap_or_default();
        if size_hex.is_empty() || !size_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("bad chunk-size line `{line}`"));
        }
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|e| format!("chunk-size line `{line}`: {e}"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        let framed = size
            .checked_add(2)
            .filter(|&framed| rest.len() >= framed)
            .ok_or_else(|| format!("chunk of {size} bytes truncated"))?;
        if &rest[size..framed] != b"\r\n" {
            return Err("chunk data not followed by CRLF".to_string());
        }
        body.extend_from_slice(&rest[..size]);
        rest = &rest[framed..];
    }
    // Trailer section: zero or more `name: value` fields, then the final CRLF.
    loop {
        let line_end =
            find(rest, b"\r\n").ok_or("chunked body lacks its final CRLF (truncated)")?;
        if line_end == 0 {
            rest = &rest[2..];
            break;
        }
        if !rest[..line_end].contains(&b':') {
            return Err(format!(
                "trailer line `{}` has no colon",
                String::from_utf8_lossy(&rest[..line_end])
            ));
        }
        rest = &rest[line_end + 2..];
    }
    if !rest.is_empty() {
        return Err(format!("{} bytes after the chunked body", rest.len()));
    }
    Ok(body)
}

// ---- the harness's own guarantees --------------------------------------------------------------

/// The parser the legs read every answer through refuses a torn response, so a body that happens
/// to hold the expected XML cannot pass on incomplete framing.
#[test]
fn the_response_parser_refuses_torn_framing() {
    const CHUNKED: &str = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
    let chunked = |body: &str| format!("{CHUNKED}{body}");

    // Whole framing parses, and de-frames the body.
    for (whole, body) in [
        (chunked("1\r\nx\r\n0\r\n\r\n"), &b"x"[..]),
        (chunked("1;ext=1\r\nx\r\n0\r\ntrailer: v\r\n\r\n"), b"x"),
        (chunked("0\r\n\r\n"), b""),
        (
            "HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\nx".to_string(),
            b"x",
        ),
        ("HTTP/1.1 204 No Content\r\n\r\n".to_string(), b""),
    ] {
        let reply = parse_response(whole.as_bytes(), false)
            .unwrap_or_else(|e| panic!("{whole:?} is whole: {e}"));
        assert_eq!(reply.body, body, "{whole:?}");
    }
    let head = parse_response(
        b"HTTP/1.1 501 Not Implemented\r\ncontent-length: 9\r\n\r\n",
        true,
    );
    assert_eq!(head.expect("a HEAD has no body").status, 501);

    // Torn or contradictory framing does not.
    for torn in [
        chunked("1\r\nx\r\n"),                      // no last chunk
        chunked("1\r\nx\r\n0\r\n"),                 // no final CRLF
        chunked("1\r\nx\r\n0\r\ntrailer: v\r\n"),   // a trailer with no final CRLF
        chunked("1\r\nx\r\n0\r\nno-colon\r\n\r\n"), // a trailer that is not a field
        chunked("1\r\nx\r\n0\r\n\r\nextra"),        // bytes after the final CRLF
        chunked("2\r\nx"),                          // a short chunk
        chunked("1\r\nxy0\r\n\r\n"),                // chunk data without its CRLF
        chunked("+1\r\nx\r\n0\r\n\r\n"),            // a signed chunk size
        chunked("\r\n"),                            // a blank chunk size
        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nx".to_string(),
        "HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\nxy".to_string(),
        "HTTP/1.1 200 OK\r\ncontent-length: +1\r\n\r\nx".to_string(),
        "HTTP/1.1 200 OK\r\ncontent-length: 1\r\ncontent-length: 1\r\n\r\nx".to_string(),
        "HTTP/1.1 200 OK\r\ncontent-length: 5\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n"
            .to_string(),
        "HTTP/1.1 200 OK\r\ntransfer-encoding: gzip\r\n\r\nx".to_string(),
        "HTTP/1.1 200 OK\r\n\r\nno framing at all".to_string(),
        "HTTP/1.1 204 No Content\r\n\r\nx".to_string(),
        "HTTP/1.1 200 OK\r\ncontent-length: 1".to_string(),
        "HTTP/9.9 200 OK\r\ncontent-length: 0\r\n\r\n".to_string(),
        "HTTP/1.1 2000 OK\r\ncontent-length: 0\r\n\r\n".to_string(),
    ] {
        assert!(
            parse_response(torn.as_bytes(), false).is_err(),
            "{torn:?} must not parse"
        );
    }
    assert!(
        parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\nx", true).is_err(),
        "a HEAD response carrying body bytes must not parse"
    );
}

/// Dropping the fixture stops the server it started. The serving task and what it runs hold
/// every strong handle to the backend, and the router holding those handles is what the accept
/// loop serves, so the backend going away shows the task and its listener went too.
#[tokio::test]
async fn dropping_the_fixture_stops_its_server() {
    let served = start().await;
    let backend = served.backend.clone();
    assert!(
        backend.strong_count() > 0,
        "the server is holding its backend"
    );
    drop(served);
    tokio::time::timeout(HTTP_TIMEOUT, async {
        while backend.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the server stops once its fixture is dropped");
}

// ---- 1. percent-encoded spellings ------------------------------------------------------------

/// A key is the key it signs as. On the base each of these missed the raw match: the PUT
/// overwrote `k` (`200`), the DELETE deleted it (`204`), and the POST fell to the method table
/// (`405`).
#[tokio::test]
async fn percent_encoded_spellings_classify_as_their_decoded_key() {
    let served = start().await;
    run(
        &served,
        &[
            // `partNumber` without `uploadId`: an ill-formed UploadPart, see leg 2.
            case("PUT", K, "part%4Eumber=1", 400, INVALID_ARGUMENT),
            case(
                "DELETE",
                K,
                format!("upload%49d={WELL_FORMED_ID}"),
                501,
                NOT_IMPLEMENTED,
            )
            .naming("AbortMultipartUpload"),
            case("POST", K, "upload%73", 501, NOT_IMPLEMENTED).naming("CreateMultipartUpload"),
        ],
    )
    .await;
}

// ---- 2. ill-formed multipart shapes ------------------------------------------------------------

/// A multipart key in a shape no S3 operation takes answers `400 InvalidArgument`: never a
/// `2xx`, never the plain verb, never a listing.
#[tokio::test]
async fn ill_formed_multipart_shapes_answer_400_invalid_argument() {
    let served = start().await;
    let mut cases = vec![
        // A key missing, or the wrong key, for an UploadPart.
        case("PUT", K, "partNumber=1", 400, INVALID_ARGUMENT),
        case("PUT", K, "uploadId=U", 400, INVALID_ARGUMENT),
        case("PUT", K, "uploads", 400, INVALID_ARGUMENT),
    ];
    // A part number that is not canonical decimal digits: a word, a sign (raw and encoded), a
    // leading zero, empty (with and without `=`), a fraction.
    for part in ["abc", "+1", "%2B1", "01", "", "1.0"] {
        cases.push(case(
            "PUT",
            K,
            format!("partNumber={part}&uploadId=U"),
            400,
            INVALID_ARGUMENT,
        ));
    }
    cases.extend([
        case("PUT", K, "partNumber&uploadId=U", 400, INVALID_ARGUMENT),
        // A multipart key repeated is refused, not first-wins: each row would be a well-formed
        // operation if only its first occurrence counted.
        case("PUT", K, "uploadId=U&uploadId=V", 400, INVALID_ARGUMENT),
        case(
            "PUT",
            K,
            "partNumber=1&uploadId=U&uploadId=V",
            400,
            INVALID_ARGUMENT,
        ),
        case("DELETE", K, "uploadId=U&uploadId=V", 400, INVALID_ARGUMENT),
        case("POST", K, "uploadId=U&uploadId=V", 400, INVALID_ARGUMENT),
        case(
            "DELETE",
            K,
            "uploadId=U&upload%49d=V",
            400,
            INVALID_ARGUMENT,
        ),
        case(
            "PUT",
            K,
            "partNumber=1&partNumber=2&uploadId=U",
            400,
            INVALID_ARGUMENT,
        ),
        // On the bucket route: neither key belongs to a bucket-scoped operation.
        case("GET", "/b", "uploadId=U", 400, INVALID_ARGUMENT),
        case("GET", "/b", "partNumber=1", 400, INVALID_ARGUMENT),
    ]);
    run(&served, &cases).await;
}

// ---- 3. well-formed multipart shapes ---------------------------------------------------------

/// Each of the six operations, well-formed, answers `501 NotImplemented` naming it — unchanged
/// on the wire until #668–#670 serve them. The DELETE rows are the point: a DELETE carrying an
/// upload id, in any spelling and whatever the id, never reaches the object DELETE.
#[tokio::test]
async fn well_formed_multipart_shapes_answer_501_naming_the_operation() {
    let served = start().await;
    let mut cases = vec![
        case("POST", K, "uploads", 501, NOT_IMPLEMENTED).naming("CreateMultipartUpload"),
        case("PUT", K, "partNumber=1&uploadId=U", 501, NOT_IMPLEMENTED).naming("UploadPart"),
        case("POST", K, "uploadId=U", 501, NOT_IMPLEMENTED).naming("CompleteMultipartUpload"),
        case("GET", K, "uploadId=U", 501, NOT_IMPLEMENTED).naming("ListParts"),
        case("GET", "/b", "uploads", 501, NOT_IMPLEMENTED).naming("ListMultipartUploads"),
    ];
    for id in [WELL_FORMED_ID, UNKNOWN_ID, MALFORMED_ID] {
        for key in ["uploadId", "%75ploadId"] {
            cases.push(
                case("DELETE", K, format!("{key}={id}"), 501, NOT_IMPLEMENTED)
                    .naming("AbortMultipartUpload"),
            );
        }
    }
    run(&served, &cases).await;
}

// ---- 4. valid forms that are not multipart -----------------------------------------------------

/// `partNumber` on a GET or HEAD is a part-ranged GetObject / HeadObject, a valid S3 form that is
/// not multipart and that this gateway does not implement: `501`, not `400`. An ordinary GET, a
/// PUT carrying the `x-id` an SDK adds, and a ListObjectsV2 are served as before, and so is a PUT
/// whose query key only spells a multipart key once decoded twice.
#[tokio::test]
async fn valid_non_multipart_forms_are_unaffected() {
    let served = start().await;
    run(
        &served,
        &[
            case("GET", K, "partNumber=1", 501, NOT_IMPLEMENTED),
            case("HEAD", K, "partNumber=1", 501, NOT_IMPLEMENTED),
        ],
    )
    .await;

    let get = send(served.addr, "GET", K, "", b"").await;
    assert_eq!(get.status, 200, "ordinary GET {K}: {}", get.text());
    assert_eq!(get.body, K_BYTES, "ordinary GET {K} returns k's bytes");
    assert_intact(&served, "the ordinary GET").await;

    let plain = "/b/plain";
    let plain_bytes: &[u8] = b"stored by a PUT carrying x-id=PutObject";
    let put = send(served.addr, "PUT", plain, "x-id=PutObject", plain_bytes).await;
    assert_eq!(
        put.status,
        200,
        "PUT {plain}?x-id=PutObject: {}",
        put.text()
    );
    let back = send(served.addr, "GET", plain, "", b"").await;
    assert_eq!(back.status, 200, "GET {plain}: {}", back.text());
    assert_eq!(back.body, plain_bytes, "the x-id PUT stored its body");
    assert_intact(&served, "the x-id PUT").await;

    // A key is decoded once, as SigV4 decodes it: `part%254Eumber` signs as the literal key
    // `part%4Eumber`, which is no multipart key, so this is a plain PUT. Decoding twice would
    // read it as `partNumber` and refuse it.
    let once = "/b/decoded-once";
    let put = send(served.addr, "PUT", once, "part%254Eumber=1", plain_bytes).await;
    assert_eq!(
        put.status,
        200,
        "PUT {once}?part%254Eumber=1: {}",
        put.text()
    );
    let back = send(served.addr, "GET", once, "", b"").await;
    assert_eq!(
        back.body, plain_bytes,
        "the once-decoded PUT stored its body"
    );
    assert_intact(&served, "the once-decoded PUT").await;

    let list = send(served.addr, "GET", "/b", "list-type=2", b"").await;
    let listing = list.text();
    assert_eq!(list.status, 200, "ListObjectsV2: {listing}");
    assert!(
        listing.contains("<ListBucketResult") && listing.contains("<Key>k</Key>"),
        "ListObjectsV2 lists k: {listing}"
    );
    assert_intact(&served, "the ListObjectsV2").await;
}

// ---- 5. a denylisted subresource wins ------------------------------------------------------------

/// A subresource still on the denylist wins over a multipart shape beside it, in any spelling.
#[tokio::test]
async fn a_denylisted_subresource_wins_in_any_spelling() {
    let served = start().await;
    run(
        &served,
        &[
            case(
                "PUT",
                K,
                "partNumber=1&uploadId=U&t%61gging=1",
                501,
                NOT_IMPLEMENTED,
            )
            .naming("`tagging`"),
            case("GET", "/b", "uploads&%61cl", 501, NOT_IMPLEMENTED).naming("`acl`"),
        ],
    )
    .await;
}

// ---- 6. a bucket alias on a multipart route ------------------------------------------------------

/// The path is split on its first LITERAL `/` and only then decoded, so `/b%2Fa/k` names the same
/// flat object as `/b/a/k`. No S3 bucket name contains `/`, so every multipart route refuses the
/// alias `400 InvalidBucketName`, the answer the object PUT gives it. The object the alias names
/// is seeded too, so a route that served it would show.
#[tokio::test]
async fn a_bucket_alias_is_refused_on_every_multipart_route() {
    const ALIAS: &str = "/b%2Fa/k";
    let mut served = start().await;
    served
        .seed("/b/a/k", b"b/a/k: the object the alias names")
        .await;
    run(
        &served,
        &[
            case("POST", ALIAS, "uploads", 400, INVALID_BUCKET_NAME),
            case(
                "PUT",
                ALIAS,
                "partNumber=1&uploadId=U",
                400,
                INVALID_BUCKET_NAME,
            ),
            case("POST", ALIAS, "uploadId=U", 400, INVALID_BUCKET_NAME),
            case("DELETE", ALIAS, "uploadId=U", 400, INVALID_BUCKET_NAME),
            case("GET", ALIAS, "uploadId=U", 400, INVALID_BUCKET_NAME),
            // The bucket-scoped multipart route, ListMultipartUploads, refuses it the same way.
            case("GET", "/b%2Fa", "uploads", 400, INVALID_BUCKET_NAME),
        ],
    )
    .await;
}

// ---- 7. bulk delete -------------------------------------------------------------------------------

/// A well-formed `<Delete>` body naming `k` and `k2`.
fn delete_body() -> Vec<u8> {
    b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <Delete><Object><Key>k</Key></Object><Object><Key>k2</Key></Object></Delete>"
        .to_vec()
}

/// `POST /b?delete` is intercepted before the bucket route's classifier, so it must refuse a
/// multipart key itself: `501` naming the key, and neither object the body names is deleted. A
/// regression guard: the base refuses these through the denylist the multipart keys leave.
#[tokio::test]
async fn bulk_delete_keeps_refusing_a_multipart_selector() {
    let mut served = start().await;
    served.seed(K2, K2_BYTES).await;
    let delete = delete_body();
    run(
        &served,
        &[
            case("POST", "/b", "delete&uploads", 501, NOT_IMPLEMENTED)
                .naming("`uploads`")
                .with_body(delete.clone()),
            case("POST", "/b", "delete&uploadId=U", 501, NOT_IMPLEMENTED)
                .naming("`uploadId`")
                .with_body(delete.clone()),
            case("POST", "/b", "delete&part%4Eumber=1", 501, NOT_IMPLEMENTED)
                .naming("`partNumber`")
                .with_body(delete.clone()),
        ],
    )
    .await;

    // The control, last because it deletes: the same body without a multipart key IS a live bulk
    // delete of both objects, so each refusal above withheld a real deletion.
    let control = send(served.addr, "POST", "/b", "delete", &delete).await;
    assert_eq!(control.status, 200, "POST /b?delete: {}", control.text());
    for path in [K, K2] {
        let head = send(served.addr, "HEAD", path, "", b"").await;
        assert_eq!(
            head.status, 404,
            "{path} after the control bulk delete: the fixture body must delete it"
        );
    }
}

/// `POST /b?d%65lete` signs as `delete`, so it IS the bulk delete `POST /b?delete` is (leg 7's
/// control): `200`, and both objects the body names are gone. Without this the marker would be
/// the one key of the implemented forms still matched raw, and a form the gateway implements
/// would be unreachable in a spelling the client signed.
#[tokio::test]
async fn an_encoded_bulk_delete_marker_runs_the_bulk_delete() {
    let mut served = start().await;
    served.seed(K2, K2_BYTES).await;
    let reply = send(served.addr, "POST", "/b", "d%65lete", &delete_body()).await;
    assert_eq!(reply.status, 200, "POST /b?d%65lete: {}", reply.text());
    assert!(
        reply.text().contains("<DeleteResult"),
        "POST /b?d%65lete must answer a <DeleteResult>: {}",
        reply.text()
    );
    for path in [K, K2] {
        let head = send(served.addr, "HEAD", path, "", b"").await;
        assert_eq!(
            head.status, 404,
            "{path} after POST /b?d%65lete: the bulk delete must remove it"
        );
    }
}

/// The encoded marker opens the bulk route and nothing else: a refused key beside it is still
/// refused by name, and off that route (another method, an object path, or a second encoding,
/// which signs as another key) it deletes nothing.
#[tokio::test]
async fn an_encoded_bulk_delete_marker_keeps_every_refusal() {
    let mut served = start().await;
    served.seed(K2, K2_BYTES).await;
    let delete = delete_body();
    run(
        &served,
        &[
            case("POST", "/b", "d%65lete&uploads", 501, NOT_IMPLEMENTED)
                .naming("`uploads`")
                .with_body(delete.clone()),
            case("POST", "/b", "d%65lete&upload%49d=U", 501, NOT_IMPLEMENTED)
                .naming("`uploadId`")
                .with_body(delete.clone()),
            case(
                "POST",
                "/b",
                "d%65lete&part%4Eumber=1",
                501,
                NOT_IMPLEMENTED,
            )
            .naming("`partNumber`")
            .with_body(delete.clone()),
            case("POST", "/b", "d%65lete&version%49d=v", 501, NOT_IMPLEMENTED)
                .naming("`versionId`")
                .with_body(delete.clone()),
            case("POST", "/b", "d%2565lete", 400, "InvalidRequest").with_body(delete.clone()),
            case("POST", K, "d%65lete", 501, NOT_IMPLEMENTED)
                .naming("`delete`")
                .with_body(delete.clone()),
            case("DELETE", K, "d%65lete", 501, NOT_IMPLEMENTED).naming("`delete`"),
            case("GET", "/b", "d%65lete", 501, NOT_IMPLEMENTED).naming("`delete`"),
        ],
    )
    .await;
}
