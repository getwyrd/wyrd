//! **The multipart gateway seam** (issue #666, the first slice of #508).
//!
//! `MultipartGateway` is the companion trait that carries the six S3 multipart verbs across
//! the gateway seam in neutral container/object vocabulary (ADR-0046 decision 6, ADR-0010).
//! This slice adds the seam only: the composition root answers every verb "not implemented",
//! the S3 crate requires the trait of its gateway type, and the wire does not change. What is
//! asserted, each separately:
//!
//! 1. [`the_composition_root_answers_every_multipart_verb_not_implemented`] — each of the six
//!    methods, called on a real `wyrd_server::Gateway`, returns `Ok` carrying the seam's typed
//!    `MultipartRefusal::NotImplemented` arm: a value the caller matches on, not a panic and
//!    not an untyped boxed error.
//!    [`the_trait_defaults_answer_every_multipart_verb_not_implemented`] runs the same six
//!    checks against an empty `impl MultipartGateway`, so the trait's default bodies (what the
//!    S3 crate's object-only test doubles rely on) answer the same typed value.
//!    [`an_empty_complete_list_cannot_reach_the_seam`] pins the one input the answer
//!    vocabulary has no arm for: `NamedParts` refuses an empty list.
//! 2. [`the_real_gateway_meets_the_s3_crates_gateway_bound`] — a helper bounded on
//!    `ObjectGateway + ContainerGateway + MultipartGateway` accepts the real `Gateway` and
//!    composes `S3Gateway::new` over it; the served router still answers every multipart form
//!    `501`.
//! 3. [`every_multipart_form_still_answers_501_without_entering_the_seam`] — over the
//!    in-process listener, a sentinel gateway counts every call into its seams. Each multipart
//!    form answers `501 NotImplemented` naming its subresource, and afterwards the multipart
//!    counter is **zero**. The status alone cannot tell "refused by routing" from "entered the
//!    seam, got `NotImplemented`, mapped it to `501`"; the counter can.
//!
//! The wire oracle of legs 2 and 3 ([`judge_501`]) first proves the response is completely
//! framed (`content-length` equal to the bytes received, or every chunked delimiter through
//! the final CRLF), then checks status and XML. [`the_wire_oracle_refuses_torn_framing`]
//! shows a torn response fails at the framing check even when it carries every asserted XML
//! fragment.
//!
//! RED on the base: the file names `MultipartGateway`, which does not exist there, so it does
//! not build. Leg 3's assertions would hold on the base too, on purpose: it pins "routing
//! untouched" (#667 owns routing), and its counter goes red if a multipart form ever reaches
//! the seam, whatever status the request then answers.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates against a live in-process server use
// real wall time; nothing here mixes clock sources within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never ships, so no production
// lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::Stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_core::{
    ContainerGateway, ContentHash, ListedObject, MultipartAbortAnswer, MultipartCompleteAnswer,
    MultipartCreateAnswer, MultipartGateway, MultipartListPartsAnswer, MultipartListUploadsAnswer,
    MultipartRefusal, MultipartUploadPartAnswer, NamedParts, ObjectGateway, ObjectMeta, ObjectRead,
};
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::Result;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// How long one request/response exchange may take before the test fails instead of hanging.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The six S3 multipart wire forms, as `(method, path, query, the subresource the 501 names)`.
/// The named subresource is the FIRST denylisted query key, which is what the S3 crate's guard
/// reports (`crates/gateway-s3/src/lib.rs`, `unsupported_subresource`).
const MULTIPART_FORMS: [(&str, &str, &str, &str); 6] = [
    // CreateMultipartUpload
    ("POST", "/b/k", "uploads", "uploads"),
    // UploadPart
    ("PUT", "/b/k", "partNumber=1&uploadId=u", "partNumber"),
    // CompleteMultipartUpload
    ("POST", "/b/k", "uploadId=u", "uploadId"),
    // AbortMultipartUpload
    ("DELETE", "/b/k", "uploadId=u", "uploadId"),
    // ListParts
    ("GET", "/b/k", "uploadId=u", "uploadId"),
    // ListMultipartUploads (bucket-scoped)
    ("GET", "/b", "uploads", "uploads"),
];

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// The real composition-root gateway, over the in-memory redb store as the peer suites build it
/// (`s3_list_objects.rs`, `request_capacity_planes.rs`).
fn real_gateway(dir: &std::path::Path) -> Backend {
    Gateway::new(
        RedbMetadataStore::in_memory().expect("redb"),
        FsChunkStore::open(dir).expect("fs store"),
        MemCoordination::new(),
    )
}

// ---- leg 1: the composition root answers every verb, typed ---------------------------------

#[tokio::test]
async fn the_composition_root_answers_every_multipart_verb_not_implemented() {
    let dir = tempfile::tempdir().expect("temp dir");
    assert_every_verb_not_implemented(&real_gateway(dir.path())).await;
}

/// A gateway with no multipart concept: the empty `impl` the S3 crate's object-only test
/// doubles rely on, so only the trait's default bodies answer for it.
struct NoMultipart;

impl MultipartGateway for NoMultipart {}

#[tokio::test]
async fn the_trait_defaults_answer_every_multipart_verb_not_implemented() {
    assert_every_verb_not_implemented(&NoMultipart).await;
}

/// The seam's answer vocabulary has no arm for a Complete that names no part (core's
/// `RecordError::NoPartsNamed`). It needs none, because such a list cannot be built: the wire
/// layer meets the `None` while decoding the request and answers it as malformed itself.
#[test]
fn an_empty_complete_list_cannot_reach_the_seam() {
    assert_eq!(NamedParts::new(Vec::new()), None);

    let sent = vec![(2, "b".to_string()), (1, "a".to_string())];
    let named = NamedParts::new(sent.clone()).expect("a non-empty list is accepted");
    assert_eq!(named.as_slice(), sent.as_slice(), "kept in the order sent");
    assert_eq!(named.into_vec(), sent);
}

/// Call all six verbs on `gateway` and assert each returns `Ok` carrying the typed
/// `Refused(NotImplemented)` value. Each call is fully qualified, so it provably reaches `G`'s
/// `MultipartGateway` impl, never a same-named inherent method.
async fn assert_every_verb_not_implemented<G: MultipartGateway>(gateway: &G) {
    let create = MultipartGateway::create_multipart_upload(
        gateway,
        "b",
        "k",
        Some("text/plain".to_string()),
    )
    .await
    .expect("create answers a value, not Err");
    assert_eq!(
        create,
        MultipartCreateAnswer::Refused(MultipartRefusal::NotImplemented),
        "create_multipart_upload",
    );

    let body = futures_util::stream::iter(vec![Ok::<Bytes, wyrd_traits::BoxError>(
        Bytes::from_static(b"part one"),
    )]);
    let upload =
        MultipartGateway::upload_part(gateway, "b", "k", "u", 1, body, ContentHash::Unverified)
            .await
            .expect("upload_part answers a value, not Err");
    assert_eq!(
        upload,
        MultipartUploadPartAnswer::Refused(MultipartRefusal::NotImplemented),
        "upload_part",
    );

    let named = NamedParts::new(vec![(1, "etag-of-part-one".to_string())]).expect("non-empty");
    let complete = MultipartGateway::complete_multipart_upload(gateway, "b", "k", "u", named)
        .await
        .expect("complete answers a value, not Err");
    assert_eq!(
        complete,
        MultipartCompleteAnswer::Refused(MultipartRefusal::NotImplemented),
        "complete_multipart_upload",
    );

    let abort = MultipartGateway::abort_multipart_upload(gateway, "b", "k", "u")
        .await
        .expect("abort answers a value, not Err");
    assert_eq!(
        abort,
        MultipartAbortAnswer::Refused(MultipartRefusal::NotImplemented),
        "abort_multipart_upload",
    );

    let parts = MultipartGateway::list_parts(gateway, "b", "k", "u")
        .await
        .expect("list_parts answers a value, not Err");
    assert_eq!(
        parts,
        MultipartListPartsAnswer::Refused(MultipartRefusal::NotImplemented),
        "list_parts",
    );

    let uploads = MultipartGateway::list_multipart_uploads(gateway, "b")
        .await
        .expect("list_multipart_uploads answers a value, not Err");
    assert_eq!(
        uploads,
        MultipartListUploadsAnswer::Refused(MultipartRefusal::NotImplemented),
        "list_multipart_uploads",
    );
}

// ---- leg 2: the real Gateway satisfies the S3 crate's bound --------------------------------

/// A served S3 front door on a loopback port. Dropping it aborts the serving task, so no
/// server outlives the test that started it.
struct Served {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve `gateway` over an ephemeral loopback port. Bounded on the full triple the S3 crate's
/// `S3Gateway::new` requires, so a gateway missing any one of the three seams does not compile
/// here.
async fn start_s3<G>(gateway: Arc<G>) -> Served
where
    G: ObjectGateway + ContainerGateway + MultipartGateway,
{
    let config = S3Config::new(vec![Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    let task = tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    Served { addr, task }
}

#[tokio::test]
async fn the_real_gateway_meets_the_s3_crates_gateway_bound() {
    let dir = tempfile::tempdir().expect("temp dir");
    let served = start_s3(Arc::new(real_gateway(dir.path()))).await;
    for (method, path, query, subresource) in MULTIPART_FORMS {
        assert_refused_501(served.addr, method, path, query, subresource).await;
    }
}

// ---- leg 3: the wire is unchanged, and the seam is not entered -----------------------------

/// A gateway that records every call into any of its seams. The counters are fields, not
/// statics (ADR-0035). Every method answers what a gateway without the capability would —
/// the multipart verbs `NotImplemented`, the object verbs an error, the listing "no such
/// container" — so a request that DID enter a seam could still come back with a plausible
/// status; only the counters tell.
#[derive(Default)]
struct Sentinel {
    /// Calls into any `MultipartGateway` method.
    multipart_calls: AtomicUsize,
    /// Calls into any `ObjectGateway` or `ContainerGateway` method.
    other_calls: AtomicUsize,
}

impl Sentinel {
    fn multipart(&self) {
        self.multipart_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn other(&self) -> wyrd_traits::BoxError {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
        "the sentinel's object seam is never reached by a multipart form".into()
    }
}

impl MultipartGateway for Sentinel {
    async fn create_multipart_upload(
        &self,
        _container: &str,
        _key: &str,
        _content_type: Option<String>,
    ) -> Result<MultipartCreateAnswer> {
        self.multipart();
        Ok(MultipartCreateAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }

    async fn upload_part<S>(
        &self,
        _container: &str,
        _key: &str,
        _upload_id: &str,
        _part_number: u32,
        _body: S,
        _expected: ContentHash,
    ) -> Result<MultipartUploadPartAnswer>
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        self.multipart();
        Ok(MultipartUploadPartAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }

    async fn complete_multipart_upload(
        &self,
        _container: &str,
        _key: &str,
        _upload_id: &str,
        _named_parts: NamedParts,
    ) -> Result<MultipartCompleteAnswer> {
        self.multipart();
        Ok(MultipartCompleteAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }

    async fn abort_multipart_upload(
        &self,
        _container: &str,
        _key: &str,
        _upload_id: &str,
    ) -> Result<MultipartAbortAnswer> {
        self.multipart();
        Ok(MultipartAbortAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }

    async fn list_parts(
        &self,
        _container: &str,
        _key: &str,
        _upload_id: &str,
    ) -> Result<MultipartListPartsAnswer> {
        self.multipart();
        Ok(MultipartListPartsAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }

    async fn list_multipart_uploads(&self, _container: &str) -> Result<MultipartListUploadsAnswer> {
        self.multipart();
        Ok(MultipartListUploadsAnswer::Refused(
            MultipartRefusal::NotImplemented,
        ))
    }
}

impl ContainerGateway for Sentinel {
    async fn list_container(&self, _container: &str) -> Result<Option<Vec<ListedObject>>> {
        let _ = self.other();
        Ok(None)
    }
}

impl ObjectGateway for Sentinel {
    async fn put_object_streaming<S>(
        &self,
        _key: &str,
        _source: S,
        _expected: ContentHash,
        _content_type: Option<String>,
    ) -> Result<String>
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        Err(self.other())
    }

    async fn get_object_streaming(self: Arc<Self>, _key: &str) -> Result<Option<ObjectRead>> {
        Err(self.other())
    }

    async fn head_object(&self, _key: &str) -> Result<Option<ObjectMeta>> {
        Err(self.other())
    }

    async fn delete_object(&self, _key: &str) -> Result<bool> {
        Err(self.other())
    }
}

#[tokio::test]
async fn every_multipart_form_still_answers_501_without_entering_the_seam() {
    let sentinel = Arc::new(Sentinel::default());
    let served = start_s3(Arc::clone(&sentinel)).await;

    for (method, path, query, subresource) in MULTIPART_FORMS {
        assert_refused_501(served.addr, method, path, query, subresource).await;
    }

    // Each response above was read to the end and proven completely framed, so its handler
    // has returned: any seam call a form made has already been counted.
    assert_eq!(
        sentinel.multipart_calls.load(Ordering::SeqCst),
        0,
        "a multipart form entered the MultipartGateway seam; routing is #667's, not this slice's",
    );
    assert_eq!(
        sentinel.other_calls.load(Ordering::SeqCst),
        0,
        "a multipart form fell through to an object or listing verb",
    );
}

// ---- the wire oracle -----------------------------------------------------------------------

/// Send one signed multipart form and assert it is refused `501 NotImplemented`, the message
/// naming `subresource`, in a completely framed response.
async fn assert_refused_501(
    addr: SocketAddr,
    method: &str,
    path: &str,
    query: &str,
    subresource: &str,
) {
    let raw = send_signed(addr, method, path, query).await;
    if let Err(e) = judge_501(&raw, subresource) {
        panic!(
            "{method} {path}?{query}: {e}\nraw response: {}",
            String::from_utf8_lossy(&raw)
        );
    }
}

/// The whole oracle for one multipart form, over the raw response bytes. Framing first: the
/// bytes must be one complete HTTP/1.1 response ([`parse_response`]), so a torn or truncated
/// response fails here even if it carries every fragment checked below. Then the status must be
/// `501`, the S3 error code `NotImplemented`, and the message must name `subresource`.
fn judge_501(raw: &[u8], subresource: &str) -> std::result::Result<(), String> {
    let response = parse_response(raw).map_err(|e| format!("malformed response: {e}"))?;
    let body = std::str::from_utf8(&response.body).map_err(|e| format!("body not UTF-8: {e}"))?;
    if response.status != 501 {
        return Err(format!("expected 501, got {}", response.status));
    }
    if !body.contains("<Code>NotImplemented</Code>") {
        return Err("expected the NotImplemented code".to_string());
    }
    if !body.contains(&format!("the `{subresource}` S3 subresource")) {
        return Err(format!("expected the 501 to name `{subresource}`"));
    }
    Ok(())
}

/// Send a raw SigV4-signed `{method} {path}?{query}` with an empty body and return every byte
/// the server sent before closing. The query is signed as it rides on the wire (`sign`
/// canonicalizes it), so a valueless key like `uploads` passes authentication and reaches
/// routing. Bounded: a server that never answers or never closes fails the test instead of
/// hanging it.
async fn send_signed(addr: SocketAddr, method: &str, path: &str, query: &str) -> Vec<u8> {
    let host = addr.to_string();
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, query, &host, &amz_date, b"", &creds, REGION, "s3",
    );
    let mut request = format!("{method} {path}?{query} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    request.push_str(&format!("authorization: {}\r\n", signed.authorization));
    request.push_str(&format!("x-amz-date: {}\r\n", signed.amz_date));
    request.push_str(&format!(
        "x-amz-content-sha256: {}\r\n",
        signed.content_sha256
    ));
    request.push_str("content-length: 0\r\n");
    request.push_str("connection: close\r\n\r\n");

    let exchange = async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        stream.flush().await.expect("flush");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        raw
    };
    tokio::time::timeout(HTTP_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| panic!("{method} {path}?{query}: no response within {HTTP_TIMEOUT:?}"))
}

/// A parsed response: the status code and the de-framed body.
struct Response {
    status: u16,
    body: Vec<u8>,
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse one HTTP/1.1 response, refusing any body whose length is not proven: a declared
/// `content-length` must equal the bytes received, a chunked body must carry every delimiter
/// through the final CRLF, and a body framed by neither is an error, because a truncation would
/// read as a short body instead of a failure. The same checks as `s3_chunk_size_flag.rs`'s
/// `parse_response`, plus a refused repeated `transfer-encoding`.
fn parse_response(raw: &[u8]) -> std::result::Result<Response, String> {
    let head_end = find(raw, b"\r\n\r\n").ok_or("no end of the header section")?;
    let head = std::str::from_utf8(&raw[..head_end]).map_err(|e| format!("head: {e}"))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or("empty response")?;
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(format!("status line `{status_line}`: bad version"));
    }
    let code = parts.next().unwrap_or_default();
    if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("status line `{status_line}`: bad status code"));
    }
    let status: u16 = code.parse().map_err(|e| format!("status: {e}"))?;

    let mut content_length = None;
    let mut chunked = false;
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
                    .map_err(|e| format!("content-length: {e}"))?,
            );
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !value.eq_ignore_ascii_case("chunked") {
                return Err(format!(
                    "unsupported or repeated transfer-encoding `{value}`"
                ));
            }
            chunked = true;
        }
    }

    let rest = &raw[head_end + 4..];
    let body = match (chunked, content_length) {
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
            return Err("neither content-length nor chunked: body length unprovable".to_string())
        }
    };
    Ok(Response { status, body })
}

/// Decode a chunked body that must end exactly at the final CRLF: every chunk-size line
/// hexadecimal (no sign, no blank), every chunk's data whole and followed by CRLF, the
/// zero-size last chunk present, then the trailer section and its final CRLF, then nothing.
fn decode_chunked(mut rest: &[u8]) -> std::result::Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let line_end = find(rest, b"\r\n").ok_or("chunk-size line without CRLF (truncated)")?;
        let line =
            std::str::from_utf8(&rest[..line_end]).map_err(|e| format!("chunk size: {e}"))?;
        let size_hex = line.split(';').next().unwrap_or_default();
        if size_hex.is_empty() || !size_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("bad chunk-size line `{line}`"));
        }
        let size = usize::from_str_radix(size_hex, 16).map_err(|e| format!("chunk size: {e}"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        let framed = size
            .checked_add(2)
            .ok_or_else(|| format!("chunk size {size} overflows"))?;
        if rest.len() < framed {
            return Err(format!(
                "chunk of {size} bytes truncated: {} bytes left",
                rest.len()
            ));
        }
        body.extend_from_slice(&rest[..size]);
        if &rest[size..framed] != b"\r\n" {
            return Err("chunk data not followed by CRLF".to_string());
        }
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

// ---- the wire oracle refuses torn framing --------------------------------------------------

/// An S3 501 body carrying every fragment [`judge_501`] looks for, for `uploads`.
const BODY_501: &str =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NotImplemented</Code>\
<Message>the `uploads` S3 subresource/operation is not supported</Message></Error>";

/// [`BODY_501`] split in two after the message text, so the first half alone carries every
/// fragment the oracle checks.
fn body_halves() -> (&'static str, &'static str) {
    BODY_501.split_at(BODY_501.find("</Message>").expect("message end"))
}

/// A response head with `framing` as its framing header line(s).
fn head_501(framing: &str) -> String {
    format!("HTTP/1.1 501 Not Implemented\r\ncontent-type: application/xml\r\n{framing}\r\n")
}

/// [`BODY_501`] with a correct `content-length`.
fn fixed_501() -> Vec<u8> {
    format!(
        "{}{BODY_501}",
        head_501(&format!("content-length: {}\r\n", BODY_501.len()))
    )
    .into_bytes()
}

/// [`BODY_501`] as two chunks ([`body_halves`]), then the last chunk and the final CRLF.
fn chunked_501() -> Vec<u8> {
    let (first, second) = body_halves();
    format!(
        "{}{:x}\r\n{first}\r\n{:x}\r\n{second}\r\n0\r\n\r\n",
        head_501("transfer-encoding: chunked\r\n"),
        first.len(),
        second.len(),
    )
    .into_bytes()
}

/// `raw` with `from`, which must occur exactly once, replaced by `to`.
fn with_replaced(raw: &[u8], from: &str, to: &str) -> Vec<u8> {
    let text = String::from_utf8(raw.to_vec()).expect("fixtures are UTF-8");
    assert_eq!(text.matches(from).count(), 1, "`{from}` must occur once");
    text.replacen(from, to, 1).into_bytes()
}

/// The well-framed fixtures pass the oracle, so each variant below differs from a passing
/// response only in its framing.
#[test]
fn the_wire_oracle_accepts_a_well_framed_501() {
    assert_eq!(judge_501(&fixed_501(), "uploads"), Ok(()));
    assert_eq!(judge_501(&chunked_501(), "uploads"), Ok(()));
    let with_trailer = with_replaced(
        &chunked_501(),
        "\r\n0\r\n\r\n",
        "\r\n0\r\nx-trailer: v\r\n\r\n",
    );
    assert_eq!(judge_501(&with_trailer, "uploads"), Ok(()));
}

/// Every response here carries all the XML the oracle checks and breaks one framing rule: the
/// head's grammar, the declared length, or a chunk delimiter. Every one must fail at the
/// framing check, before the status and XML checks are reached.
#[test]
fn the_wire_oracle_refuses_torn_framing() {
    let fixed = fixed_501();
    let chunked = chunked_501();
    let (first, _) = body_halves();
    let declared = format!("content-length: {}", BODY_501.len());
    let first_size_line = format!("\r\n\r\n{:x}\r\n", first.len());
    let after_first_chunk =
        find(&chunked, b"supported\r\n").expect("first chunk end") + b"supported\r\n".len();
    let before_last_chunk = find(&chunked, b"\r\n0\r\n\r\n").expect("last chunk") + 2;

    let torn: Vec<(&str, Vec<u8>)> = vec![
        (
            "fixed: body shorter than the declared content-length",
            with_replaced(
                &fixed,
                &declared,
                &format!("content-length: {}", BODY_501.len() + 10),
            ),
        ),
        (
            "fixed: body longer than the declared content-length",
            [fixed.as_slice(), b"<extra/>"].concat(),
        ),
        (
            "fixed: content-length repeated",
            with_replaced(&fixed, &declared, &format!("{declared}\r\n{declared}")),
        ),
        (
            "fixed: content-length signed",
            with_replaced(
                &fixed,
                &declared,
                &format!("content-length: +{}", BODY_501.len()),
            ),
        ),
        (
            "neither content-length nor chunked",
            with_replaced(&fixed, &format!("{declared}\r\n"), ""),
        ),
        (
            "both content-length and chunked",
            with_replaced(
                &chunked,
                "transfer-encoding: chunked",
                &format!("transfer-encoding: chunked\r\n{declared}"),
            ),
        ),
        (
            "chunked: transfer-encoding repeated",
            with_replaced(
                &chunked,
                "transfer-encoding: chunked",
                "transfer-encoding: chunked\r\ntransfer-encoding: chunked",
            ),
        ),
        (
            "chunked: EOF after a complete first chunk, no last chunk",
            chunked[..after_first_chunk].to_vec(),
        ),
        (
            "chunked: EOF after every data chunk, no last chunk",
            chunked[..before_last_chunk].to_vec(),
        ),
        (
            "chunked: last chunk without the final CRLF",
            chunked[..chunked.len() - 2].to_vec(),
        ),
        (
            "chunked: bytes after the final CRLF",
            [chunked.as_slice(), b"0\r\n\r\n"].concat(),
        ),
        (
            "chunked: unparsable chunk-size line",
            with_replaced(&chunked, "\r\n0\r\n\r\n", "\r\nzz\r\n\r\n"),
        ),
        (
            "chunked: signed chunk size",
            with_replaced(&chunked, "\r\n0\r\n\r\n", "\r\n+0\r\n\r\n"),
        ),
        (
            "chunked: trailer line without a colon",
            with_replaced(&chunked, "\r\n0\r\n\r\n", "\r\n0\r\nnot-a-field\r\n\r\n"),
        ),
        (
            "chunked: data shorter than its chunk size",
            with_replaced(
                &chunked,
                &first_size_line,
                &format!("\r\n\r\n{:x}\r\n", first.len() + 40),
            ),
        ),
        (
            "chunked: chunk data not followed by CRLF",
            with_replaced(&chunked, "supported\r\n", "supportedxx"),
        ),
        (
            "no CRLF CRLF ending the header section",
            with_replaced(&fixed, "\r\n\r\n", "\n\n"),
        ),
        (
            "bad status code",
            with_replaced(&fixed, "HTTP/1.1 501 ", "HTTP/1.1 5x1 "),
        ),
    ];

    for (case, raw) in torn {
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.contains("<Code>NotImplemented</Code>")
                && text.contains("the `uploads` S3 subresource"),
            "{case}: the variant must still carry every asserted XML fragment",
        );
        match judge_501(&raw, "uploads") {
            Err(e) if e.starts_with("malformed response:") => {}
            other => panic!("{case}: expected a framing failure, got {other:?}\nraw: {text}"),
        }
    }
}
