//! `wyrd s3 --chunk-size N` (issue #738), driven through the BUILT `wyrd` binary.
//!
//! Before this slice the `s3` role parsed no chunk size: `ParsedArgs` stored any unknown
//! `--flag value` without complaint, so `wyrd s3 --chunk-size 65536` started, ignored the
//! flag and chunked at the gateway's 1 MiB default, and `--chunk-size 0` started too. The
//! four legs below pin the end result an operator sees:
//!
//! * **(A)** the flag sets the chunk size on the local-FS plane, and leaving it out keeps
//!   1 MiB (a 2 MiB object makes 4 chunks at 512 KiB, 2 chunks with no flag);
//! * **(B)** the accepted range is exactly `1..=16777216`: both ends start serving, while
//!   `0`, `16777217` and `1MiB` exit non-zero naming `--chunk-size`, never binding;
//! * **(C)** the 16 MiB ceiling crosses today's cluster transport: a role fanning out to a
//!   production `DServer` (gRPC limits untouched) stores a 16 MiB object as ONE chunk and
//!   reads it back byte-equal;
//! * **(D)** the usage text lists the flag.
//!
//! Every role is a child process pinned to `--metadata-backend redb --coordination-backend
//! mem` and `--s3-listen 127.0.0.1:0`; its port is parsed from the role's own listen line.
//! The role serves forever, so every child is killed on every exit path (a `Drop` guard)
//! and every wait is bounded: a pre-fix role that wrongly starts is a red, never a hang.
//! Chunks are counted as `<32-hex>` directories holding `.frag` files, the `FsChunkStore`
//! layout. The test names only the binary and APIs that existed before the flag, so on a
//! tree without the fix it compiles and fails on its assertions.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates against a live child gateway use
// real wall time, the clock the gateway's freshness check reads; nothing here mixes clock
// sources within one asserted lifecycle (#619). File scope is deliberate: a test crate
// never ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_server::dserver::{DServer, DSERVER_GROUP};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// The role's listen line (`cmd_s3`), printed only once the listener is bound.
const LISTEN_MARKER: &str = "wyrd s3: serving S3-compatible HTTP on ";
/// How long a child may take to either print its listen line or exit.
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(120);
/// How long one HTTP exchange (connect, send, read to close) may take.
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

const MIB: usize = 1 << 20;

// ─── child processes ────────────────────────────────────────────────────────────────

/// A spawned `wyrd` child. Dropping it kills and reaps the process, so a panicking
/// assertion never leaves a serving role behind.
struct WyrdProcess {
    child: Child,
    stderr: Receiver<String>,
    transcript: Vec<String>,
}

impl WyrdProcess {
    fn spawn(args: &[&str]) -> Self {
        let mut child = Command::new(WYRD)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the wyrd binary");
        let stderr = child.stderr.take().expect("piped stderr");
        let (tx, rx) = mpsc::channel();
        // Drain stderr to EOF on a helper thread, so the child never blocks on a full pipe
        // and the test can wait on lines with a timeout. EOF comes when the child exits
        // (or is killed), which ends the thread.
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                // The receiver may be gone once the test has what it needs; keep draining.
                let _ = tx.send(line);
            }
        });
        Self {
            child,
            stderr: rx,
            transcript: Vec::new(),
        }
    }

    /// Wait (bounded) until the child either prints its listen line or exits.
    fn launch(mut self) -> Launch {
        let deadline = Instant::now() + LAUNCH_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.stderr.recv_timeout(remaining) {
                Ok(line) => {
                    let listening = line.find(LISTEN_MARKER).map(|at| {
                        let rest = &line[at + LISTEN_MARKER.len()..];
                        let addr = rest.split_whitespace().next().unwrap_or_default();
                        addr.parse::<SocketAddr>()
                            .unwrap_or_else(|e| panic!("listen line `{line}`: bad address: {e}"))
                    });
                    self.transcript.push(line);
                    if let Some(addr) = listening {
                        return Launch::Serving { role: self, addr };
                    }
                }
                // stderr closed: the child is exiting. Reap it, still bounded.
                Err(RecvTimeoutError::Disconnected) => {
                    let status = self.wait_for_exit(deadline);
                    return Launch::Exited {
                        status,
                        stderr: self.transcript.join("\n"),
                    };
                }
                Err(RecvTimeoutError::Timeout) => panic!(
                    "the child neither served nor exited within {LAUNCH_TIMEOUT:?}; stderr so far:\n{}",
                    self.transcript.join("\n")
                ),
            }
        }
    }

    /// Run to completion (bounded), returning the exit status and all of stderr.
    fn run_to_exit(mut self) -> (ExitStatus, String) {
        let deadline = Instant::now() + LAUNCH_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.stderr.recv_timeout(remaining) {
                Ok(line) => self.transcript.push(line),
                Err(RecvTimeoutError::Disconnected) => {
                    let status = self.wait_for_exit(deadline);
                    return (status, self.transcript.join("\n"));
                }
                Err(RecvTimeoutError::Timeout) => panic!(
                    "the child did not exit within {LAUNCH_TIMEOUT:?}; stderr so far:\n{}",
                    self.transcript.join("\n")
                ),
            }
        }
    }

    fn wait_for_exit(&mut self, deadline: Instant) -> ExitStatus {
        loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "the child closed stderr but did not exit in time; stderr:\n{}",
                self.transcript.join("\n")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for WyrdProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

enum Launch {
    /// The role bound its listener and is serving on `addr`. Dropping `role` kills it.
    Serving {
        role: WyrdProcess,
        addr: SocketAddr,
    },
    Exited {
        status: ExitStatus,
        stderr: String,
    },
}

/// The pinned `wyrd s3` invocation every leg uses, plus the leg's own extra flags.
fn s3_args<'a>(data_dir: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec![
        "s3",
        "--access-key",
        ACCESS_KEY,
        "--secret-key",
        SECRET_KEY,
        "--region",
        REGION,
        "--s3-listen",
        "127.0.0.1:0",
        "--data-dir",
        data_dir,
        "--metadata-backend",
        "redb",
        "--coordination-backend",
        "mem",
    ];
    args.extend_from_slice(extra);
    args
}

/// Start a role that MUST come up serving; panic (killing the child) otherwise.
fn serve(data_dir: &Path, extra: &[&str]) -> (WyrdProcess, SocketAddr) {
    let data_dir = data_dir.to_str().expect("utf-8 path");
    match WyrdProcess::spawn(&s3_args(data_dir, extra)).launch() {
        Launch::Serving { role, addr } => (role, addr),
        Launch::Exited { status, stderr } => {
            panic!("`wyrd s3 {extra:?}` must start serving, but exited {status}:\n{stderr}")
        }
    }
}

// ─── HTTP over a raw socket, parsed strictly ────────────────────────────────────────

struct Response {
    status: u16,
    body: Vec<u8>,
}

/// Send one SigV4-signed request over a fresh connection and read the response to close.
fn signed_request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Response {
    let host = addr.to_string();
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    // Fresh, so the request is inside the gateway's freshness window.
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, "", &host, &amz_date, body, &creds, REGION, "s3",
    );
    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         host: {host}\r\n\
         authorization: {}\r\n\
         x-amz-date: {}\r\n\
         x-amz-content-sha256: {}\r\n\
         content-length: {}\r\n\
         connection: close\r\n\r\n",
        signed.authorization,
        signed.amz_date,
        signed.content_sha256,
        body.len()
    );

    let deadline = Instant::now() + HTTP_TIMEOUT;
    let mut stream = TcpStream::connect_timeout(&addr, HTTP_TIMEOUT).expect("connect");
    stream
        .set_read_timeout(Some(HTTP_TIMEOUT))
        .expect("timeout");
    stream
        .set_write_timeout(Some(HTTP_TIMEOUT))
        .expect("timeout");
    // A write error is kept, not raised: a server that answers early and closes still
    // sent a response worth reporting.
    let sent = stream
        .write_all(head.as_bytes())
        .and_then(|()| stream.write_all(body));

    let mut raw = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        assert!(
            Instant::now() < deadline,
            "{method} {path}: no complete response within {HTTP_TIMEOUT:?}"
        );
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("{method} {path}: reading the response failed: {e} (send: {sent:?})"),
        }
    }
    parse_response(&raw)
        .unwrap_or_else(|e| panic!("{method} {path}: malformed response: {e} (send: {sent:?})"))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse an HTTP/1.1 response, refusing any body whose length is not proven: a declared
/// `content-length` must equal the bytes received, a chunked body must carry every
/// delimiter including the final CRLF, and a body framed by neither is an error, because a
/// truncation would read as a short body instead of a failure.
fn parse_response(raw: &[u8]) -> Result<Response, String> {
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
            if !value.eq_ignore_ascii_case("chunked") {
                return Err(format!("unsupported transfer-encoding `{value}`"));
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
        let size = usize::from_str_radix(size_hex, 16).map_err(|e| format!("chunk size: {e}"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        if rest.len() < size + 2 {
            return Err(format!(
                "chunk of {size} bytes truncated: {} bytes left",
                rest.len()
            ));
        }
        body.extend_from_slice(&rest[..size]);
        if &rest[size..size + 2] != b"\r\n" {
            return Err("chunk data not followed by CRLF".to_string());
        }
        rest = &rest[size + 2..];
    }
    // Trailer section: zero or more fields, then the final CRLF.
    loop {
        let line_end =
            find(rest, b"\r\n").ok_or("chunked body lacks its final CRLF (truncated)")?;
        let empty = line_end == 0;
        rest = &rest[line_end + 2..];
        if empty {
            break;
        }
    }
    if !rest.is_empty() {
        return Err(format!("{} bytes after the chunked body", rest.len()));
    }
    Ok(body)
}

// ─── object bytes and chunk counting ────────────────────────────────────────────────

/// `len` deterministic, non-repeating bytes (xorshift), so a misplaced or dropped chunk
/// cannot read back equal by accident.
fn object(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// Count chunk directories under an `FsChunkStore` root: a `<32-hex>` directory holding at
/// least one `.frag` file (`root/<32-hex chunk>/<05-index>.frag`).
fn count_chunk_dirs(root: &Path) -> usize {
    let entries = std::fs::read_dir(root)
        .unwrap_or_else(|e| panic!("read chunk store root {}: {e}", root.display()));
    entries
        .map(|entry| entry.expect("read a chunk store entry").path())
        .filter(|path| {
            let name_is_chunk_id = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.len() == 32 && n.bytes().all(|b| b.is_ascii_hexdigit()));
            name_is_chunk_id
                && path.is_dir()
                && std::fs::read_dir(path).is_ok_and(|frags| {
                    frags
                        .flatten()
                        .any(|f| f.path().extension().and_then(|e| e.to_str()) == Some("frag"))
                })
        })
        .count()
}

// ─── (A) the flag sets the chunk size; its absence keeps 1 MiB ──────────────────────

#[test]
fn a_chunk_size_flag_sets_the_local_chunk_size_and_its_absence_keeps_one_mib() {
    let data = object(2 * MIB, 0xA738);
    let path = "/wyrd-bucket/two-mib-object";

    // --chunk-size 524288: a 2 MiB object is FOUR chunks.
    let flagged = tempfile::tempdir().expect("temp dir");
    {
        let (_role, addr) = serve(flagged.path(), &["--chunk-size", "524288"]);
        let put = signed_request(addr, "PUT", path, &data);
        assert_eq!(
            put.status,
            200,
            "PUT with --chunk-size 524288: {:?}",
            String::from_utf8_lossy(&put.body)
        );
        let get = signed_request(addr, "GET", path, b"");
        assert_eq!(get.status, 200, "GET with --chunk-size 524288");
        assert!(
            get.body == data,
            "GET body differs from the PUT body ({} bytes read)",
            get.body.len()
        );
    }
    assert_eq!(
        count_chunk_dirs(&flagged.path().join("chunks")),
        4,
        "--chunk-size 524288 must split a 2 MiB object into 4 chunks"
    );

    // No --chunk-size: the gateway's 1 MiB default, so the same object is TWO chunks.
    let default = tempfile::tempdir().expect("temp dir");
    {
        let (_role, addr) = serve(default.path(), &[]);
        let put = signed_request(addr, "PUT", path, &data);
        assert_eq!(
            put.status,
            200,
            "PUT with no --chunk-size: {:?}",
            String::from_utf8_lossy(&put.body)
        );
    }
    assert_eq!(
        count_chunk_dirs(&default.path().join("chunks")),
        2,
        "with no --chunk-size the role must keep 1 MiB chunks"
    );
}

// ─── (B) exact accept/refuse boundaries ─────────────────────────────────────────────

#[test]
fn b_chunk_size_accepts_exactly_one_to_sixteen_mib_and_refuses_the_rest_before_binding() {
    for accepted in ["1", "16777216"] {
        let dir = tempfile::tempdir().expect("temp dir");
        // `serve` panics unless the role prints its listen line.
        let (_role, _addr) = serve(dir.path(), &["--chunk-size", accepted]);
    }

    for refused in ["0", "16777217", "1MiB"] {
        let dir = tempfile::tempdir().expect("temp dir");
        let data_dir = dir.path().to_str().expect("utf-8 path");
        match WyrdProcess::spawn(&s3_args(data_dir, &["--chunk-size", refused])).launch() {
            Launch::Serving { role, addr } => {
                drop(role);
                panic!(
                    "`--chunk-size {refused}` must be refused, but the role is serving on {addr}"
                );
            }
            Launch::Exited { status, stderr } => {
                assert!(
                    !status.success(),
                    "`--chunk-size {refused}` must exit non-zero, got {status}:\n{stderr}"
                );
                assert!(
                    stderr.contains("--chunk-size"),
                    "`--chunk-size {refused}`: stderr must name the flag:\n{stderr}"
                );
                assert!(
                    !stderr.contains(LISTEN_MARKER),
                    "`--chunk-size {refused}`: the listener must never bind:\n{stderr}"
                );
            }
        }
    }
}

// ─── (C) the ceiling fits today's cluster transport ─────────────────────────────────

/// One production D server, in process, over an `FsChunkStore` in a temp dir. It is served
/// by the production `DServer::serve`, the same path `wyrd d-server` runs, so its gRPC
/// message limits are the ones a deployed D server has. Dropping it signals shutdown
/// and stops its runtime, both bounded.
struct InProcessDServer {
    runtime: Option<tokio::runtime::Runtime>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    served: Option<tokio::task::JoinHandle<wyrd_traits::Result<()>>>,
    endpoint: String,
    store: tempfile::TempDir,
}

impl InProcessDServer {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let store = tempfile::tempdir().expect("temp dir");
        let chunks = FsChunkStore::open(store.path()).expect("open the D server's store");
        let (shutdown, stop) = tokio::sync::oneshot::channel::<()>();
        let (endpoint, served) = runtime.block_on(async {
            let coord = Arc::new(MemCoordination::new());
            let server = DServer::bind(chunks, "127.0.0.1:0".parse().expect("addr"))
                .await
                .expect("bind the D server");
            let endpoint = server.endpoint().to_string();
            let lease = server
                .register(&*coord, DSERVER_GROUP, Duration::from_secs(600))
                .await
                .expect("register the D server");
            let served =
                tokio::spawn(
                    server.serve(coord, lease, Duration::from_secs(60), async move {
                        let _ = stop.await;
                    }),
                );
            (endpoint, served)
        });
        Self {
            runtime: Some(runtime),
            shutdown: Some(shutdown),
            served: Some(served),
            endpoint,
            store,
        }
    }
}

impl Drop for InProcessDServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(runtime) = self.runtime.take() {
            if let Some(served) = self.served.take() {
                // The timer must be built inside the runtime, so wrap it in the future.
                let _ = runtime.block_on(async move {
                    tokio::time::timeout(Duration::from_secs(10), served).await
                });
            }
            runtime.shutdown_timeout(Duration::from_secs(10));
        }
    }
}

#[test]
fn c_the_sixteen_mib_ceiling_crosses_a_production_dserver_as_one_chunk() {
    let dserver = InProcessDServer::start();
    let gateway_dir = tempfile::tempdir().expect("temp dir");
    let data = object(16 * MIB, 0xC738);
    let path = "/wyrd-bucket/sixteen-mib-object";
    {
        let (_role, addr) = serve(
            gateway_dir.path(),
            &["--endpoints", &dserver.endpoint, "--chunk-size", "16777216"],
        );
        let put = signed_request(addr, "PUT", path, &data);
        assert_eq!(
            put.status,
            200,
            "PUT of a 16 MiB object at --chunk-size 16777216 over gRPC: {:?}",
            String::from_utf8_lossy(&put.body)
        );
        assert_eq!(
            count_chunk_dirs(dserver.store.path()),
            1,
            "a 16 MiB object at --chunk-size 16777216 must be ONE chunk on the D server"
        );
        let get = signed_request(addr, "GET", path, b"");
        assert_eq!(get.status, 200, "GET of the 16 MiB object over gRPC");
        assert_eq!(get.body.len(), data.len(), "GET body length");
        assert!(get.body == data, "GET body differs from the PUT body");
    }
}

// ─── (D) the usage text lists the flag ──────────────────────────────────────────────

#[test]
fn d_the_usage_text_lists_the_chunk_size_flag_on_the_s3_line() {
    let (status, stderr) = WyrdProcess::spawn(&[]).run_to_exit();
    assert_eq!(status.code(), Some(2), "bare `wyrd` exits 2:\n{stderr}");
    let s3_line = stderr
        .lines()
        .find(|line| line.trim_start().starts_with("wyrd s3 "))
        .unwrap_or_else(|| panic!("no `wyrd s3` usage line:\n{stderr}"));
    assert!(
        s3_line.contains("[--chunk-size N]"),
        "the `wyrd s3` usage line must list `[--chunk-size N]`: {s3_line}"
    );
}
