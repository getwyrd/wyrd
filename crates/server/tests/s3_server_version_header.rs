//! The S3 front door names the build that produced each response (#779): every response
//! the gateway service emits carries `Server: wyrd/<v>`, where `<v>` is the build identity
//! the binary records as the `version` field of its `role started` event (#778).
//!
//! Drives the BUILT binary (`CARGO_BIN_EXE_wyrd`, the `cli_roundtrip.rs` idiom) as a real
//! `s3` role, so the real composition root, the real router and the one stamp point in
//! `handle` are all on the path. It observes only response status, response headers and
//! the child's stderr, and names no symbol the fix introduces, so it compiles against a
//! tree without the fix and fails there on the absent header.
//!
//! One child process, four response categories, each checked for byte equality with the
//! SAME process's logged identity:
//!
//! 1. success — a signed `PUT` answered `200`;
//! 2. client error, refused before auth — an UNSIGNED `GET` answered `403`;
//! 3. the streaming-GET head — a signed `GET` of the object just written, answered `200`;
//! 4. server-error class, refused after auth — a signed `GET /<bucket>?acl`, answered
//!    `501 NotImplemented` by the subresource denylist before any handler runs.
//!
//! A genuine `500 InternalError` needs an injected backend fault and is not driven here;
//! it leaves `dispatch` through the same stamp point as the `501`.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates are stamped with real wall time so
// the requests fall inside the gateway's freshness window; nothing here owns a clocked
// lifecycle (#619).
#![allow(clippy::disallowed_methods)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// How long the role gets to bind and log its startup. Generous for a loaded CI box; the
/// child is killed on every exit path, so a hang can never outlive the test.
const STARTUP_BUDGET: Duration = Duration::from_secs(60);

/// Per-request socket budget, so a wedged server fails the test instead of hanging it.
const IO_BUDGET: Duration = Duration::from_secs(30);

/// Kills and reaps the role on drop — the `s3` role serves forever, so EVERY exit path
/// (assertion failure, timeout, panic) must stop it.
struct RoleGuard(Child);

impl Drop for RoleGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A running `wyrd s3` role: where it listens and the build identity it logged.
struct Role {
    _guard: RoleGuard,
    _data_dir: tempfile::TempDir,
    addr: SocketAddr,
    version: String,
}

/// Spawn `wyrd s3` on an ephemeral port and read its stderr until both the serving line
/// (which reports the listener's real address) and the `role started` JSON event (which
/// carries the identity) have appeared.
fn start_role() -> Role {
    let data_dir = tempfile::tempdir().expect("temp data dir");
    let child = Command::new(WYRD)
        .args([
            "s3",
            "--s3-listen",
            "127.0.0.1:0",
            "--data-dir",
            data_dir.path().to_str().expect("utf-8 temp path"),
            "--access-key",
            ACCESS_KEY,
            "--secret-key",
            SECRET_KEY,
            "--log-format",
            "json",
        ])
        // The role must start on its default local backends, in the default region, at the
        // default level, whatever the calling environment configures.
        .env_remove("WYRD_S3_ACCESS_KEY")
        .env_remove("WYRD_S3_SECRET_KEY")
        .env_remove("WYRD_METADATA_BACKEND")
        .env_remove("WYRD_COORDINATION_BACKEND")
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the wyrd binary");
    let mut guard = RoleGuard(child);
    let stderr = guard.0.stderr.take().expect("piped stderr");

    // A reader thread forwards lines, so the wait below is bounded by a deadline rather
    // than blocked on a read that might never return. It keeps draining after startup so
    // the role never blocks on a full stderr pipe.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            let _ = tx.send(line);
        }
    });

    let deadline = Instant::now() + STARTUP_BUDGET;
    let mut seen = Vec::new();
    let mut addr: Option<SocketAddr> = None;
    let mut version: Option<String> = None;
    while addr.is_none() || version.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = match rx.recv_timeout(left) {
            Ok(line) => line,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "wyrd s3 did not log its startup within {STARTUP_BUDGET:?}; stderr so far:\n{}",
                seen.join("\n")
            ),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                "wyrd s3 closed stderr (exited: {:?}) before logging its startup; stderr:\n{}",
                guard.0.try_wait(),
                seen.join("\n")
            ),
        };
        // `wyrd s3: serving S3-compatible HTTP on <addr> (data-dir …)` — the ephemeral port
        // is observed, never guessed.
        if let Some(rest) = line.split("wyrd s3: serving S3-compatible HTTP on ").nth(1) {
            let reported = rest.split_whitespace().next().unwrap_or_default();
            addr = Some(
                reported
                    .parse()
                    .unwrap_or_else(|e| panic!("unparsable serving address `{reported}`: {e}")),
            );
        }
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
            let fields = &event["fields"];
            if fields["message"] == "role started" && fields["role"] == "s3" {
                let logged = fields["version"].as_str().unwrap_or_else(|| {
                    panic!("the `role started` event carries no string `version` field: {event}")
                });
                assert!(!logged.is_empty(), "empty version in {event}");
                version = Some(logged.to_string());
            }
        }
        seen.push(line);
    }
    Role {
        _guard: guard,
        _data_dir: data_dir,
        addr: addr.expect("loop exits only once the address is known"),
        version: version.expect("loop exits only once the version is known"),
    }
}

/// SigV4 headers for one request, signed by the production `sigv4::sign` over the path
/// AND the raw query — `sign` canonicalizes the query itself, so a request carrying
/// `?acl` must be signed with `acl`, not with `""`, or the gateway refuses it `403`.
fn signed_headers(
    method: &str,
    path: &str,
    query: &str,
    host: &str,
    body: &[u8],
) -> Vec<(String, String)> {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, query, host, &amz_date, body, &creds, REGION, "s3",
    );
    vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ]
}

/// One response: status, the header block (lines after the status line), and the body.
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    head: String,
}

impl Reply {
    /// Every value of header `name` (case-insensitive), in order.
    fn values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    }
}

/// Send one HTTP/1.1 request over a fresh connection (`connection: close`) and return the
/// response's status and header block. The target is `path` plus `?query` when non-empty.
fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Reply {
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut request = format!("{method} {target} HTTP/1.1\r\nhost: {addr}\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n", body.len()));
    request.push_str("connection: close\r\n\r\n");

    let mut stream = TcpStream::connect_timeout(&addr, IO_BUDGET).expect("connect");
    stream
        .set_read_timeout(Some(IO_BUDGET))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(IO_BUDGET))
        .expect("write timeout");
    stream.write_all(request.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    stream.flush().expect("flush");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| {
            panic!(
                "response has no header terminator: {:?}",
                String::from_utf8_lossy(&raw)
            )
        });
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("unparsable status line `{status_line}`"));
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
        .collect();
    Reply {
        status,
        headers,
        head,
    }
}

/// `reply` carries exactly one `Server` header, and it is exactly `expected`.
fn assert_server(leg: &str, reply: &Reply, expected: &str) {
    let values = reply.values("server");
    assert_eq!(
        values,
        vec![expected],
        "{leg}: expected exactly one `Server: {expected}`; response head:\n{}",
        reply.head
    );
}

#[test]
fn every_response_category_names_the_logged_build_identity() {
    let role = start_role();
    let addr = role.addr;
    let host = addr.to_string();
    // Leg 5 for every leg below: the wire value is byte-equal to the identity THIS process
    // logged — not a shape, not a constant.
    let expected = format!("wyrd/{}", role.version);

    let key = "/wyrd-bucket/server-version-object";
    let body = b"the build that wrote this names itself";

    // Leg 1 — success: a signed PUT.
    let put = send(
        addr,
        "PUT",
        key,
        "",
        &signed_headers("PUT", key, "", &host, body),
        body,
    );
    assert_eq!(put.status, 200, "signed PUT; head:\n{}", put.head);
    assert_server("signed PUT (200)", &put, &expected);

    // Leg 2 — client error, pre-auth: the same object, unsigned.
    let unsigned = send(addr, "GET", key, "", &[], b"");
    assert_eq!(
        unsigned.status, 403,
        "unsigned GET; head:\n{}",
        unsigned.head
    );
    assert_server("unsigned GET (403)", &unsigned, &expected);

    // Leg 3 — the streaming-GET head: the object just written.
    let get = send(
        addr,
        "GET",
        key,
        "",
        &signed_headers("GET", key, "", &host, b""),
        b"",
    );
    assert_eq!(get.status, 200, "signed GET; head:\n{}", get.head);
    assert_server("signed GET (200, streamed)", &get, &expected);

    // Leg 4 — server-error class, post-auth: a signed bucket subresource the floor does
    // not implement. Signed WITH its query, so it passes auth and reaches the denylist.
    let bucket = "/wyrd-bucket";
    let acl = send(
        addr,
        "GET",
        bucket,
        "acl",
        &signed_headers("GET", bucket, "acl", &host, b""),
        b"",
    );
    assert_eq!(acl.status, 501, "signed GET ?acl; head:\n{}", acl.head);
    assert_server("signed GET ?acl (501)", &acl, &expected);
}
