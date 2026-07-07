//! A reusable, **networked observable S3 client** for the #329 consistency-checker harness
//! (ADR-0041 decision 1, the file-as-register model). It drives an overwriting **PUT / GET /
//! DELETE** workload against the S3 HTTP wire surface (`wyrd-gateway-s3`) over a real
//! loopback listener — exactly the driving composition
//! `crates/server/tests/s3_http_wire.rs` uses (sign with the production
//! [`wyrd_gateway_s3::sigv4::sign`], issue the request over a fresh
//! [`tokio::net::TcpStream`]) — and records a **client-observed, real-time-ordered
//! history**: per operation, the op kind, the object key, the register version observed,
//! and the wall-clock `[start, end]` span the client measured the round trip over.
//!
//! # Why the register version lives IN the object bytes
//! The wire surface's floor (issue #364) is plain object PUT/GET/DELETE — it does not echo
//! the backend's internal inode `version` back to the client (no ETag/version verb; the
//! wire surface exposes only PUT/GET/DELETE, `crates/gateway-s3/src/lib.rs:40`). So this
//! client models the register ADR-0041 decision 1 describes — an overwriting PUT bumps the
//! inode `version` under the commit-point CAS (`commit_overwrite`/`commit_chunk_map`), and a
//! GET returns the currently committed version — as a caller-assigned monotonic tag carried
//! as the object's own bytes (see [`encode_version`]/[`decode_version`]): a PUT of version
//! `n` writes the decimal digits of `n`, and a GET's observed version is whatever decimal
//! tag currently comes back (or `None` for a 404 — the key is absent). The round trip
//! itself is entirely real: every PUT genuinely commits through the gateway's production
//! write path over the wire, and every GET genuinely re-reads whatever is currently
//! committed, so a stale, torn, or reordered version recorded here is a real observation of
//! the backend's actual commit order, not a fabricated one.
//!
//! This client is the harness's #329 slice-2 deliverable. The linearizability verdict
//! (Elle/JVM, a privileged off-Check job per ADR-0041) and the real-cluster partition
//! nemesis (#257) are later, separate slices this client is built to feed — driving them is
//! out of scope here.

use std::net::SocketAddr;
use std::time::SystemTime;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};

/// One register operation the observable can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// An overwriting PUT — bumps the object's register version at the commit point.
    Put,
    /// A GET — reads the currently committed register version (`None` if absent).
    Get,
    /// A DELETE — removes the object. Idempotent; carries no register version.
    Delete,
}

/// One client-observed history entry: which op, on which key, what register version it
/// carried or observed, the wire status the gateway returned, and the real-time span
/// `[start, end]` the client measured the round trip over (`start` just before the request
/// is written, `end` just after the response is fully read) — exactly what a register-model
/// linearizability checker needs to place the op in real time.
#[derive(Debug, Clone)]
pub struct OpRecord {
    /// Which operation this entry records.
    pub kind: OpKind,
    /// The object key the op targeted.
    pub key: String,
    /// PUT: the version written. GET: the version read back (`None` for a 404 — the key was
    /// absent). DELETE: always `None` (a delete carries no register value).
    pub version: Option<u64>,
    /// The HTTP status the wire surface returned (200/204/404/…).
    pub status: u16,
    /// Wall-clock time observed just before the request began.
    pub start: SystemTime,
    /// Wall-clock time observed just after the response was fully read.
    pub end: SystemTime,
}

impl OpRecord {
    /// A single op's real-time span is well-formed iff it did not appear to end before it
    /// started — a client clock/measurement bug would otherwise silently corrupt the
    /// real-time ordering a downstream checker relies on.
    pub fn well_formed(&self) -> bool {
        self.end >= self.start
    }
}

/// The recorded, real-time-ordered per-operation history a register-model linearizability
/// checker consumes: every op the observable drove, in the order it was invoked (for a
/// single client driving one request at a time, invocation order is also real-time order).
#[derive(Debug, Default, Clone)]
pub struct History {
    ops: Vec<OpRecord>,
}

impl History {
    /// The recorded ops, in real-time (invocation) order.
    pub fn ops(&self) -> &[OpRecord] {
        &self.ops
    }

    /// Non-vacuous **and** every op individually well-formed: at least one op was recorded,
    /// and none of them carries a reversed or missing timestamp span. A checker fed an empty
    /// or malformed history proves nothing.
    pub fn well_formed(&self) -> bool {
        !self.ops.is_empty() && self.ops.iter().all(OpRecord::well_formed)
    }

    /// No stale or torn read, per key: the sequence of observed register versions (a PUT's
    /// written version and a GET's read version) never regresses in real-time order — a GET
    /// must never observe an older version than one already committed/observed earlier for
    /// the same key (ADR-0041 decision 1: the commit-point CAS totally orders versions).
    pub fn versions_monotone_per_key(&self) -> bool {
        use std::collections::HashMap;
        let mut last_by_key: HashMap<&str, u64> = HashMap::new();
        for op in &self.ops {
            let Some(version) = op.version else {
                continue;
            };
            match last_by_key.get(op.key.as_str()) {
                Some(&prev) if version < prev => return false,
                _ => {
                    last_by_key.insert(op.key.as_str(), version);
                }
            }
        }
        true
    }
}

/// A reusable, networked observable S3 client: drives signed PUT/GET/DELETE over a fresh
/// `TcpStream` against a live S3 HTTP wire listener and records every op into a [`History`].
/// The driving composition mirrors `crates/server/tests/s3_http_wire.rs`'s
/// `send`/`signed_headers`.
pub struct ObservableS3Client {
    addr: SocketAddr,
    bucket: String,
    creds: Credentials,
    region: String,
    history: History,
}

impl ObservableS3Client {
    /// A client bound at `addr` (the gateway's loopback listener), driving object keys
    /// under `bucket`, signing every request with `creds` in the SigV4 `region`/`s3` scope.
    pub fn new(
        addr: SocketAddr,
        bucket: impl Into<String>,
        creds: Credentials,
        region: impl Into<String>,
    ) -> Self {
        Self {
            addr,
            bucket: bucket.into(),
            creds,
            region: region.into(),
            history: History::default(),
        }
    }

    /// Drive an overwriting PUT of `key` carrying register version `version` (ADR-0041
    /// decision 1: an overwrite is a new inode version) — encoded as the object's own bytes
    /// (see module docs) so the plain PUT/GET floor doubles as the register's carried value.
    /// Records the op (start/end/version/status) into the client's [`History`].
    pub async fn put(&mut self, key: &str, version: u64) -> std::io::Result<()> {
        let path = self.object_path(key);
        let body = encode_version(version);
        let start = SystemTime::now();
        let (status, _response_body) = self.send("PUT", &path, &body).await?;
        let end = SystemTime::now();
        self.history.ops.push(OpRecord {
            kind: OpKind::Put,
            key: key.to_string(),
            version: Some(version),
            status,
            start,
            end,
        });
        Ok(())
    }

    /// Drive a GET of `key`, returning the currently committed register version (`None` if
    /// absent — a 404). Records the op into the client's [`History`].
    pub async fn get(&mut self, key: &str) -> std::io::Result<Option<u64>> {
        let path = self.object_path(key);
        let start = SystemTime::now();
        let (status, body) = self.send("GET", &path, &[]).await?;
        let end = SystemTime::now();
        let version = if status == 200 {
            Some(decode_version(&body).map_err(std::io::Error::other)?)
        } else {
            None
        };
        self.history.ops.push(OpRecord {
            kind: OpKind::Get,
            key: key.to_string(),
            version,
            status,
            start,
            end,
        });
        Ok(version)
    }

    /// Drive a DELETE of `key` (idempotent: succeeds whether or not the key was present).
    /// Records the op into the client's [`History`].
    pub async fn delete(&mut self, key: &str) -> std::io::Result<()> {
        let path = self.object_path(key);
        let start = SystemTime::now();
        let (status, _response_body) = self.send("DELETE", &path, &[]).await?;
        let end = SystemTime::now();
        self.history.ops.push(OpRecord {
            kind: OpKind::Delete,
            key: key.to_string(),
            version: None,
            status,
            start,
            end,
        });
        Ok(())
    }

    /// The history recorded so far — the input a register-model linearizability checker
    /// consumes.
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Consume the client and take ownership of its recorded history.
    pub fn into_history(self) -> History {
        self.history
    }

    fn object_path(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, key)
    }

    /// Sign and issue one HTTP/1.1 request over a fresh connection, returning `(status,
    /// body)` (mirrors `s3_http_wire.rs`'s `send`/`signed_headers`/`parse_response`).
    async fn send(&self, method: &str, path: &str, body: &[u8]) -> std::io::Result<(u16, Vec<u8>)> {
        let host = self.addr.to_string();
        let amz_date = format_amz_date(SystemTime::now());
        let signed = sign(
            method,
            path,
            "",
            &host,
            &amz_date,
            body,
            &self.creds,
            &self.region,
            "s3",
        );

        let mut request = format!("{method} {path} HTTP/1.1\r\n");
        request.push_str(&format!("host: {host}\r\n"));
        request.push_str(&format!("authorization: {}\r\n", signed.authorization));
        request.push_str(&format!("x-amz-date: {}\r\n", signed.amz_date));
        request.push_str(&format!(
            "x-amz-content-sha256: {}\r\n",
            signed.content_sha256
        ));
        request.push_str(&format!("content-length: {}\r\n", body.len()));
        request.push_str("connection: close\r\n\r\n");

        let mut stream = TcpStream::connect(self.addr).await?;
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await?;

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        Ok(parse_response(&raw))
    }
}

/// Encode a register version as its decimal ASCII bytes — the PUT body this client sends
/// for the value `version`.
fn encode_version(version: u64) -> Vec<u8> {
    version.to_string().into_bytes()
}

/// Decode a register version from a GET's returned bytes, or an error describing why the
/// bytes don't carry a well-formed version — a **torn or corrupted read** (bytes that are
/// not a clean decimal tag) surfaces here rather than being silently swallowed.
fn decode_version(body: &[u8]) -> Result<u64, String> {
    std::str::from_utf8(body)
        .map_err(|_| "GET returned non-UTF-8 bytes: torn/corrupted register value".to_string())?
        .parse::<u64>()
        .map_err(|_| "GET returned a non-numeric register value: torn/corrupted read".to_string())
}

/// Split a raw HTTP/1.1 response into `(status, body)`, de-framing a chunked-transfer body
/// should the wire surface ever fall back to it (mirrors `s3_http_wire.rs::parse_response`).
fn parse_response(raw: &[u8]) -> (u16, Vec<u8>) {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]);
    let status_line = head.lines().next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let raw_body = &raw[split + 4..];
    let is_chunked = head.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let body = if is_chunked {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    (status, body)
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body (mirrors `s3_http_wire.rs::dechunk`).
fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = raw
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("chunk size line");
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let size = usize::from_str_radix(size_str.trim(), 16).expect("hex chunk size");
        raw = &raw[line_end + 2..];
        if size == 0 {
            break;
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..]; // skip the chunk's trailing CRLF
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips() {
        assert_eq!(decode_version(&encode_version(42)).unwrap(), 42);
    }

    #[test]
    fn decode_rejects_a_torn_value() {
        assert!(decode_version(b"not-a-number").is_err());
    }

    #[test]
    fn empty_history_is_not_well_formed() {
        assert!(!History::default().well_formed());
    }

    #[test]
    fn an_op_with_a_reversed_span_is_not_well_formed() {
        let start = SystemTime::now();
        let end = start - std::time::Duration::from_secs(1);
        let op = OpRecord {
            kind: OpKind::Get,
            key: "k".to_string(),
            version: Some(1),
            status: 200,
            start,
            end,
        };
        assert!(!op.well_formed());
    }

    #[test]
    fn a_regressing_version_is_not_monotone() {
        let now = SystemTime::now();
        let history = History {
            ops: vec![
                OpRecord {
                    kind: OpKind::Put,
                    key: "k".to_string(),
                    version: Some(2),
                    status: 200,
                    start: now,
                    end: now,
                },
                OpRecord {
                    kind: OpKind::Get,
                    key: "k".to_string(),
                    version: Some(1),
                    status: 200,
                    start: now,
                    end: now,
                },
            ],
        };
        assert!(!history.versions_monotone_per_key());
    }
}
