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
//!
//! # Every invoked op is recorded — a transport failure is an OBSERVATION, not a gap (#408)
//!
//! The whole point of driving this client under a real nemesis (#407) is that ops *fail*: a
//! partitioned coordinator refuses/resets the connection, a paused process never answers. Such
//! an op's effect is **indeterminate** — it may or may not have committed — which is precisely
//! Jepsen/Elle's `:info`. So [`ObservableS3Client::put`]/[`get`]/[`delete`] record **every**
//! invoked op into the [`History`], stamping [`INDETERMINATE_STATUS`] when the round trip never
//! produced a status, and return the transport error *in addition to* (never instead of)
//! recording it. Two failure modes this closes, both of which a checked history cannot survive:
//! an op **omitted** from the history (the checker is handed a fabricated gap where a real
//! indeterminate op raced the fault), and a caller reading "the status of the op I just drove"
//! off the history's tail (`ops().last()`), which — when the op was never pushed — silently
//! inherits the **previous** op's status and serializes an indeterminate op as a definite `:ok`.
//! Callers never guess: each method **returns the [`OpRecord`] it recorded**.
//!
//! [`get`]: ObservableS3Client::get
//! [`delete`]: ObservableS3Client::delete

use std::net::SocketAddr;
use std::time::SystemTime;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};

/// The **synthetic status** this client stamps on an op whose round trip produced no HTTP status
/// at all — a connection refused/reset, a timeout, an unreadable response: exactly what a #407
/// nemesis leg induces. It is the "synthetic-0 status" the checker substrate's INV-1 predicate
/// already names (`consistency_workload::is_indeterminate`), so an op recorded with it serializes
/// as `:info` ("may or may not have happened") and every local check skips it. Never a definite
/// completion, and never — the #408 fix — a dropped op.
pub const INDETERMINATE_STATUS: u16 = 0;

/// An op whose round trip failed: it **was** recorded (stamped [`INDETERMINATE_STATUS`]) and this
/// carries both that [`OpRecord`] and the underlying cause.
///
/// Why the record travels in the error: a workload driver under a nemesis treats a transport
/// failure as *data* (an indeterminate op) and needs the very record the client recorded, while a
/// caller driving a healthy wire wants the failure to be loud. Handing the record back here serves
/// both without either re-reading it off the history's tail (`ops().last()`) — which is precisely
/// how an op comes to inherit a neighbour's status — or re-deriving a "probably indeterminate"
/// record of its own that could drift from what was actually recorded.
#[derive(Debug)]
pub struct OpFailed {
    /// The op as recorded in the [`History`] — indeterminate, never omitted.
    pub record: OpRecord,
    /// Why the round trip failed (connection refused/reset, timeout, a torn response body).
    pub cause: std::io::Error,
}

impl OpFailed {
    /// Take the recorded op, discarding the cause — for a driver whose history *is* the point and
    /// for which an indeterminate op is an ordinary observation, not an error to handle.
    #[must_use]
    pub fn into_record(self) -> OpRecord {
        self.record
    }
}

impl std::fmt::Display for OpFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} on `{}` failed at the transport (recorded as indeterminate): {}",
            self.record.kind, self.record.key, self.cause,
        )
    }
}

impl std::error::Error for OpFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

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
    ///
    /// An op stamped [`INDETERMINATE_STATUS`] is **skipped**: since #408 the client records the
    /// ops whose round trip never produced a status (a partitioned PUT still carries the version
    /// it *attempted*), and such a write may or may not have committed — so counting it as an
    /// observed version would let a *correct* later read of the previous version read as a
    /// regression. A check must never derive a definite claim from an indeterminate op, in either
    /// direction: no fabricated violations, no fabricated certainty.
    pub fn versions_monotone_per_key(&self) -> bool {
        use std::collections::HashMap;
        let mut last_by_key: HashMap<&str, u64> = HashMap::new();
        for op in &self.ops {
            if op.status == INDETERMINATE_STATUS {
                continue;
            }
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

    /// Push one recorded op onto the history and hand the caller back its [`OpRecord`], so a
    /// caller never has to re-read "the op I just drove" off the history's tail (`ops().last()`),
    /// which is what silently inherits a neighbour's status when a record is missing.
    fn record(&mut self, record: OpRecord) -> OpRecord {
        self.history.ops.push(record.clone());
        record
    }

    /// Drive an overwriting PUT of `key` carrying register version `version` (ADR-0041
    /// decision 1: an overwrite is a new inode version) — encoded as the object's own bytes
    /// (see module docs) so the plain PUT/GET floor doubles as the register's carried value.
    ///
    /// **Always records the op** and returns the [`OpRecord`] it recorded. A transport failure
    /// records it with [`INDETERMINATE_STATUS`] — the write may or may not have committed, so it
    /// is `:info`, never omitted (module docs) — and returns the error as well. The recorded
    /// `version` is the version this PUT *attempted*, indeterminate or not: that is the write the
    /// checker's `:invoke` micro-op states (`[:w key version]`), and a versionless write has no
    /// representable `rw-register` encoding at all.
    pub async fn put(&mut self, key: &str, version: u64) -> Result<OpRecord, OpFailed> {
        let path = self.object_path(key);
        let body = encode_version(version);
        let start = SystemTime::now();
        let sent = self.send("PUT", &path, &body).await;
        let end = SystemTime::now();
        match sent {
            Ok((status, _response_body)) => Ok(self.record(OpRecord {
                kind: OpKind::Put,
                key: key.to_string(),
                version: Some(version),
                status,
                start,
                end,
            })),
            Err(cause) => Err(OpFailed {
                record: self.record(OpRecord {
                    kind: OpKind::Put,
                    key: key.to_string(),
                    version: Some(version),
                    status: INDETERMINATE_STATUS,
                    start,
                    end,
                }),
                cause,
            }),
        }
    }

    /// Drive a GET of `key`. **Always records the op** and returns the [`OpRecord`] it recorded —
    /// whose `version` is the currently committed register version (`None` if absent: a 404).
    ///
    /// A transport failure, or a 200 whose bytes do not decode to a version tag (a torn read),
    /// records the op with [`INDETERMINATE_STATUS`] and `version: None` and returns the error:
    /// what the read observed is unknown, so it is `:info`. Recording a torn read as a *definite*
    /// 200-of-`nil` would claim the register was unwritten — a fabrication INV-1 forbids.
    pub async fn get(&mut self, key: &str) -> Result<OpRecord, OpFailed> {
        let path = self.object_path(key);
        let start = SystemTime::now();
        let sent = self.send("GET", &path, &[]).await;
        let decoded = match sent {
            Ok((status, body)) => {
                if status == 200 {
                    match decode_version(&body) {
                        Ok(v) => Ok((status, Some(v))),
                        Err(e) => Err(std::io::Error::other(e)),
                    }
                } else {
                    Ok((status, None))
                }
            }
            Err(e) => Err(e),
        };
        let end = SystemTime::now();
        match decoded {
            Ok((status, version)) => Ok(self.record(OpRecord {
                kind: OpKind::Get,
                key: key.to_string(),
                version,
                status,
                start,
                end,
            })),
            Err(cause) => Err(OpFailed {
                record: self.record(OpRecord {
                    kind: OpKind::Get,
                    key: key.to_string(),
                    version: None,
                    status: INDETERMINATE_STATUS,
                    start,
                    end,
                }),
                cause,
            }),
        }
    }

    /// Drive a DELETE of `key` (idempotent: succeeds whether or not the key was present).
    /// **Always records the op** and returns the [`OpRecord`] it recorded; a transport failure
    /// records it with [`INDETERMINATE_STATUS`] (the delete may or may not have committed) and
    /// returns the error as well.
    pub async fn delete(&mut self, key: &str) -> Result<OpRecord, OpFailed> {
        let path = self.object_path(key);
        let start = SystemTime::now();
        let sent = self.send("DELETE", &path, &[]).await;
        let end = SystemTime::now();
        match sent {
            Ok((status, _response_body)) => Ok(self.record(OpRecord {
                kind: OpKind::Delete,
                key: key.to_string(),
                version: None,
                status,
                start,
                end,
            })),
            Err(cause) => Err(OpFailed {
                record: self.record(OpRecord {
                    kind: OpKind::Delete,
                    key: key.to_string(),
                    version: None,
                    status: INDETERMINATE_STATUS,
                    start,
                    end,
                }),
                cause,
            }),
        }
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
        parse_response(&raw).map_err(std::io::Error::other)
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
/// should the wire surface ever fall back to it (the framing mirrors
/// `s3_http_wire.rs::parse_response`, but fallibly: that test helper may panic on a torn
/// response because its peer is healthy by construction — here a peer that accepts the
/// connection and then closes without a complete response is exactly what a nemesis
/// induces, so a malformed response is an `Err` the caller records as `:info`, never a
/// panic that aborts the workload task and drops the op from the history).
fn parse_response(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("torn response: no header terminator (peer closed mid-response)")?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let status_line = head.lines().next().ok_or("torn response: no status line")?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("torn response: status line carries no status code")?
        .parse()
        .map_err(|_| "torn response: non-numeric status code")?;
    let raw_body = &raw[split + 4..];
    let is_chunked = head.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let body = if is_chunked {
        dechunk(raw_body)?
    } else {
        // Enforce the declared `content-length`: the gateway emits an exact length for every
        // body it sends, so a body cut mid-transfer (a reset after the first byte of a `42`
        // register value) must be an `Err` recorded as `:info` — accepting the prefix would
        // decode as a DETERMINATE read of version `4`, a fabricated observation in the
        // history handed to Elle (INV-1's exact prohibition).
        let declared = head.lines().find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().to_string())
        });
        if let Some(value) = declared {
            let declared: usize = value
                .parse()
                .map_err(|_| "torn response: non-numeric content-length")?;
            if raw_body.len() != declared {
                return Err(format!(
                    "torn response: body carries {} of {declared} declared bytes (peer closed \
                     mid-body)",
                    raw_body.len(),
                ));
            }
        }
        raw_body.to_vec()
    };
    Ok((status, body))
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body (mirrors `s3_http_wire.rs::dechunk`,
/// fallibly — see [`parse_response`]).
fn dechunk(mut raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = raw
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("torn chunked body: no chunk size line")?;
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let size = usize::from_str_radix(size_str.trim(), 16)
            .map_err(|_| "torn chunked body: non-hex chunk size")?;
        raw = &raw[line_end + 2..];
        if size == 0 {
            // The terminal `0` chunk must be closed by the trailer-section CRLF: a peer that
            // resets after `0\r\n` sent a truncated message, not a complete one. The wire
            // surface never sends trailer fields, so anything but the bare terminator is
            // equally torn/unspoken protocol — an `Err`, never an accepted body.
            if raw != b"\r\n" {
                return Err("torn chunked body: missing the terminal trailer CRLF".to_string());
            }
            break;
        }
        if raw.len() < size + 2 {
            return Err("torn chunked body: chunk shorter than its declared size".to_string());
        }
        if &raw[size..size + 2] != b"\r\n" {
            return Err(
                "torn chunked body: chunk data not closed by its CRLF delimiter".to_string(),
            );
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
    Ok(out)
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
    fn a_torn_response_is_an_error_never_a_panic() {
        // A peer that accepts the connection and then closes without a complete response —
        // exactly the reset/torn-response shape a nemesis induces — must surface as `Err`
        // so `send`'s caller records the op as `:info`. A panic here aborts the workload
        // task and drops the invoked op from the history (the module's INV-1 prohibition).
        assert!(
            parse_response(b"").is_err(),
            "empty response (peer closed on accept)"
        );
        assert!(
            parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\n").is_err(),
            "headers truncated before the terminator"
        );
        assert!(
            parse_response(b"garbage\r\n\r\n").is_err(),
            "status line carries no numeric code"
        );
        assert!(
            parse_response(b"HTTP/1.1 abc OK\r\n\r\n").is_err(),
            "non-numeric status code"
        );
    }

    #[test]
    fn a_body_shorter_than_its_declared_content_length_is_an_error_never_a_prefix_read() {
        // The fabrication this guards against: a `42` register value reset after its first
        // byte would otherwise decode as a DETERMINATE 200 read of version `4` — a wrong
        // observation in the history handed to Elle, not merely a lost one.
        assert!(
            parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n4").is_err(),
            "body cut mid-transfer must be torn, not a shorter read"
        );
        assert!(
            parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n420").is_err(),
            "a body LONGER than declared is protocol garbage, not a longer read"
        );
        assert!(
            parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: abc\r\n\r\n42").is_err(),
            "a non-numeric content-length is torn, not ignorable"
        );
    }

    #[test]
    fn a_wellformed_response_still_parses() {
        let (status, body) = parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n42")
            .expect("a complete response parses");
        assert_eq!((status, body.as_slice()), (200, b"42".as_slice()));
    }

    #[test]
    fn a_torn_chunked_body_is_an_error_never_a_panic() {
        let torn = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nff\r\nshort";
        assert!(
            parse_response(torn).is_err(),
            "chunk shorter than its declared size"
        );
        let no_size = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nnothex";
        assert!(parse_response(no_size).is_err(), "no chunk size line");
        let reset_after_zero =
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\n42\r\n0\r\n";
        assert!(
            parse_response(reset_after_zero).is_err(),
            "a peer that resets after `0\\r\\n` but before the terminal trailer CRLF sent a \
             truncated message, not a determinate body"
        );
        let bad_delimiter =
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\n42XX0\r\n\r\n";
        assert!(
            parse_response(bad_delimiter).is_err(),
            "chunk data must be closed by its CRLF delimiter, not arbitrary bytes"
        );
        let ok = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\n42\r\n0\r\n\r\n";
        assert_eq!(
            parse_response(ok)
                .expect("a well-formed chunked body de-frames")
                .1,
            b"42".to_vec()
        );
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

    #[test]
    fn an_indeterminate_write_is_not_counted_as_an_observed_version() {
        // Since #408 the client records the ops whose transport failed, and an indeterminate PUT
        // still carries the version it ATTEMPTED. This history is entirely correct: the write of
        // v5 may never have committed, so a later read of v1 is a perfectly good read — not a
        // regression. Counting v5 as an observed version would fabricate a violation out of an op
        // that may never have happened (the mirror image of fabricating certainty: INV-1 forbids
        // deriving a definite claim from an indeterminate op in EITHER direction).
        let now = SystemTime::now();
        let op = |version, status| OpRecord {
            kind: OpKind::Put,
            key: "k".to_string(),
            version: Some(version),
            status,
            start: now,
            end: now,
        };
        let history = History {
            ops: vec![
                op(1, 200),
                op(5, INDETERMINATE_STATUS),
                OpRecord {
                    kind: OpKind::Get,
                    ..op(1, 200)
                },
            ],
        };
        assert!(
            history.versions_monotone_per_key(),
            "an indeterminate write must not make a correct later read read as a stale-read \
             violation"
        );

        // The guard is scoped to indeterminacy, not blanket permissiveness: a DETERMINATE
        // regression is still a violation.
        let determinate_regression = History {
            ops: vec![
                op(5, 200),
                OpRecord {
                    kind: OpKind::Get,
                    ..op(1, 200)
                },
            ],
        };
        assert!(!determinate_regression.versions_monotone_per_key());
    }
}
