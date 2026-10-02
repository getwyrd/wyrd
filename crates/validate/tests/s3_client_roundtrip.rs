//! Issue #852: the S3 client every `wyrd-validate` scenario calls through, proven against a
//! real Wyrd S3 gateway served in-process on loopback (the composition
//! `crates/server/tests/s3_http_wire.rs` uses: redb + `MemCoordination` + a local
//! `FsChunkStore`, 256 KiB chunks). No container, no network, no mock: the production
//! [`S3Client`] drives the real `aws-sdk-s3` over real TCP into the real `S3Gateway`.
//!
//! Between the client and the gateway sits, where a test needs one, a **relay**: a loopback
//! TCP forwarder with small buffers on its own sockets that counts bytes per direction,
//! captures what the gateway sent, and can pace, hold or cut the response body. It is the
//! oracle the client cannot see.
//!
//! * **Round trip** (criterion 1): PUT → GET → DELETE, byte-identical, then a GET reports
//!   the typed 404 `NoSuchKey`; an empty object round-trips too.
//! * **Typed error** (criterion 2): every field of an error equals what the relay saw on the
//!   wire — status line, `x-amz-request-id` header, `<Code>` and `<Message>` — with a decoy
//!   `x-amzn-requestid` added to the response on its way to the client, so the id is shown to
//!   come from `x-amz-request-id` itself (scope c), not from the SDK's generic accessor.
//! * **Streaming** (criterion 3), each oracle holding for the whole transfer:
//!   - PUT lag: at every pull, bytes the source has produced minus request-body bytes the
//!     relay has forwarded is at most `W`;
//!   - PUT retention: at every pull, at most `K` of the source's pieces are alive (each
//!     piece is a `Bytes::from_owner` whose owner counts itself);
//!   - GET lag: the relay never writes more than `W` response-body bytes past what the test
//!     has taken from the client, so a client that waits for more than `W` before handing a
//!     piece over stalls, and the test's bounded wait reports it.
//! * **Integrity and bounded waits** (criterion 4): a PUT source that ends short, runs long
//!   or fails is the body error and stores nothing; a GET cut mid-body is the body error;
//!   the connect, operation and body-idle deadlines each expire as their own typed timeout,
//!   and a refused connection is no response, not a timeout; a source that is always ready
//!   with empty pieces still yields to the operation deadline, on a current-thread runtime
//!   too, and its connection is closed; an empty key is a request never built, and nothing
//!   reaches the wire.
//!
//! # The window `W` and the piece bound `K`
//!
//! `W` bounds what can sit, legitimately, between a source pull and the relay's write to the
//! gateway: every buffer on that path, each with the source of its bound.
//!
//! | Term | Bound | Source |
//! |---|---|---|
//! | SDK re-chunking | `SDK_CHUNK + PIECE` | aws-chunked buffers source bytes until it has one 64 KiB chunk (`aws-runtime` 1.10.0 `content_encoding.rs:26`, body `http_body_1_x.rs:55-80`) |
//! | hyper's write buffer | `HYPER_WRITE_BUF + SDK_CHUNK + FRAMING` | hyper stops polling the body once it holds 8192 + 4096 × 100 bytes (`hyper` 1.10.1 `proto/h1/io.rs:23`, `:575-582`), so it holds less than that plus one signed chunk |
//! | client socket send queue | `tcp_wmem[2] + LOOPBACK_SKB` | the SDK's socket autotunes its send buffer up to `net.ipv4.tcp_wmem[2]` (read, never set), overshooting by at most one 64 KiB loopback segment |
//! | relay socket receive queue | `2 × RELAY_SOCKBUF` | set on the relay's own sockets; the kernel doubles `SO_RCVBUF` |
//! | relay read buffer | `RELAY_BUF` | the relay's own |
//!
//! With the kernel default `tcp_wmem[2]` of 4 MiB, `W` is about 4.7 MiB and the payload,
//! `8 × W` rounded up to whole pieces, about 37 MiB.
//!
//! "Forwarded" counts payload bytes only. The SDK sends a streaming PUT `aws-chunked`: each
//! 64 KiB chunk carries a size line with its signature and a CRLF, and the body ends with a
//! checksum trailer. The relay decodes that framing as it forwards ([`Framing`]) and
//! credits only chunk data, never the request head, the framing or the trailer, so the lag
//! the oracle measures is payload produced minus payload forwarded, exactly. Framing still
//! takes room in the buffers of the table, so payload in flight is at most their sum, `W`.
//! The test checks the decoder too: when the PUT completes, it must have credited exactly
//! the object's length.
//!
//! `K = SDK_CHUNK / PIECE = 4`: the aws-chunked buffer polls the source only while it holds
//! less than one chunk, so at most three 16 KiB pieces, and the piece being pulled is the
//! fourth. Every later stage holds the SDK's signed copy of a chunk, never a source piece.
//!
//! For the GET, `W` is the relay's credit. A streaming client hands over each piece as hyper
//! reads it, so any credit lets it progress; a client that must see more than `W` bytes
//! before handing one over cannot.
//!
//! What no oracle here sees, and review covers instead: a client that copies bytes it has
//! already passed on into a buffer of its own.
//!
//! RED before #852: the S3 client this file drives does not exist, so the file does not
//! compile.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream::{self, Stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::timeout;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::Credentials as GatewayCredentials;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_validate::{
    resolve_config, BodyError, Deadlines, ErrorCode, ObjectBody, Phase, PutOutcome, PutSource,
    ResolvedConfig, S3Client, S3Error,
};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "validate";

/// The gateway's chunk size: a realistic one, not the 8-byte chunk of the wire tests.
const GATEWAY_CHUNK: usize = 256 * 1024;

/// The size of each piece a test source yields.
const PIECE: usize = 16 * 1024;
/// The SDK's aws-chunked chunk size (`aws-runtime` 1.10.0 `content_encoding.rs:26`); the
/// client does not configure it.
const SDK_CHUNK: u64 = 64 * 1024;
/// An upper bound on one signed chunk's framing: hex length, `;chunk-signature=`, 64 hex
/// digits, two CRLFs (90 bytes for a 64 KiB chunk).
const FRAMING: u64 = 128;
/// hyper's default write-buffer ceiling (`hyper` 1.10.1 `proto/h1/io.rs:23`).
const HYPER_WRITE_BUF: u64 = 8192 + 4096 * 100;
/// The largest loopback segment: what a socket send queue can overshoot its limit by.
const LOOPBACK_SKB: u64 = 64 * 1024;
/// `SO_RCVBUF`/`SO_SNDBUF` on the relay's own sockets.
const RELAY_SOCKBUF: usize = 16 * 1024;
/// The relay's read buffer.
const RELAY_BUF: usize = 16 * 1024;
/// How many of the source's pieces may be alive at a pull.
const K: usize = (SDK_CHUNK / PIECE as u64) as usize;
/// How much of what the gateway sends the relay keeps for the test to parse.
const CAPTURE_LIMIT: usize = 64 * 1024;

/// The test's own bound on any one wait for the client. Longer than the client's body-idle
/// deadline, so a production deadline fires first where one applies.
const STALL: Duration = Duration::from_secs(30);
/// The test's bound on a whole streaming transfer.
const TRANSFER: Duration = Duration::from_secs(300);

/// The ceiling of the SDK socket's autotuned send buffer: `net.ipv4.tcp_wmem[2]`.
fn client_send_queue_cap() -> u64 {
    std::fs::read_to_string("/proc/sys/net/ipv4/tcp_wmem")
        .ok()
        .and_then(|text| text.split_whitespace().nth(2)?.parse().ok())
        // The Linux default, where `/proc` cannot be read.
        .unwrap_or(4 * 1024 * 1024)
}

/// `W`, from the table in the module docs.
fn window() -> u64 {
    let sdk = SDK_CHUNK + PIECE as u64;
    let hyper = HYPER_WRITE_BUF + SDK_CHUNK + FRAMING;
    let client_socket = client_send_queue_cap() + LOOPBACK_SKB;
    let relay = 2 * RELAY_SOCKBUF as u64 + RELAY_BUF as u64;
    sdk + hyper + client_socket + relay
}

/// The streaming payload: at least `8 × W`, in whole pieces.
fn streaming_payload(window: u64) -> u64 {
    (8 * window).div_ceil(PIECE as u64) * PIECE as u64
}

// --- payload -------------------------------------------------------------------------------

/// The byte at `offset` of every test object: a function of the absolute offset, so a
/// dropped, repeated or reordered piece cannot compare equal.
fn payload_byte(offset: u64) -> u8 {
    let mixed = offset.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (mixed >> 56) as u8 ^ offset as u8
}

fn payload(offset: u64, len: usize) -> Vec<u8> {
    (offset..offset + len as u64).map(payload_byte).collect()
}

/// A piece's owner: counts itself alive until the last `Bytes` sharing it is dropped.
struct Tracked {
    bytes: Vec<u8>,
    live: Arc<AtomicUsize>,
}

impl Tracked {
    fn piece(bytes: Vec<u8>, live: &Arc<AtomicUsize>) -> Bytes {
        live.fetch_add(1, Ordering::SeqCst);
        Bytes::from_owner(Tracked {
            bytes,
            live: Arc::clone(live),
        })
    }
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What the PUT oracles observed, pull by pull.
#[derive(Default)]
struct PutProbe {
    relay: Option<Arc<RelayStats>>,
    pulls: AtomicU64,
    max_lag: AtomicU64,
    /// `(pull, produced, forwarded)` at the largest lag seen.
    worst: Mutex<(u64, u64, u64)>,
    max_live: AtomicUsize,
}

impl PutProbe {
    fn on_relay(relay: &Arc<RelayStats>) -> Arc<Self> {
        Arc::new(Self {
            relay: Some(Arc::clone(relay)),
            ..Self::default()
        })
    }

    /// Called at every pull, after the pulled piece (if any) exists.
    fn observe(&self, produced: u64, live: usize) {
        let pull = self.pulls.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_live.fetch_max(live, Ordering::SeqCst);
        if let Some(relay) = &self.relay {
            let forwarded = relay.upstream_payload.load(Ordering::SeqCst);
            let lag = produced.saturating_sub(forwarded);
            if lag > self.max_lag.fetch_max(lag, Ordering::SeqCst) {
                *self.worst.lock().expect("probe lock") = (pull, produced, forwarded);
            }
        }
    }
}

/// A generated source of `total` bytes in `piece`-sized `Bytes::from_owner` pieces, never
/// materialised whole.
struct Generated {
    offset: u64,
    total: u64,
    piece: usize,
    live: Arc<AtomicUsize>,
    probe: Option<Arc<PutProbe>>,
}

impl Generated {
    fn new(total: u64, piece: usize) -> Self {
        Self {
            offset: 0,
            total,
            piece,
            live: Arc::new(AtomicUsize::new(0)),
            probe: None,
        }
    }

    fn probed(mut self, probe: &Arc<PutProbe>, live: &Arc<AtomicUsize>) -> Self {
        self.probe = Some(Arc::clone(probe));
        self.live = Arc::clone(live);
        self
    }
}

impl Stream for Generated {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = (this.offset < this.total).then(|| {
            let len = (this.total - this.offset).min(this.piece as u64) as usize;
            let piece = Tracked::piece(payload(this.offset, len), &this.live);
            this.offset += len as u64;
            Ok(piece)
        });
        if let Some(probe) = &this.probe {
            probe.observe(this.offset, this.live.load(Ordering::SeqCst));
        }
        Poll::Ready(next)
    }
}

// --- the in-process gateway ----------------------------------------------------------------

/// A Wyrd S3 gateway on an ephemeral loopback port. Field order is drop order: the serving
/// task is aborted before its chunk store's directory is removed.
struct GatewayFixture {
    _tasks: JoinSet<()>,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn start_gateway() -> GatewayFixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(
        Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(GATEWAY_CHUNK),
    );
    let config = S3Config::new(vec![GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    GatewayFixture {
        _tasks: tasks,
        addr,
        _dir: dir,
    }
}

// --- the client, built the way the binary builds it ----------------------------------------

/// A resolved configuration for `endpoint`, through the production argument parser and
/// credential resolution.
fn resolved(endpoint: SocketAddr, secret: &str) -> ResolvedConfig {
    let endpoint = format!("http://{endpoint}");
    let pairs = [
        ("--endpoint", endpoint.as_str()),
        ("--region", REGION),
        ("--bucket", BUCKET),
        ("--scenario", "smoke"),
        ("--duration", "1m"),
        ("--workers", "1"),
        ("--seed", "852"),
        ("--out", "unused"),
        ("--run-id", "s3-client-roundtrip"),
        ("--driver-placement", "loopback"),
    ];
    let args: Vec<String> = pairs
        .iter()
        .flat_map(|(flag, value)| [flag.to_string(), value.to_string()])
        .collect();
    let lookup = |name: &str| -> Option<OsString> {
        match name {
            "AWS_ACCESS_KEY_ID" => Some(ACCESS_KEY.into()),
            "AWS_SECRET_ACCESS_KEY" => Some(secret.into()),
            _ => None,
        }
    };
    resolve_config(&args, &lookup).expect("the configuration resolves")
}

fn deadlines() -> Deadlines {
    Deadlines {
        connect: Duration::from_secs(5),
        operation: Duration::from_secs(120),
        body_idle: Duration::from_secs(10),
    }
}

fn client(endpoint: SocketAddr) -> S3Client {
    S3Client::with_deadlines(&resolved(endpoint, SECRET_KEY), deadlines())
}

/// A client talking to the gateway directly, for setup and read-back outside the relay.
fn client_direct(gateway: &GatewayFixture) -> S3Client {
    client(gateway.addr)
}

/// Read `body` to its end, checking every byte against the payload function.
async fn expect_payload(body: &mut ObjectBody, len: u64) {
    assert_eq!(body.content_length(), len, "declared Content-Length");
    let mut offset = 0u64;
    loop {
        let piece = timeout(STALL, body.next_piece())
            .await
            .unwrap_or_else(|_| panic!("GET stalled at offset {offset}"))
            .unwrap_or_else(|e| panic!("GET failed at offset {offset}: {e}"));
        let Some(piece) = piece else { break };
        assert!(
            piece[..] == payload(offset, piece.len())[..],
            "GET bytes differ from the PUT within [{offset}, {})",
            offset + piece.len() as u64
        );
        offset += piece.len() as u64;
    }
    assert_eq!(offset, len, "GET handed over the whole object");
}

async fn put_generated(client: &S3Client, key: &str, len: u64) {
    let source = PutSource::new(len, Generated::new(len, 64 * 1024));
    timeout(TRANSFER, client.put_object(key, source))
        .await
        .expect("PUT finished within the limit")
        .unwrap_or_else(|e| panic!("PUT {key}: {e}"));
}

async fn get(client: &S3Client, key: &str) -> Result<ObjectBody, S3Error> {
    timeout(STALL, client.get_object(key))
        .await
        .unwrap_or_else(|_| panic!("GET {key} produced no response head within {STALL:?}"))
}

/// The key is absent: a GET reports the typed 404 `NoSuchKey`, field by field.
async fn expect_no_such_key(client: &S3Client, key: &str) {
    match get(client, key).await {
        Err(S3Error::Service {
            status,
            code,
            message,
            request_id,
        }) => {
            assert_eq!(status, 404, "status");
            assert_eq!(code, ErrorCode::Code("NoSuchKey".to_string()), "code");
            assert!(
                message.as_deref().is_some_and(|m| !m.is_empty()),
                "a <Message> is reported: {message:?}"
            );
            let id = request_id.expect("an x-amz-request-id is reported");
            assert!(
                id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
                "the gateway's request-id form (32 hex digits): {id:?}"
            );
        }
        Err(other) => panic!("GET {key}: expected 404 NoSuchKey, got {other:?}"),
        Ok(_) => panic!("GET {key}: expected 404 NoSuchKey, but the object exists"),
    }
}

// --- the relay -----------------------------------------------------------------------------

/// What the relay does with response bodies.
#[derive(Debug, Clone, Copy)]
enum Downstream {
    /// Forward as the gateway sends.
    Free,
    /// Write at most `window` response-body bytes past what the test has taken.
    Paced { window: u64 },
    /// Forward the head and the first `body` bytes of the body, then hold the rest with the
    /// connection open.
    HoldAfter { body: u64 },
    /// Forward the head and the first `body` bytes of the body, then close both connections.
    CutAfter { body: u64 },
}

/// The relay's counters. Body counts start after the first head on a connection; each test
/// that reads them sends one request per relay.
#[derive(Default)]
struct RelayStats {
    connections: AtomicU64,
    /// Request payload bytes written to the gateway: the body with any `aws-chunked`
    /// framing and trailer decoded away.
    upstream_payload: AtomicU64,
    /// Response-body bytes written to the client.
    downstream_body: AtomicU64,
    /// Response-body bytes the test has taken from the client.
    taken: AtomicU64,
    credit: Notify,
    /// The first `CAPTURE_LIMIT` bytes the gateway sent.
    captured: Mutex<Vec<u8>>,
}

impl RelayStats {
    fn took(&self, n: usize) {
        self.taken.fetch_add(n as u64, Ordering::SeqCst);
        self.credit.notify_one();
    }

    fn capture(&self, bytes: &[u8]) {
        let mut captured = self.captured.lock().expect("capture lock");
        let room = CAPTURE_LIMIT.saturating_sub(captured.len());
        captured.extend_from_slice(&bytes[..room.min(bytes.len())]);
    }
}

/// Collects the first HTTP head on a connection (up to and including `\r\n\r\n`) across
/// reads.
#[derive(Default)]
struct HeadScan {
    head: Vec<u8>,
    matched: usize,
    done: bool,
}

impl HeadScan {
    /// Feeds the next bytes read; returns how many of them lie past the end of the head.
    /// The bytes before that end are kept in `head`.
    fn feed(&mut self, bytes: &[u8]) -> usize {
        if self.done {
            return bytes.len();
        }
        for (i, &b) in bytes.iter().enumerate() {
            self.matched = match (self.matched, b) {
                (0 | 2, b'\r') => self.matched + 1,
                (1 | 3, b'\n') => self.matched + 1,
                (_, b'\r') => 1,
                _ => 0,
            };
            if self.matched == 4 {
                self.done = true;
                self.head.extend_from_slice(&bytes[..=i]);
                return bytes.len() - i - 1;
            }
        }
        self.head.extend_from_slice(bytes);
        0
    }

    /// The value of header `name` in the collected head, if present.
    fn header(&self, name: &str) -> Option<String> {
        let text = std::str::from_utf8(&self.head).expect("the head is text");
        text.split("\r\n").skip(1).find_map(|line| {
            let (field, value) = line.split_once(':')?;
            field
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }
}

/// Where a request body's payload is, for crediting forwarded bytes. A body whose
/// `Content-Encoding` is `aws-chunked` is chunked framing (RFC 9112 §7.1, the size line
/// carrying a `;chunk-signature=` extension) ending in a checksum trailer: only chunk data
/// is payload. Any other body is payload whole.
enum Framing {
    Plain,
    /// In a chunk-size line: the size read so far, and whether its extension has begun.
    Size {
        size: u64,
        in_extension: bool,
    },
    /// In a chunk's data, with this much left.
    Data(u64),
    /// In the CRLF after a chunk's data, with this many of its bytes left.
    DataEnd(usize),
    /// After the last chunk: the trailer, none of it payload.
    Trailer,
}

impl Framing {
    fn of_request(head: &HeadScan) -> Self {
        match head.header("content-encoding") {
            Some(coding) if coding.to_ascii_lowercase().contains("aws-chunked") => Self::Size {
                size: 0,
                in_extension: false,
            },
            _ => Self::Plain,
        }
    }

    /// How many of `bytes`, the next body bytes forwarded, are payload.
    fn payload(&mut self, mut bytes: &[u8]) -> u64 {
        let mut payload = 0u64;
        while let Some(&b) = bytes.first() {
            match self {
                Self::Plain => {
                    payload += bytes.len() as u64;
                    bytes = &[];
                }
                Self::Data(left) => {
                    let n = (*left).min(bytes.len() as u64);
                    payload += n;
                    *left -= n;
                    bytes = &bytes[n as usize..];
                    if *left == 0 {
                        *self = Self::DataEnd(2);
                    }
                }
                Self::DataEnd(left) => {
                    let n = (*left).min(bytes.len());
                    *left -= n;
                    bytes = &bytes[n..];
                    if *left == 0 {
                        *self = Self::Size {
                            size: 0,
                            in_extension: false,
                        };
                    }
                }
                Self::Size { size, in_extension } => {
                    bytes = &bytes[1..];
                    match b {
                        b'\n' if *size == 0 => *self = Self::Trailer,
                        b'\n' => *self = Self::Data(*size),
                        b';' => *in_extension = true,
                        b'\r' => {}
                        _ if *in_extension => {}
                        _ => {
                            let digit = char::from(b).to_digit(16).expect("a hex chunk size");
                            *size = *size * 16 + u64::from(digit);
                        }
                    }
                }
                Self::Trailer => bytes = &[],
            }
        }
        payload
    }
}

/// The response header the relay adds, when told to, before the gateway's own headers: the
/// one the SDK's generic request-id accessor prefers over `x-amz-request-id`.
const DECOY_HEADER: &str = "x-amzn-requestid";
/// The decoy's value: never a request id the gateway issues (those are 32 hex digits).
const DECOY_REQUEST_ID: &str = "decoy-x-amzn-requestid-852";

/// A loopback relay to `upstream`. Every task it spawns is owned and aborted on drop.
struct Relay {
    _tasks: JoinSet<()>,
    addr: SocketAddr,
    stats: Arc<RelayStats>,
}

fn small_socket() -> TcpSocket {
    let socket = TcpSocket::new_v4().expect("relay socket");
    socket
        .set_recv_buffer_size(RELAY_SOCKBUF as u32)
        .expect("relay SO_RCVBUF");
    socket
        .set_send_buffer_size(RELAY_SOCKBUF as u32)
        .expect("relay SO_SNDBUF");
    socket
}

impl Relay {
    async fn start(upstream: SocketAddr, mode: Downstream) -> Self {
        Self::start_with(upstream, mode, false).await
    }

    /// As [`Relay::start`], adding a decoy `x-amzn-requestid` to every response when
    /// `decoy` is set.
    async fn start_with(upstream: SocketAddr, mode: Downstream, decoy: bool) -> Self {
        // Set before `listen`, so every accepted socket inherits the small buffers.
        let socket = small_socket();
        socket
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("relay bind");
        let listener = socket.listen(64).expect("relay listen");
        let addr = listener.local_addr().expect("relay addr");
        let stats = Arc::new(RelayStats::default());
        let counters = Arc::clone(&stats);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            // Owned by the accept task: aborting it drops this set, which aborts them.
            let mut connections = JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                while connections.try_join_next().is_some() {}
                counters.connections.fetch_add(1, Ordering::SeqCst);
                connections.spawn(relay_connection(
                    client,
                    upstream,
                    mode,
                    decoy,
                    Arc::clone(&counters),
                ));
            }
        });
        Self {
            _tasks: tasks,
            addr,
            stats,
        }
    }
}

async fn relay_connection(
    client: TcpStream,
    upstream: SocketAddr,
    mode: Downstream,
    decoy: bool,
    stats: Arc<RelayStats>,
) {
    let Ok(gateway) = small_socket().connect(upstream).await else {
        return;
    };
    let (mut client_rd, mut client_wr) = client.into_split();
    let (mut gateway_rd, mut gateway_wr) = gateway.into_split();
    // Either side ending ends the connection: both sockets are dropped together.
    tokio::select! {
        () = pump_up(&mut client_rd, &mut gateway_wr, &stats) => {}
        () = pump_down(&mut gateway_rd, &mut client_wr, mode, decoy, &stats) => {}
    }
}

async fn pump_up(from: &mut OwnedReadHalf, to: &mut OwnedWriteHalf, stats: &RelayStats) {
    let mut buf = vec![0u8; RELAY_BUF];
    let mut head = HeadScan::default();
    let mut framing = None;
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
        // Credited once written: the gateway's socket has accepted these bytes.
        let body_len = head.feed(&buf[..n]);
        if head.done && framing.is_none() {
            framing = Some(Framing::of_request(&head));
        }
        if let Some(framing) = framing.as_mut() {
            let payload = framing.payload(&buf[n - body_len..n]);
            stats.upstream_payload.fetch_add(payload, Ordering::SeqCst);
        }
    }
}

/// `head` with the decoy `x-amzn-requestid` header added after its status line.
fn with_decoy(head: &[u8]) -> Vec<u8> {
    let status_end = head
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("a status line")
        + 2;
    let mut out = head[..status_end].to_vec();
    out.extend_from_slice(format!("{DECOY_HEADER}: {DECOY_REQUEST_ID}\r\n").as_bytes());
    out.extend_from_slice(&head[status_end..]);
    out
}

async fn pump_down(
    from: &mut OwnedReadHalf,
    to: &mut OwnedWriteHalf,
    mode: Downstream,
    decoy: bool,
    stats: &RelayStats,
) {
    let mut buf = vec![0u8; RELAY_BUF];
    let mut head = HeadScan::default();
    let mut written = 0u64;
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        // What the gateway sent, before any decoy is added.
        stats.capture(&buf[..n]);
        let head_was_done = head.done;
        let body_len = head.feed(&buf[..n]);
        if !head.done {
            // The head is forwarded whole once its end arrives.
            continue;
        }
        if !head_was_done {
            let forwarded = if decoy {
                with_decoy(&head.head)
            } else {
                head.head.clone()
            };
            if to.write_all(&forwarded).await.is_err() {
                return;
            }
        }
        let mut body = &buf[n - body_len..n];
        match mode {
            Downstream::Free => {
                if to.write_all(body).await.is_err() {
                    return;
                }
                written += body.len() as u64;
            }
            Downstream::Paced { window } => {
                while !body.is_empty() {
                    let allowed = loop {
                        let taken = stats.taken.load(Ordering::SeqCst);
                        let allowed = (taken + window).saturating_sub(written);
                        if allowed > 0 {
                            break allowed;
                        }
                        // `notify_one` stores a permit when nobody waits, so a credit
                        // granted between the check and this await is not lost.
                        stats.credit.notified().await;
                    };
                    let (now, rest) = body.split_at(body.len().min(allowed as usize));
                    if to.write_all(now).await.is_err() {
                        return;
                    }
                    written += now.len() as u64;
                    stats.downstream_body.store(written, Ordering::SeqCst);
                    body = rest;
                }
            }
            Downstream::HoldAfter { body: limit } | Downstream::CutAfter { body: limit } => {
                let room = limit.saturating_sub(written).min(body.len() as u64) as usize;
                if to.write_all(&body[..room]).await.is_err() {
                    return;
                }
                written += room as u64;
                if written >= limit {
                    stats.downstream_body.store(written, Ordering::SeqCst);
                    if let Downstream::HoldAfter { .. } = mode {
                        // Hold the tail: the connection stays open until the relay drops.
                        std::future::pending::<()>().await;
                    }
                    return;
                }
            }
        }
        stats.downstream_body.store(written, Ordering::SeqCst);
    }
}

/// A complete chunked body's data (RFC 9112 §7.1), up to its last chunk.
fn dechunk(mut framed: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    loop {
        let line_end = framed
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("a chunk-size line was captured");
        let size_text = std::str::from_utf8(&framed[..line_end]).expect("a textual chunk size");
        let size_text = size_text.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16).expect("a hex chunk size");
        if size == 0 {
            return data;
        }
        let chunk = &framed[line_end + 2..];
        assert!(
            chunk.len() >= size + 2 && &chunk[size..size + 2] == b"\r\n",
            "a complete chunk was captured"
        );
        data.extend_from_slice(&chunk[..size]);
        framed = &chunk[size + 2..];
    }
}

/// The first response the relay captured: status, request-id header, `<Code>`, `<Message>`.
struct WireError {
    status: u16,
    request_id: Option<String>,
    code: String,
    message: Option<String>,
}

fn parse_captured(bytes: &[u8]) -> WireError {
    let head_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a complete response head was captured")
        + 4;
    let head = std::str::from_utf8(&bytes[..head_end]).expect("the head is text");
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .expect("a status line");
    let mut request_id = None;
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("x-amz-request-id") {
            request_id = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(value.parse::<usize>().expect("a numeric Content-Length"));
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value.eq_ignore_ascii_case("chunked");
        }
    }
    // The gateway frames its error bodies chunked; a length-framed body is read as declared.
    let body = match (chunked, content_length) {
        (true, _) => dechunk(&bytes[head_end..]),
        (false, Some(len)) => bytes[head_end..head_end + len].to_vec(),
        (false, None) => panic!("the error response frames its body neither way"),
    };
    let body = String::from_utf8(body).expect("the body is text");
    let doc = roxmltree::Document::parse(&body).expect("the error body is XML");
    let root = doc.root_element();
    assert_eq!(root.tag_name().name(), "Error", "an S3 <Error> document");
    let child = |name: &str| {
        root.children()
            .find(|n| n.has_tag_name(name))
            .map(|n| n.text().unwrap_or_default().to_string())
    };
    WireError {
        status,
        request_id,
        code: child("Code").expect("the document carries a <Code>"),
        message: child("Message"),
    }
}

// --- criterion 1: round trip ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_delete_round_trips_byte_identical() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr);
    // Several gateway chunks and a ragged tail.
    let len = 4 * GATEWAY_CHUNK as u64 + 7;

    put_generated(&client, "round-trip", len).await;
    let mut body = get(&client, "round-trip").await.expect("GET");
    expect_payload(&mut body, len).await;

    timeout(STALL, client.delete_object("round-trip"))
        .await
        .expect("DELETE answered")
        .expect("DELETE");
    expect_no_such_key(&client, "round-trip").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_object_round_trips() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr);

    put_generated(&client, "empty", 0).await;
    let mut body = get(&client, "empty").await.expect("GET");
    expect_payload(&mut body, 0).await;

    timeout(STALL, client.delete_object("empty"))
        .await
        .expect("DELETE answered")
        .expect("DELETE");
    expect_no_such_key(&client, "empty").await;
}

// --- criterion 2: the typed error equals the wire ------------------------------------------

/// GET `key` through a capturing relay with `secret`, and compare the typed error with the
/// response the relay saw. The relay adds a decoy `x-amzn-requestid` on the way to the
/// client: the SDK's generic accessor would report it, and the typed error must not.
async fn error_matches_wire(gateway: SocketAddr, secret: &str, key: &str) -> WireError {
    let relay = Relay::start_with(gateway, Downstream::Free, true).await;
    let client = S3Client::with_deadlines(&resolved(relay.addr, secret), deadlines());
    let err = match get(&client, key).await {
        Err(e) => e,
        Ok(_) => panic!("GET {key} succeeded; an error response was expected"),
    };
    let wire = parse_captured(&relay.stats.captured.lock().expect("capture lock"));
    let request_id = wire
        .request_id
        .clone()
        .expect("the gateway sent an x-amz-request-id");
    assert_ne!(
        request_id, DECOY_REQUEST_ID,
        "the decoy is the relay's, never the gateway's"
    );
    assert_eq!(
        err,
        S3Error::Service {
            status: wire.status,
            code: ErrorCode::Code(wire.code.clone()),
            message: wire.message.clone(),
            request_id: Some(request_id),
        },
        "every field of the typed error is the one on the wire"
    );
    wire
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_response_fields_equal_what_the_gateway_sent() {
    let gateway = start_gateway().await;

    let missing = error_matches_wire(gateway.addr, SECRET_KEY, "never-written").await;
    assert_eq!((missing.status, missing.code.as_str()), (404, "NoSuchKey"));

    let forged = error_matches_wire(gateway.addr, "not-the-secret", "never-written").await;
    assert_eq!(forged.status, 403, "a wrong signature is refused");
}

// --- criterion 3: streaming, bounded at every point ----------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_streams_within_the_window_at_every_pull() {
    let w = window();
    let len = streaming_payload(w);
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr, Downstream::Free).await;
    let client = client(relay.addr);

    let live = Arc::new(AtomicUsize::new(0));
    let probe = PutProbe::on_relay(&relay.stats);
    let source = Generated::new(len, PIECE).probed(&probe, &live);
    timeout(
        TRANSFER,
        client.put_object("streamed-put", PutSource::new(len, source)),
    )
    .await
    .expect("the streaming PUT finished within its limit")
    .expect("the streaming PUT succeeded");

    let pulls = probe.pulls.load(Ordering::SeqCst);
    assert!(
        pulls > len / PIECE as u64,
        "every piece was pulled, then the end: {pulls} pulls"
    );
    let (pull, produced, forwarded) = *probe.worst.lock().expect("probe lock");
    let max_lag = probe.max_lag.load(Ordering::SeqCst);
    assert!(
        max_lag <= w,
        "PUT lag: at pull {pull} the source had produced {produced} bytes but the relay had \
         forwarded {forwarded}; {max_lag} bytes held between them exceeds W = {w}"
    );
    let max_live = probe.max_live.load(Ordering::SeqCst);
    assert!(
        max_live <= K,
        "PUT retention: {max_live} of the source's pieces were alive at one pull; K = {K}"
    );
    assert_eq!(live.load(Ordering::SeqCst), 0, "every piece released");

    // The relay's payload count is the oracle's "forwarded": it must credit the object's
    // bytes exactly, never the aws-chunked framing or trailer. The relay credits a write
    // just after the gateway's socket accepts it, so give that last step a moment.
    let credited = || relay.stats.upstream_payload.load(Ordering::SeqCst);
    let started = Instant::now();
    while credited() != len && started.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        credited(),
        len,
        "the relay credited exactly the payload as forwarded"
    );

    // What was stored is the source, byte for byte (read back without the relay).
    let mut body = get(&client_direct(&gateway), "streamed-put")
        .await
        .expect("GET");
    expect_payload(&mut body, len).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_hands_over_pieces_within_the_window() {
    let w = window();
    let len = streaming_payload(w);
    let gateway = start_gateway().await;
    put_generated(&client_direct(&gateway), "streamed-get", len).await;

    let relay = Relay::start(gateway.addr, Downstream::Paced { window: w }).await;
    let client = client(relay.addr);
    let stalled = |offset: u64| {
        format!(
            "GET lag: the client handed over nothing for {STALL:?} at offset {offset} while the \
             relay had written {} body bytes, W = {w} past what the test had taken: it waits \
             for more than W before handing over a piece",
            relay.stats.downstream_body.load(Ordering::SeqCst)
        )
    };
    let mut body = timeout(STALL, client.get_object("streamed-get"))
        .await
        .unwrap_or_else(|_| panic!("{}", stalled(0)))
        .expect("GET");
    assert_eq!(body.content_length(), len);
    let mut offset = 0u64;
    loop {
        let piece = timeout(STALL, body.next_piece())
            .await
            .unwrap_or_else(|_| panic!("{}", stalled(offset)))
            .unwrap_or_else(|e| panic!("GET failed at offset {offset}: {e}"));
        let Some(piece) = piece else { break };
        assert!(
            piece[..] == payload(offset, piece.len())[..],
            "GET bytes differ within [{offset}, {})",
            offset + piece.len() as u64
        );
        offset += piece.len() as u64;
        relay.stats.took(piece.len());
    }
    assert_eq!(offset, len, "the whole object was handed over");
}

// --- criterion 4: integrity ----------------------------------------------------------------

/// PUT `key` from `source` declaring `declared` bytes: it must fail with `expected`, and the
/// key must stay absent.
async fn put_fails_and_stores_nothing<S>(
    client: &S3Client,
    key: &str,
    declared: u64,
    source: S,
    expected: BodyError,
) where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static,
{
    let outcome = timeout(
        STALL,
        client.put_object(key, PutSource::new(declared, source)),
    )
    .await
    .expect("the PUT ended within the limit");
    let err = outcome.expect_err("the PUT failed");
    assert_eq!(err, S3Error::Body(expected.clone()), "PUT {key}");
    let source = std::error::Error::source(&err).and_then(|e| e.downcast_ref::<BodyError>());
    assert_eq!(
        source,
        Some(&expected),
        "the body error is the error's source"
    );
    expect_no_such_key(client, key).await;
}

fn pieces(
    parts: Vec<Result<Bytes, std::io::Error>>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync {
    stream::iter(parts)
}

fn piece_at(offset: u64, len: usize) -> Result<Bytes, std::io::Error> {
    Ok(Bytes::from(payload(offset, len)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_source_that_ends_short_runs_long_or_fails_stores_nothing() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr);
    const P: usize = 64 * 1024;
    const L: u64 = P as u64;

    // Ends short: 128 KiB against a declared 256 KiB.
    put_fails_and_stores_nothing(
        &client,
        "short",
        4 * L,
        Generated::new(2 * L, P),
        BodyError::SourceLength {
            declared: 4 * L,
            produced: 2 * L,
        },
    )
    .await;

    // Runs long inside a piece: the second piece crosses the declared length.
    put_fails_and_stores_nothing(
        &client,
        "long-in-piece",
        L + 100,
        Generated::new(2 * L, P),
        BodyError::SourceLength {
            declared: L + 100,
            produced: 2 * L,
        },
    )
    .await;

    // Runs long in a separate piece: exactly the declared length, then one byte more.
    put_fails_and_stores_nothing(
        &client,
        "long-separate",
        2 * L,
        pieces(vec![piece_at(0, P), piece_at(L, P), piece_at(2 * L, 1)]),
        BodyError::SourceLength {
            declared: 2 * L,
            produced: 2 * L + 1,
        },
    )
    .await;

    // Fails after one piece.
    put_fails_and_stores_nothing(
        &client,
        "source-error",
        2 * L,
        pieces(vec![
            piece_at(0, P),
            Err(std::io::Error::other("the source failed on purpose")),
        ]),
        BodyError::SourceFailed {
            produced: L,
            detail: "the source failed on purpose".to_string(),
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_cut_mid_body_is_the_body_error_never_a_shorter_object() {
    let gateway = start_gateway().await;
    let len = 8 * GATEWAY_CHUNK as u64;
    put_generated(&client_direct(&gateway), "cut", len).await;

    let cut_at = GATEWAY_CHUNK as u64 + 13;
    let relay = Relay::start(gateway.addr, Downstream::CutAfter { body: cut_at }).await;
    let client = client(relay.addr);
    let mut body = get(&client, "cut").await.expect("GET head");
    assert_eq!(body.content_length(), len);
    let mut handed = 0u64;
    let err = loop {
        match timeout(STALL, body.next_piece()).await.expect("bounded") {
            Ok(Some(piece)) => handed += piece.len() as u64,
            Ok(None) => panic!("the cut GET ended as a {handed}-byte object of {len}"),
            Err(e) => break e,
        }
    };
    match err {
        S3Error::Body(BodyError::Transport {
            declared, received, ..
        }) => {
            assert_eq!(declared, len, "declared length");
            assert_eq!(received, handed, "the error counts what was handed over");
            assert_eq!(handed, cut_at, "everything before the cut was handed over");
        }
        other => panic!("expected the body error, got {other:?}"),
    }
    // The failure is sticky: the body never turns into an end.
    assert!(matches!(body.next_piece().await, Err(S3Error::Body(_))));
}

// --- criterion 4: bounded waits ------------------------------------------------------------

/// A listener whose accept queue is full: further connection attempts hang in SYN-SENT.
/// Returned with the queued connections so they stay open for the test.
async fn saturated_listener() -> (SocketAddr, TcpListener, Vec<TcpStream>) {
    let socket = TcpSocket::new_v4().expect("socket");
    socket
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let listener = socket.listen(1).expect("listen");
    let addr = listener.local_addr().expect("addr");
    let mut queued = Vec::new();
    while let Ok(connected) = timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
        queued.push(connected.expect("a queued connection"));
        assert!(queued.len() < 64, "the accept queue never filled");
    }
    (addr, listener, queued)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_deadline_expires_as_a_connect_timeout() {
    let (addr, _listener, _queued) = saturated_listener().await;
    let limits = Deadlines {
        connect: Duration::from_millis(500),
        ..deadlines()
    };
    let client = S3Client::with_deadlines(&resolved(addr, SECRET_KEY), limits);
    let started = Instant::now();
    let outcome = timeout(STALL, client.get_object("any"))
        .await
        .expect("bounded");
    assert_eq!(
        outcome.err(),
        Some(S3Error::Timeout {
            phase: Phase::Connect,
            limit: limits.connect,
        })
    );
    assert!(started.elapsed() >= limits.connect);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operation_deadline_expires_as_an_operation_timeout() {
    // A peer that accepts and never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let mut peer = JoinSet::new();
    peer.spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let limits = Deadlines {
        operation: Duration::from_millis(500),
        ..deadlines()
    };
    let client = S3Client::with_deadlines(&resolved(addr, SECRET_KEY), limits);
    let started = Instant::now();
    let outcome = timeout(STALL, client.get_object("any"))
        .await
        .expect("bounded");
    assert_eq!(
        outcome.err(),
        Some(S3Error::Timeout {
            phase: Phase::Operation,
            limit: limits.operation,
        })
    );
    assert!(started.elapsed() >= limits.operation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_idle_deadline_expires_as_a_body_idle_timeout() {
    let gateway = start_gateway().await;
    let len = 8 * GATEWAY_CHUNK as u64;
    put_generated(&client_direct(&gateway), "held", len).await;

    let hold_at = GATEWAY_CHUNK as u64;
    let relay = Relay::start(gateway.addr, Downstream::HoldAfter { body: hold_at }).await;
    let limits = Deadlines {
        body_idle: Duration::from_millis(500),
        ..deadlines()
    };
    let client = S3Client::with_deadlines(&resolved(relay.addr, SECRET_KEY), limits);
    let mut body = get(&client, "held").await.expect("GET head");
    let mut handed = 0u64;
    let err = loop {
        match timeout(STALL, body.next_piece()).await.expect("bounded") {
            Ok(Some(piece)) => handed += piece.len() as u64,
            Ok(None) => panic!("the held GET ended as a {handed}-byte object of {len}"),
            Err(e) => break e,
        }
    };
    assert_eq!(
        err,
        S3Error::Timeout {
            phase: Phase::BodyIdle,
            limit: limits.body_idle,
        }
    );
    assert_eq!(
        handed, hold_at,
        "everything before the held tail was handed over"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_connection_is_no_response_not_a_timeout() {
    // A port held by a socket that is bound but not listening: the kernel refuses every
    // connection to it at once, and no other test can take the port meanwhile.
    let holder = TcpSocket::new_v4().expect("socket");
    holder
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let addr = holder.local_addr().expect("addr");
    let client = client(addr);
    let started = Instant::now();
    let outcome = timeout(STALL, client.get_object("any"))
        .await
        .expect("bounded");
    assert!(
        matches!(outcome, Err(S3Error::NoResponse { .. })),
        "a refused connection is no response: {:?}",
        outcome.err()
    );
    assert!(
        started.elapsed() < deadlines().connect,
        "refused at once: no deadline ran"
    );
}

// --- criterion 4: a source that is always ready with empty pieces ---------------------------

/// How long a run below waits, after its PUT gave up, for the peer to see the connection
/// close.
const CLOSE_WAIT: Duration = Duration::from_secs(10);

/// What one run of [`empty_piece_put`] observed.
struct EmptyPieceRun {
    put: Result<PutOutcome, S3Error>,
    elapsed: Duration,
    /// Whether the peer saw the connection close within `CLOSE_WAIT` of the PUT giving up.
    closed: bool,
    /// The thread the run happened on. It ends once its runtime has shut down.
    thread: std::thread::JoinHandle<()>,
}

/// PUT a 10-byte object from a source that is always ready with empty pieces, to a peer
/// that reads whatever arrives and never answers, under the `operation` deadline, on
/// `runtime`.
///
/// The run happens on a thread of its own, and this thread waits for it on a plain channel:
/// if the body spun inside one poll, no timer on that runtime (neither the SDK's deadline
/// nor a `tokio::time::timeout` of the test's) could fire, so only a wait off the runtime
/// can report it. The caller owns the thread through its handle; only a run that never
/// reports is left behind, because a thread that spins cannot be joined.
fn empty_piece_put(runtime: tokio::runtime::Runtime, operation: Duration) -> EmptyPieceRun {
    let (report, outcome) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let run = runtime.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("addr");
            let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
            let mut peer = JoinSet::new();
            peer.spawn(async move {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = vec![0u8; RELAY_BUF];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                let _ = closed_tx.send(());
            });
            let limits = Deadlines {
                operation,
                ..deadlines()
            };
            let client = S3Client::with_deadlines(&resolved(addr, SECRET_KEY), limits);
            let source = stream::repeat_with(|| Ok::<_, std::io::Error>(Bytes::new()));
            let started = Instant::now();
            let put = client
                .put_object("empty-pieces", PutSource::new(10, source))
                .await;
            let elapsed = started.elapsed();
            let closed = matches!(timeout(CLOSE_WAIT, closed_rx).await, Ok(Ok(())));
            (put, elapsed, closed)
        });
        let _ = report.send(run);
    });
    match outcome.recv_timeout(STALL) {
        Ok((put, elapsed, closed)) => EmptyPieceRun {
            put,
            elapsed,
            closed,
            thread,
        },
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
            "a PUT from an always-ready source of empty pieces neither finished nor timed out \
             within {STALL:?}: its body spins inside one poll, where the {operation:?} \
             operation deadline cannot fire"
        ),
        // The run panicked before it could report: surface its panic.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(thread.join().expect_err("the run panicked"))
        }
    }
}

/// The run's PUT expires as the operation timeout, its connection is then closed rather than
/// left driving the abandoned body, and its runtime shuts down.
fn yields_to_the_operation_deadline(mut runtime: tokio::runtime::Builder) {
    let runtime = runtime.enable_all().build().expect("runtime");
    let operation = Duration::from_secs(1);
    let run = empty_piece_put(runtime, operation);
    assert_eq!(
        run.put.err(),
        Some(S3Error::Timeout {
            phase: Phase::Operation,
            limit: operation,
        })
    );
    assert!(run.elapsed >= operation, "the deadline ran its course");
    assert!(
        run.closed,
        "the connection was still open {CLOSE_WAIT:?} after the PUT gave up: the abandoned \
         body is still being driven"
    );
    // A multi-thread runtime shuts down only once every worker is back from the task it is
    // polling, so a task that never yields holds the run's thread here.
    let started = Instant::now();
    while !run.thread.is_finished() {
        assert!(
            started.elapsed() < STALL,
            "the run's runtime did not shut down within {STALL:?}: a task on it never yields"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    run.thread.join().expect("the run's thread ended cleanly");
}

#[test]
fn an_always_ready_empty_source_yields_to_the_deadline_on_a_current_thread_runtime() {
    yields_to_the_operation_deadline(tokio::runtime::Builder::new_current_thread());
}

#[test]
fn an_always_ready_empty_source_yields_to_the_deadline_on_a_multi_thread_runtime() {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(2);
    yields_to_the_operation_deadline(runtime);
}

// --- criterion 4: a request never built ----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_key_is_a_request_never_built() {
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr, Downstream::Free).await;
    let client = client(relay.addr);

    let probe = PutProbe::on_relay(&relay.stats);
    let live = Arc::new(AtomicUsize::new(0));
    let source = Generated::new(1024, PIECE).probed(&probe, &live);
    let put = timeout(STALL, client.put_object("", PutSource::new(1024, source)))
        .await
        .expect("bounded");
    assert!(
        matches!(put, Err(S3Error::RequestNotBuilt { .. })),
        "PUT with an empty key: {put:?}"
    );
    assert_eq!(
        probe.pulls.load(Ordering::SeqCst),
        0,
        "the source was never pulled"
    );

    let got = get(&client, "").await;
    assert!(
        matches!(got, Err(S3Error::RequestNotBuilt { .. })),
        "GET with an empty key: {:?}",
        got.err()
    );
    let deleted = timeout(STALL, client.delete_object(""))
        .await
        .expect("bounded");
    assert!(
        matches!(deleted, Err(S3Error::RequestNotBuilt { .. })),
        "DELETE with an empty key: {deleted:?}"
    );
    assert_eq!(
        relay.stats.connections.load(Ordering::SeqCst),
        0,
        "nothing reached the wire"
    );
}
