//! Issue #854: the S3 client's PUT against a peer that answers before it has the whole body,
//! or that stops reading. The production [`S3Client`] drives the real `aws-sdk-s3` over real
//! loopback TCP into a scripted peer: a listener that reads the request head and then does
//! exactly what its script says, nothing more.
//!
//! # The lifetime contract
//!
//! One contract, with fixed bounds, holds for every PUT here. The operation deadline is
//! `T_OP = 3 s`, and the test wraps each call in a `T_OP + 1 s` timeout of its own:
//!
//! * the call returns within `T_OP + 1 s`; not returning is a failure with a message, never a
//!   hang;
//! * at return the source has **already been dropped**: the test's own source type records
//!   when it is dropped, and the record is read in the very poll in which the call returns,
//!   with no grace period ([`AtReturn`]);
//! * the connection is **closed** within 1 s of the call's **actual return**, seen twice. The
//!   test stamps the return in that same poll and hands the stamp to the peer, which derives
//!   one absolute deadline from it, `return + 1 s`, for both sightings; a close seen after
//!   that deadline fails, an EOF or a reset included. First the client's own socket, in
//!   `/proc/net/tcp`, must leave `ESTABLISHED` while the peer still reads nothing: draining
//!   first would unblock a client stuck mid-write and let it finish on its own, which is not
//!   the client closing. Then the peer reads, discarding whatever the client had queued ahead
//!   of its FIN, until it hits EOF or a reset.
//!
//! A PUT the caller drops instead of awaiting is held to the same release: its source is
//! dropped, and its connection closed, within 1 s of the drop.
//!
//! # The scenarios
//!
//! 1. **No receipt while the source had not ended.** The peer answers `200` with
//!    `x-amz-request-id: early` as soon as it has read the request head, and stops reading.
//!    A 64 MiB source has not given its whole length by then: the PUT is the body error, and
//!    the error carries the acknowledgement's request id. More cases pin the moment the
//!    answer is read ([`Hold`]). A 10-byte source gives 4 bytes and is held; the peer answers;
//!    the source is ready to go on at its first poll after the client has read the answer off
//!    the socket. Ready to fail, it is "a source that fails after the acknowledgement"; ready
//!    to give its last 6 bytes, it is a final piece that is ready once the client has the
//!    answer. Both are the body error with the answer's request id, never a receipt, and the
//!    source is never polled past its hold. Each case runs twice: with an answer that has no
//!    body, and with one whose body the peer sends [`LATE_BODY_DELAY`] after its head, so the
//!    client is still reading the response, and hyper still polling the request body, after
//!    the request has seen the answer. A rejection whose body arrives late, to a source ready
//!    to fail, is the server's error, not the source's.
//! 2. **Early acknowledgement under backpressure.** The peer reads the head, waits until the
//!    client's writes back up (the client socket's send queue, read from `/proc/net/tcp`, is
//!    non-empty and has stopped moving), then answers `200`, keeps the socket open and never
//!    reads again. The source yields 32 MiB pieces of a 512 MiB object. The connection is then
//!    blocked mid-write, where hyper never polls the request body again, so waking the body is
//!    not enough to release it: the body error, plus the lifetime contract.
//! 3. **Stops reading, never answers.** The peer reads the head, stops reading, never answers
//!    and keeps the socket open; the same 32 MiB pieces. The typed operation timeout, plus the
//!    lifetime contract.
//! 4. **The receipt rule.** A receipt requires that the source had **ended** before the client
//!    read the answer: it gave its whole declared length and then reported its end. Three
//!    10-byte PUTs pin the boundary from both sides:
//!    * the source has reported its end before the peer answers `200`: a receipt
//!      ([`a_source_that_ended_before_the_acknowledgement_is_a_receipt`]);
//!    * the source has given all 10 bytes and holds its end when the peer answers: not a
//!      receipt, the body error with the answer's request id
//!      ([`a_source_that_gave_its_whole_length_but_held_its_end_is_not_a_receipt`]);
//!    * the source has given its declared 10 bytes and holds an 11th when the peer answers:
//!      not a receipt, the same body error. The extra byte is never taken, so the overrun is
//!      never seen
//!      ([`a_source_that_gave_its_declared_length_and_held_an_extra_byte_is_not_a_receipt`]).
//!
//!    The two held cases use the same [`Hold`] as scenario 1, so what releases the source is
//!    what the client has read, never what the peer has sent. The client takes nothing from
//!    its source once it has read the answer, so a source held that way is never polled past
//!    its hold while the call runs, and each case has one legal outcome whatever the
//!    scheduling.
//! 5. **Dropped mid-write.** The blocked connection of scenario 3, with no answer, is dropped
//!    by its caller, under an operation deadline far past anything the test waits for: the
//!    source and the connection are released within 1 s of the drop, so a dropped PUT does
//!    not run on until its deadline. The wait before the drop, for the peer to see the
//!    client's writes back up, has a bound of the test's own too ([`DROPPED_SETUP_BOUND`]): a
//!    request that never reaches the peer fails the test with a message, never hangs it.
//!
//! # What the host must offer
//!
//! Every test here watches the client's socket from outside, in the kernel's table of TCP
//! sockets, `/proc/net/tcp`. That is how the peer sees the client's socket closed while it
//! reads nothing, how it sees the client's writes back up, and how a held source learns that
//! the client has read the answer. Without the table each of those checks would fail open: a
//! socket the table does not show looks closed, and a hold that cannot open looks like a
//! source that was never polled past it. So no check falls back to a guess, and no test runs
//! on a host where the table does not show what the checks read from it. Every peer starts by
//! proving that it does, on a connection of its own ([`require_socket_observation`]), and the
//! test fails with a message saying so where it does not. A table that cannot be read later
//! fails the test the same way ([`tcp_table`]), and the peer must have seen the client's
//! socket established in the table before a socket missing from it counts as closed
//! ([`PeerReport::seen_open`]).
//!
//! # The stated limit, not tested
//!
//! The client sees what it handed to the HTTP stack and the moment it reads the answer off
//! the socket. It cannot see whether the peer read the bytes it was handed, and it cannot see
//! an answer that has reached its own kernel's receive buffer but that hyper has not read yet.
//! A source that ends inside that second window gets a receipt for an answer the peer sent
//! early. No test here asserts on that window, in either direction: the held cases release
//! their source only on what the client has read, which keeps them out of it.
//!
//! # Base compatibility
//!
//! The file names only #852's public API and dev-dependencies, so it compiles against #852's
//! client unchanged. That is why the body error is recognised without naming its variant,
//! which #852 does not have: the variant is read from the error's `Debug` text, and its
//! request id from its `Display`, which prints every request id the same way
//! (`x-amz-request-id <id>`).

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream::Stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at};
use wyrd_validate::{
    resolve_config, Deadlines, ErrorCode, Phase, PutOutcome, PutSource, ResolvedConfig, S3Client,
    S3Error,
};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "validate";
const KEY: &str = "upload-peers";

/// The operation deadline every PUT here runs under, but the one the test drops.
const T_OP: Duration = Duration::from_secs(3);
/// The operation deadline of the PUT the test drops: far past anything the test waits for,
/// so whatever releases that PUT's source and connection within `CLOSE_BOUND` of the drop is
/// the drop, never the deadline, however long the client takes to back up.
const DROPPED_T_OP: Duration = Duration::from_secs(60);
/// The test's own bound on one call: `T_OP + 1 s`.
const CALL_BOUND: Duration = Duration::from_secs(4);
/// How long after the call returns, or is dropped, the source may still be alive and the
/// connection open: zero for the source of a call that returned, this for everything else.
const CLOSE_BOUND: Duration = Duration::from_secs(1);

/// The request id every answer of the peer carries.
const REQUEST_ID: &str = "early";
/// The peer's acknowledgement: a success with no body.
const ACK: Reply = Reply {
    status: "200 OK",
    body: "",
};
/// An acknowledgement whose body arrives `LATE_BODY_DELAY` after its head.
const ACK_BODY_LATE: Reply = Reply {
    status: "200 OK",
    body: "ok",
};
/// A rejection whose `<Error>` document arrives `LATE_BODY_DELAY` after its head.
const REJECTION_BODY_LATE: Reply = Reply {
    status: "403 Forbidden",
    body: "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code>\
           <Message>Access Denied</Message><RequestId>early</RequestId></Error>",
};
/// How long after an answer's head the peer sends its body. Far longer than the client takes
/// to read the head and poll the request body again, hold check included ([`Hold`]), so a
/// source polled after the request has seen the answer is polled while the body is still on
/// its way. A check that outlasts it finds the body unread and holds the source, which can
/// only hide a source polled too late, never fail a client that polls it right.
const LATE_BODY_DELAY: Duration = Duration::from_millis(500);
// The peer does not hear that the call has ended while it waits out this delay, so a call
// that ends meanwhile is watched late. Kept to half of `CLOSE_BOUND`, the delay still leaves
// the peer half the bound to see the close; a longer one could fail a client that closed in
// time.
const _: () = assert!(LATE_BODY_DELAY.as_millis() * 2 <= CLOSE_BOUND.as_millis());

/// The held cases' 10-byte source and where scenario 1 holds it: 4 bytes in, 6 left.
const HELD_TOTAL: u64 = 10;
const HELD_AT: u64 = 4;

/// The piece size of the backpressure scenarios.
const LARGE_PIECE: usize = 32 * 1024 * 1024;
/// The object size of the backpressure scenarios: sixteen large pieces, far more than every
/// buffer between the source and the peer can hold.
const LARGE_OBJECT: u64 = 512 * 1024 * 1024;
/// `SO_RCVBUF` on the peer's sockets: small, so its receive window closes early (the kernel
/// doubles it, and setting it turns receive autotuning off).
const PEER_RCVBUF: u32 = 64 * 1024;
/// How long the client socket's send queue must hold still, non-empty, to count as backed
/// up, and how often it is sampled.
const BACKED_UP_STEADY: Duration = Duration::from_millis(200);
const BACKED_UP_SAMPLE: Duration = Duration::from_millis(25);
/// The longest the peer waits for the client's writes to back up before going on anyway.
/// Well inside `T_OP`, so an answer still arrives before the operation deadline.
const BACKED_UP_WAIT: Duration = Duration::from_secs(2);
/// The same for the PUT the test drops, which has no answer to deliver in time.
const DROPPED_BACKED_UP_WAIT: Duration = Duration::from_secs(10);
/// The longest the PUT the test drops may take to get its request head read by the peer:
/// the client's connection, the head, and the peer's read of it. Over loopback that takes
/// milliseconds, and the client's own connect deadline is 2 s; this only ends the wait for a
/// request that never gets there.
const DROPPED_HEAD_WAIT: Duration = Duration::from_secs(5);
/// The test's own bound on its wait before it drops that PUT: `DROPPED_HEAD_WAIT` for the
/// request head to reach the peer, then the peer's whole `DROPPED_BACKED_UP_WAIT`. A peer
/// that has read the head always says how its wait for backpressure ended, whatever it saw;
/// past this bound the test stops waiting for it, and for the PUT, and fails with a message.
const DROPPED_SETUP_BOUND: Duration = DROPPED_HEAD_WAIT.saturating_add(DROPPED_BACKED_UP_WAIT);
// The bound must run out before the PUT's own deadline can: a PUT that returned on that
// deadline would end the wait first, as a PUT that "returned before the test dropped it",
// and a drop made that late could not be told from the deadline anyway.
const _: () =
    assert!(DROPPED_SETUP_BOUND.as_millis() + CLOSE_BOUND.as_millis() < DROPPED_T_OP.as_millis());
/// The longest the peer waits for a held source to reach its hold before answering anyway.
const PARK_WAIT: Duration = Duration::from_secs(2);
/// The longest a held source waits, inside one poll, for the peer's write of its answer to
/// return once the peer has started it ([`Hold::answer_read`]).
const ANSWER_WRITE_WAIT: Duration = Duration::from_millis(100);

// --- the client ------------------------------------------------------------------------------

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
        ("--seed", "854"),
        ("--out", "unused"),
        ("--run-id", "s3-client-upload-peers"),
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

/// A client for `endpoint` whose operation deadline is `operation`.
fn client(endpoint: SocketAddr, operation: Duration) -> S3Client {
    let deadlines = Deadlines {
        connect: Duration::from_secs(2),
        operation,
        body_idle: Duration::from_secs(2),
    };
    S3Client::with_deadlines(&resolved(endpoint), deadlines)
}

// --- the source ------------------------------------------------------------------------------

/// What the test sees of its source from outside.
#[derive(Debug, Default)]
struct Probe {
    /// Bytes the source has given.
    given: AtomicU64,
    /// The source has reported its end.
    exhausted: AtomicBool,
    /// When the source was dropped.
    dropped_at: OnceLock<Instant>,
}

impl Probe {
    fn dropped(&self) -> bool {
        self.dropped_at.get().is_some()
    }
}

/// Where a held source stops, and what lets it go on.
///
/// Once the source has given the bytes before its hold, every poll asks whether the client
/// has **read** the peer's answer off the socket: the peer has written it, the client's
/// kernel has acknowledged every byte of it (the peer socket's send queue is empty), and
/// after that the client socket's receive queue is empty. Until then the poll is pending; the
/// first poll that finds the answer read goes on.
///
/// A pending poll arranges no wake of its own, and needs none. While the body waits, hyper is
/// waiting on the socket for the response, so the answer's arrival wakes the connection. In
/// that poll hyper reads the answer, parses it and hands it to the request in one go, and only
/// then polls the body (`hyper-1.10.1/src/proto/h1/io.rs:183-219`, `dispatch.rs:173-174`).
/// Only hyper reads that socket. So the source goes on only after hyper has handed the
/// response over, at the first poll of the body after that: the moment the receipt rule turns
/// on, reached without any timing. A poll that comes sooner, for any other reason, is
/// harmless: it finds the answer unread and stays pending.
///
/// The source does not wake itself to be polled sooner: that would keep the client's runtime
/// busy polling it, each poll reading `/proc/net/tcp`, and tokio looks at the socket's events
/// only between runs of such polls.
///
/// The source can still be polled after the answer has arrived and before hyper has read it:
/// another wake of the connection (its socket turning writable, say) can run it before tokio
/// has seen the answer arrive. That check finds the answer unread, and while it runs hyper
/// cannot read either, since both run on the client's one thread. A check reads the kernel's
/// whole table of TCP sockets, so its cost grows with the table. The checks keep that cost
/// small: the first read stops a check that finds the answer unread, and the peer closes every
/// connection with a reset ([`serve`]), so the test leaves none of its sockets in the table.
///
/// A hold that could never open would hide a source polled too late: such a source finds the
/// hold shut and stays put, exactly like one that was never polled. So no test runs before
/// this check has been seen to open, on this host, for an answer that was read
/// ([`require_socket_observation`]), and a table that cannot be read fails the test rather
/// than keep the hold shut ([`tcp_table`]).
#[derive(Debug, Default)]
struct Hold {
    /// The client's and the peer's ends of the connection, set as the peer accepts it.
    link: OnceLock<(SocketAddr, SocketAddr)>,
    /// The source has reached its hold.
    parked: AtomicBool,
    /// The peer has started writing its answer.
    answering: AtomicBool,
    /// The peer's write of its answer has returned.
    answered: AtomicBool,
    /// The source went on past its hold.
    went_on: AtomicBool,
}

impl Hold {
    /// Whether the client has read the peer's answer off the socket.
    fn answer_read(&self) -> bool {
        if !self.answering.load(Ordering::SeqCst) {
            return false;
        }
        // The peer's write goes into an empty send buffer and returns at once. When the client
        // runs on a thread of its own, wait for that return here rather than end this poll:
        // ending it now would let the request see the answer before the source had its turn.
        // On a thread shared with the peer the wait cannot succeed and ends at its bound.
        let started = Instant::now();
        while !self.answered.load(Ordering::SeqCst) {
            if started.elapsed() >= ANSWER_WRITE_WAIT {
                return false;
            }
            std::thread::yield_now();
        }
        let Some(&(client, peer)) = self.link.get() else {
            return false;
        };
        // Bytes waiting in the client's receive queue can only be the answer, unread: one
        // table read settles it. That is the usual finding when the source is polled after the
        // answer has arrived but before hyper has read it.
        if tcp_socket(client, peer).is_none_or(|end| end.recv_queue > 0) {
            return false;
        }
        // Delivered first, then read: one table read for each, in that order, so an empty
        // receive queue is never mistaken for an answer not yet arrived.
        let delivered = tcp_socket(peer, client).is_some_and(|end| end.send_queue == 0);
        delivered && tcp_socket(client, peer).is_some_and(|end| end.recv_queue == 0)
    }
}

/// A source's hold, and what it does once it goes on.
struct Held {
    /// Bytes given before the hold.
    at: u64,
    hold: Arc<Hold>,
    /// Going on, it fails rather than giving the rest.
    then_fail: bool,
}

/// A source of `total` bytes in pieces of up to `piece.len()`, each a slice of one shared
/// buffer, so a large piece costs no allocation. Dropping it is recorded on the probe.
struct Source {
    total: u64,
    piece: Bytes,
    given: u64,
    held: Option<Held>,
    probe: Arc<Probe>,
}

impl Source {
    fn new(total: u64, piece: usize) -> (Self, Arc<Probe>) {
        let probe = Arc::new(Probe::default());
        let source = Self {
            total,
            piece: Bytes::from(vec![0x5a; piece]),
            given: 0,
            held: None,
            probe: Arc::clone(&probe),
        };
        (source, probe)
    }

    /// Hold after `at` bytes until the client has read the answer ([`Hold`]), then fail if
    /// `then_fail`, else give the rest.
    fn held_at(mut self, at: u64, hold: &Arc<Hold>, then_fail: bool) -> Self {
        self.held = Some(Held {
            at,
            hold: Arc::clone(hold),
            then_fail,
        });
        self
    }
}

impl Stream for Source {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut limit = this.total;
        if let Some(held) = &this.held {
            let hold = &held.hold;
            if this.given < held.at {
                limit = held.at;
            } else if !hold.went_on.load(Ordering::SeqCst) {
                hold.parked.store(true, Ordering::SeqCst);
                if !hold.answer_read() {
                    // No wake of its own: see `Hold`.
                    return Poll::Pending;
                }
                hold.went_on.store(true, Ordering::SeqCst);
                if held.then_fail {
                    return Poll::Ready(Some(Err(std::io::Error::other(
                        "the source failed after the acknowledgement",
                    ))));
                }
            }
        }
        let left = limit - this.given;
        if left == 0 {
            this.probe.exhausted.store(true, Ordering::SeqCst);
            return Poll::Ready(None);
        }
        let len = left.min(this.piece.len() as u64) as usize;
        this.given += len as u64;
        this.probe.given.store(this.given, Ordering::SeqCst);
        Poll::Ready(Some(Ok(this.piece.slice(..len))))
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.probe.dropped_at.set(Instant::now());
    }
}

// --- the scripted peer -----------------------------------------------------------------------

/// An answer of the peer: its head, written at once, then its body, if any, written
/// `LATE_BODY_DELAY` later. The head declares the body's length and carries
/// `x-amz-request-id: early`.
#[derive(Debug, Clone, Copy)]
struct Reply {
    status: &'static str,
    body: &'static str,
}

impl Reply {
    fn head(&self) -> Vec<u8> {
        format!(
            "HTTP/1.1 {}\r\nContent-Length: {}\r\nx-amz-request-id: {REQUEST_ID}\r\n\r\n",
            self.status,
            self.body.len()
        )
        .into_bytes()
    }
}

/// When the peer answers, after it has read the request head. Every script that answers
/// answers [`ACK`], but the held one, which names its answer.
enum Answer {
    /// At once.
    AtHead,
    /// Once the held source has reached its hold.
    WhenHeld(Arc<Hold>, Reply),
    /// Once the source has given its whole length and reported its end.
    WhenExhausted(Arc<Probe>),
    /// Once the client's writes have backed up.
    WhenBackedUp,
    /// Never.
    Never,
    /// Never; once the client's writes have backed up, it says so on the channel.
    NeverTellingBackedUp(oneshot::Sender<BackedUp>),
}

/// Whether the client's writes backed up before the peer answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackedUp {
    /// Not asked for by the script.
    NotWaited,
    /// The client socket's send queue held this many bytes, unmoving.
    Yes(u64),
    /// The send queue never held still within the peer's wait; the last sample.
    No(Option<u64>),
}

/// Whether a held source had reached its hold when the peer answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Parked {
    /// Not asked for by the script.
    NotWaited,
    /// It had; the peer wrote its answer after.
    BeforeAnswer,
    /// It never did within `PARK_WAIT`.
    Never,
}

/// Whether the client closed its end, judged from its socket in `/proc/net/tcp` while the
/// peer reads nothing. Every duration here and in [`Closed`] counts from the moment the call
/// ended: its actual return, or its drop, as the test stamped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientClose {
    /// The socket was seen out of `ESTABLISHED`, or gone, this long after the call ended. The
    /// sighting may be late: [`assert_connection_closed`] holds it to `CLOSE_BOUND`.
    After(Duration),
    /// Still `ESTABLISHED` `CLOSE_BOUND` after the call ended.
    Not,
}

/// How the peer's connection ended, as seen by reads after the call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Closed {
    /// EOF, read `after` the call ended, past `drained` bytes the client had queued. The read
    /// may be late: [`assert_connection_closed`] holds it to `CLOSE_BOUND`.
    Eof { drained: u64, after: Duration },
    /// The client reset the connection; the same.
    Reset { drained: u64, after: Duration },
    /// Still open `CLOSE_BOUND` after the call ended; `drained` bytes read meanwhile.
    StillOpen { drained: u64 },
}

impl Closed {
    /// How long after the call ended the peer saw its connection closed, if it did.
    fn closed_after(self) -> Option<Duration> {
        match self {
            Self::Eof { after, .. } | Self::Reset { after, .. } => Some(after),
            Self::StillOpen { .. } => None,
        }
    }
}

/// What the peer observed.
#[derive(Debug)]
struct PeerReport {
    /// The table listed the client's socket as established once the peer had read the request
    /// head, before it answered. Only a socket the table has shown open can later be seen
    /// closed in it: without this sighting, a socket missing from the table proves nothing.
    seen_open: bool,
    backed_up: BackedUp,
    parked: Parked,
    answered: bool,
    /// `None` when the answer had no body; else whether the peer's late write of it succeeded.
    late_body_written: Option<bool>,
    client_close: ClientClose,
    closed: Closed,
}

/// A loopback peer serving one connection by its script. Dropping it aborts its task, which
/// closes whatever socket it still holds.
struct Peer {
    addr: SocketAddr,
    /// Carries the moment the call ended, as the test stamped it.
    ended: Option<oneshot::Sender<Instant>>,
    report: Option<oneshot::Receiver<PeerReport>>,
    task: JoinHandle<()>,
}

impl Peer {
    /// Start a peer playing `answer`. Every test starts one, and none starts on a host where
    /// the fixture cannot watch a socket ([`require_socket_observation`]).
    async fn start(answer: Answer) -> Self {
        require_socket_observation().await;
        let socket = TcpSocket::new_v4().expect("socket");
        socket.set_recv_buffer_size(PEER_RCVBUF).expect("SO_RCVBUF");
        socket
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("bind");
        let listener = socket.listen(16).expect("listen");
        let addr = listener.local_addr().expect("addr");
        let (ended_tx, ended_rx) = oneshot::channel();
        let (report_tx, report_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            if let Some(report) = serve(listener, answer, ended_rx).await {
                let _ = report_tx.send(report);
            }
        });
        Self {
            addr,
            ended: Some(ended_tx),
            report: Some(report_rx),
            task,
        }
    }

    /// Tell the peer the call ended at `ended_at` (it returned, or was dropped), and collect
    /// what the peer saw. `None` if it never got a connection to watch.
    async fn after_call(&mut self, ended_at: Instant) -> Option<PeerReport> {
        if let Some(ended) = self.ended.take() {
            let _ = ended.send(ended_at);
        }
        let report = self.report.take()?;
        // The peer stops watching at `ended_at + CLOSE_BOUND`; the margin covers scheduling
        // only, and buys the connection nothing: a close seen past the peer's deadline fails
        // whenever the report arrives.
        timeout(CLOSE_BOUND + Duration::from_secs(1), report)
            .await
            .ok()?
            .ok()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve one connection by `answer`, then watch it close. `None` if the test ended without
/// saying when its call ended: there is then no moment to measure the close from.
async fn serve(
    listener: TcpListener,
    answer: Answer,
    ended: oneshot::Receiver<Instant>,
) -> Option<PeerReport> {
    let (mut stream, client) = listener.accept().await.expect("accept");
    // Dropped only once the peer has watched the call end and the connection close, and then
    // with a reset: neither end stays in `TIME_WAIT`, so the table of TCP sockets every hold
    // check reads ([`Hold`]) stays small however many PUTs run, here or alongside.
    stream.set_zero_linger().expect("SO_LINGER 0");
    let peer = stream.local_addr().expect("peer addr");
    read_head(&mut stream).await;
    // The client is waiting for an answer, or still sending: its socket is open, and the
    // table must say so before the same lookup can later say it closed.
    let seen_open = established(client, peer);
    let mut backed_up = BackedUp::NotWaited;
    let mut parked = Parked::NotWaited;
    let mut held = None;
    let reply = match answer {
        Answer::AtHead => Some(ACK),
        Answer::WhenHeld(hold, reply) => {
            let _ = hold.link.set((client, peer));
            parked = wait_parked(&hold).await;
            held = Some(hold);
            Some(reply)
        }
        Answer::WhenExhausted(probe) => {
            while !probe.exhausted.load(Ordering::SeqCst) {
                sleep(Duration::from_millis(5)).await;
            }
            Some(ACK)
        }
        Answer::WhenBackedUp => {
            backed_up = wait_backed_up(client, peer, BACKED_UP_WAIT).await;
            Some(ACK)
        }
        Answer::Never => None,
        Answer::NeverTellingBackedUp(tell) => {
            backed_up = wait_backed_up(client, peer, DROPPED_BACKED_UP_WAIT).await;
            let _ = tell.send(backed_up);
            None
        }
    };
    let mut late_body_written = None;
    if let Some(reply) = reply {
        if let Some(hold) = &held {
            hold.answering.store(true, Ordering::SeqCst);
        }
        stream
            .write_all(&reply.head())
            .await
            .expect("write the answer's head");
        if let Some(hold) = &held {
            hold.answered.store(true, Ordering::SeqCst);
        }
        if !reply.body.is_empty() {
            sleep(LATE_BODY_DELAY).await;
            // Not unwrapped: a client that has already given up on the answer may have closed
            // the connection, and the report says so instead of a panic here.
            late_body_written = Some(stream.write_all(reply.body.as_bytes()).await.is_ok());
        }
    }
    // Stop reading and hold the socket open until the call has ended.
    let ended_at = ended.await.ok()?;
    // One absolute deadline for both sightings of the close, counted from the moment the call
    // ended, not from the moment this task heard of it.
    let deadline = ended_at + CLOSE_BOUND;
    let client_close = wait_client_close(client, peer, ended_at, deadline).await;
    let closed = drain_until_closed(&mut stream, ended_at, deadline).await;
    Some(PeerReport {
        seen_open,
        backed_up,
        parked,
        answered: reply.is_some(),
        late_body_written,
        client_close,
        closed,
    })
}

/// Read until the end of the request head. Body bytes that arrive with it are discarded.
async fn read_head(stream: &mut TcpStream) {
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await.expect("read the request head");
        assert!(n > 0, "the client closed before sending a request head");
        seen.extend_from_slice(&buf[..n]);
        assert!(seen.len() < 1 << 20, "no end of the request head in 1 MiB");
    }
}

/// Wait until the held source has reached its hold.
async fn wait_parked(hold: &Hold) -> Parked {
    let deadline = Instant::now() + PARK_WAIT;
    while !hold.parked.load(Ordering::SeqCst) {
        if Instant::now() >= deadline {
            return Parked::Never;
        }
        sleep(Duration::from_millis(1)).await;
    }
    Parked::BeforeAnswer
}

/// Wait until the client socket's send queue is non-empty and holds still for
/// `BACKED_UP_STEADY`: on loopback a queue only stays non-empty while the peer's receive
/// window is closed, and it only stops growing once the client has stopped writing. Gives up
/// after `wait`.
///
/// Held still is timed by the clock, from the first sample of the unchanged value, not by a
/// count of samples: one read of the table can take a large part of `BACKED_UP_STEADY` when
/// it is long and the host busy, and a count would then outlast `wait`.
async fn wait_backed_up(client: SocketAddr, peer: SocketAddr, wait: Duration) -> BackedUp {
    let deadline = Instant::now() + wait;
    let mut last = None;
    let mut since = Instant::now();
    while Instant::now() < deadline {
        let queued = tcp_socket(client, peer).map(|socket| socket.send_queue);
        let sampled = Instant::now();
        if queued.is_some_and(|q| q > 0) && queued == last {
            if sampled.duration_since(since) >= BACKED_UP_STEADY {
                return BackedUp::Yes(queued.unwrap_or_default());
            }
        } else {
            since = sampled;
        }
        last = queued;
        sleep(BACKED_UP_SAMPLE).await;
    }
    BackedUp::No(last)
}

/// Wait, reading nothing, until the client's socket has left `ESTABLISHED` or is gone: the
/// client has closed its end, whether or not its FIN can reach the peer yet. The sighting is
/// timed from `ended_at`, and stamped once the table has been read: the socket was closed by
/// then at the latest.
///
/// A socket the table does not list counts as gone. That is sound only because the table was
/// seen to list this socket while it was open ([`PeerReport::seen_open`], which [`watched`]
/// requires of every report the close is judged on), and because a table that cannot be read
/// fails the test instead of listing nothing ([`tcp_table`]).
async fn wait_client_close(
    client: SocketAddr,
    peer: SocketAddr,
    ended_at: Instant,
    deadline: Instant,
) -> ClientClose {
    loop {
        let open = established(client, peer);
        let seen = Instant::now();
        if !open {
            return ClientClose::After(seen.saturating_duration_since(ended_at));
        }
        if seen >= deadline {
            return ClientClose::Not;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

// --- what the host must offer ----------------------------------------------------------------

/// The kernel's table of IPv4 TCP sockets.
const TCP_TABLE: &str = "/proc/net/tcp";

/// The `st` value of an established connection in the table.
const TCP_ESTABLISHED: u8 = 0x01;

/// How long [`require_socket_observation`] waits for the table to show each thing it asks
/// for. On loopback each shows at once; the bound only ends the wait on a host where one
/// never does.
const OBSERVATION_WAIT: Duration = Duration::from_secs(2);

/// The failure message of a test that this host cannot run.
fn unsupported(what: &str) -> String {
    format!(
        "this host cannot run this test: {what}. Every check here watches the client's socket \
         in {TCP_TABLE}; where that cannot be done a pass could not be told from a miss, so \
         the test fails here and nothing was tested."
    )
}

/// One end of a loopback connection as the kernel's table of IPv4 TCP sockets lists it.
struct TcpEntry {
    /// The `st` column.
    state: u8,
    /// The `tx_queue` column: bytes written and not yet acknowledged by the other end.
    send_queue: u64,
    /// The `rx_queue` column: bytes received and not yet read by this end's owner.
    recv_queue: u64,
}

/// Read the table, or fail the test. An unreadable table is never passed on as an empty one:
/// to every check here a socket the table does not list is a socket that is gone, and a host
/// that hides the table would then pass them all.
fn tcp_table() -> String {
    std::fs::read_to_string(TCP_TABLE).unwrap_or_else(|e| {
        panic!(
            "{}",
            unsupported(&format!("{TCP_TABLE} cannot be read ({e})"))
        )
    })
}

/// The end of a loopback connection at `local` facing `remote`, `None` once the kernel no
/// longer lists it.
fn tcp_socket(local: SocketAddr, remote: SocketAddr) -> Option<TcpEntry> {
    let table = tcp_table();
    let local = format!("0100007F:{:04X}", local.port());
    let remote = format!("0100007F:{:04X}", remote.port());
    table.lines().skip(1).find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if *fields.get(1)? != local.as_str() || *fields.get(2)? != remote.as_str() {
            return None;
        }
        let state = u8::from_str_radix(fields.get(3)?, 16).ok()?;
        let (tx, rx) = fields.get(4)?.split_once(':')?;
        let send_queue = u64::from_str_radix(tx, 16).ok()?;
        let recv_queue = u64::from_str_radix(rx, 16).ok()?;
        Some(TcpEntry {
            state,
            send_queue,
            recv_queue,
        })
    })
}

/// Whether the table lists the end of a connection at `local` facing `remote` as established.
fn established(local: SocketAddr, remote: SocketAddr) -> bool {
    tcp_socket(local, remote).is_some_and(|end| end.state == TCP_ESTABLISHED)
}

/// Poll `seen` until it holds, for `OBSERVATION_WAIT` at most.
async fn observed(mut seen: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + OBSERVATION_WAIT;
    loop {
        if seen() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(1)).await;
    }
}

/// One step on [`require_socket_observation`]'s own connection, bounded like its waits: a
/// step that fails or does not finish fails the test with a message, never hangs it.
async fn step<T>(what: &str, io: impl Future<Output = std::io::Result<T>>) -> T {
    match timeout(OBSERVATION_WAIT, io).await {
        Ok(Ok(done)) => done,
        Ok(Err(e)) => panic!("{}", unsupported(&format!("it could not {what} ({e})"))),
        Err(_) => panic!(
            "{}",
            unsupported(&format!("it could not {what} within {OBSERVATION_WAIT:?}"))
        ),
    }
}

/// Prove that this host lets the fixture watch a socket, or fail the test with a message.
///
/// A readable table is not enough: one that listed no socket of this test, or showed every
/// queue empty, would fail the checks open just the same. So this opens a loopback connection
/// of its own and asks the table, through the lookups the checks themselves use, for
/// everything they lean on:
///
/// * both ends are listed as established while the connection is open, which is what makes
///   a socket no longer listed that way a closed one ([`wait_client_close`]);
/// * bytes sent and not yet read show in the reader's receive queue, and the hold check takes
///   them for an answer the client has not read ([`Hold::answer_read`]);
/// * once they are read, that same check says so: a hold can open here, so a source that was
///   never seen past its hold was never polled there
///   ([`assert_not_polled_past_the_hold`]);
/// * an end that has been closed is no longer listed as established.
///
/// A send queue that backs up needs no such proof: it is only ever taken from a sample that
/// showed it non-empty ([`BackedUp::Yes`]).
///
/// Both sockets are closed with a reset, like the peer's own ([`serve`]), so nothing of this
/// stays in the table.
async fn require_socket_observation() {
    const ANSWER: &[u8] = b"answer";
    let listener = step("bind a loopback listener", TcpListener::bind("127.0.0.1:0")).await;
    let listening = listener.local_addr().expect("addr");
    // The reader plays the client and the writer the peer, as `Hold` names the two ends.
    let mut writer = step("connect over loopback", TcpStream::connect(listening)).await;
    let (mut reader, _) = step("accept a loopback connection", listener.accept()).await;
    writer.set_zero_linger().expect("SO_LINGER 0");
    reader.set_zero_linger().expect("SO_LINGER 0");
    let client = reader.local_addr().expect("reader addr");
    let peer = writer.local_addr().expect("writer addr");
    assert!(
        observed(|| established(client, peer) && established(peer, client)).await,
        "{}",
        unsupported(&format!(
            "{TCP_TABLE} does not list both ends of an open loopback connection of this test \
             as established"
        ))
    );

    step("write over loopback", writer.write_all(ANSWER)).await;
    let unread = ANSWER.len() as u64;
    assert!(
        observed(|| tcp_socket(client, peer).is_some_and(|end| end.recv_queue == unread)).await,
        "{}",
        unsupported(&format!(
            "{TCP_TABLE} does not show {unread} bytes that were sent and not yet read in the \
             reader's receive queue"
        ))
    );
    let hold = Hold::default();
    let _ = hold.link.set((client, peer));
    hold.answering.store(true, Ordering::SeqCst);
    hold.answered.store(true, Ordering::SeqCst);
    assert!(
        !hold.answer_read(),
        "{}",
        unsupported("the hold check takes an answer nobody has read for one the client has read")
    );

    let mut answer = [0u8; ANSWER.len()];
    step("read over loopback", reader.read_exact(&mut answer)).await;
    assert!(
        observed(|| hold.answer_read()).await,
        "{}",
        unsupported("the hold check never sees that an answer has been read, so no hold can open")
    );

    drop(reader);
    assert!(
        observed(|| !established(client, peer)).await,
        "{}",
        unsupported(&format!(
            "{TCP_TABLE} still lists a socket as established after it was closed"
        ))
    );
}

/// Read, discarding, until EOF or a reset, until `deadline`. The close is timed from
/// `ended_at` and stamped once the read has returned. A read that is ready is taken even when
/// the deadline has passed, so the stamp can lie past it; the stamp is what is judged.
async fn drain_until_closed(
    stream: &mut TcpStream,
    ended_at: Instant,
    deadline: Instant,
) -> Closed {
    let deadline = tokio::time::Instant::from_std(deadline);
    let mut buf = vec![0u8; 256 * 1024];
    let mut drained = 0u64;
    loop {
        let read = timeout_at(deadline, stream.read(&mut buf)).await;
        let after = Instant::now().saturating_duration_since(ended_at);
        match read {
            Ok(Ok(0)) => return Closed::Eof { drained, after },
            Ok(Ok(n)) => drained += n as u64,
            Ok(Err(_)) => return Closed::Reset { drained, after },
            Err(_) => return Closed::StillOpen { drained },
        }
    }
}

// --- one PUT against one peer ----------------------------------------------------------------

/// What the test read in the very poll in which the call returned. No await lies between
/// the call's return and these reads, so nothing on the test's runtime runs in between and a
/// source released late gets no time to catch up: the "no grace period" of the lifetime
/// contract.
struct AtReturn {
    /// The call's actual return, the moment the connection's close is timed from.
    at: Instant,
    dropped: bool,
    given: u64,
    ended: bool,
}

impl AtReturn {
    fn read(probe: &Probe) -> Self {
        Self {
            at: Instant::now(),
            dropped: probe.dropped(),
            given: probe.given.load(Ordering::SeqCst),
            ended: probe.exhausted.load(Ordering::SeqCst),
        }
    }
}

/// Everything one call was observed to do.
#[derive(Debug)]
struct Run {
    outcome: Result<PutOutcome, S3Error>,
    declared: u64,
    elapsed: Duration,
    /// Whether the source had been dropped when the call returned ([`AtReturn`]).
    dropped_at_return: bool,
    /// Bytes the source had given by then.
    given_at_return: u64,
    /// Whether the source had reported its end by then.
    ended_at_return: bool,
    peer: Option<PeerReport>,
}

/// PUT `declared` bytes from `source` against a peer playing `answer`, then let the peer
/// watch for the close.
async fn put_against(answer: Answer, declared: u64, source: Source, probe: &Probe) -> Run {
    let mut peer = Peer::start(answer).await;
    let client = client(peer.addr, T_OP);
    let started = Instant::now();
    let call = timeout(CALL_BOUND, async {
        let outcome = client
            .put_object(KEY, PutSource::new(declared, source))
            .await;
        (outcome, AtReturn::read(probe))
    })
    .await;
    let Ok((outcome, at_return)) = call else {
        panic!(
            "put_object did not return within {CALL_BOUND:?} (T_op {T_OP:?} + 1 s); the \
             source had given {} of {declared} bytes",
            probe.given.load(Ordering::SeqCst)
        );
    };
    let report = peer.after_call(at_return.at).await;
    Run {
        outcome,
        declared,
        elapsed: at_return.at.duration_since(started),
        dropped_at_return: at_return.dropped,
        given_at_return: at_return.given,
        ended_at_return: at_return.ended,
        peer: report,
    }
}

/// The lifetime contract: returned in time, source dropped at return, connection closed
/// within `CLOSE_BOUND` of the return.
fn assert_lifetime(run: &Run) {
    assert!(
        run.elapsed <= CALL_BOUND,
        "the call took {:?}, past T_op + 1 s: {run:?}",
        run.elapsed
    );
    assert!(
        run.dropped_at_return,
        "the source was still alive when put_object returned: the upload outlived its call. \
         {run:?}"
    );
    assert_connection_closed(run.peer.as_ref(), run);
}

/// The peer's report, from a peer that saw the client's socket in the table while the request
/// was open. The two checks that look that socket up later rest on this sighting, because
/// both read a socket missing from the table their own way: as a closed one
/// ([`wait_client_close`]), and as one that has not read the answer ([`Hold::answer_read`]).
fn watched<'a>(report: Option<&'a PeerReport>, context: &dyn std::fmt::Debug) -> &'a PeerReport {
    let report =
        report.unwrap_or_else(|| panic!("the peer never reported on its connection: {context:?}"));
    assert!(
        report.seen_open,
        "the peer did not see the client's socket established in {TCP_TABLE} once it had read \
         the request head: the client had closed it by then, or the table does not show it. \
         That socket missing from the table later proves neither a close nor an unread answer, \
         so nothing was tested. {report:?}; {context:?}"
    );
    report
}

/// The client closed the connection, and the peer saw it closed, within `CLOSE_BOUND` of the
/// moment the call ended. A close seen later than that fails like no close at all, whether
/// the peer saw it as an EOF or as a reset.
fn assert_connection_closed(report: Option<&PeerReport>, context: &dyn std::fmt::Debug) {
    let report = watched(report, context);
    let client_closed_in_time = match report.client_close {
        ClientClose::After(after) => after <= CLOSE_BOUND,
        ClientClose::Not => false,
    };
    assert!(
        client_closed_in_time,
        "the client's socket was not seen out of ESTABLISHED within {CLOSE_BOUND:?} of the \
         call's end: the client had not closed the connection in time. {report:?}; {context:?}"
    );
    assert!(
        report
            .closed
            .closed_after()
            .is_some_and(|after| after <= CLOSE_BOUND),
        "the peer did not see its connection closed within {CLOSE_BOUND:?} of the call's end. \
         {report:?}; {context:?}"
    );
}

/// Whether the peer answered, as its script says it should have.
fn assert_answered(run: &Run, expected: bool) {
    let answered = run.peer.as_ref().map(|report| report.answered);
    assert_eq!(
        answered,
        Some(expected),
        "the peer's answer is not what its script says: {run:?}"
    );
}

/// The fixture of the held cases: the source had reached its hold before the peer answered.
fn assert_parked_before_answer(run: &Run) {
    let parked = run.peer.as_ref().map(|report| report.parked);
    assert_eq!(
        parked,
        Some(Parked::BeforeAnswer),
        "the source never reached its hold, so this run did not answer while the source held \
         back: {run:?}"
    );
}

/// The fixture of the held cases with bytes left: the source had reached its hold, short of
/// its declared length, before the peer answered.
fn assert_held_before_answer(run: &Run, at: u64) {
    assert_parked_before_answer(run);
    assert!(
        at < run.declared,
        "a hold at the declared length holds no bytes back: {run:?}"
    );
}

/// The fixture of the late-body cases: the peer wrote its answer's body `LATE_BODY_DELAY`
/// after the head, so the client was still reading the response well after it had the head.
fn assert_body_sent_late(run: &Run) {
    let written = run
        .peer
        .as_ref()
        .and_then(|report| report.late_body_written);
    assert_eq!(
        written,
        Some(true),
        "the peer never wrote its answer's body after the head, so this run did not answer \
         with a body still on its way: {run:?}"
    );
}

/// From the moment the client had the answer the source was not polled again: it never went
/// on past its hold, and gave nothing past it.
///
/// A hold that could not open would say the same of a source polled too late, so this claim
/// rests on two sightings: the hold check opens on this host once an answer has been read
/// ([`require_socket_observation`], which every peer runs before it starts), and the table
/// showed this run's own client socket ([`watched`]).
fn assert_not_polled_past_the_hold(run: &Run, hold: &Hold, at: u64) {
    watched(run.peer.as_ref(), run);
    assert!(
        !hold.went_on.load(Ordering::SeqCst),
        "the source was polled past its hold, after the client had read the answer: {run:?}"
    );
    assert_eq!(
        run.given_at_return, at,
        "the source gave bytes past its hold, after the client had read the answer: {run:?}"
    );
}

/// The outcome is the body error for an acknowledgement that came before the source had
/// ended, `BodyError::AcknowledgedEarly`, and carries the acknowledgement's request id. The
/// variant is read from the error's `Debug` text and the request id from its `Display`: #852's
/// client has no such variant, and naming it would stop this file compiling there.
fn assert_body_error_carrying_the_answer(run: &Run) {
    let Err(err) = &run.outcome else {
        panic!(
            "an acknowledgement that arrived before the source had ended must be the body \
             error carrying request id `{REQUEST_ID}`, never a receipt: {run:?}"
        );
    };
    assert!(
        format!("{err:?}").starts_with("Body(AcknowledgedEarly {"),
        "an acknowledgement that arrived before the source had ended must be the body error \
         for exactly that, `AcknowledgedEarly`: {run:?}"
    );
    assert!(
        err.to_string()
            .contains(&format!("x-amz-request-id {REQUEST_ID}")),
        "the body error does not carry the acknowledgement's request id: {err}. {run:?}"
    );
}

// --- scenario 1: no receipt while the source had not ended ------------------------------------

#[tokio::test]
async fn an_acknowledgement_before_the_source_is_done_is_the_body_error() {
    let declared = 64 * 1024 * 1024;
    let (source, probe) = Source::new(declared, 1024 * 1024);
    let run = put_against(Answer::AtHead, declared, source, &probe).await;
    // The fixture must include the fault: an answer while the source still had bytes.
    assert_answered(&run, true);
    assert!(
        run.given_at_return < run.declared,
        "the source had given its whole length, so this run did not answer early: {run:?}"
    );
    assert_body_error_carrying_the_answer(&run);
    assert_lifetime(&run);
}

/// PUT the held cases' source against a peer that answers `reply` once the source is held.
/// Once the client has read the answer the source is ready to fail if `then_fail`, else to
/// give its last bytes. The run is checked for the fixture every held case shares; the hold is
/// handed back for the test's own checks.
async fn put_held(reply: Reply, then_fail: bool) -> (Run, Arc<Hold>) {
    let hold = Arc::new(Hold::default());
    let (source, probe) = Source::new(HELD_TOTAL, HELD_TOTAL as usize);
    let source = source.held_at(HELD_AT, &hold, then_fail);
    let answer = Answer::WhenHeld(Arc::clone(&hold), reply);
    let run = put_against(answer, HELD_TOTAL, source, &probe).await;
    assert_answered(&run, true);
    assert_held_before_answer(&run, HELD_AT);
    (run, hold)
}

#[tokio::test]
async fn a_source_that_fails_after_the_acknowledgement_is_the_body_error() {
    let (run, hold) = put_held(ACK, true).await;
    // The source was ready to fail at its first poll after the client read the answer; a
    // failure then is not the outcome, the answer's early success is.
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_AT);
    assert_lifetime(&run);
}

#[tokio::test]
async fn a_final_piece_ready_once_the_client_has_read_the_acknowledgement_is_the_body_error() {
    let (run, hold) = put_held(ACK, false).await;
    // The source was ready with its last 6 bytes at its first poll after the client read the
    // answer; giving them then does not make the answer a receipt.
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_AT);
    assert_lifetime(&run);
}

// The same two, with the acknowledgement's body still on its way: the request has seen the
// answer and goes on reading the response, and hyper goes on polling the request body while
// it waits. An answer with no body ends the request in the poll that sees it, before hyper
// polls the request body again, so only these show that the source is not polled once the
// client has the answer.

#[tokio::test]
async fn a_source_that_fails_while_the_acknowledgements_body_is_on_its_way_is_the_body_error() {
    let (run, hold) = put_held(ACK_BODY_LATE, true).await;
    assert_body_sent_late(&run);
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_AT);
    assert_lifetime(&run);
}

#[tokio::test]
async fn a_final_piece_ready_while_the_acknowledgements_body_is_on_its_way_is_the_body_error() {
    let (run, hold) = put_held(ACK_BODY_LATE, false).await;
    assert_body_sent_late(&run);
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_AT);
    assert_lifetime(&run);
}

#[tokio::test]
async fn a_rejection_whose_body_is_on_its_way_is_the_servers_error_not_the_sources() {
    let (run, hold) = put_held(REJECTION_BODY_LATE, true).await;
    assert_body_sent_late(&run);
    // The source was ready to fail once the client had the rejection's head; the server's
    // rejection is the outcome, whole, request id included.
    assert_eq!(
        run.outcome,
        Err(S3Error::Service {
            status: 403,
            code: ErrorCode::Code("AccessDenied".to_owned()),
            message: Some("Access Denied".to_owned()),
            request_id: Some(REQUEST_ID.to_owned()),
        }),
        "an early rejection must be the server's error, never a source failure that came \
         after it: {run:?}"
    );
    assert_not_polled_past_the_hold(&run, &hold, HELD_AT);
    assert_lifetime(&run);
}

// --- scenario 2: early acknowledgement under backpressure ------------------------------------

#[tokio::test]
async fn an_acknowledgement_under_backpressure_is_the_body_error_and_releases_the_upload() {
    let (source, probe) = Source::new(LARGE_OBJECT, LARGE_PIECE);
    let run = put_against(Answer::WhenBackedUp, LARGE_OBJECT, source, &probe).await;
    // The fixture must include the fault: the peer answered a client blocked mid-write, while
    // the source still had bytes.
    let backed_up = run.peer.as_ref().map(|report| report.backed_up);
    assert!(
        matches!(backed_up, Some(BackedUp::Yes(_))),
        "the client's writes were not seen to back up before the peer answered, so this run \
         did not test backpressure: {run:?}"
    );
    assert_answered(&run, true);
    assert!(
        run.given_at_return < run.declared,
        "the source had given its whole length, so this run did not answer early: {run:?}"
    );
    assert_body_error_carrying_the_answer(&run);
    assert_lifetime(&run);
}

// --- scenario 3: stops reading, never answers -----------------------------------------------

#[tokio::test]
async fn a_peer_that_stops_reading_and_never_answers_is_the_operation_timeout() {
    let (source, probe) = Source::new(LARGE_OBJECT, LARGE_PIECE);
    let run = put_against(Answer::Never, LARGE_OBJECT, source, &probe).await;
    assert_answered(&run, false);
    assert_eq!(
        run.outcome,
        Err(S3Error::Timeout {
            phase: Phase::Operation,
            limit: T_OP,
        }),
        "{run:?}"
    );
    assert!(run.elapsed >= T_OP, "the deadline ran its course: {run:?}");
    assert_lifetime(&run);
}

// --- scenario 4: the receipt rule -------------------------------------------------------------

#[tokio::test]
async fn a_source_that_ended_before_the_acknowledgement_is_a_receipt() {
    let (source, probe) = Source::new(HELD_TOTAL, HELD_TOTAL as usize);
    let run = put_against(
        Answer::WhenExhausted(Arc::clone(&probe)),
        HELD_TOTAL,
        source,
        &probe,
    )
    .await;
    // The fixture: the peer answers only once the source has reported its end, and it did
    // answer.
    assert_answered(&run, true);
    assert!(
        run.ended_at_return,
        "the source never reported its end: {run:?}"
    );
    assert_eq!(run.given_at_return, run.declared, "{run:?}");
    // The source had ended before the answer: a receipt.
    assert_eq!(run.outcome, Ok(PutOutcome { etag: None }), "{run:?}");
    assert_lifetime(&run);
}

/// PUT a source declared at `HELD_TOTAL` bytes that gives them all and then holds until the
/// client has read the answer ([`Hold`]): it holds its end if `total` is `HELD_TOTAL`, and an
/// extra byte if `total` is one more. The peer answers `200` once the source holds. The hold
/// is handed back for the test's checks.
async fn put_held_at_its_declared_length(total: u64) -> (Run, Arc<Hold>) {
    let hold = Arc::new(Hold::default());
    let (source, probe) = Source::new(total, total as usize);
    let source = source.held_at(HELD_TOTAL, &hold, false);
    let answer = Answer::WhenHeld(Arc::clone(&hold), ACK);
    let run = put_against(answer, HELD_TOTAL, source, &probe).await;
    (run, hold)
}

#[tokio::test]
async fn a_source_that_gave_its_whole_length_but_held_its_end_is_not_a_receipt() {
    let (run, hold) = put_held_at_its_declared_length(HELD_TOTAL).await;
    // The fixture must include the edge: the answer came while the source held its end.
    assert_answered(&run, true);
    assert_parked_before_answer(&run);
    // All 10 bytes given, the end not yet reported, when the client read the answer: the
    // source had not ended, so the answer is early, never a receipt.
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_TOTAL);
    assert!(
        !run.ended_at_return,
        "the source reported its end, so this run did not hold it back: {run:?}"
    );
    assert_lifetime(&run);
}

#[tokio::test]
async fn a_source_that_gave_its_declared_length_and_held_an_extra_byte_is_not_a_receipt() {
    let (run, hold) = put_held_at_its_declared_length(HELD_TOTAL + 1).await;
    // The fixture must include the edge: the answer came while the source held its 11th byte.
    assert_answered(&run, true);
    assert_parked_before_answer(&run);
    // The declared 10 bytes given, an 11th held back, when the client read the answer: the
    // source had not ended, so the answer is early, never a receipt. The 11th byte is never
    // taken, so the outcome is not the overrun either.
    assert_body_error_carrying_the_answer(&run);
    assert_not_polled_past_the_hold(&run, &hold, HELD_TOTAL);
    assert_lifetime(&run);
}

// --- scenario 5: a PUT its caller drops while it is blocked mid-write --------------------------

/// What a dropped PUT was observed to do.
#[derive(Debug)]
struct Dropped {
    backed_up: BackedUp,
    /// When the test dropped the call, counted from its start.
    dropped_after: Duration,
    /// Bytes the source had given when the call was dropped.
    given_at_drop: u64,
    /// How long after the drop the source was dropped; `None` if it was still alive
    /// `CLOSE_BOUND` after the drop.
    source_released: Option<Duration>,
    peer: Option<PeerReport>,
}

#[tokio::test]
async fn a_put_dropped_while_blocked_mid_write_releases_the_upload() {
    let (source, probe) = Source::new(LARGE_OBJECT, LARGE_PIECE);
    let (tell, told) = oneshot::channel();
    let mut peer = Peer::start(Answer::NeverTellingBackedUp(tell)).await;
    let client = client(peer.addr, DROPPED_T_OP);
    let started = Instant::now();
    let mut put = Box::pin(client.put_object(KEY, PutSource::new(LARGE_OBJECT, source)));
    // Bounded on the test's own runtime. A request that never reaches the peer leaves both
    // arms pending for good: the PUT does not return, and the peer, still waiting for a
    // request head, has nothing to say.
    let setup = timeout(DROPPED_SETUP_BOUND, async {
        tokio::select! {
            outcome = &mut put => {
                panic!("put_object returned before the test dropped it: {outcome:?}")
            }
            backed_up = told => backed_up.expect("the peer says whether the client backed up"),
        }
    })
    .await;
    let Ok(backed_up) = setup else {
        panic!(
            "the PUT never got as far as the drop. {DROPPED_SETUP_BOUND:?} after the call began \
             ({DROPPED_HEAD_WAIT:?} for its request head to reach the peer, then the peer's \
             {DROPPED_BACKED_UP_WAIT:?} wait for the client's writes to back up), put_object had \
             not returned and the peer had not said how its wait ended, which it does, whatever \
             it saw, once it has read a request head. The source had given {} of {LARGE_OBJECT} \
             bytes and {} been dropped.",
            probe.given.load(Ordering::SeqCst),
            if probe.dropped() { "had" } else { "had not" },
        );
    };
    let given_at_drop = probe.given.load(Ordering::SeqCst);
    // Stamped before the drop, so the bound covers the drop itself too.
    let dropped = Instant::now();
    drop(put);
    let dropped_after = dropped.duration_since(started);
    let report = peer.after_call(dropped).await;
    while !probe.dropped() && dropped.elapsed() < CLOSE_BOUND {
        sleep(Duration::from_millis(5)).await;
    }
    let source_released = probe
        .dropped_at
        .get()
        .map(|at| at.saturating_duration_since(dropped));
    let run = Dropped {
        backed_up,
        dropped_after,
        given_at_drop,
        source_released,
        peer: report,
    };
    // The fixture must include the fault: a client blocked mid-write with bytes left, dropped
    // early enough that the operation deadline cannot be what releases it within the bound.
    assert!(
        matches!(run.backed_up, BackedUp::Yes(_)),
        "the client's writes were not seen to back up, so this run did not drop a blocked PUT: \
         {run:?}"
    );
    assert!(
        run.given_at_drop < LARGE_OBJECT,
        "the source had given its whole length before the drop: {run:?}"
    );
    assert!(
        run.dropped_after + CLOSE_BOUND < DROPPED_T_OP,
        "the PUT was dropped too close to its operation deadline to tell the drop's release \
         from the deadline's: {run:?}"
    );
    assert!(
        run.source_released
            .is_some_and(|after| after <= CLOSE_BOUND),
        "the source outlived the dropped PUT by more than {CLOSE_BOUND:?}: {run:?}"
    );
    assert_connection_closed(run.peer.as_ref(), &run);
}
