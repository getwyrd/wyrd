//! The client's HTTP connector: every request on a TCP connection of its own, driven through
//! hyper's own HTTP/1.1 connection, so the client sees what the endpoint sent after the end
//! of a response (invariant: a response that is not a whole, correctly framed S3 response is
//! never reported as one).
//!
//! hyper reads exactly what a response's framing says, a `Content-Length` worth of body or a
//! chunked body up to its terminal chunk, trailer section and final CRLF, and hands that over
//! as the whole body. Whatever the endpoint sent past that end stays in hyper's read buffer or
//! the socket: a body longer than its `Content-Length`, a second document after the first,
//! bytes after a chunked body's terminal chunk and trailer section, a body after a `204`.
//! The SDK's own connector (`aws-smithy-http-client`, over hyper-util's pool) never shows it
//! to the caller, and by the time hyper drops the connection over it the response has been
//! handed back as whole.
//! Here, a response body reports its end only after the connection has said what it held
//! past that end ([`WholeResponse`]); anything at all is [`Overrun`]. A chunked body's trailer
//! section, the last thing before that end, is dropped unread: S3 defines no response
//! trailers, and nothing in the client reads them.
//!
//! Nothing here reads the response: the framing, the head and the body are hyper's, as they
//! were under the SDK's connector. The connection is taken back from hyper with
//! [`Connection::into_parts`], whose `read_buf` is "bytes that have been read but not
//! processed as HTTP" (`hyper` 1.10.1 `client/conn/http1.rs:36-44`), and the socket is
//! peeked, without waiting, for what has arrived but hyper has not read.
//!
//! The limit: bytes that reach the client after it has asked are not seen. The endpoint has
//! by then sent a whole response and the client has read it; what it sends later belongs to
//! no response the client reads, and the connection is closed under it.
//!
//! The cost: no connection is reused. A connection that held bytes past a response cannot
//! carry another request, and one that did not cannot be told apart from one whose extra
//! bytes are still in flight, so each request has a fresh connection and the client closes
//! it ([`super::S3Client::put_object`] counts what that costs).

use std::fmt;
use std::future::{poll_fn, Future};
use std::io::ErrorKind::{ConnectionAborted, ConnectionReset, WouldBlock};
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
use aws_sdk_s3::config::{HttpClient, RuntimeComponents};
use aws_sdk_s3::error::{BoxError, ConnectorError};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use bytes::Bytes;
use http_body::Frame;
use hyper::body::Incoming;
use hyper::client::conn::http1::{self, Connection, Parts};
use hyper::header::{HeaderValue, HOST};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A connection's transport: a plain TCP socket (TLS is terminated in front of the gateway,
/// proposal 0017 §10).
type Io = TokioIo<TcpStream>;

/// The client's [`HttpClient`]: one connection per request (module docs).
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct WireClient;

impl HttpClient for WireClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(WireConnector {
            connect: settings.connect_timeout(),
        })
    }
}

#[derive(Debug)]
struct WireConnector {
    /// The connect deadline. It is the only connector timeout the client sets: the read
    /// timeout is disabled (`S3Client::with_deadlines`), and every later wait is bounded by
    /// the SDK's operation deadline or a GET body's own deadlines.
    connect: Option<Duration>,
}

impl HttpConnector for WireConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        HttpConnectorFuture::new(exchange(request, self.connect))
    }
}

/// Send `request` on a fresh connection and hand back its response, whose body checks the
/// connection at its end ([`WholeResponse`]).
async fn exchange(
    request: HttpRequest,
    connect: Option<Duration>,
) -> Result<HttpResponse, ConnectorError> {
    let mut request = request
        .try_into_http1x()
        .map_err(|e| ConnectorError::user(e.into()))?;
    let uri = request.uri().clone();
    if uri.scheme_str() != Some("http") {
        return Err(ConnectorError::user(
            format!("the client speaks plain HTTP only, and {uri} is not an http URL").into(),
        ));
    }
    let host = uri
        .host()
        .ok_or_else(|| ConnectorError::user(format!("{uri} names no host").into()))?;
    let stream = connect_to(host, uri.port_u16().unwrap_or(80), connect).await?;
    // What hyper-util's client does for a request on a direct connection (`hyper-util` 0.1.20
    // `client/legacy/client.rs:298-319`): a `Host` header from the URL, and the request target
    // in origin form.
    let host_header = host_header(host, uri.port_u16()).map_err(ConnectorError::user)?;
    request.headers_mut().entry(HOST).or_insert(host_header);
    *request.uri_mut() = uri
        .path_and_query()
        .map_or_else(Uri::default, |target| Uri::from(target.clone()));
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| ConnectorError::io(e.into()))?;
    let (stop, stopped) = oneshot::channel();
    // Spawned onto the runtime the request runs on, as hyper-util spawns its connection
    // tasks; aborted when this future or the response body drops ([`ConnectionTask`]).
    let task = ConnectionTask {
        stop: Some(stop),
        handle: tokio::spawn(drive(connection, stopped)),
    };
    let response = sender.send_request(request).await.map_err(|e| {
        if e.is_user() {
            ConnectorError::user(e.into())
        } else {
            ConnectorError::io(e.into())
        }
    })?;
    // `sender` drops with this function: the connection carries no second request. hyper
    // notices only once the response is over (its dispatcher polls for a next request only
    // when idle), so this cannot cut the response or a request body still being written.
    let response = response.map(|body| {
        SdkBody::from_body_1_x(WholeResponse {
            body,
            state: State::Open(task),
        })
    });
    HttpResponse::try_from(response).map_err(|e| ConnectorError::other(e.into(), None))
}

/// The `Host` header for a URL's `host` and `port`: the port is named unless it is HTTP's
/// default, as hyper-util names it (`hyper-util` 0.1.20 `client/legacy/client.rs:978-984`).
fn host_header(host: &str, port: Option<u16>) -> Result<HeaderValue, BoxError> {
    let value = match port {
        Some(port) if port != 80 => HeaderValue::try_from(format!("{host}:{port}"))?,
        _ => HeaderValue::try_from(host)?,
    };
    Ok(value)
}

/// A TCP connection to `host:port`, within the connect deadline. Each address the name
/// resolves to is tried in turn (`tokio` `TcpStream::connect`); an IPv6 literal comes in
/// brackets from the URL and is connected to without them.
async fn connect_to(
    host: &str,
    port: u16,
    deadline: Option<Duration>,
) -> Result<TcpStream, ConnectorError> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    // Clock: tokio's runtime clock, the one the SDK's operation deadline also sleeps on.
    let connected = match deadline {
        Some(limit) => tokio::time::timeout(limit, TcpStream::connect((host, port)))
            .await
            .map_err(|elapsed| ConnectorError::timeout(elapsed.into()))?,
        None => TcpStream::connect((host, port)).await,
    };
    let stream = connected.map_err(|e| ConnectorError::io(e.into()))?;
    // The SDK's connector sets it too: a small request is not held back for an ACK.
    stream
        .set_nodelay(true)
        .map_err(|e| ConnectorError::io(e.into()))?;
    Ok(stream)
}

/// Run `connection` until its response's reader asks what it held past the response's end
/// (or drops the question), then take the connection back from hyper and count what it held.
///
/// The connection's own outcome is not looked at: hyper reports a failure to the request
/// through its response future and to the response through its body. hyper fails an idle
/// connection that holds unread bytes (`hyper` 1.10.1 `proto/h1/conn.rs`,
/// `require_empty_read`), but it keeps those bytes, so they are counted all the same.
async fn drive(
    mut connection: Connection<Io, SdkBody>,
    mut stopped: oneshot::Receiver<()>,
) -> Result<u64, String> {
    let mut finished = false;
    poll_fn(|cx| {
        if !finished {
            finished = connection.poll_without_shutdown(cx).is_ready();
        }
        // Asked, or the question dropped: either way the response is over.
        Pin::new(&mut stopped).poll(cx).map(drop)
    })
    .await;
    let Parts { io, read_buf, .. } = connection.into_parts();
    unread(&read_buf, io)
}

/// The bytes a connection holds that hyper never processed: those in its read buffer,
/// `read_buf`, and those that have reached the socket and that hyper has not read. The socket
/// is peeked once, without waiting, and closed when this returns. A socket that cannot be
/// checked is an error, never a clean count.
fn unread(read_buf: &[u8], io: Io) -> Result<u64, String> {
    let unchecked =
        |e: std::io::Error| format!("the connection's socket could not be checked: {e}");
    let socket = io.into_inner().into_std().map_err(unchecked)?;
    let mut probe = [0u8; 1024];
    // Non-blocking, as tokio left it: a close reads as `0`. Nothing pending is `WouldBlock`, and
    // a peer that reset the connection after its response left no bytes to count.
    let pending = match socket.peek(&mut probe) {
        Ok(n) => n as u64,
        Err(e) if matches!(e.kind(), WouldBlock | ConnectionReset | ConnectionAborted) => 0,
        Err(e) => return Err(unchecked(e)),
    };
    Ok(read_buf.len() as u64 + pending)
}

/// A connection's task and the question its response's reader asks it.
struct ConnectionTask {
    stop: Option<oneshot::Sender<()>>,
    /// What the connection held past the response's end.
    handle: JoinHandle<Result<u64, String>>,
}

impl Drop for ConnectionTask {
    /// The connection goes with whatever holds it: a request whose response never arrived, a
    /// response body dropped unread. Aborting a finished task does nothing.
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The bytes the endpoint sent after the end of a response: the response's framing put its
/// end before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Overrun {
    /// How many such bytes the client had received when it read the response's end: at
    /// least this many were sent.
    pub(super) after: u64,
}

impl fmt::Display for Overrun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the endpoint sent at least {} bytes after the end of the response, where its \
             framing ends it: a body longer than its Content-Length, bytes after a chunked \
             body's terminal chunk, or bytes after a response that has no body",
            self.after
        )
    }
}

impl std::error::Error for Overrun {}

/// Where a [`WholeResponse`] is.
enum State {
    /// Handing over the body hyper reads while the task's question is unasked; once hyper's
    /// body has ended and the question is asked, waiting for the count.
    Open(ConnectionTask),
    /// Nothing came after: the body's end.
    Ended,
    /// Bytes came after the end.
    Overran(Overrun),
    /// No count: the socket could not be checked, or the connection's task ended without one
    /// (it does not panic, so only a runtime shutting down ends it so). Nothing shows the
    /// response whole.
    Lost(String),
}

/// A response body that ends only where its connection agrees it does.
///
/// Every data frame hyper reads is handed over as it arrives. When hyper's body ends, which it
/// does exactly where the framing says, the connection's task is asked what it held past that
/// end, and the body ends only if the answer is nothing; otherwise it fails with [`Overrun`].
/// The wait is not on the endpoint: the task answers on its next poll, from what it already
/// holds.
///
/// A chunked body's trailer section is dropped, and never handed over. hyper hands it over as
/// the body's last frame (`hyper` 1.10.1 `body/incoming.rs:229-233`), and a reader may take
/// that frame as the end and never poll again: the SDK's does, for a GET object
/// (`aws-smithy-types` 1.8.1 `body.rs:220-226`), so a section passed on at once would let the
/// body end with nothing checked. Nor can every reader take one: the SDK's checksum
/// validator, which wraps a GET's body whatever its status whenever the response carries an
/// `x-amz-checksum-*` header (`aws-sdk-s3` 1.152.0 `http_response_checksum.rs:130-140`), takes
/// every frame for data and panics on one that is not (`aws-smithy-checksums` 0.65.0
/// `body/validate.rs:57`). S3 defines no response trailers, and nothing in the client reads
/// them, so the section goes no further than here: hyper's body is polled on past it to its
/// end, and the connection is asked then.
///
/// It keeps the default `is_end_stream` and `size_hint`. An empty body must still be polled
/// to its end, or nothing would check the connection behind it, and a hint forwarded from a
/// large declared length would invite a consumer to reserve that much.
pub(super) struct WholeResponse {
    body: Incoming,
    state: State,
}

impl http_body::Body for WholeResponse {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                State::Open(task) => {
                    if task.stop.is_some() {
                        match ready!(Pin::new(&mut this.body).poll_frame(cx)) {
                            Some(Ok(frame)) => match frame.into_trailers() {
                                // Dropped, and hyper's body polled on to its end: once the
                                // section is out, the trailers channel reads as closed and
                                // the body as ended (`body/incoming.rs:229-233`).
                                Ok(_section) => continue,
                                Err(data) => return Poll::Ready(Some(Ok(data))),
                            },
                            Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                            None => {}
                        }
                        // hyper's body has ended. The task is alive while its handle is held,
                        // so it hears the question.
                        if let Some(stop) = task.stop.take() {
                            let _ = stop.send(());
                        }
                    }
                    this.state = match ready!(Pin::new(&mut task.handle).poll(cx)) {
                        Ok(Ok(0)) => State::Ended,
                        Ok(Ok(after)) => State::Overran(Overrun { after }),
                        Ok(Err(detail)) => State::Lost(detail),
                        Err(e) => State::Lost(format!(
                            "the connection's task ended before it counted what followed the \
                             response: {e}"
                        )),
                    };
                }
                State::Ended => return Poll::Ready(None),
                State::Overran(overrun) => return Poll::Ready(Some(Err((*overrun).into()))),
                State::Lost(detail) => return Poll::Ready(Some(Err(detail.clone().into()))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! [`unread`] over real loopback sockets. Through the client, the scripted endpoint of
    //! `tests/s3_client_nonconforming_responses.rs` reaches only hyper's buffer: hyper reads
    //! whatever has arrived with a response's end before the client asks, so bytes left on the
    //! socket itself cannot be staged that way deterministically.
    //!
    //! And [`exchange`]'s response body frame by frame, which the client's public calls do not
    //! show: that a trailer section is never among them.

    use http_body::Body as _;
    use hyper::header::HeaderMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// What a response body handed over, in order.
    #[derive(Debug, PartialEq)]
    enum Seen {
        Data(Bytes),
        Trailers(HeaderMap),
        Overrun(u64),
        Failed(String),
    }

    /// The frames of the response body [`exchange`] hands back for a GET, when a loopback
    /// endpoint reads the request's head and answers with exactly `response`. A failure is
    /// the last thing seen.
    async fn frames(response: &'static [u8]) -> Vec<Seen> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let endpoint = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(peer.read_u8().await.expect("a whole request head"));
            }
            peer.write_all(response).await.expect("write");
            // Open until the client closes its end, so the response ends where its framing
            // says and nowhere else.
            let _ = peer.read_u8().await;
        });
        let mut request = HttpRequest::new(SdkBody::empty());
        request
            .set_uri(format!("http://{addr}/bucket/key"))
            .expect("a URI");
        let read = async {
            let mut body = exchange(request, None)
                .await
                .expect("a response")
                .into_body();
            let mut seen = Vec::new();
            while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                match frame.map(Frame::into_trailers) {
                    Ok(Ok(trailers)) => seen.push(Seen::Trailers(trailers)),
                    Ok(Err(data)) => {
                        seen.push(Seen::Data(data.into_data().expect("a data frame")));
                    }
                    Err(e) => {
                        seen.push(match e.downcast_ref::<Overrun>() {
                            Some(overrun) => Seen::Overrun(overrun.after),
                            None => Seen::Failed(e.to_string()),
                        });
                        break;
                    }
                }
            }
            seen
        };
        // Bounded, so a body that never ends fails the test instead of hanging it.
        let seen = tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .expect("the body ended");
        tokio::time::timeout(Duration::from_secs(10), endpoint)
            .await
            .expect("the client closed the connection")
            .expect("the endpoint ran");
        seen
    }

    #[tokio::test]
    async fn a_trailer_section_is_dropped_and_the_body_ends_only_once_nothing_followed() {
        const TRAILED: &[u8] = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
                                 5\r\nhello\r\n0\r\nx-trailer: value\r\n\r\n";
        // Nothing after the section: the data, then the body's end, and the section nowhere.
        assert_eq!(
            frames(TRAILED).await,
            [Seen::Data(Bytes::from_static(b"hello"))],
            "a clean end"
        );
        // Four bytes after it: the body fails instead, and the section is still nowhere.
        const THEN_JUNK: &[u8] = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
                                   5\r\nhello\r\n0\r\nx-trailer: value\r\n\r\njunk";
        assert_eq!(
            frames(THEN_JUNK).await,
            [Seen::Data(Bytes::from_static(b"hello")), Seen::Overrun(4)],
            "junk after the section"
        );
    }

    /// A connected loopback pair: the client's end, then the peer's.
    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (peer, _) = listener.accept().await.expect("accept");
        (client, peer)
    }

    #[tokio::test]
    async fn bytes_hyper_kept_and_bytes_waiting_on_the_socket_both_count() {
        let (client, mut peer) = pair().await;
        peer.write_all(b"junk").await.expect("write");
        // In the client's receive queue, not yet read by anyone.
        client.readable().await.expect("readable");
        assert_eq!(unread(b"xy", TokioIo::new(client)), Ok(6));
    }

    #[test]
    fn the_host_header_names_the_port_unless_it_is_the_default() {
        for (host, port, want) in [
            ("gateway", None, "gateway"),
            ("gateway", Some(80), "gateway"),
            ("gateway", Some(9000), "gateway:9000"),
            ("[::1]", Some(9000), "[::1]:9000"),
        ] {
            let header = host_header(host, port).expect("a valid header value");
            assert_eq!(header, want, "{host} {port:?}");
        }
    }

    #[tokio::test]
    async fn a_quiet_or_closed_socket_holds_nothing() {
        let (quiet, _peer) = pair().await;
        assert_eq!(unread(b"", TokioIo::new(quiet)), Ok(0));
        let (closed, peer) = pair().await;
        drop(peer);
        // The peer's close has arrived: the socket reads as ended.
        closed.readable().await.expect("readable");
        assert_eq!(unread(b"", TokioIo::new(closed)), Ok(0));
    }
}
