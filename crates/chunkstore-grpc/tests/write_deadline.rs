//! Issue #638 — the D-server-**enforced** fragment-write authorization deadline
//! (`W_write`, proposal 0016 decision 5's end-to-end-deadline argument,
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md:1551-1576`).
//!
//! A caller-side `await` timeout alone (`crates/chunkstore-grpc/src/client.rs:169-190`)
//! bounds only how long the *writer waits*, never when an already-accepted write *takes
//! effect* — 0016 requires the D server itself to refuse, not queue, a write whose deadline
//! has elapsed, or `G_orphan > W_write + δ_clock` (`0016:1478`) bounds nothing and 0016
//! outcome (a) — an unreferenced *and* unevidenced fragment — is reachable.
//!
//! Every leg below drives the **real, unmodified** `ChunkStoreService` over a real tonic
//! loopback connection with the real `FsChunkStore` behind it; only the client's request
//! *bytes* are hand-rolled, for the reason in the next paragraph.
//!
//! **Falsifiability.** On the base seam a deadline cannot be expressed in Rust at all:
//! `ChunkStore::put_fragment` takes `(id, fragment)` and `FragmentPutRequest` carries only
//! `id`/`fragment`. Calling the patched 3-argument method (or naming the patched request
//! field) would make the base fail to **compile** rather than fail an **assertion** — a red
//! an exit-code check cannot tell from a spurious break, and one that proves nothing about
//! the behaviour gap this issue closes. So every `PutFragment` here is sent as hand-encoded
//! raw protobuf over a bare `tonic::client::Grpc<Channel>`: proto3's forward-compatible wire
//! format means a pre-#638 server decodes the two fields it knows, ignores the third,
//! applies the write — and the assertion "refused, and not stored" genuinely fails. The
//! *typed* client-side reconstruction (`WriteDeadlineExpired`), the D server's own clock,
//! and the **placement** of the enforcement point inside the store (after the fragment's
//! bytes are on disk, before the publishing rename) need the patched API to even name, so
//! they are asserted in existing files this base-compiles bar does not apply to:
//! `tests/round_trip.rs` (production client + a scripted server clock),
//! `crates/chunkstore-fs/tests/conformance.rs` (the local store, leg E, with a clock
//! anchored to the store's own on-disk progress) and `tests/concurrent_put.rs` there
//! (expired writers racing live ones), and the seeded network cases in
//! `crates/dst/tests/network.rs` (a write parked past its deadline by a D server whose
//! store is the real `FsChunkStore`, applied after its caller has given up; and one whose
//! deadline elapses inside that store, between its data write and its publication).

#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::codegen::http::{Request as HttpRequest, Response as HttpResponse};
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Status};
use wyrd_chunk_format::{encode as encode_fragment, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId};

/// Every await on the D server is bounded, fail-closed — the rubric's await discipline
/// (`AGENTS.md:181-183`) binds tests too: a regression that hangs the RPC must fail this
/// file, not stall the suite. It is **generous by design** — orders of magnitude past every
/// deadline and park below — so a client-side bound can never be what produces a refusal
/// (the brief's leg A requirement).
const WATCHDOG: Duration = Duration::from_secs(30);

async fn bounded<T>(what: &str, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(WATCHDOG, f)
        .await
        .unwrap_or_else(|_| panic!("{what} did not complete within {WATCHDOG:?}"))
}

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode_fragment(&header, payload))
}

fn epoch_millis(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis() as u64
}

/// The clock these deadlines are stated against: the **D server's own wall clock**, which
/// owns the write-deadline lifecycle (`FsChunkStore::put_fragment` reads it to judge
/// expiry). The test must derive its past/future deadlines from that same source rather
/// than a virtualised one, or the two evaluation sites would disagree by construction
/// (#619, AGENTS.md § Review rubric — "one clock per correctness lifecycle").
fn dserver_clock_now() -> u64 {
    // wall-clock exempt: see above — this IS the acceptor's clock, read from the test side.
    #[allow(clippy::disallowed_methods)]
    let now = SystemTime::now();
    epoch_millis(now)
}

/// Comfortably in the past — any implementation enforcing `W_write` must refuse it.
fn already_expired_deadline() -> u64 {
    dserver_clock_now().saturating_sub(60_000)
}

/// Comfortably in the future — a live write must be unaffected by it.
fn comfortably_future_deadline() -> u64 {
    dserver_clock_now() + 60_000
}

// ---------------------------------------------------------------------------
// Hand-rolled wire encoding for `PutFragment`, deliberately bypassing the generated
// `FragmentPutRequest` Rust field and the `ChunkStore` trait's changed `put_fragment`
// arity (see the module doc's Falsifiability note). Depends on nothing but `bytes` —
// already an ordinary, unpatched dependency of this crate — so the encoder carries no
// manifest change to keep in step with the base seam either.
// ---------------------------------------------------------------------------

/// A protobuf varint: 7 bits per byte, LSB first, continuation bit (`0x80`) set on every
/// byte but the last (the wire format prost's own `encode_varint` uses).
fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
            break;
        }
    }
}

/// A protobuf field key: `(field_number << 3) | wire_type`, varint-encoded. Wire types used
/// below: `0` varint, `1` 64-bit (fixed64), `2` length-delimited.
fn write_tag(field: u32, wire_type: u8, out: &mut Vec<u8>) {
    write_varint((u64::from(field) << 3) | u64::from(wire_type), out);
}

/// `ChunkId { fixed64 hi = 1; fixed64 lo = 2; }` — two `fixed64` (wire type 1,
/// little-endian per the protobuf spec) fields.
fn encode_wire_chunk_id(chunk: ChunkId, out: &mut Vec<u8>) {
    let hi = (chunk >> 64) as u64;
    let lo = chunk as u64;
    write_tag(1, 1, out);
    out.put_u64_le(hi);
    write_tag(2, 1, out);
    out.put_u64_le(lo);
}

/// `FragmentId { ChunkId chunk = 1; uint32 index = 2; }` — mirrors
/// `crates/chunkstore-grpc/src/conv.rs::to_wire_fragment_id` (private to that crate),
/// inlined here since these two messages are untouched by this issue. The submessage is
/// encoded into a scratch buffer first so its length prefix is exact, not assumed.
fn encode_wire_fragment_id(id: FragmentId, out: &mut Vec<u8>) {
    let mut chunk_bytes = Vec::new();
    encode_wire_chunk_id(id.chunk, &mut chunk_bytes);
    // field 1 (`id.chunk`, the `ChunkId` submessage), wire type 2 (length-delimited).
    write_tag(1, 2, out);
    write_varint(chunk_bytes.len() as u64, out);
    out.extend_from_slice(&chunk_bytes);
    // field 2 (`id.index`), wire type 0 (varint).
    write_tag(2, 0, out);
    write_varint(u64::from(id.index), out);
}

/// Hand-encode a `FragmentPutRequest` — `FragmentId id = 1; bytes fragment = 2; optional
/// uint64 deadline_millis = 3;` — as raw protobuf bytes. `id`/`fragment` are the base seam's
/// own two fields; `deadline_millis`, when present, is appended as field 3 exactly as the
/// additive proto change adds it. Never touches the generated struct, so this compiles
/// unchanged whether or not that struct carries the new field.
fn encode_put_fragment_request(
    id: FragmentId,
    payload: &[u8],
    deadline_millis: Option<u64>,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut id_bytes = Vec::new();
    encode_wire_fragment_id(id, &mut id_bytes);
    // field 1 (`id`, the `FragmentId` submessage), wire type 2 (length-delimited) — `id`'s
    // own fields are encoded into a scratch buffer first (above) so this length prefix is
    // exact, matching how `encode_wire_fragment_id` wraps its own `chunk` submessage.
    write_tag(1, 2, &mut out);
    write_varint(id_bytes.len() as u64, &mut out);
    out.extend_from_slice(&id_bytes);
    // field 2 (`fragment`), wire type 2 (length-delimited).
    write_tag(2, 2, &mut out);
    write_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    if let Some(deadline) = deadline_millis {
        // field 3 (`deadline_millis`), wire type 0 (varint) — proto3 `optional`, so its
        // absence in the `None` branch is byte-for-byte a pre-#638 request: additive
        // compatibility, asserted over the wire (leg D).
        write_tag(3, 0, &mut out);
        write_varint(deadline, &mut out);
    }
    out
}

/// Opaque already-encoded request bytes, sent as-is by [`RawEncoder`].
struct RawRequest(Vec<u8>);

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = RawRequest;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item.0);
        Ok(())
    }
}

/// Decodes a `FragmentPutResponse` (`message FragmentPutResponse {}`, untouched by this
/// issue) by hand — trivial, since it carries no fields: any bytes present are unknown
/// fields a well-behaved peer never sends here, so an empty response decodes to `Some(())`.
struct RawResponseDecoder;

impl Decoder for RawResponseDecoder {
    type Item = ();
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        src.advance(src.remaining());
        Ok(Some(()))
    }
}

struct RawCodec;

impl Codec for RawCodec {
    type Encode = RawRequest;
    type Decode = ();
    type Encoder = RawEncoder;
    type Decoder = RawResponseDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawResponseDecoder
    }
}

/// Send one `PutFragment` RPC over `channel` with hand-encoded bytes (see the module doc).
/// Exactly the path the generated `ChunkStoreClient::put_fragment` uses —
/// `/wyrd.v0.ChunkStore/PutFragment` over the same `tonic::client::Grpc` dispatch — so this
/// still drives the real tonic wire machinery, not a fake transport. Only the codec differs.
///
/// Both awaits are watchdog-bounded: a server that hangs fails the test instead of parking
/// the suite forever, and the bound is far too generous to produce a deadline refusal.
async fn put_fragment_raw(
    channel: Channel,
    id: FragmentId,
    payload: &[u8],
    deadline_millis: Option<u64>,
) -> Result<(), Status> {
    let mut grpc = Grpc::new(channel);
    bounded("the channel becoming ready", grpc.ready())
        .await
        .map_err(|e| Status::unknown(e.to_string()))?;
    let bytes = encode_put_fragment_request(id, payload, deadline_millis);
    let path = PathAndQuery::from_static("/wyrd.v0.ChunkStore/PutFragment");
    bounded(
        "the PutFragment rpc",
        grpc.unary(tonic::Request::new(RawRequest(bytes)), path, RawCodec),
    )
    .await
    .map(|_| ())
}

// ---------------------------------------------------------------------------
// Server-side "parked in the accept queue" seam (leg B) — a thin wrapper that delays every
// request before it reaches the real generated `ChunkStoreServer`. Implemented directly
// against `tonic::codegen::Service` / `tonic::server::NamedService` (both re-exported by
// `tonic`, an ordinary unpatched dependency here) rather than a `tower::Layer`, so it needs
// no new dependency and compiles identically against base and patched code. Only the
// *store* behind it enforces the deadline.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DelayedService<S> {
    inner: S,
    delay: Duration,
}

impl<S: NamedService> NamedService for DelayedService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<HttpRequest<B>> for DelayedService<S>
where
    S: Service<
            HttpRequest<B>,
            Response = HttpResponse<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
        // Swap in a ready clone so the delayed call doesn't hold up `poll_ready`'s caller —
        // the standard "clone then call" pattern for a `Clone` inner service whose `call`
        // needs to outlive this borrow.
        let mut inner = self.inner.clone();
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            inner.call(req).await
        })
    }
}

/// Stand up the **real, unmodified** `ChunkStoreService<FsChunkStore>` over a real tonic
/// server on an ephemeral loopback port, with every request delayed by `delay` before it
/// reaches the service (0016's "parked in the accept queue", `0016:1784`, when non-zero;
/// `Duration::ZERO` for the other legs). Returns the bound `Channel` (for the raw client
/// above) and a typed [`GrpcChunkStore`] for `get_fragment`, whose signature this issue does
/// not change — so the production read path is safe to use here.
///
/// The client channel is dialed **without any timeout**: nothing client-side may be able to
/// produce the refusals asserted below.
async fn serve(
    delay: Duration,
) -> (
    Channel,
    GrpcChunkStore,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    serve_in(dir, delay).await
}

async fn serve_in(
    dir: tempfile::TempDir,
    delay: Duration,
) -> (
    Channel,
    GrpcChunkStore,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
) {
    let store = FsChunkStore::open(dir.path()).expect("open store");
    let service = ChunkStoreService::new(store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(DelayedService {
                inner: ChunkStoreServer::new(service),
                delay,
            })
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });

    let channel = bounded(
        "dialing the D server",
        Endpoint::new(format!("http://{addr}"))
            .expect("endpoint")
            .connect(),
    )
    .await
    .expect("connect raw channel");
    let client = GrpcChunkStore::new(channel.clone());
    (channel, client, dir, server)
}

// ---- (A) an already-expired deadline is refused AT THE SERVER, and never stored ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_deadline_is_refused_by_the_server_and_never_stored() {
    let (channel, client, _dir, server) = serve(Duration::ZERO).await;

    let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0001, 0);
    let frag = fragment(id, b"authorized too long ago");

    let err = put_fragment_raw(channel, id, &frag, Some(already_expired_deadline()))
        .await
        .expect_err("a write whose deadline already elapsed must be refused by the D server");
    assert_eq!(
        err.code(),
        Code::FailedPrecondition,
        "the refusal must ride a distinct, operator-classifiable status, not a bare or \
         internal error: {err}"
    );

    // Asserting only the error would pass an implementation that refuses the caller AFTER
    // persisting the bytes — the exact outcome (a) leak this slice prevents.
    let got = bounded("the read-back", client.get_fragment(id))
        .await
        .unwrap();
    assert!(
        got.is_none(),
        "a refused write must NOT be observable via get_fragment afterwards"
    );

    server.abort();
}

// ---- (B) a write parked past its deadline is refused when it is finally processed ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_parked_past_its_deadline_is_refused_when_finally_applied() {
    // The service sits behind a 300 ms delay before every request reaches it — 0016's own
    // failure-mode row: "authorize a fragment write, park it in the D server's accept queue
    // past `W_write` … and assert it is refused" (`0016:1784`).
    let (channel, client, _dir, server) = serve(Duration::from_millis(300)).await;

    let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0002, 0);
    let frag = fragment(id, b"authorized, then parked past its window");
    // Authorized now with only 50 ms of headroom; the 300 ms park guarantees the deadline
    // has elapsed by the time the store is asked to apply the write. The caller is bounded
    // by nothing but the 30 s watchdog — 600× the deadline — so a caller-timeout-only design
    // cannot fail this: the client is still waiting happily. Only server-side enforcement
    // can produce the refusal.
    let deadline = dserver_clock_now() + 50;

    let err = put_fragment_raw(channel, id, &frag, Some(deadline))
        .await
        .expect_err(
            "a write parked past its deadline must be refused once the D server finally \
             attempts to apply it",
        );
    assert_eq!(
        err.code(),
        Code::FailedPrecondition,
        "must classify as a deadline refusal: {err}"
    );

    let got = bounded("the read-back", client.get_fragment(id))
        .await
        .unwrap();
    assert!(
        got.is_none(),
        "the parked-then-refused write must never be observable — asserting only the error \
         would pass an implementation that stores the bytes and refuses the caller \
         afterwards (0016 outcome (a))"
    );

    server.abort();
}

// ---- (C) a live write, comfortably inside its deadline, is unaffected ----
//
// Without this, legs A/B/F could pass by refusing every write, deadline or not.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_write_within_its_deadline_stores_and_reads_back_byte_identical() {
    let (channel, client, _dir, server) = serve(Duration::ZERO).await;

    let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0003, 0);
    let frag = fragment(id, b"comfortably inside its window");

    put_fragment_raw(channel, id, &frag, Some(comfortably_future_deadline()))
        .await
        .expect("a live write well inside its deadline must succeed");

    let got = bounded("the read-back", client.get_fragment(id))
        .await
        .unwrap();
    assert_eq!(
        got.as_deref(),
        Some(frag.as_ref()),
        "a live write must round-trip byte-identical"
    );

    server.abort();
}

// ---- (D) an absent deadline stores exactly as before #638, asserted over the wire ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_deadline_stores_exactly_as_before_issue_638() {
    let (channel, client, _dir, server) = serve(Duration::ZERO).await;

    let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0004, 0);
    let frag = fragment(id, b"no deadline at all");

    // The request bytes are byte-for-byte a pre-#638 request (field 3 simply absent), which
    // is what keeps every existing writer — the ordinary write path, backfill,
    // reconstruction, rebalance — working unchanged.
    put_fragment_raw(channel, id, &frag, None)
        .await
        .expect("a request carrying no deadline field must store exactly as it did before");

    let got = bounded("the read-back", client.get_fragment(id))
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some(frag.as_ref()));

    server.abort();
}

// ---- (F) the refusal is classifiable — against BOTH neighbours, not just one ----
//
// A caller must be able to tell an expected "refused, too late" from (i) a malformed
// fragment it offered and (ii) a genuine backend fault ("the disk is broken"). The second
// control is the one that matters most: without it, a server that reported *every* failure
// as a deadline refusal would pass this leg.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_refusal_is_distinguishable_from_both_a_client_fault_and_a_disk_fault() {
    let dir = tempfile::tempdir().expect("temp dir");

    // A chunk whose directory path is occupied by a *file*: every write beneath it fails
    // with a real `ENOTDIR` from the filesystem — a genuine backend fault, on any platform
    // and as any user (no chmod, so it holds under a root test runner too).
    let broken = fid(0xdead_0000_0000_0000_0000_0000_0000_0007, 0);
    std::fs::write(
        dir.path().join(format!("{:032x}", broken.chunk)),
        b"not a directory",
    )
    .expect("plant the obstruction");

    let (channel, _client, _dir, server) = serve_in(dir, Duration::ZERO).await;

    let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0005, 0);
    let deadline_err = put_fragment_raw(
        channel.clone(),
        id,
        &fragment(id, b"authorized too long ago, again"),
        Some(already_expired_deadline()),
    )
    .await
    .expect_err("an expired deadline must be refused");
    assert_eq!(deadline_err.code(), Code::FailedPrecondition);

    let malformed_err = put_fragment_raw(
        channel.clone(),
        fid(0xdead_0000_0000_0000_0000_0000_0000_0006, 0),
        b"not a fragment",
        None,
    )
    .await
    .expect_err("a malformed fragment must still be rejected");
    assert_eq!(
        malformed_err.code(),
        Code::InvalidArgument,
        "a malformed-fragment fault keeps its own distinct code"
    );

    let disk_err = put_fragment_raw(
        channel,
        broken,
        &fragment(broken, b"doomed by the disk"),
        Some(comfortably_future_deadline()),
    )
    .await
    .expect_err("a genuine backend I/O failure must surface as an error");
    assert_eq!(
        disk_err.code(),
        Code::Internal,
        "a broken backend stays an internal fault — reporting it as a deadline refusal \
         would have a caller stop re-authorizing and start ignoring a real fault: {disk_err}"
    );

    // The three outcomes are mutually distinguishable, which is the property a caller
    // branches on (and, seam-side, `wyrd_traits::is_write_deadline_expired` — asserted on
    // the typed error in `tests/round_trip.rs` and the fs conformance suite).
    assert_ne!(deadline_err.code(), malformed_err.code());
    assert_ne!(deadline_err.code(), disk_err.code());
    assert_ne!(malformed_err.code(), disk_err.code());

    server.abort();
}
