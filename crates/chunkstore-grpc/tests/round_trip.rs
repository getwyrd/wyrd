//! M2.2 definition of done (issue #112): `GrpcChunkStore` round-trips
//! `put` / `get` / `health` against an **in-process tonic server** over a real
//! loopback gRPC connection — real HTTP/2 framing and prost (de)serialization of
//! the fragment-addressed messages, not an in-memory fake.
//!
//! The server hosts the real `FsChunkStore` (the dev-dependency standing in for
//! `server`'s composition), so this also exercises the integrity contract: a
//! not-found get returns `Ok(None)`, and a non-fragment put is rejected by the
//! store and surfaced as a transport error.

#![forbid(unsafe_code)]

use bytes::Bytes;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore, TransportError};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

/// Stand up a D-server service over a fresh `FsChunkStore`, bound to an
/// ephemeral loopback port, and return a connected client. The listener is bound
/// (and thus accepting into the OS backlog) before the client dials, so there is
/// no startup race.
async fn connected() -> (
    GrpcChunkStore,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
) {
    connected_with_clock(wyrd_testkit::SystemClock).await
}

/// Like [`connected`], but the hosted `FsChunkStore` judges write deadlines against
/// `clock` (issue #638) — the D server's *own* clock, which is the site 0016 decision 5
/// requires to enforce `W_write`.
async fn connected_with_clock<C: wyrd_testkit::Clock + Send + Sync + 'static>(
    clock: C,
) -> (
    GrpcChunkStore,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");
    let service = ChunkStoreService::new(store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });

    let client = GrpcChunkStore::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    (client, dir, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_get_health_round_trip_over_grpc() {
    let (client, _dir, server) = connected().await;

    // health — the D server reports itself Healthy over the wire.
    assert_eq!(client.health().await.unwrap(), Health::Healthy);

    // a get for a fragment that was never stored is Ok(None), not an error.
    assert!(client.get_fragment(fid(99, 0)).await.unwrap().is_none());

    // put then get — byte-identical across the wire, at a non-zero EC index.
    let id = fid(0xdead_beef_cafe_babe_0000_0000_1234_5678, 3);
    let frag = fragment(id, b"a parity fragment over the wire");
    client.put_fragment(id, frag.clone(), None).await.unwrap();
    let got = client.get_fragment(id).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(frag.as_ref()),
        "fragment must round-trip byte-identical over gRPC"
    );

    // the store verifies on put: garbage is rejected, not stored.
    assert!(
        client
            .put_fragment(fid(1, 0), Bytes::from_static(b"not a fragment"), None)
            .await
            .is_err(),
        "a non-fragment put must be rejected by the D server"
    );
    assert!(client.get_fragment(fid(1, 0)).await.unwrap().is_none());

    server.abort();
}

/// The gRPC `Status::code` carried by a `ChunkStore` boxed error.
///
/// The `GrpcChunkStore` boxes a [`TransportError`] wrapping the wire `Status` — directly
/// for a terminal status, and underneath a `wyrd_traits::TransientFault` for a
/// known-transient one, since #577 made the seam's failure class wrap the backend's error
/// rather than replace it. Walking the chain (the `wyrd_traits::is_integrity_fault` idiom)
/// finds it either way.
fn transport_status_code(err: &wyrd_traits::BoxError) -> Code {
    let mut next: Option<&(dyn std::error::Error + 'static)> = Some(err.as_ref());
    let te = loop {
        let e = next.unwrap_or_else(|| {
            panic!("a transport failure carries a TransportError in its chain; got: {err}")
        });
        if let Some(te) = e.downcast_ref::<TransportError>() {
            break te;
        }
        next = e.source();
    };
    match te {
        TransportError::Unavailable(s) | TransportError::Timeout(s) | TransportError::Rpc(s) => {
            s.code()
        }
        TransportError::Connect(e) => panic!("expected a gRPC status, got a connect error: {e}"),
    }
}

/// Issue #207 — the gRPC **corruption** classification leg: a fragment that rots on the
/// D server's disk is detected on read and surfaced over the wire **distinguishably**
/// from a transient fault. The server emits `DATA_LOSS` (not `INTERNAL`), and the client
/// reconstructs it as a seam-level [`wyrd_traits::IntegrityFault`] — the exact predicate
/// scrub branches on (`is_integrity_fault` ⇒ repair-and-continue, not retry). Without the
/// fix the store's verify failure surfaced as `INTERNAL`/`Rpc`, indistinguishable from a
/// transient fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_of_a_rotten_fragment_is_an_integrity_fault_over_grpc() {
    let (client, dir, server) = connected().await;

    // Store a valid fragment, then rot its on-disk bytes behind the server's back.
    let id = fid(0xC0DE_0000_0000_0000_0000_0000_0000_0001, 0);
    client
        .put_fragment(id, fragment(id, b"healthy until it rots"), None)
        .await
        .unwrap();
    let path = fragment_path(dir.path(), id);
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff; // break the trailing checksum
    std::fs::write(&path, &bytes).unwrap();

    let err = client
        .get_fragment(id)
        .await
        .expect_err("a corrupt fragment must not round-trip as valid bytes");

    assert!(
        wyrd_traits::is_integrity_fault(err.as_ref()),
        "a rotten fragment surfaces as a corruption finding the consumer can repair, \
         not a transient fault to retry"
    );
    assert!(
        err.downcast_ref::<TransportError>().is_none(),
        "corruption is NOT carried as a transient/rpc TransportError — it is distinguishable"
    );

    server.abort();
}

/// Issue #207 — the gRPC **PUT classification** leg: a client that offers a malformed
/// fragment gets `INVALID_ARGUMENT` (a client fault), not `INTERNAL` (a server fault that
/// invites futile retries). Same error-classification seam as the corruption leg, the
/// opposite direction: the bytes are the *caller's* to fix, so the code names the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_of_a_malformed_fragment_is_invalid_argument_over_grpc() {
    let (client, _dir, server) = connected().await;

    let err = client
        .put_fragment(fid(7, 0), Bytes::from_static(b"not a fragment"), None)
        .await
        .expect_err("a non-fragment put must be rejected");

    assert_eq!(
        transport_status_code(&err),
        Code::InvalidArgument,
        "a malformed-fragment put is a client (invalid-argument) fault, not server-internal"
    );

    server.abort();
}

// ---- Issue #638 — the D-server-enforced write deadline, production client path ----
//
// The wire-level red→green proof lives in `tests/write_deadline.rs` (hand-encoded
// protobuf, so it compiles and fails by *assertion* against the base seam, where this
// Rust API cannot express a deadline at all). These two legs exercise what that file
// deliberately cannot: the shipped `GrpcChunkStore` client type and the D server's own
// `Clock` — including the *second* deadline outcome, which only the typed client can
// observe at all.

/// Every await on the D server is bounded, fail-closed — the rubric's await discipline
/// (`AGENTS.md:181-183`) applies to tests too: a regression that hangs the RPC must fail
/// this test, not stall the suite. The bound is *generous* (orders of magnitude past every
/// deadline used below), so it can never be what produces a refusal.
const RPC_WATCHDOG: std::time::Duration = std::time::Duration::from_secs(30);

async fn bounded<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(RPC_WATCHDOG, f)
        .await
        .unwrap_or_else(|_| panic!("{what} did not complete within {RPC_WATCHDOG:?}"))
}

/// Issue #638 — the **production** `GrpcChunkStore::put_fragment` sends an expired
/// authorization deadline, and the D server's refusal reconstructs client-side as a typed
/// `wyrd_traits::WriteDeadlineExpired`, the same way `IntegrityFault`/`BlockReadFault`
/// already reconstruct (`client.rs::classify_put_status`) — so a caller branches on the
/// class instead of matching a string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_with_an_expired_deadline_is_refused_and_reconstructs_as_write_deadline_expired() {
    // The D server's clock stands at 10_000 ms; the write was authorized until 9_000 ms.
    // No wall clock is involved: the refusal is decided by the *server's* clock, which is
    // the point of 0016 decision 5.
    let (client, _dir, server) = connected_with_clock(wyrd_testkit::ManualClock::new(10_000)).await;

    let id = fid(0xdead_beef_0000_0000_0000_0000_0000_0001, 0);
    let err = bounded(
        "an expired-deadline put",
        client.put_fragment(id, fragment(id, b"authorized too long ago"), Some(9_000)),
    )
    .await
    .expect_err("an expired deadline must be refused by the production client path too");
    assert!(
        wyrd_traits::is_write_deadline_expired(err.as_ref()),
        "the production client must reconstruct WriteDeadlineExpired: {err}"
    );
    assert!(
        bounded("the read-back", client.get_fragment(id))
            .await
            .unwrap()
            .is_none(),
        "a refused write must not be stored"
    );

    // A live write on the same server, in the same class of call, still lands — so the
    // refusal above is the deadline's work, not a server that rejects everything.
    let live = fid(0xdead_beef_0000_0000_0000_0000_0000_0002, 0);
    let frag = fragment(live, b"comfortably inside its window");
    bounded(
        "a live put",
        client.put_fragment(live, frag.clone(), Some(70_000)),
    )
    .await
    .expect("a write inside its deadline must be stored");
    assert_eq!(
        bounded("the read-back", client.get_fragment(live))
            .await
            .unwrap()
            .as_deref(),
        Some(frag.as_ref())
    );

    server.abort();
}

/// Issue #638, 0016's own failure-mode row (`0016:1784`) at the **server**: a write the D
/// server *accepted* while it was live, but which expires before the server applies it,
/// must be refused there — and this is the leg a caller-side timeout cannot fake, because
/// the client below is never bounded by anything near the deadline (30 s watchdog vs a
/// 10 s-scale deadline) and is waiting happily when the refusal arrives.
///
/// The server's scripted clock reads 9_500 when the store admits the write and 10_500 when
/// it reaches its publication point, straddling the write's 10_000 deadline: a D server
/// that judges the deadline only when it *accepts* the request stores the fragment and
/// this test goes red. (The script assumes exactly those two reads, which is what
/// `remaining()` below pins. *Where* the second read falls relative to the store's own
/// data write and publishing rename is not observable over the wire; that is pinned on the
/// store itself, by `chunkstore-fs/tests/conformance.rs`'s progress-anchored clock.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_that_expires_before_the_server_applies_it_is_refused_over_grpc() {
    let clock = wyrd_testkit::SteppedClock::new([9_500, 10_500]);
    let (client, _dir, server) = connected_with_clock(clock.clone()).await;

    let id = fid(0xdead_beef_0000_0000_0000_0000_0000_0003, 0);
    let err = bounded(
        "a put that expires before it is applied",
        client.put_fragment(
            id,
            fragment(id, b"live on arrival, late at the publication point"),
            Some(10_000),
        ),
    )
    .await
    .expect_err(
        "the D server must refuse a write whose deadline elapsed between acceptance and \
         application — nothing on the client side bounds this call near its deadline",
    );
    let outcome = wyrd_traits::write_deadline_outcome(err.as_ref()).expect(
        "the server's refusal must cross the wire as the deadline class, not as a transient \
         or internal fault a caller would retry",
    );
    assert_eq!(
        outcome.effect,
        wyrd_traits::WriteEffect::NotApplied,
        "and with its effect intact: this write was refused *before* publication, so the \
         caller is entitled to the definite 'nothing landed' — the wire must not flatten the \
         two effects into one: {err}"
    );
    assert_eq!(
        wyrd_traits::classify(err.as_ref()),
        wyrd_traits::ErrorClass::Terminal,
        "a clean refusal is terminal — re-authorize, do not retry: {err}"
    );
    assert_eq!(
        clock.remaining(),
        0,
        "the server must read its clock again at the publish point — an unconsumed reading \
         means the deadline was judged only on acceptance"
    );
    assert!(
        bounded("the read-back", client.get_fragment(id))
            .await
            .unwrap()
            .is_none(),
        "and the refused write must never become observable (0016 outcome (a))"
    );

    server.abort();
}

/// Issue #638 — the **other** deadline outcome over the production client: a D server that
/// began publishing in time but could not verify the publication landed in window answers
/// `ABORTED`, and the client must reconstruct it as `WriteEffect::Unknown` /
/// `ErrorClass::Indeterminate`.
///
/// This is the wire path that must not be flattened. `Unknown` and `NotApplied` are both
/// `WriteDeadlineExpired`, but they license opposite caller behaviour: one says "nothing
/// landed, re-authorize", the other says "durable state may have changed, re-read". A client
/// that reconstructed `ABORTED` as the clean refusal — or let it fall through to the generic
/// transport mapping and lose the class — would have the caller record "nothing landed" over
/// bytes that may be on that D server.
///
/// The server's scripted clock reads 9_500 at admission and at the publication point (the
/// write is live for both, so nothing is refused) and 10_500 once publication has returned,
/// straddling the 10_000 deadline. *Where* those reads fall relative to the store's own
/// rename is not observable over the wire and is pinned on the store itself, by
/// `chunkstore-fs/tests/conformance.rs`'s progress-anchored clock; what this leg pins is
/// that the outcome survives the wire with its effect and class intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publication_the_server_could_not_verify_reconstructs_as_indeterminate_over_grpc() {
    let clock = wyrd_testkit::SteppedClock::new([9_500, 9_500, 10_500]);
    let (client, _dir, server) = connected_with_clock(clock.clone()).await;

    let id = fid(0xdead_beef_0000_0000_0000_0000_0000_0004, 0);
    let frag = fragment(id, b"published, but the store could not time it");
    let err = bounded(
        "a put whose publication the server cannot verify",
        client.put_fragment(id, frag.clone(), Some(10_000)),
    )
    .await
    .expect_err(
        "the D server could not verify the publication landed in window, so it must not \
         acknowledge the write",
    );

    let outcome = wyrd_traits::write_deadline_outcome(err.as_ref()).expect(
        "the outcome must cross the wire as the typed deadline class, not as a transient or \
         internal fault a caller would retry blind",
    );
    assert_eq!(
        outcome.effect,
        wyrd_traits::WriteEffect::Unknown,
        "and with its effect intact: reconstructing this as the clean refusal would have the \
         caller record 'nothing landed' over possibly-durable bytes: {err}"
    );
    assert!(
        outcome.effect.may_have_landed(),
        "the caller must be able to read off that it has to re-read: {err}"
    );
    assert_eq!(
        wyrd_traits::classify(err.as_ref()),
        wyrd_traits::ErrorClass::Indeterminate,
        "neither terminal nor transient — the third class exists for exactly this: {err}"
    );
    assert_eq!(
        clock.remaining(),
        0,
        "the server must read its clock a third time, after publication — an unconsumed \
         reading means the publication was assumed rather than verified"
    );

    // The report is honest in the other direction too: `Unknown` claims nothing about the
    // bytes, and the store did not unlink what it published.
    assert_eq!(
        bounded("the read-back", client.get_fragment(id))
            .await
            .unwrap()
            .as_deref(),
        Some(frag.as_ref()),
    );

    server.abort();
}
