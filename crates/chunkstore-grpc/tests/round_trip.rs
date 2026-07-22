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
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
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
    client.put_fragment(id, frag.clone()).await.unwrap();
    let got = client.get_fragment(id).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(frag.as_ref()),
        "fragment must round-trip byte-identical over gRPC"
    );

    // the store verifies on put: garbage is rejected, not stored.
    assert!(
        client
            .put_fragment(fid(1, 0), Bytes::from_static(b"not a fragment"))
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
        .put_fragment(id, fragment(id, b"healthy until it rots"))
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
        .put_fragment(fid(7, 0), Bytes::from_static(b"not a fragment"))
        .await
        .expect_err("a non-fragment put must be rejected");

    assert_eq!(
        transport_status_code(&err),
        Code::InvalidArgument,
        "a malformed-fragment put is a client (invalid-argument) fault, not server-internal"
    );

    server.abort();
}
