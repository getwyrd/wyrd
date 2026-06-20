//! M2.2 definition of done (issue #112): `GrpcChunkStore` round-trips
//! `put` / `get` / `health` against an **in-process tonic server** over a real
//! loopback gRPC connection — real HTTP/2 framing and prost (de)serialization of
//! the fragment-addressed messages, not an in-memory fake.
//!
//! The server hosts the real `FsChunkStore` (the dev-dependency standing in for
//! `server`'s composition), so this also exercises the integrity contract: a
//! not-found get returns `Ok(None)`, and a non-fragment put is rejected by the
//! store and surfaced as a transport error.

use bytes::Bytes;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore};
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
