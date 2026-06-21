//! M3.2 definition of done (issue #140): a `ChunkStore` can be **walked**
//! (`list_fragments`) and a fragment's bytes **deleted** (`delete_fragment`) —
//! the two affordances M1/M2 deliberately left out (proposal 0005, "`ChunkStore`:
//! enumerate + delete"). The maintenance plane needs them: scrub walks a D server
//! to diff it against the chunk map; GC reclaims orphaned bytes.
//!
//! These are NET-NEW affordances, so "red" before the fix is criterion-absence:
//! the methods do not exist on the trait, so this test does not compile against
//! the unfixed tree. Post-fix it is green over both an **in-process** store and a
//! **local-tonic** gRPC round-trip (real HTTP/2 framing + prost (de)serialization
//! of the additive `ListFragments` / `DeleteFragment` messages), mirroring the
//! shape of `round_trip.rs`.

use std::collections::HashSet;

use bytes::Bytes;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

/// Collect a store's fragments into a set — `list_fragments` makes no order
/// promise, so the assertion is set equality.
async fn listed(store: &impl ChunkStore) -> HashSet<FragmentId> {
    store.list_fragments().await.unwrap().into_iter().collect()
}

/// Stand up a D-server service over a fresh `FsChunkStore`, bound to an ephemeral
/// loopback port, and return a connected client. Mirrors `round_trip.rs`: the
/// listener is bound before the client dials, so there is no startup race.
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

/// The criterion, exercised against any `ChunkStore`: a store can be enumerated,
/// and a fragment's bytes deleted, with other fragments unaffected. Run over both
/// the in-process store and the local-tonic client below.
async fn list_and_delete_round_trip(store: &impl ChunkStore) {
    // A fresh store enumerates as empty.
    assert!(
        listed(store).await.is_empty(),
        "an empty store lists no fragments"
    );

    // Place three fragments across two chunks, including a non-zero EC index.
    let chunk_a = 0xdead_beef_cafe_babe_0000_0000_1234_5678;
    let chunk_b = 0x0000_0000_0000_0000_ffff_ffff_ffff_ffff;
    let ids = [fid(chunk_a, 0), fid(chunk_a, 3), fid(chunk_b, 0)];
    for &id in &ids {
        store
            .put_fragment(id, fragment(id, b"a fragment to be walked"))
            .await
            .unwrap();
    }

    // list_fragments returns EXACTLY the ids the store holds.
    assert_eq!(
        listed(store).await,
        ids.into_iter().collect::<HashSet<_>>(),
        "list_fragments returns exactly the stored fragment ids"
    );

    // The seam is load-bearing: the bytes are present BEFORE the delete.
    let victim = fid(chunk_a, 3);
    assert!(
        store.get_fragment(victim).await.unwrap().is_some(),
        "the fragment's bytes are present before delete"
    );

    // delete_fragment removes the victim's bytes...
    store.delete_fragment(victim).await.unwrap();
    assert!(
        store.get_fragment(victim).await.unwrap().is_none(),
        "after delete_fragment, get_fragment returns Ok(None)"
    );

    // ...while the other fragments are unaffected, in bytes and in the listing.
    for &survivor in &[fid(chunk_a, 0), fid(chunk_b, 0)] {
        assert!(
            store.get_fragment(survivor).await.unwrap().is_some(),
            "a sibling fragment is unaffected by the delete"
        );
    }
    assert_eq!(
        listed(store).await,
        [fid(chunk_a, 0), fid(chunk_b, 0)]
            .into_iter()
            .collect::<HashSet<_>>(),
        "the listing reflects exactly the surviving fragments"
    );

    // delete_fragment is idempotent: deleting an absent fragment is Ok(()).
    store.delete_fragment(victim).await.unwrap();
    store.delete_fragment(fid(0x9999, 7)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_and_delete_in_process() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    list_and_delete_round_trip(&store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_and_delete_over_grpc() {
    let (client, _dir, server) = connected().await;
    list_and_delete_round_trip(&client).await;
    server.abort();
}
