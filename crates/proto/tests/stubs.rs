//! Pins the M2.1 wire contract (issue #111): the generated surface has both
//! gRPC client and server stubs, and the put/get requests are fragment-addressed.
//! This is a compile-first assertion — it fails to build if a stub is missing or
//! the requests lose their `FragmentId`.

#![forbid(unsafe_code)]

use wyrd_proto::v0::{
    chunk_store_client::ChunkStoreClient, chunk_store_server::ChunkStoreServer, ChunkId,
    FragmentGetResponse, FragmentId, FragmentPutRequest,
};

#[test]
fn requests_are_fragment_addressed() {
    // A put carries a FragmentId { chunk, index } — the M1 fragment-addressed unit,
    // not the M0 bare ChunkId.
    let req = FragmentPutRequest {
        id: Some(FragmentId {
            chunk: Some(ChunkId { hi: 1, lo: 2 }),
            index: 7,
        }),
        fragment: vec![0xab, 0xcd],
    };
    assert_eq!(req.id.unwrap().index, 7);

    // A get response carries optional bytes — absent maps to the trait's `None`.
    assert!(FragmentGetResponse { fragment: None }.fragment.is_none());
}

#[test]
fn both_client_and_server_stubs_are_generated() {
    // Naming the generated types in type position is the assertion; this won't
    // compile unless tonic emitted both the client and server sides.
    fn assert_type<T>() {}
    let _ = assert_type::<ChunkStoreClient<()>>;
    let _ = assert_type::<ChunkStoreServer<()>>;
}
