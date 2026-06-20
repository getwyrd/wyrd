//! Generated protobuf wire contracts: message shapes plus the gRPC `ChunkStore`
//! service (M2, proposal 0004) — fragment-addressed `PutFragment`/`GetFragment`
//! and `Health`, with both client and server stubs. Code is generated at build
//! time by [`tonic-prost-build`] from a [`protox`]-produced descriptor set, so no
//! system `protoc` is required (ADR-0016).

#![forbid(unsafe_code)]

/// Version-0 wire contracts (package `wyrd.v0`): message types and the
/// `chunk_store_client` / `chunk_store_server` gRPC stubs.
pub mod v0 {
    include!(concat!(env!("OUT_DIR"), "/wyrd.v0.rs"));
}
