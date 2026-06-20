//! gRPC [`ChunkStore`](wyrd_traits::ChunkStore): the networked storage seam
//! (Milestone 2, proposal 0004). This one crate hosts **both** sides of the wire
//! contract (coarse-then-split, ADR-0016):
//!
//! - [`GrpcChunkStore`] — a `traits::ChunkStore` *client* that dials a D-server
//!   endpoint and turns each `put_fragment` / `get_fragment` / `health` call into
//!   a tonic round-trip. It is a consumer of the trait it implements.
//! - [`ChunkStoreService`] — the D-server-side gRPC service, generic over an
//!   injected `S: ChunkStore`, so it hosts the filesystem store in production and
//!   a fault-injecting fake under DST. It is deliberately dumb: it moves bytes
//!   and reports liveness, with no placement, metadata, or identity (§5, §8.5).
//!
//! Per the dependency rule (ADR-0010) this crate depends only on `traits` and
//! the `proto` wire contract — **never** on a concrete store. Integrity is the
//! injected store's responsibility (the trait contract: implementations verify
//! a fragment's self-describing checksums on put and get); the service does not
//! re-interpret the bytes. The server side is the coarse start of the
//! architecture's named `dserver` crate.

#![forbid(unsafe_code)]

mod client;
mod conv;
mod error;
mod fanout;
mod server;

pub use client::GrpcChunkStore;
pub use error::TransportError;
pub use fanout::FanoutChunkStore;
pub use server::ChunkStoreService;

/// The generated tonic server wrapper, re-exported so a host (the `server`
/// binary's `d-server` role, or a test) can mount a [`ChunkStoreService`]
/// without reaching into the `proto` crate's generated module layout.
pub use wyrd_proto::v0::chunk_store_server::ChunkStoreServer;
