//! Regression test for issue #268 — a `get_fragment` that fails at the D server
//! with a block-layer read fault (`EIO` / dead sector) must arrive at the client
//! as a **block-read fault** (`wyrd_traits::BlockReadFault`), distinct from both
//! an integrity/corruption fault and a transient fault.
//!
//! Exercises the WIRE seam: a real tonic `Status` crosses an in-process
//! `client ↔ server` channel (the same loopback transport `round_trip.rs` uses,
//! no Docker required). The fault-injecting inner store is a simple in-test
//! struct — the server hosts it behind the gRPC service just as production code
//! hosts `FsChunkStore`.
//!
//! Asserts BOTH halves of the success criterion (brief §Success criterion):
//!
//! (a) `wyrd_traits::is_block_read_fault(err)` returns `true` — so
//!     `reconstruction::is_permanent_read_fault` (which calls `is_block_read_fault`
//!     via the source chain) returns `true`, and reconstruction reads around the
//!     fragment rather than retrying it.
//!
//! (b) `wyrd_traits::is_integrity_fault(err)` returns `false` — the fault is NOT
//!     a corruption finding; scrub takes the same branch as a local `EIO` at
//!     `scrub.rs:108` (`Err(e) => return Err(e)`), never `emit_corruption`.
//!
//! Also asserts the third-category invariant: a generic non-EIO, non-integrity
//! server error must remain a `TransportError` (the retry-policy class) and must
//! NOT be promoted to a permanent fault by the EIO fix.
//!
//! Pre-fix (main): the server maps every non-`IntegrityFault` error to
//! `Status::internal`; the client maps `INTERNAL` to `TransportError::Rpc`, which
//! carries no `io::Error` in its source chain, so `is_block_read_fault` returns
//! `false` and a dead-sector fault is classified transient → retried forever.

#![forbid(unsafe_code)]

use std::io;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore, TransportError};
use wyrd_traits::{ChunkStore, FragmentId, Health, Result as WyrdResult};

/// Fragment ID used across the tests.
fn fid() -> FragmentId {
    FragmentId {
        chunk: 0x0000_0000_0000_0000_0000_0000_0000_0268,
        index: 0,
    }
}

// ---------------------------------------------------------------------------
// Fault-injecting inner stores
// ---------------------------------------------------------------------------

/// A `ChunkStore` whose `get_fragment` always returns POSIX `EIO` (errno 5) —
/// the same raw error a real `FsChunkStore` surfaces when the block device reports
/// a dead-sector read failure at the OS level.
struct EioStore;

#[async_trait]
impl ChunkStore for EioStore {
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> WyrdResult<()> {
        Ok(())
    }

    async fn get_fragment(&self, _id: FragmentId) -> WyrdResult<Option<Bytes>> {
        Err(Box::new(io::Error::from_raw_os_error(5))) // POSIX EIO
    }

    async fn list_fragments(&self) -> WyrdResult<Vec<FragmentId>> {
        Ok(vec![])
    }

    async fn delete_fragment(&self, _id: FragmentId) -> WyrdResult<()> {
        Ok(())
    }

    async fn health(&self) -> WyrdResult<Health> {
        Ok(Health::Healthy)
    }
}

/// A `ChunkStore` whose `get_fragment` returns a generic server-side error that
/// is neither a corruption fault nor a block-read fault — it should arrive at the
/// client as a `TransportError` (the retry-policy's transient class), not be
/// promoted to a permanent fault.
struct GenericErrStore;

#[async_trait]
impl ChunkStore for GenericErrStore {
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> WyrdResult<()> {
        Ok(())
    }

    async fn get_fragment(&self, _id: FragmentId) -> WyrdResult<Option<Bytes>> {
        Err(Box::new(io::Error::other(
            "generic server-side error (not EIO, not corruption)",
        )))
    }

    async fn list_fragments(&self) -> WyrdResult<Vec<FragmentId>> {
        Ok(vec![])
    }

    async fn delete_fragment(&self, _id: FragmentId) -> WyrdResult<()> {
        Ok(())
    }

    async fn health(&self) -> WyrdResult<Health> {
        Ok(Health::Healthy)
    }
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Stand up an in-process gRPC D-server service wrapping `store`, bound to an
/// ephemeral loopback port, and return a connected `GrpcChunkStore` client.
///
/// The listener is bound before the client dials, eliminating any startup race
/// (mirrors `round_trip.rs::connected`). Servers are aborted at the end of each
/// test via the returned `JoinHandle`.
async fn stand_up<S: ChunkStore + 'static>(
    store: S,
) -> (GrpcChunkStore, tokio::task::JoinHandle<()>) {
    let service = ChunkStoreService::new(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    let client = GrpcChunkStore::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    (client, handle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Issue #268 — remote EIO block-read fault over the wire:
///
/// A D-server `get_fragment` that fails with POSIX `EIO` (errno 5) must arrive
/// at the client as a **block-read fault**, satisfying BOTH halves of the
/// success criterion:
///
/// (a) `wyrd_traits::is_block_read_fault` returns `true` →
///     `reconstruction::is_permanent_read_fault` returns `true` (via the source
///     chain: `BlockReadFault::source()` → `io::Error(EIO)`) → reconstruction
///     reads around the fragment rather than retrying it.
///
/// (b) `wyrd_traits::is_integrity_fault` returns `false` → the scrub consumer
///     does NOT enter the `emit_corruption` / `enqueue_repair` branch — it
///     takes the same path as a local `EIO` (`scrub.rs:108`).
///
/// Pre-fix: the server maps EIO to `Status::internal`; the client wraps it in
/// `TransportError::Rpc`, which carries no `io::Error` in its chain, so
/// `is_block_read_fault` returns `false` and the fault is classified transient.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_eio_is_block_read_fault_not_corruption_over_grpc() {
    let (client, server) = stand_up(EioStore).await;

    let err = client
        .get_fragment(fid())
        .await
        .expect_err("EIO from the D server must propagate as an error, not Ok");

    // (a) Permanent-read-fault: reconstruction reads around it rather than retrying.
    assert!(
        wyrd_traits::is_block_read_fault(err.as_ref()),
        "a remote EIO must be classified as a permanent block-read fault so \
         reconstruction reads around it — not retried as transient; err = {err}"
    );

    // (b) NOT a corruption finding: scrub must not call emit_corruption for a dead sector.
    assert!(
        !wyrd_traits::is_integrity_fault(err.as_ref()),
        "a remote EIO must NOT be classified as a corruption/integrity fault — \
         a dead sector is not a checksum failure; err = {err}"
    );

    server.abort();
}

/// Issue #268 — third-category invariant: a generic non-EIO, non-integrity server
/// error must remain a `TransportError` (the retry-policy's transient class).
///
/// The EIO-classification fix must not accidentally promote unrelated errors to
/// permanent; a non-EIO, non-integrity failure is the retry case (transient), not
/// a read-around or a corruption finding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_generic_error_stays_transient_transport_error_over_grpc() {
    let (client, server) = stand_up(GenericErrStore).await;

    let err = client
        .get_fragment(fid())
        .await
        .expect_err("a server-side error must propagate as an error, not Ok");

    // Neither a corruption finding nor a block-read fault.
    assert!(
        !wyrd_traits::is_integrity_fault(err.as_ref()),
        "a generic server error must NOT be classified as corruption; err = {err}"
    );
    assert!(
        !wyrd_traits::is_block_read_fault(err.as_ref()),
        "a generic server error must NOT be classified as a block-read fault; err = {err}"
    );

    // Arrives as a TransportError — the retry-policy's transient classification.
    assert!(
        err.downcast_ref::<TransportError>().is_some(),
        "a generic server error must arrive as a TransportError (the retry class), \
         not as a permanent fault; err = {err}"
    );

    server.abort();
}
