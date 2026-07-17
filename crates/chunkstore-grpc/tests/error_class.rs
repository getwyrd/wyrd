//! Issue #577 — the failure **class** survives the `crates/traits` seam and the chunkstore
//! gRPC seam (proposal 0010 §"Scope boundary" item 6).
//!
//! Before this, everything crossing the seam except four *specific* typed faults was an
//! opaque `BoxError` string: a caller could not tell "try again" (the D server is
//! unreachable / slow / busy) from "retry cannot help" (the data is gone, the config is
//! wrong). `crates/chunkstore-grpc/src/client.rs` reconstructed exactly one class from the
//! wire — `DATA_LOSS` → `IntegrityFault` — and a timeout or an unreachable node arrived as
//! an unclassifiable transport string.
//!
//! Both faults here are raised by a **real default-compiled producer** over a **real
//! loopback tonic connection** (the peer shape: `tests/round_trip.rs`) — never by a double
//! injecting the new seam types:
//!
//! * terminal — a genuine `FsChunkStore` integrity fault, from bytes rotted on the
//!   server's own disk and detected by the store's checksum verify on read;
//! * transient — a genuine transport-level failure: the D server's listener is gone
//!   (unreachable), and separately a request that outlives its deadline (timed out).
//!
//! What is asserted is the **class**, reconstructed client-side: `wyrd_traits::classify`
//! returns a public class *value* with a stable label, not a bare boolean.

use std::time::Duration;

use bytes::Bytes;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore, TransportError};
use wyrd_traits::{classify, BoxError, ChunkId, ChunkStore, ErrorClass, FragmentId};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

/// A running D server that a test can genuinely **kill**.
///
/// `abort()`ing the serve task is not enough and it is worth saying why: tonic spawns each
/// accepted connection onto its own task, so aborting the acceptor leaves the established
/// connection serving happily — a "killed" server that still answers, which would make a
/// transient-fault assertion vacuous. Graceful shutdown is what actually takes the
/// listener *and* the connections down; [`Killable::kill`] waits for the serve future to
/// finish before returning, so afterwards the port is genuinely dead.
struct Killable {
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

impl Killable {
    async fn kill(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.await;
    }
}

/// A D server hosting the real `FsChunkStore` on an ephemeral loopback port, plus a
/// connected client. Mirrors `round_trip.rs::connected` (bound before the client dials, so
/// there is no startup race), with a shutdown signal so the server can be killed for real.
async fn stand_up() -> (GrpcChunkStore, tempfile::TempDir, Killable) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    let (client, killable) = serve(ChunkStoreService::new(store)).await;
    (client, dir, killable)
}

/// Mount `service` on an ephemeral loopback port and return a client connected to it.
async fn serve<S>(service: ChunkStoreService<S>) -> (GrpcChunkStore, Killable)
where
    S: ChunkStore + 'static,
{
    let (client, killable, _) = serve_with(service, |endpoint| async move {
        GrpcChunkStore::connect(endpoint).await.expect("connect")
    })
    .await;
    (client, killable)
}

/// As [`serve`], but the caller builds the client from the endpoint — so a test can dial
/// with a request deadline.
async fn serve_with<S, F, Fut>(
    service: ChunkStoreService<S>,
    dial: F,
) -> (GrpcChunkStore, Killable, String)
where
    S: ChunkStore + 'static,
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = GrpcChunkStore>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown, rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = rx.await;
            })
            .await
            .expect("serve");
    });

    let endpoint = format!("http://{addr}");
    let client = dial(endpoint.clone()).await;
    (client, Killable { shutdown, handle }, endpoint)
}

/// Whether the producing backend's own `TransportError` is still reachable anywhere in
/// `err`'s source chain — the codebase's own idiom for finding a wrapped fault
/// (`wyrd_traits::is_integrity_fault` walks the chain the same way).
fn transport_error_in_chain(err: &BoxError) -> bool {
    let mut next: Option<&(dyn std::error::Error + 'static)> = Some(err.as_ref());
    while let Some(e) = next {
        if e.is::<TransportError>() {
            return true;
        }
        next = e.source();
    }
    false
}

/// **Terminal, over the wire.** A fragment that rots on the D server's disk is detected by
/// the real `FsChunkStore` verify, rides `DATA_LOSS`, and reconstructs client-side as the
/// seam's `Integrity` class — a *distinct* class that is nonetheless terminal, because
/// retrying the same fetch can never make bad bytes verify.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_genuine_integrity_fault_classifies_integrity_over_grpc() {
    let (client, dir, server) = stand_up().await;

    let id = fid(0xC0DE_0000_0000_0000_0000_0000_0000_0577, 0);
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

    assert_eq!(
        classify(err.as_ref()),
        ErrorClass::Integrity,
        "a rotten fragment must reconstruct as the integrity class client-side; err = {err}"
    );
    assert!(
        classify(err.as_ref()).is_terminal(),
        "integrity is a TERMINAL class — retrying cannot make bad bytes verify; err = {err}"
    );
    assert!(
        !classify(err.as_ref()).is_transient(),
        "corruption must never be offered to a retry policy; err = {err}"
    );

    server.kill().await;
}

/// **Transient, over the wire.** The binding leg: the D server's listener is gone, so a
/// request on the established channel fails at the transport — a genuine unreachable-node
/// fault, produced by tonic, not injected. It must reconstruct as `Transient`.
///
/// Red pre-fix: the client boxed a bare `TransportError`, which carries no seam type, so
/// the class could not be recovered at all — every consumer had only a string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_genuine_unreachable_d_server_classifies_transient_over_grpc() {
    let (client, _dir, server) = stand_up().await;

    // Prove the seam is live first, so the failure below is the server dying and not a
    // connection that never worked.
    assert!(client.get_fragment(fid(1, 0)).await.unwrap().is_none());

    // Kill the D server for real — listener and established connections both.
    server.kill().await;

    let err = client
        .get_fragment(fid(1, 0))
        .await
        .expect_err("a get against a dead D server must fail, not answer Ok(None)");

    assert_eq!(
        classify(err.as_ref()),
        ErrorClass::Transient,
        "an unreachable D server is the transient class — the node may be back a second \
         later, and a caller must be able to tell that from data being gone; err = {err}"
    );
    assert!(
        !classify(err.as_ref()).is_terminal(),
        "an unreachable node must not be reported as terminal; err = {err}"
    );
    // The class must not be confused with the corruption class it has to stay distinct from.
    assert!(
        !wyrd_traits::is_integrity_fault(err.as_ref()),
        "a transient fault is not a corruption finding; err = {err}"
    );
}

/// **Transient, over the wire — the timed-out flavour.** A request that outlives the
/// channel's deadline fails with a genuine tonic `DEADLINE_EXCEEDED`, which is the second
/// of proposal 0010's transient trio (unreachable / timed out / busy).
///
/// The store parks so the deadline is the only thing that can answer; the *fault* is still
/// produced by the real client + transport, and the class is reconstructed by the real
/// mapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_genuine_request_timeout_classifies_transient_over_grpc() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = ParkedStore {
        inner: FsChunkStore::open(dir.path()).expect("open store"),
    };
    let (client, server, _) = serve_with(ChunkStoreService::new(store), |endpoint| async move {
        GrpcChunkStore::connect_with_timeout(endpoint, Duration::from_millis(250))
            .await
            .expect("connect")
    })
    .await;

    let err = client
        .get_fragment(fid(2, 0))
        .await
        .expect_err("a request past its deadline must fail, not hang");

    assert_eq!(
        classify(err.as_ref()),
        ErrorClass::Transient,
        "a timed-out request is the transient class — the D server may simply be slow; \
         err = {err}"
    );

    drop(server);
}

/// Naming the class must not cost the detail the backend already had: the `TransportError`
/// — and through it the wire `Status`, its code and its message — stays reachable in the
/// source chain. A classification that threw the transport away would answer "why did it
/// fail" at the class level while destroying it at the diagnostic level.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_transient_class_keeps_the_transport_detail_reachable() {
    let (client, _dir, server) = stand_up().await;
    server.kill().await;

    let err = client
        .get_fragment(fid(3, 0))
        .await
        .expect_err("a get against a dead D server must fail");

    assert_eq!(classify(err.as_ref()), ErrorClass::Transient);
    assert!(
        transport_error_in_chain(&err),
        "the producing TransportError must survive underneath the seam class — the class \
         wraps the backend's error, it does not replace it; err = {err}"
    );
}

/// The whole point, in one assertion: a transient fault and a terminal fault raised behind
/// the same seam are **mutually distinguishable** by class, which is exactly what a
/// `BoxError` string could not do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_and_terminal_are_distinguishable_across_the_seam() {
    // Terminal: a genuine integrity fault.
    let (client, dir, server) = stand_up().await;
    let id = fid(0xBEEF, 0);
    client
        .put_fragment(id, fragment(id, b"rots in a moment"))
        .await
        .unwrap();
    let path = fragment_path(dir.path(), id);
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let terminal = client.get_fragment(id).await.expect_err("corrupt");

    // Transient: the same client, the same seam, after the D server dies.
    server.kill().await;
    let transient = client.get_fragment(fid(4, 0)).await.expect_err("dead");

    let (tc, uc) = (classify(terminal.as_ref()), classify(transient.as_ref()));
    assert_ne!(
        tc, uc,
        "the two faults must not collapse into one class: terminal={terminal}, \
         transient={transient}"
    );
    assert!(tc.is_terminal() && uc.is_transient());
    // The label form issue #575's error counter keys on — a class value, not a bool.
    assert_eq!(tc.as_str(), "integrity");
    assert_eq!(uc.as_str(), "transient");
}

/// A store whose `get_fragment` never answers, so a request can only be ended by the
/// client's deadline. It injects **no** seam type: the transient fault the test asserts on
/// is manufactured by tonic's own timeout and classified by the real client mapping.
struct ParkedStore {
    inner: FsChunkStore,
}

#[async_trait::async_trait]
impl ChunkStore for ParkedStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> wyrd_traits::Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, _id: FragmentId) -> wyrd_traits::Result<Option<Bytes>> {
        // Longer than any deadline this test sets; the request never completes on its own.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(None)
    }

    async fn list_fragments(&self) -> wyrd_traits::Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> wyrd_traits::Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> wyrd_traits::Result<wyrd_traits::Health> {
        self.inner.health().await
    }
}
