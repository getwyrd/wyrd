//! The typed transport-error enum surfaced by [`GrpcChunkStore`](crate::GrpcChunkStore).
//!
//! The trait returns a boxed error ([`wyrd_traits::BoxError`]); this enum is what
//! it boxes, so a caller can **downcast and branch on the failure kind** rather
//! than match on strings. The data-path *policy* that consumes these — retry a
//! fragment elsewhere on `Unavailable`/`Timeout`, treat a slow/dead D server as
//! absent — lands with the parallel write and any-*k* read in M2.4/M2.5; M2.2
//! only classifies. (A not-found `get_fragment` is **not** here: it maps to
//! `Ok(None)`, preserving the trait's `Option` contract.)

use std::fmt;

use tonic::{Code, Status};

/// A failure from the gRPC `ChunkStore` client, classified for the data-path
/// policy that consumes it.
#[derive(Debug)]
pub enum TransportError {
    /// The D server was unreachable or returned `UNAVAILABLE` — retry elsewhere.
    Unavailable(Status),
    /// The request deadline was exceeded — treat as a slow/dead fragment.
    Timeout(Status),
    /// Any other gRPC status from a reachable server (e.g. the server rejected a
    /// malformed or failing-integrity put).
    Rpc(Status),
    /// The channel to the endpoint could not be established.
    Connect(tonic::transport::Error),
}

impl From<Status> for TransportError {
    fn from(status: Status) -> Self {
        match status.code() {
            Code::Unavailable => TransportError::Unavailable(status),
            Code::DeadlineExceeded => TransportError::Timeout(status),
            _ => TransportError::Rpc(status),
        }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Unavailable(s) => write!(f, "D server unavailable: {s}"),
            TransportError::Timeout(s) => write!(f, "D server request timed out: {s}"),
            TransportError::Rpc(s) => write!(f, "D server rpc error: {s}"),
            TransportError::Connect(e) => write!(f, "could not connect to D server: {e}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransportError::Unavailable(s)
            | TransportError::Timeout(s)
            | TransportError::Rpc(s) => Some(s),
            TransportError::Connect(e) => Some(e),
        }
    }
}
