//! Combined client and protocol logic.
//!
//! Coarse on purpose at Milestone 0 (ADR-0016): the client write/read paths and
//! the commit protocol will live here and split into finer crates as the
//! boundaries firm up and compile times demand. It depends on the `traits`
//! seams, never on concrete backends — only the `server` binary wires
//! concretes (ADR-0010).
//!
#![forbid(unsafe_code)]

pub mod erasure;
pub mod metadata;
pub mod placement;
pub mod read;
pub mod repair;
pub mod write;
