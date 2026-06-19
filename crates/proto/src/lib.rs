//! Generated protobuf/prost wire-contract message shapes.
//!
//! The shapes are minimal at Milestone 0 (commit, chunk put/get); gRPC
//! transport is deferred (proposal 0001). Code is generated at build time by
//! [`prost-build`] via the pure-Rust [`protox`] frontend, so no system `protoc`
//! is required.

#![forbid(unsafe_code)]

/// Version-0 wire contracts (package `wyrd.v0`).
pub mod v0 {
    include!(concat!(env!("OUT_DIR"), "/wyrd.v0.rs"));
}
