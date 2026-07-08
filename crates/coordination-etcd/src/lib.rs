//! etcd-backed [`Coordination`](wyrd_traits::Coordination): the networked SECOND
//! implementation of the L5 coordination trait (ADR-0006), behind the
//! byte-for-byte-unchanged trait and selected by `server` composition
//! (ADR-0008/0016) — the mirror of `metadata-tikv` for `MetadataStore`.
//!
//! ## What is real, and where it is proven
//!
//! Every method carries genuine etcd semantics: leased/expiring registration +
//! `discover`; single-leader election whose fencing token (etcd's mvcc revision)
//! rises across terms; mutually-exclusive fenced locks; and config with a
//! monotonic revision. A background keep-alive holds each leadership/lock lease
//! for the life of the hold and revokes it on release (unlock, cancellation, or
//! drop), so a hold never silently lapses and a cancelled campaign never leaks an
//! orphaned lease.
//!
//! The store ([`store`]) is compiled two ways from **one** source:
//! - `--cfg madsim` aliases `etcd-client` to the **madsim etcd simulator** (no
//!   protoc, no live etcd), so `cargo xtask dst` drives the shared contract suite
//!   AND cross-instance single-leader/mutual-exclusion/expiry properties against
//!   it DETERMINISTICALLY (`crates/dst/tests/coordination.rs`). This is the
//!   #264/#258-style DST-fidelity answer for an etcd backend.
//! - `--features etcd` pulls the **real** `etcd-client`; `cargo xtask
//!   etcd-conformance` drives the same shared suite against a throwaway `deploy/`
//!   etcd (endpoint-gated, so `ci` stays green without one).
//!
//! The pure key-layout and token/interval helpers ([`keyspace`], [`fencing`],
//! [`hold`]) carry no etcd dependency, so they are unit-tested on **every** build,
//! including the default feature-off `cargo xtask ci`.

#![forbid(unsafe_code)]

pub mod fencing;
pub mod hold;
pub mod keyspace;

#[cfg(any(feature = "etcd", madsim))]
mod store;
#[cfg(any(feature = "etcd", madsim))]
pub use store::EtcdCoordination;
