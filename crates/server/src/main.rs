//! The Wyrd single binary.
//!
//! This is the one crate that knows the concrete backends and wires them behind
//! the trait seams (ADR-0010), which is what makes a backend swap a composition
//! change rather than a refactor. The M0 walking skeleton — S3 PUT → four-phase
//! commit → byte-identical GET in one process — is assembled here as the later
//! milestones land.
//!
//! The S3 path, redb metadata, and filesystem chunks attach here as M0.5–M0.8
//! land; for now this wires the L5 coordination seam to its single-process
//! backend (M0.7).

#![forbid(unsafe_code)]

use wyrd_coordination_mem::MemCoordination;
use wyrd_traits::Coordination;

/// Choose the L5 coordination backend for this deployment. The concrete is named
/// here and nowhere else (ADR-0010): a networked profile swaps in etcd at this
/// one line without touching any caller.
fn coordination() -> Box<dyn Coordination> {
    Box::new(MemCoordination::new())
}

fn main() {
    // Construct the seam to prove it composes behind the trait; the walking
    // skeleton that drives it (S3 PUT → commit → GET) is assembled in M0.8.
    let _coordination = coordination();
    println!("wyrd: walking skeleton not yet assembled (M0.1 scaffold)");
}
