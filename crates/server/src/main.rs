//! The Wyrd single binary.
//!
//! This is the one crate that knows the concrete backends and wires them behind
//! the trait seams (ADR-0010), which is what makes a backend swap a composition
//! change rather than a refactor. The M0 walking skeleton — S3 PUT → four-phase
//! commit → byte-identical GET in one process — is assembled here as the later
//! milestones land.
//!
//! Stub at Milestone 0.1.

#![forbid(unsafe_code)]

fn main() {
    println!("wyrd: walking skeleton not yet assembled (M0.1 scaffold)");
}
