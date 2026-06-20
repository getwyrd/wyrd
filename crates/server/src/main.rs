//! The Wyrd single binary — the `wyrd` CLI test frontend over M0's in-process
//! write/read paths (issue #90). It composes the concrete backends behind the
//! trait seams (ADR-0010) and exposes `put` / `get` / `demo`; see [`cli`] for
//! the command surface. The command logic lives in the library so it is
//! unit-testable; this entry point is a thin dispatch.
//!
//! [`cli`]: wyrd_server::cli

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    wyrd_server::cli::run(std::env::args())
}
