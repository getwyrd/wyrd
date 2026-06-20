//! Deterministic-simulation tests for the commit protocol on madsim (ADR-0009).
//!
//! This crate holds no production code — the tests live in `tests/` and run on
//! madsim's single-threaded, seed-reproducible runtime, which requires building
//! with `--cfg madsim`. Run them with `cargo xtask dst`, which sets that flag.

#![forbid(unsafe_code)]
