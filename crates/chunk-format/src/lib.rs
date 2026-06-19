//! Reference encoder/decoder for the Wyrd on-disk fragment format.
//!
//! The on-disk bytes are the one spec-first, normative artifact in the project
//! (ADR-0002): data outlives software, so a fragment written under format `v1`
//! must be readable by software written years later. The byte layout is fully
//! specified in `docs/design/specs/chunk-format/v1.md` (ADR-0019).
//!
//! This crate is a **stub at Milestone 0.1**. The 44-byte v1 header
//! encode/decode, the header and payload checksums, and the conformance reader
//! land in Milestone 0.2 (issue #65).

#![forbid(unsafe_code)]

/// The magic that begins every Wyrd fragment: ASCII `"WYRD"` (ADR-0019).
pub const MAGIC: u32 = 0x5759_5244;

/// The v1 format version.
pub const FORMAT_VERSION_V1: u16 = 1;
