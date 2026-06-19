//! Reference encoder/decoder for the Wyrd on-disk fragment format.
//!
//! The on-disk bytes are the one spec-first, normative artifact in the project
//! (ADR-0002): data outlives software, so a fragment written under format `v1`
//! must be readable by software written years later. The byte layout is fully
//! specified in `docs/design/specs/chunk-format/v1.md` (ADR-0019).
//!
//! The format is **v0/unstable**: the layout may still change and is stamped
//! `v1` only once validated by a second independent reader or a sustained
//! fault-injection run (the spec's own rule).

#![forbid(unsafe_code)]

mod codec;
mod error;
mod header;

pub use codec::{decode, encode, DecodedFragment};
pub use error::FragmentError;
pub use header::{
    ChecksumAlgo, EcSchemeType, EncryptionScheme, FragmentHeader, CORE_HEADER_LEN, FLAG_ENCRYPTED,
};

/// The magic that begins every Wyrd fragment: ASCII `"WYRD"` (ADR-0019).
pub const MAGIC: u32 = 0x5759_5244;

/// The v1 format version.
pub const FORMAT_VERSION_V1: u16 = 1;
