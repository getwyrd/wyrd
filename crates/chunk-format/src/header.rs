//! The v1 fragment header: field layout, code-point enums, and the parsed
//! [`FragmentHeader`] a reader produces.
//!
//! See `docs/design/specs/chunk-format/v1.md` for the normative byte layout.
//! All multi-byte integers are little-endian.

use crate::error::FragmentError;

/// Size in bytes of the fixed v1 core header. The payload begins at
/// `header_length`, which is this value when there is no extension.
pub const CORE_HEADER_LEN: u16 = 44;

/// Byte offsets of each core-header field (little-endian).
pub(crate) mod offset {
    pub(crate) const MAGIC: usize = 0;
    pub(crate) const FORMAT_VERSION: usize = 4;
    pub(crate) const HEADER_LENGTH: usize = 6;
    pub(crate) const FLAGS: usize = 8;
    pub(crate) const CHECKSUM_ALGO: usize = 9;
    pub(crate) const ENCRYPTION_SCHEME: usize = 10;
    pub(crate) const EC_SCHEME_TYPE: usize = 11;
    pub(crate) const EC_K: usize = 12;
    pub(crate) const EC_M: usize = 13;
    pub(crate) const EC_FRAGMENT_INDEX: usize = 14;
    pub(crate) const CHUNK_ID: usize = 16;
    pub(crate) const PAYLOAD_LENGTH: usize = 32;
    pub(crate) const HEADER_CHECKSUM: usize = 40;
}

/// `flags` bit 0: the payload is encrypted (reserved for a future writer; a v1
/// writer MUST leave it clear).
pub const FLAG_ENCRYPTED: u8 = 0b0000_0001;

/// The payload-checksum algorithm (`checksum_algo`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgo {
    /// crc32c (Castagnoli) — the v1 default.
    Crc32c = 0,
    /// blake3 — reserved; not produced by a v1 writer.
    Blake3 = 1,
}

impl TryFrom<u8> for ChecksumAlgo {
    type Error = FragmentError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ChecksumAlgo::Crc32c),
            1 => Ok(ChecksumAlgo::Blake3),
            other => Err(FragmentError::UnsupportedChecksumAlgo(other)),
        }
    }
}

/// The payload encryption scheme (`encryption_scheme`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionScheme {
    /// No encryption — the only scheme a v1 writer produces.
    None = 0,
}

impl TryFrom<u8> for EncryptionScheme {
    type Error = FragmentError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EncryptionScheme::None),
            other => Err(FragmentError::UnsupportedEncryptionScheme(other)),
        }
    }
}

/// The erasure-coding scheme recorded per fragment (`ec_scheme_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcSchemeType {
    /// Single copy, no erasure coding.
    None = 0,
    /// Replication.
    Replication = 1,
    /// Reed-Solomon.
    ReedSolomon = 2,
}

impl TryFrom<u8> for EcSchemeType {
    type Error = FragmentError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EcSchemeType::None),
            1 => Ok(EcSchemeType::Replication),
            2 => Ok(EcSchemeType::ReedSolomon),
            other => Err(FragmentError::ReservedEcScheme(other)),
        }
    }
}

/// The parsed v1 header fields a reader exposes, and a writer supplies.
///
/// `magic`, `header_length`, and `header_checksum` are not stored here: they are
/// computed and validated by the codec, not chosen by a caller. In v1 the header
/// has no extension, so `header_length` is always [`CORE_HEADER_LEN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    /// The format version. `1` for v1.
    pub format_version: u16,
    /// Raw `flags` byte. A v1 writer sets this to 0.
    pub flags: u8,
    /// Payload-checksum algorithm.
    pub checksum_algo: ChecksumAlgo,
    /// Payload encryption scheme.
    pub encryption_scheme: EncryptionScheme,
    /// Erasure-coding scheme for this fragment.
    pub ec_scheme_type: EcSchemeType,
    /// Data-fragment count k (1 for none/replication).
    pub ec_k: u8,
    /// Parity-fragment count m (0 for none/replication).
    pub ec_m: u8,
    /// This fragment's 0-based index within its stripe.
    pub ec_fragment_index: u16,
    /// The chunk identifier.
    pub chunk_id: u128,
    /// Length in bytes of the stored payload.
    pub payload_length: u64,
}

impl FragmentHeader {
    /// Build a header for a single-copy (`none`) v1 fragment carrying
    /// `payload_length` bytes, with the v1 writer invariants applied: crc32c
    /// checksum, no encryption, no erasure coding.
    pub fn new_v1(chunk_id: u128, payload_length: u64) -> Self {
        Self {
            format_version: crate::FORMAT_VERSION_V1,
            flags: 0,
            checksum_algo: ChecksumAlgo::Crc32c,
            encryption_scheme: EncryptionScheme::None,
            ec_scheme_type: EcSchemeType::None,
            ec_k: 1,
            ec_m: 0,
            ec_fragment_index: 0,
            chunk_id,
            payload_length,
        }
    }
}
