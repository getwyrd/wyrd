//! Errors a reader raises when a fragment does not conform to the v1 format.
//!
//! Every variant corresponds to a rule a conforming reader MUST enforce
//! (`docs/design/specs/chunk-format/v1.md`): a reader rejects a malformed or
//! unsupported fragment rather than guessing at, or returning, bytes it cannot
//! correctly interpret.

use std::fmt;

/// A reason a fragment was rejected during [`decode`](crate::decode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentError {
    /// The buffer is too short to contain the 44-byte core header.
    TruncatedHeader,
    /// The leading four bytes are not the `WYRD` magic.
    BadMagic,
    /// The `format_version` is not one this reader understands.
    UnsupportedVersion(u16),
    /// `header_length` is smaller than the 44-byte core header.
    HeaderTooShort(u16),
    /// `header_length` runs past the end of the buffer.
    HeaderLengthExceedsBuffer,
    /// The header checksum does not match the header bytes.
    HeaderChecksumMismatch,
    /// A reserved `flags` bit (other than encryption) is set.
    ReservedFlagSet(u8),
    /// The encryption flag is set; a v1 reader cannot return undecrypted bytes
    /// and MUST refuse rather than hand back ciphertext as if it were content.
    EncryptedPayloadUnsupported,
    /// `checksum_algo` names an algorithm this reader does not support.
    UnsupportedChecksumAlgo(u8),
    /// `encryption_scheme` names a scheme this reader does not support.
    UnsupportedEncryptionScheme(u8),
    /// `ec_scheme_type` is a reserved/unknown code point.
    ReservedEcScheme(u8),
    /// The buffer ends before the declared payload and its checksum.
    TruncatedPayload,
    /// The payload checksum does not match the stored payload bytes.
    PayloadChecksumMismatch,
}

impl fmt::Display for FragmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FragmentError::TruncatedHeader => {
                write!(f, "buffer too short for the 44-byte core header")
            }
            FragmentError::BadMagic => write!(f, "bad magic; not a WYRD fragment"),
            FragmentError::UnsupportedVersion(v) => {
                write!(f, "unsupported format_version {v}")
            }
            FragmentError::HeaderTooShort(n) => {
                write!(f, "header_length {n} is below the 44-byte core")
            }
            FragmentError::HeaderLengthExceedsBuffer => {
                write!(f, "header_length runs past the end of the buffer")
            }
            FragmentError::HeaderChecksumMismatch => write!(f, "header checksum mismatch"),
            FragmentError::ReservedFlagSet(flags) => {
                write!(f, "reserved flags bit set in {flags:#010b}")
            }
            FragmentError::EncryptedPayloadUnsupported => {
                write!(f, "payload is encrypted; a v1 reader cannot return it")
            }
            FragmentError::UnsupportedChecksumAlgo(a) => {
                write!(f, "unsupported checksum_algo {a}")
            }
            FragmentError::UnsupportedEncryptionScheme(s) => {
                write!(f, "unsupported encryption_scheme {s}")
            }
            FragmentError::ReservedEcScheme(t) => {
                write!(f, "reserved ec_scheme_type {t}")
            }
            FragmentError::TruncatedPayload => {
                write!(f, "buffer ends before the declared payload and checksum")
            }
            FragmentError::PayloadChecksumMismatch => write!(f, "payload checksum mismatch"),
        }
    }
}

impl std::error::Error for FragmentError {}

impl FragmentError {
    /// The variant's identifier, used by the conformance harness to assert that
    /// an `invalid/` vector is rejected for the *specific* documented reason and
    /// not merely rejected for some other reason.
    pub fn variant_name(&self) -> &'static str {
        match self {
            FragmentError::TruncatedHeader => "TruncatedHeader",
            FragmentError::BadMagic => "BadMagic",
            FragmentError::UnsupportedVersion(_) => "UnsupportedVersion",
            FragmentError::HeaderTooShort(_) => "HeaderTooShort",
            FragmentError::HeaderLengthExceedsBuffer => "HeaderLengthExceedsBuffer",
            FragmentError::HeaderChecksumMismatch => "HeaderChecksumMismatch",
            FragmentError::ReservedFlagSet(_) => "ReservedFlagSet",
            FragmentError::EncryptedPayloadUnsupported => "EncryptedPayloadUnsupported",
            FragmentError::UnsupportedChecksumAlgo(_) => "UnsupportedChecksumAlgo",
            FragmentError::UnsupportedEncryptionScheme(_) => "UnsupportedEncryptionScheme",
            FragmentError::ReservedEcScheme(_) => "ReservedEcScheme",
            FragmentError::TruncatedPayload => "TruncatedPayload",
            FragmentError::PayloadChecksumMismatch => "PayloadChecksumMismatch",
        }
    }
}
