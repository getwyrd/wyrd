//! Encode and decode v1 fragments (`docs/design/specs/chunk-format/v1.md`).
//!
//! A stored fragment is a 44-byte core header, then the payload, then the
//! payload checksum. Both checksums are crc32c (Castagnoli). The payload
//! checksum is computed over the **stored** payload bytes — the ciphertext when
//! a payload is encrypted — so integrity is verifiable without the decryption
//! key. All integers are little-endian.

use crate::error::FragmentError;
use crate::header::{
    offset, ChecksumAlgo, EcSchemeType, EncryptionScheme, FragmentHeader, CORE_HEADER_LEN,
    FLAG_ENCRYPTED,
};
use crate::{FORMAT_VERSION_V1, MAGIC};

/// A fragment parsed and verified by [`decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFragment {
    /// The verified header fields.
    pub header: FragmentHeader,
    /// The stored payload bytes (verified against the payload checksum).
    pub payload: Vec<u8>,
}

/// Encode a v1 fragment: header, then `payload`, then the payload checksum.
///
/// This is a v1 writer, so it produces the v1 invariants — `header_length = 44`
/// (no extension), crc32c checksums — and records `payload.len()` as the
/// authoritative payload length. The header's `flags`, EC fields, and chunk id
/// are taken from `header`; a v1 writer supplies a header built via
/// [`FragmentHeader::new_v1`].
pub fn encode(header: &FragmentHeader, payload: &[u8]) -> Vec<u8> {
    let header_length = CORE_HEADER_LEN;
    let mut buf = vec![0u8; header_length as usize];

    buf[offset::MAGIC..offset::MAGIC + 4].copy_from_slice(&MAGIC.to_le_bytes());
    buf[offset::FORMAT_VERSION..offset::FORMAT_VERSION + 2]
        .copy_from_slice(&header.format_version.to_le_bytes());
    buf[offset::HEADER_LENGTH..offset::HEADER_LENGTH + 2]
        .copy_from_slice(&header_length.to_le_bytes());
    buf[offset::FLAGS] = header.flags;
    buf[offset::CHECKSUM_ALGO] = header.checksum_algo as u8;
    buf[offset::ENCRYPTION_SCHEME] = header.encryption_scheme as u8;
    buf[offset::EC_SCHEME_TYPE] = header.ec_scheme_type as u8;
    buf[offset::EC_K] = header.ec_k;
    buf[offset::EC_M] = header.ec_m;
    buf[offset::EC_FRAGMENT_INDEX..offset::EC_FRAGMENT_INDEX + 2]
        .copy_from_slice(&header.ec_fragment_index.to_le_bytes());
    buf[offset::CHUNK_ID..offset::CHUNK_ID + 16].copy_from_slice(&header.chunk_id.to_le_bytes());
    buf[offset::PAYLOAD_LENGTH..offset::PAYLOAD_LENGTH + 8]
        .copy_from_slice(&(payload.len() as u64).to_le_bytes());

    // header_checksum: crc32c over [0, header_length) with its own four bytes
    // (already zero in `buf`) taken as zero.
    let header_checksum = crc32c::crc32c(&buf);
    buf[offset::HEADER_CHECKSUM..offset::HEADER_CHECKSUM + 4]
        .copy_from_slice(&header_checksum.to_le_bytes());

    buf.extend_from_slice(payload);
    buf.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    buf
}

/// Decode and fully verify a v1 fragment.
///
/// Enforces the reader rules in order: the header checksum is verified before
/// any other field is acted on, unknown code points and reserved/encryption
/// flags are rejected, an unrecognized header extension is skipped via
/// `header_length`, and the payload checksum is verified before the payload is
/// returned.
pub fn decode(buf: &[u8]) -> Result<DecodedFragment, FragmentError> {
    if buf.len() < CORE_HEADER_LEN as usize {
        return Err(FragmentError::TruncatedHeader);
    }

    if read_u32(buf, offset::MAGIC) != MAGIC {
        return Err(FragmentError::BadMagic);
    }

    let format_version = read_u16(buf, offset::FORMAT_VERSION);
    if format_version != FORMAT_VERSION_V1 {
        return Err(FragmentError::UnsupportedVersion(format_version));
    }

    let header_length = read_u16(buf, offset::HEADER_LENGTH);
    if header_length < CORE_HEADER_LEN {
        return Err(FragmentError::HeaderTooShort(header_length));
    }
    if header_length as usize > buf.len() {
        return Err(FragmentError::HeaderLengthExceedsBuffer);
    }

    // Verify the header checksum before acting on any other field.
    let stored_header_checksum = read_u32(buf, offset::HEADER_CHECKSUM);
    if stored_header_checksum != header_checksum(&buf[..header_length as usize]) {
        return Err(FragmentError::HeaderChecksumMismatch);
    }

    let flags = buf[offset::FLAGS];
    if flags & FLAG_ENCRYPTED != 0 {
        // A v1 reader cannot return undecrypted bytes; refuse rather than guess.
        return Err(FragmentError::EncryptedPayloadUnsupported);
    }
    if flags & !FLAG_ENCRYPTED != 0 {
        return Err(FragmentError::ReservedFlagSet(flags));
    }

    let checksum_algo = ChecksumAlgo::try_from(buf[offset::CHECKSUM_ALGO])?;
    if checksum_algo == ChecksumAlgo::Blake3 {
        // A defined-but-reserved code point; this v1 reader does not support it.
        return Err(FragmentError::UnsupportedChecksumAlgo(
            ChecksumAlgo::Blake3 as u8,
        ));
    }
    let encryption_scheme = EncryptionScheme::try_from(buf[offset::ENCRYPTION_SCHEME])?;
    let ec_scheme_type = EcSchemeType::try_from(buf[offset::EC_SCHEME_TYPE])?;

    let payload_length = read_u64(buf, offset::PAYLOAD_LENGTH);

    // The payload begins at header_length (skipping any extension we did not
    // need to understand) and is followed by its checksum.
    let payload_start = header_length as usize;
    let payload_end = payload_start
        .checked_add(payload_length as usize)
        .ok_or(FragmentError::TruncatedPayload)?;
    let checksum_end = payload_end
        .checked_add(4) // crc32c
        .ok_or(FragmentError::TruncatedPayload)?;
    if checksum_end > buf.len() {
        return Err(FragmentError::TruncatedPayload);
    }

    let payload = &buf[payload_start..payload_end];
    let stored_payload_checksum = read_u32(buf, payload_end);
    if stored_payload_checksum != crc32c::crc32c(payload) {
        return Err(FragmentError::PayloadChecksumMismatch);
    }

    let header = FragmentHeader {
        format_version,
        flags,
        checksum_algo,
        encryption_scheme,
        ec_scheme_type,
        ec_k: buf[offset::EC_K],
        ec_m: buf[offset::EC_M],
        ec_fragment_index: read_u16(buf, offset::EC_FRAGMENT_INDEX),
        chunk_id: read_u128(buf, offset::CHUNK_ID),
        payload_length,
    };

    Ok(DecodedFragment {
        header,
        payload: payload.to_vec(),
    })
}

/// crc32c over the header bytes with the four `header_checksum` bytes taken as
/// zero (the rule the field's own value cannot contribute to its checksum).
fn header_checksum(header_bytes: &[u8]) -> u32 {
    let mut tmp = header_bytes.to_vec();
    tmp[offset::HEADER_CHECKSUM..offset::HEADER_CHECKSUM + 4].fill(0);
    crc32c::crc32c(&tmp)
}

// Little-endian field readers. Each call site has already bounds-checked the
// region, so the fixed-size slice conversions cannot fail.
fn read_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(buf[at..at + 2].try_into().unwrap())
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().unwrap())
}

fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().unwrap())
}

fn read_u128(buf: &[u8], at: usize) -> u128 {
    u128::from_le_bytes(buf[at..at + 16].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{ChecksumAlgo, EcSchemeType};

    const CHUNK_ID: u128 = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10;

    /// Re-stamp a valid header checksum after mutating header bytes, so a test
    /// can exercise a check that happens *after* the header-checksum gate.
    fn restamp_header_checksum(buf: &mut [u8]) {
        let hc = header_checksum(&buf[..CORE_HEADER_LEN as usize]);
        buf[offset::HEADER_CHECKSUM..offset::HEADER_CHECKSUM + 4]
            .copy_from_slice(&hc.to_le_bytes());
    }

    fn valid_fragment(payload: &[u8]) -> Vec<u8> {
        encode(
            &FragmentHeader::new_v1(CHUNK_ID, payload.len() as u64),
            payload,
        )
    }

    #[test]
    fn round_trips_a_basic_fragment() {
        let payload = b"hello wyrd";
        let decoded = decode(&valid_fragment(payload)).unwrap();
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.header.chunk_id, CHUNK_ID);
        assert_eq!(decoded.header.payload_length, payload.len() as u64);
        assert_eq!(decoded.header.ec_scheme_type, EcSchemeType::None);
    }

    #[test]
    fn round_trips_an_empty_payload() {
        let decoded = decode(&valid_fragment(b"")).unwrap();
        assert!(decoded.payload.is_empty());
        assert_eq!(decoded.header.payload_length, 0);
    }

    /// `:72` (`< -> <=`) and `:89` (`> -> >=`) — a buffer of EXACTLY the header
    /// length is a present, intact header with its payload truncated, NOT a
    /// truncated header (`:72`) and NOT a header that overruns the buffer (`:89`).
    /// Both boundary mutants misclassify it: `<=` at :72 returns `TruncatedHeader`
    /// and `>=` at :89 returns `HeaderLengthExceedsBuffer`, so pinning the result
    /// to `TruncatedPayload` kills both. (Prior decode tests only hit strictly
    /// shorter / strictly longer buffers, never `len == header_length`.)
    #[test]
    fn header_length_exact_buffer_is_truncated_payload() {
        // An empty-payload fragment is header + 0 payload + 4-byte checksum; drop
        // the checksum so the buffer is exactly the header length.
        let mut buf = valid_fragment(b"");
        buf.truncate(CORE_HEADER_LEN as usize);
        assert_eq!(buf.len(), CORE_HEADER_LEN as usize);
        assert_eq!(decode(&buf), Err(FragmentError::TruncatedPayload));
    }

    #[test]
    fn round_trips_a_replication_fragment() {
        let mut header = FragmentHeader::new_v1(CHUNK_ID, 3);
        header.ec_scheme_type = EcSchemeType::Replication;
        header.ec_fragment_index = 2;
        let decoded = decode(&encode(&header, b"abc")).unwrap();
        assert_eq!(decoded.header.ec_scheme_type, EcSchemeType::Replication);
        assert_eq!(decoded.header.ec_fragment_index, 2);
    }

    #[test]
    fn rejects_a_short_buffer() {
        assert_eq!(decode(&[0u8; 10]), Err(FragmentError::TruncatedHeader));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut buf = valid_fragment(b"x");
        buf[offset::MAGIC] ^= 0xff;
        assert_eq!(decode(&buf), Err(FragmentError::BadMagic));
    }

    #[test]
    fn rejects_an_unknown_version() {
        let mut buf = valid_fragment(b"x");
        buf[offset::FORMAT_VERSION..offset::FORMAT_VERSION + 2]
            .copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(decode(&buf), Err(FragmentError::UnsupportedVersion(2)));
    }

    #[test]
    fn rejects_header_length_below_core() {
        let mut buf = valid_fragment(b"x");
        buf[offset::HEADER_LENGTH..offset::HEADER_LENGTH + 2].copy_from_slice(&43u16.to_le_bytes());
        assert_eq!(decode(&buf), Err(FragmentError::HeaderTooShort(43)));
    }

    #[test]
    fn rejects_header_length_past_buffer() {
        let mut buf = valid_fragment(b"x");
        let too_big = (buf.len() as u16) + 1;
        buf[offset::HEADER_LENGTH..offset::HEADER_LENGTH + 2]
            .copy_from_slice(&too_big.to_le_bytes());
        assert_eq!(decode(&buf), Err(FragmentError::HeaderLengthExceedsBuffer));
    }

    #[test]
    fn rejects_a_corrupt_header() {
        let mut buf = valid_fragment(b"x");
        // Corrupt a header field without re-stamping the checksum.
        buf[offset::CHUNK_ID] ^= 0xff;
        assert_eq!(decode(&buf), Err(FragmentError::HeaderChecksumMismatch));
    }

    #[test]
    fn refuses_an_encrypted_payload() {
        let mut header = FragmentHeader::new_v1(CHUNK_ID, 1);
        header.flags = FLAG_ENCRYPTED;
        let buf = encode(&header, b"x");
        assert_eq!(
            decode(&buf),
            Err(FragmentError::EncryptedPayloadUnsupported)
        );
    }

    #[test]
    fn rejects_a_reserved_flag() {
        let mut header = FragmentHeader::new_v1(CHUNK_ID, 1);
        header.flags = 0b0000_0010;
        let buf = encode(&header, b"x");
        assert_eq!(
            decode(&buf),
            Err(FragmentError::ReservedFlagSet(0b0000_0010))
        );
    }

    #[test]
    fn rejects_an_unsupported_checksum_algo() {
        let mut header = FragmentHeader::new_v1(CHUNK_ID, 1);
        header.checksum_algo = ChecksumAlgo::Blake3;
        let buf = encode(&header, b"x");
        assert_eq!(decode(&buf), Err(FragmentError::UnsupportedChecksumAlgo(1)));
    }

    #[test]
    fn rejects_an_unsupported_encryption_scheme() {
        let mut buf = valid_fragment(b"x");
        buf[offset::ENCRYPTION_SCHEME] = 5;
        restamp_header_checksum(&mut buf);
        assert_eq!(
            decode(&buf),
            Err(FragmentError::UnsupportedEncryptionScheme(5))
        );
    }

    #[test]
    fn rejects_a_reserved_ec_scheme() {
        let mut buf = valid_fragment(b"x");
        buf[offset::EC_SCHEME_TYPE] = 3;
        restamp_header_checksum(&mut buf);
        assert_eq!(decode(&buf), Err(FragmentError::ReservedEcScheme(3)));
    }

    #[test]
    fn rejects_a_corrupt_payload() {
        let mut buf = valid_fragment(b"payload");
        buf[CORE_HEADER_LEN as usize] ^= 0xff;
        assert_eq!(decode(&buf), Err(FragmentError::PayloadChecksumMismatch));
    }

    #[test]
    fn rejects_a_truncated_payload() {
        let mut buf = valid_fragment(b"0123456789");
        // Cut into the trailing payload checksum.
        buf.truncate(buf.len() - 2);
        assert_eq!(decode(&buf), Err(FragmentError::TruncatedPayload));
    }
}
