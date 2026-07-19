//! The `x-amz-checksum-*` streaming-trailer checksums (issue #505): the `aws-chunked`
//! `-TRAILER` framings a stock modern SDK (boto3 / aws-sdk / aws-cli) **defaults** to
//! carry a declared checksum trailer after the terminating zero-length chunk, and the
//! gateway must actually validate it against the streamed content — never
//! consume-and-ignore it (that would be the same half-accept `sigv4.rs` refuses today, in
//! new clothes).
//!
//! # Algorithm set (brief #505 scope)
//! At minimum `crc32`, `crc32c`, and `sha256` — what a stock SDK actually sends:
//! * `crc32c` is the workspace's already-vetted `crc32c` crate (root `Cargo.toml`, already
//!   used by `wyrd-chunk-format`) — no new dependency, no ADR-0003 audit.
//! * `sha256` reuses [`crate::crypto::Sha256`], already vetted for the SigV4 boundary.
//! * plain `crc32` (IEEE 802.3, what default aws-cli/boto3 send) is a **different**
//!   polynomial the `crc32c` crate does not compute, and is small enough (a 256-entry
//!   table + an XOR loop) to keep **in-tree** rather than pull a new crate — a new
//!   dependency here would trigger ADR-0003's three-test audit + `deny.toml` allowlist,
//!   a human-only sign-off item (INTEGRATION §4), for a few dozen lines of well-known,
//!   independently-checkable arithmetic.
//!
//! `x-amz-checksum-crc64nvme` is out of scope for this slice (brief #505): a `-TRAILER`
//! declaration naming it is refused up front by [`ChecksumAlgorithm::from_trailer_name`]
//! returning `None`, before any body is read — the same "refuse, never half-accept" rule
//! as an unrecognised streaming sentinel.

use crate::crypto;

/// A checksum algorithm the gateway can compute and verify against a declared
/// `x-amz-checksum-<algo>` streaming trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (IEEE 802.3) — the aws-cli / boto3 **default**.
    Crc32,
    /// CRC-32C (Castagnoli).
    Crc32c,
    /// SHA-256.
    Sha256,
}

impl ChecksumAlgorithm {
    /// Recognise a declared `x-amz-trailer` value as one of the algorithms this gateway
    /// can verify, case-insensitively. `None` for anything else — including
    /// `x-amz-checksum-crc64nvme` (out of scope) and any non-checksum trailer name — so
    /// the caller refuses rather than half-accepts a trailer it cannot validate.
    pub fn from_trailer_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "x-amz-checksum-crc32" => Some(ChecksumAlgorithm::Crc32),
            "x-amz-checksum-crc32c" => Some(ChecksumAlgorithm::Crc32c),
            "x-amz-checksum-sha256" => Some(ChecksumAlgorithm::Sha256),
            _ => None,
        }
    }
}

/// An incremental checksum over the DECODED (de-framed) object bytes as they stream past —
/// so the declared trailer value is checked against what was **actually** written, never
/// the client's unverified claim (issue #505's invariant: "consumed but not validated
/// would be the same half-accept in new clothes").
pub enum RunningChecksum {
    Crc32(u32),
    Crc32c(u32),
    Sha256(crypto::Sha256),
}

impl RunningChecksum {
    /// A fresh accumulator over the empty input for `algorithm`.
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Crc32 => RunningChecksum::Crc32(0),
            ChecksumAlgorithm::Crc32c => RunningChecksum::Crc32c(0),
            ChecksumAlgorithm::Sha256 => RunningChecksum::Sha256(crypto::Sha256::new()),
        }
    }

    /// Feed `data` into the running digest.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            RunningChecksum::Crc32(crc) => *crc = crc32_ieee_append(*crc, data),
            RunningChecksum::Crc32c(crc) => *crc = crc32c::crc32c_append(*crc, data),
            RunningChecksum::Sha256(hasher) => hasher.update(data),
        }
    }

    /// The final checksum, as the raw bytes AWS's `x-amz-checksum-*` trailer base64-encodes
    /// (a big-endian 4-byte CRC for `crc32`/`crc32c`, the 32-byte digest for `sha256`).
    pub fn finalize(self) -> Vec<u8> {
        match self {
            RunningChecksum::Crc32(crc) => crc.to_be_bytes().to_vec(),
            RunningChecksum::Crc32c(crc) => crc.to_be_bytes().to_vec(),
            RunningChecksum::Sha256(hasher) => hasher.finalize().to_vec(),
        }
    }
}

/// The standard CRC-32 (IEEE 802.3, reflected polynomial `0xEDB88320`) lookup table —
/// generated once, ahead of time, rather than at runtime (no `OnceLock`/`lazy_static`
/// needed for a fixed 256-entry table). Reproduces the standard `crc32` check value for
/// `"123456789"` (`0xCBF43926`, pinned in `tests::crc32_ieee_check_value`).
#[rustfmt::skip]
const CRC32_IEEE_TABLE: [u32; 256] = [
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
    0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
    0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
    0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
    0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
    0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
    0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
    0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
    0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
    0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
    0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
    0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
    0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
    0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
    0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
    0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
    0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
    0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
    0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
    0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
    0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
    0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
];

/// CRC-32 (IEEE 802.3) of `data`, chained from a previous `crc` (`0` for a fresh start) —
/// the table-driven counterpart to [`crc32c::crc32c_append`], kept in-tree (see module docs).
fn crc32_ieee_append(crc: u32, data: &[u8]) -> u32 {
    let mut crc = !crc;
    for &byte in data {
        crc = CRC32_IEEE_TABLE[((crc ^ byte as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Encode raw bytes as **standard** (`+`/`/`, `=`-padded) base64 — the inverse of
/// [`base64_decode`]. Used to make an **opaque** ListObjectsV2 continuation token out of a
/// listing's resume key (issue #507): the wire echoes it as `<NextContinuationToken>` and the
/// client hands it back verbatim, and [`base64_decode`] recovers the key (an undecodable token
/// is a `400 InvalidArgument`, never a silent restart). Empty input encodes to `""`; a resume
/// key is always a non-empty object key, so a real token is never empty.
pub fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[(n >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Decode a **standard** (`+`/`/`, `=`-padded) base64 string to raw bytes — the encoding
/// AWS uses for every `x-amz-checksum-*` trailer value. `None` on anything malformed
/// (wrong alphabet, wrong length, bad padding), so the caller refuses a declared checksum
/// that is not even well-formed base64 rather than half-parsing it.
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return None;
    }
    let value_of = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let group_count = bytes.len() / 4;
    for (gi, group) in bytes.chunks_exact(4).enumerate() {
        // Padding (`=`) is only valid in the last one or two positions of the FINAL group;
        // treat it as "value 0, drop from output" and reject any other placement/count.
        // Padding anywhere but the final quartet (e.g. `Zg==Zg==`) is non-canonical and
        // must be refused — otherwise a decodable-but-illegal string slips past.
        let pad = group.iter().rev().take_while(|&&b| b == b'=').count();
        let is_last = gi + 1 == group_count;
        if pad > 2 || (pad > 0 && !is_last) || group[..4 - pad].contains(&b'=') {
            return None;
        }
        let mut vals = [0u8; 4];
        for (i, &b) in group.iter().enumerate() {
            vals[i] = if b == b'=' { 0 } else { value_of(b)? };
        }
        // Reject non-canonical encodings: the low bits that padding drops MUST be zero.
        // Without this, two distinct strings decode to the same bytes (e.g. `6Le+Qx==`
        // and `6Le+Qw==` both -> the CRC-32 of "a"), which would let a tampered/forged
        // declared checksum masquerade as a valid one — a silent accept, the very
        // half-accept the fail-closed trailer contract forbids.
        if (pad == 2 && vals[1] & 0x0f != 0) || (pad == 1 && vals[2] & 0x03 != 0) {
            return None;
        }
        let n = (u32::from(vals[0]) << 18)
            | (u32::from(vals[1]) << 12)
            | (u32::from(vals[2]) << 6)
            | u32::from(vals[3]);
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard CRC-32 (IEEE 802.3) check value for the canonical `"123456789"`
    /// vector — the independent oracle every from-scratch CRC-32 implementation is
    /// checked against.
    #[test]
    fn crc32_ieee_check_value() {
        assert_eq!(crc32_ieee_append(0, b"123456789"), 0xcbf4_3926);
    }

    // Cross-checked independently against a bit-by-bit reference implementation and
    // Python's `zlib.crc32` (both agree): `crc32("a") == 0xe8b7be43`, whose big-endian
    // bytes base64-encode to `6Le+Qw==` — the shape a real `x-amz-checksum-crc32` trailer
    // value takes.
    #[test]
    fn crc32_ieee_matches_a_second_known_answer() {
        assert_eq!(crc32_ieee_append(0, b"a"), 0xe8b7_be43);
    }

    /// The running accumulator agrees with a one-shot call, for arbitrary split points —
    /// the streaming trailer-checksum path feeds it whatever piece sizes the wire delivers.
    #[test]
    fn crc32_running_matches_split_points() {
        let data: Vec<u8> = (0..500u32).map(|i| (i * 7 + 3) as u8).collect();
        let whole = crc32_ieee_append(0, &data);
        for split in [0usize, 1, 63, 250, 499, 500] {
            let a = crc32_ieee_append(0, &data[..split]);
            let b = crc32_ieee_append(a, &data[split..]);
            assert_eq!(b, whole, "split at {split}");
        }
    }

    /// RFC 4648 §10 test vectors — the published base64 known answers.
    #[test]
    fn base64_decode_rfc4648_vectors() {
        assert_eq!(base64_decode("").as_deref(), None); // empty is not a valid group
        assert_eq!(base64_decode("Zg==").unwrap(), b"f");
        assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
        assert_eq!(base64_decode("Zm9v").unwrap(), b"foo");
        assert_eq!(base64_decode("Zm9vYg==").unwrap(), b"foob");
        assert_eq!(base64_decode("Zm9vYmE=").unwrap(), b"fooba");
        assert_eq!(base64_decode("Zm9vYmFy").unwrap(), b"foobar");
    }

    /// RFC 4648 §10 encode vectors — the inverse of [`base64_decode`], and encode→decode is
    /// the identity, so an opaque ListObjectsV2 continuation token round-trips (issue #507).
    #[test]
    fn base64_encode_rfc4648_vectors_round_trip() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // A realistic resume key (contains `/`, non-ASCII) survives encode → decode intact.
        for key in ["photos/2024/x.jpg", "α/β", "key-06", "a&b<c>\"d"] {
            let token = base64_encode(key.as_bytes());
            assert_eq!(base64_decode(&token).unwrap(), key.as_bytes());
        }
    }

    /// The real-world shape: a base64-encoded CRC-32C trailer value, matching the AWS
    /// published `aws-c-auth` trailing-header integration example (`x-amz-checksum-crc32c:
    /// wdBDMA==` for the 1-byte payload `"a"`) — decodes to the CRC-32C big-endian bytes.
    #[test]
    fn base64_decode_matches_aws_crc32c_trailer_example() {
        let decoded = base64_decode("wdBDMA==").unwrap();
        assert_eq!(decoded, crc32c::crc32c(b"a").to_be_bytes());
    }

    #[test]
    fn base64_decode_rejects_malformed_input() {
        assert!(base64_decode("not base64!!").is_none());
        assert!(base64_decode("Zg=").is_none()); // wrong length
        assert!(base64_decode("=g==").is_none()); // padding before data
    }

    /// Canonical base64: the low bits a `=`-pad drops MUST be zero, and padding may only
    /// appear in the final quartet. A decoder that ignores either rule lets two distinct
    /// strings map to the same bytes, so a tampered/forged declared checksum can slip
    /// past as a silent accept (issue #505 fail-closed contract).
    #[test]
    fn base64_decode_rejects_non_canonical_encodings() {
        // `6Le+Qw==` is the canonical CRC-32-of-"a" trailer value; `6Le+Qx==` differs
        // only in the two dropped pad bits and must NOT decode to the same bytes.
        let canonical = base64_decode("6Le+Qw==").unwrap();
        assert_eq!(canonical, 0xe8b7_be43u32.to_be_bytes());
        assert!(
            base64_decode("6Le+Qx==").is_none(),
            "non-zero pad bits (two-pad quartet) must be rejected"
        );
        // One-pad quartet: `Zm8=` is canonical for "fo"; `Zm9=` sets a dropped pad bit.
        assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
        assert!(
            base64_decode("Zm9=").is_none(),
            "non-zero pad bits (one-pad quartet) must be rejected"
        );
        // Padding is only legal in the final quartet.
        assert!(
            base64_decode("Zg==Zg==").is_none(),
            "padding outside the final quartet must be rejected"
        );
    }
}
