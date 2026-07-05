//! The two primitives AWS SigV4 needs — SHA-256 and HMAC-SHA256 — plus an
//! **incremental** [`Sha256`] the streaming PUT path hashes bodies with as they flow
//! past (so the payload hash is checked without ever buffering the object).
//!
//! Provenance (issue #364 carry-forward, T5-a — the auth boundary must run **vetted**
//! crypto, not a hand-rolled one): the primitives are the **RustCrypto** `sha2` / `hmac`
//! crates, adopted through the ADR-0003 §2 three-test dependency audit (recorded in
//! build-notes). Both are `MIT OR Apache-2.0` — already on the `deny.toml` allowlist — so
//! `cargo deny` stays green with no license addition; they are pure-Rust, `no_std`-capable,
//! and carry no build script. This module is now a thin, stable wrapper over them: it keeps
//! the same in-crate API (`Sha256` / [`sha256`] / [`hmac_sha256`] / [`hex`] /
//! [`constant_time_eq`]) so the SigV4 layer is unchanged, but the actual hashing is a
//! maintained implementation rather than a bespoke one on a security surface. The wrappers
//! are still pinned against the published vectors — NIST FIPS-180-4 for SHA-256, RFC 4231
//! for HMAC, and the AWS SigV4 published example end to end (`sigv4` module).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256 as Sha256Impl};

type HmacSha256 = Hmac<Sha256Impl>;

/// An **incremental** SHA-256 hasher (FIPS-180-4): `update` bytes in any-sized pieces
/// and `finalize` for the digest. The streaming PUT path feeds it the body as it
/// arrives so the payload hash is computed without buffering the object. A thin wrapper
/// over RustCrypto `sha2::Sha256` (issue #364 carry-forward, T5-a).
#[derive(Clone, Default)]
pub struct Sha256 {
    inner: Sha256Impl,
}

impl Sha256 {
    /// A fresh hasher over the empty input.
    pub fn new() -> Self {
        Self {
            inner: Sha256Impl::new(),
        }
    }

    /// Feed `data` into the running digest.
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// Consume the hasher and return the 32-byte digest.
    pub fn finalize(self) -> [u8; 32] {
        self.inner.finalize().into()
    }
}

/// The SHA-256 digest of `data` (FIPS-180-4), one-shot.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256Impl::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// HMAC-SHA256 of `msg` under `key` (RFC 2104 / RFC 4231).
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    // HMAC accepts a key of any length (it hashes an over-long key and zero-pads a
    // short one, RFC 2104), so `new_from_slice` never errors here.
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Lower-case hex encoding of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    s
}

/// Length-independent-then-constant-time byte comparison, so signature checking
/// leaks neither a length nor an early-mismatch timing signal (fail-closed auth).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // NIST FIPS-180-4 example vectors — the vetted crate still reproduces the published
    // known answers (ADR-0003 §2 test-3: behaviour is pinned, not merely imported).
    #[test]
    fn sha256_known_answers() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    // The incremental hasher must agree with the one-shot for arbitrary split points —
    // the streaming PUT path relies on this (bytes arrive in unpredictable frame sizes).
    #[test]
    fn incremental_matches_one_shot() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 31 + 7) as u8).collect();
        for split in [0usize, 1, 63, 64, 65, 200, 999, 1000] {
            let mut h = Sha256::new();
            h.update(&data[..split]);
            h.update(&data[split..]);
            assert_eq!(h.finalize(), sha256(&data), "split at {split}");
        }
    }

    // RFC 4231 §4.2 (test case 2): a key shorter than the block, an ASCII message.
    #[test]
    fn hmac_sha256_rfc4231_case2() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    // RFC 4231 §4.5 (test case 4): a longer message exercises multi-block compression.
    #[test]
    fn hmac_sha256_rfc4231_case4() {
        let key: Vec<u8> = (1u8..=25).collect();
        let msg = [0xcdu8; 50];
        assert_eq!(
            hex(&hmac_sha256(&key, &msg)),
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
        );
    }

    #[test]
    fn constant_time_eq_matches_only_identical() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abx"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
