//! Derived payloads: `bytes = f(seed, key, size, generation)` (proposal 0017 §4).
//!
//! A run never stores an object body. Every byte it writes is a pure function of four inputs
//! the run records anyway — the `--seed` value, the object key, the object's size and the
//! write's generation number — so a checkpoint keeps those and a digest, and a resumed run
//! (or the oracle reading an object back) re-derives the bytes instead of keeping them.
//! Nothing here reads process state: no hasher seed, no address, no clock. The same inputs
//! give the same bytes in every process, on every host, in every release that keeps the
//! construction below.
//!
//! # The construction (version 1)
//!
//! Stated exactly, so it can be reimplemented. **Changing it silently invalidates every
//! checkpoint ever written**; the known-answer constants in
//! `crates/validate/tests/payload_and_sizes.rs` are what make such a change visible.
//!
//! All integers are unsigned 64-bit, big-endian. Let
//!
//! ```text
//! prefix  = "wyrd-validate/payload/v1" || 0x00
//!        || u64(len(seed)) || seed          (the --seed value's exact UTF-8 bytes)
//!        || u64(len(key))  || key           (the object key's exact UTF-8 bytes)
//!        || u64(size)
//!        || u64(generation)
//! block_i = SHA-256(prefix || u64(i))       for i = 0, 1, 2, ...
//! bytes   = the first `size` bytes of block_0 || block_1 || block_2 || ...
//! ```
//!
//! Each variable-length field carries its length, so no two input tuples share a `prefix`.
//! Block `i` depends on `i` alone, so the bytes of any range `[a, b)` cost
//! `⌈(b − a) / 32⌉ + 1` hashes at most, wherever the range sits in the object, and the state a
//! [`Payload`] holds (the SHA-256 state after `prefix`) does not grow with `size`.
//!
//! # What the bytes can and cannot tell apart
//!
//! Because `size` and `generation` are both in `prefix`, two payloads that differ in any input
//! are independent SHA-256 streams. Over `n` bytes they coincide with probability `2^(-8n)`:
//! negligible from a few bytes up, but **not** at the smallest sizes. At 0 bytes every input
//! gives the same (empty) payload, and a 1-byte payload takes only 256 values, so two
//! generations of one key coincide with probability 1/256. Below one block (32 bytes) the
//! guarantee is only that probability, not a certainty. At those sizes the bytes alone cannot
//! tell a stale generation from the current one, and neither can the `ETag`, which the gateway
//! derives from the body. The oracle (#746) must account for that: a read of a tiny object
//! that returns the right bytes is not, by itself, proof that it returned the right generation.

use std::fmt;

use sha2::{Digest, Sha256};

/// The construction version stated in the module docs, and the tag every `prefix` starts with.
const DOMAIN: &[u8] = b"wyrd-validate/payload/v1\0";

/// Bytes per derived block: one SHA-256 output.
pub const BLOCK_BYTES: u64 = 32;

/// The largest piece [`Pieces`] yields, whatever the payload's size (every piece but the last
/// is exactly this long). A caller streaming an object holds at most this much of it at a time.
pub const PIECE_BYTES: usize = 64 * 1024;

/// One object body, derived and never stored: what to send for `(seed, key, size, generation)`.
///
/// Holds the hash state after the `prefix` of the construction and the size — a fixed amount of
/// state whatever the size. Bytes are produced on demand, by [`Payload::fill`] for a range or by
/// [`Payload::pieces`] / [`Payload::range`] as a stream.
#[derive(Clone)]
pub struct Payload {
    keyed: Sha256,
    size: u64,
    generation: u64,
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload")
            .field("size", &self.size)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// A range that does not lie inside the payload. Refused, never truncated to fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRange {
    /// The requested start.
    pub start: u64,
    /// The requested end (exclusive); `None` when `start + len` overflows `u64`.
    pub end: Option<u64>,
    /// The payload's size.
    pub size: u64,
}

impl fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.end {
            Some(end) => write!(
                f,
                "byte range [{}, {end}) is not inside a {}-byte payload",
                self.start, self.size
            ),
            None => write!(
                f,
                "byte range starting at {} overflows a 64-bit offset (payload is {} bytes)",
                self.start, self.size
            ),
        }
    }
}

impl std::error::Error for OutOfRange {}

/// A `usize` length as a `u64`. Lossless on every target Rust supports, where `usize` is at
/// most 64 bits wide.
fn len_u64(len: usize) -> u64 {
    len as u64
}

impl Payload {
    /// The payload for `(seed, key, size, generation)`: `seed` is the `--seed` value exactly as
    /// given, `key` the object key, `generation` the write's number for that key.
    pub fn new(seed: &str, key: &str, size: u64, generation: u64) -> Self {
        let mut keyed = Sha256::new();
        keyed.update(DOMAIN);
        keyed.update(len_u64(seed.len()).to_be_bytes());
        keyed.update(seed.as_bytes());
        keyed.update(len_u64(key.len()).to_be_bytes());
        keyed.update(key.as_bytes());
        keyed.update(size.to_be_bytes());
        keyed.update(generation.to_be_bytes());
        Self {
            keyed,
            size,
            generation,
        }
    }

    /// The payload's length in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The generation the payload was derived for.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// `block_index` of the construction.
    fn block(&self, block_index: u64) -> [u8; 32] {
        let mut hasher = self.keyed.clone();
        hasher.update(block_index.to_be_bytes());
        hasher.finalize().into()
    }

    /// Fill `buf` with the payload's bytes `[offset, offset + buf.len())`, computed directly:
    /// only the blocks the range touches are hashed, wherever it sits. A range that does not
    /// lie inside the payload is refused and `buf` is left untouched.
    pub fn fill(&self, offset: u64, buf: &mut [u8]) -> Result<(), OutOfRange> {
        let end = offset.checked_add(len_u64(buf.len()));
        if end.is_none_or(|end| end > self.size) {
            return Err(OutOfRange {
                start: offset,
                end,
                size: self.size,
            });
        }
        self.fill_inside(offset, buf);
        Ok(())
    }

    /// [`Payload::fill`] for a range already known to lie inside the payload.
    fn fill_inside(&self, offset: u64, buf: &mut [u8]) {
        let mut position = offset;
        let mut written = 0;
        while written < buf.len() {
            let block = self.block(position / BLOCK_BYTES);
            // `position % BLOCK_BYTES` is below 32, so the cast is exact.
            let within = (position % BLOCK_BYTES) as usize;
            let take = (block.len() - within).min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&block[within..within + take]);
            written += take;
            position += len_u64(take);
        }
    }

    /// The bytes `[start, end)` as a stream of pieces of at most [`PIECE_BYTES`] each. The
    /// first piece is computed directly from `start`, never by generating the prefix before it.
    pub fn range(&self, start: u64, end: u64) -> Result<Pieces, OutOfRange> {
        if start > end || end > self.size {
            return Err(OutOfRange {
                start,
                end: Some(end),
                size: self.size,
            });
        }
        Ok(Pieces {
            payload: self.clone(),
            next: start,
            end,
        })
    }

    /// The whole payload as a stream of pieces of at most [`PIECE_BYTES`] each.
    pub fn pieces(&self) -> Pieces {
        Pieces {
            payload: self.clone(),
            next: 0,
            end: self.size,
        }
    }

    /// The SHA-256 of the whole payload, computed by streaming it piece by piece — the digest a
    /// checkpoint and the oracle key on. Costs one pass over `size` bytes; holds one piece.
    pub fn sha256(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        for piece in self.pieces() {
            digest.update(&piece);
        }
        digest.finalize().into()
    }
}

/// A payload range, streamed: the items concatenate to exactly the range. Every item but the
/// last is exactly [`PIECE_BYTES`] long, the last holds the rest (1 to [`PIECE_BYTES`] bytes),
/// and an empty range yields no item. No item is ever empty, so a range of `n` bytes yields
/// `⌈n / PIECE_BYTES⌉` items, the count [`Iterator::size_hint`] reports exactly.
#[derive(Debug, Clone)]
pub struct Pieces {
    payload: Payload,
    next: u64,
    end: u64,
}

impl Iterator for Pieces {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        if self.next >= self.end {
            return None;
        }
        // At most `PIECE_BYTES`, so the cast back to `usize` is exact.
        let len = (self.end - self.next).min(len_u64(PIECE_BYTES)) as usize;
        let mut piece = vec![0; len];
        self.payload.fill_inside(self.next, &mut piece);
        self.next += len_u64(len);
        Some(piece)
    }

    /// Exact: the items still to come. A 64-bit range has at most 2^48 of them, so the count
    /// fits a 64-bit `usize`; where it would not, the hint is `(usize::MAX, None)`.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let pieces = usize::try_from((self.end - self.next).div_ceil(len_u64(PIECE_BYTES))).ok();
        (pieces.unwrap_or(usize::MAX), pieces)
    }
}
