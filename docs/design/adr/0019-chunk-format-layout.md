# 0019. Chunk/fragment on-disk format layout

Date: design phase
Status: Accepted

## Context

The on-disk chunk/fragment format is the one spec-first, normative artifact in
the project (ADR-0002), because data outlives software: a provider with
petabytes written under format `v1` must read them with software written years
later. The draft spec (`specs/chunk-format/v1.md`) enumerated *what* the header
must let a reader determine but left the byte layout, field widths, endianness,
checksum algorithm, and EC-scheme encoding as `[TO BE SPECIFIED]`. Fixing them
is a prerequisite for the Milestone-0 `chunk-format` crate
(proposal 0001, PR #2). Several of the choices are trade-offs that needed to be
made deliberately rather than discovered in code, because the format is the one
contract that is expensive to change.

## Decision

The full layout is specified in `specs/chunk-format/v1.md`. The decisions that
carried a genuine trade-off, and their reasoning:

1. **Little-endian, fixed-width, generously sized.** All multi-byte integers are
   little-endian (the native order of every target platform; avoids byte-swaps
   on the hot path). The header is fixed-layout and naturally aligned. Fields
   that count or identify are sized generously (the chunk id is 128-bit, lengths
   are 64-bit) because the header appears once per fragment, so its size is
   negligible against the payload, while a too-narrow field is a format break.

2. **`chunk_id` is u128.** 128 bits allows chunk identifiers to be generated
   **without central coordination** (random/UUID-style), which suits the
   direct-write data path (the client mints ids without a round-trip to a
   central allocator) and keeps the door open for the cross-provider-federation
   reversal contemplated by ADR-0005. The extra 8 bytes over a u64 are noise
   against payload size.

3. **crc32c is the default payload checksum; blake3 is reserved.** A
   `checksum_algo` byte selects the algorithm. crc32c is the v1 default: it is
   hardware-accelerated, it is what the scrubber runs constantly, and integrity
   (not cryptographic authenticity) is all the closed-federation trust model
   (ADR-0005) requires. blake3 is reserved (code point 1) for future
   content-addressing or cryptographic integrity without a format break.

4. **A self-describing `header_length` is the evolution mechanism.** The header
   carries its own total length, with a fixed core followed by an optional
   extension region. A reader locates the payload from `header_length` and skips
   any extension it does not understand rather than failing. This converts
   "every new header field is a breaking change" into "new fields go in the
   extension; old readers skip them," which is what lets the format evolve
   (encryption, compression, federation metadata) without version churn. It is
   the single most important structural decision in the format.

5. **A separate header checksum.** The header is guarded by its own crc32c,
   distinct from the payload checksum, because a silently-corrupted header field
   (a wrong length or chunk id) is more dangerous than a corrupted payload. A
   reader verifies the header checksum before acting on any other field.

6. **The payload checksum is computed over the *stored* bytes.** When a payload
   is encrypted, the checksum covers the ciphertext, not the plaintext. This is
   mandatory so that integrity is verifiable **without the decryption key**: a
   D server or scrubber holding only ciphertext (the envelope-encryption trust
   model, §8.5) must detect bit-rot without decrypting. Plaintext authenticity
   under encryption is the AEAD scheme's job, separate from this storage
   checksum. This rule is fixed now, even though encryption ships later, because
   getting the checksum order wrong would be a format-and-protocol break.

7. **Client-side encryption is reserved, not built.** A `flags` bit, an
   `encryption_scheme` byte, and the header extension region (for nonce and key
   identifier) are reserved so envelope encryption (ADR-0005 §8.5) can be added
   without a breaking change. A v1 writer writes zeros.

8. **The EC scheme is recorded per fragment and per chunk.** `ec_scheme_type`,
   `ec_k`, `ec_m`, and `ec_fragment_index` are in the header (so a fragment
   validates standalone, which the scrubber needs) and MUST agree with the
   chunk's authoritative metadata (so the system knows a chunk's scheme without
   reading fragments). This realizes the per-chunk-scheme requirement of
   ADR-0008, letting a zone carry mixed-era data as it grows from replication
   into EC.

The format remains **v0/unstable** until validated by a second independent
reader implementation or a sustained fault-injection run, then is stamped `v1`.

## Consequences

- M0's `chunk-format` crate has a complete, unambiguous layout to implement
  against, and seed conformance vectors can be produced.
- The format can absorb encryption, compression, content-addressing, and
  federation metadata without a version increment, via reserved code points and
  the header extension region — backward-compatible additions need new
  conformance vectors but not a breaking change.
- Integrity remains verifiable by a key-less D server or scrubber even once
  encryption exists, preserving the trust-minimized-storage property.
- The 128-bit chunk id permits coordination-free id generation, at 8 bytes per
  fragment over a u64 — accepted as negligible.
- A breaking change (altering an existing core field's meaning or layout) still
  requires a `format_version` increment and the strict spec change process; the
  extension mechanism deliberately makes such breaks rare, not impossible.
