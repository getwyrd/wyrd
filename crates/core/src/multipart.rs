//! The **multipart commit protocol**'s key space and the validated identity types it is
//! spelled in (proposal 0016,
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md`).
//!
//! This is slices 1–2 of 3 of issue #654's own re-split (itself slice 1 of 7 of #636). It is
//! deliberately **pure**: no [`wyrd_traits::MetadataStore`] call, no `WriteBatch`, no
//! `async fn`. After this module a reader can name every key the protocol will ever write
//! (`0016` §1, `:333-527`), parse it back, **and decode the one record value admission turns
//! on** — the `mpuctl` singleton ([`AdmissionRecord`], its [`Budget`] profile and the two
//! derivations that profile establishes, `0016:348`, `:1469-1470`). The remaining record
//! values (`mpu:`'s session shape, `part:`'s chunk list) are the next children's; the outcome
//! enums, the answer table and `multipart_etag` are the child after that's (`0016` decision 3,
//! `:894-1037`, `0016:3064-3070`).
//!
//! There is **no** `encode_record`/`decode_record` envelope, and this header's earlier forward
//! reference to one is withdrawn: `0016` §1 gives every value a **key-determined** shape
//! (`:333-356`) and a stored value carries no type tag, so a per-record arm would have nothing
//! to dispatch on. Each record type instead validates inside its own `Deserialize` over the
//! store-wide codec [`crate::metadata::encode`] / [`crate::metadata::decode`] — the shape
//! [`AdmissionRecord`] lands here and every later child repeats.
//!
//! # The keyed classes (`0016` §1, `:333-527`)
//!
//! | Key | What it addresses |
//! |---|---|
//! | `mpuctl` | the fleet **admission ledger** singleton — no id, no parser |
//! | `mpu:<id>` | one **session** |
//! | `slot:<id>:<k>` | one **in-flight part slot**; the key space *is* the per-session cap |
//! | `part:<id>:<n>` | a **committed part** |
//! | `psum:<id>:<n>` | that part's **summary** |
//! | `sidx:<id>:<n>:<chunk>` | one **owned staging entry**, under a prefix disjoint from `pending:` (`0016:475-491`) |
//! | `retire:bytes:<token>` | a **retirement obligation**: orphan-mark bytes, then delete the naming records |
//! | `retire:records:<token>` | records to delete whose bytes something else protects |
//!
//! Every prefix here is disjoint from every other and from the pre-existing `inode:` /
//! `dirent:` / `pending:` / `bucket:` / `orphan:` (`metadata.rs:30-70`), `seg:` / `seggrp:`
//! (`metadata.rs:293-300`) and `desired:dserver:` (`custodian/src/desired_state.rs:33`),
//! and none is a prefix of another, so no `scan` returns a neighbour's records
//! (`0016:333-356`; ADR-0046 decision 1, disjoint first-class records under a disjoint
//! prefix — never an encoding smuggled into an existing namespace).
//!
//! # Structural validity is a type, not a convention (ADR-0045)
//!
//! Every component a key is built from arrives here as a **validated type** —
//! [`UploadId`], [`AttemptId`], [`PartNumber`], [`SlotIndex`], [`Digest`] — mirroring
//! [`crate::metadata::SegmentNonce`], whose doc records why (`metadata.rs:714-733`):
//! parsing the rule into the type is what makes an unvalidated value *unrepresentable*
//! rather than merely unlikely (ADR-0045, parse-don't-validate). Two consequences this
//! module leans on:
//!
//! * every key constructor is **total and infallible** — it cannot mint a key its own
//!   parser would reject, the property `seg_key` states at `metadata.rs:1219-1233`;
//! * `parse(key(x)) == x` for every keyed class, and a **non-canonical** spelling of the
//!   same record (`slot:<id>:7`, `slot:<id>:0000007`, `slot:<id>:+7`) parses as **no**
//!   record at all. Two spellings of one record would defeat a `require_absent` guard and
//!   hide a record from a bounded range scan — residue nothing enumerates and therefore
//!   nothing reclaims (**C-1**, `docs/principles.md` §5 C-1, the form
//!   `metadata.rs:724` cites it in).
//!
//! Structural validity is checked **at decode**, never by convention at a call site
//! (`0016:390-414`).
//!
//! # Nothing here is written yet — where the living-architecture update belongs
//!
//! This module is the key **grammar** plus the admission ledger's record **shape**: it has no
//! writer, no store call and no production consumer (the first writers are the store round
//! trips, #656–#659). The living
//! architecture doc describes the system **as it is** (`docs/design/README.md:28`), and its
//! metadata model (`docs/design/architecture/05-building-block-view.md:183-195`) therefore
//! gains these namespaces with the slice that first *persists* one — documenting records no
//! code emits would make the living doc describe a system that does not exist. Until then the
//! normative description of this key space is proposal 0016 §1 (`0016:333-356`), already
//! merged on `main`.

use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use wyrd_traits::{ChunkId, SCAN_CAP};

use crate::metadata::{self, InodeId};

// ===========================================================================
// 1. Errors — every structural violation is a typed error, never a value
//    (ADR-0045; `0016:390-414`)
// ===========================================================================

/// A structural violation of a multipart **key**, or of a stored multipart **record value**
/// — never a silently-corrected default (ADR-0045, parse-don't-validate; `0016:390-414`).
///
/// Every value-level variant names **one** rule, so a consumer can tell which rule a stored
/// record broke without parsing a message; that is what lets [`decode_admission_record`]
/// attribute a torn `mpuctl` to the exact relation it violates rather than to "undecodable".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// A key whose prefix, field count or field syntax does not parse — including a key
    /// whose bytes are not UTF-8, a truncated key and one with a trailing component.
    MalformedKey {
        /// The namespace expected.
        namespace: &'static str,
        /// The key as UTF-8 (lossy), for the operator signal.
        key: String,
    },
    /// A token component (upload id, attempt id) that is not exactly [`TOKEN_HEX_LEN`]
    /// lowercase-hex characters.
    TokenNotHex {
        /// The rejected token.
        token: String,
    },
    /// A part number outside `[1, MAX_PART_NUMBER]` — the **format** bound the fixed-width
    /// key grammar can spell, never a live capacity knob.
    PartNumberOutOfRange {
        /// The number found.
        part_number: u64,
    },
    /// A slot index outside `[0, MAX_SLOT_INDEX]` — the **format** bound.
    SlotIndexOutOfRange {
        /// The index found.
        index: u64,
    },
    /// A `retire:` key whose mode component is neither `bytes` nor `records`
    /// (`0016:434-440`).
    UnknownRetireMode {
        /// The mode as read.
        mode: String,
    },
    /// A digest that is not 64 **lowercase**-hex characters (a SHA-256, ADR-0047).
    DigestNotHex {
        /// The rejected text.
        digest: String,
    },
    /// A record **value** whose bytes are not a well-formed record of its class at all: not
    /// JSON, the wrong shape, a field missing or of the wrong type or outside its wire type's
    /// range, or — under `deny_unknown_fields` — a field this build does not know.
    MalformedRecordValue {
        /// The record class expected (`mpuctl`).
        namespace: &'static str,
        /// The decoder's own message, for the operator signal.
        detail: String,
    },
    /// **G1** — a profile whose `max_part_chunks` is `0` (`0016:1466`, `> 0`). The
    /// **totality precondition** of the whole record rather than a peer of the rules below:
    /// at zero the `U_ref` of `0016:1469` is `0`, so the `MAX_SESSIONS` quotient of
    /// `0016:1470` has no divisor and the ledger's identity is undefined, not merely wrong.
    MaxPartChunksZero,
    /// **G2** — a profile whose `max_inflight_parts` is `0` (`0016:1471`, range `[1, …]`): no
    /// slot can ever be reserved, so no part can ever be committed and no session can ever
    /// progress.
    MaxInflightPartsZero,
    /// **G3** — a profile promising more parts per session than the `part:`/`psum:`/`sidx:`
    /// key grammar can address ([`MAX_PART_NUMBER`]): the session's later parts would name
    /// records no parser could read. A **format** bound of the encoding, never a live knob —
    /// `0016`'s knob table states no operator range for `MAX_PARTS_PER_SESSION` at all.
    PartsPerSessionUnaddressable {
        /// The cap found.
        max_parts_per_session: u32,
    },
    /// **G4** — a profile with more parts in flight than the session may ever hold
    /// (`0016:1471` clamp 1).
    InflightPartsExceedParts {
        /// The in-flight cap found.
        max_inflight_parts: u32,
        /// The per-session part cap it exceeds.
        max_parts_per_session: u32,
    },
    /// **G5** — a profile whose worst-case owned `sidx:` population per session,
    /// `max_inflight_parts × max_part_chunks`, is past `SCAN_CAP/2` (`0016:1471`, `:2098`):
    /// the per-session `scan("sidx:<id>:")` every teardown depends on would fail
    /// complete-or-fail-loud, stranding that session's residue with no pass that enumerates
    /// it.
    StagingRangeUnscannable {
        /// The owned population that profile can reach, exact.
        owned_sidx: u128,
    },
    /// **G6** — a profile whose staged-chunk ceiling is below one maximal part (`0016:1468`,
    /// the lower end of the settled range): at least one maximal part must remain stageable,
    /// or the ceiling refuses the very first part every session must be able to commit.
    StagedChunksBelowPart {
        /// The staged ceiling found.
        max_staged_chunks: u32,
        /// The per-part chunk cap it is below.
        max_part_chunks: u32,
    },
    /// **G7** — a profile whose reference budget `W_ref` is below one session's own
    /// worst-case footprint `U_ref` (`0016:1473`, the range `[U_ref, deployment RAM]`): the
    /// derivation yields a ledger that can never admit a session.
    BudgetBelowFootprint {
        /// The budget found.
        w_ref: u64,
        /// The per-session footprint it is below (`0016:1469`), exact — this is the one
        /// quantity a torn record can drive past `u64`, so it is reported at the width it was
        /// computed at rather than saturated into the field's.
        u_ref: u128,
    },
    /// **G8** — a `mpuctl` whose stored `max_sessions` is not what its **own** profile
    /// derives (`0016:1470`). The identity this record exists for: `max_sessions` is derived,
    /// never chosen, and a stored limit disagreeing with the profile beside it admits
    /// sessions past the memory bound the reconcile pass is sized for (`0016:2593`, X64).
    MaxSessionsNotDerived {
        /// The limit the record carries.
        stored: u64,
        /// What its own profile derives.
        derived: u64,
    },
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedKey { namespace, key } => {
                write!(f, "malformed {namespace} key {key:?}")
            }
            Self::TokenNotHex { token } => write!(
                f,
                "{token:?} is not {TOKEN_HEX_LEN} lowercase-hex characters (a 128-bit token)"
            ),
            Self::PartNumberOutOfRange { part_number } => write!(
                f,
                "part number {part_number} is outside the format bound [1, {MAX_PART_NUMBER}]"
            ),
            Self::SlotIndexOutOfRange { index } => write!(
                f,
                "slot index {index} is outside the format bound [0, {MAX_SLOT_INDEX}]"
            ),
            Self::UnknownRetireMode { mode } => write!(
                f,
                "retire: key mode {mode:?} is neither `bytes` nor `records`"
            ),
            Self::DigestNotHex { digest } => write!(
                f,
                "{digest:?} is not 64 lowercase-hex characters (a SHA-256 digest)"
            ),
            Self::MalformedRecordValue { namespace, detail } => {
                write!(f, "malformed {namespace} record value: {detail}")
            }
            Self::MaxPartChunksZero => write!(f, "`max_part_chunks` is zero, so U_ref is zero"),
            Self::MaxInflightPartsZero => {
                write!(f, "`max_inflight_parts` is zero: no slot can be reserved")
            }
            Self::PartsPerSessionUnaddressable {
                max_parts_per_session,
            } => write!(
                f,
                "`max_parts_per_session` {max_parts_per_session} is past the format bound \
                 {MAX_PART_NUMBER} the part: key grammar can address"
            ),
            Self::InflightPartsExceedParts {
                max_inflight_parts,
                max_parts_per_session,
            } => write!(
                f,
                "`max_inflight_parts` {max_inflight_parts} exceeds `max_parts_per_session` \
                 {max_parts_per_session}"
            ),
            Self::StagingRangeUnscannable { owned_sidx } => write!(
                f,
                "owned sidx: entries per session {owned_sidx} is past SCAN_CAP/2 ({SCAN_HALF})"
            ),
            Self::StagedChunksBelowPart {
                max_staged_chunks,
                max_part_chunks,
            } => write!(
                f,
                "`max_staged_chunks` {max_staged_chunks} is below one maximal part's \
                 `max_part_chunks` {max_part_chunks}"
            ),
            Self::BudgetBelowFootprint { w_ref, u_ref } => write!(
                f,
                "`w_ref` {w_ref} is below one session's worst-case footprint U_ref {u_ref}"
            ),
            Self::MaxSessionsNotDerived { stored, derived } => write!(
                f,
                "stored `max_sessions` {stored} is not the {derived} its own profile derives"
            ),
        }
    }
}

impl std::error::Error for RecordError {}

fn malformed_key(namespace: &'static str, key: &[u8]) -> RecordError {
    RecordError::MalformedKey {
        namespace,
        key: String::from_utf8_lossy(key).into_owned(),
    }
}

// ===========================================================================
// 2. The validated components every key is built from
// ===========================================================================

/// A 128-bit token's canonical textual length: 32 lowercase-hex characters
/// (`0016:493-497`). Taken from [`crate::metadata::SEG_NONCE_HEX_LEN`]
/// (`metadata.rs:289`) rather than restated, since an upload id, an attempt id and a
/// segment-group nonce are the same 128-bit-token shape minted the same way.
pub const TOKEN_HEX_LEN: usize = metadata::SEG_NONCE_HEX_LEN;

/// Whether `token` is a well-formed 128-bit token: exactly [`TOKEN_HEX_LEN`] lowercase-hex
/// characters (`0016:493-497`). Rejects an empty string, a short or long token, uppercase
/// hex, and — because `:` is never a hex digit — a token that embeds the key separator.
pub fn is_token(token: &str) -> bool {
    token.len() == TOKEN_HEX_LEN
        && token
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn require_token(token: String) -> Result<String, RecordError> {
    if is_token(&token) {
        Ok(token)
    } else {
        Err(RecordError::TokenNotHex { token })
    }
}

/// An **upload id**: the 128-bit token a gateway mints for one session, rendered as 32
/// lowercase hex characters (`0016:493-497`).
///
/// A validated type rather than a `String` for the reason
/// [`crate::metadata::SegmentNonce`] is (`metadata.rs:714-733`): it is the component every
/// per-session **range** is derived from (`slot:<id>:`, `part:<id>:`, `sidx:<id>:`), so an
/// id carrying the key separator would name another session's live range, and an empty one
/// would name every session's.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct UploadId(String);

impl UploadId {
    /// The validating constructor — the only way to obtain one.
    pub fn new(id: impl Into<String>) -> Result<Self, RecordError> {
        require_token(id.into()).map(Self)
    }

    /// The id as its 32 hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UploadId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// An **attempt id**: the 128-bit token one `UploadPart` attempt stamps into the `slot:`
/// record it reserved, so an ambiguous reserve is settled by re-reading rather than by
/// re-reserving a different index (`0016:349`). Same grammar as [`UploadId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AttemptId(String);

impl AttemptId {
    /// The validating constructor — the only way to obtain one.
    pub fn new(id: impl Into<String>) -> Result<Self, RecordError> {
        require_token(id.into()).map(Self)
    }

    /// The id as its 32 hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AttemptId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The decimal width a part number is zero-padded to in `part:` / `psum:` / `sidx:` keys, so
/// the key's byte-lexicographic order equals part-number order — the property
/// `metadata.rs:270-273` states for `SEG_INDEX_WIDTH`, mirrored for this slice's key space.
///
/// **Six digits, deliberately, pinned at Plan.** The gateway seam is protocol-neutral
/// (ADR-0046 decision 6, `docs/design/adr/0046-bucket-model-real-namespace.md:54-57`:
/// `crates/gateway-core` admits no S3 vocabulary), so the *format* must clear every known
/// front door's ceiling with margin, not just S3's: S3 caps a multipart upload at 10,000
/// parts per its wire protocol, Azure block blobs at 50,000 committed / 100,000 staged
/// blocks. Five digits (S3's own headroom) does not clear Azure's; six does, with room to
/// spare. Each *protocol's* cap is enforced at admission as **capacity**, never here —
/// mirroring `SEG_INDEX_WIDTH` vs `MAX_ROOT_SEGMENTS` (`metadata.rs:270-321`). Widening the
/// key space later is a stored-format change with a migration, exactly as that constant's
/// doc records, so the headroom is bought now rather than borrowed.
pub const PART_NUMBER_WIDTH: usize = 6;

/// The largest part number the `part:`/`psum:`/`sidx:` key grammar can address — the whole
/// key space [`PART_NUMBER_WIDTH`] opens, `999_999`.
///
/// A **format** bound (a constant of the encoding), never the live `MAX_PARTS_PER_SESSION`
/// knob (#655's), which is enforced where a part is *admitted*. Mirrors
/// [`crate::metadata::MAX_SEGMENT_INDEX`] and its reasoning (`metadata.rs:275-286`).
pub const MAX_PART_NUMBER: u32 = 10u32.pow(PART_NUMBER_WIDTH as u32) - 1;

/// A **part number**: `[1, MAX_PART_NUMBER]`, the range the fixed-width key grammar can both
/// render and parse back. Zero is not a part (S3 numbers parts from 1) and a number past the
/// key space would name a `part:` record no parser could read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PartNumber(u32);

impl PartNumber {
    /// The validating constructor — the only way to obtain one.
    pub fn new(part_number: u32) -> Result<Self, RecordError> {
        if part_number == 0 || part_number > MAX_PART_NUMBER {
            return Err(RecordError::PartNumberOutOfRange {
                part_number: u64::from(part_number),
            });
        }
        Ok(Self(part_number))
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PartNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for PartNumber {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(u32::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The decimal width a `slot:` key's index is zero-padded to, so byte-lexicographic key
/// order equals index order (`metadata.rs:270-273`'s rule, mirrored).
///
/// **Six digits, pinned at Plan.** `MAX_INFLIGHT_PARTS`'s own clamp arithmetic
/// (`0016:1471`) reaches **≈524,288** at `MAX_PART_CHUNKS = 1` before the `SCAN_CAP`
/// byte-envelope clamp brings it back down — so the format must address at least that many
/// indices, or a legal (if extreme) `MAX_INFLIGHT_PARTS` would mint a slot index no key
/// could spell. Five digits tops out at 99,999, under that figure; six (`999_999`) clears
/// it with margin, mirroring the `PART_NUMBER_WIDTH` headroom rule above.
pub const SLOT_INDEX_WIDTH: usize = 6;

/// The largest index the `slot:` key grammar can address — the whole key space
/// [`SLOT_INDEX_WIDTH`] opens, `999_999`. A **format** bound: the live `MAX_INFLIGHT_PARTS`
/// (#655's) is enforced at *reservation*, never at decode, because 0016 permits lowering it
/// while live sessions still hold indices above the new cap (`0016:390-402`) — and a record
/// that stopped decoding the moment a knob dropped could no longer be renewed, committed or
/// torn down.
pub const MAX_SLOT_INDEX: u32 = 10u32.pow(SLOT_INDEX_WIDTH as u32) - 1;

/// An **in-flight slot index**: `[0, MAX_SLOT_INDEX]`. Index 0 is a real slot (the key space
/// *is* the per-session cap, `0016:349`), so only the upper bound rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SlotIndex(u32);

impl SlotIndex {
    /// The validating constructor — the only way to obtain one.
    pub fn new(index: u32) -> Result<Self, RecordError> {
        if index > MAX_SLOT_INDEX {
            return Err(RecordError::SlotIndexOutOfRange {
                index: u64::from(index),
            });
        }
        Ok(Self(index))
    }

    /// The index.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for SlotIndex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(u32::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// A SHA-256 digest, stored as 64 **lowercase**-hex characters.
///
/// The basis ADR-0047 settled for Wyrd's opaque change token — **never MD5**. A validated
/// type because a digest's *shape* is structural: a short or uppercase digest is not a
/// value any composition could use. Computing one (`Digest::of`, over `sha2`) is the next
/// child's — `sha2` is not a dependency of this crate yet, so this slice carries only the
/// **shape** every later child's `Digest::of` will populate: construction from raw bytes,
/// and the validating hex parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// A digest from its 32 raw bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The validating parser: exactly 64 lowercase-hex characters. Rejects uppercase hex
    /// (a second spelling of one digest), any other length, and any non-hex byte.
    pub fn from_hex(text: &str) -> Result<Self, RecordError> {
        let not_hex = || RecordError::DigestNotHex {
            digest: text.to_string(),
        };
        let raw = text.as_bytes();
        if raw.len() != 64 {
            return Err(not_hex());
        }
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = lowercase_hex_digit(raw[2 * i]).ok_or_else(not_hex)?;
            let lo = lowercase_hex_digit(raw[2 * i + 1]).ok_or_else(not_hex)?;
            *slot = hi * 16 + lo;
        }
        Ok(Self(out))
    }

    /// The 32 raw digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The canonical lowercase-hex rendering.
    pub fn to_hex(&self) -> String {
        hex_lower(&self.0)
    }
}

/// The 16 lowercase-hex characters, in nibble order — the **one** table [`hex_lower`]
/// renders from, so no rendering path can fall back to a stand-in character.
const HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";

/// One lowercase-hex character back to its nibble; `None` for every other byte —
/// uppercase included, since `A` and `a` would otherwise be two spellings of one digest.
/// The exact inverse of [`HEX_DIGITS`]; the test asserts that over all 256 byte values.
fn lowercase_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_hex(&String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// Bytes as lowercase hex — the rendering ADR-0047 settled for the change token, matching
/// the wrapper style `crates/gateway-s3/src/crypto.rs:21-60` carries for this workspace's
/// other SHA-256 usage (a `sha2`-free rendering, since `sha2` is the next child's
/// dependency, not this one's).
///
/// Total by construction: a nibble indexes the private `HEX_DIGITS` table directly, so there
/// is **no** fallback character a rendering bug could hide behind. A digest that rendered a
/// wrong character for some byte value would be a second identity for one set of bytes,
/// which is the same two-spellings-of-one-record fault the key grammar refuses (C-1).
pub fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

// ===========================================================================
// 3. Keys and their canonical parsers
// ===========================================================================

/// The singleton admission-ledger key (`0016:348`). Never deleted; an absent record reads
/// as `{count: 0}` (a later slice's concern — this module states only the key). Its
/// **lack** of a trailing `:` is exactly what keeps it disjoint from [`MPU_PREFIX`]: see
/// that constant's doc.
pub const MPUCTL_KEY: &[u8] = b"mpuctl";

/// Key prefix for session records. The trailing `:` is what keeps this **disjoint from**
/// [`MPUCTL_KEY`] — `"mpuctl".starts_with("mpu:")` is `false` because `mpuctl`'s 4th byte is
/// `c`, not `:` — so `scan(MPU_PREFIX)` can never return the admission singleton
/// (`0016:342-344`).
pub const MPU_PREFIX: &[u8] = b"mpu:";
/// Key prefix for in-flight part slots.
pub const SLOT_PREFIX: &[u8] = b"slot:";
/// Key prefix for committed part records.
pub const PART_PREFIX: &[u8] = b"part:";
/// Key prefix for committed part summaries.
pub const PSUM_PREFIX: &[u8] = b"psum:";
/// Key prefix for owned staging entries — **disjoint from `pending:`**, which is the whole
/// point of the class: no global `scan("pending:")` (the restore pass, the expiry sweep)
/// ever enumerates an owned entry (`0016:475-491`).
pub const SIDX_PREFIX: &[u8] = b"sidx:";
/// Key prefix for byte-mode retirement obligations: orphan-mark, then delete the naming
/// records (`0016:355`).
pub const RETIRE_BYTES_PREFIX: &[u8] = b"retire:bytes:";
/// Key prefix for record-mode retirement obligations: delete records only — never
/// orphan-mark (`0016:356`).
pub const RETIRE_RECORDS_PREFIX: &[u8] = b"retire:records:";

/// A **fixed-width** `width`-digit decimal: exactly `width` ASCII digits — no `+`/`-` sign,
/// no short spelling, no over-wide one. This is the rule the zero-padded key fields are
/// checked against, and it is what makes byte-lexicographic key order equal numeric order
/// (`metadata.rs:270-273`). `u32::from_str` alone would accept `+7`, `7` and `0000007` as
/// three spellings of one record.
fn fixed_width_u32(text: &str, width: usize) -> Option<u32> {
    if text.len() != width || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A decimal in **canonical** form: ASCII digits only (no sign), and no leading zero unless
/// the value *is* `0` — the rule `parse_seg_key` applies to its own un-padded epoch
/// (`metadata.rs:1296-1300`), applied to every variable-width decimal this module parses (a
/// chunk id, a retirement token's epoch, version and per-part-suffix part number).
///
/// Stated here rather than shared with `metadata::parse_canonical_u64`
/// (`metadata.rs:1310-1318`), which is **private** and `u64`-only: making it shared is an
/// edit to `metadata.rs`, and this slice's scope pins that file untouched. The child that
/// first *writes* one of these records is where the two become one generic helper; until
/// then the semantics are pinned identical by the same rejection table
/// (`crates/core/tests/multipart_keys.rs`), digit-for-digit.
fn canonical_decimal<T: std::str::FromStr>(text: &str) -> Option<T> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if text.len() > 1 && text.starts_with('0') {
        return None;
    }
    text.parse().ok()
}

/// Split `key` into exactly `fields` `:`-separated components after `prefix`, failing closed
/// on a non-UTF-8 key, a missing prefix, or the wrong field count (a truncated key, or one
/// with a trailing component) — the shape every keyed-record parser below shares.
fn split_key<'a>(
    key: &'a [u8],
    prefix: &str,
    namespace: &'static str,
    fields: usize,
) -> Result<Vec<&'a str>, RecordError> {
    let text = std::str::from_utf8(key).map_err(|_| malformed_key(namespace, key))?;
    let rest = text
        .strip_prefix(prefix)
        .ok_or_else(|| malformed_key(namespace, key))?;
    let parts: Vec<&str> = rest.splitn(fields, ':').collect();
    if parts.len() != fields {
        return Err(malformed_key(namespace, key));
    }
    Ok(parts)
}

/// Key for a session record: `mpu:<upload-id>`.
pub fn mpu_key(upload_id: &UploadId) -> Vec<u8> {
    format!("mpu:{upload_id}").into_bytes()
}

/// Parse an [`mpu_key`] back into its upload id.
pub fn parse_mpu_key(key: &[u8]) -> Result<UploadId, RecordError> {
    let fields = split_key(key, "mpu:", "mpu:", 1)?;
    UploadId::new(fields[0])
}

/// Key for one in-flight part slot: `slot:<upload-id>:<index>`, zero-padded to
/// [`SLOT_INDEX_WIDTH`].
pub fn slot_key(upload_id: &UploadId, index: SlotIndex) -> Vec<u8> {
    format!(
        "slot:{upload_id}:{:0width$}",
        index.get(),
        width = SLOT_INDEX_WIDTH
    )
    .into_bytes()
}

/// The bounded per-session slot range: `slot:<upload-id>:` (`0016:349`).
pub fn slot_range(upload_id: &UploadId) -> Vec<u8> {
    format!("slot:{upload_id}:").into_bytes()
}

/// Parse a [`slot_key`] back into `(upload_id, index)`, rejecting every non-canonical
/// spelling of one slot.
pub fn parse_slot_key(key: &[u8]) -> Result<(UploadId, SlotIndex), RecordError> {
    let fields = split_key(key, "slot:", "slot:", 2)?;
    let index =
        fixed_width_u32(fields[1], SLOT_INDEX_WIDTH).ok_or_else(|| malformed_key("slot:", key))?;
    Ok((UploadId::new(fields[0])?, SlotIndex::new(index)?))
}

fn part_scoped_key(prefix: &str, upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    format!(
        "{prefix}{upload_id}:{:0width$}",
        part_number.get(),
        width = PART_NUMBER_WIDTH
    )
    .into_bytes()
}

fn parse_part_scoped_key(
    key: &[u8],
    prefix: &'static str,
    namespace: &'static str,
) -> Result<(UploadId, PartNumber), RecordError> {
    let fields = split_key(key, prefix, namespace, 2)?;
    let part_number = fixed_width_u32(fields[1], PART_NUMBER_WIDTH)
        .ok_or_else(|| malformed_key(namespace, key))?;
    Ok((UploadId::new(fields[0])?, PartNumber::new(part_number)?))
}

/// Key for a committed part: `part:<upload-id>:<part-number>`, zero-padded to
/// [`PART_NUMBER_WIDTH`] so a range read is in part-number order.
pub fn part_key(upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    part_scoped_key("part:", upload_id, part_number)
}

/// The bounded per-session part range: `part:<upload-id>:`.
pub fn part_range(upload_id: &UploadId) -> Vec<u8> {
    format!("part:{upload_id}:").into_bytes()
}

/// Parse a [`part_key`] back into `(upload_id, part_number)`.
pub fn parse_part_key(key: &[u8]) -> Result<(UploadId, PartNumber), RecordError> {
    parse_part_scoped_key(key, "part:", "part:")
}

/// Key for a committed part's summary: `psum:<upload-id>:<part-number>`.
pub fn psum_key(upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    part_scoped_key("psum:", upload_id, part_number)
}

/// The bounded per-session summary range: `psum:<upload-id>:`.
pub fn psum_range(upload_id: &UploadId) -> Vec<u8> {
    format!("psum:{upload_id}:").into_bytes()
}

/// Parse a [`psum_key`] back into `(upload_id, part_number)`.
pub fn parse_psum_key(key: &[u8]) -> Result<(UploadId, PartNumber), RecordError> {
    parse_part_scoped_key(key, "psum:", "psum:")
}

/// Key for one **owned staging entry**: `sidx:<upload-id>:<part-number>:<chunk-id>`. The
/// `<part-number>` component is what lets a reclaimer attribute residue to the part attempt
/// that staged it (`0016:353`).
pub fn sidx_key(upload_id: &UploadId, part_number: PartNumber, chunk: ChunkId) -> Vec<u8> {
    format!(
        "sidx:{upload_id}:{:0width$}:{chunk}",
        part_number.get(),
        width = PART_NUMBER_WIDTH
    )
    .into_bytes()
}

/// The bounded per-session owned range: `sidx:<upload-id>:` — the **only** way any pass
/// reads owned entries; there is no global scan of them anywhere (`0016:475-491`).
pub fn sidx_range(upload_id: &UploadId) -> Vec<u8> {
    format!("sidx:{upload_id}:").into_bytes()
}

/// Parse a [`sidx_key`] back into `(upload_id, part_number, chunk_id)`. The chunk-id
/// component is **canonical** decimal, not fixed-width — `7` and `007` must never both
/// parse, the same canonicality rule the padded fields enforce by width instead.
pub fn parse_sidx_key(key: &[u8]) -> Result<(UploadId, PartNumber, ChunkId), RecordError> {
    let fields = split_key(key, "sidx:", "sidx:", 3)?;
    let part_number =
        fixed_width_u32(fields[1], PART_NUMBER_WIDTH).ok_or_else(|| malformed_key("sidx:", key))?;
    let chunk: ChunkId = canonical_decimal(fields[2]).ok_or_else(|| malformed_key("sidx:", key))?;
    Ok((
        UploadId::new(fields[0])?,
        PartNumber::new(part_number)?,
        chunk,
    ))
}

// ===========================================================================
// 4. The `retire:` token grammar (`0016:358-380`)
// ===========================================================================

/// Which of the two retirement modes an obligation is in — read from the **key**, never a
/// field: a boolean misread once is silent data loss, a malformed key prefix is an error at
/// decode (`0016:434-440`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireMode {
    /// Orphan-mark the bytes, then delete the records that named them.
    Bytes,
    /// Delete records only — **never** orphan-mark.
    Records,
}

impl RetireMode {
    /// Every mode, for a total dispatch.
    pub const ALL: [Self; 2] = [Self::Bytes, Self::Records];

    /// The key prefix this mode lives under.
    pub const fn prefix(self) -> &'static [u8] {
        match self {
            Self::Bytes => RETIRE_BYTES_PREFIX,
            Self::Records => RETIRE_RECORDS_PREFIX,
        }
    }
}

/// Classify the **mode component** alone: which of the two prefixes the key opens with, or
/// which of the two errors it earns. Deliberately **private** — a mode is meaningful only
/// once the key naming it decodes as a whole, which is what [`parse_retire_mode`] answers;
/// nothing outside this module may act on a prefix match.
///
/// The two failures are **different errors**, deliberately, because they are different
/// faults for the drain that meets one: [`RecordError::UnknownRetireMode`] means a key in
/// the `retire:` namespace named a third mode (a record whose disposal rule this build does
/// not know — the drain must not guess between orphan-marking and record-only deletion),
/// while [`RecordError::MalformedKey`] means the key is not a `retire:` key at all (a
/// neighbour's record, or a truncated `retire:bytes` with no token). Reporting the second as
/// "mode `mpu` is neither" would name a mode nothing wrote and send an operator looking for a
/// retirement obligation that does not exist.
fn retire_mode_prefix(key: &[u8]) -> Result<RetireMode, RecordError> {
    if key.starts_with(RETIRE_BYTES_PREFIX) {
        return Ok(RetireMode::Bytes);
    }
    if key.starts_with(RETIRE_RECORDS_PREFIX) {
        return Ok(RetireMode::Records);
    }
    let malformed = || malformed_key("retire:", key);
    let rest = std::str::from_utf8(key)
        .ok()
        .and_then(|text| text.strip_prefix("retire:"))
        .ok_or_else(malformed)?;
    // `retire:<mode>:…` — a mode component exists, and it is neither of the two.
    let (mode, _) = rest.split_once(':').ok_or_else(malformed)?;
    Err(RecordError::UnknownRetireMode {
        mode: mode.to_string(),
    })
}

/// Parse the mode out of a `retire:` key, failing closed on **every** other spelling — the
/// boundary the mode-in-the-key argument rests on: the drain dispatches on the mode and
/// "treats a `retire:` key it cannot parse as an error, never as a default"
/// (`0016:438-440`).
///
/// It therefore answers a mode only for a key that decodes **whole**, token included
/// ([`parse_retire_key`]) — a prefix match is not a mode. `retire:bytes:` with no token, and
/// `retire:bytes:` followed by bytes that are not UTF-8, are `retire:`-namespace keys that
/// name no obligation, so they are [`RecordError::MalformedKey`], not `Ok(Bytes)`: a caller
/// that dispatched on the prefix alone (the retirement drain of #656–#659) would orphan-mark
/// the fragments of a key it cannot read, and orphan-marking on a guess is the permanent,
/// data-losing failure mode C-1 refuses (`docs/principles.md` §5 C-1). Deciding the mode by
/// the same decode that yields the token is what keeps the two answers from ever disagreeing
/// — one spelling, one decision, as everywhere else in this module (ADR-0045).
pub fn parse_retire_mode(key: &[u8]) -> Result<RetireMode, RecordError> {
    parse_retire_key(key).map(|(mode, _)| mode)
}

/// A retirement token, whose grammar makes reuse impossible (`0016:358-380`): every
/// component is minted once — an epoch is bumped by every fence, a `(part, attempt)` pair
/// belongs to one `UploadPart`, an `(inode, version)` pair is produced by exactly one
/// publication. Installation is `require_absent(retire:<mode>:<token>)`, so a collision is a
/// `Conflict` the installer classifies, never a silent overwrite that would replace one
/// obligation's payload with another's and permanently lose the reclamation evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetireToken {
    /// A **session-scoped** obligation: `s:<upload-id>:<epoch>` — the whole-session teardown
    /// an epoch's fence installs — or, with the optional suffix,
    /// `s:<upload-id>:<epoch>:<part-number>:<attempt-id>` for a **per-part** obligation (a
    /// re-uploaded part's superseded chunks, a losing writer's compensation).
    ///
    /// Its part number is **canonical** decimal, not the zero-padded fixed width the
    /// `part:`/`psum:`/`sidx:` keys use, because the two grammars answer different questions:
    /// those keys are read as *ranges in part-number order*, where padding is what makes byte
    /// order equal numeric order, while the only `retire:` range anything reads is the
    /// session's emptiness gate `retire:<mode>:s:<upload-id>:` (`0016:374-380`), which is
    /// order-free. Canonicality — one spelling per token, so `require_absent` cannot be
    /// defeated — is preserved either way: the leading-zero rule here, the width rule there.
    Session {
        /// The owning session's upload id.
        upload_id: UploadId,
        /// The epoch whose fence installed the obligation.
        epoch: u64,
        /// The part attempt, for a per-part obligation.
        part: Option<(PartNumber, AttemptId)>,
    },
    /// A **superseded or deleted generation**: `g:<inode-id>:<version>`.
    Generation {
        /// The inode.
        inode: InodeId,
        /// Its version.
        version: u64,
    },
}

impl fmt::Display for RetireToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session {
                upload_id,
                epoch,
                part: None,
            } => write!(f, "s:{upload_id}:{epoch}"),
            Self::Session {
                upload_id,
                epoch,
                part: Some((part_number, attempt_id)),
            } => write!(f, "s:{upload_id}:{epoch}:{part_number}:{attempt_id}"),
            Self::Generation { inode, version } => write!(f, "g:{inode}:{version}"),
        }
    }
}

/// Key for a retirement obligation: `retire:<mode>:<token>`.
pub fn retire_key(mode: RetireMode, token: &RetireToken) -> Vec<u8> {
    let mut key = mode.prefix().to_vec();
    key.extend_from_slice(token.to_string().as_bytes());
    key
}

/// The bounded session-scoped obligation range for one mode: `retire:<mode>:s:<upload-id>:`.
/// A session's terminal delete gate is two of these emptiness reads, never a walk of the
/// whole `retire:` namespace, which is deliberately not cardinality-bounded
/// (`0016:374-380`).
pub fn retire_session_range(mode: RetireMode, upload_id: &UploadId) -> Vec<u8> {
    let mut key = mode.prefix().to_vec();
    key.extend_from_slice(format!("s:{upload_id}:").as_bytes());
    key
}

/// Parse a [`retire_key`] back into `(mode, token)`, failing closed on any other spelling: an
/// absent or truncated token, a trailing component, a non-canonical epoch/version/part-number
/// (a `+` sign or a leading zero), a malformed upload/attempt id, or a token whose bytes are
/// not UTF-8. This is the **whole** `retire:` decode — [`parse_retire_mode`] is this function
/// keeping only its first half of the answer, never a cheaper prefix test.
pub fn parse_retire_key(key: &[u8]) -> Result<(RetireMode, RetireToken), RecordError> {
    let mode = retire_mode_prefix(key)?;
    let text = std::str::from_utf8(key).map_err(|_| malformed_key("retire:", key))?;
    let rest = &text[mode.prefix().len()..];
    let fields: Vec<&str> = rest.split(':').collect();
    let malformed = || malformed_key("retire:", key);
    let token = match fields.as_slice() {
        ["s", upload_id, epoch] => RetireToken::Session {
            upload_id: UploadId::new(*upload_id)?,
            epoch: canonical_decimal(epoch).ok_or_else(malformed)?,
            part: None,
        },
        ["s", upload_id, epoch, part_number, attempt_id] => RetireToken::Session {
            upload_id: UploadId::new(*upload_id)?,
            epoch: canonical_decimal(epoch).ok_or_else(malformed)?,
            part: Some((
                PartNumber::new(canonical_decimal(part_number).ok_or_else(malformed)?)?,
                AttemptId::new(*attempt_id)?,
            )),
        },
        ["g", inode, version] => RetireToken::Generation {
            inode: canonical_decimal(inode).ok_or_else(malformed)?,
            version: canonical_decimal(version).ok_or_else(malformed)?,
        },
        _ => return Err(malformed()),
    };
    Ok((mode, token))
}

// ===========================================================================
// 5. The admission ledger — the `mpuctl` record VALUE (`0016:348`)
// ===========================================================================

/// Half [`SCAN_CAP`] — the cardinality ceiling `0016` states two of this record's rules
/// against: the per-session owned-`sidx:` population (`0016:1471`, `:2098`) and the
/// `MAX_SESSIONS` clamp that keeps the reaper's `scan("mpu:")` inside one complete-or-fail
/// scan (`0016:1470`).
///
/// [`SCAN_CAP`] earns a place at **decode** where a live capacity knob does not: it is a
/// **seam** constant, documented in the trait crate as "a correctness constraint, not a
/// tuning knob" (`crates/traits/src/lib.rs:272-286`) — one number every backend of the trait
/// must agree on, not a number a deployment chooses. Refusing a stored record against it is
/// therefore refusing it against the format's own arithmetic, and no operator action can make
/// a durable ledger unreadable (`0016:390-402`, the boundary
/// [`crate::metadata::MAX_ROOT_SEGMENTS`] draws for the other direction).
const SCAN_HALF: u64 = (SCAN_CAP as u64) / 2;

/// The budget **profile** tuple `mpuctl` stores and every admitter and custodian compares
/// whole (`0016:348`): `(W_ref, MAX_PART_CHUNKS, MAX_PARTS_PER_SESSION, MAX_INFLIGHT_PARTS,
/// MAX_STAGED_CHUNKS)`. Stored rather than derived per gateway because equal quotients can
/// hide unequal footprints, so a rolling configuration change cannot leave two gateways
/// enforcing different bounds (`0016:2605`, X76; `0016:2593`, X64).
///
/// The fields are private behind the one fallible conversion every surface funnels through
/// (`TryFrom<BudgetWire>`, the rules `Budget::checked_rules` states), so no `Budget` exists
/// whose own derivations are undefined: [`Budget::u_ref`] and [`Budget::max_sessions`] are
/// total for every value of this type, and a tuple that would make them otherwise is an error
/// at the boundary rather than a value inside the program (ADR-0045, parse-don't-validate). A
/// writer-side constructor is deliberately absent — the first writers are the store round
/// trips (#656–#659), and a knob-range check over an operator's *configuration* is a
/// different boundary, #508's and #655's (`0016:1458-1466`).
///
/// The wire shape is **closed** (`deny_unknown_fields`) for the reason [`AdmissionRecord`]
/// records: this tuple is part of the value CAS compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BudgetWire")]
pub struct Budget {
    w_ref: u64,
    max_part_chunks: u32,
    max_parts_per_session: u32,
    max_inflight_parts: u32,
    max_staged_chunks: u32,
}

impl Budget {
    /// `W_ref` — the reconcile pass's staged-reference memory budget, in chunk-refs.
    pub const fn w_ref(&self) -> u64 {
        self.w_ref
    }

    /// `MAX_PART_CHUNKS` — chunk-refs one `part:` record may hold.
    pub const fn max_part_chunks(&self) -> u32 {
        self.max_part_chunks
    }

    /// `MAX_PARTS_PER_SESSION` — committed parts one session may hold.
    pub const fn max_parts_per_session(&self) -> u32 {
        self.max_parts_per_session
    }

    /// `MAX_INFLIGHT_PARTS` — the `slot:` key space, hence parts in flight per session.
    pub const fn max_inflight_parts(&self) -> u32 {
        self.max_inflight_parts
    }

    /// `MAX_STAGED_CHUNKS` — chunk-refs a session may hold in committed `part:` records.
    pub const fn max_staged_chunks(&self) -> u32 {
        self.max_staged_chunks
    }

    /// The worst-case **owned `sidx:` population of one session** — `MAX_INFLIGHT_PARTS ×
    /// MAX_PART_CHUNKS`, the quantity `0016` bounds by `SCAN_CAP/2` (`0016:1471`, `:2098`) and
    /// charges twice in [`Budget::u_ref_exact`]'s second term (`0016:1469`).
    ///
    /// **One definition, used by both**, so the rule (G5) and the charge can never disagree
    /// about what "in-flight owned refs" means — the reason `checked_chunk_bytes` exists for
    /// the other cross-checked quantity in this repo (`metadata.rs:1208-1218`).
    fn inflight_owned_refs(&self) -> u128 {
        u128::from(self.max_inflight_parts) * u128::from(self.max_part_chunks)
    }

    /// `U_ref` in **exact** integers, verbatim `0016:1469`:
    ///
    /// ```text
    /// U_ref = min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) x MAX_PART_CHUNKS ,
    ///              MAX_STAGED_CHUNKS + 2 x MAX_INFLIGHT_PARTS x MAX_PART_CHUNKS )
    /// ```
    ///
    /// The first term is the raw part-number space; the second is the enforced staged ceiling
    /// plus the bounded commit overshoot plus the in-flight owned entries — each part charged
    /// its **full** `max_part_chunks`, because a part is not one unit (`0016:1469`). Which
    /// term binds is a property of the profile, not a formality: at maximal parts the raw term
    /// charges far more than Complete would let the session publish, which is why the ceiling
    /// term exists at all.
    ///
    /// **The `u128` width is load-bearing rather than defensive.** A decoder evaluates this
    /// over bytes it has not yet judged (G7 does, on a candidate), so both terms are computed
    /// from values a torn record may set to their field maxima: at `max_staged_chunks =
    /// u32::MAX` the second term leaves `u32`, and at maximal `max_part_chunks` the first
    /// leaves `u64` — while the `min` can make the leaving term irrelevant, so a record that
    /// names it may still be legal and must still decode. Same-width arithmetic answers those
    /// two cases with a panic (debug overflow checks) or a wrapped verdict (release); `u128`
    /// answers both with the mathematical value, and every operand here is at most `2^65`, so
    /// the width itself cannot overflow. ADR-0045 names checked arithmetic for the same reason
    /// on `InodeRecord` version increments and `PendingEntry` lease timestamps
    /// (`docs/design/adr/0045-metadata-validation-boundaries.md:73-74`).
    fn u_ref_exact(&self) -> u128 {
        let raw = (u128::from(self.max_parts_per_session) + u128::from(self.max_inflight_parts))
            * u128::from(self.max_part_chunks);
        let ceiling = u128::from(self.max_staged_chunks) + 2 * self.inflight_owned_refs();
        raw.min(ceiling)
    }

    /// `U_ref` — this profile's worst-case per-session staged-reference footprint
    /// (`Budget::u_ref_exact`, `0016:1469`).
    ///
    /// Total, and `u64` rather than the `u128` it is computed in: G7 refuses any tuple whose
    /// `U_ref` exceeds its own `w_ref`, and `w_ref` is a `u64`, so every `Budget` that exists
    /// has a `U_ref` inside the width its budget is stated in. The narrowing therefore states
    /// a type invariant the way [`crate::metadata::encode`] states serialization's
    /// (`metadata.rs:1562-1566`) — not a fallible step with a hidden failure mode.
    pub fn u_ref(&self) -> u64 {
        u64::try_from(self.u_ref_exact()).expect("G7 bounds every Budget's U_ref by its w_ref")
    }

    /// `MAX_SESSIONS = min( ⌊W_ref / U_ref⌋ , SCAN_CAP/2 )` — **derived, never chosen**
    /// (`0016:1470`).
    ///
    /// Both terms bind. The quotient is the memory bound the reconcile pass is sized for
    /// (`Σ_sessions U_ref ≤ W_ref` by construction); the `SCAN_CAP/2` term is a clamp the
    /// implementation applies rather than an operator range check, because `W_ref` is sized
    /// from host RAM and `U_ref` from the caps — a legal pairing (a large `W_ref` with small
    /// parts) makes the quotient exceed `SCAN_CAP` and break the reaper's `scan("mpu:")`.
    /// The clamp is what makes the two bounds compose.
    ///
    /// Total: G1 ∧ G2 put `U_ref ≥ 1`, so the division always has a divisor.
    pub fn max_sessions(&self) -> u64 {
        (self.w_ref / self.u_ref()).min(SCAN_HALF)
    }

    /// The profile's whole rule set, in one place and applied wherever a [`Budget`] can come
    /// into existence — the shape `InodeRecord::checked_shape` uses for the other cross-field
    /// record invariant in this repo (`metadata.rs:1458-1474`).
    ///
    /// **These are record rules, not a configuration validator.** Every one relates the
    /// tuple's own stored components to each other or to a constant of the **format** that
    /// cannot move under a stored record — [`MAX_PART_NUMBER`] and `SCAN_CAP/2`. The knob
    /// *ranges* `0016` settles for an operator's choice (the `max_chunkref_bytes`
    /// value-ceiling that puts `MAX_PART_CHUNKS` in 165–381, the `B_ops` clamp, the
    /// `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS` ceiling on `MAX_STAGED_CHUNKS`) are deliberately
    /// **absent**: those constants have no definition on this base, they are #508's and
    /// #625's to value, and `0016:1466`/`:1468` enforce them where work is admitted
    /// (`UploadPart`, part commit). A decode that consulted one would make a durable ledger
    /// unreadable the day a deployment moved it — what `0016:390-402` and
    /// [`crate::metadata::MAX_ROOT_SEGMENTS`] both forbid, and this ledger is the record every
    /// teardown path must read to decrement `count`.
    ///
    /// `max_parts_per_session ≥ 1` and `max_inflight_parts ≤ MAX_SLOT_INDEX + 1` are
    /// deliberately **not** rules of their own: G2 ∧ G4 implies the first, and G3 ∧ G4 binds
    /// the second tighter than [`MAX_SLOT_INDEX`] would ([`MAX_PART_NUMBER`] is `999_999`).
    fn checked_rules(&self) -> Result<(), RecordError> {
        // G1 (`0016:1466`, `> 0`) — the totality precondition, checked FIRST and before any
        // derivation: at zero, `U_ref` is zero and `MAX_SESSIONS`' quotient has no divisor.
        if self.max_part_chunks == 0 {
            return Err(RecordError::MaxPartChunksZero);
        }
        // G2 (`0016:1471`, the range `[1, …]`) — the second half of that precondition.
        if self.max_inflight_parts == 0 {
            return Err(RecordError::MaxInflightPartsZero);
        }
        // G3 — the `part:` key space, the only bound `0016`'s knob table leaves for this cap.
        if self.max_parts_per_session > MAX_PART_NUMBER {
            return Err(RecordError::PartsPerSessionUnaddressable {
                max_parts_per_session: self.max_parts_per_session,
            });
        }
        // G4 (`0016:1471` clamp 1).
        if self.max_inflight_parts > self.max_parts_per_session {
            return Err(RecordError::InflightPartsExceedParts {
                max_inflight_parts: self.max_inflight_parts,
                max_parts_per_session: self.max_parts_per_session,
            });
        }
        // G5 (`0016:1471`, `:2098`) — the same owned-`sidx:` product `U_ref`'s ceiling term
        // charges, exact in `u128`, so no wrap can defeat the comparison.
        let owned_sidx = self.inflight_owned_refs();
        if owned_sidx > u128::from(SCAN_HALF) {
            return Err(RecordError::StagingRangeUnscannable { owned_sidx });
        }
        // G6 (`0016:1468`, the lower end of the settled range).
        if self.max_staged_chunks < self.max_part_chunks {
            return Err(RecordError::StagedChunksBelowPart {
                max_staged_chunks: self.max_staged_chunks,
                max_part_chunks: self.max_part_chunks,
            });
        }
        // G7 (`0016:1473`, `W_ref`'s range `[U_ref, deployment RAM]`) — against the exact
        // `u128` footprint, so one past `u64` compares as the number it is rather than as a
        // saturated stand-in. It is also what makes [`Budget::u_ref`]'s narrowing total.
        let u_ref = self.u_ref_exact();
        if u128::from(self.w_ref) < u_ref {
            return Err(RecordError::BudgetBelowFootprint {
                w_ref: self.w_ref,
                u_ref,
            });
        }
        Ok(())
    }
}

/// The wire shape of [`Budget`], field order and names exactly as `0016:348` states them.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetWire {
    w_ref: u64,
    max_part_chunks: u32,
    max_parts_per_session: u32,
    max_inflight_parts: u32,
    max_staged_chunks: u32,
}

impl TryFrom<BudgetWire> for Budget {
    type Error = RecordError;

    fn try_from(wire: BudgetWire) -> Result<Self, RecordError> {
        // The candidate is a local value, returned only once every rule holds — so no caller
        // can hold a `Budget` that broke one, and the rules can be stated over the same
        // derivations the type exposes rather than over a parallel copy of them.
        let budget = Self {
            w_ref: wire.w_ref,
            max_part_chunks: wire.max_part_chunks,
            max_parts_per_session: wire.max_parts_per_session,
            max_inflight_parts: wire.max_inflight_parts,
            max_staged_chunks: wire.max_staged_chunks,
        };
        budget.checked_rules()?;
        Ok(budget)
    }
}

/// The **admission ledger** singleton, the value under [`MPUCTL_KEY`]: one record, three
/// fields, CAS'd **whole**, so the count and the limit it was checked against can never be
/// read apart (`0016:348`, decision 6).
///
/// `max_sessions` is checked against `profile` at decode because it is **derived**
/// ([`Budget::max_sessions`], `0016:1470`) and never independently chosen. Admission enforces
/// the **stored** limit — deliberately, so every gateway in the fleet agrees on one number
/// (`0016:2593`, X64) — which is exactly why the stored number may not be free: a torn ledger
/// naming a larger `max_sessions` than its own profile derives would be trusted fleet-wide,
/// and `Σ_sessions U_ref ≤ W_ref`, the bound the whole reconcile pass is sized for, would be
/// exceeded on hosts that never observe the overrun (an OOM landing on the maintenance plane
/// rather than on the gateway that caused it).
///
/// `count` is deliberately **not** checked against `max_sessions`. Occupancy above a lowered
/// cap is legitimate live state, not a torn identity: a profile lowered while sessions are
/// live leaves the ledger over its new cap until the population drains, and admission simply
/// refuses to grow it. Refusing it at decode would make the ledger unreadable exactly when
/// every teardown path needs to read it to decrement `count` — wedging multipart fleet-wide
/// with no path that clears it (`0016:390-402`; the same liberal-on-read boundary
/// [`crate::metadata::MAX_ROOT_SEGMENTS`] draws). The line this record class settles: two
/// stored spellings of **one** quantity (`max_sessions` versus what `profile` derives) are a
/// decode error; one quantity merely being large relative to another (`count` versus
/// `max_sessions`) is not.
///
/// # Serialization identity, stated with its domain
///
/// For bytes **this codec wrote**, decode→encode is byte-identical: every field is required,
/// none is optional, defaulted or skipped, and the wire shape is **closed**
/// (`deny_unknown_fields`), so [`crate::metadata::encode`] re-emits exactly the names, order
/// and numbers it read. That is the property a whole-record CAS needs, and the reason the
/// shape is closed rather than tolerant: a dropped-on-read field would be silent, and the two
/// CAS shapes this repo already contains punish it differently but both durably — the
/// `inode:` commits precondition on the **re-encoded** prior (`metadata.rs:1794`, `:1919`;
/// ADR-0047), where the re-encode would no longer equal the stored bytes and every later CAS
/// would `Conflict` forever, while the `pending:` commits precondition on the **raw bytes they
/// read** (`metadata.rs:2012`), where the CAS succeeds and the put silently writes the record
/// back without the field. `0016:348` does not say which shape `mpuctl` takes (that is
/// #656–#659's), so the closed shape forecloses both — a loud typed decode error at the one
/// place a human can read it (ADR-0045). Its cost is that a future additive field to this
/// record is a versioned format change, exactly as `0016:390-402` says a format maximum is.
///
/// What that identity does **not** claim: decode is not a canonicalisation check. A foreign
/// spelling of the same value — fields reordered, whitespace inserted — still decodes, to the
/// same value, and re-encodes in this codec's spelling rather than in its own; JSON, not this
/// record, is what makes those spellings equal. So a CAS preconditioned on the *re-encoded*
/// prior holds exactly while every `mpuctl` writer goes through [`crate::metadata::encode`] —
/// an obligation of the slices that add the writer (#656–#659), and the reason the alternative
/// precondition (the raw bytes just read, `metadata.rs:2012`) is equally available to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AdmissionRecordWire")]
pub struct AdmissionRecord {
    count: u64,
    max_sessions: u64,
    profile: Budget,
}

impl AdmissionRecord {
    /// How many `mpu:` records exist, in any state (`0016:348`).
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// The governing limit those increments were admitted against.
    pub const fn max_sessions(&self) -> u64 {
        self.max_sessions
    }

    /// The budget tuple that establishes [`Self::max_sessions`].
    pub const fn profile(&self) -> &Budget {
        &self.profile
    }
}

/// The wire shape of [`AdmissionRecord`], field order and names exactly as `0016:348` states
/// them. Its `profile` is the **unvalidated** [`BudgetWire`], deliberately: the profile's own
/// rules are applied by [`Budget`]'s conversion inside this record's conversion, where the
/// typed [`RecordError`] survives — a nested validating `Deserialize` would have been
/// stringified by serde's `custom` funnel before [`decode_admission_record`] could see it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecordWire {
    count: u64,
    max_sessions: u64,
    profile: BudgetWire,
}

/// The ledger's own rule, over a profile [`Budget`]'s conversion has already judged — the one
/// place an [`AdmissionRecord`] can come into existence:
///
/// * **G8** `max_sessions == profile.max_sessions()` (`0016:1470`).
impl TryFrom<AdmissionRecordWire> for AdmissionRecord {
    type Error = RecordError;

    fn try_from(wire: AdmissionRecordWire) -> Result<Self, RecordError> {
        let profile = Budget::try_from(wire.profile)?;
        let derived = profile.max_sessions();
        let stored = wire.max_sessions;
        if stored != derived {
            return Err(RecordError::MaxSessionsNotDerived { stored, derived });
        }
        Ok(Self {
            count: wire.count,
            max_sessions: stored,
            profile,
        })
    }
}

/// Decode the `mpuctl` value ([`MPUCTL_KEY`]) with its rejection **attributed**: the rule a
/// stored ledger broke comes back as its own [`RecordError`] variant, not as prose.
///
/// The peer of [`crate::metadata`]'s per-record decoders (`decode_segment_record`,
/// `metadata.rs:2536-2547`), and public for the same reason: the store round trips that read
/// `mpuctl` (#656–#659) need the fault typed, because "this ledger is torn" and "the store is
/// failing" are different operator actions and a stringified error is indistinguishable from a
/// backend outage.
///
/// It reaches the wire struct through the store-wide [`crate::metadata::decode`] and then
/// applies the record's rules directly, rather than decoding into [`AdmissionRecord`] and
/// recovering the type afterwards: serde's `Error::custom` funnel turns a domain error into a
/// `serde_json::Error` on the way out, so a `downcast` after the fact cannot see it. Decoding
/// through [`AdmissionRecord`]'s own `Deserialize` — what [`crate::metadata::decode`] does for
/// any consumer holding the type — applies the **same** rules and differs only in that the
/// failure arrives untyped.
pub fn decode_admission_record(value: &[u8]) -> Result<AdmissionRecord, RecordError> {
    let wire: AdmissionRecordWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "mpuctl",
            detail: err.to_string(),
        })?;
    AdmissionRecord::try_from(wire)
}
