//! The L4 metadata model, layered on the narrow [`MetadataStore`] primitive.
//!
//! The store is a conditional key/value commit; this module gives it
//! filesystem meaning (architecture §5): hierarchical **inode + dirent** keys so
//! that `create` writes an inode and its dirent atomically and `rename` is a
//! single dirent mutation, a per-inode **version** for compare-and-set at the
//! commit point, and the **pending-chunk ledger**. It is backend-agnostic —
//! generic over `&impl MetadataStore` — so the same model runs over redb today
//! and TiKV later (ADR-0008, ADR-0010).
//!
//! Records are encoded as JSON for M0 (debuggable; a compact codec is a later
//! optimization). The four-phase write protocol that drives these operations
//! lands with the client write path (M0.5).

use std::fmt;

use bytes::Bytes;
use serde::de::{DeserializeOwned, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use wyrd_traits::{
    ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

/// An inode identifier.
pub type InodeId = u64;

/// The reserved global version-fence counter (ADR-0015). Initialized but not yet
/// enforced as a read fence in M0; per-inode versions carry the commit CAS.
pub const VERSION_KEY: &[u8] = b"meta:version";

/// Key for an inode record: `inode:<id>`.
pub fn inode_key(id: InodeId) -> Vec<u8> {
    format!("inode:{id}").into_bytes()
}

/// Key for a directory entry: `dirent:<parent_id>/<name>`.
pub fn dirent_key(parent: InodeId, name: &str) -> Vec<u8> {
    format!("dirent:{parent}/{name}").into_bytes()
}

/// Key for a pending-chunk ledger entry: `pending:<chunk_id>`.
pub fn pending_key(chunk: ChunkId) -> Vec<u8> {
    format!("pending:{chunk}").into_bytes()
}

/// Key for a **bucket record**: `bucket:<name>` (ADR-0046 decision 1) — disjoint from
/// `inode:`/`dirent:`/`pending:`/`orphan:`. The record is the authority on bucket
/// existence. CreateBucket (#511) **writes** it; ListObjectsV2 / GET / HEAD (#507) **read**
/// it so an absent bucket answers `NoSuchBucket` rather than an empty listing or `NoSuchKey`.
pub fn bucket_key(name: &str) -> Vec<u8> {
    format!("bucket:{name}").into_bytes()
}

/// Key prefix for the **orphan ledger** — the reader-safe grace record an orphaning
/// operation (a delete, or a completed reconstruction / rebalance) writes when it
/// strands a fragment, so the custodian **GC** loop reclaims the bytes only once the
/// grace window has elapsed (proposal 0005, "The four custodian loops" / GC,
/// `0005:288-295`; the reader-safe window `0005:291-294`). The value is the
/// logical-millis instant the fragment became orphaned.
pub const ORPHAN_PREFIX: &[u8] = b"orphan:";

/// Key for an orphan-ledger grace record: `orphan:<dserver>:<chunk>:<index>`.
///
/// Defined here beside [`pending_key`] because the orphan ledger is a **metadata-store
/// key protocol shared by both sides of a delete**: the delete path ([`unlink`], and the
/// gateway's `delete_object`) **writes** it, and the custodian GC
/// (`crates/custodian/src/gc.rs`) **reads** it. A single source of truth so a delete's
/// grace record and GC's scan can never key-format-drift — the crash-leak backstop is only
/// real if the record a delete writes is the exact key GC reclaims (issue #364).
pub fn orphan_key(dserver: DServerId, frag: FragmentId) -> Vec<u8> {
    format!("orphan:{dserver}:{}:{}", frag.chunk, frag.index).into_bytes()
}

/// Parse an [`orphan_key`] back into its `(dserver, fragment)`, or `None` if `key` is
/// not a well-formed orphan-ledger key. The inverse GC uses to read the ledger.
pub fn parse_orphan_key(key: &[u8]) -> Option<(DServerId, FragmentId)> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix("orphan:")?;
    let mut parts = rest.splitn(3, ':');
    let dserver = parts.next()?.parse().ok()?;
    let chunk = parts.next()?.parse().ok()?;
    let index = parts.next()?.parse().ok()?;
    Some((dserver, FragmentId { chunk, index }))
}

/// Whether an inode's content is fully committed or still being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InodeState {
    /// Content not yet committed (chunks may be in the pending ledger).
    Pending,
    /// The chunk map is committed and readable.
    Committed,
}

/// The durability scheme a chunk is stored under (ADR-0008 mixed-era data: the
/// scheme is recorded per chunk, so chunks written under different schemes read
/// correctly through one path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EcScheme {
    /// A single fragment per chunk at index 0 (the M0 `replication(1)`/`none`
    /// behaviour).
    None,
    /// Reed-Solomon erasure coding: `k` data + `m` parity fragments per chunk
    /// (`k`/`m` are `u8` to match the v1 header's `ec_k`/`ec_m`).
    ReedSolomon {
        /// Data-fragment count.
        k: u8,
        /// Parity-fragment count.
        m: u8,
    },
}

/// One chunk in an inode's chunk map: its id, durability scheme, **logical length**
/// (the reader truncates to this after reconstruction, stripping shard padding), and
/// the **placement record** — the stable D-server holding each fragment.
///
/// `placement[i]` is the [`DServerId`] of the D server holding the fragment at index
/// `i` (proposal 0005, "The placement record", M3.1): recorded at the write commit
/// point and consumed by the read path **in place of** M2's stateless `index % n`, so
/// a fragment a custodian has *moved* is still resolved. It is **additive** metadata
/// on a never-yet-deployed schema (`#[serde(default)]`), so an inode written before
/// the field decodes with an empty vector and the read falls back to the identity
/// placement (M0–M2 read through the same path).
///
/// (Carrying a `Vec` makes `ChunkRef` no longer `Copy`; the chunk map is cloned
/// where ownership is needed.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    /// The chunk's id (shared by all its fragments).
    pub id: ChunkId,
    /// How the chunk is fragmented.
    pub scheme: EcScheme,
    /// The chunk's logical (pre-coding) length in bytes.
    pub len: u64,
    /// The stable D-server id holding each fragment, by fragment index (length `n`).
    /// Empty on a pre-M3 record; the read path then resolves by fragment index.
    #[serde(default)]
    pub placement: Vec<DServerId>,
}

impl ChunkRef {
    /// The total number of fragments this chunk has, derived from its EC scheme:
    /// `EcScheme::None` → 1; `EcScheme::ReedSolomon { k, m }` → `k + m`. This is
    /// the authoritative fragment count shared by the read path, GC, scrub, and
    /// reconstruction — the single source of truth for "how many fragments does this
    /// chunk have?"
    pub fn fragment_count(&self) -> u16 {
        match self.scheme {
            EcScheme::None => 1,
            EcScheme::ReedSolomon { k, m } => u16::from(k) + u16::from(m),
        }
    }

    /// The D server holding fragment `index` of this chunk, applying the
    /// **identity-placement fallback** for pre-M3 / mixed-era records whose
    /// `placement` vector is empty or shorter than `n` (decoded via
    /// `#[serde(default)]`): if `placement[index]` is absent, the fragment resolves
    /// to D-server `index`. This is the **single authoritative placement-resolution
    /// definition** for the read path (`read.rs:fragment_dserver`), GC
    /// (`gc.rs:referenced_fragments`), scrub, reconstruction
    /// (`reconstruction.rs:assess`), and rebalance (`rebalance.rs:plan_evacuations`),
    /// so placement semantics cannot drift across callers.
    pub fn placed_dserver(&self, index: u16) -> DServerId {
        self.placement
            .get(index as usize)
            .copied()
            .unwrap_or(u64::from(index))
    }

    /// Every fragment of this chunk, resolved to its holding D server: the full
    /// `0..fragment_count()` index space, each index resolved through
    /// [`Self::placed_dserver`] (ADR-0040 decision 1, the normative expansion rule).
    /// This is *the* "walk every fragment to its holding D-server" call (ADR-0040
    /// decision 2) — the single definition every read-expansion consumer draws from
    /// instead of open-coding `(0..fragment_count()).map(|i| placed_dserver(i))`
    /// itself: GC's `referenced_fragments` (`gc.rs`), reconstruction's `assess`
    /// (`reconstruction.rs`), and rebalance's `plan_evacuations` (`rebalance.rs`).
    ///
    /// Deliberately **liberal**, like `placed_dserver`: it applies the identity
    /// fallback unconditionally and does not validate `placement`'s length, so it is
    /// infallible and safe for the read path. A malformed (non-empty, wrong-length)
    /// vector is a maintenance-loop concern (ADR-0040 decisions 3–4) — classifying and
    /// rejecting one *before* expansion is a separate, fallible companion
    /// (`checked_fragments()` / `placement_is_valid()`, #348), not a property of this
    /// helper.
    pub fn fragments(&self) -> impl Iterator<Item = (u16, DServerId)> + '_ {
        (0..self.fragment_count()).map(move |i| (i, self.placed_dserver(i)))
    }

    /// Whether the committed `placement` vector is **well-formed** — the single
    /// classifier the maintenance loops share (ADR-0040 decision 3, the "liberal read,
    /// strict maintenance" boundary). A committed `placement` is valid **iff** it is
    /// **empty** (pre-M3 / mixed-era → identity fallback) **or** its length equals
    /// [`Self::fragment_count`] (an explicit full-length record). Any other non-empty
    /// length is **malformed**: no writer emits it (the write path always commits a
    /// full-length vector; `#[serde(default)]` only ever yields empty), so in practice
    /// it can only mean truncation or corruption.
    ///
    /// This is the strict counterpart to the deliberately liberal [`Self::fragments`]
    /// expansion (#348): the read path stays liberal via `fragments()`, while a
    /// maintenance loop consults this gate (or [`Self::checked_fragments`]) *before*
    /// expanding, so a malformed vector is never silently identity-filled.
    pub fn placement_is_valid(&self) -> bool {
        self.placement.is_empty() || self.placement.len() == self.fragment_count() as usize
    }

    /// The **strict** companion to [`Self::fragments`]: the same full-index-space
    /// expansion, but only **after** classifying the committed `placement` (ADR-0040
    /// decision 4). A valid vector (empty or full-length) expands exactly as
    /// `fragments()` does; a **malformed** one (non-empty, `len != fragment_count()`) is
    /// rejected with [`MalformedPlacement`] *before* any expansion, so no identity entry
    /// is ever fabricated for its missing tail.
    ///
    /// Every maintenance loop resolves committed placement through this gate — GC/scrub
    /// treat a malformed chunk as fully referenced and audit it; reconstruction/rebalance
    /// skip it and flag NEEDS-HUMAN — while the read path keeps using the infallible
    /// `fragments()` (availability first).
    pub fn checked_fragments(
        &self,
    ) -> std::result::Result<impl Iterator<Item = (u16, DServerId)> + '_, MalformedPlacement> {
        if self.placement_is_valid() {
            Ok(self.fragments())
        } else {
            Err(MalformedPlacement {
                expected: self.fragment_count(),
                actual: self.placement.len(),
            })
        }
    }
}

/// A committed `placement` vector classified as **malformed** by
/// [`ChunkRef::checked_fragments`] (ADR-0040 decision 3): non-empty but of a length
/// other than the chunk's [`ChunkRef::fragment_count`]. It carries the mismatch so a
/// maintenance loop can surface it as an operator signal (audit event / NEEDS-HUMAN).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedPlacement {
    /// The fragment count the chunk's [`EcScheme`] requires (`fragment_count()`).
    pub expected: u16,
    /// The actual length of the committed `placement` vector.
    pub actual: usize,
}

// ---------------------------------------------------------------------------
// Chunk-map segmentation (proposal 0016 decision 7(a), `0016:2314-2331`)
// ---------------------------------------------------------------------------
//
// A published chunk map is ONE metadata value, and a value has a size ceiling — 100 KB
// on FoundationDB, the tightest backend in play (`crates/traits/src/lib.rs:746-752`). A
// bare inline `Vec<ChunkRef>` therefore caps an object's chunk count far below the
// >10 GiB launch requirement. A large map is instead SEGMENTED: the root keeps the
// group identity plus one `SegmentRef` per segment, and segment `i`'s chunks live in
// the record `seg:<nonce>:<epoch>:<index>` (`0016:354`).
//
// The two shapes are discriminated by JSON type — a flat map is a JSON array, exactly
// the pre-existing encoding — so every legacy record decodes and re-encodes
// byte-identically and every `require(key, encode(prior))` CAS in this module keeps
// matching the bytes already in the store (see the `skip_serializing_if` rationale on
// `InodeRecord::etag`, `:277-286`).
//
// This slice lands the shape, its decode-time invariants and its `seg:`/`seggrp:` key
// helpers only — no resolver, no producer (#649 onward). So every pre-existing
// `.chunk_map` site in this module treats `ChunkMap::Segmented` as the typed error
// [`ChunkMapError::SegmentedMapUnsupported`], never as an empty chunk list: a consumer
// that cannot yet resolve a segmented map must fail closed for that object rather than
// answer "this object owns no chunks" (an answer indistinguishable from a genuinely
// empty object, and how a live object's fragments would go unreferenced).

/// The number of decimal digits a `seg:` key's segment index is zero-padded to, so the
/// key's byte-lexicographic order equals index order. [`parse_seg_key`] rejects any
/// other width rather than admitting two spellings of one segment.
pub const SEG_INDEX_WIDTH: usize = 6;

/// The largest segment index the `seg:` key grammar can address — the whole key space
/// [`SEG_INDEX_WIDTH`] opens, `999_999`.
///
/// This is a **format**-level bound, not a capacity policy like [`MAX_ROOT_SEGMENTS`],
/// and that is why it *is* enforced at decode ([`SegmentedMap::new`]) while the capacity
/// ceiling is not: a segment past it has no canonical key at all, so nothing — no
/// resolver, no GC pass, no reconstruction, at any capacity setting — could ever address
/// its record. Admitting such a root as a value would hand every consumer a map it can
/// only half-resolve, which is exactly the "this object owns no chunks" answer this
/// module refuses to give (C-1). Widening the key space is a stored-format change with a
/// migration, never a constant tweak.
pub const MAX_SEGMENT_INDEX: u32 = 10u32.pow(SEG_INDEX_WIDTH as u32) - 1;

/// The length of a segment-group nonce in lowercase-hex characters (128 bits).
pub const SEG_NONCE_HEX_LEN: usize = 32;

/// Key prefix for **segment records** — one segment of a published, segmented chunk map
/// (`0016:354`). Disjoint from every other namespace.
pub const SEG_PREFIX: &[u8] = b"seg:";

/// Key prefix for the **segment-group reservation** marker (`0016:499-527`).
pub const SEGGRP_PREFIX: &[u8] = b"seggrp:";

/// The value of a `seggrp:<nonce>` marker record: its **presence** is the whole
/// meaning, so the value is the empty JSON object.
pub const SEGGRP_MARKER: &[u8] = b"{}";

/// The most segments one root may name (`0016:2432-2440`).
///
/// Its **budget rule** is `0016:1467`: `max_segref_bytes × MAX_ROOT_SEGMENTS ≤ V / 2` —
/// a worst-case segment table fits [`MAX_ROOT_VALUE_BYTES`], i.e. HALF the value ceiling,
/// not merely inside it. The other half is the reserve the caller's object metadata and
/// any later field addition are spent from, so raising this constant means re-measuring
/// the encoded worst case against [`MAX_ROOT_VALUE_BYTES`]
/// (`crates/core/tests/segmented_map_record.rs` measures exactly that, on
/// `encode(...).len()`, with and without a reserve-filling metadata block).
///
/// A **capacity** guard, enforced where a segment table becomes work — the publication
/// that writes one and the ranged read that would spend it (#649/#653) — and
/// deliberately **not** at decode: rejecting a stored record on a derived capacity
/// constant would turn a durable object unreadable if the constant ever moved
/// (ADR-0045's liberal-on-read boundary). A stored table past this ceiling therefore
/// still decodes, and fails closed only when something tries to resolve it.
///
/// Contrast [`MAX_SEGMENT_INDEX`], which *is* a decode invariant: this constant is a
/// number this deployment chooses, that one is the addressable key space of the stored
/// format itself.
pub const MAX_ROOT_SEGMENTS: usize = 512;

/// The value ceiling every backend inherits — FoundationDB's, the tightest in play
/// (`crates/traits/src/lib.rs:746-752`). **Decimal**, not the binary rounding of "100
/// KB": FoundationDB's hard limit is 100 000 bytes, not `100 * 1024`.
pub const MAX_VALUE_BYTES: usize = 100_000;

/// The byte budget the **segment table and the root's own fields** must fit: half
/// [`MAX_VALUE_BYTES`], the 2× headroom `0016:1467` requires of [`MAX_ROOT_SEGMENTS`]
/// (`max_segref_bytes × MAX_ROOT_SEGMENTS ≤ V / 2`).
///
/// The **other half is a reserve**, and it is spent on things the record shape does not
/// choose: the ADR-0047 object metadata a client supplies (`etag`, `content_type`,
/// `modified` — `content_type` is verbatim from the request header, so its width is the
/// caller's), and whatever field a later revision adds. Sizing the segment table against
/// the *whole* ceiling instead would leave a root that is legal today and unwritable the
/// moment either grows — and a root that cannot be re-written is an object whose
/// placement can never be repaired (every repair is `require(inode, encode(prior)) +
/// put(inode, encode(next))`), so the headroom is a durability property rather than
/// tidiness.
///
/// The split is measured, not asserted in prose: `crates/core/tests/segmented_map_record.rs`
/// encodes a worst-case `MAX_ROOT_SEGMENTS` root and requires (a) the table root inside
/// this budget and (b) that same root carrying object metadata filling the whole reserve
/// still inside [`MAX_VALUE_BYTES`]. A record whose metadata exceeds the reserve is
/// refused by the tightest backend when it is *published* — a clean create failure, the
/// same one an equally large flat record already meets today, and not a durability
/// hazard: an object that was published fits, and repairs re-encode the same fields.
/// Bounding a caller-supplied header belongs to the protocol gateway, not to the record
/// shape. The `const` assertion below keeps the two halves tied if either is edited.
pub const MAX_ROOT_VALUE_BYTES: usize = 50_000;

const _: () = assert!(MAX_ROOT_VALUE_BYTES * 2 <= MAX_VALUE_BYTES);

/// A structural violation of the segmented chunk-map shape.
///
/// Every variant is raised **at decode** (a stored record is parsed into a value that
/// cannot be malformed — ADR-0045, parse-don't-validate) or by a caller that met
/// [`ChunkMap::Segmented`] at a site this slice has not wired a resolver for yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkMapError {
    /// A segment-group nonce that is not exactly [`SEG_NONCE_HEX_LEN`] lowercase hex
    /// characters — it would key a `seg:` range no writer can reproduce.
    NonceNotHex {
        /// The rejected nonce.
        nonce: String,
    },
    /// A segmented map naming no segments: an empty map is the flat shape, not this
    /// one.
    NoSegments,
    /// `segment_count` disagrees with the number of `segments` present.
    SegmentCountMismatch {
        /// The `segment_count` the record declares.
        declared: u32,
        /// How many `SegmentRef`s it actually carries.
        actual: usize,
    },
    /// The segment indices are not exactly `0..segment_count` in ascending order — a
    /// duplicate, a gap, or an out-of-order entry.
    SegmentIndexOutOfOrder {
        /// The position in the `segments` list.
        position: usize,
        /// The index found there.
        found: u32,
    },
    /// A segment index past [`MAX_SEGMENT_INDEX`] — the `seg:` key grammar cannot
    /// address it, so its record is unreachable for every consumer, forever. Because
    /// indices are exactly `0..segment_count`, this is equally the format's maximum
    /// **segment count**: a root naming more segments than the key space holds is
    /// rejected as a whole rather than decoded into a map only part of which resolves.
    SegmentIndexUnaddressable {
        /// The first index that has no canonical key.
        index: u32,
        /// The largest index the key space can address ([`MAX_SEGMENT_INDEX`]).
        max: u32,
    },
    /// The segments do not tile the object contiguously from byte 0: this one's
    /// `byte_offset` is not the end of its predecessor (a gap, an overlap, or a
    /// non-monotonic offset).
    SegmentsNotContiguous {
        /// The offending segment's index.
        index: u32,
        /// The offset the tiling requires.
        expected: u64,
        /// The offset the record carries.
        found: u64,
    },
    /// A segment covering no bytes — it can hold no chunk, so it can only be
    /// corruption.
    EmptySegment {
        /// The offending segment's index.
        index: u32,
    },
    /// A root segment table whose byte spans **overflow `u64`** when tiled.
    SegmentSpanOverflow {
        /// The segment at which the running offset overflowed.
        index: u32,
    },
    /// A **segment record** carrying no chunks, or no bytes.
    EmptySegmentRecord {
        /// The first byte of the object the empty record claimed to cover.
        byte_offset: u64,
        /// How many chunks it carried (0, or a list whose lengths sum to 0).
        chunks: usize,
    },
    /// A **segment record** whose own extent does not exist: `byte_offset + byte_len`
    /// overflows `u64`, so the record claims a last byte no offset can address.
    SegmentSpanUnrepresentable {
        /// The first byte the record claims.
        byte_offset: u64,
        /// The length it claims from there.
        byte_len: u64,
    },
    /// A segment record whose chunks' lengths do not sum to its declared `byte_len`.
    SegmentLengthMismatch {
        /// The declared byte length.
        declared: u64,
        /// The sum of the record's chunk lengths.
        chunks: u64,
    },
    /// A segment record whose chunk lengths **overflow `u64`** when summed. The sum is
    /// checked rather than wrapped: an unchecked aggregate would wrap in a release
    /// build to a small total that could then *match* a forged `byte_len` — admitting a
    /// structurally impossible record as a value.
    SegmentLengthOverflow {
        /// How many chunks the record carries.
        chunks: usize,
    },
    /// A key under the `seg:` prefix that is not a well-formed segment key (a
    /// wrong-width index, a missing field, a non-canonical epoch).
    SegmentKeyMalformed {
        /// The rejected key, lossily rendered.
        key: String,
    },
    /// A segmented root whose segment table does not span exactly
    /// [`InodeRecord::size`] bytes. The table is the object's byte index, so a
    /// disagreement is structural corruption, not a contextual detail.
    SizeSpanMismatch {
        /// The `size` the root declares.
        size: u64,
        /// The bytes its segment table actually spans.
        span: u64,
    },
    /// A caller met [`ChunkMap::Segmented`] at a `.chunk_map` site this slice has not
    /// wired a resolver for (#649-#651): nothing publishes a segmented map yet, so this
    /// is unreachable in production today, but every read site fails closed here rather
    /// than silently treating the map as empty.
    SegmentedMapUnsupported {
        /// The call site that met it, for diagnostics.
        operation: &'static str,
    },
}

impl fmt::Display for ChunkMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonceNotHex { nonce } => write!(
                f,
                "segment-group nonce is not {SEG_NONCE_HEX_LEN} lowercase hex characters: {nonce:?}"
            ),
            Self::NoSegments => write!(f, "segmented chunk map names no segments"),
            Self::SegmentCountMismatch { declared, actual } => write!(
                f,
                "segment_count {declared} disagrees with {actual} segments present"
            ),
            Self::SegmentIndexOutOfOrder { position, found } => write!(
                f,
                "segment index {found} at position {position}: indices must be 0..segment_count, ascending, with no gap or duplicate"
            ),
            Self::SegmentIndexUnaddressable { index, max } => write!(
                f,
                "segment index {index} exceeds the {SEG_INDEX_WIDTH}-digit `seg:` key space (max {max}): its record could never be addressed"
            ),
            Self::SegmentsNotContiguous {
                index,
                expected,
                found,
            } => write!(
                f,
                "segment {index} starts at byte {found}, not {expected}: segments must tile the object contiguously"
            ),
            Self::EmptySegment { index } => write!(f, "segment {index} covers no bytes"),
            Self::SegmentSpanOverflow { index } => write!(
                f,
                "segment table overflows u64 at segment {index}: the tiling cannot be represented"
            ),
            Self::EmptySegmentRecord {
                byte_offset,
                chunks,
            } => write!(
                f,
                "segment record at byte {byte_offset} carries {chunks} chunks covering no bytes"
            ),
            Self::SegmentSpanUnrepresentable {
                byte_offset,
                byte_len,
            } => write!(
                f,
                "a segment record at byte {byte_offset} claiming {byte_len} bytes ends past u64: its extent cannot be represented"
            ),
            Self::SegmentLengthMismatch { declared, chunks } => write!(
                f,
                "segment declares byte_len {declared} but its chunks total {chunks} bytes"
            ),
            Self::SegmentLengthOverflow { chunks } => write!(
                f,
                "segment's {chunks} chunk lengths overflow u64 when summed — rejected, never wrapped"
            ),
            Self::SegmentKeyMalformed { key } => write!(f, "malformed segment key: {key:?}"),
            Self::SizeSpanMismatch { size, span } => write!(
                f,
                "inode declares size {size} but its segment table spans {span} bytes"
            ),
            Self::SegmentedMapUnsupported { operation } => write!(
                f,
                "{operation} met a segmented chunk map, which this build cannot yet resolve"
            ),
        }
    }
}

impl std::error::Error for ChunkMapError {}

/// A **validated** segment-group nonce: exactly [`SEG_NONCE_HEX_LEN`] lowercase hex
/// characters, and therefore carrying none of the `:` separators the `seg:` key grammar
/// is built out of.
///
/// It is a type rather than a `&str` because the key helpers below **mint key ranges**
/// from it, and a range is what a cleanup pass deletes. Given a bare string,
/// `seg_group_prefix("<nonce>:<epoch>")` — a spelling nothing would have rejected —
/// renders `seg:<nonce>:<epoch>:`, which is byte-for-byte the *epoch* range
/// [`seg_range_prefix`] mints for that group's live generation. A pass sweeping "every
/// epoch of this group" would then delete a live generation's segment records, orphaning
/// every fragment they name: the permanent, data-losing failure mode C-1 forbids
/// (`docs/principles.md` §5). Parsing the rule into the type (ADR-0045,
/// parse-don't-validate) is what makes an unvalidated prefix unrepresentable rather than
/// merely unlikely — and the fixed width is what makes one group's prefix unable to
/// reach another's keys at all.
///
/// `Serialize` is `transparent`, so the stored form is the plain JSON string it always
/// was — the type is a compile-time rule, not a wire change. There is deliberately no
/// `Deserialize`: the only decode path is [`SegmentGroup`]'s, which routes through the
/// validating constructor, so no derive can produce one of these unvalidated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SegmentNonce(String);

impl SegmentNonce {
    /// The validating constructor — the **only** way to obtain a `SegmentNonce`, and the
    /// single home of the nonce rule (both [`SegmentGroup::new`] and [`parse_seg_key`]
    /// route through it, so a stored key and a stored record can never disagree about
    /// what a nonce is).
    pub fn new(nonce: impl Into<String>) -> std::result::Result<Self, ChunkMapError> {
        let nonce = nonce.into();
        if nonce.len() != SEG_NONCE_HEX_LEN
            || !nonce
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ChunkMapError::NonceNotHex { nonce });
        }
        Ok(Self(nonce))
    }

    /// The nonce as its 32 hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SegmentNonce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The identity of one **segment group**: an independent 128-bit nonce minted with the
/// publishing session, paired with the `Completing` fence **epoch** of the attempt that
/// wrote the segments (`0016:2352-2380`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SegmentGroup {
    nonce: SegmentNonce,
    epoch: u64,
}

impl SegmentGroup {
    /// The validating constructor — the **only** way to obtain a `SegmentGroup`, so a
    /// nonce that could not key a reproducible `seg:` range is never representable
    /// (ADR-0045, parse-don't-validate).
    pub fn new(nonce: impl Into<String>, epoch: u64) -> std::result::Result<Self, ChunkMapError> {
        Ok(Self {
            nonce: SegmentNonce::new(nonce)?,
            epoch,
        })
    }

    /// The group nonce (32 lowercase hex characters), validated — so it can be handed
    /// straight to [`seg_group_prefix`] / [`seggrp_key`].
    pub fn nonce(&self) -> &SegmentNonce {
        &self.nonce
    }

    /// The `Completing` fence epoch that wrote this generation's segments.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl<'de> Deserialize<'de> for SegmentGroup {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            nonce: String,
            epoch: u64,
        }
        let raw = Raw::deserialize(deserializer)?;
        SegmentGroup::new(raw.nonce, raw.epoch).map_err(DeError::custom)
    }
}

/// One segment of a published map, as the **root** records it: which segment it is and
/// the byte span of the object it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRef {
    /// The segment's index, which is also the fixed-width tail of its `seg:` key.
    pub index: u32,
    /// The first byte of the object this segment covers.
    pub byte_offset: u64,
    /// How many bytes it covers.
    pub byte_len: u64,
}

/// The **segmented** shape of an [`InodeRecord::chunk_map`]: the group identity plus
/// the ordered segment table (`0016:2314-2330`).
///
/// The **structural** invariants — `segment_count == segments.len()`, indices exactly
/// `0..count` in ascending order and inside the addressable key space
/// ([`MAX_SEGMENT_INDEX`], which is therefore also the format's segment-count maximum),
/// a contiguous byte tiling from 0 — are enforced by [`Self::new`], which the
/// `Deserialize` impl routes through, so a malformed stored record is an **error at
/// decode** and never a value a consumer could half-resolve. The **capacity** bound
/// ([`MAX_ROOT_SEGMENTS`]) deliberately is not one of them (see its doc comment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentedMap {
    group: SegmentGroup,
    segments: Vec<SegmentRef>,
}

impl SegmentedMap {
    /// The validating constructor. See the type's invariants.
    pub fn new(
        group: SegmentGroup,
        segments: Vec<SegmentRef>,
    ) -> std::result::Result<Self, ChunkMapError> {
        if segments.is_empty() {
            return Err(ChunkMapError::NoSegments);
        }
        let mut next_offset: u64 = 0;
        for (position, segment) in segments.iter().enumerate() {
            // The FORMAT's own maximum, checked FIRST — before the ordering rule, so an
            // unaddressable index is reported as what it is at any position, and so the
            // check is reachable without a million-entry table. A segment past the key
            // space has no canonical `seg:` key, so admitting the root would hand a
            // consumer a table whose tail it could never resolve. This is also the
            // format's maximum segment COUNT: indices are exactly `0..count`, so a root
            // naming more segments than the key space holds cannot get past here.
            // Unlike `MAX_ROOT_SEGMENTS` the bound is not a tunable, so enforcing it at
            // decode cannot strand a durable object.
            checked_segment_index(segment.index)?;
            if segment.index as usize != position {
                return Err(ChunkMapError::SegmentIndexOutOfOrder {
                    position,
                    found: segment.index,
                });
            }
            if segment.byte_offset != next_offset {
                return Err(ChunkMapError::SegmentsNotContiguous {
                    index: segment.index,
                    expected: next_offset,
                    found: segment.byte_offset,
                });
            }
            if segment.byte_len == 0 {
                return Err(ChunkMapError::EmptySegment {
                    index: segment.index,
                });
            }
            next_offset = next_offset.checked_add(segment.byte_len).ok_or(
                ChunkMapError::SegmentSpanOverflow {
                    index: segment.index,
                },
            )?;
        }
        Ok(Self { group, segments })
    }

    /// The bytes this table spans — the end of its last segment, which is also the
    /// object's `size` (checked at decode, [`ChunkMapError::SizeSpanMismatch`]). Never
    /// overflows: [`Self::new`] rejected a tiling that could not be represented.
    pub fn span(&self) -> u64 {
        self.segments
            .last()
            .map_or(0, |last| last.byte_offset.saturating_add(last.byte_len))
    }

    /// The group this map's segments are keyed by.
    pub fn group(&self) -> &SegmentGroup {
        &self.group
    }

    /// The ordered segment table.
    pub fn segments(&self) -> &[SegmentRef] {
        &self.segments
    }

    /// How many segments the map has — always `segments().len()`, which is what the
    /// encoded `segment_count` field carries.
    pub fn segment_count(&self) -> u32 {
        self.segments.len() as u32
    }

    /// Validate a decoded `(group, segment_count, segments)` triple into a map — the
    /// **whole** structural check of the segmented shape, in one place and returning a
    /// typed error. The `Deserialize` impl routes through it (stringifying the error,
    /// as serde's `Error::custom` requires).
    ///
    /// The format's **segment-count maximum** needs no separate test here: it is
    /// [`MAX_SEGMENT_INDEX`] + 1, and [`Self::new`] rejects the first index past that
    /// space — which a root exceeding the count must contain, since the indices are
    /// exactly `0..segment_count`.
    fn from_wire(
        group: SegmentGroup,
        segment_count: u32,
        segments: Vec<SegmentRef>,
    ) -> std::result::Result<Self, ChunkMapError> {
        if segment_count as usize != segments.len() {
            return Err(ChunkMapError::SegmentCountMismatch {
                declared: segment_count,
                actual: segments.len(),
            });
        }
        Self::new(group, segments)
    }
}

/// The wire shape of [`SegmentedMap`] on **encode**, field order included:
/// `{"group":{"nonce":…,"epoch":…},"segment_count":…,"segments":[…]}`.
#[derive(Serialize)]
struct SegmentedMapWireOut<'a> {
    group: &'a SegmentGroup,
    segment_count: u32,
    segments: &'a [SegmentRef],
}

/// The wire shape of [`SegmentedMap`] on **decode** — same fields, owned.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentedMapWireIn {
    group: SegmentGroup,
    segment_count: u32,
    segments: Vec<SegmentRef>,
}

impl Serialize for SegmentedMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        SegmentedMapWireOut {
            group: &self.group,
            segment_count: self.segment_count(),
            segments: &self.segments,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SegmentedMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = SegmentedMapWireIn::deserialize(deserializer)?;
        SegmentedMap::from_wire(wire.group, wire.segment_count, wire.segments)
            .map_err(DeError::custom)
    }
}

/// An inode's chunk map: the ordered chunk list itself (**flat**), or the segment table
/// that names it (**segmented**) — proposal 0016 decision 7(a).
///
/// Discriminated by JSON type, so the flat shape is **byte-identical to the pre-0016
/// encoding in both directions** and every `require(key, encode(prior))` CAS in this
/// module keeps matching the bytes already in the store. Making the two shapes one
/// value (rather than a flat list plus an optional sidecar) is what stops a consumer
/// from resolving one shape and silently seeing nothing in the other.
///
/// **A consumer never reads this field directly to get an object's chunks** once a
/// resolver exists (#649); until then, [`Self::as_flat`] is the only sanctioned read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkMap {
    /// The ordered chunk list inline — a JSON array, exactly as every record before
    /// proposal 0016 wrote it.
    Flat(Vec<ChunkRef>),
    /// The chunks live in `seg:<nonce>:<epoch>:<index>` records; the root carries the
    /// group identity and the segment table.
    Segmented(SegmentedMap),
}

impl ChunkMap {
    /// The inline chunk list, or `None` when the map is segmented (whose chunks live in
    /// `seg:` records this slice has no resolver for yet).
    pub fn as_flat(&self) -> Option<&[ChunkRef]> {
        match self {
            Self::Flat(chunks) => Some(chunks),
            Self::Segmented(_) => None,
        }
    }

    /// The inline chunk list **by value**, or `None` when the map is segmented — the
    /// owning counterpart of [`Self::as_flat`] for a consumer that holds the map (a
    /// streamed read moving the list into its reader task) and would otherwise deep-clone
    /// every placement vector just to get an owned copy.
    pub fn into_flat(self) -> Option<Vec<ChunkRef>> {
        match self {
            Self::Flat(chunks) => Some(chunks),
            Self::Segmented(_) => None,
        }
    }

    /// The segment table, or `None` when the map is flat.
    pub fn segmented(&self) -> Option<&SegmentedMap> {
        match self {
            Self::Flat(_) => None,
            Self::Segmented(map) => Some(map),
        }
    }

    /// Whether the map is segmented.
    pub fn is_segmented(&self) -> bool {
        matches!(self, Self::Segmented(_))
    }
}

impl Default for ChunkMap {
    fn default() -> Self {
        Self::Flat(Vec::new())
    }
}

impl From<Vec<ChunkRef>> for ChunkMap {
    /// The one-line conversion every **flat** construction site goes through, so the
    /// shape change stays mechanical at the call sites.
    fn from(chunks: Vec<ChunkRef>) -> Self {
        Self::Flat(chunks)
    }
}

impl Serialize for ChunkMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Flat(chunks) => chunks.serialize(serializer),
            Self::Segmented(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ChunkMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ByJsonType;

        impl<'de> Visitor<'de> for ByJsonType {
            type Value = ChunkMap;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a flat chunk array or a segmented chunk-map object")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                seq: A,
            ) -> std::result::Result<ChunkMap, A::Error> {
                Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
                    .map(ChunkMap::Flat)
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<ChunkMap, A::Error> {
                Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(ChunkMap::Segmented)
            }
        }

        deserializer.deserialize_any(ByJsonType)
    }
}

/// One **segment record** (`seg:<nonce>:<epoch>:<index>`, `0016:354`): that segment's
/// chunks and the byte span of the object they cover.
///
/// The `byte_len == sum(chunk.len)` invariant is enforced at decode, so the root's
/// segment table and the record can never disagree about how much of the object a
/// segment holds. The fields are **private** and there is no field-wise constructor:
/// the invariant holds for every value that exists (ADR-0045 / parse-don't-validate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    chunks: Vec<ChunkRef>,
    byte_offset: u64,
    byte_len: u64,
}

impl SegmentRecord {
    /// Build a segment record over `chunks` starting at `byte_offset`, deriving
    /// `byte_len` from the chunks themselves.
    ///
    /// **Checked**, not summed: a chunk list whose lengths overflow `u64` is rejected
    /// ([`ChunkMapError::SegmentLengthOverflow`]) rather than wrapping in a release
    /// build — a wrapped total is a `byte_len` the decode check would then happily
    /// confirm.
    pub fn new(
        chunks: Vec<ChunkRef>,
        byte_offset: u64,
    ) -> std::result::Result<Self, ChunkMapError> {
        let byte_len = checked_chunk_bytes(&chunks)?;
        Self::checked(chunks, byte_offset, byte_len)
    }

    /// The record's structural invariants, in one place: the chunk lengths total
    /// `byte_len` (the caller has already derived or read it), the segment is **not
    /// empty**, and the span it claims — `byte_offset + byte_len` — is representable.
    fn checked(
        chunks: Vec<ChunkRef>,
        byte_offset: u64,
        byte_len: u64,
    ) -> std::result::Result<Self, ChunkMapError> {
        if chunks.is_empty() || byte_len == 0 {
            return Err(ChunkMapError::EmptySegmentRecord {
                byte_offset,
                chunks: chunks.len(),
            });
        }
        if byte_offset.checked_add(byte_len).is_none() {
            return Err(ChunkMapError::SegmentSpanUnrepresentable {
                byte_offset,
                byte_len,
            });
        }
        Ok(Self {
            chunks,
            byte_offset,
            byte_len,
        })
    }

    /// Validate a decoded `(chunks, byte_offset, byte_len)` triple into a record — the
    /// decode's whole structural check, returning a typed error.
    fn from_wire(
        chunks: Vec<ChunkRef>,
        byte_offset: u64,
        byte_len: u64,
    ) -> std::result::Result<Self, ChunkMapError> {
        let total = checked_chunk_bytes(&chunks)?;
        if total != byte_len {
            return Err(ChunkMapError::SegmentLengthMismatch {
                declared: byte_len,
                chunks: total,
            });
        }
        Self::checked(chunks, byte_offset, byte_len)
    }

    /// This segment's ordered chunks.
    pub fn chunks(&self) -> &[ChunkRef] {
        &self.chunks
    }

    /// The chunks, consumed.
    pub fn into_chunks(self) -> Vec<ChunkRef> {
        self.chunks
    }

    /// The first byte of the object this segment covers.
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    /// How many bytes it covers — the sum of its chunks' lengths.
    pub fn byte_len(&self) -> u64 {
        self.byte_len
    }
}

/// The total byte length of `chunks`, **checked**: overflow is an error, never a wrap.
/// One definition, used by both the constructor and the decode check, so the two can
/// never disagree about what "the chunks total" means.
fn checked_chunk_bytes(chunks: &[ChunkRef]) -> std::result::Result<u64, ChunkMapError> {
    chunks
        .iter()
        .try_fold(0u64, |total, chunk| total.checked_add(chunk.len))
        .ok_or(ChunkMapError::SegmentLengthOverflow {
            chunks: chunks.len(),
        })
}

/// The wire shape of [`SegmentRecord`], field order included.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecordWire {
    chunks: Vec<ChunkRef>,
    byte_offset: u64,
    byte_len: u64,
}

impl Serialize for SegmentRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        SegmentRecordWire {
            chunks: self.chunks.clone(),
            byte_offset: self.byte_offset,
            byte_len: self.byte_len,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SegmentRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = SegmentRecordWire::deserialize(deserializer)?;
        SegmentRecord::from_wire(wire.chunks, wire.byte_offset, wire.byte_len)
            .map_err(DeError::custom)
    }
}

/// Key for one segment record: `seg:<nonce>:<epoch>:<index>`, the index zero-padded to
/// [`SEG_INDEX_WIDTH`] digits.
///
/// **Fallible**, because the padding is not a formatting nicety: an index past
/// [`MAX_SEGMENT_INDEX`] would render `SEG_INDEX_WIDTH + 1` digits, which
/// [`parse_seg_key`] then rejects — a key that writes but never reads back. Refusing it
/// here keeps `parse_seg_key(seg_key(g, i)?) == (nonce, epoch, i)` total over every key
/// this module can produce. Decode enforces the same bound
/// ([`ChunkMapError::SegmentIndexUnaddressable`]), so a table that reached a value has
/// no index this can refuse.
pub fn seg_key(group: &SegmentGroup, index: u32) -> std::result::Result<Vec<u8>, ChunkMapError> {
    checked_segment_index(index)?;
    let mut key = seg_range_prefix(group);
    key.extend_from_slice(format!("{index:0width$}", width = SEG_INDEX_WIDTH).as_bytes());
    Ok(key)
}

/// The addressability rule for a segment index, **in one place**: an index the `seg:`
/// key grammar can both render at [`SEG_INDEX_WIDTH`] digits and parse back.
///
/// One definition, used by [`seg_key`] (which must not mint a key that reads back as
/// malformed) and by [`SegmentedMap::new`] (which must not admit a table naming a segment
/// no key can reach), so the two can never disagree about what "addressable" means.
fn checked_segment_index(index: u32) -> std::result::Result<(), ChunkMapError> {
    if index > MAX_SEGMENT_INDEX {
        return Err(ChunkMapError::SegmentIndexUnaddressable {
            index,
            max: MAX_SEGMENT_INDEX,
        });
    }
    Ok(())
}

/// The **bounded per-object range** a segmented map resolves through:
/// `seg:<nonce>:<epoch>:` (`0016:2463-2469`). Never a global `seg:` scan.
pub fn seg_range_prefix(group: &SegmentGroup) -> Vec<u8> {
    format!("seg:{}:{}:", group.nonce(), group.epoch()).into_bytes()
}

/// The prefix naming **every** epoch of one segment group: `seg:<nonce>:`.
///
/// Takes a [`SegmentNonce`], not a string, because this is the range a cleanup pass
/// deletes: a nonce carrying a `:` would render another generation's *epoch* range
/// (`seg:<nonce>:<epoch>:`) and take a live group's segments with it. See
/// [`SegmentNonce`].
pub fn seg_group_prefix(nonce: &SegmentNonce) -> Vec<u8> {
    format!("seg:{nonce}:").into_bytes()
}

/// Key for a segment-group reservation marker: `seggrp:<nonce>`.
pub fn seggrp_key(nonce: &SegmentNonce) -> Vec<u8> {
    format!("seggrp:{nonce}").into_bytes()
}

/// Parse a [`seg_key`] back into `(nonce, epoch, index)`, **strictly**: the index must
/// be exactly [`SEG_INDEX_WIDTH`] ASCII digits, so one segment has exactly one key and
/// a stray record cannot smuggle itself into a resolution under a second spelling.
pub fn parse_seg_key(key: &[u8]) -> std::result::Result<(SegmentNonce, u64, u32), ChunkMapError> {
    let malformed = || ChunkMapError::SegmentKeyMalformed {
        key: String::from_utf8_lossy(key).into_owned(),
    };
    let rest = std::str::from_utf8(key)
        .ok()
        .and_then(|k| k.strip_prefix("seg:"))
        .ok_or_else(malformed)?;
    let mut parts = rest.split(':');
    let nonce = parts.next().ok_or_else(malformed)?;
    let epoch = parts.next().ok_or_else(malformed)?;
    let index = parts.next().ok_or_else(malformed)?;
    if parts.next().is_some() || index.len() != SEG_INDEX_WIDTH {
        return Err(malformed());
    }
    // The parsed nonce comes back VALIDATED, so what a caller derives from a stored key
    // — the group's `seg:` range, its `seggrp:` marker — cannot be minted from a
    // spelling this grammar would have refused.
    let nonce = SegmentNonce::new(nonce).map_err(|_| malformed())?;
    // The epoch is CANONICAL decimal, parsed strictly rather than through `from_str`:
    // `u64::from_str` accepts `+7` and `007`, so a segment could be addressed by keys
    // that differ in bytes but agree in value — two spellings of one segment, which is
    // exactly what the fixed-width index rule exists to forbid.
    let epoch = parse_canonical_u64(epoch).ok_or_else(malformed)?;
    if !index.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    let index: u32 = index.parse().map_err(|_| malformed())?;
    Ok((nonce, epoch, index))
}

/// A `u64` in **canonical** decimal: ASCII digits only (no sign), and no leading zero
/// unless the value *is* `0`. `None` for every other spelling.
fn parse_canonical_u64(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if text.len() > 1 && text.starts_with('0') {
        return None;
    }
    text.parse().ok()
}

/// Object metadata surfaced on the wire beyond byte size (ADR-0047): the content
/// `etag`, the client's declared `content_type`, and the content-publication time
/// (`modified`). Set together at **content publication** (create / overwrite) and
/// **preserved** across reconstruction/backfill commits, so a repair never moves
/// `Last-Modified` or drops the content type. Every field is optional so a record
/// written before this model — or by a path that has no value to record — degrades on
/// the wire to the pre-metadata behaviour (no ETag, `application/octet-stream`) rather
/// than to an error. `x-amz-meta-*` user metadata is deliberately not modelled here; the
/// flat shape leaves room to add it later.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The content digest as an opaque change-token: the lowercase-hex SHA-256 of the
    /// object bytes (ADR-0047; **not** MD5). Rendered quoted on the wire as S3's `ETag`.
    pub etag: Option<String>,
    /// The `Content-Type` the writing client declared, round-tripped verbatim.
    pub content_type: Option<String>,
    /// Content-publication time in epoch milliseconds; rendered RFC-7231 IMF-fixdate
    /// as `Last-Modified` on the wire.
    pub modified: Option<u64>,
}

/// An inode: attributes, the ordered chunk map, state, and version.
///
/// Decoding goes through [`InodeRecordWire`] so one **cross-field** structural
/// invariant is enforced at decode rather than admitted as a value (ADR-0045,
/// parse-don't-validate): a **segmented** map's segment table must span exactly
/// `size` bytes. A flat map keeps today's liberal treatment: its chunk list is the
/// bytes, so there is no second statement to disagree with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "InodeRecordWire")]
pub struct InodeRecord {
    /// Logical content length in bytes.
    pub size: u64,
    /// The ordered chunks making up the content — inline (**flat**) or named by a
    /// segment table (**segmented**, proposal 0016 decision 7). Read it through
    /// [`ChunkMap::as_flat`] until a resolver exists (#649); never treat
    /// [`ChunkMap::Segmented`] as an empty list.
    pub chunk_map: ChunkMap,
    /// Commit state.
    pub state: InodeState,
    /// Monotonic per-inode version; the commit point bumps it under CAS.
    pub version: u64,
    /// The content digest (opaque change-token), quoted as S3's `ETag` on the wire.
    /// `Option` + `#[serde(default)]` for stored-record compatibility (ADR-0047): a
    /// record written before this field decodes with `None`. Set only at content
    /// publication; preserved across reconstruction/backfill.
    ///
    /// `skip_serializing_if` is **load-bearing**, not cosmetic: every CAS commit in
    /// this module (`require(key, encode(prior))`) compares the RE-ENCODED prior
    /// record byte-for-byte against the bytes still in the store. A legacy record
    /// decodes these fields to `None`; serializing that as `"etag":null` could never
    /// equal the stored legacy JSON, so every overwrite and every
    /// backfill/reconstruction/rebalance of a pre-ADR-0047 object would return
    /// `Conflict` forever. Skipping `None` makes decode→encode the identity on
    /// legacy bytes, so the CAS sees exactly what the store holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The client's declared `Content-Type`, round-tripped verbatim. `Option` +
    /// `#[serde(default)]` for stored-record compatibility; falls back to
    /// `application/octet-stream` on the wire when absent. `skip_serializing_if`:
    /// see `etag` — required for the CAS round trip on legacy records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Content-publication time (epoch millis), rendered `Last-Modified` on the wire.
    /// `Option` + `#[serde(default)]` for stored-record compatibility.
    /// `skip_serializing_if`: see `etag` — required for the CAS round trip on legacy
    /// records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified: Option<u64>,
}

/// The wire shape of [`InodeRecord`] — identical field-for-field, so decoding is
/// unchanged for every record ever written; it exists only to give the decode a
/// place to enforce the size-vs-segment-table invariant before the value exists.
#[derive(Deserialize)]
struct InodeRecordWire {
    size: u64,
    chunk_map: ChunkMap,
    state: InodeState,
    version: u64,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    modified: Option<u64>,
}

impl TryFrom<InodeRecordWire> for InodeRecord {
    type Error = ChunkMapError;

    fn try_from(wire: InodeRecordWire) -> std::result::Result<Self, ChunkMapError> {
        let record = Self {
            size: wire.size,
            chunk_map: wire.chunk_map,
            state: wire.state,
            version: wire.version,
            etag: wire.etag,
            content_type: wire.content_type,
            modified: wire.modified,
        };
        record.checked_shape()?;
        Ok(record)
    }
}

impl InodeRecord {
    /// The record's **cross-field structural invariant**, in one place: a segmented
    /// map's segment table spans exactly `size`. Both the decode (via
    /// `TryFrom<InodeRecordWire>`) and [`InodeRecord::new_empty`]'s callers go through
    /// the same rule, so a value this module refuses to *read* is one no committer
    /// here can leave unreadable behind it.
    fn checked_shape(&self) -> std::result::Result<(), ChunkMapError> {
        if let ChunkMap::Segmented(map) = &self.chunk_map {
            let span = map.span();
            if span != self.size {
                return Err(ChunkMapError::SizeSpanMismatch {
                    size: self.size,
                    span,
                });
            }
        }
        Ok(())
    }

    /// The gate **every durable inode write in this module passes** before the record
    /// reaches a [`WriteBatch`] — the write-side mirror of the decode.
    ///
    /// `size` and `chunk_map` are independent public fields and `Serialize` is derived
    /// (it must stay derived: the flat encoding is byte-identical to what is already
    /// stored, `:277-286`), so a caller *can* hand [`create`] a record whose segment
    /// table disagrees with `size`. Encoding that record would put bytes in the store
    /// that this very type refuses to decode — a permanently unreadable object, which is
    /// precisely the failure mode C-1 forbids. So the check happens where the record
    /// becomes durable:
    ///
    /// 1. [`Self::checked_shape`] — never persist what cannot be read back; then
    /// 2. the segmented shape has **no producer in this build** (#653 lands the staged
    ///    publication committer that writes the `seg:` records first). A root published
    ///    without them names segments that do not exist, so it is refused here rather
    ///    than written and half-resolved later.
    ///
    /// Both steps report through distinct variants — [`ChunkMapError::SizeSpanMismatch`]
    /// and [`ChunkMapError::SegmentedMapUnsupported`] — so dropping either is visible,
    /// and #653 lifts only step 2.
    ///
    /// [`create`] and [`create_leased`] are the sites that take a **caller-built**
    /// record, so they are the sites that call this. The `commit_chunk_map*` helpers
    /// build their own `next` from a `Vec<ChunkRef>`, which is [`ChunkMap::Flat`] by
    /// construction and has no cross-field invariant to break; what they guard instead
    /// is the **`prior` they supersede**, which may be any stored shape.
    fn checked_for_publication(
        &self,
        operation: &'static str,
    ) -> std::result::Result<(), ChunkMapError> {
        self.checked_shape()?;
        if self.chunk_map.is_segmented() {
            return Err(ChunkMapError::SegmentedMapUnsupported { operation });
        }
        Ok(())
    }

    /// A freshly-created, empty inode at version 1, awaiting content.
    pub fn new_empty() -> Self {
        Self {
            size: 0,
            chunk_map: ChunkMap::default(),
            state: InodeState::Pending,
            version: 1,
            etag: None,
            content_type: None,
            modified: None,
        }
    }

    /// The object metadata carried on this record (ADR-0047), collected into an
    /// [`ObjectMeta`] for the wire layer.
    pub fn object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            etag: self.etag.clone(),
            content_type: self.content_type.clone(),
            modified: self.modified,
        }
    }
}

impl Default for InodeRecord {
    /// The empty inode ([`InodeRecord::new_empty`]) — so struct-update construction
    /// (`InodeRecord { size, chunk_map, state, version, ..Default::default() }`) fills
    /// the optional metadata fields with `None` at the many call sites that do not set
    /// object metadata.
    fn default() -> Self {
        Self::new_empty()
    }
}

/// A directory entry: the inode a name binds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentRecord {
    /// The inode this name resolves to.
    pub inode: InodeId,
}

/// A pending-chunk ledger entry: a lease on a provisionally-written chunk id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    /// When the lease expires (logical milliseconds); a custodian sweep may
    /// reclaim the chunk after this.
    pub lease_expiry_millis: u64,
}

/// Encode a record to its stored bytes. Serialization of these plain structs is
/// infallible.
pub fn encode<T: Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).expect("metadata record serialization is infallible"))
}

/// Decode a record from stored bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Atomically create an inode and the dirent that names it. Fails with
/// [`CommitOutcome::Conflict`] if the name (or the inode id) already exists, so a
/// just-created file is never duplicated or clobbered.
///
/// Errors (before touching the store) on a record this build must not make durable —
/// see [`InodeRecord::checked_for_publication`].
pub async fn create(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    id: InodeId,
    record: &InodeRecord,
) -> Result<CommitOutcome> {
    record.checked_for_publication("create")?;
    let batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), encode(record))
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
    store.commit(batch).await
}

/// Like [`create`], but the inode + dirent are published only if every chunk in
/// `pending_chunks` still holds a **live, unexpired** `pending:<id>` lease at `now_millis`,
/// enforced **atomically** with the create (issue #490). This is phase 3 of a **streaming**
/// write: an early chunk's fragments are protected from the custodian GC only by their pending
/// lease until the commit publishes the inode, so a commit that outran the lease (a stall past
/// the TTL after the last chunk, or between `stream_write_data` returning and the caller
/// driving this commit) must fail closed rather than publish an object over bytes the GC may
/// reclaim.
///
/// The per-chunk `require(pending_key, read-back-value)` preconditions ride in the **same**
/// [`WriteBatch`] as the create ([`live_lease_guards`]), so a sweep that reclaims a lease
/// between the read-back and the commit yields [`CommitOutcome::Conflict`], never a publish;
/// an already-absent or already-lapsed lease refuses up front with the same `Conflict`.
/// [`create`] is this with no leases to guard.
pub async fn create_leased(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    id: InodeId,
    record: &InodeRecord,
    pending_chunks: &[ChunkId],
    now_millis: u64,
) -> Result<CommitOutcome> {
    record.checked_for_publication("create_leased")?;
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let mut batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), encode(record))
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
    for (key, value) in guards {
        batch = batch.require(key, value);
    }
    store.commit(batch).await
}

/// Rename: move a name binding in a single dirent mutation. The inode is
/// untouched. Fails with [`CommitOutcome::Conflict`] if the source moved
/// concurrently or the target name is taken; returns `Conflict` if the source
/// does not exist.
pub async fn rename(
    store: &impl MetadataStore,
    old_parent: InodeId,
    old_name: &str,
    new_parent: InodeId,
    new_name: &str,
) -> Result<CommitOutcome> {
    let old_key = dirent_key(old_parent, old_name);
    let Some(current) = store.get(&old_key).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let batch = WriteBatch::new()
        .require(old_key.clone(), current.clone()) // source unchanged since read
        .require_absent(dirent_key(new_parent, new_name)) // target free
        .delete(old_key)
        .put(dirent_key(new_parent, new_name), current);
    store.commit(batch).await
}

/// The result of an [`unlink`] attempt on a bound name: the commit `outcome` and, when
/// the dirent resolved to one, the `inode` record that was removed — so the caller can
/// reclaim exactly that object's chunk fragments on a winning commit (issue #364).
#[derive(Debug, Clone)]
pub struct Unlinked {
    /// Whether the removal committed or lost a compare-and-set to a racing writer.
    pub outcome: CommitOutcome,
    /// The inode the removed dirent pointed at (`None` only for a dangling dirent).
    pub inode: Option<InodeRecord>,
}

/// Atomically remove a name binding and the inode it resolves to — the metadata
/// half of an S3 DELETE (issue #364). Returns `Ok(None)` if the name is already
/// unbound (an idempotent no-op the caller reports as success), else an [`Unlinked`]
/// carrying the commit outcome and the removed inode.
///
/// Compare-and-set on **both** the dirent and the inode so a delete racing an
/// overwrite (which replaces the inode) or a concurrent delete loses with
/// [`CommitOutcome::Conflict`] rather than removing a record a racing writer just
/// changed — the caller retries or treats an already-absent key as success so the
/// *observable* DELETE is idempotent (S3's 204).
///
/// This removes the **metadata** records **and**, in the *same atomic commit*, writes an
/// **orphan grace record** ([`orphan_key`], value `orphaned_at_millis`) for every fragment
/// the removed object placed — keyed by the **D-server the chunk map actually placed it on**
/// ([`ChunkRef::fragments`]), the placement-aware address GC reclaims from. The fragment bytes
/// are **not** reclaimed eagerly on the delete path: they are left under the orphan ledger for
/// the custodian **GC** (`crates/custodian/src/gc.rs`) to reclaim once the reader-safe grace
/// window elapses (proposal 0005, `0005:288-295`), so a concurrent reader still streaming the
/// prior object from those fragments is never torn mid-read (a GET during a DELETE completes
/// intact). Because the records are durable the instant the object becomes unreferenced, a
/// crash never strands the bytes forever either. This is a *real* backstop, not the
/// pending-ledger sweep: the **pending sweep**
/// ([`sweep_pending`] / [`sweep_expired_leases`]) scans `pending:` lease keys only, and a
/// committed object's fragments carry no pending entry, so without the orphan record GC would
/// see an unreferenced-but-undeadlined fragment and conservatively keep it forever
/// (`gc.rs:reconcile`) — the crash-leak this record closes (issue #364).
///
/// `orphaned_at_millis` is the caller's logical clock; GC honours the grace window relative
/// to it. On a lost CAS ([`CommitOutcome::Conflict`]) the whole batch rolls back, so no
/// orphan record is written for a delete that did not remove the object.
pub async fn unlink(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    orphaned_at_millis: u64,
) -> Result<Option<Unlinked>> {
    let dirent_key = dirent_key(parent, name);
    let Some(dirent_bytes) = store.get(&dirent_key).await? else {
        return Ok(None);
    };
    let dirent: DirentRecord = decode(&dirent_bytes)?;
    let inode_key = inode_key(dirent.inode);
    let inode_bytes = store.get(&inode_key).await?;
    let inode = inode_bytes
        .as_ref()
        .map(|bytes| decode::<InodeRecord>(bytes))
        .transpose()?;

    let mut batch = WriteBatch::new()
        .require(dirent_key.clone(), dirent_bytes)
        .delete(dirent_key)
        .delete(inode_key.clone());
    batch = match inode_bytes {
        Some(bytes) => batch.require(inode_key, bytes),
        None => batch.require_absent(inode_key),
    };
    // Grace-record every fragment the removed object placed, in the SAME atomic commit
    // that unbinds it (placement-aware: keyed by the D-server the chunk map placed the
    // fragment on, not `index`), so GC can reclaim it after a crash before the eager
    // reclaim runs.
    if let Some(inode) = &inode {
        let chunks = inode
            .chunk_map
            .as_flat()
            .ok_or(ChunkMapError::SegmentedMapUnsupported {
                operation: "unlink",
            })?;
        for chunk in chunks {
            for (index, dserver) in chunk.fragments() {
                let frag = FragmentId {
                    chunk: chunk.id,
                    index,
                };
                batch = batch.put(
                    orphan_key(dserver, frag),
                    orphaned_at_millis.to_string().into_bytes(),
                );
            }
        }
    }
    let outcome = store.commit(batch).await?;
    Ok(Some(Unlinked { outcome, inode }))
}

/// Commit a chunk map and size onto an inode at the commit point, bumping its
/// version **conditional on the prior record** (full-value compare-and-set). A
/// writer holding a stale `prior` loses with [`CommitOutcome::Conflict`];
/// exactly one concurrent writer wins.
///
/// A **segmented** `prior` is refused rather than replaced: its chunks live in `seg:`
/// records keyed by the prior generation's group, and this build has no resolver to
/// enumerate them and no committer to retire them (#649/#653). Overwriting the root with
/// a flat map would leave those records — and the fragments they name — referenced by
/// nothing, which is the unreferenced-live-bytes failure C-1 forbids. The superseding
/// commits below fail closed on the same shape through [`ChunkMap::as_flat`], since they
/// must additionally orphan every fragment the prior map placed.
pub async fn commit_chunk_map(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
) -> Result<CommitOutcome> {
    if prior.chunk_map.is_segmented() {
        return Err(ChunkMapError::SegmentedMapUnsupported {
            operation: "commit_chunk_map",
        }
        .into());
    }
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: prior.version + 1,
        // Reconstruction/backfill re-commits the SAME content, so it PRESERVES the
        // publication metadata (ADR-0047): a repair must not move `Last-Modified` or
        // drop the content type. Only the superseding commits below set new metadata.
        ..prior.clone()
    };
    let key = inode_key(id);
    let batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    store.commit(batch).await
}

/// Commit a new chunk map onto an inode (an object-content **overwrite**), CAS-conditional
/// on `prior`, **and** orphan every fragment the *prior* chunk map placed — in the *same
/// atomic batch*. This is the overwrite counterpart of the orphan grace records [`unlink`]
/// writes for a DELETE (issue #364, PUT-overwrite reclaim): the superseded fragments become
/// unreferenced the instant the new map wins, so a crash *after* the CAS never strands the
/// prior object's bytes — the custodian **GC** (`crates/custodian/src/gc.rs`) reclaims each
/// recorded orphan once the reader-safe grace window elapses (proposal 0005, `0005:288-295`).
///
/// Reclaim is left to GC (not done eagerly) precisely so a concurrent reader still holding the
/// prior chunk map reads its fragments intact within the grace window — the same reader-safe
/// discipline that keeps a GET during a DELETE from being truncated. The prior fragments are
/// orphaned by their **placed** D-server ([`ChunkRef::fragments`]), the address GC reclaims
/// from. [`commit_chunk_map`] (used by reconstruction/backfill, which *keep* the fragments and
/// only re-place them) is deliberately left non-orphaning — only a content overwrite
/// supersedes the bytes.
///
/// A `Conflict` (a stale writer lost the CAS) rolls the whole batch back, so no orphan record
/// is ever written for an overwrite that did not win.
pub async fn commit_chunk_map_superseding(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
    orphaned_at_millis: u64,
    meta: &ObjectMeta,
) -> Result<CommitOutcome> {
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: prior.version + 1,
        // A content **overwrite** is a fresh publication (ADR-0047), so it stamps the new
        // object metadata (digest / content type / publication time) rather than carrying
        // the prior version's forward.
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone(),
        modified: meta.modified,
    };
    let key = inode_key(id);
    let mut batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    for chunk in prior
        .chunk_map
        .as_flat()
        .ok_or(ChunkMapError::SegmentedMapUnsupported {
            operation: "commit_chunk_map_superseding",
        })?
    {
        for (index, dserver) in chunk.fragments() {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            batch = batch.put(
                orphan_key(dserver, frag),
                orphaned_at_millis.to_string().into_bytes(),
            );
        }
    }
    store.commit(batch).await
}

/// Like [`commit_chunk_map_superseding`], but the overwrite CAS lands only if every chunk in
/// `pending_chunks` still holds a **live, unexpired** `pending:<id>` lease at `now_millis`,
/// enforced **atomically** with the inode CAS and the prior fragments' orphaning (issue #490).
/// This is phase 3 of a **streaming overwrite**: the new version's chunks are protected from
/// the custodian GC only by their pending leases until this commit publishes them, so a commit
/// that outran a lease (a stall past the TTL after the last chunk, or between
/// `stream_write_data` returning and the caller driving this commit) must fail closed rather
/// than publish an object over bytes the GC may reclaim.
///
/// The per-chunk `require(pending_key, read-back-value)` preconditions ride in the **same**
/// [`WriteBatch`] as the CAS and every `orphan:` record ([`live_lease_guards`]), so a sweep
/// that reclaims a lease between the read-back and the commit yields [`CommitOutcome::Conflict`]
/// — never a publish, and never a stranded orphan record — and an already-absent or
/// already-lapsed lease refuses up front with the same `Conflict`.
/// [`commit_chunk_map_superseding`] is this with no leases to guard.
///
/// **Shape first, lease second.** A segmented `prior` is refused *before* the leases are
/// read, so the caller gets [`ChunkMapError::SegmentedMapUnsupported`] at every lease
/// state. Checking the lease first would report an unresolvable shape as
/// [`CommitOutcome::Conflict`] whenever the lease had also lapsed — and a `Conflict` is
/// the *retry* answer (a racing writer won; re-read and try again), so a caller obeying it
/// would spin against a shape no retry can fix, instead of failing closed for that object.
#[allow(clippy::too_many_arguments)]
pub async fn commit_chunk_map_superseding_leased(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
    orphaned_at_millis: u64,
    pending_chunks: &[ChunkId],
    now_millis: u64,
    meta: &ObjectMeta,
) -> Result<CommitOutcome> {
    let prior_chunks = prior
        .chunk_map
        .as_flat()
        .ok_or(ChunkMapError::SegmentedMapUnsupported {
            operation: "commit_chunk_map_superseding_leased",
        })?;
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: prior.version + 1,
        // A content **overwrite** is a fresh publication (ADR-0047): stamp the new object
        // metadata rather than carrying the prior version's forward.
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone(),
        modified: meta.modified,
    };
    let key = inode_key(id);
    let mut batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    for chunk in prior_chunks {
        for (index, dserver) in chunk.fragments() {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            batch = batch.put(
                orphan_key(dserver, frag),
                orphaned_at_millis.to_string().into_bytes(),
            );
        }
    }
    for (pk, pv) in guards {
        batch = batch.require(pk, pv);
    }
    store.commit(batch).await
}

/// Write a pending-chunk ledger entry (the Intent phase of the write protocol).
pub async fn put_pending(
    store: &impl MetadataStore,
    chunk: ChunkId,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    store
        .commit(WriteBatch::new().put(pending_key(chunk), encode(entry)))
        .await
}

/// Clear pending-chunk ledger entries (the Release phase / a custodian sweep).
pub async fn sweep_pending(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
) -> Result<CommitOutcome> {
    let mut batch = WriteBatch::new();
    for &chunk in chunks {
        batch = batch.delete(pending_key(chunk));
    }
    store.commit(batch).await
}

/// **Renew** the pending-ledger lease on every chunk in `chunks` to `entry` in one atomic,
/// **conditional** batch. The streaming write path calls this as an upload progresses so an
/// already-written but not-yet-committed chunk's lease never lapses before the final commit:
/// until the commit publishes the inode, an in-flight chunk's fragments are protected from
/// the custodian **GC** only by its unexpired pending lease (they are in no committed chunk
/// map, so GC's reference set does not cover them). A single start-of-upload deadline let a
/// slow upload run past it and the GC would reclaim the early chunks as expired garbage
/// before the commit — publishing an object with missing fragments (issue #364 durability
/// finding 2, `write::stream_write_data`).
///
/// Renewal may only **extend** a lease that still exists and has not lapsed — it must never
/// re-create authority the sweep already revoked (issue #490). A *blind* overwrite of each
/// `pending:<id>` entry resurrected a chunk whose lease had already lapsed and been swept
/// mid-upload, and the upload then committed an inode pointing at bytes the GC was free to
/// reclaim. So each entry is read back and the renewal **refuses** — returning
/// [`CommitOutcome::Conflict`], nothing written — when a chunk's entry is either:
///  * **absent** — a sweep reclaimed it ([`sweep_expired_leases`]), or
///  * present but its recorded `lease_expiry_millis` is **`<= now_millis`** — lapsed but not
///    yet reaped (renewing it would resurrect revoked authority, `write.rs:417-418`). The
///    `<=` boundary is the sweep's own reap condition (`write.rs:572`): both lease consumers
///    agree a lease is dead at `expiry <= now`, so a renewal at exactly the deadline (`now ==
///    expiry`) is renewing a lease the reaper is already entitled to take.
///
/// The check and the write are ONE batch: for every chunk it pairs
/// `require(pending_key, current-value)` with `put(pending_key, entry)`, so a sweep that
/// deletes an entry **between** the read-back and the commit turns the precondition false and
/// the whole batch is `Conflict` — a read-verify-then-blind-put in two commits could not
/// close that interleave. An empty slice is a no-op.
pub async fn renew_pending(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
    now_millis: u64,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    if chunks.is_empty() {
        return Ok(CommitOutcome::Committed);
    }
    let mut batch = WriteBatch::new();
    for &chunk in chunks {
        let key = pending_key(chunk);
        let current = match store.get(&key).await? {
            // Swept out from under the upload — refuse rather than resurrect.
            None => return Ok(CommitOutcome::Conflict),
            Some(bytes) => bytes,
        };
        let existing: PendingEntry = decode(&current)?;
        if existing.lease_expiry_millis <= now_millis {
            // Lapsed but not yet reaped — renewing it would revive revoked authority.
            return Ok(CommitOutcome::Conflict);
        }
        batch = batch.require(key.clone(), current).put(key, encode(entry));
    }
    store.commit(batch).await
}

/// Read back the `pending:<id>` ledger entry of every chunk in `chunks` and, when all are
/// still **live**, return the compare-and-set preconditions that pin each key to the exact
/// bytes just read. This is the lease-conditional guard the phase-3 committers thread into the
/// **same** [`WriteBatch`] as the inode create/CAS (issue #490): a racing custodian sweep that
/// deletes an entry **between** this read-back and the commit turns its precondition false, so
/// the whole batch is [`CommitOutcome::Conflict`] — the object is never published over
/// fragments the GC is free to reclaim.
///
/// Returns `Ok(None)` — the commit must **refuse, fail-closed** — as soon as any chunk's entry
/// is either **absent** (already reaped by [`sweep_expired_leases`]) or present but **lapsed**
/// (`lease_expiry_millis <= now_millis`, the sweep's own reap boundary, `write.rs:572`): a
/// lapsed lease is dead authority and GC reclaims its bytes keyed on expiry even while the
/// entry is still present (`crates/custodian/src/gc.rs:142-144`). An empty slice yields
/// `Ok(Some(vec![]))` — no leases to guard, so [`create`] / [`commit_chunk_map_superseding`]
/// (their unconditional counterparts) delegate through here unchanged.
async fn live_lease_guards(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
    now_millis: u64,
) -> Result<Option<Vec<(Vec<u8>, Bytes)>>> {
    let mut guards = Vec::with_capacity(chunks.len());
    for &chunk in chunks {
        let key = pending_key(chunk);
        let Some(current) = store.get(&key).await? else {
            return Ok(None);
        };
        let entry: PendingEntry = decode(&current)?;
        if entry.lease_expiry_millis <= now_millis {
            return Ok(None);
        }
        guards.push((key, current));
    }
    Ok(Some(guards))
}

/// Parse the inode id out of an `inode:<id>` key (the inverse of [`inode_key`]).
fn parse_inode_key(key: &[u8]) -> Option<InodeId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// Parse the chunk id out of a `pending:<id>` key (the inverse of [`pending_key`]).
fn parse_pending_chunk_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}

/// The high-water marks of the **in-process id allocators** over the persisted metadata:
/// the largest inode id any record uses, and the largest **in-process-scheme** chunk id
/// (those below `2^64`) any committed chunk map, pending-ledger entry, **or orphan-ledger
/// grace record** references.
///
/// A single-process gateway (`wyrd_server::Gateway`) allocates inode and chunk ids from
/// in-memory counters; on a restart over a **non-empty** store those counters must resume
/// *above* everything already on disk. Otherwise a new-key PUT reuses a committed inode id
/// — a bogus "concurrent writer won" conflict, because [`create`] is `require_absent` on the
/// inode key — and an overwrite mints a chunk id that already backs a committed object,
/// clobbering its fragments on the shared chunk store (issue #364 durability finding 1).
/// This scan supplies those marks so `Gateway::recover` can bump the counters. An empty
/// store yields `(0, 0)` and allocation starts at 1, unchanged.
///
/// The `orphan:` scan closes a third re-mint hazard: after `PUT → DELETE → restart` the
/// deleted object's inode key is gone ([`unlink`] removes it) and its chunk was already
/// committed (so no `pending:` entry survives), yet its fragments are still on disk under a
/// live [`orphan_key`] grace record until the custodian GC's reader-safe window elapses
/// (`crates/custodian/src/gc.rs:134-141`). Were that chunk id not counted here, `recover`
/// would re-mint it for the next object — and GC's reference gate keys on `(dserver, chunk,
/// index)` (`ReferenceSet::protects`, `gc.rs:200`), so the stale orphan record then either
/// leaks the old bytes permanently (the id now looks referenced) or reclaims a fragment the
/// re-minting object has just written but not yet committed (data loss). Projecting the
/// orphan record's chunk id into `max_chunk` makes re-mint step past every id whose orphan
/// record / on-disk fragments are still live (issue #364 durability finding, iter-8 review).
///
/// Chunk ids are projected to the `< 2^64` in-process space on purpose: the cluster client
/// mode derives chunk ids as `(inode << 64) | seq` (`server::cli::chunk_id_minter`) and
/// resumes *its* allocator from the durable `meta:next_inode` counter, so those disjoint,
/// above-`2^64` ids are not the in-process counter's to recover from (and never collide with
/// it — the in-process counter only ever mints ids below `2^64`).
pub async fn high_water_marks(store: &impl MetadataStore) -> Result<(InodeId, ChunkId)> {
    const IN_PROCESS_CHUNK_CEILING: ChunkId = 1 << 64;
    let mut max_inode: InodeId = 0;
    let mut max_chunk: ChunkId = 0;
    for (key, value) in store.scan(b"inode:").await? {
        if let Some(id) = parse_inode_key(&key) {
            max_inode = max_inode.max(id);
        }
        let record: InodeRecord = decode(&value)?;
        let chunks = record
            .chunk_map
            .as_flat()
            .ok_or(ChunkMapError::SegmentedMapUnsupported {
                operation: "high_water_marks",
            })?;
        for chunk in chunks {
            if chunk.id < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(chunk.id);
            }
        }
    }
    for (key, _value) in store.scan(b"pending:").await? {
        if let Some(chunk) = parse_pending_chunk_key(&key) {
            if chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(chunk);
            }
        }
    }
    // Orphan grace records (`orphan:<dserver>:<chunk>:<index>`): a deleted object's
    // fragments still live on disk under this ledger until GC's grace window elapses, so
    // their chunk id is not yet free to re-mint even though no `inode:`/`pending:` key
    // references it any more (see the doc comment above; issue #364).
    for (key, _value) in store.scan(ORPHAN_PREFIX).await? {
        if let Some((_dserver, frag)) = parse_orphan_key(&key) {
            if frag.chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(frag.chunk);
            }
        }
    }
    Ok((max_inode, max_chunk))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs_chunk(placement: Vec<DServerId>) -> ChunkRef {
        // ReedSolomon { k: 4, m: 2 } → fragment_count() == 6.
        ChunkRef {
            id: 0xC0,
            scheme: EcScheme::ReedSolomon { k: 4, m: 2 },
            len: 5,
            placement,
        }
    }

    #[test]
    fn empty_placement_is_valid_pre_m3_identity() {
        // A pre-M3 / mixed-era record decodes with an empty vector (`#[serde(default)]`):
        // valid, resolved by the identity fallback (ADR-0040 decision 3).
        let chunk = rs_chunk(vec![]);
        assert!(chunk.placement_is_valid());
        assert!(chunk.checked_fragments().is_ok());
    }

    #[test]
    fn full_length_placement_is_valid() {
        // len == fragment_count() (6): an explicit full-length record is valid.
        let chunk = rs_chunk(vec![10, 11, 12, 13, 14, 15]);
        assert!(chunk.placement_is_valid());
        let resolved: Vec<_> = chunk.checked_fragments().unwrap().collect();
        assert_eq!(
            resolved,
            vec![(0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15)]
        );
    }

    #[test]
    fn non_empty_wrong_length_placement_is_malformed() {
        // fragment_count() == 6 but a length-2 vector: malformed (truncation/corruption),
        // rejected BEFORE expansion — never identity-filled (ADR-0040 decisions 3–4).
        let chunk = rs_chunk(vec![10, 11]);
        assert!(!chunk.placement_is_valid());
        assert_eq!(
            chunk.checked_fragments().err(),
            Some(MalformedPlacement {
                expected: 6,
                actual: 2,
            })
        );
    }

    #[test]
    fn read_path_fragments_stays_liberal_for_malformed_placement() {
        // The read path is UNCHANGED (ADR-0040 decision 4, availability first): the
        // liberal `fragments()` still resolves the same malformed-placement chunk via the
        // per-index identity fallback — indices 0..2 from the vector, 2..6 identity-filled.
        let chunk = rs_chunk(vec![10, 11]);
        let resolved: Vec<_> = chunk.fragments().collect();
        assert_eq!(
            resolved,
            vec![(0, 10), (1, 11), (2, 2), (3, 3), (4, 4), (5, 5)]
        );
    }
}

/// The #648 rules `crates/core/tests/segmented_map_record.rs` cannot reach — either
/// because they need a patch-added symbol (`parse_seg_key`, `SegmentRecord`,
/// `MAX_SEGMENT_INDEX`, `ChunkMapError`) or because they assert **which** typed reason
/// refused a record, which the boxed trait error only yields on downcast. That file
/// imports nothing this patch adds, so it can stay an assertion-red on `origin/main`;
/// these live co-located instead, where `C4-ci` runs them.
///
/// Covered here: the two decode invariants the brief names (a wrong-width `seg:` key
/// index; a segment record whose chunk lengths do not sum to its declared span), the
/// key-space bound that is also the format's segment-count maximum, the canonical-epoch
/// key grammar, and the **write-side** guards — no record this build cannot read back or
/// cannot publish completely may reach the store.
#[cfg(test)]
mod segmented_shape_invariants {
    use super::*;

    const NONCE: &str = "0123456789abcdef0123456789abcdef";

    /// A well-formed segmented root: two segments tiling `size` (0..5, 5..12) under one
    /// group. The same byte string the acceptance target uses.
    const SEGMENTED_ROOT_OK: &[u8] = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;

    /// A pre-0016 flat record, exactly as it is already stored.
    const FLAT_ROOT: &[u8] = br#"{"size":3,"chunk_map":[{"id":8,"scheme":"None","len":3,"placement":[]}],"state":"Committed","version":3}"#;

    /// The typed reason behind a `WriteBatch`-path failure. The trait surface boxes its
    /// errors (`wyrd_traits::BoxError`), so a caller that must *act* on the shape —
    /// #653's publisher, a maintenance pass — recovers the variant by downcast; asserting
    /// on it here is what pins WHICH rule refused, not merely that something did.
    fn chunk_map_error<T: std::fmt::Debug>(result: Result<T>) -> ChunkMapError {
        let err = result.expect_err("the call must fail closed");
        *err.downcast::<ChunkMapError>()
            .expect("a refused chunk-map shape surfaces as a typed ChunkMapError")
    }

    /// A **real** metadata backend (redb, in-memory mode), not a fake: these tests assert
    /// that a refused record never reaches a store, which only means something against an
    /// implementation that would otherwise have kept it.
    fn store() -> wyrd_metadata_redb::RedbMetadataStore {
        wyrd_metadata_redb::RedbMetadataStore::in_memory().expect("in-memory redb store")
    }

    #[test]
    fn wrong_width_segment_index_key_is_malformed() {
        let group = SegmentGroup::new(NONCE, 1).unwrap();
        // A well-formed key zero-pads the index to SEG_INDEX_WIDTH (6) digits.
        assert!(parse_seg_key(&seg_key(&group, 7).unwrap()).is_ok());
        // One digit short: "seg:<nonce>:1:00007" (5 digits) instead of "000007".
        let wrong_width = format!("seg:{NONCE}:1:00007");
        assert_eq!(
            parse_seg_key(wrong_width.as_bytes()),
            Err(ChunkMapError::SegmentKeyMalformed {
                key: wrong_width.clone()
            })
        );
    }

    #[test]
    fn segment_record_chunk_lengths_not_summing_to_byte_len_is_err() {
        // Two chunks totalling 8 bytes, but the record declares byte_len 9 — checked,
        // not summed, so this can never be masked by a wrapped total.
        let bytes = br#"{"chunks":[{"id":1,"scheme":"None","len":5,"placement":[]},{"id":1,"scheme":"None","len":3,"placement":[]}],"byte_offset":0,"byte_len":9}"#;
        let record: std::result::Result<SegmentRecord, _> = decode(bytes);
        assert!(
            record.is_err(),
            "a segment record whose chunk lengths do not sum to byte_len must be Err"
        );
        // The same rule, typed: the decode routes through `from_wire`, which reports
        // WHICH totals disagreed rather than a bare parse failure.
        let chunks = vec![
            ChunkRef {
                id: 1,
                scheme: EcScheme::None,
                len: 5,
                placement: vec![],
            },
            ChunkRef {
                id: 1,
                scheme: EcScheme::None,
                len: 3,
                placement: vec![],
            },
        ];
        assert_eq!(
            SegmentRecord::from_wire(chunks, 0, 9),
            Err(ChunkMapError::SegmentLengthMismatch {
                declared: 9,
                chunks: 8
            })
        );
    }

    #[test]
    fn segment_key_round_trips_over_the_whole_addressable_index_space() {
        // A multi-digit epoch on purpose: the epoch is parsed as CANONICAL decimal, and a
        // rule that rejected every multi-digit spelling would still pass a `1`-epoch test.
        let group = SegmentGroup::new(NONCE, 42).unwrap();
        let nonce = SegmentNonce::new(NONCE).unwrap();
        assert_eq!(group.nonce(), &nonce);
        assert_eq!(group.nonce().as_str(), NONCE);
        assert_eq!(group.epoch(), 42);
        for index in [0, 7, MAX_SEGMENT_INDEX] {
            let key = seg_key(&group, index).expect("an addressable index has a key");
            assert_eq!(
                parse_seg_key(&key),
                Ok((nonce.clone(), 42, index)),
                "seg_key -> parse_seg_key must be the identity on every addressable index"
            );
            assert!(
                key.starts_with(&seg_range_prefix(&group)),
                "every segment key lies inside its group+epoch range prefix"
            );
        }
        // Byte-lexicographic order equals index order — the reason the index is padded.
        assert!(seg_key(&group, 9).unwrap() < seg_key(&group, 10).unwrap());
        assert_eq!(
            seg_range_prefix(&group),
            format!("seg:{NONCE}:42:").into_bytes()
        );
        assert_eq!(
            seg_group_prefix(&nonce),
            format!("seg:{NONCE}:").into_bytes()
        );
        assert_eq!(seggrp_key(&nonce), format!("seggrp:{NONCE}").into_bytes());
    }

    #[test]
    fn a_group_prefix_can_never_alias_another_generations_epoch_range() {
        // The failure this closes: a `seg:` range is what a cleanup pass DELETES, so a
        // prefix minted from an unvalidated string is a delete aimed at someone else's
        // records. `seg_group_prefix("<nonce>:<epoch>")` would render
        // `seg:<nonce>:<epoch>:` — byte-for-byte the live epoch range of that group —
        // and a sweep of "every epoch of this group" would take a live generation's
        // segments, and every fragment they name, with it (C-1).
        let nonce = SegmentNonce::new(NONCE).unwrap();
        // 1. The aliasing spelling is not a nonce at all, so it cannot reach a helper.
        let aliasing = format!("{NONCE}:7");
        assert_eq!(
            SegmentNonce::new(aliasing.clone()),
            Err(ChunkMapError::NonceNotHex {
                nonce: aliasing.clone()
            }),
            "a nonce carrying the key grammar's separator must be refused, not rendered \
             into someone else's range"
        );
        assert_eq!(
            SegmentGroup::new(aliasing.clone(), 1),
            Err(ChunkMapError::NonceNotHex { nonce: aliasing }),
            "the group constructor enforces the same single rule"
        );
        // Every other spelling that could widen or shift a range: short, long, uppercase
        // (a second spelling of the same 128 bits), non-hex, empty, and a truncation that
        // is a strict PREFIX of a live nonce.
        for bad in [
            "",
            "0123456789abcdef0123456789abcde",   // 31 — one short
            "0123456789abcdef0123456789abcdef0", // 33 — one long
            "0123456789ABCDEF0123456789abcdef",  // uppercase
            "0123456789abcdef0123456789abcdeg",  // non-hex digit
            "0123456789abcdef0123456789abcd:f",  // separator smuggled mid-nonce
        ] {
            assert!(
                SegmentNonce::new(bad).is_err(),
                "{bad:?} must not be usable as a nonce"
            );
        }
        // 2. A prefix built from a VALIDATED nonce addresses that group and nothing else.
        // Fixed width plus a hex-only alphabet is what makes this true: no valid nonce is
        // a prefix of another, so no group prefix can reach another group's keys.
        let other = SegmentNonce::new("fedcba9876543210fedcba9876543210").unwrap();
        let group_prefix = seg_group_prefix(&nonce);
        assert_eq!(
            group_prefix.iter().filter(|b| **b == b':').count(),
            2,
            "a group prefix names a group, never a generation: `seg:<nonce>:`"
        );
        for epoch in [0, 7, u64::MAX] {
            let mine = SegmentGroup::new(NONCE, epoch).unwrap();
            let theirs = SegmentGroup::new(other.as_str(), epoch).unwrap();
            assert_eq!(
                seg_range_prefix(&mine)
                    .iter()
                    .filter(|b| **b == b':')
                    .count(),
                3,
                "an epoch range names one generation: `seg:<nonce>:<epoch>:`"
            );
            assert_ne!(
                group_prefix,
                seg_range_prefix(&mine),
                "a group prefix must never equal an epoch range prefix"
            );
            assert!(
                seg_key(&mine, 0).unwrap().starts_with(&group_prefix),
                "every epoch of MY group lies under my group prefix"
            );
            assert!(
                !seg_key(&theirs, 0).unwrap().starts_with(&group_prefix),
                "no other group's segment key may lie under my group prefix"
            );
            assert!(
                !seg_key(&mine, 0)
                    .unwrap()
                    .starts_with(&seg_group_prefix(&other)),
                "and mine must not lie under theirs"
            );
        }
        assert_ne!(seggrp_key(&nonce), seggrp_key(&other));
    }

    #[test]
    fn a_segment_index_past_the_key_space_is_neither_a_key_nor_a_value() {
        let group = SegmentGroup::new(NONCE, 1).unwrap();
        let past = MAX_SEGMENT_INDEX + 1;
        // It has no key: rendering it would produce SEG_INDEX_WIDTH + 1 digits, which
        // `parse_seg_key` rejects — a key that writes but never reads back.
        assert_eq!(
            seg_key(&group, past),
            Err(ChunkMapError::SegmentIndexUnaddressable {
                index: past,
                max: MAX_SEGMENT_INDEX
            })
        );
        // And it is no value either: a root naming it is refused at decode rather than
        // becoming a map whose tail nothing could ever resolve. This is equally the
        // format's segment-COUNT maximum — indices are exactly `0..segment_count`.
        assert_eq!(
            SegmentedMap::new(
                SegmentGroup::new(NONCE, 1).unwrap(),
                vec![SegmentRef {
                    index: past,
                    byte_offset: 0,
                    byte_len: 5,
                }],
            ),
            Err(ChunkMapError::SegmentIndexUnaddressable {
                index: past,
                max: MAX_SEGMENT_INDEX
            })
        );
        let root = format!(
            r#"{{"size":5,"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":1}},"segment_count":1,"segments":[{{"index":{past},"byte_offset":0,"byte_len":5}}]}},"state":"Committed","version":1}}"#
        );
        assert!(
            decode::<InodeRecord>(root.as_bytes()).is_err(),
            "a root whose segment index has no `seg:` key must not decode"
        );
        // The last addressable index is NOT rejected — the bound is the key space, not
        // one short of it.
        assert!(seg_key(&group, MAX_SEGMENT_INDEX).is_ok());
    }

    #[test]
    fn only_canonical_epoch_spellings_address_a_segment() {
        // `u64::from_str` would accept `+7` and `007`; either would give one segment two
        // keys that differ in bytes but agree in value.
        for epoch in ["007", "+7", "", "7 ", "0x7"] {
            let key = format!("seg:{NONCE}:{epoch}:000007");
            assert_eq!(
                parse_seg_key(key.as_bytes()),
                Err(ChunkMapError::SegmentKeyMalformed { key: key.clone() }),
                "a non-canonical epoch spelling must not resolve to a segment"
            );
        }
        // `0` itself is canonical, and so is any multi-digit epoch.
        for (epoch, value) in [("0", 0u64), ("42", 42), ("18446744073709551615", u64::MAX)] {
            let key = format!("seg:{NONCE}:{epoch}:000007");
            assert_eq!(
                parse_seg_key(key.as_bytes()),
                Ok((SegmentNonce::new(NONCE).unwrap(), value, 7))
            );
        }
    }

    #[test]
    fn a_decoded_segment_record_reports_the_span_its_chunks_cover() {
        let bytes = br#"{"chunks":[{"id":1,"scheme":"None","len":5,"placement":[]},{"id":2,"scheme":"None","len":3,"placement":[]}],"byte_offset":11,"byte_len":8}"#;
        let record: SegmentRecord = decode(bytes).expect("a well-formed segment record decodes");
        assert_eq!(record.byte_offset(), 11);
        assert_eq!(record.byte_len(), 8);
        assert_eq!(record.chunks().len(), 2);
        assert_eq!(record.chunks()[1].id, 2);
        // Re-encoding is the identity, so a `require(key, encode(prior))` CAS over a
        // segment record matches the bytes the store holds.
        assert_eq!(encode(&record).as_ref(), &bytes[..]);
        // The same list, consumed — what a resolver splices into the object's map.
        assert_eq!(record.clone().into_chunks(), record.chunks().to_vec());
        // `new` derives byte_len from the chunks themselves, agreeing with the decode.
        assert_eq!(SegmentRecord::new(record.chunks().to_vec(), 11), Ok(record));
    }

    #[test]
    fn a_segment_record_covering_no_bytes_is_err() {
        // Chunks present, but they cover nothing: a segment that can hold no byte of the
        // object is corruption, not an empty-but-valid record.
        let bytes = br#"{"chunks":[{"id":1,"scheme":"None","len":0,"placement":[]}],"byte_offset":0,"byte_len":0}"#;
        assert!(
            decode::<SegmentRecord>(bytes).is_err(),
            "a segment record covering no bytes must be Err"
        );
        assert_eq!(
            SegmentRecord::new(
                vec![ChunkRef {
                    id: 1,
                    scheme: EcScheme::None,
                    len: 0,
                    placement: vec![],
                }],
                0,
            ),
            Err(ChunkMapError::EmptySegmentRecord {
                byte_offset: 0,
                chunks: 1
            })
        );
        // And so is one carrying no chunks at all.
        assert_eq!(
            SegmentRecord::new(vec![], 4),
            Err(ChunkMapError::EmptySegmentRecord {
                byte_offset: 4,
                chunks: 0
            })
        );
    }

    /// A `ChunkRef` of `len` bytes — the shape the overflow arithmetic below reads.
    fn chunk_of_len(id: ChunkId, len: u64) -> ChunkRef {
        ChunkRef {
            id,
            scheme: EcScheme::None,
            len,
            placement: vec![],
        }
    }

    #[test]
    fn every_span_arithmetic_that_leaves_u64_is_refused_not_wrapped() {
        // WHY these three cases exist at all: each of the sums below is `checked_add`,
        // and the alternative is not a panic — an unchecked sum WRAPS in a release build
        // to a small total that the very next equality check would then confirm. A record
        // whose chunk lengths wrap to its declared `byte_len`, or a table whose tiling
        // wraps back to `size`, would decode as a VALUE: a map that under-reports the
        // bytes its object owns, which is how a live object's fragments go unreferenced
        // (C-1). Each path is exercised at the boundary it guards.

        // 1. The ROOT's tiling: segment 0 covers the whole u64 space, so segment 1 —
        //    contiguous, non-empty, correctly indexed, i.e. past every earlier check —
        //    pushes the running offset over the end.
        assert_eq!(
            SegmentedMap::new(
                SegmentGroup::new(NONCE, 1).unwrap(),
                vec![
                    SegmentRef {
                        index: 0,
                        byte_offset: 0,
                        byte_len: u64::MAX,
                    },
                    SegmentRef {
                        index: 1,
                        byte_offset: u64::MAX,
                        byte_len: 1,
                    },
                ],
            ),
            Err(ChunkMapError::SegmentSpanOverflow { index: 1 })
        );
        // The same table as stored bytes, under the FORGED `size` a wrapping (or
        // saturating) implementation would confirm: an inode carrying it does not decode
        // either.
        let root = format!(
            r#"{{"size":{max},"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":1}},"segment_count":2,"segments":[{{"index":0,"byte_offset":0,"byte_len":{max}}},{{"index":1,"byte_offset":{max},"byte_len":1}}]}},"state":"Committed","version":1}}"#,
            max = u64::MAX,
        );
        assert!(
            decode::<InodeRecord>(root.as_bytes()).is_err(),
            "a root whose tiling leaves u64 must not decode — a wrapped span would agree \
             with a forged `size`"
        );
        // The last table that DOES fit is admitted: the bound is the end of the space,
        // not one short of it.
        assert!(SegmentedMap::new(
            SegmentGroup::new(NONCE, 1).unwrap(),
            vec![
                SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: u64::MAX - 1,
                },
                SegmentRef {
                    index: 1,
                    byte_offset: u64::MAX - 1,
                    byte_len: 1,
                },
            ],
        )
        .is_ok());

        // 2. The RECORD's own extent: its chunks total 2 bytes and it starts one byte
        //    below the end, so the last byte it claims has no offset.
        let offset = u64::MAX - 1;
        assert_eq!(
            SegmentRecord::new(vec![chunk_of_len(1, 2)], offset),
            Err(ChunkMapError::SegmentSpanUnrepresentable {
                byte_offset: offset,
                byte_len: 2
            })
        );
        let bytes = format!(
            r#"{{"chunks":[{{"id":1,"scheme":"None","len":2,"placement":[]}}],"byte_offset":{offset},"byte_len":2}}"#
        );
        assert!(
            decode::<SegmentRecord>(bytes.as_bytes()).is_err(),
            "a stored segment record whose extent ends past u64 must not decode"
        );
        // One byte earlier the extent is representable, so this is the boundary and not a
        // blanket refusal of large offsets.
        assert!(SegmentRecord::new(vec![chunk_of_len(1, 2)], offset - 1).is_ok());

        // 3. The RECORD's chunk lengths: two chunks that leave u64 when summed. Wrapped,
        //    they would total 0 — which `byte_len: 0` would then "confirm".
        let overflowing = vec![chunk_of_len(1, u64::MAX), chunk_of_len(2, 1)];
        assert_eq!(
            SegmentRecord::new(overflowing.clone(), 0),
            Err(ChunkMapError::SegmentLengthOverflow { chunks: 2 })
        );
        assert_eq!(
            SegmentRecord::from_wire(overflowing, 0, 0),
            Err(ChunkMapError::SegmentLengthOverflow { chunks: 2 }),
            "the decode path must reject the sum BEFORE comparing it with the declared \
             byte_len — a wrapped total of 0 would match this record's own claim"
        );
        let bytes = format!(
            r#"{{"chunks":[{{"id":1,"scheme":"None","len":{max},"placement":[]}},{{"id":2,"scheme":"None","len":1,"placement":[]}}],"byte_offset":0,"byte_len":0}}"#,
            max = u64::MAX,
        );
        assert!(
            decode::<SegmentRecord>(bytes.as_bytes()).is_err(),
            "a stored segment record whose chunk lengths leave u64 must not decode"
        );
        // And the largest total that fits is still a record.
        assert!(SegmentRecord::new(vec![chunk_of_len(1, u64::MAX)], 0).is_ok());
    }

    #[test]
    fn a_decoded_root_exposes_which_shape_it_is() {
        let segmented: InodeRecord =
            decode(SEGMENTED_ROOT_OK).expect("a well-formed segmented root decodes");
        assert!(segmented.chunk_map.is_segmented());
        assert!(
            segmented.chunk_map.as_flat().is_none(),
            "a segmented map must never answer `as_flat` — that answer is \
             indistinguishable from an object owning no chunks"
        );
        let map = segmented
            .chunk_map
            .segmented()
            .expect("the segmented map is reachable");
        assert_eq!(map.group().nonce().as_str(), NONCE);
        assert_eq!(map.group().epoch(), 1);
        assert_eq!(map.segment_count(), 2);
        assert_eq!(map.segments().len(), 2);
        assert_eq!(map.segments()[1].byte_offset, 5);
        assert_eq!(map.span(), segmented.size);

        let flat: InodeRecord = decode(FLAT_ROOT).expect("a legacy flat root decodes");
        assert!(!flat.chunk_map.is_segmented());
        assert!(flat.chunk_map.segmented().is_none());
        assert_eq!(flat.chunk_map.as_flat().map(<[ChunkRef]>::len), Some(1));
    }

    #[test]
    fn create_refuses_a_record_it_could_not_read_back() {
        // The failure this closes: `size` and `chunk_map` are independent public fields,
        // so a caller CAN present a segmented record whose table disagrees with `size` —
        // and `encode` would write bytes that this very type then refuses to decode. An
        // object nothing can read is the permanent, data-losing failure mode C-1 forbids,
        // so the record is refused BEFORE it reaches the store.
        let store = store();
        let mut record: InodeRecord = decode(SEGMENTED_ROOT_OK).unwrap();
        record.size = 99;
        assert_eq!(
            chunk_map_error(pollster::block_on(create(&store, 1, "obj", 2, &record))),
            ChunkMapError::SizeSpanMismatch { size: 99, span: 12 }
        );
        assert!(
            pollster::block_on(store.get(&inode_key(2)))
                .unwrap()
                .is_none(),
            "the refused record must not have reached the store"
        );
    }

    #[test]
    fn create_refuses_a_segmented_record_this_build_cannot_publish() {
        // Well-formed, and still refused: the segments live in `seg:` records only #653's
        // staged-publication committer writes. Publishing the root alone would name
        // segments that do not exist — a map every reader must fail closed on.
        let store = store();
        let record: InodeRecord = decode(SEGMENTED_ROOT_OK).unwrap();
        assert_eq!(
            chunk_map_error(pollster::block_on(create(&store, 1, "obj", 2, &record))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "create"
            }
        );
        assert_eq!(
            chunk_map_error(pollster::block_on(create_leased(
                &store,
                1,
                "obj",
                2,
                &record,
                &[],
                0
            ))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "create_leased"
            }
        );
        assert!(
            pollster::block_on(store.get(&inode_key(2)))
                .unwrap()
                .is_none(),
            "the refused record must not have reached the store"
        );
        // A flat record travels the same path unchanged.
        let flat: InodeRecord = decode(FLAT_ROOT).unwrap();
        assert_eq!(
            pollster::block_on(create(&store, 1, "flat", 3, &flat)).unwrap(),
            CommitOutcome::Committed
        );
    }

    #[test]
    fn every_chunk_map_commit_refuses_a_segmented_prior_instead_of_stranding_its_segments() {
        // Replacing a segmented root with a flat map would leave that generation's `seg:`
        // records — and every fragment they name — referenced by nothing, with no
        // resolver (#649) to enumerate them and no committer (#653) to retire them.
        // `commit_chunk_map_superseding` is the same refusal one step worse: it also
        // *orphans* the prior map's fragments in the same commit, so proceeding would
        // publish the overwrite while deadlining nothing — the prior generation's bytes
        // would survive with no chunk map naming them and no grace record to reclaim
        // them, leaked for good.
        let store = store();
        let prior: InodeRecord = decode(SEGMENTED_ROOT_OK).unwrap();
        let key = inode_key(2);
        pollster::block_on(store.commit(WriteBatch::new().put(key.clone(), SEGMENTED_ROOT_OK)))
            .unwrap();
        let next_map = || {
            vec![ChunkRef {
                id: 9,
                scheme: EcScheme::None,
                len: 12,
                placement: vec![],
            }]
        };
        assert_eq!(
            chunk_map_error(pollster::block_on(commit_chunk_map(
                &store,
                2,
                &prior,
                next_map(),
                12
            ))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "commit_chunk_map"
            }
        );
        assert_eq!(
            chunk_map_error(pollster::block_on(commit_chunk_map_superseding(
                &store,
                2,
                &prior,
                next_map(),
                12,
                7,
                &ObjectMeta::default(),
            ))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "commit_chunk_map_superseding"
            }
        );
        assert_eq!(
            pollster::block_on(store.get(&key)).unwrap().as_deref(),
            Some(SEGMENTED_ROOT_OK),
            "the stored root must be exactly as it was"
        );
        assert!(
            pollster::block_on(store.scan(ORPHAN_PREFIX))
                .unwrap()
                .is_empty(),
            "an overwrite that did not commit may not deadline a fragment for reclamation"
        );
    }

    #[test]
    fn a_segmented_prior_outranks_the_lease_state_it_is_committed_under() {
        // ORDER, not merely outcome. `commit_chunk_map_superseding_leased` reads the
        // pending leases and answers `Ok(Conflict)` when one is absent or lapsed. Were
        // that read to happen BEFORE the prior's shape is judged, a segmented prior
        // committed under a swept lease would come back as `Conflict` — the RETRY answer
        // ("a racing writer won; re-read and try again") — for a shape no retry can
        // resolve. The caller would spin, and the shape it must fail closed on would
        // never surface. So the shape is judged first and the typed error is the answer
        // at EVERY lease state.
        let store = store();
        let prior: InodeRecord = decode(SEGMENTED_ROOT_OK).unwrap();
        let key = inode_key(2);
        pollster::block_on(store.commit(WriteBatch::new().put(key.clone(), SEGMENTED_ROOT_OK)))
            .unwrap();
        let chunk: ChunkId = 9;
        let now = 10;
        // `None` — no `pending:` entry at all (a sweep already reaped it); `Some(10)` —
        // present but lapsed at exactly the sweep's reap boundary (`expiry <= now`);
        // `Some(11)` — live. The first two are the states that short-circuit to
        // `Conflict`.
        for lease_expiry_millis in [None, Some(now), Some(now + 1)] {
            match lease_expiry_millis {
                Some(lease_expiry_millis) => pollster::block_on(put_pending(
                    &store,
                    chunk,
                    &PendingEntry {
                        lease_expiry_millis,
                    },
                )),
                None => pollster::block_on(sweep_pending(&store, &[chunk])),
            }
            .unwrap();
            let next_map = vec![ChunkRef {
                id: chunk,
                scheme: EcScheme::None,
                len: 12,
                placement: vec![],
            }];
            assert_eq!(
                chunk_map_error(pollster::block_on(commit_chunk_map_superseding_leased(
                    &store,
                    2,
                    &prior,
                    next_map,
                    12,
                    0,
                    &[chunk],
                    now,
                    &ObjectMeta::default(),
                ))),
                ChunkMapError::SegmentedMapUnsupported {
                    operation: "commit_chunk_map_superseding_leased"
                },
                "a segmented prior must be a typed refusal, never the retriable `Conflict` \
                 an absent or lapsed lease answers with (lease expiry {lease_expiry_millis:?})"
            );
        }
        assert_eq!(
            pollster::block_on(store.get(&key)).unwrap().as_deref(),
            Some(SEGMENTED_ROOT_OK),
            "the stored root must be exactly as it was"
        );
    }

    #[test]
    fn unlink_refuses_a_segmented_inode_rather_than_unbind_fragments_it_cannot_orphan() {
        // The DESTRUCTIVE metadata path, and the one where "read a shape you cannot
        // resolve as an empty chunk list" is unrecoverable. `unlink` deletes the dirent
        // AND the inode, and in the SAME commit writes one orphan grace record per
        // fragment the removed map placed (`unlink`, above) — the deadline the custodian
        // GC reclaims from, and the ledger `high_water_marks` re-mints past. A map's chunks
        // live in `seg:` records nothing in this build can enumerate (#649), so an unlink
        // that proceeded would unbind the object while orphaning NOTHING: every fragment
        // it owns would end up referenced by no chunk map and deadlined by no grace
        // record, which is exactly the unreferenced-but-undeadlined state GC keeps
        // forever. Permanently leaked bytes that no record names — the failure mode C-1
        // forbids. So the shape is judged BEFORE the commit and the binding survives.
        let store = store();
        let key = inode_key(2);
        let name = dirent_key(1, "obj");
        let dirent = encode(&DirentRecord { inode: 2 });
        pollster::block_on(
            store.commit(
                WriteBatch::new()
                    .put(key.clone(), SEGMENTED_ROOT_OK)
                    .put(name.clone(), dirent.clone()),
            ),
        )
        .unwrap();

        assert_eq!(
            chunk_map_error(pollster::block_on(unlink(&store, 1, "obj", 7))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "unlink"
            }
        );
        assert_eq!(
            pollster::block_on(store.get(&key)).unwrap().as_deref(),
            Some(SEGMENTED_ROOT_OK),
            "the inode must survive a delete that could not orphan the fragments it owns"
        );
        assert_eq!(
            pollster::block_on(store.get(&name)).unwrap(),
            Some(dirent),
            "the name must still bind: the delete is atomic, so it did not happen at all"
        );
        assert!(
            pollster::block_on(store.scan(ORPHAN_PREFIX))
                .unwrap()
                .is_empty(),
            "no fragment may be deadlined for reclamation by a delete that did not commit"
        );

        // A flat sibling travels the identical path unchanged — unbound, removed, and its
        // one placed fragment deadlined at the caller's logical instant. Without this leg
        // the assertions above would hold just as well for a guard that refused EVERY
        // unlink, which would be its own permanent failure (no object could be deleted).
        let flat: InodeRecord = decode(FLAT_ROOT).unwrap();
        assert_eq!(
            pollster::block_on(create(&store, 1, "flat", 3, &flat)).unwrap(),
            CommitOutcome::Committed
        );
        let unlinked = pollster::block_on(unlink(&store, 1, "flat", 7))
            .unwrap()
            .expect("the bound name resolves to a record");
        assert_eq!(unlinked.outcome, CommitOutcome::Committed);
        assert!(
            pollster::block_on(store.get(&inode_key(3)))
                .unwrap()
                .is_none(),
            "the flat inode is removed"
        );
        assert!(
            pollster::block_on(store.get(&dirent_key(1, "flat")))
                .unwrap()
                .is_none(),
            "and its name is unbound — the pair the refused unlink above left intact"
        );
        assert_eq!(
            pollster::block_on(store.get(&orphan_key(0, FragmentId { chunk: 8, index: 0 })))
                .unwrap()
                .as_deref(),
            Some(b"7".as_slice()),
            "the flat map's fragment is deadlined at the unlink's logical instant"
        );
    }

    #[test]
    fn high_water_marks_refuses_a_segmented_root_rather_than_re_mint_its_chunk_ids() {
        // `Gateway::recover` resumes the in-process chunk-id counter from this scan, so
        // that a restart never re-mints an id whose fragments are still on disk (issue
        // #364 durability finding 1). A segmented root read as "owns no chunks" would
        // contribute nothing to `max_chunk`, so the next PUT could mint an id that
        // object's fragments already occupy and overwrite them on the shared chunk store
        // — a live object's bytes lost with no error raised anywhere. The scan therefore
        // fails closed: recovery stops rather than hand out an id it cannot prove is free.
        let segmented_store = store();
        pollster::block_on(
            segmented_store.commit(WriteBatch::new().put(inode_key(2), SEGMENTED_ROOT_OK)),
        )
        .unwrap();
        assert_eq!(
            chunk_map_error(pollster::block_on(high_water_marks(&segmented_store))),
            ChunkMapError::SegmentedMapUnsupported {
                operation: "high_water_marks"
            }
        );
        // The same scan over the same key WITHOUT the unreadable shape still reports the
        // marks, so the refusal above is the segmented root's and not the scan's.
        let flat_store = store();
        pollster::block_on(flat_store.commit(WriteBatch::new().put(inode_key(2), FLAT_ROOT)))
            .unwrap();
        assert_eq!(
            pollster::block_on(high_water_marks(&flat_store)).unwrap(),
            (2, 8),
            "the flat record's inode and chunk ids are the marks"
        );
    }
}
