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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bytes::Bytes;
use serde::de::{DeserializeOwned, Error as _, MapAccess, SeqAccess, Visitor};
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
// Chunk-map segmentation (proposal 0016 decision 7, `0016:2280-2496`)
// ---------------------------------------------------------------------------
//
// A published chunk map is ONE value, and a value has a ceiling (FoundationDB's 100 KB
// is the tightest in play and therefore the de-facto limit for every backend,
// `crates/traits/src/lib.rs:746-752`). At ~131-302 B per `ChunkRef` a flat map holds
// 165-381 chunks — 165-381 MiB at the 1 MiB default chunk size (`0016:218-232`), far
// under the >10 GiB launch requirement. A larger map is therefore **segmented**: the
// root records the group identity plus one `SegmentRef` per segment, and segment `i`'s
// chunks live in the record `seg:<nonce>:<epoch>:<index>` (`0016:354`).
//
// The two shapes are discriminated by **JSON type** — a flat map is an array, exactly as
// today, so every pre-existing record decodes and re-encodes byte-identically and every
// `require(key, encode(prior))` compare-and-swap in this module keeps matching the bytes
// in the store (see the `skip_serializing_if` rationale on `InodeRecord::etag`).

/// The number of decimal digits a `seg:` key's segment index is zero-padded to, so the
/// key's byte-lexicographic order equals index order and a range read is debuggable.
/// The width is **fixed**: [`parse_seg_key`] rejects any other width rather than
/// admitting two spellings of one segment.
pub const SEG_INDEX_WIDTH: usize = 6;

/// The length of a segment-group nonce in lowercase-hex characters (128 bits).
pub const SEG_NONCE_HEX_LEN: usize = 32;

/// Key prefix for **segment records** — one segment of a published, segmented chunk map
/// (`0016:354`). Disjoint from every other namespace, and no prefix of another, so no
/// `scan` returns a neighbour's records.
pub const SEG_PREFIX: &[u8] = b"seg:";

/// Key prefix for the **segment-group reservation** marker (`0016:499-527`).
pub const SEGGRP_PREFIX: &[u8] = b"seggrp:";

/// The value of a `seggrp:<nonce>` marker record: its **presence** is the whole meaning,
/// so the value is the empty JSON object and nothing may be added to it — a later slice
/// that CAS'd on its exact bytes would be broken by a value that changed shape.
pub const SEGGRP_MARKER: &[u8] = b"{}";

/// The most segments one root may name (`MAX_ROOT_SEGMENTS`, `0016:2432-2440`).
///
/// A **capacity** guard, enforced at the two points where a table becomes work — the
/// publication that writes one ([`SegmentedPublication::plan`]) and the range read that
/// would spend one ([`read_group_range`]) — and deliberately **not** at decode: rejecting a
/// stored record on a derived capacity constant would turn a durable object unreadable if
/// the constant ever moved (ADR-0045's liberal-on-read boundary). A stored table past this
/// ceiling therefore still decodes, and still fails closed for the object that carries it
/// the moment anything tries to resolve it ([`ChunkMapError::TooManySegments`]).
pub const MAX_ROOT_SEGMENTS: usize = 512;

/// The value ceiling every backend inherits — FoundationDB's, the tightest in play
/// (`crates/traits/src/lib.rs:746-752`).
///
/// **Decimal, not binary.** FoundationDB's hard limit is 100 000 bytes; the trait's "100
/// KB" prose is that limit rounded, not a 102 400-byte allowance. A budget written
/// `100 * 1024` would call 2 400 bytes of every value spendable that the backend rejects
/// **permanently** — the one failure a publication cannot retry out of.
pub const MAX_VALUE_BYTES: usize = 100_000;

/// The key ceiling every backend inherits — FoundationDB's 10 000 bytes, decimal for the
/// reason [`MAX_VALUE_BYTES`] states (`crates/traits/src/lib.rs:747-752`).
pub const MAX_KEY_BYTES: usize = 10_000;

/// Half the transaction envelope (`E_tx/2`, `0016:2331-2337`,`:661`): the byte budget one
/// segment-write batch stays inside. The batch is byte-budgeted, **not** a fixed record
/// count (the iteration-2 envelope fix). `E_tx` is FoundationDB's 10 000 000 bytes —
/// decimal, as above.
pub const MAX_BATCH_BYTES: usize = 5_000_000;

/// The same envelope in **operations** — `B_ops` (`0016:640-648`): the number of
/// preconditions plus mutations one batch may carry.
///
/// A batch is bounded by `min(B_bytes, B_ops)`, not by bytes alone, because the two
/// bound different resources. `E_tx` bounds the transaction's *size*; the backend also
/// states a **5 s transaction deadline** (`crates/traits/src/lib.rs:747-752`), and a batch
/// of thousands of tiny keys is inside every size limit while spending that deadline on
/// sequential round trips. Such a batch is not slow, it is **stuck** — it fails the same
/// way at every retry, permanently, which is the one failure class a staged publication
/// cannot recover from (`0016:636-648`).
///
/// The value is the largest count 0016's own batch inventory budgets for: the retirement
/// drain's byte-budgeted step is sized at "~1,000 small orphan marks" per commit
/// (`0016:667`), so a cap of 1 000 admits every row the protocol plans to emit and refuses
/// the unplanned ones. It is a **knob**, deployment-tunable *downward* per
/// [`SegmentedPublication::ops_budget`] for a slower backend — never a magic number
/// hard-wired past the caller.
pub const MAX_BATCH_OPS: usize = 1_000;

/// The encoded-byte target for ONE segment record: the value ceiling with the 2x headroom
/// the object-ceiling arithmetic assumes (`0016:220-223`).
pub const SEGMENT_TARGET_BYTES: usize = MAX_VALUE_BYTES / 2;

/// A structural violation of the segmented chunk-map shape.
///
/// Every variant is raised **at decode** (a stored record is parsed into a value that
/// cannot be malformed — ADR-0045 / `AGENTS.md:146-149`, parse-don't-validate) or by the
/// resolver when the store's segment records disagree with the root. None is ever
/// admitted as a value, so no consumer can observe a torn map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkMapError {
    /// A segment-group nonce that is not exactly [`SEG_NONCE_HEX_LEN`] lowercase hex
    /// characters — it would key a `seg:` range no writer can reproduce.
    NonceNotHex {
        /// The rejected nonce.
        nonce: String,
    },
    /// A segmented map naming no segments: an empty map is the flat shape, not this one.
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
    /// The segments do not tile the object contiguously from byte 0: this one's
    /// `byte_offset` is not the end of its predecessor, so the root could not answer
    /// "which segment covers byte N" (a gap, an overlap, or a non-monotonic offset).
    SegmentsNotContiguous {
        /// The offending segment's index.
        index: u32,
        /// The offset the tiling requires.
        expected: u64,
        /// The offset the record carries.
        found: u64,
    },
    /// A segment covering no bytes — it can hold no chunk, so it can only be corruption.
    EmptySegment {
        /// The offending segment's index.
        index: u32,
    },
    /// A **segment record** carrying no chunks, or no bytes: the record-level spelling of
    /// [`Self::EmptySegment`], rejected at decode for the same reason.
    ///
    /// A segment exists to hold chunks; an empty one is a record no publication can plan
    /// (`push_segment` closes a segment only when it has chunks) and one whose `SegmentRef`
    /// the root would refuse ([`SegmentedMap::new`]). Admitting it as a *value* would let a
    /// resolve concatenate a hole into an object's chunk list and call the result the map —
    /// the parse-don't-validate rule this repo states for structural invariants
    /// (`AGENTS.md:146-149`).
    EmptySegmentRecord {
        /// The first byte of the object the empty record claimed to cover.
        byte_offset: u64,
        /// How many chunks it carried (0, or a list whose lengths sum to 0).
        chunks: usize,
    },
    /// A segment record whose chunks' lengths do not sum to its declared `byte_len`.
    SegmentLengthMismatch {
        /// The declared byte length.
        declared: u64,
        /// The sum of the record's chunk lengths.
        chunks: u64,
    },
    /// A segment record whose chunk lengths **overflow `u64`** when summed. The sum is
    /// checked rather than wrapped: an unchecked aggregate would panic in a debug build
    /// and, released, wrap to a small total that could then *match* a forged `byte_len`
    /// — admitting a structurally impossible record as a value.
    SegmentLengthOverflow {
        /// How many chunks the record carries.
        chunks: usize,
    },
    /// A root segment table whose byte spans **overflow `u64`** when tiled — the same
    /// checked-aggregate rule one level up.
    SegmentSpanOverflow {
        /// The segment at which the running offset overflowed.
        index: u32,
    },
    /// A **segment record** whose own extent does not exist: `byte_offset + byte_len`
    /// overflows `u64`, so the record claims a last byte no offset can address.
    ///
    /// The same impossibility as [`Self::SegmentSpanOverflow`] one level down — that one is
    /// the root's table failing to tile, this one is a single record failing to describe
    /// itself — and it is rejected at decode for the same reason: the ranged read asks each
    /// segment "do you cover byte N", and a record admitted here would answer from an
    /// addition that wraps.
    SegmentSpanUnrepresentable {
        /// The first byte the record claims.
        byte_offset: u64,
        /// The length it claims from there.
        byte_len: u64,
    },
    /// A segmented root whose segment table does not span exactly
    /// [`InodeRecord::size`] bytes. The table is the object's byte index — the gateway
    /// frames a response from `size` and streams from the table — so a disagreement is
    /// structural corruption, not a contextual detail.
    SizeSpanMismatch {
        /// The `size` the root declares.
        size: u64,
        /// The bytes its segment table actually spans.
        span: u64,
    },
    /// A segment record whose byte span disagrees with the `SegmentRef` the root carries.
    SegmentBoundsMismatch {
        /// The segment's index.
        index: u32,
        /// The `(byte_offset, byte_len)` the root names.
        root: (u64, u64),
        /// The `(byte_offset, byte_len)` the segment record carries.
        segment: (u64, u64),
    },
    /// A segment the root names is **absent** while the root still names its group: a
    /// live generation never loses a segment, so this is an invariant violation and is
    /// **fail-closed** — an error, never a torn success (`0016:2463-2471`).
    SegmentAbsent {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The missing segment's index.
        index: u32,
    },
    /// A record under the group's `seg:` range that the root does not name.
    SegmentUnknown {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The unexpected index.
        index: u32,
    },
    /// A key under the `seg:` prefix that is not a well-formed segment key (a
    /// wrong-width index, a missing field, a non-numeric epoch).
    SegmentKeyMalformed {
        /// The rejected key, lossily rendered.
        key: String,
    },
    /// A stored **segmented root** whose `chunk_map` object is not the shape the record
    /// class defines — a field missing, of the wrong JSON type, or one the encoding does
    /// not carry.
    ///
    /// The typed twin of a `serde` failure, and it exists so that *every* structural
    /// failure of this record class is a `ChunkMapError` (see [`decode`]): the containment
    /// rule that keeps one damaged object from ending a fleet-wide pass is written as a
    /// downcast to this type (`crates/custodian/src/gc.rs:298`,`:317`), so a fault that
    /// reached a consumer wearing `serde_json::Error` instead would abort the pass for
    /// every *other* object in the store.
    SegmentedRootMalformed {
        /// What the decode could not do with the bytes.
        detail: String,
    },
    /// A stored **segment record** whose bytes are not the shape the record class defines.
    /// The `seg:`-level twin of [`Self::SegmentedRootMalformed`], and contained the same
    /// way.
    SegmentRecordMalformed {
        /// What the decode could not do with the bytes.
        detail: String,
    },
    /// A resolve that kept meeting a **retired** generation: each restart re-read the
    /// root and found the generation it named already retired again, more times than
    /// [`MAX_RESOLVE_RESTARTS`] allows. Fail closed — a consumer that gave up and
    /// answered "this object owns no bytes" is the data-loss shape decision 7(h) exists
    /// to prevent.
    MapResolutionUnstable {
        /// How many restarts were spent.
        attempts: usize,
    },
    /// A repoint whose chunk position does not exist in the record that was supposed to
    /// carry it — the resolution it was planned from is stale.
    ChunkPositionOutOfRange {
        /// The position the repoint expected.
        position: usize,
    },
    /// A repoint planned against a **flat** map met a root that is now segmented (or the
    /// reverse): the map changed shape between the resolve and the compare-and-swap.
    MapShapeChanged,
    /// A map naming more than [`MAX_ROOT_SEGMENTS`] segments — a publication whose split
    /// produced them, or a **stored root** whose table declares them.
    ///
    /// Both are refusals at the moment the table becomes *work*: the publication that would
    /// write it ([`SegmentedPublication::plan`]) and the range read that would spend it
    /// ([`read_group_range`]). The second half matters as much as the first, because the
    /// resolve budget is the root's own claim: a table over the ceiling authorises a read
    /// far past the object ceiling the constant *is* (`0016:2432-2440`), so it is answered
    /// with this error rather than performed — per object, contained, and without one row of
    /// the range being read.
    TooManySegments {
        /// How many segments the split produced, or the table declared.
        segments: usize,
    },
    /// A batch whose **whole** mutation — its segment records *plus* the caller's fence
    /// and per-batch contribution, or the root CAS plus the caller's flip mutations —
    /// does not fit the transaction envelope, and no finer split would make it. Refused
    /// here rather than handed to a backend that can only reject it permanently
    /// (`crates/traits/src/lib.rs:747-752`).
    BatchOverBudget {
        /// The offending batch's 0-based position in the phase.
        batch: usize,
        /// The bytes it carries.
        bytes: usize,
        /// The envelope it had to fit.
        budget: usize,
    },
    /// A resumed publication whose recovered `segments_written` cursor names more
    /// segments than the plan has — the cursor and the plan disagree, so the attempt
    /// cannot know what is already durable and must not guess.
    ResumePastPlan {
        /// The cursor the attempt resumed from.
        resume_from: u32,
        /// How many segments the plan has.
        segments: usize,
    },
    /// A publication whose **durable range does not match the plan it is about to
    /// publish**: a record already on disk at an index this plan covers carries different
    /// bytes from the one this attempt puts there.
    ///
    /// Two ways to arrive, and neither may write a byte first.
    ///
    /// *A cursor that was wrong.* A cursor is a claim about bytes this attempt did not
    /// write, and a resume that took it on trust would skip writing segments `0..N` from
    /// *its* chunk list, publish a root describing *its* list, and leave the object mapped
    /// by another list's records — a publication that reports `Committed` and that **no
    /// consumer can ever resolve** (every read fails [`Self::SegmentBoundsMismatch`],
    /// silent at publication and terminal at read), or — when the two lists share their
    /// byte extents and differ only in chunk identity — one that resolves cleanly into a
    /// **hybrid** map and serves the wrong bytes for ever.
    ///
    /// *A record this attempt was about to overwrite.* Above the cursor the attempt writes
    /// the record itself, so a durable one there looks harmless — and is not: at the same
    /// `(nonce, epoch)` it belongs to a generation some root may still name, and
    /// overwriting it tears **that** object's map, one segment at a time, before this
    /// publication's own root compare-and-swap has decided anything. The check therefore
    /// covers the whole plan rather than the claimed prefix, and the first disagreement
    /// refuses instead of publishing.
    ResumePrefixMismatch {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The index whose durable record disagrees with the plan.
        index: u32,
    },
    /// A publication whose group range already holds a segment record **past the end of
    /// the plan it is about to publish** — an earlier, longer attempt's tail at the same
    /// `(nonce, epoch)`.
    ///
    /// Nothing in this committer deletes a `seg:` record, so that tail survives the shorter
    /// republication; and a root must name every record in its own range, so the resolver
    /// fails closed on it ([`Self::SegmentUnknown`]). Publishing over it would report
    /// `Committed` and leave an object **no consumer can ever resolve** — every read, GC,
    /// restore, rebalance and reconstruction pass over that object failing for ever, from a
    /// publication that looked like a success. Refused before the first write instead: the
    /// generation the tail belongs to is rolled back (or resumed at its own length) by the
    /// session that owns it.
    PublicationTailStranded {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The lowest durable index the plan does not reach.
        index: u32,
        /// How many segments this attempt's plan has.
        planned: usize,
    },
    /// A **flip** over a group range that does not yet hold the whole plan: the root it
    /// would publish names segment `index`, and that record is absent from the store or
    /// carries bytes this plan does not put there.
    ///
    /// The flip *is* the publication instant, so it is the last moment at which the
    /// difference between "the segments are durable" and "the completer believes they are"
    /// can still be caught. A flip that ran after a phase-1 crash — or that a recovery drove
    /// on its own, phase 2 without phase 1 — publishes a live root over an empty or partial
    /// range: every read of that object then fails [`Self::SegmentAbsent`] for ever, GC and
    /// restore cannot resolve it either, and no rollback will reclaim it because the root is
    /// live. Distinct from [`Self::ResumePrefixAbsent`] on purpose: that one says a
    /// *cursor's claim* was wrong, this one says the publication is **not finished**, and
    /// the operator's move (run phase 1 to completion) differs from fixing a cursor.
    PublicationIncomplete {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The lowest planned index the store does not hold as this plan wrote it.
        index: u32,
        /// How many segments the root would name.
        planned: usize,
    },
    /// A staged-publication batch whose caller contribution **pins** the publication's
    /// fence record without **moving** it.
    ///
    /// The fence makes the publisher lose to the rollback that reclaims a crashed attempt
    /// (see [`Self::Unfenced`]) — but only while the rollback's own precondition is still
    /// satisfiable. A flip that leaves the fence record exactly as it found it publishes a
    /// root and leaves `require(mpu == Completing@E)` **true**, so the rollback that was
    /// racing it commits *afterwards* and deletes the `seg:` records the now-live root
    /// names: the live-root-naming-deleted-segments hazard, one step later, and the flip's
    /// own fence did nothing to stop it. The publication instant is normatively the batch
    /// that also carries `session → Completed` (`0016:2338-2345`), i.e. the transition
    /// itself, so this refuses a contribution that omits it — with zero I/O, before any
    /// segment is written.
    ///
    /// **Each segment-write batch is held to the same rule**, for the concurrency the
    /// cursor exists to settle. A batch that pins `Completing@E|written=N` and leaves it at
    /// `N` is satisfiable again immediately, so two completers of the same generation can
    /// each verify the range, each write their own segments over the other's, and each
    /// believe they wrote what the root will name (`0016:350`, the `segments_written`
    /// cursor; the batch's `written` is handed to the contribution for exactly this).
    /// Requiring the batch to *move* the record it pins is the format-agnostic spelling of
    /// "advance the cursor": what the record means stays the caller's, that it changed is
    /// the committer's.
    FenceNotTransitioned {
        /// The fence record the batch pinned but did not move, rendered.
        fence: String,
        /// Which phase refused: `"segments"` or `"flip"`.
        phase: &'static str,
        /// The batch's 0-based position in its phase (`0` for the single flip batch).
        batch: usize,
    },
    /// A **segment-write** batch whose contribution **deletes** the publication's fence
    /// record.
    ///
    /// A delete is the strongest transition there is, and at the **flip** — the last batch
    /// this publication will ever commit — that is exactly what
    /// [`Self::FenceNotTransitioned`] asks for. Before the flip it is self-destruction with
    /// durable consequences: every remaining segment batch, and the flip itself, states a
    /// value precondition on that record ([`Self::Unfenced`]), so once it is gone none of
    /// them can ever be satisfied — the publication is permanently stuck **after** it has
    /// made `seg:` records durable. And the record it removed is the rollback's own
    /// precondition, so the reaper that would reclaim those rows has lost the evidence it
    /// keys on: the stranded generation is unreclaimable rather than merely unfinished.
    ///
    /// Refused with the other zero-I/O refusals, before the first record is written, since
    /// the contribution for every batch is known before any of them commits.
    FenceRemovedBeforeFlip {
        /// The fence record the batch deleted, rendered.
        fence: String,
        /// The batch's 0-based position in the segment-write phase.
        batch: usize,
    },
    /// A phase whose batches take the fence record **back to a state it has already held** —
    /// the `A → B → A` cycle that satisfies the per-batch transition rule
    /// ([`Self::FenceNotTransitioned`]) one batch at a time while undoing it across the phase.
    ///
    /// A fence protects by being **unrepeatable**: the moment it returns to a value it has
    /// already held, every precondition that was satisfiable then is satisfiable again, so a
    /// second completer of the same generation — or the rollback this publisher is racing —
    /// commits against it and overwrites this attempt's `seg:` records while it is still
    /// writing them. The published root then names a **hybrid** of two plans, which the
    /// durable-prefix check can only catch when the two disagree about byte extents.
    ///
    /// Refused with zero I/O: every batch's contribution is known before the first commits.
    FenceCycled {
        /// The fence record whose state repeated, rendered.
        fence: String,
        /// Which phase refused: `"segments"` or `"flip"`.
        phase: &'static str,
        /// The 0-based position of the batch that pinned a state an earlier one already did.
        batch: usize,
    },
    /// A resumed publication one of whose claimed-durable segments is **absent** — the
    /// cursor names progress the store does not have (a rollback deleted it, or the cursor
    /// was never true). Same fail-closed treatment as [`Self::ResumePrefixMismatch`].
    ResumePrefixAbsent {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The index the cursor claimed was durable.
        index: u32,
    },
    /// A segment repoint whose `seg:` key belongs to a **different generation** than the
    /// root it was planned against: the resolution is stale, and its compare-and-swap
    /// would rewrite a record the live root no longer names.
    SegmentGroupMismatch {
        /// The generation the root names, as `nonce:epoch`.
        root: String,
        /// The generation the repoint's home names, as `nonce:epoch`.
        home: String,
    },
    /// An operation that a **segmented** map cannot express in this slice: retiring a
    /// segmented generation is the staged `retire:bytes:{generation}` obligation of
    /// proposal 0016 decision 4 / 7(f), which the multipart session slice (#636) owns.
    /// Raised as an explicit, fail-closed error rather than a partial delete that would
    /// strand a live object's bytes or blow the transaction envelope inline.
    SegmentedRetirementUnsupported {
        /// The operation that refused.
        operation: &'static str,
    },
    /// A publication over a prior generation whose `version` is already `u64::MAX`, so the
    /// version this flip would publish does not exist.
    ///
    /// Refused rather than wrapped: the version is the monotonic per-inode counter every CAS
    /// in this module orders writers by, and one that returned to zero would make a stale
    /// writer look newer than the generation it lost to.
    RootVersionOverflow {
        /// The prior generation's version.
        version: u64,
    },
    /// A **segmented root offered to a committer that does not publish segments** — a
    /// create, or any other path whose batch carries the root alone.
    ///
    /// A segmented root is only meaningful beside the `seg:` records its table names, and
    /// only one committer in this module writes both and verifies the whole range is durable
    /// before the root goes live ([`SegmentedPublication`], `0016:2332-2345`). A path that
    /// merely puts the record the caller handed it would publish a **committed object no
    /// consumer can ever resolve** ([`Self::SegmentAbsent`] at every read, GC, restore and
    /// rebalance pass) and one no rollback can reclaim either, because the root is live. The
    /// [`InodeRecord`] fields are public — the shape change had to stay a one-line edit at
    /// ~150 construction sites — so this is where that door is closed: at the committers,
    /// which are the only way a record in this module reaches the store.
    SegmentedPublicationBypassed {
        /// The operation that refused.
        operation: &'static str,
    },
    /// A single put whose **value** exceeds [`MAX_VALUE_BYTES`]. Charged per record, not
    /// per batch: a transaction can be inside the aggregate envelope and still carry one
    /// value the tightest backend must reject, and that rejection is **permanent** — no
    /// retry, no finer split, and a publication wearing it can never complete.
    ValueOverCeiling {
        /// The offending key, lossily rendered.
        key: String,
        /// The value's byte length.
        bytes: usize,
        /// The ceiling it passed ([`MAX_VALUE_BYTES`]).
        ceiling: usize,
    },
    /// A key that exceeds [`MAX_KEY_BYTES`] — the same permanent-rejection class as
    /// [`Self::ValueOverCeiling`], one field over.
    KeyOverCeiling {
        /// The offending key, lossily rendered (truncated for the message).
        key: String,
        /// The key's byte length.
        bytes: usize,
        /// The ceiling it passed ([`MAX_KEY_BYTES`]).
        ceiling: usize,
    },
    /// A staged-publication batch that does not state a value precondition on the
    /// publication's **declared fence record** ([`SegmentedPublication::fence_key`]) — an
    /// unfenced segment write, or an unfenced flip.
    ///
    /// Staged publication is only safe *under* a fence (`0016:2331-2345`): the rollback
    /// that reclaims a crashed attempt deletes its `seg:` records, and the fence
    /// (`require(mpu == Completing@E)`) is what makes the publisher lose to it. Unfenced,
    /// a rollback can delete the segments between the segment phase and the flip, and the
    /// flip's own root compare-and-swap — which the rollback never touches — still
    /// succeeds: a **live root naming records that are gone**.
    ///
    /// "Some precondition" is not the property: the fence has to be on the record whose
    /// state the rollback moves, in **every** batch of both phases. A batch preconditioned
    /// on an unrelated dirent or bucket record satisfies a bare non-empty check and is
    /// exactly as overtakable — which is why the publication declares one fence key and
    /// each batch is checked against it, rather than against emptiness. `require_absent`
    /// on it does not count either: after the rollback deletes the session, an absence
    /// precondition *starts* holding.
    Unfenced {
        /// Which phase refused: `"segments"` or `"flip"`.
        phase: &'static str,
        /// The batch's 0-based position in its phase (`0` for the single flip batch).
        batch: usize,
        /// The fence record the batch had to compare-and-swap on, rendered.
        fence: String,
    },
    /// A **write** whose chunk carries a `placement` vector that is not the chunk's own
    /// fragment count — a repoint's replacement vector, or a chunk a publication was asked
    /// to publish. A committed placement is well-formed iff it is empty (pre-M3 identity
    /// fallback) or exactly `fragment_count()` long (`ChunkRef::placement_is_valid`), and
    /// both of these are **write** paths, where the boundary is strict (ADR-0040
    /// decisions 3–4): a short vector would identity-fill its missing tail at the next
    /// read and send a consumer to a D-server that holds nothing.
    WritePlacementMalformed {
        /// The chunk's position inside the record that carries it.
        position: usize,
        /// The fragment count the chunk's EC scheme requires.
        expected: u16,
        /// The length of the replacement placement vector.
        actual: usize,
    },
    /// A chunk a **segmented publication** was asked to publish whose `placement` is
    /// **empty**.
    ///
    /// Empty is a well-formed committed placement everywhere else — the pre-M3 identity
    /// fallback ([`Self::WritePlacementMalformed`] admits it, and readers resolve it). What
    /// makes it inadmissible *here* is that the population is drained by the backfill pass,
    /// which materialises the explicit vector with an **inode** compare-and-swap; a
    /// segmented map's `ChunkRef`s are not in the inode, so that pass cannot fill them and
    /// no other one is looking (`crates/custodian/src/backfill.rs:136-173`). Publishing one
    /// would mint an object no pass can drain — and one that makes every future backfill
    /// run over the whole store raise `SegmentedPlacementUnfillable`
    /// (`crates/custodian/src/backfill.rs:220-227`). Refused with the rest of the
    /// deterministic, zero-I/O refusals, before the first `seg:` record is written.
    WritePlacementEmpty {
        /// The chunk's position in the publication's chunk list.
        position: usize,
        /// The fragment count the chunk's EC scheme requires, i.e. the length the
        /// publication's producer owed it.
        expected: u16,
    },
    /// A caller's contribution to a staged-publication batch that **writes or deletes a
    /// record the committer owns** — the inode root it is flipping, one of this group's
    /// `seg:` records, or the `seggrp:` marker reserving the group.
    ///
    /// The contribution rides the committer's own batch (that is the point of it), so an
    /// overlapping put or delete is applied in the same transaction as the publication's
    /// and the last writer in the batch wins **silently**: the publication reports
    /// `Committed` while the root it published, or a segment it just wrote, holds the
    /// caller's bytes instead. Refused rather than merged — the caller's business is its
    /// own records (`session → Completed`, `retire:…`), never the publication's.
    ContributionCollides {
        /// The offending key, rendered for the message.
        key: String,
        /// Which committer-owned record class it names.
        owner: &'static str,
        /// Which phase the contribution was made to: `"segments"` or `"flip"`.
        phase: &'static str,
    },
    /// A batch carrying more **operations** than the deadline-derived cap `B_ops`
    /// (`0016:640-648`, [`MAX_BATCH_OPS`]). Bytes are not the only envelope: a batch of
    /// many tiny mutations or preconditions is inside every size limit and still spends
    /// the backend's per-transaction *time* budget on sequential round trips, and a batch
    /// that always times out is not slow, it is stuck — it can never commit, at any retry.
    BatchOverOps {
        /// The batch's 0-based position in its phase.
        batch: usize,
        /// How many operations it carries.
        ops: usize,
        /// The cap it passed.
        cap: usize,
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
            Self::SegmentsNotContiguous {
                index,
                expected,
                found,
            } => write!(
                f,
                "segment {index} starts at byte {found}, not {expected}: segments must tile the object contiguously"
            ),
            Self::EmptySegment { index } => {
                write!(f, "segment {index} covers no bytes")
            }
            Self::EmptySegmentRecord {
                byte_offset,
                chunks,
            } => write!(
                f,
                "segment record at byte {byte_offset} carries {chunks} chunks covering no bytes"
            ),
            Self::SegmentLengthMismatch { declared, chunks } => write!(
                f,
                "segment declares byte_len {declared} but its chunks total {chunks} bytes"
            ),
            Self::SegmentLengthOverflow { chunks } => write!(
                f,
                "segment's {chunks} chunk lengths overflow u64 when summed — rejected, never wrapped"
            ),
            Self::SegmentSpanOverflow { index } => write!(
                f,
                "segment table overflows u64 at segment {index}: the tiling cannot be represented"
            ),
            Self::SegmentSpanUnrepresentable {
                byte_offset,
                byte_len,
            } => write!(
                f,
                "a segment record at byte {byte_offset} claiming {byte_len} bytes ends past u64: its extent cannot be represented"
            ),
            Self::SizeSpanMismatch { size, span } => write!(
                f,
                "inode declares size {size} but its segment table spans {span} bytes"
            ),
            Self::SegmentBoundsMismatch {
                index,
                root,
                segment,
            } => write!(
                f,
                "segment {index} spans {segment:?} but the root names {root:?}"
            ),
            Self::SegmentAbsent {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "segment {index} of live generation seg:{nonce}:{epoch} is absent — fail closed"
            ),
            Self::SegmentUnknown {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "segment {index} exists under seg:{nonce}:{epoch} but the root does not name it"
            ),
            Self::SegmentKeyMalformed { key } => {
                write!(f, "malformed segment key: {key:?}")
            }
            Self::SegmentedRootMalformed { detail } => {
                write!(f, "malformed segmented chunk map: {detail}")
            }
            Self::SegmentRecordMalformed { detail } => {
                write!(f, "malformed segment record: {detail}")
            }
            Self::MapResolutionUnstable { attempts } => write!(
                f,
                "the chunk map's generation was retired under {attempts} successive resolves — fail closed rather than answer that the object owns no bytes"
            ),
            Self::ChunkPositionOutOfRange { position } => write!(
                f,
                "no chunk at position {position} of the record that was to carry it"
            ),
            Self::MapShapeChanged => write!(
                f,
                "the chunk map changed shape between the resolve and the repoint"
            ),
            Self::TooManySegments { segments } => write!(
                f,
                "a published map needs {segments} segments, over the {MAX_ROOT_SEGMENTS} ceiling"
            ),
            Self::BatchOverBudget {
                batch,
                bytes,
                budget,
            } => write!(
                f,
                "batch {batch} carries {bytes} bytes, over the {budget}-byte transaction envelope, and no finer split fits it"
            ),
            Self::ResumePastPlan {
                resume_from,
                segments,
            } => write!(
                f,
                "a publication resumed at segment {resume_from} but its plan has only {segments}"
            ),
            Self::ResumePrefixMismatch {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "the durable segment seg:{nonce}:{epoch}:{index} does not match the segment this resumed publication's own plan puts at that index"
            ),
            Self::ResumePrefixAbsent {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "a publication resumed past segment {index} but seg:{nonce}:{epoch}:{index} is absent"
            ),
            Self::PublicationTailStranded {
                nonce,
                epoch,
                index,
                planned,
            } => write!(
                f,
                "seg:{nonce}:{epoch}:{index} is already durable but this publication's plan has only {planned} segments: publishing over it would leave a root no consumer can resolve"
            ),
            Self::PublicationIncomplete {
                nonce,
                epoch,
                index,
                planned,
            } => write!(
                f,
                "the flip would publish a root naming {planned} segments while seg:{nonce}:{epoch}:{index} is not durable as this plan wrote it: phase 1 has not finished"
            ),
            Self::FenceNotTransitioned {
                fence,
                phase,
                batch,
            } => write!(
                f,
                "{phase} batch {batch} of a staged publication pins its fence record {fence:?} without moving it: a rollback (or a second completer) fenced on the same value can still commit afterwards"
            ),
            Self::FenceRemovedBeforeFlip { fence, batch } => write!(
                f,
                "segments batch {batch} of a staged publication deletes its fence record {fence:?}: every later batch and the flip are fenced on it, so the publication would strand durable segments it can never finish — and never roll back either"
            ),
            Self::FenceCycled {
                fence,
                phase,
                batch,
            } => write!(
                f,
                "{phase} batch {batch} of a staged publication pins a state of its fence record {fence:?} that an earlier batch already pinned: a fence that returns to a value it has held re-opens every precondition satisfiable at that value, so a second completer can overwrite this attempt's segments"
            ),
            Self::SegmentGroupMismatch { root, home } => write!(
                f,
                "a repoint planned against generation {home} met a root naming {root}"
            ),
            Self::SegmentedRetirementUnsupported { operation } => write!(
                f,
                "{operation} of a segmented generation needs the staged retirement obligation of proposal 0016 decision 4/7(f) (multipart session slice)"
            ),
            Self::RootVersionOverflow { version } => write!(
                f,
                "the prior generation is at version {version}, the last one that exists: this publication has no version to take"
            ),
            Self::SegmentedPublicationBypassed { operation } => write!(
                f,
                "{operation} cannot publish a segmented root: its segment records are written and verified only by the staged publication, so this batch would commit an object no consumer could resolve"
            ),
            Self::ValueOverCeiling {
                key,
                bytes,
                ceiling,
            } => write!(
                f,
                "the value put at {key:?} is {bytes} bytes, over the {ceiling}-byte per-value ceiling the tightest backend in play enforces"
            ),
            Self::KeyOverCeiling {
                key,
                bytes,
                ceiling,
            } => write!(
                f,
                "the key {key:?} is {bytes} bytes, over the {ceiling}-byte per-key ceiling the tightest backend in play enforces"
            ),
            Self::Unfenced {
                phase,
                batch,
                fence,
            } => write!(
                f,
                "the {phase} batch {batch} of a staged publication states no value precondition on its fence record {fence:?}: an unfenced phase can be overtaken by the rollback that deletes its segments"
            ),
            Self::WritePlacementMalformed {
                position,
                expected,
                actual,
            } => write!(
                f,
                "a repoint of the chunk at position {position} carries {actual} placement entries, but its scheme has {expected} fragments"
            ),
            Self::WritePlacementEmpty { position, expected } => write!(
                f,
                "the chunk at position {position} of a segmented publication has an empty placement, but its scheme has {expected} fragments: a `seg:` record's placement is not backfillable, so it must be published explicitly"
            ),
            Self::ContributionCollides { key, owner, phase } => write!(
                f,
                "the caller's contribution to the {phase} batch writes {key:?}, which is the publication's own {owner}"
            ),
            Self::BatchOverOps { batch, ops, cap } => write!(
                f,
                "batch {batch} carries {ops} operations, over the {cap}-operation cap a transaction deadline allows"
            ),
        }
    }
}

impl std::error::Error for ChunkMapError {}

/// The identity of one **segment group**: an independent 128-bit nonce minted with the
/// publishing session, paired with the `Completing` fence **epoch** of the attempt that
/// wrote the segments.
///
/// The nonce is deliberately **not** the upload id (`0016:499-527`, iteration-14 finding
/// 2): a `Completed` session's `mpu:` record is deleted at the end of its tombstone
/// window while its `seg:` records must survive as long as the object they map, so the
/// one stated guard (`require_absent(mpu:<id>)`) is already gone by the time a colliding
/// id would overwrite a live object's segment records. The group is reserved by
/// `require_absent(seggrp:<nonce>)` plus a [`SEGGRP_MARKER`] record instead.
///
/// The epoch scoping is load-bearing (the F18 fix, `0016:2352-2380`): a rolled-back
/// attempt's segments live in a key range disjoint from any later attempt's, so a stale
/// rollback obligation can never delete a published generation's segments.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SegmentGroup {
    nonce: String,
    epoch: u64,
}

impl SegmentGroup {
    /// The validating constructor — the **only** way to obtain a `SegmentGroup`, so a
    /// nonce that could not key a reproducible `seg:` range is never representable
    /// (ADR-0045, parse-don't-validate).
    pub fn new(nonce: impl Into<String>, epoch: u64) -> std::result::Result<Self, ChunkMapError> {
        let nonce = nonce.into();
        if nonce.len() != SEG_NONCE_HEX_LEN
            || !nonce
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ChunkMapError::NonceNotHex { nonce });
        }
        Ok(Self { nonce, epoch })
    }

    /// The group nonce (32 lowercase hex characters).
    pub fn nonce(&self) -> &str {
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
        SegmentGroup::new(raw.nonce, raw.epoch).map_err(D::Error::custom)
    }
}

/// One segment of a published map, as the **root** records it: which segment it is and
/// the byte span of the object it covers — so the root alone answers "which segment
/// covers byte N" without reading a single segment record.
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

/// The **segmented** shape of an [`InodeRecord::chunk_map`]: the group identity plus the
/// ordered segment table (`0016:2314-2330`).
///
/// The **structural** invariants — `segment_count == segments.len()`, indices exactly
/// `0..count` in ascending order, a contiguous byte tiling from 0 — are enforced by
/// [`Self::new`], which the `Deserialize` impl routes through, so a malformed stored record
/// is an **error at decode** and never a value a consumer could half-resolve. The
/// **capacity** bound ([`MAX_ROOT_SEGMENTS`]) deliberately is not one of them: it is
/// enforced where the table becomes work — the publication that writes it and the range
/// read that would spend it ([`read_group_range`]) — so a durable object never becomes
/// undecodable because a capacity constant moved (ADR-0045's liberal-on-read boundary).
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

    /// Validate one decoded [`SegmentedMapWire`] into a map — the **whole** structural
    /// check of the segmented shape, in one place and returning a typed error.
    ///
    /// `Deserialize` routes through it (stringifying the error, as serde's `Error::custom`
    /// requires) and so does the decode boundary's fault re-derivation
    /// ([`structural_chunk_map_fault`]), which keeps the *type*. Two callers, one body:
    /// what the store admits as a value and what a consumer is told went wrong can never
    /// drift apart.
    fn from_wire(wire: SegmentedMapWire<'_>) -> std::result::Result<Self, ChunkMapError> {
        let segments = wire.segments.into_owned();
        if wire.segment_count as usize != segments.len() {
            return Err(ChunkMapError::SegmentCountMismatch {
                declared: wire.segment_count,
                actual: segments.len(),
            });
        }
        Self::new(wire.group.into_owned(), segments)
    }
}

/// The wire shape of [`SegmentedMap`] — the settled encoding, field order included:
/// `{"group":{"nonce":…,"epoch":…},"segment_count":…,"segments":[…]}`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentedMapWire<'a> {
    group: Cow<'a, SegmentGroup>,
    segment_count: u32,
    segments: Cow<'a, [SegmentRef]>,
}

impl Serialize for SegmentedMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        SegmentedMapWire {
            group: Cow::Borrowed(&self.group),
            segment_count: self.segment_count(),
            segments: Cow::Borrowed(&self.segments),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SegmentedMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = SegmentedMapWire::deserialize(deserializer)?;
        SegmentedMap::from_wire(wire).map_err(D::Error::custom)
    }
}

/// An inode's chunk map: the ordered chunk list itself (**flat**), or the segment table
/// that names it (**segmented**) — proposal 0016 decision 7(a).
///
/// Discriminated by JSON type, so the flat shape is **byte-identical to the pre-0016
/// encoding in both directions** and every `require(key, encode(prior))` compare-and-swap
/// in this module keeps matching the bytes already in the store. Making the two shapes
/// one value (rather than a flat list plus an optional sidecar) is what stops a consumer
/// from resolving one shape and silently seeing nothing in the other.
///
/// **A consumer never reads this field directly to get an object's chunks.** It goes
/// through [`resolve_chunk_map`], which reads the bounded `seg:` range when the map is
/// segmented — the "no representation of the map may be understood by one consumer and
/// opaque to another" invariant (`0016:2313-2330`,`:2463-2469`).
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
    /// `seg:` records and are reached through [`resolve_chunk_map`]).
    pub fn as_flat(&self) -> Option<&[ChunkRef]> {
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

    /// The segment group this map's chunks are keyed by, or `None` when the map is flat.
    pub fn group(&self) -> Option<&SegmentGroup> {
        self.segmented().map(SegmentedMap::group)
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
    /// shape change stays mechanical at the call sites and the reviewable surface is the
    /// resolver and its consumers.
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
/// segment holds.
///
/// The fields are **private** and there is no field-wise constructor: the invariant holds
/// for every value that exists, including one a maintenance repoint has rewritten
/// ([`Self::repoint`], which moves a chunk's placement and can change nothing else).
/// Public fields would let a caller swap in a `ChunkRef` of a different length and
/// re-encode — persisting a record that fails its own decode, which is an object no
/// consumer can read again (ADR-0045 / `AGENTS.md:146-149`, parse-don't-validate:
/// structural invariants are errors, never values) — or one of a different **id**, which
/// would republish content under a committed identity with no publication fence.
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
    /// ([`ChunkMapError::SegmentLengthOverflow`]) rather than panicking in debug or
    /// wrapping in release — a wrapped total is a `byte_len` the decode check would then
    /// happily confirm.
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
    ///
    /// Both the constructor and the decode go through here, so a record that could not be
    /// built cannot be stored either — and the reverse, which is the one that matters: a
    /// record the store already holds and this rejects can never be *written* by this
    /// committer, so the check cannot strand an object it once published.
    ///
    /// The overflow arm is the same rule the root's table enforces one level up
    /// ([`SegmentedMap::from_wire`]'s running offset, [`ChunkMapError::SegmentSpanOverflow`]),
    /// applied to the record that states its own extent. A segment whose last byte does not
    /// exist in `u64` is structurally impossible, and admitting it as a value leaves every
    /// consumer that asks "does this segment cover byte N" — the ranged read's walk — to
    /// answer from an addition that wraps.
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

    /// Validate one decoded [`SegmentRecordWire`] into a record — the decode's whole
    /// structural check, returning a typed error. Shared by `Deserialize` (which
    /// stringifies it) and by the decode boundary's fault re-derivation
    /// ([`structural_chunk_map_fault`], which keeps the type), for the same reason
    /// [`SegmentedMap::from_wire`] is shared.
    fn from_wire(wire: SegmentRecordWire) -> std::result::Result<Self, ChunkMapError> {
        let total = checked_chunk_bytes(&wire.chunks)?;
        if total != wire.byte_len {
            return Err(ChunkMapError::SegmentLengthMismatch {
                declared: wire.byte_len,
                chunks: total,
            });
        }
        Self::checked(wire.chunks, wire.byte_offset, wire.byte_len)
    }

    /// This segment's ordered chunks.
    pub fn chunks(&self) -> &[ChunkRef] {
        &self.chunks
    }

    /// The chunks, consumed — what the resolver concatenates into the object's map.
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

    /// **Repoint** the chunk at `position` — move its fragments to `placement`, the one
    /// mutation a maintenance repoint performs ([`repoint_chunk`]).
    ///
    /// It takes a *placement vector*, not a replacement [`ChunkRef`], and that is the
    /// whole guarantee: a repoint moves fragments, so the chunk's **identity** (`id`), its
    /// **framing** (`scheme`) and its **extent** (`len`) are not merely validated here,
    /// they are unspellable. A `ChunkRef`-taking mutator let a caller re-point *and*
    /// silently republish different content under a committed id — content no publication
    /// protocol had admitted and no retirement had accounted for — and a re-lengthed chunk
    /// additionally contradicts `byte_len` (the sum of the chunk lengths, repeated in the
    /// root's `SegmentRef`), leaving a record that fails its own decode.
    ///
    /// The vector must be the chunk's own `fragment_count()` long
    /// ([`ChunkMapError::WritePlacementMalformed`]) — maintenance writes are strict
    /// (ADR-0040 decisions 3–4).
    pub fn repoint(
        &mut self,
        position: usize,
        placement: Vec<DServerId>,
    ) -> std::result::Result<(), ChunkMapError> {
        let slot = self
            .chunks
            .get_mut(position)
            .ok_or(ChunkMapError::ChunkPositionOutOfRange { position })?;
        repoint_placement(slot, position, placement)
    }
}

/// Move one chunk's fragments to `placement`, in place — the shared body of both repoint
/// arms ([`SegmentRecord::repoint`] for a segmented map, [`repoint_chunk`]'s root arm for
/// a flat one), so neither shape can drift into accepting what the other refuses.
fn repoint_placement(
    chunk: &mut ChunkRef,
    position: usize,
    placement: Vec<DServerId>,
) -> std::result::Result<(), ChunkMapError> {
    if placement.len() != chunk.fragment_count() as usize {
        return Err(ChunkMapError::WritePlacementMalformed {
            position,
            expected: chunk.fragment_count(),
            actual: placement.len(),
        });
    }
    chunk.placement = placement;
    Ok(())
}

/// The total byte length of `chunks`, **checked**: `None`-free, overflow is an error.
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
        SegmentRecord::from_wire(wire).map_err(D::Error::custom)
    }
}

/// Key for one segment record: `seg:<nonce>:<epoch>:<index>`, the index zero-padded to
/// [`SEG_INDEX_WIDTH`] digits.
pub fn seg_key(group: &SegmentGroup, index: u32) -> Vec<u8> {
    let mut key = seg_range_prefix(group);
    key.extend_from_slice(format!("{index:0width$}", width = SEG_INDEX_WIDTH).as_bytes());
    key
}

/// The **bounded per-object range** a segmented map resolves through:
/// `seg:<nonce>:<epoch>:` (`0016:2463-2469`). Never a global `seg:` scan.
pub fn seg_range_prefix(group: &SegmentGroup) -> Vec<u8> {
    format!("seg:{}:{}:", group.nonce(), group.epoch()).into_bytes()
}

/// The prefix naming **every** epoch of one segment group: `seg:<nonce>:`. Used by the
/// group-adoption predicate the `seggrp:` marker's lifetime rule needs (`0016:513-527`).
pub fn seg_group_prefix(nonce: &str) -> Vec<u8> {
    format!("seg:{nonce}:").into_bytes()
}

/// Key for a segment-group reservation marker: `seggrp:<nonce>`.
pub fn seggrp_key(nonce: &str) -> Vec<u8> {
    format!("seggrp:{nonce}").into_bytes()
}

/// Parse a [`seg_key`] back into `(nonce, epoch, index)`, **strictly**: the index must be
/// exactly [`SEG_INDEX_WIDTH`] ASCII digits, so one segment has exactly one key and a
/// stray record cannot smuggle itself into a resolution under a second spelling.
pub fn parse_seg_key(key: &[u8]) -> std::result::Result<(String, u64, u32), ChunkMapError> {
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
    if SegmentGroup::new(nonce, 0).is_err() {
        return Err(malformed());
    }
    // The epoch is CANONICAL decimal, parsed strictly rather than through `from_str`:
    // `u64::from_str` accepts `+7` and `007`, so a segment could be addressed by keys
    // that differ in bytes but agree in value — two spellings of one segment, which is
    // exactly what the fixed-width index rule exists to forbid (the rubric's *Grammar
    // strictness* class).
    let epoch = parse_canonical_u64(epoch).ok_or_else(malformed)?;
    if !index.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    let index: u32 = index.parse().map_err(|_| malformed())?;
    Ok((nonce.to_string(), epoch, index))
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
/// Decoding goes through [`InodeRecordWire`] so one **cross-field** structural invariant
/// is enforced at decode rather than admitted as a value (ADR-0045, parse-don't-validate):
/// a **segmented** map's segment table must span exactly `size` bytes. The table is the
/// object's byte index — the gateway frames a response from `size` and streams the bytes
/// the table names — so a record where the two disagree can only produce a response whose
/// framing contradicts its body. A flat map keeps today's liberal treatment: its chunk
/// list is the bytes, so there is no second statement to disagree with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "InodeRecordWire")]
pub struct InodeRecord {
    /// Logical content length in bytes.
    pub size: u64,
    /// The ordered chunks making up the content — inline (**flat**) or named by a
    /// segment table (**segmented**, proposal 0016 decision 7). Resolve it through
    /// [`resolve_chunk_map`]; never read the variant directly to get an object's chunks.
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
/// unchanged for every record ever written; it exists only to give the decode a place to
/// enforce the size-vs-segment-table invariant before the value exists.
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
    /// The record's **cross-field structural invariant**, in one place: a segmented map's
    /// segment table spans exactly `size`.
    ///
    /// Both directions go through here — the decode (via `TryFrom<InodeRecordWire>`) and
    /// every committer in this module that puts an inode value ([`encode_inode`]) — for the
    /// reason [`SegmentRecord::checked`] does the same one record class down: a value this
    /// module refuses to *read* must be one it cannot *write* either. The fields are public
    /// (the `Flat | Segmented` change had to stay a one-line edit at its ~150 construction
    /// sites), so a caller can build a record whose table and size disagree; what it cannot
    /// do is get one past a committer and leave a record in the store that every later
    /// decode — every read, every GC pass, every CAS — rejects for ever.
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

/// [`encode`] for an inode record that is about to be **put**, refusing one whose own decode
/// would reject it ([`InodeRecord::checked_shape`]).
///
/// Every committer in this module puts an inode value through here, and that is the whole of
/// the guarantee: `InodeRecord`'s fields are public, so a segmented record whose table span
/// disagrees with its `size` is *representable* — but a representable value that reached the
/// store would be a **poison record**, rejected by every later decode: unreadable by the
/// gateway, unresolvable by every maintenance pass, and impossible to overwrite, because each
/// overwrite is `require(key, encode(prior))` against a prior nobody can decode. Refusing at
/// the write boundary is what keeps the invariant's enforcement at decode (ADR-0045,
/// parse-don't-validate) from turning a caller's mistake into an object that cannot be
/// repaired.
///
/// A **precondition** on a prior record still uses plain [`encode`]: it must reproduce the
/// bytes the store holds, whatever they are, and it writes nothing.
fn encode_inode(record: &InodeRecord) -> std::result::Result<Bytes, ChunkMapError> {
    record.checked_shape()?;
    Ok(encode(record))
}

/// Decode a record from stored bytes.
///
/// **A structural failure of the chunk-map record classes comes back as
/// [`ChunkMapError`], not as `serde_json::Error`** — and that is a correctness property of
/// this function, not a nicety. `serde` erases a custom error into a message
/// (`Error::custom` takes `Display`), so an invariant this module enforces at decode — the
/// segment table's tiling, a segment's `byte_len`, the size/span agreement — would reach a
/// consumer wearing the *serde* type. Containment is written as a downcast to
/// `ChunkMapError` (`crates/custodian/src/gc.rs:298`,`:317`): a fault that misses the
/// downcast makes **one damaged object end a fleet-wide pass**, taking the drain surface
/// down for every healthy object in the store — the exact blast radius the failure
/// containment table exists to bound.
///
/// So the classification happens **here**, at the one boundary every consumer's
/// `metadata::decode(&value)?` already goes through, rather than at each of them: a new
/// consumer inherits it, and one that forgets cannot exist. The re-derivation runs only on
/// the failure path, and only for the two record classes whose bytes *claim* the shape (an
/// `inode:` value whose `chunk_map` is a JSON object, a segment record's three fields), so
/// an undecodable record of any other class keeps the serde error — and with it today's
/// fail-loud behaviour for a value this slice knows nothing about.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(value),
        Err(err) => Err(structural_chunk_map_fault(bytes).map_or_else(|| err.into(), Into::into)),
    }
}

/// The commit state an `inode:` value still spells, for a consumer holding bytes that did
/// **not** decode as a whole — `None` when the bytes do not say so unambiguously.
///
/// It exists for one question, asked by the maintenance walks: *would this record have been
/// skipped anyway?* A **pending** (uncommitted) inode's map never enters the committed
/// reference set (`crates/custodian/src/gc.rs`, the `state != Committed` skip), so its being
/// unresolvable blocks nothing — and treating it as a blocker is not merely imprecise, it
/// stops every reclamation in the fleet (an incomplete reference set protects everything,
/// `ReferenceSet::protects`, `crates/custodian/src/gc.rs:268-272`) until a human repairs a
/// record that was never authorizing anything.
///
/// **Conservative by construction, because the unsafe direction is the other one.** Reading
/// `Committed` as `Pending` would drop a live object's chunks out of the reference set, which
/// is the #508-attempt-4 data loss. So this answers `Some` only from a strict, streaming
/// decode of the field: a value carrying `state` **twice** is a serde duplicate-field error
/// (there is no `serde_json::Value` here to collapse it), an unknown state string is an
/// error, and bytes that are not JSON are an error — every one of them `None`, which every
/// caller must treat as "assume it could be committed".
///
/// Unknown *other* fields are ignored on purpose: this is asked precisely when the record as
/// a whole did not decode, so the one field it reads must be readable on its own.
pub fn inode_state_hint(bytes: &[u8]) -> Option<InodeState> {
    /// The one field, with every other ignored — a struct rather than a
    /// `serde_json::Value` walk so a duplicate `state` is the error it is.
    #[derive(Deserialize)]
    struct StateOnly {
        state: InodeState,
    }
    serde_json::from_slice::<StateOnly>(bytes)
        .ok()
        .map(|probe| probe.state)
}

/// Decode one `seg:` value, where **every** failure is a [`ChunkMapError`] — not only the
/// structural ones [`decode`] can prove from the bytes.
///
/// The difference is what the caller knows: the **key** already names the record class and
/// the object, because a `seg:` record belongs to exactly one group and a group to exactly
/// one object. So a value under it that cannot be read at all — truncated mid-token, a
/// field missing, not JSON any more — is *that object's* fault and nothing else's, and it
/// is contained rather than propagated. The generic boundary cannot say that: given bytes
/// alone, a value that does not parse could have been anything, and treating an unreadable
/// record of unknown class as one object's fault would make a pass continue over damage it
/// has not attributed.
///
/// Every read of a `seg:` value goes through here, so there is no second spelling of this
/// decode for a later consumer to inherit un-contained.
fn decode_segment_record(
    key: &[u8],
    value: &[u8],
) -> std::result::Result<SegmentRecord, ChunkMapError> {
    decode::<SegmentRecord>(value).map_err(|err| {
        // A fault the boundary could name (`SegmentLengthMismatch`, `EmptySegmentRecord`, …)
        // already identifies itself and is kept as it is. The class-level one carries no
        // identity of its own, so this is where the KEY goes on it — the record it names is
        // the whole attribution a consumer gets.
        let detail = match err.downcast::<ChunkMapError>() {
            Ok(typed) => match *typed {
                ChunkMapError::SegmentRecordMalformed { detail } => detail,
                named => return named,
            },
            Err(err) => err.to_string(),
        };
        ChunkMapError::SegmentRecordMalformed {
            detail: format!("{}: {detail}", rendered_key(key)),
        }
    })
}

/// The **typed** structural fault in bytes that did not decode, when those bytes are a
/// chunk-map record class — see [`decode`], whose contract this implements.
///
/// `None` means "not this module's record classes": the caller then keeps serde's own
/// error. When the bytes *are* one of them, this always answers `Some`, and the
/// re-derivation only decides *which* fault to name — it is a re-parse, not a second
/// grammar, handing the same wire shapes to `serde_json` and the same validators the
/// `Deserialize` impls use ([`SegmentedMap::from_wire`], [`SegmentRecord::from_wire`]), so
/// what it reports and what the store admits can never disagree.
///
/// **The re-parse is a weaker reader than the decode that failed, and the fallback is what
/// makes that safe.** A `serde_json::Value` collapses duplicate object keys, so a record
/// carrying `"segment_count"` twice — which strict decoding rejects — re-parses cleanly and
/// names no fault at all. Answering `None` there would hand the caller serde's error and
/// lose the containment for a record whose shape is not in doubt, so a value that *claims*
/// one of these classes is classified whether or not the re-parse can say why.
fn structural_chunk_map_fault(bytes: &[u8]) -> Option<ChunkMapError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let object = value.as_object()?;
    if let Some(map) = object.get("chunk_map") {
        return inode_chunk_map_fault(object, map);
    }
    // A segment record names all three of its fields; nothing else this module stores
    // does. A value that merely *has* a `chunks` field is left to serde.
    if ["chunks", "byte_offset", "byte_len"]
        .iter()
        .all(|field| object.contains_key(*field))
    {
        return Some(
            match serde_json::from_value::<SegmentRecordWire>(value.clone()) {
                Ok(wire) => SegmentRecord::from_wire(wire).err(),
                Err(err) => Some(ChunkMapError::SegmentRecordMalformed {
                    detail: err.to_string(),
                }),
            }
            .unwrap_or_else(|| ChunkMapError::SegmentRecordMalformed {
                detail: "the record does not decode, and re-parsing cannot say why (a \
                         duplicate field collapses in a re-parse)"
                    .to_string(),
            }),
        );
    }
    None
}

/// The structural fault in an `inode:` value's `chunk_map`, if that is why it did not
/// decode.
///
/// A **flat** map is a JSON array and is left to serde: it is the pre-0016 encoding, this
/// slice added no invariant to it, and a legacy record that fails to decode must keep
/// failing exactly as it did before (the deletion-capable passes' "aborting is acceptable"
/// arm). A map that is a JSON **object** claims the segmented shape, so every way it can
/// fail is a fault of this record class — including the size-vs-table disagreement the
/// record-level `TryFrom` enforces, which is a chunk-map invariant one field out.
///
/// **But claiming the shape is not enough on its own: the map must be why the record did not
/// decode.** An `inode:` value can carry a perfectly well-formed segment table beside an
/// unreadable `state`, `version` or metadata field, and answering `ChunkMapError` for that
/// record would contain — as one object's chunk-map fault — a record whose *other* fields
/// are corrupt, silently converting the fail-loud behaviour every consumer has today for a
/// value this slice knows nothing about. So the fault is attributed to the map only when
/// replacing the map with the empty flat one makes the record decode ([`decodes_without`]):
/// then the map is the cause and nothing else is, whether or not the re-parse can name the
/// fault. If it still does not decode, the caller keeps serde's error.
fn inode_chunk_map_fault(
    record: &serde_json::Map<String, serde_json::Value>,
    map: &serde_json::Value,
) -> Option<ChunkMapError> {
    if !map.is_object() {
        return None;
    }
    if !decodes_without(record) {
        return None;
    }
    let wire: SegmentedMapWire = match serde_json::from_value(map.clone()) {
        Ok(wire) => wire,
        Err(err) => {
            return Some(ChunkMapError::SegmentedRootMalformed {
                detail: err.to_string(),
            })
        }
    };
    let map = match SegmentedMap::from_wire(wire) {
        Ok(map) => map,
        Err(fault) => return Some(fault),
    };
    let span = map.span();
    match record.get("size").and_then(serde_json::Value::as_u64) {
        Some(size) if span != size => Some(ChunkMapError::SizeSpanMismatch { size, span }),
        // The table is well formed and spans the recorded size, so the re-parse cannot say
        // what is wrong — and the record still did not decode, while the same record with a
        // flat map does. It claims the segmented shape and the map is the difference, which
        // is enough: see the fallback rule in [`structural_chunk_map_fault`].
        _ => Some(ChunkMapError::SegmentedRootMalformed {
            detail: "the record does not decode, and re-parsing cannot say why (a duplicate \
                     field collapses in a re-parse)"
                .to_string(),
        }),
    }
}

/// Whether `record` decodes as an [`InodeRecord`] once its `chunk_map` is replaced by the
/// empty **flat** map — the question "is the chunk map why this record did not decode?".
///
/// The stand-in is the flat empty list because it is the one map value that carries no
/// invariant of its own: a flat map is the bytes, so there is nothing for the record's other
/// fields to disagree with (the size-vs-table rule is segmented-only, `InodeRecord`'s
/// `TryFrom`). What remains failing, if anything, is therefore not the map's.
///
/// It re-parses through the **production** `Deserialize` path, so it cannot drift from what
/// the store admits. Two limits, stated rather than hidden: it runs on a
/// `serde_json::Value`, which has already collapsed any duplicate key, so a record carrying
/// `state` twice is attributed to its map when the map claims the segmented shape; and it
/// costs one extra parse on a path that has already failed one, never on the success path.
fn decodes_without(record: &serde_json::Map<String, serde_json::Value>) -> bool {
    let mut probe = record.clone();
    probe.insert(
        "chunk_map".to_string(),
        serde_json::Value::Array(Vec::new()),
    );
    serde_json::from_value::<InodeRecord>(serde_json::Value::Object(probe)).is_ok()
}

/// Atomically create an inode and the dirent that names it. Fails with
/// [`CommitOutcome::Conflict`] if the name (or the inode id) already exists, so a
/// just-created file is never duplicated or clobbered.
///
/// A **segmented** `record` is refused ([`ChunkMapError::SegmentedPublicationBypassed`]):
/// this batch carries the root and the dirent and nothing else, so it would publish a
/// committed object whose `seg:` records nobody has written or verified. A segmented root is
/// published by [`SegmentedPublication`] — including the fresh-key case
/// ([`RootPrecondition::Fresh`]), which is this create's counterpart there.
pub async fn create(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    id: InodeId,
    record: &InodeRecord,
) -> Result<CommitOutcome> {
    let batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), created_inode(record, "create")?)
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
    store.commit(batch).await
}

/// The bytes a **create** may put for `record` — [`encode_inode`] plus the rule that a
/// create does not publish segments.
///
/// One body for both creates, so the two cannot drift: an object whose root is segmented
/// exists only after its whole `seg:` range is durable, which is a property only the staged
/// publication establishes (`0016:2332-2345`), and a create that accepted the shape would be
/// a second, unverified publisher of it.
fn created_inode(
    record: &InodeRecord,
    operation: &'static str,
) -> std::result::Result<Bytes, ChunkMapError> {
    if record.chunk_map.is_segmented() {
        return Err(ChunkMapError::SegmentedPublicationBypassed { operation });
    }
    encode_inode(record)
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
    // Decided **before** the lease read, like the segmented-prior refusal in the leased
    // overwrite: it is permanent, and reporting a permanent refusal as the retryable
    // `Conflict` an absent lease produces would loop the caller for ever.
    let root = created_inode(record, "create")?;
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let mut batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), root)
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
    //
    // A **segmented** generation is refused here, explicitly (`SegmentedRetirementUnsupported`).
    // Retiring one is the staged `retire:bytes:{generation}` obligation of proposal 0016
    // decision 4 / 7(f): its fan-out (up to ~1.78 M fragment orphans plus the `seg:` record
    // deletes for a max segmented object) is drained in byte-budgeted batches, **never
    // inline** (`0016:668`), and the obligation record type belongs to the multipart session
    // slice. Failing closed keeps the two outcomes 0016 forbids off the table — a partial
    // delete that strands a live object's bytes, and an inline batch that blows the
    // transaction envelope — instead of a silent half-delete.
    if let Some(inode) = &inode {
        for chunk in
            inode
                .chunk_map
                .as_flat()
                .ok_or(ChunkMapError::SegmentedRetirementUnsupported {
                    operation: "unlink",
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
    }
    let outcome = store.commit(batch).await?;
    Ok(Some(Unlinked { outcome, inode }))
}

/// The version the record a compare-and-swap over `prior` publishes must carry:
/// `prior.version + 1`, **checked**.
///
/// One helper rather than an increment spelled out at each committer, because the unchecked
/// spelling is right up until the one record that is at [`u64::MAX`] and then it is a panic
/// (overflow checks on) or a wrap to zero (off) — and a version that went *backwards* is the
/// worse of the two: every CAS in this module orders writers by it, so a stale writer would
/// look newer than the generation it lost to and a repair could overwrite a live one. Every
/// site that advances a root's version goes through here, so no site can spell it wrong
/// ([`ChunkMapError::RootVersionOverflow`], the refusal the staged publication already
/// raised).
fn next_root_version(prior: &InodeRecord) -> std::result::Result<u64, ChunkMapError> {
    prior
        .version
        .checked_add(1)
        .ok_or(ChunkMapError::RootVersionOverflow {
            version: prior.version,
        })
}

/// Commit a chunk map and size onto an inode at the commit point, bumping its
/// version **conditional on the prior record** (full-value compare-and-set). A
/// writer holding a stale `prior` loses with [`CommitOutcome::Conflict`];
/// exactly one concurrent writer wins.
///
/// The installed map is **flat**, so a `prior` that is **segmented** is refused
/// ([`ChunkMapError::SegmentedRetirementUnsupported`]) rather than committed. Flattening a
/// segmented generation here would retire it — its `seg:` records would be named by no
/// root, and nothing deletes them, so the prior generation's segment records (and every
/// fragment they alone reference) are stranded forever. That retirement is the staged
/// `retire:bytes:{generation}` obligation of proposal 0016 decision 4 / 7(f), which the
/// multipart session slice (#636) owns; a segmented map's own maintenance rewrite is the
/// per-segment [`repoint_chunk`] compare-and-swap, which never rewrites the root at all.
/// The two superseding committers below refuse the same shape for the same reason.
pub async fn commit_chunk_map(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
) -> Result<CommitOutcome> {
    if prior.chunk_map.is_segmented() {
        return Err(ChunkMapError::SegmentedRetirementUnsupported {
            operation: "re-committing the chunk map",
        }
        .into());
    }
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: next_root_version(prior)?,
        // Reconstruction/backfill re-commits the SAME content, so it PRESERVES the
        // publication metadata (ADR-0047): a repair must not move `Last-Modified` or
        // drop the content type. Only the superseding commits below set new metadata.
        ..prior.clone()
    };
    let key = inode_key(id);
    let batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode_inode(&next)?);
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
    // Superseding a **segmented** generation is refused explicitly, and **first**, for the
    // reason [`unlink`] states: the prior generation's retirement is the staged,
    // byte-budgeted `retire:bytes:{generation}` obligation of proposal 0016 decision 4 /
    // 7(f), which also deletes its `seg:` records — never an inline orphan fan-out here.
    //
    // First, because the refusal must not be reachable only *after* the next record is
    // built: `next` advances the version, and a prior at [`u64::MAX`] would then panic
    // (overflow checks on) or wrap (off) before this shape check ever ran — turning a
    // typed, permanent refusal into a crash on a record this committer was always going to
    // decline. It is the same ordering the leased twin below already uses, and the same
    // rule the staged publication follows: deterministic refusals come before any work.
    let prior_chunks =
        prior
            .chunk_map
            .as_flat()
            .ok_or(ChunkMapError::SegmentedRetirementUnsupported {
                operation: "overwrite",
            })?;
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: next_root_version(prior)?,
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
        .put(key, encode_inode(&next)?);
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
    // The **segmented prior is refused first**, before a lease is read or a batch is built
    // ([`ChunkMapError::SegmentedRetirementUnsupported`]). It is a permanent refusal — this
    // slice cannot retire a segmented generation, and no retry changes that — while an
    // absent or lapsed lease is an ordinary `Conflict` the caller is expected to retry.
    // Deciding the lease first turns the permanent answer into the retryable one, and the
    // caller loops for ever on a call that can never succeed. Deterministic refusals come
    // before I/O here for the same reason they do in the staged publication.
    let prior_chunks =
        prior
            .chunk_map
            .as_flat()
            .ok_or(ChunkMapError::SegmentedRetirementUnsupported {
                operation: "overwrite",
            })?;
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let next = InodeRecord {
        size,
        chunk_map: chunk_map.into(),
        state: InodeState::Committed,
        version: next_root_version(prior)?,
        // A content **overwrite** is a fresh publication (ADR-0047): stamp the new object
        // metadata rather than carrying the prior version's forward.
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone(),
        modified: meta.modified,
    };
    let key = inode_key(id);
    let mut batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode_inode(&next)?);
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

// ---------------------------------------------------------------------------
// Resolving a chunk map — the ONE path every consumer takes (0016 decision 7(e),(h))
// ---------------------------------------------------------------------------

/// What resolving a committed inode's chunk map yielded.
///
/// The second arm is proposal 0016 decision 7(h)'s **resolve-retry rule**
/// (`0016:2452-2474`): retiring a generation deletes its `seg:` records, so a consumer
/// midway through a segmented resolve can find a segment absent. The root is always
/// flipped away from a generation *before* that generation's segments are deleted, so
/// re-reading the root settles it — a root now naming a **different group** (superseded)
/// or **absent** (deleted) means the generation was concurrently retired, and the answer
/// is [`MapResolution::Retired`]: a reader restarts against the current root or answers
/// `NoSuchKey`, a maintenance pass drops the stale resolution. A root **unchanged** with a
/// segment absent is an invariant violation and is **fail-closed** — an `Err`, never a
/// torn success (the rubric's *Absent or unsupported entries* rule).
///
/// The same re-read settles a **complete** read, which is the case that is easy to miss:
/// a retired generation's `seg:` records outlive its root until the drain reaches them, so
/// a resolve can read every segment and still be describing the generation the object no
/// longer has. Answering `Resolved` there would hand a maintenance pass the *previous*
/// generation's fragments as the object's protected set and omit the live one's, so a
/// resolution is `Resolved` only if the root still names the group it resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapResolution<'a> {
    /// The object's ordered chunk list — borrowed for a flat map, owned for a segmented
    /// one.
    Resolved(Cow<'a, [ChunkRef]>),
    /// The generation was concurrently retired; this resolution is stale.
    Retired,
}

impl MapResolution<'_> {
    /// The resolved chunk list, or `None` if the generation was retired.
    pub fn chunks(&self) -> Option<&[ChunkRef]> {
        match self {
            Self::Resolved(chunks) => Some(chunks),
            Self::Retired => None,
        }
    }
}

/// The record that carries one resolved chunk — what a maintenance **repoint**
/// (reconstruction re-place, rebalance evacuation) compare-and-swaps when it moves a
/// fragment (`0016:354`, the `seg:` row's writer (2); decision 7(f)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkHome {
    /// A flat map's chunk: the inode root itself carries the `ChunkRef` at `position`.
    Root {
        /// The chunk's position in the flat map.
        position: usize,
    },
    /// A segmented map's chunk: segment record `key` carries it at `position`, and
    /// `prior` is the exact bytes the resolve observed — the CAS precondition, so a
    /// repoint that raced another writer loses instead of clobbering it.
    Segment {
        /// The segment record's key.
        key: Vec<u8>,
        /// The bytes the resolve read.
        prior: Bytes,
        /// The chunk's position inside that segment record.
        position: usize,
    },
}

/// One resolved chunk together with the record that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomedChunk {
    /// The chunk itself.
    pub chunk: ChunkRef,
    /// Where its `ChunkRef` lives.
    pub home: ChunkHome,
}

/// One segment record as the resolver read it.
struct ReadSegment {
    key: Vec<u8>,
    prior: Bytes,
    record: SegmentRecord,
}

/// What one group's `seg:` range holds, as [`read_group_range`] read it.
enum GroupRange {
    /// Every row in the range, keyed by its parsed index — all of them inside the bound
    /// the caller's own table accounts for.
    Rows(BTreeMap<u32, (Vec<u8>, Bytes)>),
    /// The **first** index the caller's table does not account for. The walk stopped
    /// there: what the caller does about it differs (a resolve settles it by re-reading
    /// the root, a publication refuses to publish over it), but neither needs the rest of
    /// the range, and reading it would be the whole cost this bound exists to avoid.
    Beyond(u32),
}

/// Read one segment group's **own** range, `seg:<nonce>:<epoch>:`, in bounded pages —
/// the single way anything in this module reads a group's records.
///
/// **Bounded in what it materialises, not only in what it asks for.**
/// [`MetadataStore::scan`] is complete-or-fail-loud at `SCAN_CAP`
/// (`crates/traits/src/lib.rs:1019-1023`): asked for a range that a damaged or
/// half-rolled-back generation has filled with more records than any root names, it either
/// buffers all of them or answers `ScanCapExceeded` — a **store** error, which per-object
/// containment cannot catch, so one damaged object would end a fleet-wide maintenance pass
/// and blank the drain surface for every healthy object. So the range is paged
/// ([`MetadataStore::scan_page`], #634) one page wider than the caller's table: a complete
/// generation still arrives in a single round trip, and a range holding more than that is
/// *seen* without being read.
///
/// **And `accounted` is bounded HERE, because it is the record's own claim.** It is the
/// caller's table — the root's `segment_count` for a resolve — and a root is exactly the
/// thing under suspicion: at ~48 bytes a row a table of 2 000 segments fits inside one
/// [`MAX_VALUE_BYTES`] value, while the `seg:` records it authorises may hold ~100 KB
/// **each**, so a budget taken on trust is ~200 MB materialised per resolve, by every
/// consumer, on every pass, for one damaged object. A table past [`MAX_ROOT_SEGMENTS`] is
/// therefore **refused** ([`ChunkMapError::TooManySegments`]) — refused, not clamped: a
/// clamp would silently re-file a row the table *does* name as one it does not
/// (`GroupRange::Beyond`) and report a phantom corruption instead of the real one.
///
/// Refused *here* rather than at decode, deliberately: the ceiling is a derived capacity
/// constant, not a structural invariant of the format, so a stored record must stay
/// **decodable** if it ever moves (ADR-0045's liberal-on-read boundary, restated at
/// [`MAX_ROOT_SEGMENTS`]). What it may not do is set a consumer's read budget — and this is
/// the one place a table becomes a read.
///
/// One reader for both callers — the resolve and the publication's durable-prefix check —
/// because they ask the same question of the same range, and a second copy of it is a
/// second place for the bound (or the strict key parse below it) to be forgotten.
async fn read_group_range(
    store: &dyn MetadataStore,
    group: &SegmentGroup,
    accounted: usize,
) -> Result<GroupRange> {
    if accounted > MAX_ROOT_SEGMENTS {
        return Err(ChunkMapError::TooManySegments {
            segments: accounted,
        }
        .into());
    }
    let prefix = seg_range_prefix(group);
    let mut rows: BTreeMap<u32, (Vec<u8>, Bytes)> = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = store
            .scan_page(&prefix, after.as_deref(), accounted.saturating_add(1))
            .await?;
        for (key, value) in page {
            let (nonce, epoch, index) = parse_seg_key(&key)?;
            // The range prefix already pins both, so a mismatch here would mean a key that
            // starts with the prefix without being a key of this group — reject it rather
            // than fold a stranger's chunks into this group's records.
            if nonce != group.nonce() || epoch != group.epoch() {
                return Err(ChunkMapError::SegmentKeyMalformed {
                    key: String::from_utf8_lossy(&key).into_owned(),
                }
                .into());
            }
            // Checked before the row is kept, so the range's size can never become the
            // caller's memory cost.
            if usize::try_from(index).unwrap_or(usize::MAX) >= accounted {
                return Ok(GroupRange::Beyond(index));
            }
            rows.insert(index, (key, value));
        }
        // Clause 3 of the paging contract: `next` is `None` only when the prefix is
        // exhausted, and the cursor is exclusive, so the walk strictly advances.
        match next {
            Some(cursor) => after = Some(cursor),
            None => return Ok(GroupRange::Rows(rows)),
        }
    }
}

/// Read a segmented map's segments through the **bounded per-object range**
/// `seg:<nonce>:<epoch>:` (≤ [`MAX_ROOT_SEGMENTS`], `0016:2463-2469`) — never a global
/// `seg:` scan, and never another group's or another epoch's records.
///
/// The range read is [`read_group_range`]'s — bounded by the root's own table, so a
/// generation whose range holds more than the root names is *seen* without being
/// materialised — and **every** anomaly it can show (a row the root does not name, a row the
/// root names that is absent, a row that does not decode, a row whose extents disagree with
/// the root) is settled by the one resolve-retry rule ([`retired_or`]) rather than by a bare
/// refusal at whichever line noticed it first.
///
/// The result is ordered by the **parsed `index`**, not by the order the store returned:
/// the paging contract does state byte-lexicographic order
/// (`crates/traits/src/lib.rs:1037-1046`), but this resolve does not lean on it — the
/// fixed-width key stays a debuggability property, not a licence to concatenate in arrival
/// order. `Ok(None)` is the retired arm of the resolve-retry rule.
async fn read_segments(
    store: &dyn MetadataStore,
    root_key: &[u8],
    map: &SegmentedMap,
) -> Result<Option<Vec<ReadSegment>>> {
    let group = map.group();
    let named = map.segment_count() as usize;
    let rows = match read_group_range(store, group, named).await? {
        // A record the root does not name: an attempt that wrote further than the
        // published root, or corruption. Settled by the retry rule rather than raised
        // blind — a root that has since moved on makes this a *retired* generation, which
        // is a restart and not a fault.
        GroupRange::Beyond(index) => {
            return retired_or(store, root_key, group, || ChunkMapError::SegmentUnknown {
                nonce: group.nonce().to_string(),
                epoch: group.epoch(),
                index,
            })
            .await
        }
        GroupRange::Rows(rows) => rows,
    };
    let mut found: BTreeMap<u32, ReadSegment> = BTreeMap::new();
    for (index, (key, value)) in rows {
        let record = match decode_segment_record(&key, &value) {
            Ok(record) => record,
            // A row of this range nobody can READ is settled by the same rule as one that is
            // missing, and for the same reason: retirement DELETES a generation's records, so
            // a reader racing the drain can see a row half-overwritten by whatever reuses the
            // space, and a root that has since moved on makes this a retired generation rather
            // than a fault. Raising it blind is how a request racing an ordinary overwrite
            // fails on corruption in a generation nobody will ever read again while the healthy
            // replacement sits beside it.
            Err(fault) => return retired_or(store, root_key, group, || fault).await,
        };
        found.insert(
            index,
            ReadSegment {
                key,
                prior: value,
                record,
            },
        );
    }

    for segment in map.segments() {
        let Some(read) = found.get(&segment.index) else {
            // A segment the live root names is absent — the resolve-retry rule decides
            // whether that is a concurrent retirement or an invariant violation.
            return retired_or(store, root_key, group, || ChunkMapError::SegmentAbsent {
                nonce: group.nonce().to_string(),
                epoch: group.epoch(),
                index: segment.index,
            })
            .await;
        };
        if read.record.byte_offset() != segment.byte_offset
            || read.record.byte_len() != segment.byte_len
        {
            // Also the retry rule's, not a bare refusal: a row whose extents disagree with
            // the root is either this generation's corruption or a row of the generation that
            // replaced it, landed in the same key while this resolve held a stale root. Only
            // the root can tell the two apart.
            let (root, segment_extents) = (
                (segment.byte_offset, segment.byte_len),
                (read.record.byte_offset(), read.record.byte_len()),
            );
            let index = segment.index;
            return retired_or(store, root_key, group, || {
                ChunkMapError::SegmentBoundsMismatch {
                    index,
                    root,
                    segment: segment_extents,
                }
            })
            .await;
        }
    }
    // A resolution that read every segment can still be **stale**: the root is read at one
    // instant and the `seg:` range at another, and a supersede that flipped the root in
    // between leaves the retired generation's records in place until the drain reaches
    // them (`0016:2452-2462` — the root always moves first). Reading them all successfully
    // therefore proves nothing about currency, and a stale-but-complete map is the worse
    // failure of the two the retry rule guards: a maintenance pass would protect the
    // retired generation's fragments while omitting the **live** replacement's, and
    // restore/GC act on that omission. So the same re-read that settles an absent segment
    // also settles a complete read — one extra `get` per segmented resolve, and only for
    // the segmented shape (a flat map is one value, read atomically).
    if !root_still_names(store, root_key, group).await? {
        return Ok(None);
    }
    Ok(Some(found.into_values().collect()))
}

/// Whether the **current** root still names `group` — the re-read at the heart of the
/// resolve-retry rule (`0016:2463-2471`). An absent root (the object was deleted) answers
/// `false`: the generation is being reclaimed either way.
async fn root_still_names(
    store: &dyn MetadataStore,
    root_key: &[u8],
    group: &SegmentGroup,
) -> Result<bool> {
    let Some(bytes) = store.get(root_key).await? else {
        return Ok(false);
    };
    let current: InodeRecord = decode(&bytes)?;
    Ok(current.chunk_map.group() == Some(group))
}

/// The resolve-retry rule's arbiter (`0016:2463-2471`): re-read the root and decide
/// whether the anomaly this range shows means a concurrent retirement (`Ok(None)` —
/// restart or drop) or an invariant violation (`Err(fault())` — fail closed, never a torn
/// map).
///
/// **Every** anomaly a group range can show goes through here — a segment the root names
/// that is **absent**, a record the root does **not** name, a record that does not
/// **decode**, and a record whose byte extents **disagree** with the root's table — because
/// they all have the same benign cause: the generation was retired, its records are being
/// deleted, and the range now belongs to whatever replaced it. Deciding any of them from the
/// range alone turns an ordinary supersede into a fault — a GET racing an overwrite failing
/// on corruption in a generation nobody will read again, while the healthy replacement is
/// already published — and (worse, and the reason the rule exists) deciding the *absent* one
/// that way would let a consumer answer "this object owns no bytes".
///
/// One arbiter rather than a rule each anomaly site follows: the recorded failure of the
/// round that added it was two sites out of four routed through it, so the same damaged range
/// was a retirement at one line and a hard fault three lines later.
async fn retired_or(
    store: &dyn MetadataStore,
    root_key: &[u8],
    group: &SegmentGroup,
    fault: impl FnOnce() -> ChunkMapError,
) -> Result<Option<Vec<ReadSegment>>> {
    if root_still_names(store, root_key, group).await? {
        // The root still names THIS generation, and its range does not hold exactly what
        // the root says it does. A live generation neither loses a segment nor grows one,
        // so this is corruption or a protocol violation — fail closed.
        return Err(fault().into());
    }
    // The root moved on (a different group, back to a flat map, or gone): superseded.
    Ok(None)
}

/// Resolve a committed inode's chunk map to its ordered chunk list — **the** call every
/// consumer of `.chunk_map` makes (proposal 0016 decision 7(e), `0016:2393-2399`).
///
/// A flat map resolves to a borrow of the record and reads nothing. A segmented map reads
/// the bounded range `seg:<nonce>:<epoch>:` ([`read_group_range`]) and nothing else. `root_key` is the
/// inode's own key ([`inode_key`]): the resolver needs the root's *identity*, not just its
/// value, so that an absent segment can be settled by re-reading it (decision 7(h)) — an
/// API taking only a store and an already-decoded record could not tell a concurrent
/// retirement from corruption.
pub async fn resolve_chunk_map<'a>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &'a InodeRecord,
) -> Result<MapResolution<'a>> {
    let map = match &record.chunk_map {
        ChunkMap::Flat(chunks) => return Ok(MapResolution::Resolved(Cow::Borrowed(chunks))),
        ChunkMap::Segmented(map) => map,
    };
    let Some(segments) = read_segments(store, root_key, map).await? else {
        return Ok(MapResolution::Retired);
    };
    let mut chunks = Vec::new();
    for segment in segments {
        chunks.extend(segment.record.into_chunks());
    }
    Ok(MapResolution::Resolved(Cow::Owned(chunks)))
}

/// How many times [`resolve_live_chunk_map`] re-reads the root before it fails closed.
///
/// Each restart means the generation it just read was retired *again* under it. Two are
/// already more than the protocol can produce for one caller without a publisher looping;
/// past that, the honest answer is an error, never "the object owns no bytes".
pub const MAX_RESOLVE_RESTARTS: usize = 3;
/// A chunk map resolved against the **live** root, with the generation it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveChunkMap<'a> {
    /// The root the chunks were resolved from when the caller's own record was **not**
    /// the live one — a concurrent publication superseded it mid-resolve and this is the
    /// generation that replaced it. `None` when the caller's record is still current, in
    /// which case `chunks` borrows from it.
    pub restarted: Option<InodeRecord>,
    /// The object's ordered chunk list.
    pub chunks: Cow<'a, [ChunkRef]>,
}

/// Resolve the chunk map of the root **the store holds now**, reading the root itself —
/// the entry for a caller whose own record came from a **scan snapshot**.
///
/// The distinction from [`resolve_live_chunk_map`] is not stylistic, and it is the one a
/// maintenance pass must get right. That entry starts from the record the caller passes,
/// and for a **flat** map that record *is* the whole answer: the chunks were read
/// atomically with the root, so nothing is re-read and nothing can tear. Which means a
/// caller holding a stale snapshot gets the **stale generation's** chunks back, with no
/// indication that the object has moved on — and a pass that then treats that list as the
/// object's protected set omits the live generation from GC's reference build and from
/// restore's `orphan:` decision (`restore.rs:222`,`:266`). A `scan("inode:")` over a large
/// store is exactly such a snapshot: it is minutes old by the time the pass reaches its
/// last record.
///
/// So the maintenance plane resolves through **this** entry, which costs one `get` of the
/// root per object and answers about the generation the store has *now*, for both shapes
/// alike. The read paths keep the snapshot entry: they read the root and resolve it in the
/// same breath, so a re-read would buy nothing and cost a round trip on every GET.
///
/// `Ok(None)` means there is **no live committed generation** — the root is absent
/// (deleted) or no longer `Committed`. That is exactly the condition every maintenance
/// pass already skips on (`if record.state != InodeState::Committed { continue }`) and the
/// reader already answers `NoSuchKey` for; a deleted object's bytes are reclaimed through
/// the deletion's own evidence, never through a consumer's silence. A generation retired
/// mid-resolve is re-read up to [`MAX_RESOLVE_RESTARTS`] times, then fails closed.
pub async fn resolve_current_chunk_map(
    store: &dyn MetadataStore,
    root_key: &[u8],
) -> Result<Option<CurrentChunkMap>> {
    for _ in 0..MAX_RESOLVE_RESTARTS {
        let Some(bytes) = store.get(root_key).await? else {
            return Ok(None); // the object was deleted under this pass
        };
        let record: InodeRecord = decode(&bytes)?;
        if record.state != InodeState::Committed {
            return Ok(None); // no live committed generation right now
        }
        if let MapResolution::Resolved(chunks) = resolve_chunk_map(store, root_key, &record).await?
        {
            let chunks = chunks.into_owned();
            return Ok(Some(CurrentChunkMap { record, chunks }));
        }
    }
    Err(ChunkMapError::MapResolutionUnstable {
        attempts: MAX_RESOLVE_RESTARTS,
    }
    .into())
}

/// A chunk map resolved from the root the store holds **now** ([`resolve_current_chunk_map`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentChunkMap {
    /// The live root the chunks were resolved from — not the caller's snapshot.
    pub record: InodeRecord,
    /// That generation's ordered chunk list.
    pub chunks: Vec<ChunkRef>,
}

/// Resolve a **caller's own** committed record to its chunk list, restarting onto the live
/// root if that generation was retired mid-resolve — the entry for a consumer that has
/// just read the root itself (both read paths).
///
/// [`resolve_chunk_map`] answers [`MapResolution::Retired`] when the generation the
/// caller holds was retired mid-resolve. **Treating that as "no chunks" is data loss**:
/// GC's reference set would omit a live object, `restore` would then manufacture
/// `orphan:` evidence for its fragments (`restore.rs:222`,`:266`) and the next GC pass
/// past grace would delete them, while a reader would answer `404` for an object that
/// never stopped existing. Decision 7(h)'s "drop the stale resolution" means drop *that
/// resolution*, not the object — so a retired generation is re-read here through
/// [`resolve_current_chunk_map`], which resolves the generation that replaced it.
///
/// **What it does not do is re-read a root it did not have to.** A flat map needs no
/// segment read, so nothing can tear and nothing is re-read: the answer is the caller's
/// own record, as current as the caller's own read of it and no more. A caller whose
/// record came from a **scan snapshot** — every maintenance pass — must therefore use
/// [`resolve_current_chunk_map`] instead, which reads the root; that is where the "live"
/// guarantee is bought, and it is bought with a `get`.
///
/// `Ok(None)` has the same meaning as in [`resolve_current_chunk_map`]: no live committed
/// generation.
pub async fn resolve_live_chunk_map<'a>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &'a InodeRecord,
) -> Result<Option<LiveChunkMap<'a>>> {
    if let MapResolution::Resolved(chunks) = resolve_chunk_map(store, root_key, record).await? {
        return Ok(Some(LiveChunkMap {
            restarted: None,
            chunks,
        }));
    }
    let Some(current) = resolve_current_chunk_map(store, root_key).await? else {
        return Ok(None);
    };
    Ok(Some(LiveChunkMap {
        restarted: Some(current.record),
        chunks: Cow::Owned(current.chunks),
    }))
}

/// [`resolve_chunk_map`], but each chunk carries the record that holds it — what
/// reconstruction and rebalance need to compare-and-swap a placement they move
/// ([`repoint_chunk`]). `Ok(None)` is the same retired arm as [`MapResolution::Retired`].
pub async fn resolve_chunk_homes(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &InodeRecord,
) -> Result<Option<Vec<HomedChunk>>> {
    let map = match &record.chunk_map {
        ChunkMap::Flat(chunks) => {
            return Ok(Some(
                chunks
                    .iter()
                    .enumerate()
                    .map(|(position, chunk)| HomedChunk {
                        chunk: chunk.clone(),
                        home: ChunkHome::Root { position },
                    })
                    .collect(),
            ))
        }
        ChunkMap::Segmented(map) => map,
    };
    let Some(segments) = read_segments(store, root_key, map).await? else {
        return Ok(None);
    };
    let mut homed = Vec::new();
    for segment in segments {
        for (position, chunk) in segment.record.into_chunks().into_iter().enumerate() {
            homed.push(HomedChunk {
                chunk,
                home: ChunkHome::Segment {
                    key: segment.key.clone(),
                    prior: segment.prior.clone(),
                    position,
                },
            });
        }
    }
    Ok(Some(homed))
}

/// A chunk map resolved against the **live** root, each chunk carrying its home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveChunkHomes {
    /// The generation the homes came from when the caller's record was superseded
    /// mid-resolve — and therefore the record a repoint must CAS against
    /// ([`repoint_chunk`]'s `prior_root`). `None` when the caller's own record is still
    /// current.
    pub restarted: Option<InodeRecord>,
    /// The resolved chunks and the records that carry them.
    pub chunks: Vec<HomedChunk>,
}

/// [`resolve_current_chunk_map`] for the repoint consumers: it reads the root itself, so
/// the homes — and the root a repoint compare-and-swaps against — belong to the generation
/// the store holds **now**, not to the pass's scan snapshot.
///
/// The flat asymmetry [`resolve_live_chunk_map`] documents matters even more here: a
/// repoint planned against a stale flat root CASes on bytes the store no longer holds, so
/// it can only ever lose — a maintenance pass that never repairs anything, silently.
pub async fn resolve_current_chunk_homes(
    store: &dyn MetadataStore,
    root_key: &[u8],
) -> Result<Option<CurrentChunkHomes>> {
    for _ in 0..MAX_RESOLVE_RESTARTS {
        let Some(bytes) = store.get(root_key).await? else {
            return Ok(None);
        };
        let record: InodeRecord = decode(&bytes)?;
        if record.state != InodeState::Committed {
            return Ok(None);
        }
        if let Some(chunks) = resolve_chunk_homes(store, root_key, &record).await? {
            return Ok(Some(CurrentChunkHomes { record, chunks }));
        }
    }
    Err(ChunkMapError::MapResolutionUnstable {
        attempts: MAX_RESOLVE_RESTARTS,
    }
    .into())
}

/// A chunk map resolved from the root the store holds **now**, each chunk carrying its
/// home ([`resolve_current_chunk_homes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentChunkHomes {
    /// The live root — the record a repoint must compare-and-swap against
    /// ([`repoint_chunk`]'s `prior_root`).
    pub record: InodeRecord,
    /// The resolved chunks and the records that carry them.
    pub chunks: Vec<HomedChunk>,
}

/// [`resolve_live_chunk_map`] for the repoint consumers: total against a generation
/// retired mid-resolve, and it hands back the root a repoint must compare-and-swap
/// against, so a restart re-plans on the live generation instead of dropping the object
/// with a bare `continue`. Same snapshot caveat as [`resolve_live_chunk_map`]: a caller
/// working from a scan uses [`resolve_current_chunk_homes`].
pub async fn resolve_live_chunk_homes(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &InodeRecord,
) -> Result<Option<LiveChunkHomes>> {
    if let Some(chunks) = resolve_chunk_homes(store, root_key, record).await? {
        return Ok(Some(LiveChunkHomes {
            restarted: None,
            chunks,
        }));
    }
    let Some(current) = resolve_current_chunk_homes(store, root_key).await? else {
        return Ok(None);
    };
    Ok(Some(LiveChunkHomes {
        restarted: Some(current.record),
        chunks: current.chunks,
    }))
}

/// The compare-and-swap that **repoints** one chunk — rewriting the placement of the
/// `ChunkRef` in whichever record carries it — with the guarantees the flat repoint has
/// always had: exactly one writer wins, and a loser's rebuilt fragments are collectable
/// garbage rather than a corrupted record (`0005:200-203`, ADR-0015).
///
/// It takes the chunk's **new placement**, never a replacement `ChunkRef`: a repoint is a
/// *move*, so the chunk's id, EC scheme and length are carried over from the record the
/// CAS is preconditioned on and cannot be changed through this door at all. Publishing
/// different content under a committed chunk id is a publication, with a publication's
/// fence and a retirement for the bytes it displaces — never a placement repair.
///
/// * a flat map's chunk: `require(inode == prior)` + the rewritten root at `version + 1`,
///   exactly as before this shape existed;
/// * a segmented map's chunk: `require(seg:<g>:<E>:<i> == prior)` **and**
///   `require(inode == prior)` — the second precondition is what makes the repoint lose
///   to a concurrent supersede/delete of the generation (`0016:354`, decision 7(f); the
///   `X47` repoint-vs-supersede race). The root is not rewritten, so its version does not
///   move: the *segment* record is the one that changed. The home's key is **bound to the
///   root's generation** before either precondition is built: a stale home carrying a
///   retired group is a [`ChunkMapError::SegmentGroupMismatch`], never a batch whose
///   preconditions happen to hold over a record the root has already replaced.
///
/// The caller adds its own mutations (the repair-queue drain, the `orphan:` evidence for
/// the vacated position) to the returned batch and commits it.
///
/// **Destination pre-evidence is deliberately NOT added here — deferred: #636.** Decision
/// 7's `seg:` row also asks a repoint to pre-mark `orphan:<P_new>` *before* the
/// destination fragment is written, so a lost CAS leaves evidence for the abandoned copy
/// (`0016:354` clause (i), X47). That rule's other half is the retirement drain re-reading
/// each segment's **current** placement at drain time (`0016:2416-2430`), which is the
/// multipart slice's `retire:bytes:{generation}` obligation — the two only close the race
/// together, and half of it here would add evidence no drain yet consumes. Until then this
/// keeps the flat repoint's long-standing behaviour **unchanged for both shapes**: the
/// destination is written first and a losing CAS leaves collectable garbage
/// (`0005:298-299`, `rebalance.rs:258-259`), so segmentation adds no new exposure.
///
/// **Nor is the destination *drain* fence — deferred: #636.** The same reasoning covers
/// `require_absent(desired:dserver:<destination>)`: on `main` the flat evacuation CAS
/// carries `require(inode == prior)` and no precondition naming the destination's
/// desired-state record at all (`crates/custodian/src/rebalance.rs:274-296`), and
/// reconstruction's flat repair is the same shape. Both shapes go through *this* builder,
/// so a fence added here would change the flat path too — and it closes nothing on its own:
/// its counterpart is the retirement drain re-reading each segment's current placement
/// (`0016:2416-2430`), which is the same #636 obligation as the pre-mark above.
pub fn repoint_chunk(
    inode_id: InodeId,
    prior_root: &InodeRecord,
    home: &ChunkHome,
    placement: Vec<DServerId>,
) -> Result<WriteBatch> {
    let root_key = inode_key(inode_id);
    match home {
        ChunkHome::Root { position } => {
            let mut chunks = prior_root
                .chunk_map
                .as_flat()
                .ok_or(ChunkMapError::MapShapeChanged)?
                .to_vec();
            let slot = chunks
                .get_mut(*position)
                .ok_or(ChunkMapError::ChunkPositionOutOfRange {
                    position: *position,
                })?;
            repoint_placement(slot, *position, placement)?;
            let next_root = InodeRecord {
                chunk_map: ChunkMap::Flat(chunks),
                // Checked, exactly as the staged publication's supersede is
                // ([`next_root_version`]): this arm rewrites the root, so it is one of the
                // sites that can be asked to advance a version that has nowhere to go.
                version: next_root_version(prior_root)?,
                ..prior_root.clone()
            };
            let prior_bytes = encode(prior_root);
            let next_bytes = encode_inode(&next_root)?;
            // **The same ceiling re-check as the segmented arm below, for the same reason —
            // but charged on GROWTH.**
            //
            // A repoint keeps the placement vector's length and not its width (a `DServerId`
            // renders in 1 to 20 decimal digits), so moving a wide-EC chunk onto
            // high-numbered servers grows the record; a flat root near [`MAX_VALUE_BYTES`]
            // — a flat map is bounded by that ceiling and nothing else, so a large flat
            // object's root sits right under it — is pushed past it by a legitimate
            // evacuation. That batch is then rejected by the backend **permanently**, from a
            // layer that cannot say which record was too big, failing every retry of that
            // repair. Refusing here makes it a typed, attributed refusal of one chunk's
            // move, before anything is written.
            //
            // On growth and not absolutely, because a flat root can **predate** this
            // module's ceiling discipline: `origin/main`'s `commit_chunk_map` puts a map of
            // any size, and redb, TiKV and etcd store it, so an object whose root is already
            // past the FoundationDB-tightest ceiling exists on stores that were never on
            // FoundationDB. Charging it absolutely would refuse to *repair* that object at
            // all — including a move onto narrower ids, which shrinks it — turning a record
            // one backend would reject into an object no pass can ever fix, which is the
            // worse failure. A repoint may not make a record's ceiling problem worse; it is
            // not the place that resolves one it inherited (the flat map's ceiling is
            // `PutObject`'s chunk-size selection, #508).
            //
            // The segmented arm needs no such carve-out: a `seg:` record has exactly one
            // writer ([`SegmentedPublication`]), which charges every batch it assembles, so
            // an over-ceiling segment record cannot have been written in the first place.
            if next_bytes.len() > prior_bytes.len() {
                check_value_ceiling(&root_key, &next_bytes)?;
            }
            Ok(WriteBatch::new()
                .require(root_key.clone(), prior_bytes)
                .put(root_key, next_bytes))
        }
        ChunkHome::Segment {
            key,
            prior,
            position,
        } => {
            // The home and the root must name the SAME generation, and the root must name
            // this segment. Without both checks a home resolved from a generation that has
            // since been retired, paired with the *current* root, satisfies every other
            // precondition this batch carries: `require(seg == prior)` holds against a
            // retired record the reaper has not reached yet, `require(inode == prior_root)`
            // holds against a root that never named it, and the repoint reports
            // `Committed` while writing into a record no consumer will ever read. The
            // repair is then silently lost — the caller has been told its fragment moved.
            let table = prior_root
                .chunk_map
                .segmented()
                .ok_or(ChunkMapError::MapShapeChanged)?;
            let (nonce, epoch, index) = parse_seg_key(key)?;
            if nonce != table.group().nonce() || epoch != table.group().epoch() {
                return Err(ChunkMapError::SegmentGroupMismatch {
                    root: format!("{}:{}", table.group().nonce(), table.group().epoch()),
                    home: format!("{nonce}:{epoch}"),
                }
                .into());
            }
            if !table.segments().iter().any(|s| s.index == index) {
                return Err(ChunkMapError::SegmentUnknown {
                    nonce,
                    epoch,
                    index,
                }
                .into());
            }
            // Through the record's own mutator, which moves the placement and nothing
            // else ([`SegmentRecord::repoint`]): the chunk's id, scheme and length are
            // taken from the record this CAS is preconditioned on, so a repoint can never
            // republish different content, nor leave a `byte_len` that no longer matches
            // its chunks — which would fail its own decode at the next resolve and take
            // the object with it.
            let mut record = decode_segment_record(key, prior)?;
            record.repoint(*position, placement)?;
            let batch = WriteBatch::new()
                .require(key.clone(), prior.clone())
                .require(root_key, encode(prior_root))
                .put(key.clone(), encode(&record));
            // **A repoint can make a record bigger, so the per-record ceiling is re-checked
            // here and not only at publication.** The placement vector keeps its length, but
            // not its width: a D-server id renders in 1 to 20 decimal digits, so moving a
            // wide-EC chunk's fragments onto high-numbered servers grows every entry of it.
            // The publication's 2x headroom ([`SEGMENT_TARGET_BYTES`]) is arithmetic about
            // the *chunk* encoding (`0016:218-232`), not a bound on later repoints, so
            // enough of them can carry a segment past [`MAX_VALUE_BYTES`]. Handing that
            // batch to the backend would fail the repair with a rejection no retry and no
            // finer split can fix, from a layer that cannot say which record was too big;
            // refusing here makes it a typed, attributed refusal of one chunk's move —
            // `ValueOverCeiling` names the `seg:` key — before anything is written, and
            // leaves the record exactly as it was.
            check_record_ceilings(&batch)?;
            Ok(batch)
        }
    }
}

/// The mutation that **reserves** a segment-group nonce: `require_absent(seggrp:<nonce>)`
/// plus the [`SEGGRP_MARKER`] record (`0016:502-527`).
///
/// Returned as a [`WriteBatch`] rather than committed, and that is the point of it: the
/// reservation is normatively part of the **session's own Create batch** (#636), because a
/// nonce minted in one transaction and reserved in a second is unreserved in between — the
/// window in which a second session mints the same nonce and, later, overwrites a live
/// object's segment records. The caller merges this into that batch; the `require_absent`
/// is what makes the mint lose rather than collide.
///
/// The nonce is validated through [`SegmentGroup`], so a nonce no `seg:` key could
/// reproduce cannot be reserved either — a reservation that did not name a usable group
/// would protect nothing.
pub fn reserve_segment_group(nonce: &str) -> std::result::Result<WriteBatch, ChunkMapError> {
    SegmentGroup::new(nonce, 0)?;
    Ok(WriteBatch::new()
        .require_absent(seggrp_key(nonce))
        .put(seggrp_key(nonce), Bytes::from_static(SEGGRP_MARKER)))
}

/// Whether any `seg:` record names `nonce` — the predicate the `seggrp:` marker's two-arm
/// lifetime rule needs (`0016:513-527`): a group that was never **adopted** has its marker
/// removed by the session's terminal delete, an adopted one keeps it until the last segment
/// naming it is gone. One bounded range read, exactly as the terminal delete already gates on
/// the `sidx:` range being empty. The arms themselves are the session slice's (#636); this
/// slice ships the predicate they gate on.
pub async fn segment_group_adopted(store: &impl MetadataStore, nonce: &str) -> Result<bool> {
    // **One row, not the range.** The question is existence, and the range spans every
    // epoch this group ever attempted — a session that crashed and retried often enough
    // has a range past [`SCAN_CAP`], where `scan` answers `ScanCapExceeded` rather than
    // "yes" (`crates/traits/src/lib.rs:1019-1023`). That failure lands on the terminal
    // delete this predicate gates, so the marker could never be cleaned up again: the more
    // rubbish a group accumulated, the less able the session would be to reclaim it. A
    // one-row page is bounded whatever the range holds (#634).
    let (rows, _) = store.scan_page(&seg_group_prefix(nonce), None, 1).await?;
    Ok(!rows.is_empty())
}

// ---------------------------------------------------------------------------
// Staged publication — write the segments, then flip the root (0016 decision 7(b))
// ---------------------------------------------------------------------------

/// A **staged publication** of a segmented chunk map: write the `seg:` records in
/// byte-budgeted batches, then flip the root in **one** batch (`0016:2332-2345`).
///
/// A segmented map cannot be published in one batch — that is why it needs staged
/// publication at all — so the phases are separate and the flip is the publication
/// instant: the segments are already durable when it commits.
///
/// **The caller contributes to the flip batch, not merely preconditions to it.** The root
/// flip is normatively one batch that also carries the publishing session's own mutations
/// (`session → Completed`, `retire:records:{parts}`, any `retire:bytes:`, inventory row
/// `0016:654-663`). A committer that accepted only preconditions would force those into a
/// *second* batch, and a crash between the two would either publish an object whose parts
/// are never retired or retire parts for a publication that never landed. So [`Self::flip`]
/// merges the caller's whole [`WriteBatch`] — its preconditions **and** its mutations —
/// into the batch that carries the root CAS: they commit together or not at all.
pub struct SegmentedPublication<'a> {
    /// The inode whose root is flipped.
    pub inode_id: InodeId,
    /// What the flip's root precondition is: a freshly minted key, or the committed
    /// generation this publication supersedes.
    pub root: RootPrecondition<'a>,
    /// The segment group (nonce + `Completing` fence epoch) the segments are keyed by.
    pub group: SegmentGroup,
    /// The full ordered chunk list to publish.
    pub chunks: Vec<ChunkRef>,
    /// The object metadata to stamp at publication (ADR-0047).
    pub meta: ObjectMeta,
    /// The record this publication is **fenced on** — `mpu:<id>`, the session record that
    /// holds `Completing@E` (`0016:2336-2338`). Every segment-write batch and the flip
    /// must state a value precondition on *this* key, or the builder refuses them
    /// ([`ChunkMapError::Unfenced`]).
    ///
    /// It is declared once, here, rather than inferred from whatever preconditions the
    /// caller happened to attach: the fence's job is to make this publisher lose to the
    /// rollback that deletes its `seg:` records, and only a precondition on the record
    /// that rollback moves can do that. A publication whose phase-1 batches pinned one
    /// record and whose flip pinned another would leave exactly the window between them
    /// open. The committer does not interpret the value — the session's states are
    /// #636's — it only requires that every batch pin this record.
    pub fence_key: Vec<u8>,
    /// The caller's contribution to **each** segment-write batch — its fence *and* its
    /// own mutations — called once per batch with [`SegmentBatchInfo`]. Both the returned
    /// preconditions and the returned mutations are merged into that batch.
    ///
    /// **Required, and it must fence on [`Self::fence_key`].** A segment-write phase with
    /// no fence is refused ([`ChunkMapError::Unfenced`]): the rollback that reclaims a
    /// crashed attempt deletes that attempt's `seg:` records, and only the fence makes
    /// this publisher lose to it. Unfenced, the deletion can land between the segment
    /// phase and the flip — and the flip's root compare-and-swap, which the rollback never
    /// touches, still succeeds, publishing a live root that names records that are gone.
    ///
    /// The optionality is what made that reachable, so there is no `None`: a caller with
    /// no per-batch state of its own still has to say what it is publishing under.
    ///
    /// **It is one hook, per batch, deliberately: the fence and the progress cursor are
    /// the same record.** The fence is `require(mpu == Completing@E)` (`0016:2336-2338`)
    /// — an *exact-value* precondition on the session record — and the recoverable-progress
    /// rule has that same batch CAS the session's `segments_written` cursor forward
    /// (`0016:350`, decision 7(c)/(d)). A fence pinned once for the whole phase would
    /// therefore be stale the moment the first batch commits, and **every later batch of a
    /// multi-batch publication would `Conflict` against the value its own predecessor
    /// wrote** — a publication large enough to need staging could never finish. So the
    /// caller states the fence for the batch in front of it, from
    /// [`SegmentBatchInfo::first_index`] (the cursor value this batch requires) and
    /// [`SegmentBatchInfo::written`] (the value it leaves behind).
    ///
    /// The contribution is charged against the batch's byte envelope like any other
    /// mutation — see [`Self::segment_batches`].
    ///
    /// **It must be a pure function of its [`SegmentBatchInfo`], and it is asked more often
    /// than the phase commits.** The split is re-run against the contribution it produced
    /// until every assembled batch fits ([`Self::segment_batches`]), a resumed publication
    /// re-derives the states its durable prefix moved through one cursor position at a time
    /// ([`Self::durable_prefix_contributions`]), and a publication assembles its phase both
    /// before and after its range read ([`Self::publish`]) — so the same info is asked
    /// repeatedly, and cursor positions that are already durable are asked about at all. A
    /// hook that *consumed* something per call (an id, a token, a counter) would spend it on
    /// batches that never commit. What the protocol's own contribution does is exactly this:
    /// state the fence value the cursor at `first_index` implies and the value `written`
    /// leaves (`0016:350`,`:2336-2338`), which is a function of the info and nothing else.
    pub segment_batch: &'a dyn Fn(&SegmentBatchInfo) -> WriteBatch,
    /// How many leading segments are **already durable** — a recovered `segments_written`
    /// cursor (`0016:350`). Their records are re-derived identically and then skipped, so
    /// a completer that crashed mid-phase resumes where it stopped instead of re-committing
    /// progress its own fence has already moved past. `0` for a fresh attempt; the whole
    /// plan means there is nothing left to write and the phase is a no-op.
    pub resume_from: u32,
    /// The transaction envelope one segment-write batch stays inside, in bytes.
    /// `None` is [`MAX_BATCH_BYTES`], and a larger value is **clamped** to it: that
    /// ceiling is inherited from the tightest backend in play, not negotiable by a caller
    /// (`crates/traits/src/lib.rs:747-752`). A smaller one is honoured — a backend with a
    /// tighter envelope, or a caller that wants more, smaller commit points.
    pub batch_budget: Option<usize>,
    /// The same envelope in **operations** — `B_ops` (`0016:640-648`). `None` is
    /// [`MAX_BATCH_OPS`], a larger value is clamped to it, a smaller one is honoured.
    ///
    /// Bytes alone do not bound a transaction: a batch of many tiny mutations or
    /// preconditions is inside every size limit and still spends the backend's *deadline*
    /// on sequential round trips, and a batch that always times out can never commit at
    /// any retry. So the split is `min(B_bytes, B_ops)`, and the unsplittable flip is
    /// refused rather than emitted over the cap.
    pub ops_budget: Option<usize>,
    /// The caller's contribution to the **flip** batch: its own preconditions and its own
    /// mutations, committed atomically with the root CAS.
    pub flip: WriteBatch,
}

/// What the flip's root compare-and-swap requires — the two shapes a publication can
/// have (`0016:350`, the `publish_target` row: "absent ⇒ create with a freshly minted id;
/// present ⇒ CAS the inode it names, with `version = prior.version + 1` from that re-read
/// prior").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootPrecondition<'a> {
    /// The dirent resolved to **no** inode, so the completer minted a fresh id: the flip
    /// requires the key to be **absent** and publishes version 1. A committer that could
    /// only supersede would make this — the ordinary first publication of an object key —
    /// impossible to express.
    Fresh,
    /// The dirent named a committed generation: the flip CASes its exact bytes and
    /// publishes `prior.version + 1`, computed from the re-read prior at *this* attempt.
    Supersede(&'a InodeRecord),
}

/// Which segment-write batch a [`SegmentedPublication::segment_batch`] hook is being
/// asked to contribute to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentBatchInfo {
    /// This batch's 0-based position in the phase.
    pub batch: usize,
    /// How many batches the phase has in total.
    pub batches: usize,
    /// The first segment index this batch writes — and, because segment indices are
    /// 0-based and contiguous, **how many segments were already durable before it**: the
    /// `segments_written` value this batch's fence must require.
    pub first_index: u32,
    /// The last segment index this batch writes.
    pub last_index: u32,
    /// How many segments are durable **once this batch commits** — the value a
    /// `segments_written` cursor takes (`0016:350`).
    pub written: u32,
}

/// How much of a publication's plan must **already** be durable when
/// [`SegmentedPublication::verify_durable_range`] runs — the one difference between the
/// check the segment-write phase makes and the check the flip makes.
///
/// They are different questions, and answering the flip's with the phase's is how a live
/// root ends up naming records nobody wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableRange {
    /// Only the prefix the caller's `resume_from` cursor claims. What phase 1 requires: it
    /// is about to write everything above the cursor itself.
    ResumePrefix,
    /// **Every** planned segment. What the flip requires, because the root it publishes
    /// names all of them and nothing after it will ever write one.
    WholePlan,
}

/// The planned segment indices a publication's own range read has just found **durable and
/// byte-identical to what it plans to write** ([`SegmentedPublication::verify_durable_range`]).
///
/// It is a witness, not a cache: the bytes are not carried, because a row is only in here
/// when the store's copy of it already equals `encode(record)` for this plan's record at that
/// index, which the read compared. What the segment phase does with it is decide the shape of
/// each row's precondition ([`SegmentedPublication::assemble_segment_batches`]) — an absent
/// row is created under `require_absent`, a durable one is pinned with `require` and not
/// rewritten — so that no batch of the phase can overwrite a row the publication did not see.
///
/// Empty by default, and that default is the safe one: with no witness every row is treated
/// as the publication's to create, which is the fresh-publication shape and refuses (rather
/// than silently overwrites) anything already there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DurableSegments(BTreeSet<u32>);

impl DurableSegments {
    /// Whether the store was witnessed holding this plan's exact record at `index`.
    fn holds(&self, index: u32) -> bool {
        self.0.contains(&index)
    }

    /// Whether nothing was witnessed — the fresh-publication case, where the batches the
    /// split measured are already the batches to commit.
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl SegmentedPublication<'_> {
    /// Split the chunk list into **byte-budgeted** segments: each segment record is
    /// filled until its encoded size would pass [`SEGMENT_TARGET_BYTES`] (the value
    /// ceiling with the 2x headroom of `0016:220-223`), never to a fixed chunk count.
    pub fn plan(&self) -> std::result::Result<Vec<(SegmentRef, SegmentRecord)>, ChunkMapError> {
        self.plan_with(SEGMENT_TARGET_BYTES, MAX_ROOT_SEGMENTS)
    }

    /// [`Self::plan`] with the two capacity constants as parameters — the production
    /// path, called with the real constants by `plan()`. Taking them as arguments is what
    /// lets the `MAX_ROOT_SEGMENTS` ceiling and the split arithmetic be exercised at a
    /// small scale instead of only by a ~500 000-chunk fixture no test can afford; an
    /// untested ceiling is exactly the off-by-one the >10 GiB requirement turns on.
    fn plan_with(
        &self,
        segment_target: usize,
        max_segments: usize,
    ) -> std::result::Result<Vec<(SegmentRef, SegmentRecord)>, ChunkMapError> {
        let mut segments: Vec<(SegmentRef, SegmentRecord)> = Vec::new();
        let mut current: Vec<ChunkRef> = Vec::new();
        let mut current_bytes = SEGMENT_ENVELOPE_BYTES;
        let mut byte_offset: u64 = 0;
        for (position, chunk) in self.chunks.iter().enumerate() {
            // A publication is a **write**, and the write boundary is strict (ADR-0040
            // decisions 3–4): a placement vector is well formed when it is empty (the
            // pre-M3 identity fallback) or exactly `fragment_count()` long. A non-empty
            // short one persisted here would be identity-filled by every reader — sending
            // them to D servers holding nothing for this chunk — and rejected by the
            // maintenance paths that are strict about the same field. Refused with the
            // rest of the deterministic refusals, before anything is durable.
            if let Err(malformed) = chunk.checked_fragments() {
                return Err(ChunkMapError::WritePlacementMalformed {
                    position,
                    expected: malformed.expected,
                    actual: malformed.actual,
                });
            }
            // And an **empty** one is refused here too, though it is well formed elsewhere.
            // The pre-M3 identity fallback is drained by the backfill pass, which fills an
            // empty placement with an inode compare-and-swap — a move that does not exist
            // for a segmented map, whose `ChunkRef`s live in `seg:` records
            // (`crates/custodian/src/backfill.rs:167-173`). Publishing one would create the
            // record that pass calls structurally impossible and raises on
            // (`SegmentedPlacementUnfillable`, `backfill.rs:220-227`): an object no pass in
            // the system can drain, and a backfill run that fails on every future pass over
            // the whole store because of it. So this write boundary is strict where the
            // read boundary stays liberal (ADR-0040 decisions 3–4, ADR-0045) — the premise
            // downstream states is made true by construction here, at the only code path
            // that can publish a segmented chunk into a `seg:` record.
            if chunk.placement.is_empty() {
                return Err(ChunkMapError::WritePlacementEmpty {
                    position,
                    expected: chunk.fragment_count(),
                });
            }
            let chunk_bytes = encode(chunk).len() + 1;
            if !current.is_empty() && current_bytes + chunk_bytes > segment_target {
                byte_offset = push_segment(&mut segments, &mut current, byte_offset)?;
                current_bytes = SEGMENT_ENVELOPE_BYTES;
            }
            current.push(chunk.clone());
            current_bytes += chunk_bytes;
        }
        if !current.is_empty() {
            push_segment(&mut segments, &mut current, byte_offset)?;
        }
        if segments.is_empty() {
            return Err(ChunkMapError::NoSegments);
        }
        if segments.len() > max_segments {
            return Err(ChunkMapError::TooManySegments {
                segments: segments.len(),
            });
        }
        Ok(segments)
    }

    /// The root record the flip installs: the segment table, the published size (the sum
    /// of the chunk lengths), the next version, and the publication metadata.
    ///
    /// The version is `1` for a [`RootPrecondition::Fresh`] key — the same version
    /// [`InodeRecord::new_empty`] mints — and `prior.version + 1` for a supersede, taken
    /// from the prior this attempt actually re-read (`0016:350`).
    ///
    /// **A prior that is itself SEGMENTED is refused**
    /// ([`ChunkMapError::SegmentedRetirementUnsupported`]), with the rest of the
    /// deterministic, zero-I/O refusals and before the first `seg:` record is written. The
    /// flip would replace that generation's root, and nothing in this slice retires the
    /// generation it replaced: its `seg:` records would be named by no root — invisible to
    /// the resolver, so invisible to GC's reference build, to restore, to rebalance — while
    /// every fragment they alone name stayed referenced by nothing and reclaimed by nobody.
    /// Retiring a segmented generation is the staged `retire:bytes:{generation}` obligation
    /// of proposal 0016 decision 4 / 7(f) (fan-out up to ~1.78 M fragment orphans plus the
    /// segment deletes, drained in byte-budgeted batches, `0016:668`), which the multipart
    /// session slice (#636) owns. The two flat committers refuse the same shape for the same
    /// reason ([`commit_chunk_map`], [`commit_chunk_map_superseding`]) — this is the third
    /// door into it, and the only one that could have published *over* a segmented root.
    pub fn root(&self) -> std::result::Result<InodeRecord, ChunkMapError> {
        if let RootPrecondition::Supersede(prior) = self.root {
            if prior.chunk_map.is_segmented() {
                return Err(ChunkMapError::SegmentedRetirementUnsupported {
                    operation: "publishing a segmented map over a segmented generation",
                });
            }
        }
        let segments = self.plan()?;
        let map = SegmentedMap::new(
            self.group.clone(),
            segments.iter().map(|(r, _)| *r).collect(),
        )?;
        // The published `size` is the table's own span, so the record satisfies the
        // size-vs-table invariant its decode enforces by construction.
        let size = map.span();
        Ok(InodeRecord {
            size,
            chunk_map: ChunkMap::Segmented(map),
            state: InodeState::Committed,
            version: match self.root {
                RootPrecondition::Fresh => 1,
                // **Checked, not `+ 1`.** The prior is a record read from the store, so its
                // `version` is whatever the bytes said; at `u64::MAX` an unchecked increment
                // panics with overflow checks on and wraps to zero without them — and a
                // version that went backwards is worse than a refusal, because every CAS in
                // this module compares versions to decide who won. Fail closed instead,
                // through the one helper every version-advancing site uses.
                RootPrecondition::Supersede(prior) => next_root_version(prior)?,
            },
            etag: self.meta.etag.clone(),
            content_type: self.meta.content_type.clone(),
            modified: self.meta.modified,
        })
    }

    /// The segment-write batches, **each inside the byte envelope as a whole batch** —
    /// segment records plus the caller's per-batch contribution — and each carrying that
    /// contribution. Exposed so a caller (and the test oracle) can measure the batches
    /// rather than trust a record count.
    ///
    /// The envelope is charged on [`batch_bytes`], not on the segment puts alone: the
    /// backend's transaction limit counts every key, value and precondition in the
    /// transaction, so a split that charged only its own records would hand the backend a
    /// batch it must reject **permanently** — a publication that can never complete,
    /// wearing a `Conflict`-shaped failure it cannot retry out of. When the contribution
    /// cannot be made to fit, this fails closed with
    /// [`ChunkMapError::BatchOverBudget`](ChunkMapError::BatchOverBudget) instead.
    pub fn segment_batches(&self) -> std::result::Result<Vec<WriteBatch>, ChunkMapError> {
        self.segment_batches_over(&DurableSegments::default())
    }

    /// [`Self::segment_batches`] over the rows a [`Self::verify_durable_range`] read has
    /// just **witnessed durable, byte-for-byte** — the batches the two writing entry points
    /// actually commit ([`Self::write_segments`], [`Self::publish`]).
    ///
    /// The witness changes no batch's *boundaries* and no batch's *cost* (see
    /// [`Self::staged_batches_over`]); it changes only whether a row is written or pinned.
    fn segment_batches_over(
        &self,
        durable: &DurableSegments,
    ) -> std::result::Result<Vec<WriteBatch>, ChunkMapError> {
        self.staged_batches_over(
            SEGMENT_TARGET_BYTES,
            MAX_ROOT_SEGMENTS,
            self.batch_budget.unwrap_or(MAX_BATCH_BYTES),
            self.ops_budget.unwrap_or(MAX_BATCH_OPS),
            durable,
        )
    }

    /// [`Self::segment_batches`] with the three capacity constants as parameters, over an
    /// empty witness — the shape a **fresh** publication assembles, which is what the
    /// envelope tests measure. At the real budget the split first fires past ~113 000
    /// chunks, so without parameters the multi-batch path (and the fence and the progress
    /// hook riding on *each* batch) could only be exercised by a fixture no unit test — let
    /// alone a mutation run that repeats the suite per mutant — can afford, which is how an
    /// envelope this load-bearing ends up asserted by a one-element vector.
    ///
    /// Test-only, because production reaches the same body through
    /// [`Self::staged_batches_over`] with the witness its range read produced; the
    /// arithmetic under test is the same body either way.
    #[cfg(test)]
    fn staged_batches(
        &self,
        segment_target: usize,
        max_segments: usize,
        budget: usize,
        ops_budget: usize,
    ) -> std::result::Result<Vec<WriteBatch>, ChunkMapError> {
        self.staged_batches_over(
            segment_target,
            max_segments,
            budget,
            ops_budget,
            &DurableSegments::default(),
        )
    }

    /// [`Self::staged_batches`] over a [`DurableSegments`] witness.
    ///
    /// **The witness does not move the split**, and it does not merely *tend* not to: every
    /// planned row is charged at the cost of the shape it takes when it is *not* yet durable
    /// — its key and value, plus the [`WriteBatch::require_absent`] guarding them, and two
    /// operations for the pair — **and** the contribution-reserve loop assembles and measures
    /// the unwitnessed shape throughout, applying the witness only to the ranges it has
    /// settled on. So the batch boundaries, and with them the [`SegmentBatchInfo`] sequence
    /// the caller's fence and cursor are derived from, are a function of the plan alone: the
    /// batches a publication assembles before its range read ([`Self::publish`], where every
    /// zero-I/O refusal is decided) are the same batches, fence for fence, as the ones it
    /// assembles after it. Measuring the witnessed shape instead would let the loop converge
    /// in a different number of passes — a *finer* split for the same publication — and the
    /// flip would then be charged against a fence trajectory the phase never committed.
    fn staged_batches_over(
        &self,
        segment_target: usize,
        max_segments: usize,
        budget: usize,
        ops_budget: usize,
        durable: &DurableSegments,
    ) -> std::result::Result<Vec<WriteBatch>, ChunkMapError> {
        let planned = self.plan_with(segment_target, max_segments)?;
        let resumed = usize::try_from(self.resume_from).unwrap_or(usize::MAX);
        if resumed > planned.len() {
            return Err(ChunkMapError::ResumePastPlan {
                resume_from: self.resume_from,
                segments: planned.len(),
            });
        }
        let pending = &planned[resumed..];
        let encoded: Vec<(Vec<u8>, Bytes)> = pending
            .iter()
            .map(|(segment, record)| (seg_key(&self.group, segment.index), encode(record)))
            .collect();
        // The **guarded** cost of a row: its key and value, plus the `require_absent` that
        // makes the write a compare-and-swap rather than a blind overwrite (a precondition
        // with no expected value is charged its key alone, `batch_bytes`). Charged for every
        // planned row, witnessed or not, so the split is witness-independent.
        let costs: Vec<usize> = encoded
            .iter()
            .map(|(k, v)| k.len() + v.len() + k.len())
            .collect();
        let budget = budget.min(MAX_BATCH_BYTES);
        let ops_budget = ops_budget.min(MAX_BATCH_OPS);

        // The caller's per-batch contribution is part of the same transaction, so it is
        // charged against the same envelope — in **both** currencies, bytes and operations
        // — but its size is only known once the split is, and a finer split can make the
        // contribution *bigger* (one cursor CAS per batch). So the split is re-run against
        // the contribution it actually produced, reserving room for it, until every
        // assembled batch fits. It converges in one extra pass for any contribution whose
        // size does not depend on the batch count; when it cannot converge it fails
        // **closed** rather than emitting a transaction the backend can only reject
        // permanently.
        let mut reserve = 0usize;
        let mut reserve_ops = 0usize;
        let mut worst: Option<ChunkMapError> = None;
        for _ in 0..MAX_SPLIT_ATTEMPTS {
            let ranges = batch_ranges(
                &costs,
                budget.saturating_sub(reserve),
                // In **records**, so the caller's operation budget is spent in the currency
                // the split can actually divide: a segment row costs
                // [`SEGMENT_OPS_PER_RECORD`] operations, not one.
                ops_budget.saturating_sub(reserve_ops) / SEGMENT_OPS_PER_RECORD,
            );
            // Assembled **unwitnessed**, always: every rule below — the ceilings, the fence,
            // the cycle, and both envelopes — is therefore evaluated on one shape, so this
            // loop's control flow, and with it the batch boundaries it settles on, cannot
            // depend on what the store happened to hold. A witnessed batch is a subset of
            // the unwitnessed one (one precondition instead of a precondition and a put,
            // over the same key and value), so every check that passes here passes for it
            // too; the witness is applied once, to the split this loop has already agreed.
            let batches = self.assemble_segment_batches(
                pending,
                &encoded,
                &ranges,
                &DurableSegments::default(),
            )?;
            // Per-record ceilings and the fence are settled BEFORE the envelope loop: no
            // split makes an over-ceiling value fit, and an unfenced batch is refused
            // whatever its size — so re-splitting against either would only delay the same
            // answer.
            for (n, batch) in batches.iter().enumerate() {
                check_record_ceilings(batch)?;
                check_fenced(batch, &self.fence_key, PHASE_SEGMENTS, n)?;
                check_fence_transitioned(batch, &self.fence_key, PHASE_SEGMENTS, n)?;
            }
            // …and the phase as a whole, which no per-batch rule can see: a contribution
            // that moves the fence at every batch can still walk it back to a state it has
            // already held, and an unrepeatable fence is the whole of its protection.
            //
            // **The phase starts where the PHASE started, not where the cursor did.** A
            // resumed attempt commits only `planned[resume_from..]`, but the fence has
            // already moved through the states the durable prefix's batches carried, and a
            // batch of this attempt that returns to one of them re-opens every precondition
            // satisfiable there — the rollback that reclaims a crashed attempt, and a second
            // completer of the same generation. Those batches are re-derived from this same
            // plan and this same hook ([`Self::durable_prefix_contributions`]), one cursor
            // position at a time, so the reconstruction cannot miss a state by guessing the
            // earlier attempt's batch boundaries differently than it chose them.
            let prefix = self.durable_prefix_contributions();
            check_fence_never_cycles(
                prefix.iter().chain(batches.iter()),
                &self.fence_key,
                PHASE_SEGMENTS,
            )?;
            let measured: Vec<Measured> = batches
                .iter()
                .zip(&ranges)
                .enumerate()
                .map(|(n, (batch, range))| Measured {
                    batch: n,
                    bytes: batch_bytes(batch),
                    segment_bytes: costs[range.clone()].iter().sum(),
                    ops: batch_ops(batch),
                    segment_ops: range.len() * SEGMENT_OPS_PER_RECORD,
                })
                .collect();
            // Each currency is classified once, where it is measured, so the refusal
            // always names the envelope that was actually breached — a batch at exactly
            // the byte budget with too many operations is an operation failure, and
            // saying `BatchOverBudget` would send the caller after the wrong knob.
            let over: Vec<(&Measured, ChunkMapError)> = measured
                .iter()
                .filter_map(|m| {
                    if m.bytes > budget {
                        Some((
                            m,
                            ChunkMapError::BatchOverBudget {
                                batch: m.batch,
                                bytes: m.bytes,
                                budget,
                            },
                        ))
                    } else if m.ops > ops_budget {
                        Some((
                            m,
                            ChunkMapError::BatchOverOps {
                                batch: m.batch,
                                ops: m.ops,
                                cap: ops_budget,
                            },
                        ))
                    } else {
                        None
                    }
                })
                .collect();
            let Some((_, first)) = over.first() else {
                // The split is settled. Only now is the witness applied — to these exact
                // ranges, so the batches that commit are the batches that were measured,
                // one precondition per row either way.
                return if durable.is_empty() {
                    Ok(batches)
                } else {
                    self.assemble_segment_batches(pending, &encoded, &ranges, durable)
                };
            };
            worst = Some(first.clone());
            // How much of the offending batch is NOT segment records — the room the next
            // split must reserve, in each currency. If reserving what we just measured
            // moves neither reserve, the split is already as fine as it goes: one segment
            // record plus this caller's contribution does not fit the envelope at all.
            //
            // Both sides describe the same unwitnessed batches — `costs` charges each row its
            // key, value and guard, and that is what these batches carry — so the difference
            // is exact. It is spelled `saturating_sub` because nothing in the types pins that:
            // a later change that measured a *witnessed* batch against the unwitnessed charge
            // would underflow, and a panic inside a refusal path is a crash where a typed
            // error belongs. The invariant itself is asserted, not assumed
            // ([`the_durable_witness_never_moves_the_split`]).
            let contribution = over
                .iter()
                .map(|(m, _)| m.bytes.saturating_sub(m.segment_bytes))
                .max()
                .unwrap_or(0);
            let contribution_ops = over
                .iter()
                .map(|(m, _)| m.ops.saturating_sub(m.segment_ops))
                .max()
                .unwrap_or(0);
            let next = (reserve.max(contribution), reserve_ops.max(contribution_ops));
            if next == (reserve, reserve_ops) {
                break;
            }
            (reserve, reserve_ops) = next;
        }
        Err(worst.unwrap_or(ChunkMapError::BatchOverBudget {
            batch: 0,
            bytes: 0,
            budget,
        }))
    }

    /// The fence states a resumed publication has **already moved through** — the states its
    /// durable prefix's batches carried, re-derived from this plan and this hook.
    ///
    /// Only the caller's contribution is built, never the segment puts: those records are
    /// already durable and this attempt must not re-commit them. They exist so the cycle rule
    /// sees the whole trajectory ([`Self::staged_batches`], [`Self::flip_batch_after`]) — a
    /// later batch, or the flip, that returns the fence to a state the prefix held re-arms
    /// every precondition satisfiable there, and for a recovery that resumes after a
    /// *complete* phase 1 the prefix is the only history there is.
    ///
    /// **The reconstruction is deliberately split-independent, and that is what makes it
    /// sound.** The earlier attempt's batch boundaries are not recoverable — nothing durable
    /// records them, only the cursor — so this asks the hook for **every** cursor position the
    /// prefix passes, one segment at a time. For the contribution the protocol describes,
    /// keyed on the cursor the batch requires and the cursor it leaves (`0016:350`,
    /// `:2336-2338`), the states of *any* coarser split are a subset of these: a batch
    /// spanning `[i..j]` pins what the one-segment batch at `i` pins and puts what the one at
    /// `j-1` puts. So no state the prefix actually visited can be missed, whatever budget,
    /// operation cap, or contribution-reserve fixed point produced its batches.
    ///
    /// What it cannot reconstruct is a hook that keys on the **batch ordinal** rather than the
    /// cursor (`batch` / `batches` describe this walk, not the earlier attempt's). Such a hook
    /// is already outside the protocol — its fence would not be the session's `segments_written`
    /// CAS — and the answer is to key on the cursor, never to check nothing.
    ///
    /// Infallible on purpose: an empty prefix, or a plan this attempt cannot even compute,
    /// yields no states rather than a second refusal, because every path that would write or
    /// publish raises that refusal itself.
    fn durable_prefix_contributions(&self) -> Vec<WriteBatch> {
        let Ok(planned) = self.plan() else {
            return Vec::new();
        };
        let resumed = usize::try_from(self.resume_from).unwrap_or(usize::MAX);
        let prefix = &planned[..resumed.min(planned.len())];
        prefix
            .iter()
            .enumerate()
            .map(|(n, (segment, _))| {
                (self.segment_batch)(&SegmentBatchInfo {
                    batch: n,
                    batches: prefix.len(),
                    first_index: segment.index,
                    last_index: segment.index,
                    written: segment.index + 1,
                })
            })
            .collect()
    }

    /// One [`WriteBatch`] per range: that range's segment records, plus the caller's
    /// contribution for that batch — refused if that contribution writes a record this
    /// publication owns ([`ChunkMapError::ContributionCollides`]).
    ///
    /// **No segment row is ever written blind.** Each one carries a precondition on its own
    /// key, and which one depends on what the publication's range read witnessed
    /// ([`DurableSegments`], from [`Self::verify_durable_range`]):
    ///
    /// * **not witnessed** — `require_absent(key)` beside the put. The row is the
    ///   publication's to create, so a row that exists by the time the batch commits is
    ///   somebody else's write into this generation's range, and the batch loses
    ///   ([`CommitOutcome::Conflict`]) instead of overwriting it. Blind, it would merge two
    ///   attempts' plans into one range — a **hybrid** map that resolves cleanly and reads
    ///   the wrong bytes.
    /// * **witnessed durable, byte-for-byte** — `require(key, value)` and *no* put. The
    ///   record the store holds is already this plan's, so re-writing it can only lose a
    ///   race: between the range read and this commit, a live generation's maintenance
    ///   repoint ([`repoint_chunk`], a `require(seg == prior)` CAS) may have moved a
    ///   fragment's placement into that very row, and a blind put would **restore the stale
    ///   placement** — republishing fragments the repoint has already orphaned. This is the
    ///   ambiguous-flip recovery: it re-derives the same plan, cannot tell that its flip
    ///   landed, and re-runs a phase whose rows are now a live object's. Pinned, it commits
    ///   only while the row is untouched and `Conflict`s the moment it is not; the recovery
    ///   then re-reads and finds its own root published.
    ///
    /// Both shapes carry exactly one precondition per row, and the split charges the larger
    /// of them for every row ([`Self::staged_batches_over`]).
    fn assemble_segment_batches(
        &self,
        pending: &[(SegmentRef, SegmentRecord)],
        encoded: &[(Vec<u8>, Bytes)],
        ranges: &[std::ops::Range<usize>],
        durable: &DurableSegments,
    ) -> std::result::Result<Vec<WriteBatch>, ChunkMapError> {
        let owned = self.owned_keys();
        let mut batches = Vec::with_capacity(ranges.len());
        for (n, range) in ranges.iter().enumerate() {
            let mut batch = WriteBatch::new();
            for (position, (key, value)) in encoded[range.clone()].iter().enumerate() {
                batch = if durable.holds(pending[range.start + position].0.index) {
                    batch.require(key.clone(), value.clone())
                } else {
                    batch
                        .require_absent(key.clone())
                        .put(key.clone(), value.clone())
                };
            }
            let last = pending[range.end - 1].0.index;
            let info = SegmentBatchInfo {
                batch: n,
                batches: ranges.len(),
                first_index: pending[range.start].0.index,
                last_index: last,
                // Absolute, so a RESUMED phase's cursor keeps counting from what is
                // already durable rather than restarting at this attempt's first batch.
                written: last + 1,
            };
            merge_contribution(
                &mut batch,
                &(self.segment_batch)(&info),
                &owned,
                PHASE_SEGMENTS,
            )?;
            batches.push(batch);
        }
        Ok(batches)
    }

    /// The records this publication writes itself, which a caller's contribution may
    /// therefore not write ([`merge_contribution`]).
    fn owned_keys(&self) -> OwnedKeys {
        OwnedKeys {
            root: inode_key(self.inode_id),
            segments: seg_range_prefix(&self.group),
            marker: seggrp_key(self.group.nonce()),
        }
    }

    /// The **flip** batch: the root CAS plus the caller's preconditions and mutations, in
    /// ONE batch (`0016:2338-2345`). A [`RootPrecondition::Fresh`] publication requires
    /// the key to be absent; a supersede requires the prior's exact bytes.
    ///
    /// The caller's contribution here is unbounded in principle (`session → Completed`,
    /// `retire:records:{parts}`, any `retire:bytes:`, inventory row `0016:654-663`), so
    /// the assembled batch is charged against **both** limits the tightest backend states
    /// — the per-record ceilings ([`MAX_VALUE_BYTES`], [`MAX_KEY_BYTES`]) and the
    /// transaction envelope ([`MAX_BATCH_BYTES`]) — and refused rather than handed to a
    /// backend that can only reject it. The two are independent: a 200 KB caller value is
    /// far inside a 5 MB envelope and is still rejected permanently. The flip cannot be
    /// split: it *is* the publication instant.
    ///
    /// It must also be **fenced on [`Self::fence_key`]**: the flip carries the caller's own
    /// preconditions, and one that does not pin the publication's fence record is refused
    /// ([`ChunkMapError::Unfenced`]) — see that variant for the
    /// live-root-naming-deleted-segments hazard it closes.
    ///
    /// And it must **move** that record, not merely pin it
    /// ([`ChunkMapError::FenceNotTransitioned`]): the publication instant is normatively the
    /// batch that carries `session → Completed` (`0016:2338-2345`), and a flip that leaves
    /// the fence value unchanged leaves the racing rollback's own precondition satisfied, so
    /// it commits *after* the flip and deletes the segments the now-live root names.
    ///
    /// **Moving it is not enough on its own: it must move somewhere the publication has not
    /// already been** ([`ChunkMapError::FenceCycled`]). The cycle rule is a rule about the
    /// whole publication, not about a phase — a flip that takes the fence *back* to a value
    /// an earlier segment-write batch pinned satisfies every per-batch rule and still
    /// re-arms the rollback keyed on that value, which then deletes the segments the root
    /// this batch publishes already names. So the sequence checked here is the segment phase
    /// **plus** this flip, in commit order; assembling it is why the batch cannot be built
    /// without the phase in front of it.
    ///
    /// For a **resumed** publication the phase includes the states the durable prefix moved
    /// through ([`Self::durable_prefix_contributions`]): a recovery that flips after a
    /// completed phase 1 has nothing left to write, so a flip charged only against *this*
    /// attempt's batches would be charged against nothing at all and could restore any
    /// pre-cursor state.
    ///
    /// **Not public, deliberately.** This is the batch and not the publication instant: it
    /// carries the root mutation without the check that the range it names is durable, which
    /// only [`Self::flip`] can make ([`DurableRange::WholePlan`], one bounded range read). A
    /// caller holding this could commit a live root over segment records nobody wrote — the
    /// permanent, silent loss `flip` exists to refuse — so the flip is reachable only through
    /// the two entry points that verify first, and this stays the module's own.
    fn flip_batch(&self) -> std::result::Result<WriteBatch, ChunkMapError> {
        // **The phase's refusal is this batch's refusal too, and it is raised, not
        // swallowed.** The flip is charged against the states the segment phase holds, so a
        // phase that cannot be assembled leaves the cycle rule charged against *nothing* —
        // and a bare recovery `flip` (the one route here that does not build the phase
        // itself) would then publish a live root for a publication whose own phase 1 is
        // unfenced, fence-cyclic, or over the envelope: a generation whose segments this
        // committer would refuse to write, given a root that names them for ever.
        //
        // The two writing entry points ([`Self::write_segments`], [`Self::publish`]) raise
        // the same error first, so nothing changes for them; what this closes is the caller
        // that reaches the flip on its own. A phase with nothing left to write — the
        // recovery this call exists for, `resume_from` at the end of the plan — assembles to
        // an empty phase, which is `Ok`, so the legitimate route is unaffected.
        let phase = self.segment_batches()?;
        self.flip_batch_after(&phase)
    }

    /// [`Self::flip_batch`] over a segment phase the caller has **already** assembled —
    /// the production body, so a publication that builds both ([`Self::publish`]) splits
    /// the plan once instead of twice.
    ///
    /// `phase` must be the batches this same publication's segment-write phase would commit
    /// ([`Self::segment_batches`], whose own rules — including the cycle rule *within* the
    /// phase — have therefore already run). That is what makes the refusal this adds
    /// attributable to the flip: it is the last batch in the sequence, so a state that
    /// repeats here is one the flip brought back.
    ///
    /// The states of the **durable prefix** are prepended here rather than expected from the
    /// caller ([`Self::prefix_contributions`]), so every route to a flip — `publish`, a
    /// recovery's bare `flip`, a caller measuring `flip_batch` — is charged against the same
    /// trajectory, including the one where this attempt writes no segment at all.
    fn flip_batch_after(
        &self,
        phase: &[WriteBatch],
    ) -> std::result::Result<WriteBatch, ChunkMapError> {
        check_fenced(&self.flip, &self.fence_key, PHASE_FLIP, 0)?;
        check_fence_transitioned(&self.flip, &self.fence_key, PHASE_FLIP, 0)?;
        let prefix = self.durable_prefix_contributions();
        check_fence_never_cycles(
            prefix
                .iter()
                .chain(phase.iter())
                .chain(std::iter::once(&self.flip)),
            &self.fence_key,
            PHASE_FLIP,
        )?;
        let root = self.root()?;
        let key = inode_key(self.inode_id);
        let mut batch = match self.root {
            RootPrecondition::Fresh => WriteBatch::new().require_absent(key.clone()),
            RootPrecondition::Supersede(prior) => {
                WriteBatch::new().require(key.clone(), encode(prior))
            }
        }
        .put(key, encode_inode(&root)?);
        merge_contribution(&mut batch, &self.flip, &self.owned_keys(), PHASE_FLIP)?;
        check_record_ceilings(&batch)?;
        let budget = self
            .batch_budget
            .unwrap_or(MAX_BATCH_BYTES)
            .min(MAX_BATCH_BYTES);
        let bytes = batch_bytes(&batch);
        if bytes > budget {
            return Err(ChunkMapError::BatchOverBudget {
                batch: 0,
                bytes,
                budget,
            });
        }
        // The flip is the one batch that cannot be split — it *is* the publication
        // instant — so an over-cap operation count is refused here rather than emitted
        // into a transaction whose deadline it would spend on round trips.
        let cap = self.ops_budget.unwrap_or(MAX_BATCH_OPS).min(MAX_BATCH_OPS);
        let ops = batch_ops(&batch);
        if ops > cap {
            return Err(ChunkMapError::BatchOverOps { batch: 0, ops, cap });
        }
        Ok(batch)
    }

    /// Phase 1 — write the segment records. Stops at the first batch that does not
    /// commit (a lost fence): the segments already written are the crash-evidence the
    /// rollback obligation reclaims (`0016:2347-2362`), never a torn publication.
    ///
    /// The group's durable range is verified first ([`Self::verify_durable_range`]), so
    /// nothing is written on top of a prefix that belongs to another chunk list, and no
    /// earlier attempt's longer tail is left to strand the root this phase leads to. Only
    /// the claimed prefix ([`DurableRange::ResumePrefix`]) — this phase is what makes the
    /// rest durable.
    ///
    /// The batches are assembled **twice** and that is deliberate: once before the read, so
    /// every refusal this committer can decide without touching the store is decided with
    /// nothing durable, and once over what the read witnessed
    /// ([`Self::assemble_segment_batches`]), so each row carries the precondition its
    /// observed state calls for. The split is witness-independent
    /// ([`Self::staged_batches_over`]), so the two assemblies agree batch for batch — the
    /// second cannot introduce a refusal the first did not raise.
    pub async fn write_segments(&self, store: &impl MetadataStore) -> Result<CommitOutcome> {
        self.segment_batches()?;
        let durable = self
            .verify_durable_range(store, DurableRange::ResumePrefix)
            .await?;
        Self::commit_batches(store, self.segment_batches_over(&durable)?).await
    }

    /// Read the group's **own bounded `seg:` range** and check that what is already durable
    /// in it is exactly what this attempt is about to publish over — before anything is
    /// written (`0016:2352-2356`: a same-epoch recovery is idempotent *because* the keys and
    /// their content are fixed by the frozen part set; this is the check that they really
    /// are).
    ///
    /// `range` says how much of the plan the caller requires to be durable **already**: the
    /// claimed prefix before phase 1 (which writes the rest), the **whole plan** before the
    /// flip (which publishes a root naming all of it) — see [`DurableRange`].
    ///
    /// Four refusals, all raised from the **one** bounded range read the resolver itself
    /// performs ([`read_group_range`] over `seg:<nonce>:<epoch>:`, at most
    /// [`MAX_ROOT_SEGMENTS`] records, `0016:2463-2469`), and all *before* the first segment
    /// write:
    ///
    /// 1. **a key in the range that is not a well-formed segment key**
    ///    ([`ChunkMapError::SegmentKeyMalformed`]). Dropping it looks conservative and is
    ///    not: [`read_segments`] parses every key in this range with a strict `?`, so a row
    ///    the publication ignored here makes **every later resolve of the object fail**,
    ///    permanently. What no reader will tolerate, the publisher may not publish over.
    /// 2. **a durable record past the end of this plan**
    ///    ([`ChunkMapError::PublicationTailStranded`]). Nothing in the committer deletes a
    ///    `seg:` row, so a longer earlier attempt's tail survives at this same
    ///    `(nonce, epoch)`; the resolver fails closed on it
    ///    ([`ChunkMapError::SegmentUnknown`]) because a root must name every record in its
    ///    own range. Publishing a shorter plan over it therefore reports `Committed` and
    ///    leaves an object **no consumer can ever resolve** — silent at publication,
    ///    terminal at read, and unreachable for the rollback that would have cleaned it up.
    ///    This is checked whatever [`Self::resume_from`] says, including a fresh attempt:
    ///    the tail's existence has nothing to do with the cursor.
    /// 3. **a claimed-durable record that is absent or is not this plan's**
    ///    ([`ChunkMapError::ResumePrefixAbsent`], [`ChunkMapError::ResumePrefixMismatch`]).
    ///    [`Self::resume_from`] is a claim about records this attempt did not write; taken
    ///    on trust, an attempt resuming with a chunk list that differs anywhere in the
    ///    skipped prefix writes only the tail, flips a root computed from **its** list, and
    ///    publishes an object whose `seg:` records describe another list.
    ///
    /// **The whole prefix, not its last record.** Checking only
    /// `seg:<nonce>:<epoch>:<resume_from - 1>` catches a divergence that moves the byte
    /// extents — and misses every divergence that does not. Two chunk lists with the same
    /// chunk count and the same lengths but a different **id** or **placement** in an
    /// earlier chunk produce byte-identical `byte_offset`/`byte_len` for every later
    /// segment, so the last record matches and the flip publishes a root over a prefix
    /// belonging to the other list: a **hybrid** map, half attempt 1's fragments and half
    /// attempt 2's, that resolves cleanly and reads the wrong bytes. That is worse than the
    /// unresolvable map the check was written for, because nothing downstream ever notices.
    ///
    /// 4. **a planned record that is not durable at all, at the flip**
    ///    ([`ChunkMapError::PublicationIncomplete`], [`DurableRange::WholePlan`] only). The
    ///    flip publishes a root that names every segment in the plan, so every one of them
    ///    must already be on disk — including the ones *this* attempt was supposed to write.
    ///    A completer that lost phase 1 (a crash, a lost fence, or a recovery that drove
    ///    phase 2 on its own) would otherwise publish a live root over an empty or partial
    ///    range: unresolvable for ever, and unreachable for the rollback, because the root
    ///    is live. Under [`DurableRange::ResumePrefix`] these indices are the ones the phase
    ///    is *about* to write, so they are not required — only the cursor's claim is.
    ///
    /// Indices this attempt writes itself are not *refused* under
    /// [`DurableRange::ResumePrefix`] when they already hold this plan's exact bytes — but
    /// they are not overwritten either. Every index this read found holding them comes back
    /// as the [`DurableSegments`] witness, and the segment phase pins those rows with a
    /// compare-and-swap instead of putting over them
    /// ([`Self::assemble_segment_batches`]): what the store already has cannot be re-written
    /// by a recovery that raced a live generation's repoint into the same row.
    async fn verify_durable_range(
        &self,
        store: &impl MetadataStore,
        range: DurableRange,
    ) -> Result<DurableSegments> {
        let planned = self.plan()?;
        let claimed = usize::try_from(self.resume_from).unwrap_or(usize::MAX);
        if claimed > planned.len() {
            return Err(ChunkMapError::ResumePastPlan {
                resume_from: self.resume_from,
                segments: planned.len(),
            }
            .into());
        }
        // The group's own range, keyed by the parsed index and read through the SAME
        // bounded reader every resolve uses: order is matched by index rather than by
        // arrival position, and a range holding more than this plan accounts for is seen
        // without being materialised.
        let durable: BTreeMap<u32, Bytes> =
            match read_group_range(store, &self.group, planned.len()).await? {
                GroupRange::Beyond(stranded) => {
                    return Err(ChunkMapError::PublicationTailStranded {
                        nonce: self.group.nonce().to_string(),
                        epoch: self.group.epoch(),
                        index: stranded,
                        planned: planned.len(),
                    }
                    .into())
                }
                GroupRange::Rows(rows) => rows
                    .into_iter()
                    .map(|(index, (_, value))| (index, value))
                    .collect(),
            };
        let mut witnessed = DurableSegments::default();
        for (position, (segment, record)) in planned.iter().enumerate() {
            let durable_bytes = durable.get(&segment.index);
            if durable_bytes.is_some_and(|bytes| bytes == &encode(record)) {
                witnessed.0.insert(segment.index);
                continue;
            }
            // **Every** planned index is checked, not only the claimed prefix — the whole
            // plan is walked whichever range the caller asked about, because a durable
            // record this attempt is merely *about to overwrite* is as dangerous as one it
            // claimed. Above the cursor a record that is ABSENT is fine under
            // `ResumePrefix` (phase 1 is about to write it) and fatal under `WholePlan`
            // (the flip publishes a root naming it); a record that is PRESENT and not this
            // plan's is fatal either way. That last case is the one a prefix-only check
            // misses: a second attempt at the same `(nonce, epoch)` carrying a different
            // plan would overwrite a live generation's segment records one by one, tearing
            // the map of an object whose root still names them, and would only discover its
            // mistake at the root CAS — after the damage.
            let Some(_) = durable_bytes else {
                // Absent. Below the cursor the caller *claimed* it, so the cursor is wrong;
                // at or above it, phase 1 is about to write it — which is fine unless this
                // is the flip, whose root will name it. The two are different failures: a
                // wrong cursor is fixed by reading the store, an unfinished phase 1 by
                // running it.
                if position >= claimed {
                    if range == DurableRange::ResumePrefix {
                        continue;
                    }
                    return Err(ChunkMapError::PublicationIncomplete {
                        nonce: self.group.nonce().to_string(),
                        epoch: self.group.epoch(),
                        index: segment.index,
                        planned: planned.len(),
                    }
                    .into());
                }
                return Err(ChunkMapError::ResumePrefixAbsent {
                    nonce: self.group.nonce().to_string(),
                    epoch: self.group.epoch(),
                    index: segment.index,
                }
                .into());
            };
            // Present, and not what this plan puts there.
            return Err(ChunkMapError::ResumePrefixMismatch {
                nonce: self.group.nonce().to_string(),
                epoch: self.group.epoch(),
                index: segment.index,
            }
            .into());
        }
        Ok(witnessed)
    }

    /// Commit a segment-write phase's batches in order, stopping at the first that does
    /// not commit. Shared by [`Self::write_segments`] and by the tests that drive a
    /// multi-batch phase through [`Self::staged_batches`], so both run the same loop.
    async fn commit_batches(
        store: &impl MetadataStore,
        batches: Vec<WriteBatch>,
    ) -> Result<CommitOutcome> {
        for batch in batches {
            match store.commit(batch).await? {
                CommitOutcome::Committed => {}
                CommitOutcome::Conflict => return Ok(CommitOutcome::Conflict),
            }
        }
        Ok(CommitOutcome::Committed)
    }

    /// Phase 2 — the flip, the publication instant.
    ///
    /// The group's durable range is verified here too ([`Self::verify_durable_range`]) and
    /// not only in [`Self::publish`]: a completer that drove the phases separately (the
    /// recovery shape) would otherwise flip a root over a range this committer has never
    /// looked at, and an earlier attempt's stranded tail makes that root unresolvable for
    /// ever.
    ///
    /// **The WHOLE plan, not the resumed prefix** ([`DurableRange::WholePlan`]). The root
    /// this batch installs names every planned segment, so the check that matters here is
    /// completeness, not the cursor's honesty: a caller that flips after a phase 1 which
    /// crashed, lost its fence, or never ran at all publishes a live root over an empty or
    /// partial range. That object is then unresolvable for ever
    /// ([`ChunkMapError::SegmentAbsent`] at every read, GC, restore and rebalance pass) and
    /// no rollback will reclaim it, because the root is live — a permanent, silent loss from
    /// a call that reported `Committed`. Verifying only `0..resume_from` here would pass a
    /// `resume_from` of zero over an empty range without reading a single planned record.
    pub async fn flip(&self, store: &impl MetadataStore) -> Result<CommitOutcome> {
        let batch = self.flip_batch()?;
        self.verify_durable_range(store, DurableRange::WholePlan)
            .await?;
        store.commit(batch).await
    }

    /// Both phases. A segment-write batch that loses its fence stops the publication
    /// before the flip, so the root still names the prior generation.
    ///
    /// **Every refusal this committer can decide without touching the store is decided
    /// before the first `seg:` record is written.** Both phases' batches are assembled up
    /// front — the segment split *and* the flip — so an unfenced batch, a colliding caller
    /// contribution, an over-ceiling value or key, or a batch over either envelope
    /// ([`ChunkMapError::Unfenced`], [`ChunkMapError::ContributionCollides`],
    /// [`ChunkMapError::ValueOverCeiling`], [`ChunkMapError::KeyOverCeiling`],
    /// [`ChunkMapError::BatchOverBudget`], [`ChunkMapError::BatchOverOps`]) leaves **zero**
    /// durable `seg:` rows and no caller cursor moved. Assembling the segment phase and
    /// only then discovering the flip is unpublishable would write a whole generation's
    /// records that nothing will ever name, and would advance the caller's
    /// `segments_written` cursor past them — turning a deterministic, retry-proof refusal
    /// into garbage the rollback obligation has to reclaim.
    ///
    /// The flip batch is assembled here, before the segment writes, and committed
    /// unchanged afterwards: it is a pure function of the plan and the caller's
    /// contribution, neither of which the segment phase touches.
    ///
    /// The same completeness rule [`Self::flip`] enforces holds here by construction rather
    /// than by a second range read: the prefix the cursor claims is *verified* before the
    /// first write, this call *writes* every remaining segment itself, and it reaches the
    /// flip only on `Committed` — so the whole plan is durable at the publication instant.
    /// Re-reading the range to confirm what the store just acknowledged would cost a round
    /// trip per page and prove nothing the fence does not already hold: a rollback that
    /// deleted these segments in the window must first move the fence record every batch
    /// and the flip pin, which turns the flip into a `Conflict`.
    pub async fn publish(&self, store: &impl MetadataStore) -> Result<CommitOutcome> {
        let segments = self.segment_batches()?;
        // The flip is charged against the phase in front of it (the fence may not cycle
        // across the two), and the phase is assembled once for both uses. It is charged
        // against the *unwitnessed* phase, which carries the same batch boundaries and the
        // same contributions as the witnessed one — the witness decides a row's
        // precondition, never the split ([`Self::staged_batches_over`]).
        let flip = self.flip_batch_after(&segments)?;
        // The one store read that must precede the writes: what is already durable under
        // this group is verified, never trusted (see `verify_durable_range`) — and what it
        // witnessed is what turns each segment write into a compare-and-swap.
        let durable = self
            .verify_durable_range(store, DurableRange::ResumePrefix)
            .await?;
        let segments = self.segment_batches_over(&durable)?;
        match Self::commit_batches(store, segments).await? {
            CommitOutcome::Committed => store.commit(flip).await,
            CommitOutcome::Conflict => Ok(CommitOutcome::Conflict),
        }
    }
}

/// How many times the segment-write split is re-run against the caller's own
/// contribution before it gives up (see [`SegmentedPublication::segment_batches`]). A
/// contribution whose size is independent of the batch count converges on the second
/// pass; the extra passes exist for one that is not, and the bound is what stops a
/// pathological one from looping.
const MAX_SPLIT_ATTEMPTS: usize = 4;

/// What one segment row costs a batch in **operations**: the put and the precondition that
/// guards it ([`SegmentedPublication::assemble_segment_batches`] — a row is either created
/// under `require_absent` or pinned with `require`, and never written blind).
///
/// The split divides its operation budget by this, because `B_ops` bounds what the
/// transaction *does* (`0016:640-648`, [`batch_ops`]) and a precondition is a read the
/// backend performs before it can commit. Counting rows instead would emit batches at twice
/// the cap and discover it only in the measured check, which cannot re-split its way out.
const SEGMENT_OPS_PER_RECORD: usize = 2;

/// The transaction bytes a batch charges against the envelope: every precondition's key
/// and expected value, every put's key and value, and every delete's key.
///
/// The **whole** batch, deliberately. A backend's transaction limit counts what the
/// transaction carries, not what the caller thinks of as its own payload, so this is the
/// only honest currency for [`MAX_BATCH_BYTES`] (`crates/traits/src/lib.rs:747-752`).
pub fn batch_bytes(batch: &WriteBatch) -> usize {
    batch
        .preconditions
        .iter()
        .map(|p| p.key.len() + p.expected.as_ref().map_or(0, |v| v.len()))
        .sum::<usize>()
        + batch
            .puts
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
        + batch.deletes.iter().map(Vec::len).sum::<usize>()
}

/// The **operations** a batch charges against `B_ops` ([`MAX_BATCH_OPS`]): every
/// precondition the backend must read and every mutation it must apply.
///
/// Preconditions are counted, not just mutations: each one is a read the transaction
/// performs before it can commit, which is precisely the sequential round-trip cost the
/// deadline bounds (`0016:636-648`).
pub fn batch_ops(batch: &WriteBatch) -> usize {
    batch.preconditions.len() + batch.puts.len() + batch.deletes.len()
}

/// One assembled segment-write batch, measured in both currencies against the envelope —
/// and how much of each is the caller's contribution rather than segment records.
struct Measured {
    batch: usize,
    bytes: usize,
    segment_bytes: usize,
    ops: usize,
    segment_ops: usize,
}

/// The records a [`SegmentedPublication`] writes itself: its inode root, its group's
/// `seg:` range, and the `seggrp:` marker reserving the group.
struct OwnedKeys {
    root: Vec<u8>,
    segments: Vec<u8>,
    marker: Vec<u8>,
}

impl OwnedKeys {
    /// Which owned record class `key` names, if any.
    fn owner_of(&self, key: &[u8]) -> Option<&'static str> {
        if key == self.root.as_slice() {
            Some("inode root")
        } else if key.starts_with(&self.segments) {
            Some("segment record")
        } else if key == self.marker.as_slice() {
            Some("seggrp: marker")
        } else {
            None
        }
    }
}

/// Refuse a batch carrying any **single record** the tightest backend in play must reject
/// — a value over [`MAX_VALUE_BYTES`] or a key over [`MAX_KEY_BYTES`].
///
/// Separate from the envelope check on purpose: the two limits are independent, and the
/// aggregate one is the weaker of the pair. A 200 KB caller-contributed value sits far
/// inside a 5 MB transaction envelope and is still rejected — **permanently**, because no
/// retry and no finer split can shrink one record. A staged publication that emitted it
/// would fail at commit with an error the caller cannot act on, after its segments were
/// already durable. Every builder charges its **assembled** batch, so the caller's own
/// contribution is measured exactly like the segment records are.
fn check_record_ceilings(batch: &WriteBatch) -> std::result::Result<(), ChunkMapError> {
    let keys = batch
        .preconditions
        .iter()
        .map(|p| p.key.as_slice())
        .chain(batch.puts.iter().map(|(key, _)| key.as_slice()))
        .chain(batch.deletes.iter().map(Vec::as_slice));
    for key in keys {
        if key.len() > MAX_KEY_BYTES {
            return Err(ChunkMapError::KeyOverCeiling {
                key: rendered_key(key),
                bytes: key.len(),
                ceiling: MAX_KEY_BYTES,
            });
        }
    }
    for (key, value) in &batch.puts {
        check_value_ceiling(key, value)?;
    }
    Ok(())
}

/// One record's value against [`MAX_VALUE_BYTES`] — the per-record half of
/// [`check_record_ceilings`], for the caller that charges a **single** put rather than a
/// whole batch ([`repoint_chunk`]'s flat arm, which charges only the growth it causes).
fn check_value_ceiling(key: &[u8], value: &[u8]) -> std::result::Result<(), ChunkMapError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(ChunkMapError::ValueOverCeiling {
            key: rendered_key(key),
            bytes: value.len(),
            ceiling: MAX_VALUE_BYTES,
        });
    }
    Ok(())
}

/// Refuse a staged-publication batch that does not compare-and-swap on the publication's
/// **declared fence record** — see [`ChunkMapError::Unfenced`] for what an unfenced phase
/// lets a concurrent rollback do.
///
/// Which record that is stays the caller's to name (`mpu:<id>` at `Completing@E` belongs
/// to the multipart session slice, and this committer is parameterised over it), but *that
/// one record must be pinned by every batch of both phases*. A bare "states some
/// precondition" check is not the property: a batch fenced on an unrelated dirent or
/// bucket record passes it and is overtaken by the rollback exactly as an unfenced one is,
/// and phase 1 fenced on one record with a flip fenced on another leaves the window
/// between them open — the very window the flip's fence exists to close.
///
/// The precondition must carry an **expected value**: `require_absent(fence)` is satisfied
/// *by* the rollback that deleted the session, so it fences nothing.
///
/// The phase labels below are the ones every refusal carries. They are constants rather
/// than literals at the call sites because one of the rules keys on **which** phase it is
/// ([`check_fence_transitioned`]'s delete rule), and a rule that turned on a typo would be
/// silently permissive.
const PHASE_SEGMENTS: &str = "segments";
/// The publication's last batch — see [`PHASE_SEGMENTS`].
const PHASE_FLIP: &str = "flip";

fn check_fenced(
    batch: &WriteBatch,
    fence_key: &[u8],
    phase: &'static str,
    index: usize,
) -> std::result::Result<(), ChunkMapError> {
    let fenced = batch
        .preconditions
        .iter()
        .any(|p| p.key == fence_key && p.expected.is_some());
    if !fenced {
        return Err(ChunkMapError::Unfenced {
            phase,
            batch: index,
            fence: rendered_key(fence_key),
        });
    }
    Ok(())
}

/// Refuse a **flip** whose contribution pins the fence record without moving it — see
/// [`ChunkMapError::FenceNotTransitioned`].
///
/// A precondition freezes the fence *for the flip's own transaction*; it says nothing
/// about the instant after it commits. The rollback that reclaims a crashed attempt is
/// fenced on the same record (`require(mpu == Completing@E)` + the `seg:` deletes), so
/// unless the flip **changes** that record, the rollback's precondition is still true
/// afterwards and it deletes the published generation's segments. The transition is the
/// caller's to write (`session → Completed` is the session slice's record, `0016:2338-2345`)
/// and this is the committer refusing to publish without it.
///
/// A `delete` of the fence counts **at the flip**, which is the last batch this publication
/// commits — it is the strongest transition there is. Before the flip it is refused
/// outright ([`ChunkMapError::FenceRemovedBeforeFlip`]): every remaining segment batch and
/// the flip are fenced on that same record, so a phase-1 delete leaves them permanently
/// unsatisfiable *after* segments are durable, and takes the rollback's own precondition
/// with it — a stranded generation with the evidence for reclaiming it deleted. A `put`
/// counts only when its bytes differ from **every** value the batch pins on that key:
/// re-writing the pinned value is a no-op the backend cannot distinguish from not writing
/// it.
///
/// **Every** put on the fence key must clear that bar, not merely one of them. A
/// [`WriteBatch`] is a set of mutations with no stated apply order within one key
/// (`crates/traits/src/lib.rs:1178-1186`), so a contribution that writes the fence twice —
/// once to the terminal value and once back to the pinned one — leaves the durable fence
/// backend-dependent, and on the backend where the restoring put lands last the rollback's
/// `require(mpu == Completing@E)` is true *after* the flip and it deletes the published
/// generation's segments: exactly the hazard this check exists to close, reached through a
/// batch the "some put differs" reading admits. An honest contribution never writes its own
/// fence record twice, so refusing the ambiguity costs a caller nothing and accepting it
/// would make the publication instant depend on which backend applied the batch.
fn check_fence_transitioned(
    batch: &WriteBatch,
    fence_key: &[u8],
    phase: &'static str,
    index: usize,
) -> std::result::Result<(), ChunkMapError> {
    let pinned: Vec<&Bytes> = fence_pins(batch, fence_key).collect();
    let refuse = || ChunkMapError::FenceNotTransitioned {
        fence: rendered_key(fence_key),
        phase,
        batch: index,
    };
    let mut mutated = batch.deletes.iter().any(|key| key.as_slice() == fence_key);
    if mutated && phase != PHASE_FLIP {
        return Err(ChunkMapError::FenceRemovedBeforeFlip {
            fence: rendered_key(fence_key),
            batch: index,
        });
    }
    for (_, value) in batch
        .puts
        .iter()
        .filter(|(key, _)| key.as_slice() == fence_key)
    {
        if pinned.contains(&value) {
            return Err(refuse());
        }
        mutated = true;
    }
    if mutated {
        return Ok(());
    }
    Err(refuse())
}

/// The values a batch **pins** on the fence record — the states its contribution asserts the
/// fence is in. One definition, so the per-batch transition rule and the across-batch cycle
/// rule below can never disagree about what "pinned" means.
fn fence_pins<'a>(batch: &'a WriteBatch, fence_key: &'a [u8]) -> impl Iterator<Item = &'a Bytes> {
    batch
        .preconditions
        .iter()
        .filter(move |p| p.key == fence_key)
        .filter_map(|p| p.expected.as_ref())
}

/// Refuse a **phase** whose batches take the fence record back to a state an earlier batch
/// already pinned — see [`ChunkMapError::FenceCycled`].
///
/// [`check_fence_transitioned`] is a per-batch rule: each batch must move the record it pins.
/// Satisfying it batch by batch still admits a **cycle** across the phase — `A → B → A` — and
/// a cycle is exactly what the fence exists to prevent: the instant the fence is back at `A`,
/// every precondition that was satisfiable at `A` is satisfiable again, so a *second*
/// completer of the same generation (or the rollback this publisher is racing) can commit
/// against it and overwrite the segment records this one wrote, one batch at a time, with the
/// first completer still believing it wrote what its root will name — the hybrid map
/// `ResumePrefixMismatch` catches only when the two plans differ in their byte extents.
///
/// This is the half of the ABA hazard that needs **no knowledge of the caller's value
/// grammar**: which values are terminal is the session slice's (#636's) to know, but that the
/// fence returned to a state it had already left is visible from the batches alone, before any
/// of them commits. The strictly-grammar half stays declined and recorded.
///
/// The states are read from **both** halves of each batch — what it pins and what it writes —
/// because a cycle can be spelled either way and checking only the pins would miss
/// `pin A, put B` followed by `pin B, put A`, which is the same `A → B → A` one batch later.
/// Adjacent repeats are the *same* state, not a return to it: one batch's put is the next
/// batch's pin in every honest contribution, so only a **non-adjacent** repeat is a cycle.
///
/// **The sequence is the caller's to choose, and the flip belongs in it.** It takes an
/// iterator rather than a slice for exactly that reason: the segment phase checks its own
/// batches ([`SegmentedPublication::segment_batches`]), and the flip is checked against the
/// phase *plus itself* ([`SegmentedPublication::flip_batch_after`]), because the ABA hazard
/// does not stop at a phase boundary — an `A → B` segment transition followed by a `B → A`
/// flip is the same cycle, spelled across the two, and it re-arms the rollback keyed on `A`
/// at the very instant the root that names those segments goes live. A rule applied per
/// phase would pass it.
fn check_fence_never_cycles<'a>(
    batches: impl IntoIterator<Item = &'a WriteBatch>,
    fence_key: &[u8],
    phase: &'static str,
) -> std::result::Result<(), ChunkMapError> {
    let mut states: Vec<&Bytes> = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        let puts = batch
            .puts
            .iter()
            .filter(|(key, _)| key.as_slice() == fence_key)
            .map(|(_, value)| value);
        for value in fence_pins(batch, fence_key).chain(puts) {
            if states.last() == Some(&value) {
                continue;
            }
            if states.contains(&value) {
                return Err(ChunkMapError::FenceCycled {
                    fence: rendered_key(fence_key),
                    phase,
                    batch: index,
                });
            }
            states.push(value);
        }
    }
    Ok(())
}

/// A key rendered for an error message, truncated so an oversized key cannot carry
/// kilobytes into a log line. A key of exactly the shown width is rendered whole; only a
/// longer one is elided, so the marker means "there was more", never "there was exactly
/// this much".
fn rendered_key(key: &[u8]) -> String {
    const SHOWN: usize = 64;
    let head = &key[..key.len().min(SHOWN)];
    let mut rendered = String::from_utf8_lossy(head).into_owned();
    if key.len() > SHOWN {
        rendered.push('…');
    }
    rendered
}

/// The fixed overhead of a segment record around its chunk array —
/// `{"chunks":[],"byte_offset":<u64>,"byte_len":<u64>}` at the widest `u64` rendering (20
/// digits each) — counted so the split's budget is the ENCODED size, not the chunk bytes
/// alone.
const SEGMENT_ENVELOPE_BYTES: usize = 80;

/// Merge `extra`'s preconditions **and** mutations into `batch` — the operation that
/// makes a caller's contribution ride the *same* commit rather than a second one — after
/// refusing any put or delete that names a record the publication owns.
///
/// The contribution is merged into the committer's own batch, so an overlapping key is not
/// a conflict the backend can report: both mutations are in one transaction and the last
/// one wins **silently**, publishing a root, a segment or a group marker the caller wrote
/// while the publication reports `Committed`. A caller's business is its own records
/// (`session → Completed`, `retire:…`); the publication's are not negotiable, so a
/// collision is a typed refusal ([`ChunkMapError::ContributionCollides`]) before anything
/// is committed.
///
/// *Preconditions* on those keys are left alone: reading a record the publication also
/// writes is legitimate (a caller may want to pin what it saw), and a precondition cannot
/// overwrite anything.
fn merge_contribution(
    batch: &mut WriteBatch,
    extra: &WriteBatch,
    owned: &OwnedKeys,
    phase: &'static str,
) -> std::result::Result<(), ChunkMapError> {
    let written = extra
        .puts
        .iter()
        .map(|(key, _)| key.as_slice())
        .chain(extra.deletes.iter().map(Vec::as_slice));
    for key in written {
        if let Some(owner) = owned.owner_of(key) {
            return Err(ChunkMapError::ContributionCollides {
                key: rendered_key(key),
                owner,
                phase,
            });
        }
    }
    batch.preconditions.extend(extra.preconditions.clone());
    batch.puts.extend(extra.puts.clone());
    batch.deletes.extend(extra.deletes.clone());
    Ok(())
}

/// Split `costs` (one encoded byte cost per record, in order) into the half-open ranges
/// one commit each carries, so that every batch stays inside **both** `budget` bytes and
/// `max_records` records (`B = min(B_bytes, B_ops)`, `0016:640-648`) — except a single record
/// that exceeds the byte budget on its own, which gets its own batch rather than being
/// dropped. That last case is not the split's to decide: it hands the over-budget batch
/// up, and [`SegmentedPublication::segment_batches`] refuses it with
/// [`ChunkMapError::BatchOverBudget`] once it has measured the assembled batch. Dropping
/// the record here would silently publish a map missing a segment.
///
/// `max_records` is the operation budget **already converted into records** by the caller
/// ([`SEGMENT_OPS_PER_RECORD`]): the split's unit is a record, and a record is more than one
/// operation. It is floored at one record per batch for the same reason as above — a caller
/// contribution so operation-heavy that no segment fits beside it is refused by the measured
/// check upstream ([`ChunkMapError::BatchOverOps`]), never silently dropped here.
///
/// Pure and separate from the publication so the **envelope arithmetic itself** is
/// testable: at the real 5 MB budget a second batch first appears past ~100 000 chunks,
/// a fixture no unit test can afford, which is how a bound this load-bearing ends up
/// asserted by a one-element vector.
fn batch_ranges(costs: &[usize], budget: usize, max_records: usize) -> Vec<std::ops::Range<usize>> {
    let max_records = max_records.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (index, cost) in costs.iter().enumerate() {
        if index > start && (bytes + cost > budget || index - start >= max_records) {
            ranges.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes += cost;
    }
    if start < costs.len() {
        ranges.push(start..costs.len());
    }
    ranges
}

/// Close the segment under construction, returning the next segment's byte offset.
fn push_segment(
    segments: &mut Vec<(SegmentRef, SegmentRecord)>,
    current: &mut Vec<ChunkRef>,
    byte_offset: u64,
) -> std::result::Result<u64, ChunkMapError> {
    let record = SegmentRecord::new(std::mem::take(current), byte_offset)?;
    let segment = SegmentRef {
        index: segments.len() as u32,
        byte_offset,
        byte_len: record.byte_len(),
    };
    let next =
        byte_offset
            .checked_add(segment.byte_len)
            .ok_or(ChunkMapError::SegmentSpanOverflow {
                index: segment.index,
            })?;
    segments.push((segment, record));
    Ok(next)
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

/// How many **record-valued** rows one page of the id-floor walk holds at once (the
/// `inode:` and `seg:` namespaces). A record is bounded by [`MAX_VALUE_BYTES`], so the page
/// bounds the walk's heap at ~6 MB whatever the namespace holds — the reason it is a *page*
/// and not a scan.
const RECORD_FLOOR_PAGE: usize = 64;

/// How many **key-only** rows one page of the id-floor walk holds (the `pending:` and
/// `orphan:` ledgers). Their values are a few bytes each and the walk reads only the keys,
/// so a wider page costs a fraction of the record page's heap and a fraction of its round
/// trips.
const LEDGER_FLOOR_PAGE: usize = 1_024;

/// Walk every row under `prefix` in **bounded pages**, visiting each once.
///
/// The alternative — [`MetadataStore::scan`] — is complete-or-fail-loud at [`SCAN_CAP`]
/// (`crates/traits/src/lib.rs:1019-1023`), so it turns a large namespace into an `Err`.
/// That is the right answer for a caller that can propagate one, and the wrong answer for
/// [`high_water_marks`], whose whole containment rule is that **no arrangement of store
/// contents** makes it refuse: `Gateway::recover` calls it before the gateway serves
/// anything (`crates/server/src/lib.rs:123-124`), so one oversized namespace would cost
/// every healthy object its availability. A page never fails with `ScanCapExceeded`
/// (`crates/traits/src/lib.rs:1084-1087`).
async fn for_each_page<Visit>(
    store: &impl MetadataStore,
    prefix: &[u8],
    page: usize,
    mut visit: Visit,
) -> Result<()>
where
    Visit: FnMut(Vec<u8>, Bytes),
{
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (rows, next) = store.scan_page(prefix, after.as_deref(), page).await?;
        for (key, value) in rows {
            visit(key, value);
        }
        // Clause 3 of the paging contract: `next` is `None` only when the prefix is
        // exhausted, and the cursor is exclusive, so the walk strictly advances.
        match next {
            Some(cursor) => after = Some(cursor),
            None => return Ok(()),
        }
    }
}

/// Whether the record class a value belongs to must name a chunk id **in the value itself**
/// — the premise [`RecoveredIds::covers`] needs and cannot derive from the bytes.
///
/// A reading counts the id tokens it could not read, which is the only loss a *token* can
/// show. It cannot show a loss that took the tokens with it: `{}` is valid JSON, names no
/// `id`, and reads as "no ids here" — a complete reading with an empty answer. Whether that
/// is the truth or a destroyed chunk list is a question about the record class, and only the
/// walk that knows which prefix it is reading can answer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassIds {
    /// A `seg:` record: its whole content is a **non-empty** chunk list (an empty one is
    /// refused at decode, [`ChunkMapError::EmptySegmentRecord`]), so a `seg:` value that
    /// names no id at all has lost the ids it was holding — and the fragments they name are
    /// still on disk (issue #364).
    Required,
    /// An `inode:` root: naming no chunk id is an ordinary, healthy state of the class. A
    /// flat map may be empty (a zero-length object), and a **segmented** root names none of
    /// its ids in its own value at all — they live in its `seg:` records, which the `seg:`
    /// walk covers without needing this root. "No id here" is therefore no evidence of loss,
    /// and treating it as loss would park the in-process chunk allocator at the top of its
    /// range for every damaged root in the store.
    Optional,
}

impl ClassIds {
    /// Whether a value of this class that names **no** id has lost the ids it should carry.
    fn requires_an_id(self) -> bool {
        matches!(self, ClassIds::Required)
    }
}

/// What [`raw_chunk_id_floor`] recovered from a record that did not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RecoveredIds {
    /// The largest in-process chunk id the bytes still name, or `0` when they name none.
    floor: ChunkId,
    /// How many `id` fields were found whose value could not be read at all — the number
    /// this recovery is *not* covering, reported so it is attributed rather than silent.
    unreadable: usize,
    /// How many `id` fields were **read**, at any magnitude — in-process ids and the
    /// cluster's coordination-free ones alike ([`ScannedId::OutOfRange`], which this floor
    /// does not cover but which is still an id the value carries).
    ///
    /// Read, not covered: it answers "does this value carry a chunk id at all", which is what
    /// [`Self::covers`] needs to tell a record whose ids are *absent* from one whose ids are
    /// *above the ceiling*. Counting only in-process ids would make a `seg:` record of a
    /// cluster-mode object look like one whose chunk list was destroyed.
    found: usize,
    /// Whether this reading is known to have seen **every** id the record names.
    ///
    /// The floor's promise is that it never sits *below* an id whose fragments are on disk
    /// (issue #364), and [`Self::floor`] alone cannot keep it: it is a maximum over the ids
    /// a reading *found*, which says nothing about the ids a damaged value hid. So the
    /// reading reports its own coverage and [`Self::contribution`] answers for the record.
    ///
    /// `false` by default, deliberately: every reader starts out having proved nothing, and
    /// the reading that never parsed the value ([`raw_chunk_id_floor`]'s
    /// `unwrap_or_default`) keeps that answer.
    complete: bool,
}

impl RecoveredIds {
    /// The chunk-id floor a record whose bytes read like this contributes.
    ///
    /// A **complete** reading contributes what it read: every id the record names is at or
    /// below it, which is exactly the property the floor needs.
    ///
    /// An **incomplete** one contributes the top of the in-process range — `ceiling - 1` —
    /// because the record may name any id in it and nobody can say which. Reporting what
    /// was read instead is an **under**-approximation with no evidence behind it: bytes
    /// naming ids 5 and 900 whose second token was destroyed read as 5, and an allocator
    /// resuming there re-mints 900 over fragments that are still on disk (issue #364). The
    /// floor is allowed to over-approximate and is not allowed to under-approximate
    /// (`high_water_marks`), and this is the only answer that stays on the right side of
    /// that with no information: the same direction the module already takes for a
    /// *truncated* id token, one step further.
    ///
    /// It is a wide answer, and it is bounded in the two ways that matter. It stops at the
    /// in-process ceiling, so the cluster's coordination-free ids (`(inode << 64) | seq`,
    /// ADR-0019) are untouched; and it costs no availability — `Gateway::recover` seeds the
    /// **inode** allocator from these marks and does not read the chunk floor at all
    /// (`crates/server/src/lib.rs:123-125`, where chunk ids are coordination-free), so a
    /// damaged record raises the number a future in-process allocator must respect and
    /// stops nothing from starting. It is attributed on the audit seam as it is raised
    /// ([`emit_record_unreadable`]) rather than left for an operator to infer.
    fn contribution(&self, ceiling: ChunkId, class: ClassIds) -> ChunkId {
        if self.covers(class) {
            self.floor
        } else {
            self.floor.max(ceiling.saturating_sub(1))
        }
    }

    /// Whether this reading can be trusted to have seen every id the record names — the
    /// reading's own coverage ([`Self::complete`]) **and** the record class's minimum.
    ///
    /// The second half is not a refinement, it is the other way a value loses its ids. A
    /// reading is `complete` when every `id` token it found was readable, which says nothing
    /// about a value that no longer *has* the tokens: `{}` under a `seg:` key is syntactically
    /// perfect JSON, names no id, counts nothing unreadable, and would contribute **zero** —
    /// hiding every chunk id that record was holding from the floor while their fragments sit
    /// on disk, which is exactly the re-mint that clobbers them (issue #364). A `seg:` record
    /// cannot legitimately name no chunk id ([`ClassIds::Required`]), so "no ids here" from a
    /// value of that class that did not decode is evidence of loss, and the answer is the
    /// conservative one.
    fn covers(&self, class: ClassIds) -> bool {
        self.complete && !(class.requires_an_id() && self.found == 0)
    }
}

/// The chunk-id floor a record that **did not decode** contributes, attributed on the
/// durability seam as it is derived.
///
/// One helper behind both walks of [`high_water_marks`] — the `inode:` roots and the `seg:`
/// records — because the containment rule is one rule: recover what the bytes still name,
/// say so out loud, and never let an unreadable value quietly lower the floor. Two call
/// sites spelling it separately is how one of them ends up with the lenient half and not
/// the conservative half.
///
/// `class` is the one thing the two walks do *not* share: what "this value names no chunk id"
/// means differs between the record classes, and only the caller knows which prefix it is
/// reading ([`ClassIds`], [`RecoveredIds::covers`]).
fn unreadable_record_floor(
    action: &'static str,
    key: &[u8],
    value: &[u8],
    ceiling: ChunkId,
    class: ClassIds,
) -> ChunkId {
    let recovered = raw_chunk_id_floor(value, ceiling);
    let floor = recovered.contribution(ceiling, class);
    emit_record_unreadable(action, key, recovered.unreadable, floor);
    floor
}

/// The largest in-process chunk id **below `ceiling`** that a record's raw bytes still
/// name — the id floor's recovery path for a value that does **not** decode.
///
/// [`high_water_marks`] may not fail (`Gateway::recover` calls it before the gateway
/// serves anything, `crates/server/src/lib.rs:123-124`) and it may not
/// **under**-approximate: a floor below an id whose fragments are on disk lets the
/// allocator re-mint that id and clobber them (issue #364). An undecodable record puts
/// those two requirements in tension — refusing costs every healthy object its
/// availability, and skipping silently drops whatever ids the record still names — and
/// this resolves it: the ids are recovered from the bytes directly, so the record is
/// *attributed* rather than silently skipped and the floor still covers it.
///
/// **Two readers, and the answer is the larger of the two**, because each sees ids the
/// other cannot and the whole point is not to miss one:
///
/// * `serde_json` itself. Most values that reach here are *structurally* invalid rather
///   than syntactically broken — this slice enforces the segmented root's invariants at
///   decode (parse-don't-validate), which widens what fails `decode` without making the
///   bytes stop being JSON — so a real parse reads every `id` field exactly, wherever it
///   sits and however it is spelled (`57` or `"57"`), with no grammar of our own to get
///   wrong (the rubric's *prefer extending a shared parser*). A byte scanner looking for a
///   digit straight after the colon reads `"id":"57"` as **nothing**, and a missed id is an
///   under-report — the one direction this function may not err in.
/// * a byte scan, which is all that is left for bytes that are not JSON at all — and which
///   also sees what a parsed value **discards**: duplicate object keys collapse to the last
///   one in a `serde_json::Value`, so `{"id":7,"id":3}` parses to 3 and scans to 7.
///
/// The scan is deliberately lenient and one-way: it folds each readable `"id"` into a
/// maximum, and a token the value **cut short** is one nobody can read — counted, never
/// guessed at. What neither reader can read is counted in [`RecoveredIds::unreadable`], and
/// a reading that could not see every id says so in [`RecoveredIds::complete`] so the
/// caller's [`RecoveredIds::contribution`] can answer conservatively instead of quietly
/// reporting a floor below a live id. It never errors — that is the point — and it never
/// runs on a record that decoded.
fn raw_chunk_id_floor(value: &[u8], ceiling: ChunkId) -> RecoveredIds {
    let parsed = serde_json::from_slice::<serde_json::Value>(value)
        // Bytes that are not JSON at all prove nothing about their own field set, and
        // `RecoveredIds::default()` is exactly that answer: no ids, none unreadable, and
        // **not complete**.
        .map(|parsed| json_chunk_id_floor(&parsed, ceiling))
        .unwrap_or_default();
    let scanned = scavenged_chunk_id_floor(value, ceiling);
    RecoveredIds {
        floor: parsed.floor.max(scanned.floor),
        // The larger count, for the same reason as the larger floor: this number is an
        // operator signal, and over-stating "there are ids here nobody could read" points
        // at a record that needs looking at, while under-stating it hides one.
        unreadable: parsed.unreadable.max(scanned.unreadable),
        // The larger count again, and here it is the *lenient* direction on purpose: this
        // number only ever answers "did the value carry an id at all"
        // ([`RecoveredIds::covers`]), so either reader seeing one is enough, and taking the
        // smaller would call a record whose ids one reader read perfectly well a record whose
        // ids are gone.
        found: parsed.found.max(scanned.found),
        // **Only a parse proves the field set.** It enumerates every field of the value,
        // wherever it is nested, so a complete parse whose every `id` was readable has seen
        // every id there is. A byte scan cannot make that claim about bytes it could not
        // structure, and it is the reader of last resort precisely for values no parse
        // survived — so it never certifies completeness, only breaks it: an `id` the *scan*
        // could not read is one the parse dropped as a duplicate key, and it is as
        // uncovered as any other.
        complete: parsed.complete && scanned.unreadable == 0,
    }
}

/// Every `id` field of a **parsed** record, folded into a maximum below `ceiling`.
///
/// Iterative, not recursive: the input is by definition a value this module's own decoders
/// rejected, and a walk whose depth is the attacker's choice is not one to run on the
/// gateway's startup path.
///
/// This is the reading that can be **complete**: the walk visits every field of the parsed
/// value, so once no `id` among them was unreadable there is no id left for the record to
/// be hiding (bar a duplicate key the parse collapsed, which is the byte scan's ground and
/// which [`raw_chunk_id_floor`] folds in).
fn json_chunk_id_floor(root: &serde_json::Value, ceiling: ChunkId) -> RecoveredIds {
    let mut recovered = RecoveredIds::default();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        match node {
            serde_json::Value::Object(fields) => {
                for (name, field) in fields {
                    if name == "id" {
                        match json_chunk_id(field, ceiling) {
                            ScannedId::InProcess(id) => {
                                recovered.floor = recovered.floor.max(id);
                                recovered.found += 1;
                            }
                            // Above the in-process ceiling is not a miss: those ids are the
                            // cluster's coordination-free space, which this floor never
                            // covers (see `high_water_marks`). Still an id the value carries,
                            // so it counts as one that was READ.
                            ScannedId::OutOfRange => recovered.found += 1,
                            ScannedId::Unreadable => recovered.unreadable += 1,
                        }
                    }
                    stack.push(field);
                }
            }
            serde_json::Value::Array(items) => stack.extend(items),
            _ => {}
        }
    }
    // The walk finished and it started from a whole parsed value, so the only ids it did
    // not read are the ones it counted.
    recovered.complete = recovered.unreadable == 0;
    recovered
}

/// One `id` field's value as JSON can spell an integer: a number, or a decimal string.
/// Read through `to_string` rather than `as_u64`, so an id between `u64::MAX` and the
/// widest a `ChunkId` holds is read exactly rather than lost.
///
/// Three answers, exactly as the byte scan gives ([`ScannedId`]) — and the third one is why
/// this is not an `Option`. A **bare** JSON integer above `u64::MAX` is parsed as a float
/// (`serde_json` without `arbitrary_precision`) and renders in exponent form, so it does not
/// read back as an integer at all; that is how a cluster-mode chunk id (`(inode << 64) |
/// seq`, ADR-0019 — every one of them above the in-process ceiling) is spelled in a record
/// that failed to decode for some unrelated reason. Calling it *unreadable* would raise an
/// unrecoverable-id alarm about an id that was read perfectly well, and — since an
/// unreadable id makes the whole record's reading incomplete
/// ([`RecoveredIds::contribution`]) — would raise the in-process floor to the top of its
/// range over a value that names no in-process id at all. So a number too wide to be an
/// integer is checked against the ceiling through its float, which is enough: every `f64`
/// that large is far above `2^64`, and the answer needed is only *which side* of the
/// ceiling it falls on.
fn json_chunk_id(value: &serde_json::Value, ceiling: ChunkId) -> ScannedId {
    let id = match value {
        serde_json::Value::Number(number) => match number.to_string().parse::<ChunkId>() {
            Ok(id) => id,
            Err(_) => {
                return match number.as_f64() {
                    Some(wide) if wide >= ceiling as f64 => ScannedId::OutOfRange,
                    // A fraction, a negative, or a number no reader can place: corruption,
                    // and counted as such.
                    _ => ScannedId::Unreadable,
                };
            }
        },
        serde_json::Value::String(text) => match text.parse::<ChunkId>() {
            Ok(id) => id,
            Err(_) => return ScannedId::Unreadable,
        },
        _ => return ScannedId::Unreadable,
    };
    if id < ceiling {
        ScannedId::InProcess(id)
    } else {
        ScannedId::OutOfRange
    }
}

/// The last-resort reader for bytes that are **not JSON at all**: scan for `id` field
/// tokens and read whatever decimal follows, quoted or bare.
///
/// **In every spelling JSON allows, not only the four literal bytes `"id"`.** A key is a
/// JSON string, and a string may escape any character it likes: the escaped spelling of
/// `id` (`\u0069d`) names the same field, so a record can carry the same field twice under
/// two spellings. The parse that reads escapes ([`json_chunk_id_floor`]) keeps only the
/// **last** duplicate, so in `{"\u0069d":999,"id":3}` it sees 3 — and a scanner matching
/// literal bytes sees 3 as well. 999, an id whose fragments are on disk, would then be
/// hidden from the allocator floor by both readers at once, and the re-mint that follows
/// clobbers them (issue #364). So the token match is escape-aware: at every quote the scan
/// lexes a string token and compares what it **unescapes to**. `serde_json` does the
/// unescaping, so there is no second string grammar in this module to get wrong (the
/// rubric's *prefer extending a shared parser*).
///
/// Bounded and non-consuming, which is what keeps it linear and keeps it from *losing* ids
/// the literal scan found: a token that unescapes to `id` is at most
/// [`MAX_ID_FIELD_BYTES`] long, so each quote costs a fixed-width lex and the scan never
/// swallows a run of damaged bytes on the way to the next candidate.
///
/// It never reports itself **complete** ([`RecoveredIds::complete`], left at its `false`
/// default): a scan over bytes no parse survived has no way to know whether a field it did
/// not find is absent or merely destroyed, and claiming otherwise is exactly how a floor
/// ends up below an id whose fragments are on disk.
fn scavenged_chunk_id_floor(value: &[u8], ceiling: ChunkId) -> RecoveredIds {
    let mut recovered = RecoveredIds::default();
    // Driven by an index over the bytes rather than by a cursor this body advances itself:
    // the walk is then linear and terminating by construction, whatever the bytes are.
    for at in 0..value.len() {
        if value[at] != b'"' {
            continue;
        }
        let window = &value[at..value.len().min(at + MAX_ID_FIELD_BYTES)];
        let Some((name, token_bytes)) = json_string_token(window) else {
            continue;
        };
        if name != "id" {
            continue;
        }
        // A field, not a fragment of some longer token: whitespace then a colon.
        let after_field = &value[at + token_bytes..];
        let Some(after_colon) = skip_ascii_whitespace(after_field).strip_prefix(b":") else {
            continue;
        };
        // `"id":57` and `"id":"57"` are the same id; a scanner that only knew the first
        // would report zero for the second, which is the under-report this may not make.
        match scanned_chunk_id(skip_ascii_whitespace(after_colon), ceiling) {
            ScannedId::InProcess(id) => {
                recovered.floor = recovered.floor.max(id);
                recovered.found += 1;
            }
            // Above the in-process ceiling is **not** a miss — those ids are the cluster's
            // coordination-free space, which this floor never covers — so it is not counted
            // as unreadable, exactly as the parsed reader treats it
            // ([`json_chunk_id_floor`]). Counting it would raise an unrecoverable-id alarm
            // about a record whose id was read perfectly well. It *was* read, so it counts
            // as found.
            ScannedId::OutOfRange => recovered.found += 1,
            ScannedId::Unreadable => recovered.unreadable += 1,
        }
    }
    recovered
}

/// One `id` field's **value**, as the last-resort reader reads it: an optional opening
/// quote, then any number of insignificant leading zeros, then the id's digits — each
/// digit literal or escaped, in any mixture. `None` when there is no id to read here.
///
/// **Padding and escapes are read, not skipped, and that is the whole point.** The
/// previous reading lexed the value as a JSON string token inside a fixed byte window, so
/// a token wider than the window — `"id":"00…18446744073709551615"`, an id
/// padded with escaped zeros — closed nowhere the window could see, fell back to reading
/// bare digits, met a backslash, and reported *nothing*. In bytes that are not JSON at all
/// (the only bytes that reach here, [`raw_chunk_id_floor`]) nothing else would have read it
/// either, and a chunk id missing from the floor is one the allocator re-mints over live
/// fragments (issue #364) — the single direction this reader may not err in. There is no
/// window now: the digits themselves are bounded by `u64` (past which the token cannot be
/// an id this allocator minted), and the zero run — the only unbounded part — costs the
/// scan nothing, because a run of zeros contains no `"`, so no *other* candidate can begin
/// inside the bytes this reader consumed and the outer walk can never re-read them at a
/// second site. The scan stays linear in the value's length.
///
/// Digits that run to the very end of the value are a **truncated** token, and so are
/// digits that run into a **half-written escape** ([`torn_digit_escape`]): `"id":"12\u003`
/// is a torn *digit*, not the id 12. Either way the id was these digits followed by an
/// unknown number more, so it is one **nobody could read** — `ScannedId::Unreadable`, not a
/// guess. Reading it as the digits already seen would put the floor below a live id whose
/// suffix the value lost, and completing it by arithmetic ("the widest in-process id that
/// starts with them") is a bound this reader no longer needs to get right: an unreadable id
/// makes the record's whole reading incomplete, and an incomplete reading contributes the
/// **top of the in-process range** ([`RecoveredIds::contribution`]) — at or above every
/// completion such arithmetic could have named, with no arithmetic to be wrong about.
fn scanned_chunk_id(bytes: &[u8], ceiling: ChunkId) -> ScannedId {
    let mut at = usize::from(bytes.first() == Some(&b'"'));
    let mut zeros = 0usize;
    while let Some((b'0', width)) = decimal_digit(&bytes[at..]) {
        at += width;
        zeros += 1;
    }
    let mut id: ChunkId = 0;
    let mut digits = 0usize;
    while let Some((digit, width)) = decimal_digit(&bytes[at..]) {
        // Wider than a `ChunkId` is not an id this allocator minted, and not something to
        // guess at: unreadable, exactly as the whole-token reading treated it.
        let Some(next) = id
            .checked_mul(10)
            .and_then(|shifted| shifted.checked_add(ChunkId::from(digit - b'0')))
        else {
            return ScannedId::Unreadable;
        };
        id = next;
        at += width;
        digits += 1;
    }
    // Cut short — by the end of the value, or inside a digit's escape — is the same fact
    // whether or not any digit arrived: the token's remaining digits are lost, so no id can
    // be read here. (Only padding and NOT cut short is the id zero: readable, and worth
    // nothing to the floor.)
    if at == bytes.len() || torn_digit_escape(&bytes[at..]) {
        return ScannedId::Unreadable;
    }
    if digits == 0 {
        return match zeros > 0 {
            true => ScannedId::InProcess(0),
            false => ScannedId::Unreadable,
        };
    }
    if id < ceiling {
        ScannedId::InProcess(id)
    } else {
        ScannedId::OutOfRange
    }
}

/// What a reader made of one `id` field's value — [`scanned_chunk_id`] from the bytes and
/// [`json_chunk_id`] from a parsed value, which answer in the same three terms so the two
/// cannot disagree about what counts as a miss. Three answers, not two, because "above the
/// in-process ceiling" and "nobody could read this" are different facts and only the second
/// is an operator signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScannedId {
    /// An id this allocator mints, and therefore one the floor must cover.
    InProcess(ChunkId),
    /// A readable id at or above the in-process ceiling: the cluster's coordination-free
    /// space, which this floor never covers and which is not a miss.
    OutOfRange,
    /// No id could be read here at all — counted, so the record is attributed rather than
    /// silently skipped.
    Unreadable,
}

/// Whether what is left of a value is the **start** of a digit escape the bytes ran out of
/// mid-way: `\`, `\u`, `\u0`, `\u00` or `\u003`, and nothing after it.
///
/// A digit that was being escaped when the value was cut short is a *truncated digit*, so
/// the token it ends is truncated too — the same reading a run of literal digits cut short
/// gets. Reading it as the digits already seen would report an id **below** the one the
/// bytes were spelling, and a floor below a live id is the re-mint that clobbers its
/// fragments (issue #364).
fn torn_digit_escape(rest: &[u8]) -> bool {
    !rest.is_empty() && br"\u003".starts_with(rest)
}

/// One decimal digit at the head of `bytes`, and how many bytes it occupies — literal, or
/// in the one escape that can spell a digit.
///
/// JSON's escapes are `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t` and `\uXXXX`, and of
/// those only `0`…`9` produce a decimal digit — so this is the *whole* grammar
/// for a digit rather than a convenient subset of it, which is what lets the reader above
/// claim it cannot lose an id to a spelling. Cross-checked against `serde_json`'s own
/// unescaping in `every_digit_escape_reads_as_that_digit`, so the claim is asserted rather
/// than argued.
fn decimal_digit(bytes: &[u8]) -> Option<(u8, usize)> {
    match bytes {
        [digit, ..] if digit.is_ascii_digit() => Some((*digit, 1)),
        [b'\\', b'u', b'0', b'0', b'3', digit, ..] if digit.is_ascii_digit() => Some((*digit, 6)),
        _ => None,
    }
}

/// The widest byte spelling of the JSON field name `id`: two quotes plus two characters,
/// each either literal or a six-byte `\u00XX` escape (`"\u0069\u0064"`). The scan's window
/// bound — a longer token cannot unescape to `id`, so nothing is missed by refusing to look
/// past it, and the walk stays linear in the value's length.
const MAX_ID_FIELD_BYTES: usize = 2 + 6 + 6;

/// Leading ASCII whitespace removed — JSON's insignificant space between tokens.
fn skip_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let leading = bytes.iter().take_while(|b| b.is_ascii_whitespace()).count();
    &bytes[leading..]
}

/// The JSON string token at the head of `bytes`: what it **unescapes to**, and how many
/// bytes it occupies.
///
/// `None` when `bytes` does not start with a quote, when the token never closes (the value
/// was truncated mid-string), or when its escapes are not valid JSON — the three cases in
/// which there is no string here to read. The unescaping is `serde_json`'s, so `\u0069`,
/// `\"` and the rest are read exactly as a JSON parser would read them.
fn json_string_token(bytes: &[u8]) -> Option<(String, usize)> {
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut at = 1;
    while at < bytes.len() {
        match bytes[at] {
            // Whatever it escapes, the next byte is not the closing quote.
            b'\\' => at += 2,
            b'"' => {
                let width = at + 1;
                return serde_json::from_slice::<String>(&bytes[..width])
                    .ok()
                    .map(|text| (text, width));
            }
            _ => at += 1,
        }
    }
    None
}

/// The largest in-process chunk id named by **any** `seg:` record in the store — the
/// segmented half of [`high_water_marks`]'s chunk floor.
///
/// **Read as a namespace, not per object, and deliberately so.** Proposal 0016 forbids a
/// global `seg:` scan for *resolving a map* (`0016:2463-2469`) because resolution must be
/// bounded per object; this is not a resolution. It reads no root, resolves no map, and
/// answers a question about ids alone, so a root whose segment is missing — or missing
/// entirely — cannot make it fail. That is what keeps `Gateway::recover` total
/// (`crates/server/src/lib.rs:123-124`): the allocator floor has no safety need to know
/// which object owns which id, only that no id it hands out is already in use.
///
/// It is **paged** ([`MetadataStore::scan_page`], #634) rather than scanned for the same
/// totality reason: `scan` is complete-or-fail-loud at [`SCAN_CAP`]
/// (`crates/traits/src/lib.rs:1019-1023`), and `seg:` is the one namespace a single object
/// can contribute [`MAX_ROOT_SEGMENTS`] records to, so a store large enough would make a
/// scan of it fail — and a gateway that cannot start is exactly the blast radius this
/// containment rule exists to prevent. A page never fails with `ScanCapExceeded`
/// (`crates/traits/src/lib.rs:1084-1087`).
///
/// A record whose bytes do not decode is **attributed, and its ids are still covered**
/// ([`unreadable_record_floor`]) — never propagated as an error and never silently dropped.
/// It is one object's corruption, and the repo's containment shape for an untrustworthy
/// record is to refuse to certify, attribute the blocker and keep going
/// (`ReconciliationStatus::PendingMalformed`, `crates/custodian/src/desired_state.rs:88-101`).
/// Failing here would return the whole gateway's availability for one unreadable value;
/// skipping the record's ids — or folding in only the ids a damaged value still *let* the
/// recovery read — would leave the floor **below** ids whose fragments are on disk, which is
/// the re-mint hazard this function exists to prevent (issue #364). So the damaged record
/// costs the walk its *typed* reading, and nothing else.
async fn segment_chunk_floor(store: &impl MetadataStore, ceiling: ChunkId) -> Result<ChunkId> {
    let mut max_chunk: ChunkId = 0;
    for_each_page(store, SEG_PREFIX, RECORD_FLOOR_PAGE, |key, value| {
        let Ok(record) = decode::<SegmentRecord>(&value) else {
            tracing::warn!(monotonic_counter.metadata_segment_record_unreadable = 1_u64);
            max_chunk = max_chunk.max(unreadable_record_floor(
                "segment-record-unreadable",
                &key,
                &value,
                ceiling,
                // A `seg:` record IS a chunk list, so a value of this class that names no id
                // has lost the ids it was holding rather than legitimately holding none —
                // `{}` is the worked example ([`ClassIds::Required`]).
                ClassIds::Required,
            ));
            return;
        };
        for chunk in record.chunks() {
            if chunk.id < ceiling {
                max_chunk = max_chunk.max(chunk.id);
            }
        }
    })
    .await?;
    Ok(max_chunk)
}

/// Attribute a record the id-floor walk could not decode on the durability seam: the key,
/// how many of its `id` fields the byte-level recovery could not read either, and the floor
/// the record ended up contributing.
///
/// The two numbers are the honest edge of the containment rule. The floor covers every id
/// the bytes still name ([`raw_chunk_id_floor`]) — and bytes that name an id nobody can read
/// are covered the only way they can be, by contributing the top of the in-process range
/// ([`RecoveredIds::contribution`]); this is where both are said out loud rather than left
/// to a silent skip (the rubric's *Absent or unsupported entries*). `contributed_floor` is
/// what tells an operator which of the two happened, and therefore whether one damaged
/// record has raised the floor a future in-process allocator resumes from. It is reported,
/// not raised: `Gateway::recover` may not be stopped by one damaged record.
fn emit_record_unreadable(
    action: &'static str,
    key: &[u8],
    unreadable: usize,
    contributed_floor: ChunkId,
) {
    tracing::warn!(
        target: "wyrd.metadata.audit",
        action,
        key = %String::from_utf8_lossy(key),
        unreadable_ids = unreadable,
        contributed_floor = %contributed_floor,
        "a record did not decode while deriving the id allocator floor; every chunk id its bytes still name is folded into the floor, and a reading that could not see every id contributes the top of the in-process range instead (unreadable_ids counts the id fields nobody could read) — the process starts rather than refusing over one unreadable value",
    );
    // Emitted with its count and without a guard: a zero increment is a no-op for a sum,
    // and making the counter's *presence* rather than its value carry the signal would put
    // the operator's alert one level of indirection away from the number it is about.
    tracing::warn!(monotonic_counter.metadata_chunk_id_unrecoverable = unreadable as u64);
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
/// it — the in-process counter only ever mints ids below `2^64`). The bound is **exclusive**
/// at both readers: `2^64` itself is the first coordination-free id, not the last in-process
/// one, and a floor pinned there would park the in-process allocator at the top of its range
/// for ever.
///
/// **It is total, and it over-approximates.** No arrangement of store contents makes it
/// return `Err`: it derives ids from *keys* and from record bytes, resolves no root, and
/// contains an unreadable value by attributing it and covering its ids
/// ([`unreadable_record_floor`]) rather than by failing or by dropping it. Both halves are
/// load-bearing and they pull in opposite directions — `Gateway::recover` calls this before
/// serving, so an `Err` costs every *healthy* object its availability
/// (`crates/server/src/lib.rs:123-124`), while a floor below a live id costs an object its
/// bytes (issue #364).
///
/// "Covering its ids" is the whole of the second half, not the readable part of it: a
/// record whose reading cannot be shown to have seen every id it names contributes the top
/// of the in-process range ([`RecoveredIds::contribution`]), because the alternative is a
/// floor sitting below an id the damage hid. That is a deliberately wide answer for a
/// deliberately narrow case, and it is bounded — it never reaches the cluster's
/// coordination-free space, and it is a *number*, not a refusal, so the gateway still
/// starts and every healthy object still serves.
///
/// Totality is why **every one of its four walks is paged** ([`for_each_page`]) and none of
/// them is a `scan`. A `scan` is complete-or-fail-loud at [`SCAN_CAP`], so a store with more
/// than 1 048 576 inode records — or an orphan ledger past the same cap, which one maximum
/// segmented retirement approaches on its own (~1.78 M marks, `crates/traits/src/lib.rs:1030-1036`)
/// — would make this return `Err(ScanCapExceeded)` and stop the gateway from starting. A
/// containment rule that holds for a damaged record and breaks for a large store is not a
/// containment rule.
pub async fn high_water_marks(store: &impl MetadataStore) -> Result<(InodeId, ChunkId)> {
    const IN_PROCESS_CHUNK_CEILING: ChunkId = 1 << 64;
    let mut max_inode: InodeId = 0;
    let mut max_chunk: ChunkId = 0;
    for_each_page(store, b"inode:", RECORD_FLOOR_PAGE, |key, value| {
        if let Some(id) = parse_inode_key(&key) {
            max_inode = max_inode.max(id);
        }
        // A **flat** map carries its chunk ids inline, so they are read straight off the
        // record. A **segmented** root carries none — they live in its `seg:` records —
        // and this walk deliberately does **not** resolve it. Resolving here would make a
        // single object with a missing segment fail the whole call, and this call is what
        // `Gateway::recover` runs before serving (`crates/server/src/lib.rs:123-124`): one
        // damaged object would then take down every *healthy* object's availability. The
        // ids are recovered from the `seg:` namespace below instead, which needs no root,
        // no resolution and no object to be intact — the same shape the `pending:` and
        // `orphan:` marks below already use.
        //
        // An `inode:` value that does not decode is contained the same way, and for the
        // same reason: this slice enforces the segmented root's structural invariants at
        // decode (parse-don't-validate), which widens the set of values that can fail it,
        // and a `?` here would hand one damaged root the power to stop the whole gateway
        // from starting. It is attributed and its ids are recovered from the bytes
        // ([`unreadable_record_floor`]) so the floor never sits below an id the record
        // names — the containment rule is totality *plus* over-approximation, and a silent
        // skip would buy the first by giving up the second.
        let Ok(record) = decode::<InodeRecord>(&value) else {
            tracing::warn!(monotonic_counter.metadata_inode_record_unreadable = 1_u64);
            max_chunk = max_chunk.max(unreadable_record_floor(
                "inode-record-unreadable",
                &key,
                &value,
                IN_PROCESS_CHUNK_CEILING,
                // A root that names no chunk id is an ordinary root: an empty flat map, or a
                // segmented one whose ids are in its `seg:` records and are covered by the
                // walk below without this value ([`ClassIds::Optional`]).
                ClassIds::Optional,
            ));
            return;
        };
        if let ChunkMap::Flat(chunks) = &record.chunk_map {
            for chunk in chunks {
                if chunk.id < IN_PROCESS_CHUNK_CEILING {
                    max_chunk = max_chunk.max(chunk.id);
                }
            }
        }
    })
    .await?;
    // Every `seg:` record in the store, live or retired-but-not-yet-drained. A strict
    // **over**-approximation on purpose: a retired generation's fragments are still on
    // disk until its drain runs, so its ids are not free to re-mint either, and the floor
    // must never sit below an id whose bytes exist (issue #364).
    max_chunk = max_chunk.max(segment_chunk_floor(store, IN_PROCESS_CHUNK_CEILING).await?);
    for_each_page(store, b"pending:", LEDGER_FLOOR_PAGE, |key, _value| {
        if let Some(chunk) = parse_pending_chunk_key(&key) {
            if chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(chunk);
            }
        }
    })
    .await?;
    // Orphan grace records (`orphan:<dserver>:<chunk>:<index>`): a deleted object's
    // fragments still live on disk under this ledger until GC's grace window elapses, so
    // their chunk id is not yet free to re-mint even though no `inode:`/`pending:` key
    // references it any more (see the doc comment above; issue #364).
    for_each_page(store, ORPHAN_PREFIX, LEDGER_FLOOR_PAGE, |key, _value| {
        if let Some((_dserver, frag)) = parse_orphan_key(&key) {
            if frag.chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(frag.chunk);
            }
        }
    })
    .await?;
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

    // ---------------------------------------------------------------------
    // Chunk-map segmentation (issue #635, proposal 0016 decision 7)
    //
    // These live HERE, co-located with the record they exercise, rather than in an added
    // `tests/*.rs` file: the added-test-file gate runs every added target on the pre-fix
    // tree, and these name types this slice ADDS, so as a separate file they would be a
    // build error there and would destroy the assertion-red of
    // `crates/custodian/tests/segmented_map_consumers.rs`.
    // ---------------------------------------------------------------------

    use async_trait::async_trait;
    use pollster::block_on;
    use std::sync::Mutex;
    use wyrd_metadata_redb::RedbMetadataStore;

    const NONCE: &str = "0123456789abcdef0123456789abcdef";

    fn group(epoch: u64) -> SegmentGroup {
        SegmentGroup::new(NONCE, epoch).unwrap()
    }

    fn chunk(id: ChunkId, len: u64, placement: Vec<DServerId>) -> ChunkRef {
        ChunkRef {
            id,
            scheme: EcScheme::None,
            len,
            placement,
        }
    }

    /// An erasure-coded chunk with `n = k + m = 3` fragments — the shape the repoint
    /// tests need, since a repoint's placement vector must be exactly `fragment_count()`
    /// long (ADR-0040's strict maintenance boundary) and `EcScheme::None` has one
    /// fragment, not three.
    fn ec_chunk(id: ChunkId, len: u64, placement: Vec<DServerId>) -> ChunkRef {
        ChunkRef {
            id,
            scheme: EcScheme::ReedSolomon { k: 1, m: 2 },
            len,
            placement,
        }
    }

    fn mem_store() -> RedbMetadataStore {
        RedbMetadataStore::in_memory().unwrap()
    }

    async fn put(store: &impl MetadataStore, key: Vec<u8>, value: impl Into<Bytes>) {
        assert_eq!(
            store
                .commit(WriteBatch::new().put(key, value))
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }

    // ---- (i) legacy decode→encode is the identity, byte-for-byte ----

    /// Every CAS in this module is `require(key, encode(prior))` compared byte-for-byte
    /// against the stored value, so a `chunk_map` encoding that gained a tag or a wrapper
    /// would turn every overwrite, backfill, reconstruction and rebalance of every
    /// pre-existing object into a permanent `Conflict`. This is the same reason
    /// `skip_serializing_if` is load-bearing on `etag`/`content_type`/`modified`.
    #[test]
    fn legacy_flat_records_round_trip_byte_identically() {
        for legacy in [
            // A pre-ADR-0047 record: no etag / content_type / modified at all.
            br#"{"size":42,"chunk_map":[],"state":"Committed","version":3}"#.to_vec(),
            br#"{"size":8,"chunk_map":[{"id":7,"scheme":"None","len":8,"placement":[1]}],"state":"Committed","version":1}"#.to_vec(),
            br#"{"size":8,"chunk_map":[{"id":7,"scheme":{"ReedSolomon":{"k":2,"m":1}},"len":8,"placement":[1,2,3]}],"state":"Pending","version":9,"etag":"abc","content_type":"text/plain","modified":1700000000000}"#.to_vec(),
        ] {
            let decoded: InodeRecord = decode(&legacy).unwrap();
            assert_eq!(
                encode(&decoded).as_ref(),
                legacy.as_slice(),
                "decode→encode must be the identity on a pre-segmentation record",
            );
        }
    }

    /// …and end-to-end: a CAS against a legacy record must still commit.
    #[test]
    fn commit_chunk_map_against_a_legacy_record_commits_not_conflicts() {
        let store = mem_store();
        let legacy = br#"{"size":8,"chunk_map":[{"id":7,"scheme":"None","len":8,"placement":[1]}],"state":"Committed","version":1}"#;
        block_on(async {
            put(&store, inode_key(1), legacy.to_vec()).await;
            let prior: InodeRecord = decode(legacy).unwrap();
            let outcome = commit_chunk_map(&store, 1, &prior, vec![chunk(8, 8, vec![2])], 8)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                CommitOutcome::Committed,
                "a CAS against a pre-segmentation record must not become a permanent Conflict",
            );
        });
    }

    /// **The commit point COMMITS.** [`commit_chunk_map`] is the four-phase write's commit
    /// step (`crates/core/src/write.rs:274`): the record it publishes carries
    /// `InodeState::Committed`, whatever state the `prior` it compare-and-swaps against was
    /// in — and `prior` is a **pending** record on the path that matters, because that is the
    /// transition. A published record that inherited `Pending` is an object no read path
    /// serves, no maintenance pass protects and no GC reference set contains: its fragments
    /// look unreferenced.
    #[test]
    fn the_commit_point_publishes_a_committed_record_over_a_pending_prior() {
        let store = mem_store();
        let prior = InodeRecord {
            size: 0,
            chunk_map: Vec::new().into(),
            state: InodeState::Pending,
            version: 1,
            ..Default::default()
        };
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            assert_eq!(
                commit_chunk_map(&store, 1, &prior, vec![chunk(8, 8, vec![2])], 8)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
            let published: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                published.state,
                InodeState::Committed,
                "the commit point must publish a COMMITTED record: a pending one is invisible \
                 to every read path and outside every maintenance pass's population",
            );
            assert_eq!(
                published.version, 2,
                "…and it advances the CAS ordering by one"
            );
        });
    }

    // ---- (ii) the settled segmented encoding, and its invariants AT DECODE ----

    fn canonical_segmented() -> Vec<u8> {
        br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":16},{"index":1,"byte_offset":16,"byte_len":8}]}"#.to_vec()
    }

    #[test]
    fn the_segmented_encoding_is_the_settled_json_both_ways() {
        let map = SegmentedMap::new(
            group(7),
            vec![
                SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: 16,
                },
                SegmentRef {
                    index: 1,
                    byte_offset: 16,
                    byte_len: 8,
                },
            ],
        )
        .unwrap();
        assert_eq!(encode(&map).as_ref(), canonical_segmented().as_slice());
        let back: SegmentedMap = decode(&canonical_segmented()).unwrap();
        assert_eq!(back, map);
        // …and the same bytes as a whole `chunk_map` value, which is the shape the
        // consumer test's hand-written fixture uses.
        let as_map: ChunkMap = decode(&canonical_segmented()).unwrap();
        assert_eq!(as_map, ChunkMap::Segmented(map));
        assert_eq!(encode(&as_map).as_ref(), canonical_segmented().as_slice());
    }

    /// A single valid example is not an oracle for an invariant: each structural rule gets
    /// a raw-byte negative case, and each must be **rejected at decode** rather than
    /// admitted as a value a consumer could half-resolve (ADR-0045 / `AGENTS.md:146-149`).
    #[test]
    fn every_structural_invariant_is_rejected_at_decode() {
        let cases: [(&str, &[u8]); 7] = [
            (
                "segment_count disagrees with segments.len()",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":3,"segments":[{"index":0,"byte_offset":0,"byte_len":16}]}"#,
            ),
            (
                "a duplicate index",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":16},{"index":0,"byte_offset":16,"byte_len":8}]}"#,
            ),
            (
                "a gap in the index sequence",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":16},{"index":2,"byte_offset":16,"byte_len":8}]}"#,
            ),
            (
                "a non-monotonic byte_offset",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":16},{"index":1,"byte_offset":8,"byte_len":8}]}"#,
            ),
            (
                "an overlapping byte span",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":24},{"index":1,"byte_offset":16,"byte_len":8}]}"#,
            ),
            (
                "a zero-length segment",
                br#"{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":1,"segments":[{"index":0,"byte_offset":0,"byte_len":0}]}"#,
            ),
            (
                "a nonce that is not 32 lowercase hex characters",
                br#"{"group":{"nonce":"0123456789ABCDEF0123456789abcdef","epoch":7},"segment_count":1,"segments":[{"index":0,"byte_offset":0,"byte_len":8}]}"#,
            ),
        ];
        for (what, bytes) in cases {
            assert!(
                decode::<SegmentedMap>(bytes).is_err(),
                "{what} must be rejected at decode, not admitted as a value",
            );
            // …and through the field's own decode, so no consumer can reach it either.
            assert!(
                decode::<ChunkMap>(bytes).is_err(),
                "{what} must be rejected when decoded as a chunk map too",
            );
        }
    }

    /// **Rejected at decode is only half of it: a consumer has to be able to tell that the
    /// fault is one object's.**
    ///
    /// Containment is written as a downcast to [`ChunkMapError`]
    /// (`crates/custodian/src/gc.rs:298`,`:317`) — that is how a damaged object is
    /// attributed and skipped while the pass keeps going for every other object. `serde`
    /// erases a custom error into a message, so a structural invariant enforced inside a
    /// `Deserialize` impl arrives wearing `serde_json::Error` and misses that downcast: one
    /// malformed segment then ends the fleet-wide reference build and blanks the drain
    /// surface for every healthy object. So [`decode`] classifies the fault, and every
    /// spelling of every structural rule is asserted here — including the ones that fail
    /// before any validator runs (a field of the wrong type, an unknown field), which are
    /// as much this record class's corruption as a table that does not tile.
    #[test]
    fn every_structural_fault_reaches_a_consumer_as_a_chunk_map_error() {
        let root = |map: &str, size: u64| {
            format!(r#"{{"size":{size},"chunk_map":{map},"state":"Committed","version":1}}"#)
                .into_bytes()
        };
        let table = |count: u32, segments: &str| {
            format!(
                r#"{{"group":{{"nonce":"{NONCE}","epoch":7}},"segment_count":{count},"segments":[{segments}]}}"#
            )
        };
        let one = r#"{"index":0,"byte_offset":0,"byte_len":16}"#;
        let inodes: [(&str, Vec<u8>); 6] = [
            (
                "a table whose count disagrees with its segments",
                root(&table(2, one), 16),
            ),
            (
                "a table with a gap in its index sequence",
                root(
                    &table(
                        2,
                        &format!(r#"{one},{{"index":2,"byte_offset":16,"byte_len":8}}"#),
                    ),
                    24,
                ),
            ),
            (
                "a nonce that could not key a `seg:` range",
                root(
                    &table(1, one).replace(NONCE, "0123456789ABCDEF0123456789abcdef"),
                    16,
                ),
            ),
            (
                "a size that disagrees with the table's span",
                root(&table(1, one), 17),
            ),
            (
                "a segment_count of the wrong JSON type",
                root(
                    &table(1, one).replace(r#""segment_count":1"#, r#""segment_count":"1""#),
                    16,
                ),
            ),
            (
                "a field the segmented encoding does not carry",
                root(
                    &table(1, one).replace(r#""segments""#, r#""extra":true,"segments""#),
                    16,
                ),
            ),
        ];
        for (what, bytes) in inodes {
            let err = decode::<InodeRecord>(&bytes).unwrap_err();
            assert!(
                err.downcast_ref::<ChunkMapError>().is_some(),
                "{what}: a structural fault of a SEGMENTED root must reach a consumer as a \
                 ChunkMapError, or containment misses it and one damaged object ends the \
                 pass for the whole store — got {err}",
            );
        }

        let segments: [(&str, &[u8]); 4] = [
            (
                "chunks that do not sum to the declared byte_len",
                br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0,"byte_len":16}"#,
            ),
            (
                "a segment covering nothing at all",
                br#"{"chunks":[],"byte_offset":0,"byte_len":0}"#,
            ),
            (
                "a byte_offset of the wrong JSON type",
                br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":"0","byte_len":8}"#,
            ),
            (
                "a field the segment encoding does not carry",
                br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0,"byte_len":8,"extra":1}"#,
            ),
        ];
        for (what, bytes) in segments {
            let err = decode::<SegmentRecord>(bytes).unwrap_err();
            assert!(
                err.downcast_ref::<ChunkMapError>().is_some(),
                "{what}: a structural fault of a SEGMENT RECORD must reach a consumer as a \
                 ChunkMapError — got {err}",
            );
        }

        // A `seg:` value is contained WHATEVER is wrong with it, because the KEY already
        // names the record class and the object: truncated mid-token, a field missing, not
        // JSON at all. The generic boundary above cannot prove those from the bytes — a
        // value that does not parse could have been anything — so the read that knows the
        // key does the classifying ([`decode_segment_record`]), and every read of a `seg:`
        // value goes through it.
        for (what, bytes) in [
            (
                "a value truncated mid-token",
                br#"{"chunks":[{"id":1,"scheme":"None","len":8,"pla"#.as_slice(),
            ),
            (
                "a field the record cannot do without",
                br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0}"#
                    .as_slice(),
            ),
            ("not JSON at all", b"\x00\xff".as_slice()),
            ("empty bytes", b"".as_slice()),
            (
                "a field written twice, which a re-parse would collapse",
                br#"{"chunks":[],"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0,"byte_len":8}"#
                    .as_slice(),
            ),
        ] {
            let key = seg_key(&group(7), 3);
            let err = decode_segment_record(&key, bytes).unwrap_err();
            assert!(
                matches!(err, ChunkMapError::SegmentRecordMalformed { ref detail }
                    if detail.contains("seg:")),
                "{what}: a `seg:` value that cannot be read at all is still one object's \
                 fault — contained, and attributed to the key that names it: {err:?}",
            );
        }

        // The negative control, and it matters as much: classification may not spread to
        // records this slice knows nothing about. A legacy FLAT record that does not decode
        // keeps failing exactly as it did before segmentation existed — the containment
        // table's "aborting the pass is acceptable, it is what `decode(&value)?` already
        // does today" arm — and so does every other record class.
        //
        // **Nor may it spread to the OTHER FIELDS of a record whose map is fine.** A
        // segmented root beside an unreadable `state`, `version` or metadata field is not a
        // chunk-map fault: the table decodes, tiles and spans `size`, and what failed is a
        // field this slice did not touch. Containing it would silently convert the fail-loud
        // behaviour every consumer has today for a value of unknown shape into a per-object
        // skip — the classification is about the record classes this slice ADDS, and the map
        // has to be *why* the record did not decode ([`decodes_without`]).
        let good_table = table(1, one);
        for (what, bytes) in [
            (
                "a flat map with a malformed chunk",
                br#"{"size":8,"chunk_map":[{"id":"one","len":8}],"state":"Committed","version":1}"#
                    .to_vec(),
            ),
            (
                "a root whose state is not a state",
                br#"{"size":8,"chunk_map":[],"state":"Committd","version":1}"#.to_vec(),
            ),
            ("a dirent record", br#"{"inode":"one"}"#.to_vec()),
            (
                "a SEGMENTED root whose state is not a state",
                format!(
                    r#"{{"size":16,"chunk_map":{good_table},"state":"Committd","version":1}}"#
                )
                .into_bytes(),
            ),
            (
                "a SEGMENTED root whose version is of the wrong type",
                format!(
                    r#"{{"size":16,"chunk_map":{good_table},"state":"Committed","version":"1"}}"#
                )
                .into_bytes(),
            ),
            (
                "a SEGMENTED root missing a field the record cannot do without",
                format!(r#"{{"size":16,"chunk_map":{good_table},"state":"Committed"}}"#)
                    .into_bytes(),
            ),
            (
                "a SEGMENTED root whose etag is of the wrong type",
                format!(
                    r#"{{"size":16,"chunk_map":{good_table},"state":"Committed","version":1,"etag":7}}"#
                )
                .into_bytes(),
            ),
        ] {
            let err = decode::<InodeRecord>(&bytes).unwrap_err();
            assert!(
                err.downcast_ref::<ChunkMapError>().is_none(),
                "{what}: not a chunk-map structural fault, and classifying it as one would \
                 make a pass CONTINUE over a record it cannot read — got {err}",
            );
        }
        // …and the control for that control: the SAME segmented table beside readable
        // fields decodes, so the cases above fail for the reason they claim to.
        assert!(
            decode::<InodeRecord>(
                format!(
                    r#"{{"size":16,"chunk_map":{good_table},"state":"Committed","version":1}}"#
                )
                .as_bytes()
            )
            .is_ok(),
            "fixture: the shared table must be a valid segmented map, or the negative cases \
             above would pass for the wrong reason",
        );
    }

    /// **The fault a re-parse reports must be the fault the record has.**
    ///
    /// The classification is the operator's whole account of a record they now have to repair:
    /// `SizeSpanMismatch` says "the table and `size` disagree, and here are the two numbers",
    /// which is actionable and checkable. A record whose table *does* span its `size` and which
    /// still did not decode has a fault the re-parse cannot see — a duplicate field the
    /// `serde_json::Value` collapsed is the worked example — and the honest answer is the
    /// class-level [`ChunkMapError::SegmentedRootMalformed`] saying so.
    ///
    /// Reporting the size disagreement anyway would send whoever is on call after a
    /// contradiction: the two numbers in the message are equal. Both arms are asserted here,
    /// so the guard cannot collapse into either one.
    #[test]
    fn a_segmented_root_fault_names_the_disagreement_it_actually_found() {
        let table = |span: u64| {
            format!(
                r#"{{"group":{{"nonce":"{NONCE}","epoch":7}},"segment_count":1,"segments":[{{"index":0,"byte_offset":0,"byte_len":{span}}}]}}"#
            )
        };
        // The table tiles and spans `size`, and the record still does not decode: `state`
        // is written twice, which strict decoding refuses and a re-parse cannot see.
        let collapsed = format!(
            r#"{{"size":16,"chunk_map":{},"state":"Committed","state":"Committed","version":1}}"#,
            table(16),
        );
        assert!(
            matches!(
                decode::<InodeRecord>(collapsed.as_bytes())
                    .unwrap_err()
                    .downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::SegmentedRootMalformed { .. }),
            ),
            "a root whose table agrees with its size has no size disagreement to report: \
             {collapsed}",
        );
        // …and the arm that DOES have one still reports it, with the numbers that disagree.
        let mismatched = format!(
            r#"{{"size":17,"chunk_map":{},"state":"Committed","version":1}}"#,
            table(16),
        );
        assert_eq!(
            decode::<InodeRecord>(mismatched.as_bytes())
                .unwrap_err()
                .downcast_ref::<ChunkMapError>(),
            Some(&ChunkMapError::SizeSpanMismatch { size: 17, span: 16 }),
            "the record whose table really does disagree with `size` is named exactly, \
             numbers included",
        );
    }

    /// A segment record carrying no chunks (or no bytes) is corruption, not an empty
    /// segment: no publication can plan one ([`push_segment`] closes a segment only when it
    /// has chunks) and the root would refuse its `SegmentRef` ([`ChunkMapError::EmptySegment`]).
    /// Admitting it as a value would let a resolve concatenate a hole into an object's
    /// chunk list and call the result the map.
    #[test]
    fn an_empty_segment_record_is_not_a_value() {
        assert_eq!(
            decode::<SegmentRecord>(br#"{"chunks":[],"byte_offset":0,"byte_len":0}"#)
                .unwrap_err()
                .downcast_ref::<ChunkMapError>(),
            Some(&ChunkMapError::EmptySegmentRecord {
                byte_offset: 0,
                chunks: 0,
            }),
        );
        assert_eq!(
            SegmentRecord::new(Vec::new(), 24).unwrap_err(),
            ChunkMapError::EmptySegmentRecord {
                byte_offset: 24,
                chunks: 0,
            },
            "…and the constructor refuses what the decode refuses, so the committer can \
             never write one either",
        );
        assert_eq!(
            SegmentRecord::new(vec![chunk(1, 0, vec![0])], 0).unwrap_err(),
            ChunkMapError::EmptySegmentRecord {
                byte_offset: 0,
                chunks: 1,
            },
            "a segment whose chunks cover no bytes is the same fault one field over: the \
             root's own table rejects a zero-length SegmentRef",
        );
    }

    /// A segment record whose own extent runs past `u64` describes a last byte that cannot
    /// be addressed — structurally impossible, so it is an error at decode and in the
    /// constructor rather than a value the ranged read would probe with an addition that
    /// wraps. (The root's table has the same rule for its running offset,
    /// [`ChunkMapError::SegmentSpanOverflow`]; this is the single record's own.)
    #[test]
    fn a_segment_record_whose_extent_runs_past_u64_is_not_a_value() {
        let raw = format!(
            r#"{{"chunks":[{{"id":1,"scheme":"None","len":{len},"placement":[0]}}],"byte_offset":{offset},"byte_len":{len}}}"#,
            len = u64::MAX - 7,
            offset = 8,
        );
        assert_eq!(
            decode::<SegmentRecord>(raw.as_bytes())
                .unwrap_err()
                .downcast_ref::<ChunkMapError>(),
            Some(&ChunkMapError::SegmentSpanUnrepresentable {
                byte_offset: 8,
                byte_len: u64::MAX - 7,
            }),
            "the stored bytes are refused, and as a chunk-map fault so a consumer can \
             contain it to this object",
        );
        assert_eq!(
            SegmentRecord::new(vec![chunk(1, u64::MAX - 7, vec![0])], 8).unwrap_err(),
            ChunkMapError::SegmentSpanUnrepresentable {
                byte_offset: 8,
                byte_len: u64::MAX - 7,
            },
            "…and the constructor refuses what the decode refuses, so no committer can \
             write one",
        );
        // The boundary is exact: a segment ending at the last addressable byte is a value.
        assert!(SegmentRecord::new(vec![chunk(1, u64::MAX - 8, vec![0])], 8).is_ok());
    }

    /// A segment record whose chunks do not add up to the span it declares is a torn map
    /// at the first consumer; rejected at decode for the same reason.
    #[test]
    fn a_segment_record_whose_chunks_do_not_sum_to_its_span_is_rejected() {
        let bad = br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0,"byte_len":16}"#;
        assert!(decode::<SegmentRecord>(bad).is_err());
        let good = br#"{"chunks":[{"id":1,"scheme":"None","len":8,"placement":[0]}],"byte_offset":0,"byte_len":8}"#;
        let record: SegmentRecord = decode(good).unwrap();
        assert_eq!(encode(&record).as_ref(), good.as_slice());
    }

    /// Aggregating stored lengths is **checked**, both at decode and in the constructor.
    /// An unchecked sum panics in a debug build and, released, WRAPS — and a wrapped total
    /// can match a forged `byte_len`, admitting a structurally impossible record as a
    /// value (the rubric's *Protocol input* class: oversize input is an error, never
    /// silently accepted).
    #[test]
    fn overflowing_chunk_lengths_are_rejected_never_wrapped() {
        let overflowing = vec![chunk(1, u64::MAX, vec![0]), chunk(2, 1, vec![0])];
        assert_eq!(
            SegmentRecord::new(overflowing, 0).unwrap_err(),
            ChunkMapError::SegmentLengthOverflow { chunks: 2 },
        );
        // …and the same input as STORED BYTES, whose `byte_len` is the wrapped total the
        // unchecked sum would have produced (`u64::MAX + 1 == 0`).
        let raw = format!(
            r#"{{"chunks":[{{"id":1,"scheme":"None","len":{max},"placement":[0]}},{{"id":2,"scheme":"None","len":1,"placement":[0]}}],"byte_offset":0,"byte_len":0}}"#,
            max = u64::MAX,
        );
        assert!(
            decode::<SegmentRecord>(raw.as_bytes()).is_err(),
            "a wrapped total must not be confirmable by decode",
        );
        // The root's tiling is checked the same way, one level up.
        assert_eq!(
            SegmentedMap::new(
                group(7),
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
            )
            .unwrap_err(),
            ChunkMapError::SegmentSpanOverflow { index: 1 },
        );
    }

    /// The root's segment table is the object's byte index, so a `size` that disagrees
    /// with it is structural corruption — the gateway would frame a response from one and
    /// stream the other. Rejected AT DECODE, and only for the segmented shape (a flat
    /// map's chunk list *is* the bytes, and stays liberal on read).
    #[test]
    fn a_segmented_root_whose_size_disagrees_with_its_table_is_rejected() {
        let table = String::from_utf8(canonical_segmented()).unwrap();
        let good = format!(r#"{{"size":24,"chunk_map":{table},"state":"Committed","version":1}}"#);
        let record: InodeRecord = decode(good.as_bytes()).unwrap();
        assert_eq!(record.size, 24);
        assert_eq!(
            encode(&record).as_ref(),
            good.as_bytes(),
            "the check must not disturb the record's encoding",
        );

        let bad = format!(r#"{{"size":25,"chunk_map":{table},"state":"Committed","version":1}}"#);
        let err = decode::<InodeRecord>(bad.as_bytes())
            .expect_err("a size that is not the table's span must not decode");
        assert!(
            err.to_string().contains("spans 24 bytes"),
            "the error names both numbers: {err}",
        );

        // A FLAT record whose size disagrees with its chunks still decodes — unchanged
        // behaviour, deliberately (there is no second statement of the object's extent).
        let flat = br#"{"size":99,"chunk_map":[{"id":7,"scheme":"None","len":8,"placement":[1]}],"state":"Committed","version":1}"#;
        assert!(decode::<InodeRecord>(flat).is_ok());
    }

    /// The `seg:` key's index is FIXED width, so one segment has exactly one key and a
    /// stray record cannot smuggle itself into a resolution under a second spelling.
    #[test]
    fn a_segment_key_index_of_the_wrong_width_is_rejected() {
        assert_eq!(
            seg_key(&group(7), 1),
            format!("seg:{NONCE}:7:000001").into_bytes()
        );
        assert_eq!(
            parse_seg_key(&seg_key(&group(7), 12)).unwrap(),
            (NONCE.to_string(), 7, 12)
        );
        for bad in [
            format!("seg:{NONCE}:7:1"),
            format!("seg:{NONCE}:7:00001"),
            format!("seg:{NONCE}:7:0000001"),
            format!("seg:{NONCE}:7:00000a"),
            // The epoch is CANONICAL decimal. `u64::from_str` would accept all three of
            // these as `7`, giving one segment three keys — the exact ambiguity the
            // fixed-width index rule forbids one field to the right (the rubric's
            // *Grammar strictness* class: no `+`/`-` signs via `from_str`).
            format!("seg:{NONCE}:+7:000001"),
            format!("seg:{NONCE}:07:000001"),
            format!("seg:{NONCE}:007:000001"),
            format!("seg:{NONCE}::000001"),
            format!("seg:{NONCE}:x:000001"),
            format!("seg:{NONCE}:7:000001:9"),
            "seg:short:7:000001".to_string(),
        ] {
            assert!(
                parse_seg_key(bad.as_bytes()).is_err(),
                "{bad} must not parse as a segment key",
            );
        }
        // …and epoch 0 IS canonical: the rule rejects leading zeros, not the value zero.
        assert_eq!(
            parse_seg_key(format!("seg:{NONCE}:0:000001").as_bytes()).unwrap(),
            (NONCE.to_string(), 0, 1),
        );
    }

    // ---- (iii) staged publication: write the segments, THEN flip ----

    fn publication<'a>(prior: &'a InodeRecord, chunks: Vec<ChunkRef>) -> SegmentedPublication<'a> {
        let mut pubn = SegmentedPublication {
            inode_id: 1,
            root: RootPrecondition::Supersede(prior),
            group: group(7),
            chunks,
            meta: ObjectMeta::default(),
            // Fenced by construction, in BOTH phases: an unfenced publication is refused
            // ([`ChunkMapError::Unfenced`]), because the rollback that reclaims a crashed
            // attempt deletes its `seg:` records and only the fence makes this publisher
            // lose that race. `session_batch` is a fn item, so this borrows nothing.
            fence_key: SESSION_KEY.to_vec(),
            segment_batch: &session_batch,
            resume_from: 0,
            batch_budget: None,
            ops_budget: None,
            flip: WriteBatch::new(),
        };
        pubn.flip = flip_fence(&pubn);
        pubn
    }

    /// The default per-batch contribution: the modelled session's fence at epoch 7 plus
    /// the progress cursor that same record carries (`0016:350`,`:2336-2338`).
    fn session_batch(info: &SegmentBatchInfo) -> WriteBatch {
        session_contribution(7, info)
    }

    /// The **flip's** own contribution: the session record as the segment-write phase
    /// leaves it, compare-and-swapped **to `Completed`** (`0016:2338-2345`).
    ///
    /// Both halves are required. Without the precondition the flip is unfenced, and a
    /// rollback that deleted this attempt's segments between the phase and the flip would
    /// still let the root CAS succeed: a live root naming records that are gone. Without
    /// the *transition* the fence record is still `Completing@E` after the flip, so that
    /// same rollback commits **afterwards** and deletes the segments the now-live root
    /// names — the publication instant is the batch that moves the session, not one that
    /// merely reads it.
    fn flip_fence(pubn: &SegmentedPublication<'_>) -> WriteBatch {
        let written = pubn.plan().map_or(0, |planned| planned.len()) as u32;
        WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, written))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
    }

    /// The session record's terminal value — what the flip moves the fence to.
    fn completed_session_value(epoch: u64) -> Bytes {
        Bytes::from(format!("Completed@{epoch}"))
    }

    /// Seed the modelled session record at `Completing@7|written=0` — the state a
    /// publication's first segment-write batch fences against.
    async fn open_session(store: &impl MetadataStore) {
        put(store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
    }

    /// A segment target and a batch budget small enough that a 40-chunk fixture produces
    /// several segments across several batches — the same code paths the real constants
    /// take at ~113 000 chunks, at a size a unit test (and a mutation run that repeats the
    /// suite once per mutant) can afford.
    const SMALL_SEGMENT: usize = 200;
    const SMALL_BUDGET: usize = 700;

    /// Many small chunks, so the byte-budgeted split produces several segments.
    fn many_chunks(n: u64) -> Vec<ChunkRef> {
        (0..n)
            .map(|i| chunk(ChunkId::from(i + 1), 8, vec![i % 3]))
            .collect()
    }

    #[test]
    fn staged_publication_writes_segments_then_flips_the_root() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(1_500);
        let pubn = publication(&prior, chunks.clone());
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let root_before = store.get(&inode_key(1)).await.unwrap().unwrap();

            assert_eq!(
                pubn.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            // BEFORE the flip: the segments are durable and the root still names the
            // prior generation — the publication instant has not happened yet.
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap().unwrap(),
                root_before
            );
            assert!(
                !store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .is_empty(),
                "the segment-write phase is durable before the flip",
            );

            assert_eq!(pubn.flip(&store).await.unwrap(), CommitOutcome::Committed);
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(root.chunk_map.group(), Some(&group(7)));
            assert_eq!(root.version, prior.version + 1);

            // …and the whole ordered chunk list resolves back out of it.
            let resolved = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .unwrap();
            assert_eq!(resolved.chunks(), Some(chunks.as_slice()));
        });
    }

    /// The key a modelled session's record lives under.
    const SESSION_KEY: &[u8] = b"mpu:upload-1";

    /// The **one** record a real caller's per-batch contribution touches: the session
    /// record carries the fence state (`Completing@E`) *and* the `segments_written`
    /// cursor, exactly as `0016:350` / `:2336-2338` describe them.
    fn session_value(epoch: u64, written: u32) -> Bytes {
        Bytes::from(format!("Completing@{epoch}|written={written}"))
    }

    /// One batch's contribution from that session: require the cursor its predecessor
    /// left, and advance it to what this batch makes durable.
    fn session_contribution(epoch: u64, info: &SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(epoch, info.first_index))
            .put(SESSION_KEY.to_vec(), session_value(epoch, info.written))
    }

    /// **The segment-write fence is load-bearing and it must ride on phase 1.** It is
    /// `require(mpu == Completing@E)` (`0016:2336-2338`) — the only thing stopping a
    /// lapsed session from writing segments under a group another attempt now owns. So a
    /// fence that does not hold must refuse the phase and leave the `seg:` range EMPTY,
    /// not merely fail the flip.
    #[test]
    fn a_segment_write_batch_that_loses_its_fence_writes_no_segment() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            // The session is still Open at another epoch: this attempt's fence is lapsed.
            put(&store, SESSION_KEY.to_vec(), Bytes::from_static(b"Open@2")).await;

            let mut pubn = publication(&prior, many_chunks(40));
            pubn.segment_batch = &contribute;
            // A MULTI-batch phase (at the real constants a split needs ~113 000 chunks),
            // so "every batch carries the fence" is asserted over more than one batch.
            let batches = pubn.staged_batches(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                SMALL_BUDGET,
                MAX_BATCH_OPS,
            );
            let batches = batches.unwrap();
            assert!(batches.len() > 1, "fixture: more than one batch");
            for batch in &batches {
                assert!(
                    batch.preconditions.iter().any(|p| p.key == SESSION_KEY
                        && p.expected.as_ref() == Some(&session_value(7, n_first(batch)))),
                    "every segment-write batch carries the caller's fence, at its own cursor",
                );
            }

            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches.clone())
                    .await
                    .unwrap(),
                CommitOutcome::Conflict,
                "a lapsed fence must refuse the segment-write phase",
            );
            assert!(
                store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .is_empty(),
                "…and write no segment record at all",
            );

            // The mirror, through the production entry point at the real constants (ONE
            // batch there, so the fence's starting cursor is 0): with the fence satisfied
            // the same phase commits.
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            assert_eq!(
                pubn.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed,
            );
            assert!(!store
                .scan(&seg_range_prefix(&group(7)))
                .await
                .unwrap()
                .is_empty());
        });
    }

    /// The `segments_written` cursor a batch's fence requires: the index of the first
    /// segment it writes, parsed back out of its own first `seg:` key.
    fn n_first(batch: &WriteBatch) -> u32 {
        parse_seg_key(&batch.puts[0].0).unwrap().2
    }

    /// **A multi-batch publication must be able to FINISH.** The fence is an exact-value
    /// precondition on the very record the progress cursor advances, so a fence pinned
    /// once for the whole phase is stale the instant batch 0 commits: every later batch
    /// would `Conflict` against the value its own predecessor wrote, and a map large
    /// enough to need staging could never be published at all. The fence is therefore
    /// stated **per batch**, and this asserts the phase runs to the end and leaves the
    /// cursor naming every segment.
    #[test]
    fn a_multi_batch_phase_re_fences_each_batch_and_runs_to_completion() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        let mut pubn = publication(&prior, many_chunks(40));
        pubn.segment_batch = &contribute;
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            let batches = pubn
                .staged_batches(
                    SMALL_SEGMENT,
                    MAX_ROOT_SEGMENTS,
                    SMALL_BUDGET,
                    MAX_BATCH_OPS,
                )
                .unwrap();
            assert!(batches.len() > 2, "fixture: several batches");
            // Each batch requires what its predecessor wrote — never one pinned value.
            let required: Vec<Option<Bytes>> = batches
                .iter()
                .map(|b| {
                    b.preconditions
                        .iter()
                        .find(|p| p.key == SESSION_KEY)
                        .and_then(|p| p.expected.clone())
                })
                .collect();
            let wrote: Vec<Option<Bytes>> = batches
                .iter()
                .map(|b| {
                    b.puts
                        .iter()
                        .find(|(k, _)| k == SESSION_KEY)
                        .map(|(_, v)| v.clone())
                })
                .collect();
            assert_eq!(required[1..], wrote[..wrote.len() - 1]);
            assert!(
                required
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    == required.len(),
                "no two batches carry the same fence value",
            );

            let planned = pubn.plan_with(SMALL_SEGMENT, MAX_ROOT_SEGMENTS).unwrap();
            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
                "every batch after the first must fence against the cursor its predecessor \
                 wrote, or a staged publication can never complete",
            );
            assert_eq!(
                store.get(SESSION_KEY).await.unwrap(),
                Some(session_value(7, planned.len() as u32)),
            );
            assert_eq!(
                store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .len(),
                planned.len(),
            );
        });
    }

    /// **The envelope is charged on the WHOLE batch, and it fails closed.** The caller's
    /// per-batch contribution is part of the same transaction, so a split that charged
    /// only its own segment records would hand the backend a transaction over its limit —
    /// rejected permanently, and not as a `Conflict` the publication could retry out of
    /// (`crates/traits/src/lib.rs:747-752`).
    /// A budget several segment records wide, so the re-split has room to be WRONG in
    /// both directions: too coarse breaches the envelope, too fine spends transactions
    /// the envelope does not require.
    const RESPLIT_BUDGET: usize = 1_400;

    #[test]
    fn the_split_charges_the_callers_contribution_against_the_envelope() {
        let prior = InodeRecord::new_empty();
        // A ~300-byte contribution per batch: uncharged, it pushes every batch over.
        let bulky = |info: &SegmentBatchInfo| {
            session_contribution(7, info)
                .put(b"retire:records:1".to_vec(), Bytes::from(vec![b'x'; 300]))
        };
        let mut pubn = publication(&prior, many_chunks(40));
        pubn.segment_batch = &bulky;
        let batches = pubn
            .staged_batches(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                RESPLIT_BUDGET,
                MAX_BATCH_OPS,
            )
            .unwrap();
        assert!(batches.len() > 1, "fixture: more than one batch");
        assert!(
            batches[0].puts.len() > 2,
            "fixture: the budget must fit SEVERAL segment records plus the contribution, \
             or a split that reserved far too much would look identical to a correct one",
        );
        for (n, batch) in batches.iter().enumerate() {
            assert!(
                batch_bytes(batch) <= RESPLIT_BUDGET,
                "batch {n} carries {} bytes, over the {RESPLIT_BUDGET}-byte envelope",
                batch_bytes(batch),
            );
            assert!(
                batch.puts.iter().any(|(k, _)| k == b"retire:records:1"),
                "…and it still carries the caller's contribution",
            );
            // …and the room reserved for the contribution is the contribution's OWN
            // bytes, so each batch is still as full as the envelope allows: the next
            // batch's first record genuinely could not have ridden along. A reserve
            // computed from the whole batch would collapse the split to one record per
            // commit — inside the envelope, but spending transactions it does not need.
            if let Some(next) = batches.get(n + 1) {
                let (key, value) = &next.puts[0];
                assert!(
                    batch_bytes(batch) + key.len() + value.len() > RESPLIT_BUDGET,
                    "batch {n} left room for the next batch's first record",
                );
            }
        }

        // The envelope is a ceiling, it is INCLUSIVE, and the room reserved for the
        // caller's contribution is charged exactly ONCE. The budget here is built by hand
        // — three segment records plus one contribution, to the byte — so a split that
        // treated the ceiling as exclusive, or that double-charged the reserve, would fit
        // only two and spend a transaction the envelope did not ask for.
        let plain = publication(&prior, many_chunks(6));
        let planned = plain.plan_with(SMALL_SEGMENT, MAX_ROOT_SEGMENTS).unwrap();
        assert_eq!(planned.len(), 3, "fixture: three segment records");
        // Each record costs its key and value **plus** the `require_absent` guarding the
        // put — a precondition with no expected value is charged its key alone
        // (`batch_bytes`), and no segment row is written without one
        // (`assemble_segment_batches`).
        let records: usize = planned
            .iter()
            .map(|(segment, record)| {
                seg_key(&group(7), segment.index).len() * 2 + encode(record).len()
            })
            .sum();
        let contribution = batch_bytes(&session_contribution(
            7,
            &SegmentBatchInfo {
                batch: 0,
                batches: 1,
                first_index: 0,
                last_index: 2,
                written: 3,
            },
        ));
        let exact = records + contribution;
        let at_budget = plain
            .staged_batches(SMALL_SEGMENT, MAX_ROOT_SEGMENTS, exact, MAX_BATCH_OPS)
            .unwrap();
        assert_eq!(
            at_budget.len(),
            1,
            "a batch of exactly {exact} bytes fits an {exact}-byte envelope",
        );
        assert_eq!(
            batch_bytes(&at_budget[0]),
            exact,
            "…landing exactly on the ceiling, contribution included",
        );
        assert!(
            plain
                .staged_batches(SMALL_SEGMENT, MAX_ROOT_SEGMENTS, exact - 1, MAX_BATCH_OPS)
                .unwrap()
                .len()
                > 1,
            "…and one byte less does not fit",
        );

        // And when it CANNOT fit, the answer is a typed error, never an over-envelope
        // batch: the split is already one segment record per batch and the contribution
        // alone exceeds what is left.
        let huge = |info: &SegmentBatchInfo| {
            session_contribution(7, info)
                .put(b"retire:records:1".to_vec(), Bytes::from(vec![b'x'; 5_000]))
        };
        pubn.segment_batch = &huge;
        let err = pubn
            .staged_batches(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                SMALL_BUDGET,
                MAX_BATCH_OPS,
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                ChunkMapError::BatchOverBudget { bytes, budget, .. }
                    if bytes > SMALL_BUDGET && budget == SMALL_BUDGET
            ),
            "an unfittable contribution fails closed: {err}",
        );
    }

    /// [`batch_bytes`] is the envelope's unit of account, so its arithmetic is pinned
    /// against a hand-counted batch rather than against itself — every precondition key
    /// and expected value, every put key and value, every delete key, and nothing else.
    /// An absent-precondition is counted for its key alone: it constrains a key, it
    /// carries no value.
    #[test]
    fn batch_bytes_counts_every_key_value_and_precondition() {
        assert_eq!(batch_bytes(&WriteBatch::new()), 0);
        let batch = WriteBatch::new()
            .require(b"aaa".to_vec(), Bytes::from_static(b"bbbbb"))
            .require_absent(b"cc".to_vec())
            .put(b"dddd".to_vec(), Bytes::from_static(b"eeeeeee"))
            .delete(b"ffffffffff".to_vec());
        assert_eq!(
            batch_bytes(&batch),
            3 + 5 + 2 + 4 + 7 + 10,
            "key + expected, key alone for an absent precondition, key + value, key",
        );
    }

    /// A resumed publication (`0016:350`, decision 7(c)/(d)): the completer crashed
    /// mid-phase, its recovered `segments_written` cursor says how many segments are
    /// durable, and the restarted attempt writes **only the rest** — re-committing a
    /// segment whose cursor has already advanced would fence itself out.
    #[test]
    fn a_resumed_publication_writes_only_the_segments_the_cursor_has_not() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        // The REAL segment target, so the root the flip installs describes exactly the
        // segments the phase wrote; only the batch envelope is narrowed, to one segment
        // record per commit, so the crash lands *between* batches.
        let chunks = many_chunks(1_500);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        let mut pubn = publication(&prior, chunks.clone());
        pubn.segment_batch = &contribute;
        pubn.batch_budget = Some(SEGMENT_TARGET_BYTES + 1_000);
        let planned = pubn.plan().unwrap();
        assert!(planned.len() > 1, "fixture: more than one segment");
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;

            // Phase 1 dies after its first batch.
            let batches = pubn.segment_batches().unwrap();
            assert!(batches.len() > 1, "fixture: more than one batch");
            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches[..1].to_vec())
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
            let durable = store
                .scan(&seg_range_prefix(&group(7)))
                .await
                .unwrap()
                .len() as u32;
            assert!(durable > 0 && (durable as usize) < planned.len());
            assert_eq!(
                store.get(SESSION_KEY).await.unwrap(),
                Some(session_value(7, durable)),
                "the recovered cursor names exactly what is durable",
            );

            // The restarted attempt resumes from the cursor.
            let mut resumed = publication(&prior, chunks.clone());
            resumed.segment_batch = &contribute;
            resumed.batch_budget = pubn.batch_budget;
            resumed.resume_from = durable;
            let rest = resumed.segment_batches().unwrap();
            let indices: Vec<u32> = rest
                .iter()
                .flat_map(|b| b.puts.iter())
                .filter(|(k, _)| k.starts_with(SEG_PREFIX))
                .map(|(k, _)| parse_seg_key(k).unwrap().2)
                .collect();
            assert_eq!(
                indices,
                (durable..planned.len() as u32).collect::<Vec<_>>(),
                "a resumed phase re-writes nothing the cursor already counts",
            );
            assert_eq!(
                SegmentedPublication::commit_batches(&store, rest)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
                "…and it fences against the RECOVERED cursor, not against zero",
            );
            assert_eq!(
                store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .len(),
                planned.len(),
                "…and the phase still ends with every segment durable",
            );
            assert_eq!(
                resumed.flip(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap()
                    .chunks(),
                Some(chunks.as_slice()),
                "a resumed publication publishes the same map an uninterrupted one would",
            );

            // Resuming at the whole plan is a no-op phase, not an error: everything the
            // cursor names is already durable.
            resumed.resume_from = planned.len() as u32;
            assert!(resumed.segment_batches().unwrap().is_empty());
            // A cursor PAST the plan is a disagreement the attempt must not paper over.
            resumed.resume_from = planned.len() as u32 + 1;
            assert_eq!(
                resumed.segment_batches().unwrap_err(),
                ChunkMapError::ResumePastPlan {
                    resume_from: planned.len() as u32 + 1,
                    segments: planned.len(),
                },
            );
        });
    }

    /// **A publication that cannot be published refuses before it makes anything
    /// durable.** Every refusal the committer can decide with zero I/O — an unfenced
    /// flip, a colliding contribution, a value or key over the backend ceilings, a batch
    /// over either envelope — is decided from the plan and the caller's own batches, so
    /// none of them needs a single `seg:` record to exist first.
    ///
    /// Deciding them in phase order instead is what makes a *deterministic* refusal
    /// expensive: the whole segment phase is committed, the caller's `segments_written`
    /// cursor is advanced past records nothing will ever name, and the answer — identical
    /// at every retry — arrives only after all of it. The garbage then has to be reclaimed
    /// by the rollback obligation, for a publication that was refusable before it started.
    #[test]
    fn a_deterministically_refused_publication_writes_no_segment_at_all() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(1_500);
        // Each refusable flip, and the error it must raise. The segment phase is valid
        // and identical in every case — only the flip is unpublishable.
        let over_ceiling = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 0))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .put(
                b"retire:records:1".to_vec(),
                Bytes::from(vec![b'x'; 200_000]),
            );
        let collides = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 0))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .put(seggrp_key(NONCE), Bytes::from_static(b"{}"));
        // Pinned but never moved: the fence still reads `Completing@E` after the flip, so
        // the rollback racing it can still commit and delete the published segments.
        let unmoved = WriteBatch::new().require(SESSION_KEY.to_vec(), session_value(7, 0));
        for (what, flip) in [
            ("an unfenced flip", WriteBatch::new()),
            ("a flip carrying an over-ceiling value", over_ceiling),
            ("a flip that writes the publication's own records", collides),
            ("a flip that pins its fence without moving it", unmoved),
        ] {
            // `pubn.segment_batch` is left at the default, which advances the cursor — the
            // phase-1 refusals get their own legs below, where the flip is the valid one.
            let store = mem_store();
            let mut pubn = publication(&prior, chunks.clone());
            pubn.flip = flip;
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;

                let err = pubn
                    .publish(&store)
                    .await
                    .expect_err(&format!("{what} must be refused"));
                assert!(
                    err.downcast_ref::<ChunkMapError>().is_some(),
                    "{what}: a typed refusal, not an opaque error: {err}",
                );
                assert_eq!(
                    store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                    Vec::new(),
                    "{what}: a refused publication leaves ZERO durable seg: records",
                );
                assert_eq!(
                    store.get(SESSION_KEY).await.unwrap(),
                    Some(session_value(7, 0)),
                    "{what}: …and no caller cursor movement either",
                );
                assert_eq!(
                    store.get(&inode_key(1)).await.unwrap(),
                    Some(encode(&prior)),
                    "{what}: …and the root still names the prior generation",
                );
            });
        }

        // **A publication is a WRITE, and the write boundary is strict** (ADR-0040
        // decisions 3–4). A chunk whose placement vector is non-empty and not
        // `fragment_count()` long is malformed: persisted, every reader identity-fills its
        // missing tail and goes to a D server holding nothing for that chunk, and the
        // maintenance paths that are strict about the same field refuse it. Refused with
        // the other deterministic refusals — before a segment record exists.
        let mut malformed = chunks.clone();
        malformed[3].placement = vec![0, 1];
        {
            let store = mem_store();
            let pubn = publication(&prior, malformed);
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;
                assert_eq!(
                    pubn.publish(&store)
                        .await
                        .expect_err("a malformed placement must be refused")
                        .downcast_ref::<ChunkMapError>(),
                    Some(&ChunkMapError::WritePlacementMalformed {
                        position: 3,
                        expected: 1,
                        actual: 2,
                    }),
                    "the refusal must name the chunk's position and both lengths",
                );
                assert_eq!(
                    store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                    Vec::new(),
                    "…and nothing is durable: the check is part of the plan, not of the write",
                );
            });
        }

        // **Phase 1 is held to the same fence rule as the flip**, and for the concurrency
        // the cursor exists to settle: a segment batch that pins `Completing@E|written=N`
        // and leaves it at `N` is satisfiable again the instant it commits, so a second
        // completer of the same generation can verify the same range, write its own
        // segments over this one's, and each believe it wrote what the root will name. The
        // committer cannot read the caller's record format, but it can require the batch to
        // MOVE the record it pins — which is what advancing a cursor looks like from here.
        let pinning = |_: &SegmentBatchInfo| {
            WriteBatch::new().require(SESSION_KEY.to_vec(), session_value(7, 0))
        };
        let backwards = |_: &SegmentBatchInfo| {
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, 0))
                .put(SESSION_KEY.to_vec(), session_value(7, 0))
        };
        // **And a delete is not the strongest move before the flip — it is the end of the
        // publication.** A delete satisfies "the batch moved the record it pinned", which is
        // what the flip needs, and at the flip it is genuinely the strongest transition
        // there is. In phase 1 it removes the record every *later* batch and the flip are
        // fenced on, so the publication can never finish — after it has made `seg:` records
        // durable — and it removes the rollback's own precondition, so the reaper that would
        // reclaim those rows has lost what it keys on. Refused as its own typed error, not
        // folded into the "did not move it" one: the caller's mistake, and its fix, are
        // different.
        let deleting = |_: &SegmentBatchInfo| {
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, 0))
                .delete(SESSION_KEY.to_vec())
        };
        let not_transitioned = ChunkMapError::FenceNotTransitioned {
            fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
            phase: "segments",
            batch: 0,
        };
        for (what, hook, expected) in [
            (
                "a segment batch that only reads its fence",
                &pinning as &dyn Fn(&SegmentBatchInfo) -> WriteBatch,
                &not_transitioned,
            ),
            (
                "a segment batch that rewrites the value it pinned",
                &backwards,
                &not_transitioned,
            ),
            (
                "a segment batch that deletes the fence the rest of the publication needs",
                &deleting,
                &ChunkMapError::FenceRemovedBeforeFlip {
                    fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
                    batch: 0,
                },
            ),
        ] {
            let store = mem_store();
            let mut pubn = publication(&prior, chunks.clone());
            pubn.segment_batch = hook;
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;
                assert_eq!(
                    pubn.publish(&store)
                        .await
                        .expect_err(&format!("{what} must be refused"))
                        .downcast_ref::<ChunkMapError>(),
                    Some(expected),
                    "{what}: a typed refusal naming the phase and the batch",
                );
                assert_eq!(
                    store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                    Vec::new(),
                    "{what}: …decided with zero I/O, so nothing is durable",
                );
                assert_eq!(
                    store.get(SESSION_KEY).await.unwrap(),
                    Some(session_value(7, 0)),
                    "{what}: …and the fence record itself is untouched, so the attempt is \
                     still rollback-able",
                );
            });
        }

        // The control that keeps the rule from being "no delete anywhere": at the **flip**,
        // which is the last batch this publication commits, a delete IS the transition —
        // nothing later is fenced on the record, and the rollback racing the flip loses its
        // own precondition, which is precisely the point.
        {
            let store = mem_store();
            let mut pubn = publication(&prior, chunks.clone());
            let written = pubn.plan().unwrap().len() as u32;
            pubn.flip = WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, written))
                .delete(SESSION_KEY.to_vec());
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;
                assert_eq!(
                    pubn.publish(&store).await.unwrap(),
                    CommitOutcome::Committed,
                    "a flip that RETIRES its fence record is a valid publication",
                );
                assert_eq!(
                    store.get(SESSION_KEY).await.unwrap(),
                    None,
                    "…and the fence is gone, so no rollback fenced on it can follow the \
                     published root",
                );
            });
        }
    }

    /// **A fence that returns to a state it has already held is not a fence.**
    ///
    /// The per-batch rule ([`ChunkMapError::FenceNotTransitioned`]) says each batch must move
    /// the record it pins. A contribution can satisfy that at every batch and still walk the
    /// fence `A → B → A` across the phase — and the instant it is back at `A`, every
    /// precondition satisfiable at `A` is satisfiable again: a second completer of the same
    /// generation, or the rollback this publisher is racing, commits against it and overwrites
    /// the `seg:` records this attempt is still writing. The root then names a hybrid of two
    /// plans, which the durable-prefix check catches only when they disagree about byte
    /// extents.
    ///
    /// This is the half of the ABA hazard that needs no knowledge of the caller's value
    /// grammar (which values are *terminal* stays #636's, and stays declined): that the fence
    /// repeated a state is visible from the batches alone, before any of them commits.
    ///
    /// The fixture includes the fault rather than describing it: the session is seeded at the
    /// value the first batch pins, so **without** the rule every batch's precondition genuinely
    /// holds and the cycle really happens on the store. The control in the same test is the
    /// ordinary advancing cursor over the same chunk list and the same budget, which must still
    /// publish.
    #[test]
    fn a_phase_whose_fence_returns_to_a_state_it_held_is_refused() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(3_000);
        // One segment per batch, so the phase has several batches to cycle across.
        let budget = Some(SEGMENT_TARGET_BYTES + 2_000);
        let phase_a = Bytes::from_static(b"Completing@7|state=a");
        let phase_b = Bytes::from_static(b"Completing@7|state=b");
        assert_ne!(
            phase_a, phase_b,
            "fixture: the two states must differ, or the per-batch rule would refuse this \
             contribution before the cycle rule ever saw it",
        );
        let cycling = |info: &SegmentBatchInfo| {
            let (pin, next) = if info.batch.is_multiple_of(2) {
                (phase_a.clone(), phase_b.clone())
            } else {
                (phase_b.clone(), phase_a.clone())
            };
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), pin)
                .put(SESSION_KEY.to_vec(), next)
        };

        let store = mem_store();
        let mut pubn = publication(&prior, chunks.clone());
        pubn.segment_batch = &cycling;
        pubn.batch_budget = budget;
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, SESSION_KEY.to_vec(), phase_a.clone()).await;

            assert_eq!(
                pubn.publish(&store)
                    .await
                    .expect_err("a phase that walks its fence back must be refused")
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::FenceCycled {
                    fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
                    phase: "segments",
                    batch: 1,
                }),
                "the refusal must name the batch that took the fence BACK — batch 1, whose put \
                 restores the state batch 0 pinned, not the later batch that pins it again: a \
                 rule that watched only the preconditions would miss this spelling entirely",
            );
            assert_eq!(
                store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                Vec::new(),
                "…decided with zero I/O, so not one segment of the cycled phase is durable",
            );
            assert_eq!(
                store.get(SESSION_KEY).await.unwrap(),
                Some(phase_a.clone()),
                "…and the fence never moved either",
            );
        });

        // The control: an ordinary advancing cursor over the same list and the same budget
        // never repeats a state, so it publishes. Without it this rule could be "no
        // multi-batch publication at all" and the test above would not notice.
        let store = mem_store();
        let mut ok = publication(&prior, chunks);
        ok.batch_budget = budget;
        block_on(async {
            assert!(
                ok.segment_batches().unwrap().len() >= 3,
                "fixture: the phase must have at least three batches, or there is no state \
                 for the cycling contribution above to return to",
            );
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            assert_eq!(ok.publish(&store).await.unwrap(), CommitOutcome::Committed);
        });
    }

    /// **Nor does the trajectory start at the cursor: a resumed publication may not return
    /// the fence to a state its own durable prefix moved through.**
    ///
    /// A recovery resumes at `resume_from = N`, so the batches it *commits* are
    /// `planned[N..]` — and in the worst case there are none at all, because phase 1 finished
    /// before the crash. A cycle rule charged only against those batches is charged against a
    /// trajectory that begins mid-history: the fence has already been at every state the
    /// prefix's batches carried, and a flip that restores one of them re-arms the rollback
    /// keyed on it exactly as a within-attempt cycle would. Those states are re-derived from
    /// the same plan and the same hook ([`SegmentedPublication::prefix_contributions`]).
    ///
    /// Both shapes are asserted, because they fail for different reasons:
    ///
    /// * a resume with **nothing left to write** (`resume_from == plan.len()`), where the
    ///   attempt's own batch list is empty and the prefix is the only history there is; and
    /// * a resume with **some** batches left, where the returned state is one the prefix held
    ///   rather than one of this attempt's.
    ///
    /// **The reconstruction reports the cursor each prefix batch LEFT, not the one it started
    /// from — and the difference is the state the store is actually in.**
    ///
    /// The trajectory the cycle rule is charged against is only as good as the states it
    /// reconstructs, and the one state a resumed attempt is *certain* the fence has held is
    /// `written == resume_from`: that is where the crashed attempt left it, and it is what the
    /// store holds right now. A reconstruction that reported each batch's *starting* cursor
    /// instead would produce `0..N-1` — plausible, one short, and missing exactly that state,
    /// so a flip that walked the fence back to it would pass the cycle rule and re-arm the
    /// rollback keyed there at the instant the root goes live.
    ///
    /// Asserted on the derivation itself rather than only through a cycle, because a cycle test
    /// cannot isolate it: this attempt's own first batch pins `written == resume_from` too, so
    /// the trajectory usually contains it twice and the shortfall hides.
    #[test]
    fn the_prefix_reconstruction_reports_the_cursor_each_batch_left() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(5_000);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        let mut pubn = publication(&prior, chunks);
        pubn.segment_batch = &contribute;
        pubn.batch_budget = Some(SEGMENT_TARGET_BYTES + 1_000);
        let planned = pubn.plan().unwrap().len();
        assert!(
            planned > 2,
            "fixture: the prefix must span several segments"
        );
        pubn.resume_from = planned as u32;

        let states: Vec<Bytes> = pubn
            .durable_prefix_contributions()
            .iter()
            .flat_map(|batch| {
                batch
                    .puts
                    .iter()
                    .filter(|(key, _)| key.as_slice() == SESSION_KEY)
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            states,
            (1..=planned as u32)
                .map(|written| session_value(7, written))
                .collect::<Vec<_>>(),
            "the prefix's batches LEFT the cursor at 1..=resume_from; reporting the cursors \
             they each started from would drop the state the store is in right now, which is \
             the one a flip must not walk the fence back to",
        );
    }

    /// The control is the honest cursor contribution over the same resumed publication, which
    /// must still publish — otherwise this rule would read as "no resumed publication at all".
    #[test]
    fn a_resumed_publication_may_not_return_its_fence_to_a_state_the_prefix_held() {
        let prior = InodeRecord::new_empty();
        // Enough chunks for several segments at the REAL target, so the prefix a cursor can
        // claim really does span more than one state.
        let chunks = many_chunks(5_000);
        // One segment record per batch, so the prefix holds several distinct states.
        let budget = Some(SEGMENT_TARGET_BYTES + 1_000);
        // The prefix's contribution: the ordinary cursor CAS, whose state at batch `k` is
        // `written=k`.
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);

        let mut probe = publication(&prior, chunks.clone());
        probe.segment_batch = &contribute;
        probe.batch_budget = budget;
        let planned = probe.plan().unwrap().len();
        let batches = probe.segment_batches().unwrap().len();
        assert!(
            planned > 2 && batches > 2,
            "fixture: the prefix must hold more than one state ({planned} segments, \
             {batches} batches)",
        );

        for (what, resume_from, restore) in [
            (
                "a resume with nothing left to write",
                planned as u32,
                session_value(7, 0),
            ),
            (
                "a resume with batches left, returning to a prefix state",
                planned as u32 - 1,
                session_value(7, 0),
            ),
            (
                // The state a coarser split might never have had as a BOUNDARY: the
                // reconstruction asks the hook for every cursor position the prefix passes,
                // so a state the durable prefix moved through is caught whatever batch
                // boundaries the earlier attempt chose.
                "a resume returning to a cursor position inside the prefix",
                planned as u32,
                session_value(7, 2),
            ),
        ] {
            let mut pubn = publication(&prior, chunks.clone());
            pubn.segment_batch = &contribute;
            pubn.batch_budget = budget;
            pubn.resume_from = resume_from;
            // A flip that pins what the prefix left and puts back the state the phase
            // STARTED at: every per-batch rule is satisfied — it moved the record.
            pubn.flip = WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, resume_from))
                .put(SESSION_KEY.to_vec(), restore.clone());
            let err = pubn
                .flip_batch()
                .expect_err("a flip may not restore a state the durable prefix moved through");
            assert!(
                matches!(err, ChunkMapError::FenceCycled { phase: "flip", .. }),
                "{what}: {err}",
            );
            // …and the whole publication refuses, with nothing durable, over a store that
            // holds the prefix the cursor claims.
            let store = mem_store();
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                put(&store, SESSION_KEY.to_vec(), session_value(7, resume_from)).await;
                let durable = publication(&prior, chunks.clone());
                for (segment, record) in &durable.plan().unwrap()[..resume_from as usize] {
                    put(&store, seg_key(&group(7), segment.index), encode(record)).await;
                }
                let live = store.get(&inode_key(1)).await.unwrap();
                assert!(
                    matches!(
                        pubn.publish(&store)
                            .await
                            .expect_err("the publication must refuse")
                            .downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::FenceCycled { .. }),
                    ),
                    "{what}: the refusal must survive the whole publication path",
                );
                assert_eq!(
                    store.get(&inode_key(1)).await.unwrap(),
                    live,
                    "{what}: …and the prior generation is still the live root",
                );
            });
        }

        // The control: the honest terminal transition over the same resumed publication,
        // which must publish.
        let store = mem_store();
        let mut ok = publication(&prior, chunks.clone());
        ok.segment_batch = &contribute;
        ok.batch_budget = budget;
        ok.resume_from = planned as u32;
        ok.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, planned as u32))
            .put(SESSION_KEY.to_vec(), completed_session_value(7));
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, planned as u32),
            )
            .await;
            for (segment, record) in &ok.plan().unwrap() {
                put(&store, seg_key(&group(7), segment.index), encode(record)).await;
            }
            assert_eq!(ok.flip(&store).await.unwrap(), CommitOutcome::Committed);
        });
    }

    /// **A publication over the last version there is refuses rather than wrapping.**
    ///
    /// `version` is the monotonic per-inode counter every compare-and-swap in this module
    /// orders writers by, and the prior it is computed from is a record read out of the
    /// store — so its value is whatever the bytes said. An unchecked `+ 1` panics at
    /// `u64::MAX` with overflow checks on and wraps to **zero** without them, which is the
    /// worse outcome: a published generation that looks older than the one it replaced.
    #[test]
    fn a_publication_over_the_last_version_refuses_rather_than_wrapping() {
        let mut prior = InodeRecord::new_empty();
        prior.version = u64::MAX;
        let pubn = publication(&prior, many_chunks(4));
        assert_eq!(
            pubn.root().unwrap_err(),
            ChunkMapError::RootVersionOverflow { version: u64::MAX },
        );
        // …and the refusal reaches every entry point, before anything is durable.
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            assert!(matches!(
                pubn.publish(&store)
                    .await
                    .expect_err("no version to publish")
                    .downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::RootVersionOverflow { .. }),
            ));
            assert_eq!(
                store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                Vec::new(),
                "…with nothing written: it is a deterministic, zero-I/O refusal",
            );
        });
        // The boundary is exact: one below still publishes.
        let mut last = InodeRecord::new_empty();
        last.version = u64::MAX - 1;
        assert_eq!(
            publication(&last, many_chunks(4)).root().unwrap().version,
            u64::MAX,
        );
    }

    /// **The cycle does not stop at the phase boundary: a flip that walks the fence back
    /// into the segment phase is refused.**
    ///
    /// The per-batch rule ([`check_fence_transitioned`]) is satisfied by a flip that pins `B`
    /// and puts `A` — it moved the record. The within-phase cycle rule is satisfied too,
    /// because the flip is not in the phase. And the result is precisely the hazard the fence
    /// exists to prevent: the rollback that reclaims a crashed attempt is fenced on the value
    /// the segment phase started at (`require(mpu == A)` + the `seg:` deletes), so once the
    /// flip has put the fence back to `A` that rollback commits **after** the publication
    /// instant and deletes the segments the now-live root names.
    ///
    /// Two halves, and the second is why the first is not pedantry:
    ///
    /// * the committer **refuses** the publication, with zero I/O — no segment durable, the
    ///   fence untouched, the prior root still live; and
    /// * the same batches, committed **by hand** (the committer's rule bypassed, which is the
    ///   pre-fix behaviour), publish a root and then let the `A`-fenced rollback delete its
    ///   segments: the object resolves to [`ChunkMapError::SegmentAbsent`] for ever. That leg
    ///   is the fixture carrying the fault rather than describing it.
    #[test]
    fn a_flip_that_walks_the_fence_back_into_the_segment_phase_is_refused() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(40);
        let state_a = Bytes::from_static(b"Completing@7|state=a");
        let state_b = Bytes::from_static(b"Completing@7|state=b");
        // One batch for the whole phase, so the ONLY cycle available crosses into the flip.
        let advance = |_: &SegmentBatchInfo| {
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), state_a.clone())
                .put(SESSION_KEY.to_vec(), state_b.clone())
        };
        // The flip pins what the phase left and takes the fence BACK to where it started.
        let walk_back = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), state_b.clone())
            .put(SESSION_KEY.to_vec(), state_a.clone());

        let store = mem_store();
        let mut pubn = publication(&prior, chunks.clone());
        pubn.segment_batch = &advance;
        pubn.flip = walk_back.clone();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, SESSION_KEY.to_vec(), state_a.clone()).await;
            assert_eq!(
                pubn.segment_batches().unwrap().len(),
                1,
                "fixture: one segment batch, so no cycle exists WITHIN the phase — the rule \
                 under test is the one that spans the phase and the flip",
            );

            let refusal = ChunkMapError::FenceCycled {
                fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
                phase: "flip",
                batch: 1,
            };
            assert_eq!(
                pubn.flip_batch().unwrap_err(),
                refusal,
                "the flip must be charged against the phase in front of it, and the refusal \
                 must name the flip's position in the publication (batch 1, after the one \
                 segment batch)",
            );
            assert_eq!(
                pubn.publish(&store)
                    .await
                    .expect_err("a publication whose flip walks the fence back must refuse")
                    .downcast_ref::<ChunkMapError>(),
                Some(&refusal),
            );
            assert_eq!(
                store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                Vec::new(),
                "…decided with zero I/O: not one segment of the refused publication is durable",
            );
            assert_eq!(
                store.get(SESSION_KEY).await.unwrap(),
                Some(state_a.clone()),
                "…and the fence never moved",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and the prior generation is still the live root",
            );
        });

        // The hazard, driven by hand on a fresh store: exactly these batches, committed
        // without the rule, and then the rollback the returned fence re-arms.
        let store = mem_store();
        let pubn = {
            let mut p = publication(&prior, chunks);
            p.segment_batch = &advance;
            p.flip = walk_back;
            p
        };
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, SESSION_KEY.to_vec(), state_a.clone()).await;
            let planned = pubn.plan().unwrap();
            let root = pubn.root().unwrap();

            // Phase 1, by hand: the segment records plus the contribution that moves A → B.
            let mut phase = WriteBatch::new()
                .require(SESSION_KEY.to_vec(), state_a.clone())
                .put(SESSION_KEY.to_vec(), state_b.clone());
            for (segment, record) in &planned {
                phase = phase.put(seg_key(&group(7), segment.index), encode(record));
            }
            assert_eq!(store.commit(phase).await.unwrap(), CommitOutcome::Committed);

            // The flip, by hand: the root CAS plus the contribution that moves B → A.
            let flip = WriteBatch::new()
                .require(inode_key(1), encode(&prior))
                .put(inode_key(1), encode(&root))
                .require(SESSION_KEY.to_vec(), state_b.clone())
                .put(SESSION_KEY.to_vec(), state_a.clone());
            assert_eq!(store.commit(flip).await.unwrap(), CommitOutcome::Committed);
            assert!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .is_ok(),
                "fixture: the published object resolves at the publication instant",
            );

            // The rollback of the crashed attempt, fenced on the state the phase STARTED at
            // — which the flip has just restored, so its precondition holds again.
            let mut rollback = WriteBatch::new().require(SESSION_KEY.to_vec(), state_a.clone());
            for (segment, _) in &planned {
                rollback = rollback.delete(seg_key(&group(7), segment.index));
            }
            assert_eq!(
                store.commit(rollback).await.unwrap(),
                CommitOutcome::Committed,
                "the returned fence re-arms the rollback: this is what the refusal above \
                 prevents",
            );
            let stranded = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .expect_err("the live root now names segments that are gone");
            assert!(
                matches!(
                    stranded.downcast_ref::<ChunkMapError>(),
                    Some(ChunkMapError::SegmentAbsent { .. })
                ),
                "…and the object is unresolvable for ever, by the live root: {stranded}",
            );
        });
    }

    /// **A publication may not mint a chunk no pass in the system could ever drain.**
    ///
    /// An **empty** `placement` is a well-formed committed value everywhere else — the
    /// pre-M3 identity fallback (ADR-0040 decision 3) — and the population is drained by
    /// the custodian's backfill pass, which materialises the explicit vector with an
    /// **inode** compare-and-swap. A segmented map's `ChunkRef`s are not in the inode: they
    /// live in `seg:` records, which that pass cannot rewrite and no other pass is looking
    /// at (`crates/custodian/src/backfill.rs:136-173`). So a published segmented chunk with
    /// an empty placement is a record with no drain — and the pass, which states that
    /// combination is structurally impossible, raises on it and **fails every future
    /// backfill run over the whole store** (`SegmentedPlacementUnfillable`,
    /// `backfill.rs:220-227`).
    ///
    /// The premise is made true here, at the only writer that could break it, and with the
    /// other deterministic refusals: before the first `seg:` record is durable. (A segment
    /// repoint is already strict — its replacement vector must be exactly
    /// `fragment_count()` long, [`repoint_placement`] — so this closes the last door.)
    ///
    /// Asserted as a **positive property of what the store ends up holding**, not only as a
    /// refusal: the same fixture then publishes the admissible list and every chunk of every
    /// `seg:` record it wrote carries an explicit placement.
    #[test]
    fn a_segmented_publication_refuses_a_placement_no_pass_could_ever_fill() {
        let prior = InodeRecord::new_empty();
        let admissible = many_chunks(40);
        let mut undrainable = admissible.clone();
        undrainable[7].placement = Vec::new();
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;

            let refused = publication(&prior, undrainable);
            assert_eq!(
                refused
                    .publish(&store)
                    .await
                    .expect_err("a chunk with an empty placement must be refused")
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::WritePlacementEmpty {
                    position: 7,
                    expected: 1,
                }),
                "the refusal must name the chunk's position and the length its scheme owes",
            );
            assert_eq!(
                store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                Vec::new(),
                "…and nothing is durable: the check is part of the plan, not of the write",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and the root still names the prior generation",
            );

            // The property itself: over a publication that IS admitted, every chunk the
            // committer made durable carries the explicit vector the backfill pass would
            // otherwise have had to supply and cannot.
            assert_eq!(
                publication(&prior, admissible)
                    .publish(&store)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
            let rows = store.scan(&seg_range_prefix(&group(7))).await.unwrap();
            assert!(
                !rows.is_empty(),
                "fixture: the admitted publication must have written segment records",
            );
            for (key, value) in rows {
                for (position, chunk) in decode_segment_record(&key, &value)
                    .unwrap()
                    .chunks()
                    .iter()
                    .enumerate()
                {
                    assert!(
                        !chunk.placement.is_empty(),
                        "chunk {position} of {}: no `seg:` record this committer writes may \
                         carry a placement only an inode CAS could fill",
                        String::from_utf8_lossy(&key),
                    );
                }
            }
        });
    }

    /// Seed a real durable prefix: commit `pubn`'s **first** segment-write batch and
    /// answer how many `seg:` records that left behind — the `segments_written` cursor a
    /// crashed completer recovers. Shared by the resume tests so both drive the same
    /// production split and the same commit loop.
    async fn durable_prefix(store: &impl MetadataStore, pubn: &SegmentedPublication<'_>) -> u32 {
        let batches = pubn.segment_batches().unwrap();
        assert!(batches.len() > 1, "fixture: more than one batch");
        assert_eq!(
            SegmentedPublication::commit_batches(store, batches[..1].to_vec())
                .await
                .unwrap(),
            CommitOutcome::Committed,
        );
        store
            .scan(&seg_range_prefix(&pubn.group))
            .await
            .unwrap()
            .len() as u32
    }

    /// **A resumed publication verifies the WHOLE durable prefix it is trusting — every
    /// record of it, not its last one.**
    ///
    /// `resume_from` is a claim about records this attempt did not write. Taken on trust,
    /// an attempt whose chunk list differs anywhere inside that prefix writes only the
    /// tail, flips a root computed from **its** list, and publishes an object mapped by
    /// two different lists at once. There are two shapes of that, and only the first is
    /// loud:
    ///
    /// * the divergence moves the **byte extents** (a chunk length changed), so every
    ///   later segment's offsets disagree with the root and every read fails
    ///   `SegmentBoundsMismatch` — silent at publication, terminal at read;
    /// * the divergence keeps every extent (a chunk **id** or **placement** changed at the
    ///   same encoded width), so the root, the keys and every byte span are identical and
    ///   the published map resolves **cleanly** — as a hybrid of attempt 1's prefix and
    ///   attempt 2's tail, serving the wrong bytes for ever with nothing to notice it.
    ///
    /// A check of the last claimed-durable record alone catches the first and misses the
    /// second, which is why the fixture leaves **more than one** segment durable and puts
    /// every divergence in segment 0: under a last-record check the two same-extent cases
    /// publish.
    ///
    /// The control is in the same store: after a refusal nothing has moved, so the attempt
    /// that resumes with the **right** list still publishes normally.
    #[test]
    fn a_resumed_publication_refuses_a_durable_prefix_that_is_not_its_own() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(3_000);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        // Two segments per batch, so the durable prefix is longer than one record.
        let budget = Some(2 * SEGMENT_TARGET_BYTES + 2_000);

        // Three divergences from the published list, each entirely inside segment 0 and
        // each invisible to a `segments_written` cursor.
        let mut shorter = chunks.clone();
        shorter[0].len = 9;
        let mut transposed = chunks.clone();
        transposed[0].id = chunks[8].id;
        transposed[8].id = chunks[0].id;
        let mut moved = chunks.clone();
        moved[0].placement = vec![2];

        for (what, other) in [
            ("two chunk ids transposed", transposed),
            ("one chunk length changed", shorter),
            ("one chunk's placement changed", moved),
        ] {
            let store = mem_store();
            let mut first = publication(&prior, chunks.clone());
            first.segment_batch = &contribute;
            first.batch_budget = budget;
            let planned = first.plan().unwrap();
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;

                // Attempt 1 dies after its first batch, leaving a real durable prefix.
                let durable = durable_prefix(&store, &first).await;
                assert!(
                    durable >= 2 && (durable as usize) < planned.len(),
                    "fixture: the durable prefix must hold MORE than one segment, or a \
                     last-record check would be indistinguishable from a whole-prefix one",
                );

                // Attempt 2 resumes at that cursor with the OTHER list.
                let mut wrong = publication(&prior, other);
                wrong.segment_batch = &contribute;
                wrong.batch_budget = budget;
                wrong.resume_from = durable;
                let err = wrong.publish(&store).await.expect_err(&format!(
                    "{what}: a resume onto another list's prefix must refuse"
                ));
                assert_eq!(
                    err.downcast_ref::<ChunkMapError>(),
                    Some(&ChunkMapError::ResumePrefixMismatch {
                        nonce: NONCE.to_string(),
                        epoch: 7,
                        index: 0,
                    }),
                    "{what}: the refusal must name the FIRST index that disagrees, which \
                     is inside the prefix and not at its end",
                );
                assert_eq!(
                    store
                        .scan(&seg_range_prefix(&group(7)))
                        .await
                        .unwrap()
                        .len() as u32,
                    durable,
                    "{what}: the refusal wrote nothing — the durable prefix is exactly \
                     what it was",
                );
                assert_eq!(
                    store.get(&inode_key(1)).await.unwrap(),
                    Some(encode(&prior)),
                    "{what}: …and no root over a prefix that is not its own was published",
                );
            });
        }

        // A cursor naming segments that are NOT there is the same class of claim — and
        // the control, which proves the refusals above are the prefix check rather than
        // the resume path being broken.
        let store = mem_store();
        let mut first = publication(&prior, chunks.clone());
        first.segment_batch = &contribute;
        first.batch_budget = budget;
        let planned = first.plan().unwrap();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let durable = durable_prefix(&store, &first).await;

            let mut absent = publication(&prior, chunks.clone());
            absent.segment_batch = &contribute;
            absent.batch_budget = budget;
            absent.resume_from = planned.len() as u32;
            assert_eq!(
                absent
                    .publish(&store)
                    .await
                    .expect_err("a cursor past what is durable must refuse")
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::ResumePrefixAbsent {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: durable,
                }),
                "the refusal must name the first index the store does not have",
            );

            let mut right = publication(&prior, chunks.clone());
            right.segment_batch = &contribute;
            right.batch_budget = budget;
            right.resume_from = durable;
            assert_eq!(
                right.publish(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap()
                    .chunks(),
                Some(chunks.as_slice()),
            );
        });
    }

    /// **A fresh attempt (`resume_from == 0`) may not overwrite records it did not write.**
    ///
    /// A cursor of zero says "I have written nothing", not "the range holds nothing", and a
    /// check that only compared the *claimed prefix* compared nothing at all. A second
    /// completer at the same `(nonce, epoch)` carrying a different plan would then walk the
    /// range writing its own bytes over the first attempt's segment records — one batch at a
    /// time, leaving a **hybrid** range half of each plan — and would discover its mistake
    /// only at the root compare-and-swap, after the damage. The check therefore covers every
    /// planned index, present or claimed.
    ///
    /// The fixture is the reachable shape of that race: an attempt that wrote its segments
    /// and stopped before the flip (a crash, a lost fence), so the root is still the flat
    /// prior. A second completer over a *published* segmented generation is refused earlier
    /// and for a different reason — the prior would be retired with nothing to retire it
    /// ([`a_publication_over_a_segmented_generation_refuses_rather_than_stranding_it`]).
    #[test]
    fn a_fresh_attempt_refuses_to_overwrite_another_plans_durable_records() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(1_500);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        // Same length, same byte extents, one chunk's identity changed: invisible to a
        // cursor, invisible to the byte tiling, and a different object's bytes.
        let mut other = chunks.clone();
        other[0].id = 0xFACE;

        let store = mem_store();
        let mut published = publication(&prior, chunks.clone());
        published.segment_batch = &contribute;
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            assert_eq!(
                published.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed,
            );
            let live = store.scan(&seg_range_prefix(&group(7))).await.unwrap();
            assert!(!live.is_empty(), "fixture: the first attempt is durable");
            let root_bytes = store.get(&inode_key(1)).await.unwrap().unwrap();

            // A second completer of the same generation, fresh cursor, different plan.
            let mut second = publication(&prior, other);
            second.segment_batch = &contribute;
            second.resume_from = 0;
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            assert_eq!(
                second
                    .publish(&store)
                    .await
                    .expect_err("a fresh attempt over another plan's records must refuse")
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::ResumePrefixMismatch {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 0,
                }),
                "the refusal must name the first durable record this plan disagrees with",
            );
            assert_eq!(
                store.scan(&seg_range_prefix(&group(7))).await.unwrap(),
                live,
                "…and every segment record the first attempt wrote is byte-unchanged: the \
                 refusal has to land before the first write, not at the root CAS",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(root_bytes),
                "…and the root still names the generation it named",
            );
        });
    }

    /// **Publishing over a segmented generation would retire it, and nothing here retires
    /// one — so the publication refuses instead.**
    ///
    /// The flip replaces the root, and the root is the *only* thing that names a
    /// generation's `seg:` records: once it is gone, that generation's segment records are
    /// unreachable for every consumer (the resolver reads a group's range from the root, so
    /// GC's reference build, restore, rebalance and reconstruction never see them again),
    /// while every fragment they alone name stays on disk, referenced by nothing and
    /// reclaimed by nobody. Retiring a segmented generation is the staged
    /// `retire:bytes:{generation}` obligation of `0016` decision 4 / 7(f) — its fan-out is
    /// drained in byte-budgeted batches, never inline (`0016:668`) — and it belongs to the
    /// session slice (#636). The flat committers already refuse the same shape
    /// ([`commit_chunk_map`], [`commit_chunk_map_superseding`],
    /// [`a_whole_map_recommit_refuses_a_segmented_prior_rather_than_stranding_it`]); this is
    /// the third door into it, and the only one that could have published a *new* segmented
    /// generation over one.
    ///
    /// Refused with the other zero-I/O refusals, so nothing of the new generation is durable
    /// either: a publication that wrote its segments and then discovered it may not flip
    /// would leave a whole generation's records for the rollback to reclaim.
    #[test]
    fn a_publication_over_a_segmented_generation_refuses_rather_than_stranding_it() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let first = many_chunks(4);
        let second = many_chunks(6);
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let a = publication(&prior, first);
            assert_eq!(a.publish(&store).await.unwrap(), CommitOutcome::Committed);
            let root_a: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert!(
                root_a.chunk_map.is_segmented(),
                "fixture: the prior generation is segmented",
            );
            let segments_a = store.scan(&seg_range_prefix(&a.group)).await.unwrap();

            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            let mut b = publication(&root_a, second);
            b.group = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 9).unwrap();
            b.flip = flip_fence(&b);
            let err = b
                .publish(&store)
                .await
                .expect_err("publishing over a segmented generation retires it");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::SegmentedRetirementUnsupported {
                    operation: "publishing a segmented map over a segmented generation",
                }),
                "the refusal names the obligation the caller would have had to carry: {err}",
            );

            // Nothing of either generation moved: the prior's segments are still named by
            // the live root, and the refused publication left no row of its own.
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&root_a)),
                "the live root is untouched",
            );
            assert_eq!(
                store.scan(&seg_range_prefix(&a.group)).await.unwrap(),
                segments_a,
                "…the retired-to-be generation's segments are untouched…",
            );
            assert!(
                store
                    .scan(&seg_range_prefix(&b.group))
                    .await
                    .unwrap()
                    .is_empty(),
                "…and the refused publication wrote none of its own",
            );

            // The same refusal reaches the two batch builders and the root itself, so a
            // caller that drove the phases by hand cannot slip past it either.
            assert!(b.root().is_err(), "the root cannot even be computed");
            assert!(b.flip(&store).await.is_err(), "nor can a bare flip publish");
        });
    }

    /// **A flip may not publish for a publication whose own segment phase this committer
    /// would refuse to write.**
    ///
    /// [`SegmentedPublication::flip`] is the recovery entry point: a completer that lost its
    /// phase-1 acknowledgement re-derives the publication and flips what is already durable.
    /// It is therefore the one route to the publication instant that does not assemble the
    /// segment phase for its own sake — and the flip's fence rules are charged *against*
    /// that phase (the fence may not return to a state the phase held,
    /// [`ChunkMapError::FenceCycled`]). Swallowing the phase's refusal would charge the flip
    /// against an empty sequence and publish a live root for a generation whose segment
    /// writes are unfenced — the exact configuration [`ChunkMapError::Unfenced`] exists to
    /// keep out, arrived at through the one door that never writes a segment.
    ///
    /// The legitimate recovery is unaffected: a cursor at the end of the plan leaves nothing
    /// to write, the phase assembles to no batches at all, and the flip proceeds.
    #[test]
    fn a_flip_refuses_a_publication_whose_segment_phase_is_unfenced() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(4);
        // A phase-1 contribution with no fence at all — a caller that pins its session only
        // at the flip. The flip's own contribution is fenced and transitions, so nothing
        // *the flip carries* is wrong.
        let unfenced = |_: &SegmentBatchInfo| WriteBatch::new();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            // The range is durable: a properly fenced twin writes it, so the flip's
            // completeness check has nothing to complain about and the ONLY thing standing
            // between this publication and a live root is the phase's own refusal.
            let fenced = publication(&prior, chunks.clone());
            assert_eq!(
                fenced.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed,
            );

            let mut pubn = publication(&prior, chunks.clone());
            pubn.segment_batch = &unfenced;
            let err = pubn
                .flip(&store)
                .await
                .expect_err("a flip may not publish over an unfenced phase");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::Unfenced {
                    phase: PHASE_SEGMENTS,
                    batch: 0,
                    fence: rendered_key(SESSION_KEY),
                }),
                "the refusal names the phase that is unfenced, not the flip: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and nothing was published",
            );

            // …while the recovery this entry point exists for still publishes: the cursor
            // says the phase is complete, so there is no batch left to be unfenced.
            let mut recovery = publication(&prior, chunks);
            recovery.segment_batch = &unfenced;
            recovery.resume_from = recovery.plan().unwrap().len() as u32;
            assert_eq!(
                recovery.flip(&store).await.unwrap(),
                CommitOutcome::Committed,
                "a completed phase leaves nothing to refuse",
            );
        });
    }

    /// A store that lets **one** concurrent writer in, in the window between a
    /// publication's range read and the first segment batch it commits — the window a
    /// recovery cannot see and cannot avoid.
    struct RacingWriter {
        inner: RedbMetadataStore,
        injected: Mutex<Option<WriteBatch>>,
    }

    #[async_trait]
    impl MetadataStore for RacingWriter {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            self.inner.scan(prefix).await
        }

        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<wyrd_traits::ScanPage> {
            self.inner.scan_page(prefix, after, limit).await
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            // The race lands *first*, so the caller's batch meets a store that moved under
            // it — which is the only ordering that can tell a compare-and-swap from a put.
            let racing = self.injected.lock().unwrap().take();
            if let Some(racing) = racing {
                assert_eq!(
                    self.inner.commit(racing).await?,
                    CommitOutcome::Committed,
                    "fixture: the racing repoint itself must land",
                );
            }
            self.inner.commit(batch).await
        }
    }

    /// **A publication never writes a `seg:` row it did not see absent — because by the
    /// time it writes, that row may belong to a LIVE generation.**
    ///
    /// The shape is an ambiguous flip. A publication commits its segments, commits the
    /// flip, and the store's answer is lost (a `CommitUnknownResult`, the one outcome the
    /// trait says a caller must resolve by reading, `crates/traits/src/lib.rs:1170-1177`).
    /// The completer retries the whole publication: same nonce, same epoch, same frozen part
    /// set, so it re-derives the *same* plan and its range read finds every row already
    /// durable and byte-identical. Meanwhile the flip **did** land: the object is live, and
    /// maintenance has moved a fragment — reconstruction or rebalance repointing a chunk
    /// with [`repoint_chunk`]'s `require(seg == prior)` CAS, orphaning the placement it
    /// vacated.
    ///
    /// A phase that re-puts its rows blind would then **restore the stale placement** into a
    /// live object's map: the map would name fragments that are already orphaned and, once
    /// the grace window passes, reclaimed — and the flip that follows loses its root CAS, so
    /// the publication reports a plain `Conflict` while having silently corrupted the
    /// generation it was trying to publish. Under a compare-and-swap it cannot: the row the
    /// repoint moved is no longer the row the publication witnessed, so the batch loses and
    /// the repoint stands.
    #[test]
    fn a_segment_phase_never_overwrites_a_row_a_live_repoint_moved() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(4);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        let store = RacingWriter {
            inner: mem_store(),
            injected: Mutex::new(None),
        };
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut first = publication(&prior, chunks.clone());
            first.segment_batch = &contribute;
            assert_eq!(
                first.publish(&store).await.unwrap(),
                CommitOutcome::Committed,
                "fixture: generation 1 is published and live",
            );
            let live: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();

            // The maintenance move, built by the production repoint: chunk 0 of the live
            // generation goes to a D-server it was not on.
            let homes = resolve_chunk_homes(&store, &inode_key(1), &live)
                .await
                .unwrap()
                .expect("the live generation resolves");
            let moved = repoint_chunk(1, &live, &homes[0].home, vec![9]).unwrap();
            let (seg, _) = moved.puts[0].clone();
            let repointed = moved
                .puts
                .iter()
                .find(|(key, _)| *key == seg)
                .map(|(_, value)| value.clone())
                .expect("the repoint writes the segment record");

            // The completer retries: the same generation, a cursor of zero (its flip's
            // answer was lost, so it knows nothing), the session re-opened at the state its
            // first batch fences against.
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            *store.injected.lock().unwrap() = Some(moved);
            let mut retry = publication(&prior, chunks.clone());
            retry.segment_batch = &contribute;
            let outcome = retry.publish(&store).await.unwrap();

            assert_eq!(
                store.get(&seg).await.unwrap(),
                Some(repointed),
                "the live generation's repointed segment record is exactly what the \
                 repoint left: a recovery may not restore the placement it vacated",
            );
            assert_eq!(
                outcome,
                CommitOutcome::Conflict,
                "…and the recovery is told it lost, rather than reporting a success it \
                 did not have",
            );
            assert!(
                store.injected.lock().unwrap().is_none(),
                "fixture: the racing repoint really was injected",
            );
        });
    }

    /// **What the store already holds may change a row's precondition; it may never change
    /// the split.**
    ///
    /// The publication assembles its phase twice — before its range read, where every
    /// zero-I/O refusal is decided with nothing durable, and after it, where each row learns
    /// whether it is being created or pinned — and the flip is charged against the *first*
    /// (the fence may not cycle across the phase and the flip). If the witness could move a
    /// batch boundary, those two would be different phases: a different [`SegmentBatchInfo`]
    /// sequence, so a different fence trajectory, and a flip validated against a sequence the
    /// store never saw. The reserve loop measures the unwitnessed shape for exactly that
    /// reason, and this is the assertion that keeps it there.
    #[test]
    fn the_durable_witness_never_moves_the_split() {
        let prior = InodeRecord::new_empty();
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        let mut pubn = publication(&prior, many_chunks(40));
        pubn.segment_batch = &contribute;
        let planned = pubn.plan_with(SMALL_SEGMENT, MAX_ROOT_SEGMENTS).unwrap();
        let fresh = pubn
            .staged_batches(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                SMALL_BUDGET,
                MAX_BATCH_OPS,
            )
            .unwrap();
        assert!(
            fresh.len() > 1,
            "fixture: more than one batch, or there is no boundary to move",
        );

        // Which **distinct** `seg:` keys a batch touches, in the order it first names them —
        // whichever shape they take (a created row appears as a precondition *and* a put, a
        // pinned one only as a precondition).
        let prefix = seg_range_prefix(&group(7));
        let rows = |batch: &WriteBatch| -> Vec<Vec<u8>> {
            let mut seen: Vec<Vec<u8>> = Vec::new();
            for key in batch
                .preconditions
                .iter()
                .map(|p| p.key.clone())
                .chain(batch.puts.iter().map(|(key, _)| key.clone()))
                .filter(|key| key.starts_with(&prefix))
            {
                if !seen.contains(&key) {
                    seen.push(key);
                }
            }
            seen
        };

        // Every witness that can arise: nothing durable, everything durable, and the
        // interleaved one a resumed attempt over a partially-written range produces.
        let all: DurableSegments =
            DurableSegments(planned.iter().map(|(segment, _)| segment.index).collect());
        let alternate: DurableSegments = DurableSegments(
            planned
                .iter()
                .map(|(segment, _)| segment.index)
                .filter(|index| index % 2 == 0)
                .collect(),
        );
        for witness in [&all, &alternate] {
            let witnessed = pubn
                .staged_batches_over(
                    SMALL_SEGMENT,
                    MAX_ROOT_SEGMENTS,
                    SMALL_BUDGET,
                    MAX_BATCH_OPS,
                    witness,
                )
                .unwrap();
            assert_eq!(
                witnessed.len(),
                fresh.len(),
                "the witness may not change how many batches the phase has",
            );
            for (n, (witnessed, fresh)) in witnessed.iter().zip(&fresh).enumerate() {
                assert_eq!(
                    rows(witnessed),
                    rows(fresh),
                    "batch {n} carries the same rows, in the same order",
                );
                assert!(
                    batch_bytes(witnessed) <= batch_bytes(fresh)
                        && batch_ops(witnessed) <= batch_ops(fresh),
                    "batch {n}: the witnessed shape is charged no more than the split \
                     reserved for it",
                );
                // The caller's contribution — the fence and the cursor — is identical, which
                // is the property the flip is charged against.
                let contribution = |batch: &WriteBatch| -> Vec<(Vec<u8>, Option<Bytes>)> {
                    batch
                        .preconditions
                        .iter()
                        .map(|p| (p.key.clone(), p.expected.clone()))
                        .filter(|(key, _)| !key.starts_with(&prefix))
                        .collect()
                };
                assert_eq!(contribution(witnessed), contribution(fresh));
            }
        }

        // …and the shapes themselves: one precondition per row either way, a put only for a
        // row that was not witnessed.
        let witnessed = pubn
            .staged_batches_over(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                SMALL_BUDGET,
                MAX_BATCH_OPS,
                &alternate,
            )
            .unwrap();
        for (segment, record) in &planned {
            let key = seg_key(&group(7), segment.index);
            let pinned = witnessed
                .iter()
                .flat_map(|batch| batch.preconditions.iter())
                .filter(|p| p.key == key)
                .collect::<Vec<_>>();
            let put = witnessed
                .iter()
                .flat_map(|batch| batch.puts.iter())
                .filter(|(k, _)| *k == key)
                .count();
            assert_eq!(pinned.len(), 1, "one precondition per row, never none");
            if alternate.holds(segment.index) {
                assert_eq!(pinned[0].expected.as_ref(), Some(&encode(record)));
                assert_eq!(put, 0, "a witnessed row is pinned, not rewritten");
            } else {
                assert_eq!(pinned[0].expected, None, "an absent row is created");
                assert_eq!(put, 1);
            }
        }
    }

    /// **A publication may not leave a record in its own range that its root does not
    /// name — and an earlier, LONGER attempt at the same generation leaves exactly that.**
    ///
    /// Nothing in this committer deletes a `seg:` row, and the resolver fails closed on a
    /// row the root does not name ([`ChunkMapError::SegmentUnknown`]): a root must account
    /// for its whole range or a reader cannot tell a stranger's chunks from its own. So a
    /// republication at the same `(nonce, epoch)` whose plan is **shorter** than what is
    /// already durable would report `Committed` and leave an object no consumer can ever
    /// resolve — every read, GC, restore, rebalance and reconstruction pass over it failing
    /// for ever, from a publication that looked like a success. Silent at publication,
    /// terminal at read, and this time not even the rollback can find it.
    ///
    /// The cursor has nothing to do with it, so neither does the check: the stranded tail
    /// is refused for a **fresh** attempt (`resume_from == 0`) as much as for a resumed
    /// one. And the malformed-key half is the same rule from the other side — a key in this
    /// range that does not parse is a key `read_segments` will refuse for ever, so the
    /// publisher refuses to publish over it rather than dropping it as "not mine".
    #[test]
    fn a_publication_refuses_to_publish_over_a_range_no_resolve_could_read() {
        let prior = InodeRecord::new_empty();
        let long = many_chunks(3_000);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);
        // The shorter list is a **prefix** of the longer one, cut at a segment boundary,
        // so the two attempts' shared segments are byte-identical: what is left is the
        // stranded tail alone, with no bounds mismatch to catch it first.
        let head = publication(&prior, long.clone()).plan().unwrap();
        let head_chunks: usize = head[..2].iter().map(|(_, rec)| rec.chunks().len()).sum();
        let short = long[..head_chunks].to_vec();

        // --- the stranded tail, at a FRESH attempt (the cursor is not what saves it) ----
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut first = publication(&prior, long.clone());
            first.segment_batch = &contribute;
            let planned_long = first.plan().unwrap().len();
            assert_eq!(
                first.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed,
                "fixture: attempt 1 makes its whole segment range durable, then crashes \
                 before its flip",
            );

            let mut second = publication(&prior, short.clone());
            second.segment_batch = &contribute;
            let planned_short = second.plan().unwrap().len();
            assert_eq!(
                planned_short, 2,
                "fixture: the short plan is the first 2 segments"
            );
            assert!(
                planned_short < planned_long,
                "fixture: the second plan must be SHORTER, or there is no tail to strand",
            );
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            let err = second
                .publish(&store)
                .await
                .expect_err("a shorter republication must refuse the tail it cannot name");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::PublicationTailStranded {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: planned_short as u32,
                    planned: planned_short,
                }),
                "the refusal names the first durable index the plan does not reach: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and the root still names the prior generation",
            );
            assert_eq!(
                store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .len(),
                planned_long,
                "…and nothing was written over the range either",
            );

            // The proof that this is the hazard and not a spurious refusal: publish the
            // shorter plan anyway (as the base committer would) and every resolve of the
            // published root fails.
            let flip = second.flip_batch().unwrap();
            // The flip's own fence is the cursor phase 1 would have left; the refusal
            // above means nothing moved it, so the fixture states it explicitly rather
            // than losing this assertion to a `Conflict` that proves nothing.
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, planned_short as u32),
            )
            .await;
            assert_eq!(store.commit(flip).await.unwrap(), CommitOutcome::Committed);
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap_err()
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::SegmentUnknown {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: planned_short as u32,
                }),
                "…which is why the publication had to refuse: the root it would have \
                 published is unresolvable for ever",
            );
        });

        // --- a key in the range that no resolve can parse -----------------------------
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut pubn = publication(&prior, short.clone());
            pubn.segment_batch = &contribute;
            // The group's own range, with an index of the wrong width — the exact key
            // `parse_seg_key` refuses and `read_segments` propagates.
            let mut malformed = seg_range_prefix(&group(7));
            malformed.extend_from_slice(b"7");
            put(&store, malformed.clone(), Bytes::from_static(b"{}")).await;

            let err = pubn
                .publish(&store)
                .await
                .expect_err("a key no resolve can read must refuse the publication");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::SegmentKeyMalformed {
                    key: String::from_utf8_lossy(&malformed).into_owned(),
                }),
                "the refusal names the key: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and nothing was published",
            );
        });

        // --- a row from another group or another epoch, either one on its own ----------
        // Same rule as the resolver's (`read_segments`): the range prefix pins both, so a
        // row that answers under it while parsing to a different nonce **or** a different
        // epoch is a stranger. A check that required the two to differ *together* would
        // admit each of them singly — and publish over a range holding another
        // generation's record.
        let other_nonce = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 7).unwrap();
        for (what, key) in [
            ("another group's nonce", seg_key(&other_nonce, 0)),
            ("another epoch of this group", seg_key(&group(8), 0)),
        ] {
            let store = Impostor {
                inner: mem_store(),
                extra: (
                    key,
                    encode(&SegmentRecord::new(vec![chunk(999, 8, vec![9])], 0).unwrap()),
                ),
            };
            block_on(async {
                put(&store, inode_key(1), encode(&prior)).await;
                open_session(&store).await;
                let mut pubn = publication(&prior, short.clone());
                pubn.segment_batch = &contribute;
                let err = pubn
                    .publish(&store)
                    .await
                    .expect_err("a stranger's row in this range must refuse the publication");
                assert!(
                    matches!(
                        err.downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::SegmentKeyMalformed { .. })
                    ),
                    "{what}: expected a typed refusal, got {err}",
                );
                assert_eq!(
                    store.get(&inode_key(1)).await.unwrap(),
                    Some(encode(&prior)),
                    "{what}: …and nothing was published",
                );
            });
        }
    }

    /// **The flip may not publish a root over a range that does not hold the WHOLE plan.**
    ///
    /// [`SegmentedPublication::flip`] is a public phase-2 entry point — the recovery shape
    /// drives the two phases separately — so nothing but this check stands between a
    /// completer whose phase 1 crashed (or never ran) and a live root naming segments that
    /// are not there. Verifying only `0..resume_from` is not that check: at the ordinary
    /// `resume_from == 0` it reads no planned record at all, so an empty range passes it.
    ///
    /// What ships when it passes is unrecoverable. The root is **live**, so every read, GC,
    /// restore, rebalance and reconstruction pass over the object fails
    /// [`ChunkMapError::SegmentAbsent`] for ever; no rollback will reclaim it, because a
    /// rollback reclaims a *crashed* attempt and this one reported `Committed`; and the
    /// object's prior generation is gone, superseded by the root that replaced it. The
    /// refusal costs one range read the flip already performs.
    ///
    /// Asserted at both ends of the range — nothing durable, and a partial phase 1 — plus
    /// the cursor's arm, because a caller that *claims* the whole plan is durable must not
    /// be able to buy the flip with the claim.
    #[test]
    fn the_flip_refuses_a_range_that_is_not_the_whole_plan() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(1_500);
        let contribute = |info: &SegmentBatchInfo| session_contribution(7, info);

        // --- nothing durable at all: phase 1 never ran --------------------------------
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            let pubn = publication(&prior, chunks.clone());
            let planned = pubn.plan().unwrap().len();
            // The fence at the value phase 1 *would* have left, so the only thing that can
            // refuse this flip is the range: a fixture that left the fence unmoved would
            // pass on a `Conflict` and prove nothing.
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, planned as u32),
            )
            .await;
            let err = pubn
                .flip(&store)
                .await
                .expect_err("a flip over an empty range must refuse");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::PublicationIncomplete {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 0,
                    planned,
                }),
                "the refusal names the first planned segment that is not durable: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and the root still names the prior generation",
            );

            // The proof that this is the hazard and not a spurious refusal: commit the
            // flip batch anyway (as a committer without the check would) and the object is
            // unresolvable — with no prior generation left to fall back to.
            assert_eq!(
                store.commit(pubn.flip_batch().unwrap()).await.unwrap(),
                CommitOutcome::Committed,
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap_err()
                    .downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::SegmentAbsent {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 0,
                }),
                "…which is what the refusal prevents: a live root nothing can resolve",
            );
        });

        // --- a partial phase 1: some batches landed, the rest did not ------------------
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut pubn = publication(&prior, chunks.clone());
            pubn.segment_batch = &contribute;
            // One segment record per commit, so the phase can stop *inside* itself.
            pubn.batch_budget = Some(SEGMENT_TARGET_BYTES + 1_000);
            let planned = pubn.plan().unwrap().len();
            let batches = pubn.segment_batches().unwrap();
            assert!(batches.len() > 1, "fixture: more than one batch");
            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches[..1].to_vec())
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
            let durable = store
                .scan(&seg_range_prefix(&group(7)))
                .await
                .unwrap()
                .len();
            assert!(
                durable > 0 && durable < planned,
                "fixture: the phase must stop INSIDE the plan ({durable} of {planned})",
            );
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, planned as u32),
            )
            .await;
            let err = pubn
                .flip(&store)
                .await
                .expect_err("a flip over a partial range must refuse");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::PublicationIncomplete {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: durable as u32,
                    planned,
                }),
                "the refusal names the first index the phase did not reach: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and nothing was published",
            );

            // …and once the phase finishes, the same flip publishes: the check is about
            // the range being complete, not about the flip. The resumed attempt is the
            // honest recovery — cursor read back from the store, phase 1 run to the end.
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, durable as u32),
            )
            .await;
            let mut resumed = publication(&prior, chunks.clone());
            resumed.segment_batch = &contribute;
            resumed.batch_budget = pubn.batch_budget;
            resumed.resume_from = durable as u32;
            assert_eq!(
                resumed.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed,
            );
            assert_eq!(
                resumed.flip(&store).await.unwrap(),
                CommitOutcome::Committed,
                "a complete range publishes: the refusals above are about the range, not \
                 about this flip",
            );
        });

        // --- a cursor that claims the whole plan buys nothing --------------------------
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            let mut pubn = publication(&prior, chunks.clone());
            let planned = pubn.plan().unwrap().len();
            pubn.resume_from = planned as u32;
            put(
                &store,
                SESSION_KEY.to_vec(),
                session_value(7, planned as u32),
            )
            .await;
            let err = pubn
                .flip(&store)
                .await
                .expect_err("a cursor is a claim, and the flip verifies it");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::ResumePrefixAbsent {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 0,
                }),
                "a claimed-durable segment that is absent stays the cursor's failure, \
                 named as such: {err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(encode(&prior)),
                "…and nothing was published",
            );
        });
    }

    /// The **flip** carries the caller's mutations too (`session → Completed`,
    /// `retire:records:{parts}`, `0016:654-663`), and it cannot be split — it *is* the
    /// publication instant. So an over-envelope flip is refused here rather than handed
    /// to a backend that can only reject it.
    #[test]
    fn the_flip_batch_refuses_a_caller_contribution_over_the_envelope() {
        let prior = InodeRecord::new_empty();
        let mut pubn = publication(&prior, many_chunks(4));
        pubn.batch_budget = Some(4_000);
        let exact = batch_bytes(&pubn.flip_batch().unwrap());
        assert!(
            exact <= 4_000,
            "fixture: the bare flip is inside the budget"
        );
        // The ceiling is INCLUSIVE — a flip of exactly the envelope commits, and a flip
        // one byte past it does not. The flip cannot be split, so this boundary is the
        // whole difference between publishing and refusing to.
        //
        // Charged through `flip_batch_after` over a phase assembled at the real budget:
        // `batch_budget` is **one** envelope for both phases, and a budget cut to the flip's
        // own size is one no segment-write batch could meet — a refusal `flip_batch` now
        // raises rather than swallows (it is the phase's answer, not the flip's, and the
        // phase's own boundary is `the_split_charges_the_callers_contribution_against_the_envelope`).
        let phase = pubn.segment_batches().unwrap();
        pubn.batch_budget = Some(exact);
        assert!(
            pubn.flip_batch_after(&phase).is_ok(),
            "{exact} bytes fit {exact} bytes",
        );
        pubn.batch_budget = Some(exact - 1);
        assert!(
            pubn.flip_batch_after(&phase).is_err(),
            "…and one byte more does not",
        );

        pubn.batch_budget = Some(4_000);
        pubn.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 1))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .put(b"retire:records:1".to_vec(), Bytes::from(vec![b'x'; 8_000]));
        let err = pubn.flip_batch().unwrap_err();
        assert!(
            matches!(
                err,
                ChunkMapError::BatchOverBudget { bytes, budget, .. }
                    if bytes > 8_000 && budget == 4_000
            ),
            "the flip refuses an over-envelope contribution: {err}",
        );
    }

    /// **The envelope is not one budget but two, and the per-record one is the ceiling a
    /// batch can breach while looking perfectly healthy.** A backend's transaction limit
    /// and its per-value limit are independent (`crates/traits/src/lib.rs:747-752`): a
    /// single 100 001-byte caller value sits far inside a 5 MB transaction and is still
    /// rejected — **permanently**, since no retry and no finer split can shrink one
    /// record. A publication that emitted it would fail at commit *after* its segments
    /// were durable, with an error no caller can act on.
    ///
    /// Asserted adversarially, on the caller's contribution rather than on this
    /// committer's own well-behaved fixture: the aggregate check is the one that would
    /// pass, so it must be the caller's value that is refused.
    #[test]
    fn no_published_batch_may_carry_a_record_over_the_backend_ceilings() {
        let prior = InodeRecord::new_empty();
        let over = Bytes::from(vec![b'x'; MAX_VALUE_BYTES + 1]);
        let at_ceiling = Bytes::from(vec![b'x'; MAX_VALUE_BYTES]);

        // --- the flip -----------------------------------------------------------------
        let mut pubn = publication(&prior, many_chunks(4));
        pubn.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 1))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .put(b"retire:records:1".to_vec(), over.clone());
        assert!(
            batch_bytes(&pubn.flip) + 2 * MAX_VALUE_BYTES < MAX_BATCH_BYTES,
            "fixture: the assembled flip is far INSIDE the transaction envelope — the \
             aggregate check passes it, so only the per-value ceiling can catch it",
        );
        assert_eq!(
            pubn.flip_batch().unwrap_err(),
            ChunkMapError::ValueOverCeiling {
                key: "retire:records:1".to_string(),
                bytes: MAX_VALUE_BYTES + 1,
                ceiling: MAX_VALUE_BYTES,
            },
        );
        // The ceiling is INCLUSIVE: exactly at it publishes.
        pubn.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 1))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .put(b"retire:records:1".to_vec(), at_ceiling.clone());
        assert!(pubn.flip_batch().is_ok(), "exactly at the ceiling fits");
        // …and the key limit is charged the same way, on every key the batch carries —
        // inclusive at its own boundary, and reported without dragging the offending key
        // into the message: an error that echoed a 10 KB key would put it in every log
        // line that handled it.
        pubn.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 1))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .delete(vec![b'k'; MAX_KEY_BYTES + 1]);
        assert_eq!(
            pubn.flip_batch().unwrap_err(),
            ChunkMapError::KeyOverCeiling {
                key: format!("{}…", "k".repeat(64)),
                bytes: MAX_KEY_BYTES + 1,
                ceiling: MAX_KEY_BYTES,
            },
        );
        pubn.flip = WriteBatch::new()
            .require(SESSION_KEY.to_vec(), session_value(7, 1))
            .put(SESSION_KEY.to_vec(), completed_session_value(7))
            .delete(vec![b'k'; MAX_KEY_BYTES]);
        assert!(
            pubn.flip_batch().is_ok(),
            "a key of exactly the ceiling fits — the limit is a ceiling, not a fence post",
        );
        // A key at the rendering width is shown WHOLE: the ellipsis means "there was
        // more", so a message that elided at exactly 64 would report a key it had in full.
        assert_eq!(
            ChunkMapError::KeyOverCeiling {
                key: rendered_key(&[b'k'; 64]),
                bytes: 0,
                ceiling: 0,
            },
            ChunkMapError::KeyOverCeiling {
                key: "k".repeat(64),
                bytes: 0,
                ceiling: 0,
            },
        );

        // --- and every segment-write batch, on the same terms -------------------------
        let bloated = |info: &SegmentBatchInfo| {
            session_contribution(7, info).put(b"retire:bytes:1".to_vec(), over.clone())
        };
        let mut pubn = publication(&prior, many_chunks(4));
        pubn.segment_batch = &bloated;
        assert_eq!(
            pubn.segment_batches().unwrap_err(),
            ChunkMapError::ValueOverCeiling {
                key: "retire:bytes:1".to_string(),
                bytes: MAX_VALUE_BYTES + 1,
                ceiling: MAX_VALUE_BYTES,
            },
            "a per-batch contribution over the value ceiling is refused before it is \
             committed, not re-split forever",
        );
        let fitting = |info: &SegmentBatchInfo| {
            session_contribution(7, info).put(b"retire:bytes:1".to_vec(), at_ceiling.clone())
        };
        pubn.segment_batch = &fitting;
        assert!(pubn.segment_batches().is_ok());

        // …and this committer's OWN segment records are inside it by construction, at the
        // real target — the property the object-ceiling arithmetic rests on.
        let published = publication(&prior, many_chunks(20_000));
        let planned = published.plan().unwrap();
        assert!(planned.len() > 1, "fixture: several segment records");
        for (_, record) in &planned {
            assert!(encode(record).len() <= MAX_VALUE_BYTES);
        }
    }

    /// **Bytes are not the only envelope: a batch is bounded by `min(B_bytes, B_ops)`**
    /// (`0016:640-648`). A batch of many tiny mutations or preconditions is inside every
    /// size limit and still spends the backend's 5 s transaction deadline on sequential
    /// round trips — and a batch that always times out is not slow, it is **stuck**: it
    /// fails identically at every retry, which is the one failure a staged publication
    /// cannot recover from. So the split honours the operation cap too, and the
    /// unsplittable flip is refused rather than emitted over it.
    #[test]
    fn a_batch_is_bounded_by_operations_as_well_as_bytes() {
        let prior = InodeRecord::new_empty();
        // A contribution of tiny mutations: 12 puts of a few bytes each — nothing here is
        // remotely near a byte limit.
        let chatty = |info: &SegmentBatchInfo| {
            let mut batch = session_contribution(7, info);
            for n in 0..12u32 {
                batch = batch.put(format!("retire:records:{n}").into_bytes(), Bytes::from("1"));
            }
            batch
        };
        let mut pubn = publication(&prior, many_chunks(40));
        pubn.segment_batch = &chatty;

        // Byte-bounded only, at the REAL byte envelope: every segment record fits in one
        // transaction, so bytes bind nothing here.
        let by_bytes = pubn
            .staged_batches(
                SMALL_SEGMENT,
                MAX_ROOT_SEGMENTS,
                MAX_BATCH_BYTES,
                MAX_BATCH_OPS,
            )
            .unwrap();
        assert_eq!(by_bytes.len(), 1, "fixture: bytes alone bound nothing");
        // Operation-bounded: the SAME publication splits, and every batch is inside the
        // cap — measured in operations, not guessed from a record count.
        const OPS: usize = 20;
        let by_ops = pubn
            .staged_batches(SMALL_SEGMENT, MAX_ROOT_SEGMENTS, MAX_BATCH_BYTES, OPS)
            .unwrap();
        for (n, batch) in by_ops.iter().enumerate() {
            assert!(
                batch_ops(batch) <= OPS,
                "batch {n} carries {} operations, over the {OPS} cap",
                batch_ops(batch),
            );
            assert!(
                batch_bytes(batch) < MAX_BATCH_BYTES / 100,
                "fixture: the op cap, not the byte budget, is what bounds these batches",
            );
        }
        assert!(
            by_ops.len() > by_bytes.len(),
            "the operation cap must actually bind: {} batches by bytes, {} by ops",
            by_bytes.len(),
            by_ops.len(),
        );
        // …and it binds *minimally*: some batch reaches exactly the cap. A split that
        // reserved more than the contribution actually costs would spend transactions the
        // envelope does not require — the same "as full as the budget allows" property the
        // byte split is held to, in the other currency.
        assert!(
            by_ops.iter().any(|b| batch_ops(b) == OPS),
            "the split must fill a batch to the cap, not merely stay under it: {:?}",
            by_ops.iter().map(batch_ops).collect::<Vec<_>>(),
        );
        // Every planned segment still ships — a finer split, never a dropped record.
        let planned = pubn
            .plan_with(SMALL_SEGMENT, MAX_ROOT_SEGMENTS)
            .unwrap()
            .len();
        assert_eq!(
            by_ops
                .iter()
                .map(|b| b
                    .puts
                    .iter()
                    .filter(|(k, _)| k.starts_with(SEG_PREFIX))
                    .count())
                .sum::<usize>(),
            planned,
        );

        // A contribution so operation-heavy that not even one segment fits beside it
        // fails **closed**, rather than emitting a transaction the backend can only
        // reject at every retry.
        assert!(matches!(
            pubn.staged_batches(SMALL_SEGMENT, MAX_ROOT_SEGMENTS, MAX_BATCH_BYTES, 4),
            Err(ChunkMapError::BatchOverOps { cap: 4, .. }),
        ));
        // …and the refusal names the envelope that was actually breached. Here the batch
        // sits at **exactly** the byte budget (measured, then handed back as the budget)
        // and over the operation cap: reporting `BatchOverBudget` would send the operator
        // after a knob that is not the one binding.
        let single = pubn
            .staged_batches(SMALL_SEGMENT, MAX_ROOT_SEGMENTS, MAX_BATCH_BYTES, 4)
            .unwrap_err();
        assert!(matches!(single, ChunkMapError::BatchOverOps { .. }));
        let one_batch = pubn
            .staged_batches(
                usize::MAX,
                MAX_ROOT_SEGMENTS,
                MAX_BATCH_BYTES,
                MAX_BATCH_OPS,
            )
            .unwrap();
        assert_eq!(one_batch.len(), 1, "fixture: one segment, one batch");
        let at_budget = batch_bytes(&one_batch[0]);
        assert!(
            pubn.staged_batches(usize::MAX, MAX_ROOT_SEGMENTS, at_budget, MAX_BATCH_OPS)
                .is_ok(),
            "a batch of exactly the byte budget fits it",
        );
        let err = pubn
            .staged_batches(usize::MAX, MAX_ROOT_SEGMENTS, at_budget, 4)
            .unwrap_err();
        assert!(
            matches!(err, ChunkMapError::BatchOverOps { ops, cap: 4, .. } if ops > 4),
            "at exactly the byte budget, an operation overflow is an operation failure: {err}",
        );

        // The flip cannot be split — it *is* the publication instant — so an over-cap
        // caller contribution is refused there. At the REAL cap, from the caller's own
        // contribution, with every value a single byte.
        let mut pubn = publication(&prior, many_chunks(4));
        let mut flip = flip_fence(&pubn);
        for n in 0..MAX_BATCH_OPS {
            flip = flip.put(format!("retire:records:{n}").into_bytes(), Bytes::from("1"));
        }
        pubn.flip = flip;
        let err = pubn
            .flip_batch()
            .expect_err("an over-cap flip must be refused, not emitted");
        assert!(
            matches!(err, ChunkMapError::BatchOverOps { batch: 0, ops, cap: MAX_BATCH_OPS } if ops > MAX_BATCH_OPS),
            "{err}",
        );
        // …and it is the operation count that refused it, not the size: the same batch is
        // three orders of magnitude inside the byte envelope.
        let mut inside = flip_fence(&pubn);
        for n in 0..(MAX_BATCH_OPS - 4) {
            inside = inside.put(format!("retire:records:{n}").into_bytes(), Bytes::from("1"));
        }
        pubn.flip = inside;
        let batch = pubn.flip_batch().expect("inside the cap");
        assert!(batch_bytes(&batch) < MAX_BATCH_BYTES / 100);

        // The cap is INCLUSIVE, and the flip cannot be split, so this boundary is the whole
        // difference between publishing and refusing to — the same treatment the byte
        // envelope gets one field over.
        let exact = batch_ops(&batch);
        pubn.ops_budget = Some(exact);
        assert!(
            pubn.flip_batch().is_ok(),
            "{exact} operations fit a {exact}-operation cap"
        );
        pubn.ops_budget = Some(exact - 1);
        assert!(
            matches!(
                pubn.flip_batch(),
                Err(ChunkMapError::BatchOverOps { ops, cap, .. }) if ops == exact && cap == exact - 1
            ),
            "…and one operation more does not",
        );
    }

    /// [`batch_ops`] is the operation envelope's unit of account (`B_ops`,
    /// `0016:640-648`), so its arithmetic is pinned against a hand-counted batch rather
    /// than against itself: every precondition the transaction must read and every
    /// mutation it must apply, each counted once.
    #[test]
    fn batch_ops_counts_every_precondition_and_mutation() {
        assert_eq!(batch_ops(&WriteBatch::new()), 0);
        let batch = WriteBatch::new()
            .require(b"a".to_vec(), Bytes::from_static(b"1"))
            .require_absent(b"b".to_vec())
            .put(b"c".to_vec(), Bytes::from_static(b"2"))
            .put(b"d".to_vec(), Bytes::from_static(b"3"))
            .put(b"e".to_vec(), Bytes::from_static(b"4"))
            .delete(b"f".to_vec());
        assert_eq!(
            batch_ops(&batch),
            2 + 3 + 1,
            "2 preconditions + 3 puts + 1 delete — preconditions count, because each is a \
             read the transaction performs before it can commit",
        );
    }

    /// **A caller's contribution rides the committer's own batch, so a key it shares with
    /// the publication is not a conflict the backend can report — it is a silent
    /// overwrite.** Both mutations are in one transaction; the last one wins, and the
    /// publication reports `Committed` over a root, a segment or a group marker the caller
    /// wrote. Refused, in both phases, before anything is committed.
    #[test]
    fn a_caller_contribution_may_not_write_the_publications_own_records() {
        let prior = InodeRecord::new_empty();
        let owned = [
            ("inode root", inode_key(1)),
            ("segment record", seg_key(&group(7), 0)),
            ("seggrp: marker", seggrp_key(NONCE)),
        ];

        for (owner, key) in owned.clone() {
            // --- phase 1: a put ---
            let put_it = |info: &SegmentBatchInfo| {
                session_contribution(7, info).put(key.clone(), Bytes::from_static(b"mine"))
            };
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.segment_batch = &put_it;
            assert_eq!(
                pubn.segment_batches().unwrap_err(),
                ChunkMapError::ContributionCollides {
                    key: String::from_utf8_lossy(&key).into_owned(),
                    owner,
                    phase: "segments",
                },
            );

            // --- phase 1: a delete is the same hazard, one verb over ---
            let delete_it =
                |info: &SegmentBatchInfo| session_contribution(7, info).delete(key.clone());
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.segment_batch = &delete_it;
            assert_eq!(
                pubn.segment_batches().unwrap_err(),
                ChunkMapError::ContributionCollides {
                    key: String::from_utf8_lossy(&key).into_owned(),
                    owner,
                    phase: "segments",
                },
            );

            // --- the flip ---
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.flip = flip_fence(&pubn).put(key.clone(), Bytes::from_static(b"mine"));
            assert_eq!(
                pubn.flip_batch().unwrap_err(),
                ChunkMapError::ContributionCollides {
                    key: String::from_utf8_lossy(&key).into_owned(),
                    owner,
                    phase: "flip",
                },
            );
        }

        // A **precondition** on one of those keys is legitimate and stays allowed: reading
        // a record the publication also writes cannot overwrite anything, and a caller may
        // well want to pin what it saw.
        let mut pubn = publication(&prior, many_chunks(4));
        pubn.flip = flip_fence(&pubn).require(inode_key(1), encode(&prior));
        assert!(pubn.flip_batch().is_ok());

        // …and a `retire:` key of the PRIOR generation's group is not this publication's
        // to refuse: only its own group's range is owned.
        let other = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 3).unwrap();
        let mut pubn = publication(&prior, many_chunks(4));
        pubn.flip = flip_fence(&pubn).delete(seg_key(&other, 0));
        assert!(pubn.flip_batch().is_ok());
    }

    /// **A resolve that read every segment can still be describing a generation the object
    /// no longer has — and that is the more dangerous of the two staleness cases.**
    ///
    /// A supersede flips the root first and its predecessor's `seg:` records are deleted
    /// later, by a byte-budgeted drain (`0016:2452-2462`), so between those two instants
    /// BOTH generations' segments exist. A resolve holding the retired root then reads
    /// every segment successfully. Answering `Resolved` there hands a maintenance pass the
    /// **retired** generation's fragments as the object's protected set while omitting the
    /// live one's — GC then sees the live generation's fragments as unreferenced and
    /// restore marks them stranded. So a complete read is confirmed against the root too,
    /// and the live entry restarts onto the generation that replaced it.
    #[test]
    fn a_complete_resolve_of_a_superseded_generation_is_retired_and_restarts() {
        let store = mem_store();
        let prior = InodeRecord::new_empty();
        let first = many_chunks(40);
        let second = many_chunks(24);
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let a = publication(&prior, first.clone());
            assert_eq!(a.publish(&store).await.unwrap(), CommitOutcome::Committed);
            let root_a: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();

            // Generation B supersedes it, under its own group. The modelled session is
            // reopened for the new attempt, exactly as a second Complete would fence.
            //
            // Its **segments** are written by the committer, and its root is installed
            // directly: this committer refuses to publish over a segmented generation
            // (`SegmentedRetirementUnsupported`, `Self::root`) because nothing in this slice
            // retires the generation it replaces — the state below is the one #636's
            // retirement-carrying flip produces, and it is the state the resolver has to
            // settle either way.
            put(&store, SESSION_KEY.to_vec(), session_value(7, 0)).await;
            let mut b = publication(&prior, second.clone());
            b.group = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 9).unwrap();
            b.flip = flip_fence(&b);
            assert_eq!(
                b.write_segments(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            let root_b = InodeRecord {
                version: root_a.version + 1,
                ..b.root().unwrap()
            };
            put(&store, inode_key(1), encode_inode(&root_b).unwrap()).await;

            // The fixture contains the fault: A's segments are all still there, because
            // the drain that deletes them has not run.
            assert_eq!(
                store.scan(&seg_range_prefix(&a.group)).await.unwrap().len(),
                a.plan().unwrap().len(),
                "fixture: the retired generation's segments are still readable",
            );

            // A resolve of the retired root therefore READS EVERY SEGMENT — and must
            // still not report them as the object's chunks.
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root_a)
                    .await
                    .unwrap(),
                MapResolution::Retired,
                "a complete read of a superseded generation is stale, not resolved",
            );
            assert!(
                resolve_chunk_homes(&store, &inode_key(1), &root_a)
                    .await
                    .unwrap()
                    .is_none(),
                "the repoint entry answers the same way, or a repair would CAS a \
                 retired record",
            );

            // The live entry restarts onto the generation that replaced it — the positive
            // observable: the LIVE chunks, not the retired ones, and not an empty map.
            let live = resolve_live_chunk_map(&store, &inode_key(1), &root_a)
                .await
                .unwrap()
                .expect("the object still exists, under a new generation");
            assert_eq!(live.restarted.as_ref(), Some(&root_b));
            assert_eq!(live.chunks.as_ref(), second.as_slice());
            assert_ne!(live.chunks.as_ref(), first.as_slice());

            // …and resolving the LIVE root is unchanged: one read, no restart.
            let current = resolve_live_chunk_map(&store, &inode_key(1), &root_b)
                .await
                .unwrap()
                .expect("the live generation resolves");
            assert!(current.restarted.is_none());
            assert_eq!(current.chunks.as_ref(), second.as_slice());
        });
    }

    /// **A pass holding a stale snapshot must resolve the LIVE generation for a flat map
    /// too, and only one of the two entries promises that.**
    ///
    /// The segmented shape cannot get this wrong: its resolve reads the root a second time
    /// to settle decision 7(h), so a superseded generation is noticed and re-resolved. The
    /// **flat** shape can, and silently: the snapshot record *is* the map, so a
    /// superseded object's old chunk list comes back looking perfectly current. A
    /// maintenance pass resolving from a `scan("inode:")` — minutes old at the far end of a
    /// large store — would then enter the *previous* generation's chunks in GC's reference
    /// set while the live generation's fragments are in nobody's, and restore would
    /// manufacture `orphan:` evidence for bytes the object still owns
    /// (`restore.rs:222`,`:266`). That is the #508-attempt-4 shape one layer up: protection
    /// computed over a map that is not the object's.
    ///
    /// So the fixture supersedes a flat map with a flat map — the case with no segment read
    /// anywhere in it — and asserts the two entries by their contracts: the snapshot entry
    /// answers about the caller's record, the **current** entry reads the root and answers
    /// about the store.
    #[test]
    fn the_maintenance_entry_resolves_the_live_root_for_a_flat_map_too() {
        let store = mem_store();
        let old = vec![chunk(11, 8, vec![0]), chunk(12, 8, vec![1])];
        let new = vec![chunk(21, 8, vec![2])];
        block_on(async {
            let snapshot = InodeRecord {
                size: 16,
                chunk_map: ChunkMap::Flat(old.clone()),
                state: InodeState::Committed,
                version: 1,
                ..Default::default()
            };
            put(&store, inode_key(1), encode(&snapshot)).await;
            // The pass scans here, and the object is superseded under it.
            let live = InodeRecord {
                size: 8,
                chunk_map: ChunkMap::Flat(new.clone()),
                version: 2,
                ..snapshot.clone()
            };
            put(&store, inode_key(1), encode(&live)).await;

            let current = resolve_current_chunk_map(&store, &inode_key(1))
                .await
                .unwrap()
                .expect("a live committed generation");
            assert_eq!(
                current.chunks, new,
                "the maintenance entry must resolve the generation the store HAS, not the \
                 one the scan saw: the live chunks are the ones GC has to protect",
            );
            assert_eq!(current.record, live, "…and it hands back the live root");

            // The snapshot entry is the read path's, and it answers about the record it was
            // given — which is exactly why a pass may not use it.
            let stale = resolve_live_chunk_map(&store, &inode_key(1), &snapshot)
                .await
                .unwrap()
                .expect("the caller's own record still resolves");
            assert_eq!(stale.chunks.as_ref(), old.as_slice());
            assert!(stale.restarted.is_none());

            // The repoint entry reads the root for the same reason: a CAS against a stale
            // flat root could only ever lose.
            let homes = resolve_current_chunk_homes(&store, &inode_key(1))
                .await
                .unwrap()
                .expect("a live committed generation");
            assert_eq!(homes.record, live);
            assert_eq!(
                homes
                    .chunks
                    .iter()
                    .map(|h| h.chunk.clone())
                    .collect::<Vec<_>>(),
                new,
            );

            // A deleted object answers `None` at both current entries — never "this object
            // owns no bytes", which is the answer that gets fragments reclaimed.
            let store = mem_store();
            assert!(resolve_current_chunk_map(&store, &inode_key(1))
                .await
                .unwrap()
                .is_none());
            assert!(resolve_current_chunk_homes(&store, &inode_key(1))
                .await
                .unwrap()
                .is_none());
        });
    }

    /// **A staged publication must be fenced ON ITS DECLARED FENCE RECORD, in BOTH
    /// phases.** The rollback that reclaims a crashed attempt deletes that attempt's `seg:`
    /// records (`0016:2347-2362`), and the caller's fence — `require(mpu == Completing@E)`
    /// — is the only thing that makes this publisher lose to it. Unfenced, the deletion can
    /// land between the segment phase and the flip, and the flip's own root
    /// compare-and-swap still succeeds because the rollback never touched the root: a
    /// **live root naming records that are gone**, which every consumer then fails closed
    /// on for the life of the object.
    ///
    /// "States *a* precondition" is not that property, and the difference is the whole
    /// finding: a batch pinned to an unrelated dirent or bucket record satisfies a
    /// non-emptiness check and is overtaken by the rollback exactly as an unfenced batch
    /// is. So each batch is checked against the publication's **declared** fence key —
    /// and against a *value* precondition on it, since `require_absent` starts holding the
    /// moment the rollback deletes the session.
    #[test]
    fn a_publication_not_fenced_on_its_fence_record_is_refused_in_either_phase() {
        let prior = InodeRecord::new_empty();
        let unfenced = |_: &SegmentBatchInfo| {
            WriteBatch::new().put(b"mpu:upload-1:progress".to_vec(), Bytes::from_static(b"1"))
        };
        // A real, well-formed precondition — on the WRONG record. This is the shape a
        // bare "has some precondition" check admits.
        let elsewhere = |_: &SegmentBatchInfo| {
            WriteBatch::new()
                .require(b"dirent:1:big.bin".to_vec(), Bytes::from_static(b"{}"))
                .put(b"mpu:upload-1:progress".to_vec(), Bytes::from_static(b"1"))
        };
        // Pinned to the right record, but by ABSENCE: satisfied by the very rollback the
        // fence exists to lose to.
        let absent = |_: &SegmentBatchInfo| {
            WriteBatch::new()
                .require_absent(SESSION_KEY.to_vec())
                .put(b"mpu:upload-1:progress".to_vec(), Bytes::from_static(b"1"))
        };
        let expected = |phase| ChunkMapError::Unfenced {
            phase,
            batch: 0,
            fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
        };

        for (contribute, why) in [
            (
                &unfenced as &dyn Fn(&SegmentBatchInfo) -> WriteBatch,
                "a segment-write batch that states no precondition is refused, however \
                 well-formed its mutations are",
            ),
            (
                &elsewhere,
                "a precondition on an UNRELATED record is not a fence: the rollback moves \
                 the session record, so only a precondition on that record loses to it",
            ),
            (
                &absent,
                "require_absent on the fence record is not a fence: the rollback DELETES \
                 the session, which makes an absence precondition start holding",
            ),
        ] {
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.segment_batch = contribute;
            assert_eq!(
                pubn.segment_batches().unwrap_err(),
                expected("segments"),
                "{why}"
            );
        }

        for (flip, why) in [
            (
                WriteBatch::new().put(b"retire:records:1".to_vec(), Bytes::from_static(b"1")),
                "the root CAS is not a fence: a rollback deletes the segments without \
                 touching the root, so the flip needs the caller's own precondition",
            ),
            (
                WriteBatch::new()
                    .require(b"dirent:1:big.bin".to_vec(), Bytes::from_static(b"{}"))
                    .put(b"retire:records:1".to_vec(), Bytes::from_static(b"1")),
                "a flip fenced on another record leaves the window between the last \
                 segment batch and the publication instant wide open",
            ),
        ] {
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.flip = flip;
            assert_eq!(pubn.flip_batch().unwrap_err(), expected("flip"), "{why}");
        }

        // The positive: with the declared fence in both phases the same publication
        // builds — the refusal is about the fence, not about these mutations.
        let fenced = publication(&prior, many_chunks(4));
        assert!(fenced.segment_batches().is_ok());
        assert!(fenced.flip_batch().is_ok());
    }

    /// **The flip must MOVE its fence record, not merely pin it.**
    ///
    /// A precondition freezes the fence for the flip's own transaction and says nothing
    /// about the instant after it commits. The rollback that reclaims a crashed attempt is
    /// fenced on the *same* record — `require(mpu == Completing@E)` plus the `seg:` deletes
    /// (`0016:2347-2362`) — so a flip that leaves that record exactly as it found it
    /// publishes a root and leaves the rollback's own precondition **true**: the rollback
    /// commits afterwards and deletes the segments the live root now names. The fence's
    /// whole purpose is that one of the two loses, and an unmoved fence lets both win, one
    /// after the other. Which is why the publication instant is normatively the batch that
    /// carries `session → Completed` (`0016:2338-2345`).
    ///
    /// **Every** put on the fence key has to clear that bar, not one of them. A batch is a
    /// set of mutations with no stated apply order within a single key
    /// (`crates/traits/src/lib.rs:1178-1186`), so a contribution that writes the fence twice
    /// — once to the terminal value, once back to the pinned one — is a non-transition on
    /// whichever backend applies the restoring put last, and the flip must not publish a
    /// generation whose safety depends on that choice.
    ///
    /// The refusal is deterministic and zero-I/O, so it lands with the rest of them:
    /// before a single `seg:` record is durable (the leg above).
    #[test]
    fn the_flip_must_move_its_fence_record_not_merely_pin_it() {
        let prior = InodeRecord::new_empty();
        let pinned = publication(&prior, many_chunks(4));
        let written = pinned.plan().unwrap().len() as u32;
        let unmoved = ChunkMapError::FenceNotTransitioned {
            fence: String::from_utf8_lossy(SESSION_KEY).into_owned(),
            phase: "flip",
            batch: 0,
        };

        for (flip, why) in [
            (
                WriteBatch::new().require(SESSION_KEY.to_vec(), session_value(7, written)),
                "a flip that only reads the fence leaves the racing rollback able to \
                 commit after it",
            ),
            (
                WriteBatch::new()
                    .require(SESSION_KEY.to_vec(), session_value(7, written))
                    .put(SESSION_KEY.to_vec(), session_value(7, written)),
                "re-writing the pinned value is not a transition: the record the rollback \
                 pins is unchanged, byte for byte",
            ),
            (
                WriteBatch::new()
                    .require(SESSION_KEY.to_vec(), session_value(7, written))
                    .put(SESSION_KEY.to_vec(), completed_session_value(7))
                    .put(SESSION_KEY.to_vec(), session_value(7, written)),
                "a SECOND put restoring the pinned value is the same non-transition wearing \
                 a real one in front of it: a batch's mutations on one key have no stated \
                 apply order (`crates/traits/src/lib.rs:1178-1186`), so on the backend where \
                 the restoring put lands last the rollback's precondition is true after the \
                 flip and it deletes the published generation's segments",
            ),
            (
                WriteBatch::new()
                    .require(SESSION_KEY.to_vec(), session_value(7, written))
                    .put(SESSION_KEY.to_vec(), session_value(7, written))
                    .delete(SESSION_KEY.to_vec()),
                "…and a delete beside it does not settle the ambiguity either: whether the \
                 fence ends up gone or restored is the backend's choice, and the publication \
                 instant may not depend on it",
            ),
        ] {
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.flip = flip;
            assert_eq!(pubn.flip_batch().unwrap_err(), unmoved, "{why}");
        }

        // Both real transitions are accepted: moving the session on, and deleting it.
        for flip in [
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, written))
                .put(SESSION_KEY.to_vec(), completed_session_value(7)),
            WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, written))
                .delete(SESSION_KEY.to_vec()),
        ] {
            let mut pubn = publication(&prior, many_chunks(4));
            pubn.flip = flip;
            assert!(
                pubn.flip_batch().is_ok(),
                "a flip that moves its fence publishes",
            );
        }
    }

    /// The caller contributes to **each** segment-write batch, not just to the flip: the
    /// recoverable-progress protocol has the session CAS its `segments_written` cursor
    /// with the batch that made the progress (`0016:350`). Contributed in a *second*
    /// commit it would be exactly the crash window staged publication exists to close.
    #[test]
    fn each_segment_batch_carries_the_callers_per_batch_progress() {
        let store = mem_store();
        let cursor_key = b"mpu:upload-1:segments_written".to_vec();
        let prior = InodeRecord::new_empty();
        let seen: Mutex<Vec<(usize, u32, u32, u32)>> = Mutex::new(Vec::new());
        let progress = |info: &SegmentBatchInfo| {
            seen.lock().unwrap().push((
                info.batch,
                info.first_index,
                info.last_index,
                info.written,
            ));
            // The caller's own record IS its fence: require the cursor its predecessor
            // left, and advance it to what this batch makes durable.
            WriteBatch::new()
                .require(
                    cursor_key.clone(),
                    Bytes::from(info.first_index.to_string().into_bytes()),
                )
                .put(
                    cursor_key.clone(),
                    Bytes::from(info.written.to_string().into_bytes()),
                )
        };
        let mut pubn = publication(&prior, many_chunks(40));
        pubn.segment_batch = &progress;
        // This caller keeps its cursor in a record of its own, so that is the record it
        // declares as the publication's fence — the rule is that every batch pins the
        // DECLARED record, not that the record is spelled `mpu:<id>`.
        pubn.fence_key = cursor_key.clone();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            put(&store, cursor_key.clone(), Bytes::from_static(b"0")).await;
            let batches = pubn
                .staged_batches(
                    SMALL_SEGMENT,
                    MAX_ROOT_SEGMENTS,
                    SMALL_BUDGET,
                    MAX_BATCH_OPS,
                )
                .unwrap();
            assert!(batches.len() > 1, "fixture: more than one batch");
            // The split is charged on the FULL mutation — every key, value and
            // precondition the transaction carries — and it is minimal: each batch is as
            // full as the budget allows, so the next batch's first record genuinely could
            // not have ridden along. A cost that under-counted would breach the envelope;
            // one that over-counted would spend transactions the envelope does not require.
            for (n, batch) in batches.iter().enumerate() {
                assert!(
                    batch_bytes(batch) <= SMALL_BUDGET,
                    "batch {n} is inside the budget"
                );
                if let Some(next) = batches.get(n + 1) {
                    let (k, v) = &next.puts[0];
                    assert!(
                        batch_bytes(batch) + k.len() + v.len() > SMALL_BUDGET,
                        "batch {n} left room for the next batch's first record",
                    );
                }
            }
            for batch in &batches {
                assert!(
                    batch.puts.iter().any(|(k, _)| *k == cursor_key),
                    "every batch carries the caller's per-batch mutation",
                );
            }
            // The hook is asked once per batch of **every** split attempt: the split is
            // re-run against the contribution it just produced until every assembled batch
            // fits (the documented fixed point, `staged_batches_over`), so `seen` holds each
            // attempt in turn and the returned batches are the last attempt's. Assert on
            // that attempt — its calls are the trailing `batches.len()` — and on the fact
            // that there was at least one.
            let all = seen.lock().unwrap().clone();
            assert!(all.len() >= batches.len(), "the hook is asked per batch");
            let recorded = all[all.len() - batches.len()..].to_vec();
            let mut written = 0u32;
            for (n, (batch, first, last, count)) in recorded.iter().copied().enumerate() {
                assert_eq!(batch, n);
                assert_eq!(first, written, "the batch starts where the last one ended");
                assert!(last >= first);
                assert_eq!(count, last + 1, "the cursor counts segments made durable");
                written = last + 1;
            }
            assert_eq!(
                written as usize,
                pubn.plan_with(SMALL_SEGMENT, MAX_ROOT_SEGMENTS)
                    .unwrap()
                    .len(),
                "the last batch's cursor names every planned segment",
            );

            // …and it is a real contribution: the cursor is durable after the phase, and
            // it is durable PER BATCH — after committing only the first batch the cursor
            // already names that batch's segments, which is what makes the progress
            // recoverable rather than all-or-nothing.
            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches[..1].to_vec())
                    .await
                    .unwrap(),
                CommitOutcome::Committed
            );
            assert_eq!(
                store.get(&cursor_key).await.unwrap().as_deref(),
                Some(recorded[0].3.to_string().as_bytes()),
                "the first batch's own commit carries its progress — not a second one",
            );
            assert_eq!(
                SegmentedPublication::commit_batches(&store, batches[1..].to_vec())
                    .await
                    .unwrap(),
                CommitOutcome::Committed
            );
            assert_eq!(
                store.get(&cursor_key).await.unwrap().as_deref(),
                Some(written.to_string().as_bytes()),
            );
        });
    }

    /// The byte-budgeted split itself (`0016:2331-2337`): the arithmetic that decides
    /// which records share a commit, exercised directly because at the real 5 MB budget
    /// its first branch needs a ~113 000-chunk fixture.
    #[test]
    fn the_batch_split_is_byte_budgeted_and_never_drops_a_record() {
        // Exactly at the budget: two per batch, never three.
        assert_eq!(
            batch_ranges(&[10, 10, 10, 10, 10], 20, MAX_BATCH_OPS),
            vec![0..2, 2..4, 4..5]
        );
        // One under: the third record starts a new batch.
        assert_eq!(
            batch_ranges(&[10, 10, 10], 19, MAX_BATCH_OPS),
            vec![0..1, 1..2, 2..3]
        );
        // A single record LARGER than the budget still gets a batch of its own rather
        // than being dropped — the phase must be able to make progress.
        assert_eq!(
            batch_ranges(&[100, 10], 20, MAX_BATCH_OPS),
            vec![0..1, 1..2]
        );
        assert_eq!(
            batch_ranges(&[], 20, MAX_BATCH_OPS),
            Vec::<std::ops::Range<usize>>::new()
        );
        // The OPERATION cap splits the same list independently of its bytes
        // (`B = min(B_bytes, B_ops)`, `0016:640-648`): three records per batch here, at a
        // budget that would have taken all nine in one.
        assert_eq!(
            batch_ranges(&[10; 9], 5_000, 3),
            vec![0..3, 3..6, 6..9],
            "the split is bounded by operations as well as bytes",
        );
        // …and it is floored at one record per batch: a cap of zero cannot mean "emit a
        // batch with no records" (an infinite split) nor "drop them".
        assert_eq!(batch_ranges(&[10; 3], 5_000, 0), vec![0..1, 1..2, 2..3]);
        // Whichever binds first wins, and neither is ignored.
        assert_eq!(batch_ranges(&[10; 6], 25, 4), vec![0..2, 2..4, 4..6]);
        // Every record appears exactly once, in order, for a spread of budgets.
        for budget in [1usize, 7, 13, 50, 5_000] {
            let costs: Vec<usize> = (1..=25).map(|n| n * 3).collect();
            let ranges = batch_ranges(&costs, budget, MAX_BATCH_OPS);
            let covered: Vec<usize> = ranges.iter().flat_map(|r| r.clone()).collect();
            assert_eq!(covered, (0..costs.len()).collect::<Vec<_>>());
            for range in &ranges {
                let bytes: usize = costs[range.clone()].iter().sum();
                assert!(
                    bytes <= budget || range.len() == 1,
                    "only a single oversize record may exceed the budget",
                );
            }
        }
    }

    /// `MAX_ROOT_SEGMENTS` is the guard the object ceiling rests on (`0016:2432-2440`),
    /// and an off-by-one that admitted an over-ceiling root is exactly the failure the
    /// 10 GiB launch requirement turns on. Asserted on BOTH sides of the boundary — at the real
    /// constants the boundary needs ~500 000 chunks, which is why the ceiling is a
    /// parameter of the planner.
    #[test]
    fn the_root_segment_ceiling_is_enforced_at_publication_on_both_sides() {
        let prior = InodeRecord::new_empty();
        // One chunk per segment (a target smaller than one chunk's encoding).
        let at_ceiling = publication(&prior, many_chunks(8));
        assert_eq!(at_ceiling.plan_with(1, 8).unwrap().len(), 8);
        assert_eq!(
            at_ceiling.plan_with(1, 7).unwrap_err(),
            ChunkMapError::TooManySegments { segments: 8 },
            "one segment over the ceiling is refused",
        );
        // …and the real planner uses the real ceiling.
        assert_eq!(
            publication(&prior, many_chunks(2)).plan().unwrap().len(),
            1,
            "the production planner packs by SEGMENT_TARGET_BYTES, not one per chunk",
        );
    }

    /// A **fresh** object key: the dirent named no inode, so the completer minted an id
    /// and the flip requires the key to be ABSENT at version 1 (`0016:350`). A committer
    /// that could only supersede an existing record could not express the ordinary first
    /// publication of a key at all.
    #[test]
    fn a_fresh_key_publication_requires_absence_and_publishes_version_one() {
        let store = mem_store();
        let chunks = many_chunks(4);
        let unused_prior = InodeRecord::new_empty();
        let mut pubn = publication(&unused_prior, chunks.clone());
        pubn.root = RootPrecondition::Fresh;
        block_on(async {
            open_session(&store).await;
            assert_eq!(pubn.root().unwrap().version, 1);
            assert_eq!(
                pubn.publish(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert_eq!(root.version, 1);
            assert!(root.chunk_map.is_segmented());
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap()
                    .chunks(),
                Some(chunks.as_slice()),
            );

            // …and the absence precondition is real: a second fresh flip over the now
            // occupied key refuses rather than clobbering the published generation.
            assert_eq!(pubn.flip(&store).await.unwrap(), CommitOutcome::Conflict);
        });
    }

    /// The capacity constants are the arithmetic the object ceiling is computed from
    /// (`0016:218-232`,`:2331-2337`), so they are pinned: a value that drifted would move
    /// the ceiling — and the batch and value bounds every backend inherits — silently.
    #[test]
    fn the_capacity_constants_are_the_ones_the_arithmetic_assumes() {
        // DECIMAL, not binary: FoundationDB's hard limits are 100 000 and 10 000 bytes
        // and 10 000 000 per transaction. A binary `100 * 1024` would call 2 400 bytes of
        // every value spendable that the backend rejects permanently.
        assert_eq!(
            MAX_VALUE_BYTES, 100_000,
            "FoundationDB's value ceiling is 100 000 bytes, not 102 400"
        );
        assert_eq!(
            MAX_KEY_BYTES, 10_000,
            "…and its key ceiling is 10 000 bytes"
        );
        assert_eq!(MAX_BATCH_BYTES, 5_000_000, "E_tx/2 per batch");
        assert_eq!(
            SEGMENT_TARGET_BYTES,
            MAX_VALUE_BYTES / 2,
            "one segment record targets the value ceiling with 2x headroom",
        );
        assert_eq!(SEG_INDEX_WIDTH, 6);
        assert_eq!(SEG_NONCE_HEX_LEN, 32);
    }

    /// The envelope is measured in **encoded bytes**, never a record count
    /// (`0016:2331-2337`, the inventory row `0016:654-663`).
    #[test]
    fn every_published_batch_stays_inside_its_byte_envelope() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(4_000);
        let pubn = publication(&prior, chunks);

        let segments = pubn.plan().unwrap();
        assert!(segments.len() > 1, "the split produced several segments");
        for (_, record) in &segments {
            assert!(
                encode(record).len() <= MAX_VALUE_BYTES,
                "no single value may exceed the 100 KB backend ceiling",
            );
        }
        for batch in pubn.segment_batches().unwrap() {
            let bytes: usize = batch.puts.iter().map(|(k, v)| k.len() + v.len()).sum();
            assert!(
                bytes <= MAX_BATCH_BYTES,
                "a segment-write batch is <= E_tx/2"
            );
        }
        let flip = pubn.flip_batch().unwrap();
        let bytes: usize = flip.puts.iter().map(|(k, v)| k.len() + v.len()).sum();
        assert!(
            bytes <= 4 * MAX_VALUE_BYTES + 1_024,
            "the flip's own inventory bound is <= 4·V + O(1); measured {bytes}",
        );
        for (_, value) in &flip.puts {
            assert!(value.len() <= MAX_VALUE_BYTES);
        }
    }

    /// The flip carries the CALLER's preconditions **and** the caller's mutations in the
    /// one batch that carries the root CAS: a committer that took only preconditions would
    /// force #636 to commit `session → Completed` / `retire:records:{parts}` in a SECOND
    /// batch, and a crash between the two either publishes an object whose parts are never
    /// retired or retires parts for a publication that never landed.
    #[test]
    fn the_flip_commits_the_callers_mutation_atomically_with_the_root_cas() {
        let caller_key = b"caller-mutation".to_vec();
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(4);

        // Arm 1 — the caller's precondition is FALSE: neither the root nor the caller's
        // mutation lands, and the already-written segments stay put (the rollback shape of
        // decision 7(c), minus the session).
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut pubn = publication(&prior, chunks.clone());
            // Fenced on the session record, as production is — but on a value the
            // segment-write phase did NOT leave behind, so the fence is false at the
            // publication instant.
            pubn.flip = WriteBatch::new()
                .require(SESSION_KEY.to_vec(), session_value(7, 99))
                .put(SESSION_KEY.to_vec(), completed_session_value(7))
                .put(caller_key.clone(), Bytes::from_static(b"retired"));
            assert_eq!(
                pubn.publish(&store).await.unwrap(),
                CommitOutcome::Conflict,
                "a false caller precondition must refuse the flip",
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert!(!root.chunk_map.is_segmented(), "the root was not flipped");
            assert!(
                store.get(&caller_key).await.unwrap().is_none(),
                "the caller's mutation did not land either — one batch, or neither",
            );
            assert!(
                !store
                    .scan(&seg_range_prefix(&group(7)))
                    .await
                    .unwrap()
                    .is_empty(),
                "the segments already written stay put for the rollback obligation to reclaim",
            );
        });

        // Arm 2 — the precondition holds: the root CAS and the caller's mutation land
        // together.
        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            let mut pubn = publication(&prior, chunks);
            let fence = flip_fence(&pubn);
            pubn.flip = fence.put(caller_key.clone(), Bytes::from_static(b"retired"));
            assert_eq!(
                pubn.publish(&store).await.unwrap(),
                CommitOutcome::Committed
            );
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            assert!(root.chunk_map.is_segmented());
            assert_eq!(
                store.get(&caller_key).await.unwrap().as_deref(),
                Some(b"retired".as_slice()),
            );
        });
    }

    // ---- (iv) the resolver is total, bounded, and orders segments ITSELF ----

    /// A store double that returns every read **reversed** and records the prefixes it was
    /// asked for. `MetadataStore::scan` states "Order is unspecified"
    /// (`crates/traits/src/lib.rs:770-775`), so a resolver that concatenated in returned
    /// order would produce a scrambled object here.
    struct Shuffling {
        inner: RedbMetadataStore,
        prefixes: Mutex<Vec<Vec<u8>>>,
        /// Whether a read ever actually *arrived* out of order — a page of two rows or
        /// more that this double reversed. Asserted by the test, because a fixture that
        /// only *claims* to shuffle (one-row pages, an empty range) would make the
        /// ordering assertion pass on a resolver that trusts arrival order.
        shuffled: Mutex<bool>,
    }

    #[async_trait]
    impl MetadataStore for Shuffling {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            self.prefixes.lock().unwrap().push(prefix.to_vec());
            let mut rows = self.inner.scan(prefix).await?;
            rows.reverse();
            Ok(rows)
        }

        // The required paginated read (#634), pages **deliberately out of order**.
        //
        // The testkit helper is contract-correct — it sorts, because clause 1 of the
        // paging contract makes byte-lexicographic order normative — so a resolve driven
        // through it unaltered would never meet a shuffled page and the assertion below
        // would prove nothing. The page is therefore reversed after the helper builds it:
        // the *cursor* stays the helper's (the true maximum, so the walk still advances
        // and terminates), and only the rows' arrival order is violated. That a real store
        // may not do this is the point — the resolver must not be able to tell.
        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<wyrd_traits::ScanPage> {
            let (mut rows, next) =
                wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await?;
            if rows.len() > 1 {
                *self.shuffled.lock().unwrap() = true;
            }
            rows.reverse();
            Ok((rows, next))
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            self.inner.commit(batch).await
        }
    }

    #[test]
    fn resolution_orders_by_index_and_reads_only_this_groups_range() {
        let store = Shuffling {
            inner: mem_store(),
            prefixes: Mutex::new(Vec::new()),
            shuffled: Mutex::new(false),
        };
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(1_200);
        let pubn = publication(&prior, chunks.clone());
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            open_session(&store).await;
            assert_eq!(
                pubn.publish(&store).await.unwrap(),
                CommitOutcome::Committed
            );

            // A SECOND group's segments in the same store: never read, never returned.
            let other = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 7).unwrap();
            put(
                &store,
                seg_key(&other, 0),
                encode(&SegmentRecord::new(vec![chunk(999, 8, vec![0])], 0).unwrap()),
            )
            .await;

            store.prefixes.lock().unwrap().clear();
            *store.shuffled.lock().unwrap() = false;
            let root: InodeRecord =
                decode(&store.get(&inode_key(1)).await.unwrap().unwrap()).unwrap();
            let resolved = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .unwrap();
            assert!(
                *store.shuffled.lock().unwrap(),
                "fixture: the resolve must actually have met a multi-row read arriving out \
                 of order, or the ordering assertion below is satisfied by a store that \
                 never shuffled anything",
            );
            assert_eq!(
                resolved.chunks(),
                Some(chunks.as_slice()),
                "the resolver must order segments by their parsed index, never by scan order",
            );
            let prefixes = store.prefixes.lock().unwrap().clone();
            assert_eq!(
                prefixes,
                vec![seg_range_prefix(&group(7))],
                "resolution reads the bounded per-object range and nothing else — never a \
                 global `seg:` scan",
            );
        });
    }

    /// A real backend, wrapped only to **count the rows a resolve materialises** — the
    /// property the assertion below is about, measured rather than argued.
    struct Counting {
        inner: RedbMetadataStore,
        rows: Mutex<usize>,
    }

    #[async_trait]
    impl MetadataStore for Counting {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            self.inner.scan(prefix).await
        }

        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<wyrd_traits::ScanPage> {
            let page = self.inner.scan_page(prefix, after, limit).await?;
            *self.rows.lock().unwrap() += page.0.len();
            Ok(page)
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            self.inner.commit(batch).await
        }
    }

    /// **A group range bigger than the root's table is one object's problem, not the
    /// store's.**
    ///
    /// `MetadataStore::scan` is complete-or-fail-loud at `SCAN_CAP`
    /// (`crates/traits/src/lib.rs:1019-1023`): asked for a range a damaged or
    /// half-rolled-back generation has filled, it either buffers all of it or answers
    /// `ScanCapExceeded` — a **store** error, which the per-object containment downcast
    /// cannot catch, so one damaged object would end the reference build for every healthy
    /// object in the store and blank the drain surface with it.
    ///
    /// The fixture therefore *includes the fault*: a real backend whose scan cap is 1, so
    /// no `scan` of the group range can succeed at all. Both arms are asserted — the
    /// healthy generation still resolves, and the corrupt one fails closed with a typed
    /// fault — and so is the bound that makes the difference: at most one row more than the
    /// root's own table is ever materialised, whatever the range holds.
    #[test]
    fn a_group_range_no_scan_could_read_stays_one_objects_problem() {
        let store = Counting {
            inner: RedbMetadataStore::in_memory().unwrap().with_scan_cap(1),
            rows: Mutex::new(0),
        };
        let map = SegmentedMap::new(
            group(7),
            vec![
                SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: 8,
                },
                SegmentRef {
                    index: 1,
                    byte_offset: 8,
                    byte_len: 8,
                },
            ],
        )
        .unwrap();
        let root = InodeRecord {
            size: 16,
            chunk_map: ChunkMap::Segmented(map),
            state: InodeState::Committed,
            version: 1,
            ..Default::default()
        };
        block_on(async {
            put(&store, inode_key(1), encode(&root)).await;
            for index in 0..2u32 {
                put(
                    &store,
                    seg_key(&group(7), index),
                    encode(
                        &SegmentRecord::new(
                            vec![chunk(ChunkId::from(index) + 1, 8, vec![0])],
                            u64::from(index) * 8,
                        )
                        .unwrap(),
                    ),
                )
                .await;
            }
            assert!(
                store.scan(&seg_range_prefix(&group(7))).await.is_err(),
                "fixture: a `scan` of the group range must breach the cap, or this test \
                 proves nothing about what a resolve does instead",
            );

            let resolved = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .unwrap();
            assert_eq!(
                resolved.chunks().map(|chunks| chunks.len()),
                Some(2),
                "a complete generation must resolve even where no `scan` of its range can \
                 succeed",
            );
            assert_eq!(
                *store.rows.lock().unwrap(),
                2,
                "…reading exactly the records the root names",
            );

            // Now the fault: records under the group's range that the root does not name —
            // an attempt that wrote further, or corruption. Six of them, so a resolve that
            // read the range whole would read three times what the root names.
            for index in 2..8u32 {
                put(
                    &store,
                    seg_key(&group(7), index),
                    encode(
                        &SegmentRecord::new(
                            vec![chunk(ChunkId::from(index) + 1, 8, vec![0])],
                            u64::from(index) * 8,
                        )
                        .unwrap(),
                    ),
                )
                .await;
            }
            *store.rows.lock().unwrap() = 0;
            let err = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::SegmentUnknown {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 2,
                }),
                "a range the root does not account for must fail closed as THIS OBJECT's \
                 fault — a store error here is uncontainable and takes the pass down for \
                 every other object: {err}",
            );
            assert!(
                *store.rows.lock().unwrap() <= 3,
                "the resolve materialised {} rows for a two-segment root: a damaged \
                 generation's range may not become the store's memory cost",
                store.rows.lock().unwrap(),
            );
        });
    }

    /// **The resolve budget is the root's own table, so the table is bounded before it is
    /// ever believed.**
    ///
    /// The bound above ([`read_group_range`]) is `accounted + 1` rows, and `accounted` is
    /// what the root *declares*. A root declaring 2 000 segments is a ~94 KB value — inside
    /// [`MAX_VALUE_BYTES`], so every backend stores it without a word — while the `seg:`
    /// records it authorises a consumer to read are up to [`MAX_VALUE_BYTES`] **each**:
    /// ~200 MB materialised per resolve, by every consumer, on every pass, for one damaged
    /// object. A budget taken from the record being validated is not a bound.
    ///
    /// So the ceiling the object arithmetic rests on (`MAX_ROOT_SEGMENTS`, `0016:2432-2440`
    /// — the same number the publication's own split refuses at) is enforced **where the
    /// table becomes a read**: the range reader refuses it, per object, with a typed
    /// [`ChunkMapError`], without reading one row.
    ///
    /// Not at decode, and this test asserts that too: the ceiling is a derived **capacity**
    /// constant, not a structural invariant of the format, so a stored record must stay
    /// decodable if it ever moves — otherwise lowering it would make already-published
    /// objects unreadable (ADR-0045's liberal-on-read boundary, and this repo's
    /// old-format-data-is-read rule).
    ///
    /// The fixture includes the fault the way the store would hold it: the over-ceiling root
    /// is **committed** (the backend accepts it — the value ceiling is not the guard) with
    /// real `seg:` records under its range for a resolve to walk into.
    #[test]
    fn a_root_table_over_the_ceiling_is_refused_before_it_is_read() {
        // As stored bytes: the shape a corrupt or hostile writer leaves behind, which no
        // constructor of this type could have produced.
        const OVER: usize = 2_000;
        let segments = (0..OVER)
            .map(|i| format!(r#"{{"index":{i},"byte_offset":{},"byte_len":16}}"#, i * 16))
            .collect::<Vec<_>>()
            .join(",");
        let bytes = Bytes::from(format!(
            r#"{{"size":{},"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":7}},"segment_count":{OVER},"segments":[{segments}]}},"state":"Committed","version":1}}"#,
            OVER * 16,
        ));
        assert!(
            bytes.len() < MAX_VALUE_BYTES,
            "fixture: the over-ceiling root must fit the value ceiling ({} bytes), or the \
             backend would refuse it and this test would prove nothing about the table's \
             own bound",
            bytes.len(),
        );
        assert_eq!(
            // Mapped to the count on both arms, so a regression reports the number and not
            // two thousand `SegmentRef`s.
            decode::<InodeRecord>(&bytes)
                .map(|record| record
                    .chunk_map
                    .segmented()
                    .map(SegmentedMap::segment_count))
                .map_err(|err| err.downcast_ref::<ChunkMapError>().cloned()),
            Ok(Some(OVER as u32)),
            "the record must still DECODE: the ceiling is a capacity constant, and a stored \
             object that stops being readable because one moved is the compatibility rule \
             this repo states for on-disk data",
        );

        let store = Counting {
            inner: RedbMetadataStore::in_memory().unwrap(),
            rows: Mutex::new(0),
        };
        block_on(async {
            put(&store, inode_key(1), bytes.clone()).await;
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap(),
                Some(bytes),
                "fixture: the store must actually hold the over-ceiling root — the fault is \
                 in the store, not rejected on the way in",
            );
            // Real records under its range, so a resolve that believed the table would have
            // something to read (and would report a different fault entirely).
            for index in 0..4u32 {
                put(
                    &store,
                    seg_key(&group(7), index),
                    encode(
                        &SegmentRecord::new(
                            vec![chunk(ChunkId::from(index) + 0xC0, 16, vec![0])],
                            u64::from(index) * 16,
                        )
                        .unwrap(),
                    ),
                )
                .await;
            }
            *store.rows.lock().unwrap() = 0;

            // The maintenance plane's own entry — it reads the root from the store and
            // resolves it, which is exactly where an unbounded budget would be spent.
            let err = resolve_current_chunk_map(&store, &inode_key(1))
                .await
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::TooManySegments { segments: OVER }),
                "a consumer must be told this object's table is inadmissible, not handed a \
                 resolution built from it: {err}",
            );
            assert_eq!(
                *store.rows.lock().unwrap(),
                0,
                "…and it must not have read one row of the group's range: the budget for \
                 that read is the very number that was refused",
            );

            // Containment, in the same store: the damaged object costs the fleet nothing.
            // `Gateway::recover` runs this before serving (`crates/server/src/lib.rs:123-124`).
            let (_, max_chunk) = high_water_marks(&store).await.unwrap();
            assert!(
                max_chunk >= ChunkId::from(3u32) + 0xC0,
                "the allocator floor must still cover every chunk id the store's `seg:` \
                 records name, beside a root nothing can resolve: got {max_chunk}",
            );

            // The exact boundary, and asserted without seeding 512 records: a table AT the
            // ceiling is not refused *for being too large* — it gets as far as reading the
            // range and fails on what is actually missing there. One more is refused before
            // the read. (Both roots are built as bytes: the table is oversized only in its
            // declared `segment_count`, which is the shape a damaged store holds.)
            for (count, expected) in [
                (
                    MAX_ROOT_SEGMENTS,
                    ChunkMapError::SegmentAbsent {
                        nonce: NONCE.to_string(),
                        epoch: 7,
                        index: 4,
                    },
                ),
                (
                    MAX_ROOT_SEGMENTS + 1,
                    ChunkMapError::TooManySegments {
                        segments: MAX_ROOT_SEGMENTS + 1,
                    },
                ),
            ] {
                let segments = (0..count)
                    .map(|i| format!(r#"{{"index":{i},"byte_offset":{},"byte_len":16}}"#, i * 16))
                    .collect::<Vec<_>>()
                    .join(",");
                let root = Bytes::from(format!(
                    r#"{{"size":{},"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":7}},"segment_count":{count},"segments":[{segments}]}},"state":"Committed","version":1}}"#,
                    count * 16,
                ));
                put(&store, inode_key(2), root).await;
                let err = resolve_current_chunk_map(&store, &inode_key(2))
                    .await
                    .unwrap_err();
                assert_eq!(
                    err.downcast_ref::<ChunkMapError>(),
                    Some(&expected),
                    "a {count}-segment table: the ceiling is exact, and only the table PAST \
                     it is refused unread — got {err}",
                );
            }
        });
    }

    // ---- (v) a rolled-back attempt's segments are disjoint from a later attempt's ----

    #[test]
    fn resolution_is_scoped_to_the_roots_epoch() {
        let store = mem_store();
        let stale = SegmentRecord::new(vec![chunk(0xDEAD, 8, vec![9])], 0).unwrap();
        let live = SegmentRecord::new(vec![chunk(0xBEEF, 8, vec![1])], 0).unwrap();
        block_on(async {
            put(&store, seg_key(&group(1), 0), encode(&stale)).await;
            put(&store, seg_key(&group(2), 0), encode(&live)).await;
            let root = InodeRecord {
                size: 8,
                chunk_map: ChunkMap::Segmented(
                    SegmentedMap::new(
                        group(2),
                        vec![SegmentRef {
                            index: 0,
                            byte_offset: 0,
                            byte_len: 8,
                        }],
                    )
                    .unwrap(),
                ),
                state: InodeState::Committed,
                version: 2,
                ..Default::default()
            };
            put(&store, inode_key(1), encode(&root)).await;

            let resolved = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .unwrap();
            assert_eq!(
                resolved.chunks(),
                Some(live.chunks.as_slice()),
                "a rolled-back attempt's epoch-1 segments must be invisible to the \
                 published epoch-2 generation (the F18 epoch-scoping property)",
            );
        });
    }

    // ---- (vi) decision 7(h)'s resolve-retry rule, BOTH arms ----

    #[test]
    fn a_missing_segment_under_a_changed_root_reads_as_retired() {
        let store = mem_store();
        let map = SegmentedMap::new(
            group(7),
            vec![SegmentRef {
                index: 0,
                byte_offset: 0,
                byte_len: 8,
            }],
        )
        .unwrap();
        let stale_root = InodeRecord {
            size: 8,
            chunk_map: ChunkMap::Segmented(map),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        block_on(async {
            // The store now holds a root naming a DIFFERENT generation, and the segments
            // of the one our reader is holding have already been drained.
            let current = InodeRecord {
                chunk_map: vec![chunk(1, 8, vec![0])].into(),
                ..stale_root.clone()
            };
            put(&store, inode_key(1), encode(&current)).await;
            assert_eq!(
                resolve_chunk_map(&store, &inode_key(1), &stale_root)
                    .await
                    .unwrap(),
                MapResolution::Retired,
                "a root that moved on means the generation was concurrently retired",
            );

            // …and an absent root reads the same way (the object was deleted).
            let empty = mem_store();
            assert_eq!(
                resolve_chunk_map(&empty, &inode_key(1), &stale_root)
                    .await
                    .unwrap(),
                MapResolution::Retired,
            );
        });
    }

    #[test]
    fn a_missing_segment_under_an_unchanged_root_fails_closed() {
        let store = mem_store();
        let root = InodeRecord {
            size: 16,
            chunk_map: ChunkMap::Segmented(
                SegmentedMap::new(
                    group(7),
                    vec![
                        SegmentRef {
                            index: 0,
                            byte_offset: 0,
                            byte_len: 8,
                        },
                        SegmentRef {
                            index: 1,
                            byte_offset: 8,
                            byte_len: 8,
                        },
                    ],
                )
                .unwrap(),
            ),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        block_on(async {
            put(&store, inode_key(1), encode(&root)).await;
            // Segment 0 is present; segment 1 — which the LIVE root still names — is not.
            put(
                &store,
                seg_key(&group(7), 0),
                encode(&SegmentRecord::new(vec![chunk(1, 8, vec![0])], 0).unwrap()),
            )
            .await;

            let err = resolve_chunk_map(&store, &inode_key(1), &root)
                .await
                .expect_err(
                    "a live generation never loses a segment: an unchanged root with an \
                     absent segment must fail closed, never resolve a torn map",
                );
            let typed = err
                .downcast_ref::<ChunkMapError>()
                .expect("the failure is typed, not a stringly-typed surprise");
            assert_eq!(
                *typed,
                ChunkMapError::SegmentAbsent {
                    nonce: NONCE.to_string(),
                    epoch: 7,
                    index: 1,
                },
            );
        });
    }

    /// A segment record whose span disagrees with the `SegmentRef` the root names is a
    /// silently misaligned map — the root would answer "which segment covers byte N" with
    /// a segment that does not. **Each side of the span** is asserted separately: a check
    /// that only rejected a disagreement on *both* would admit exactly this.
    #[test]
    fn a_segment_whose_span_disagrees_with_the_root_fails_closed() {
        let table = SegmentedMap::new(
            group(7),
            vec![
                SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: 8,
                },
                SegmentRef {
                    index: 1,
                    byte_offset: 8,
                    byte_len: 8,
                },
            ],
        )
        .unwrap();
        let root = InodeRecord {
            size: 16,
            chunk_map: ChunkMap::Segmented(table),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        // (what, the stored segment 1) — segment 0 is always correct.
        let cases = [
            (
                "byte_offset disagrees, byte_len agrees",
                SegmentRecord {
                    chunks: vec![chunk(2, 8, vec![0])],
                    byte_offset: 999,
                    byte_len: 8,
                },
            ),
            (
                "byte_len disagrees, byte_offset agrees",
                SegmentRecord {
                    chunks: vec![chunk(2, 4, vec![0]), chunk(3, 12, vec![0])],
                    byte_offset: 8,
                    byte_len: 16,
                },
            ),
        ];
        for (what, stored) in cases {
            let store = mem_store();
            block_on(async {
                put(&store, inode_key(1), encode(&root)).await;
                put(
                    &store,
                    seg_key(&group(7), 0),
                    encode(&SegmentRecord::new(vec![chunk(1, 8, vec![0])], 0).unwrap()),
                )
                .await;
                put(&store, seg_key(&group(7), 1), encode(&stored)).await;
                let err = resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .expect_err(what);
                assert!(
                    matches!(
                        err.downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::SegmentBoundsMismatch { index: 1, .. })
                    ),
                    "{what}: expected a typed bounds mismatch, got {err}",
                );
            });
        }
    }

    /// **A damaged row of a RETIRED generation is a retirement, not a fault — whichever way
    /// the damage is spelled.**
    ///
    /// Retirement *deletes* a generation's `seg:` records, so a reader holding a stale root
    /// races that deletion: it can see a row missing, and it can just as easily see one
    /// half-overwritten or holding the *replacement* generation's extents. Only the root can
    /// tell "this object's map is corrupt" from "this object moved on under me", so every
    /// anomaly the range shows goes through the same re-read ([`retired_or`]) — and a
    /// consumer racing an ordinary overwrite gets the **live** generation rather than an error
    /// about bytes nobody will ever read again.
    ///
    /// Both arms are asserted for both spellings, because the arms are what the rule is: with
    /// the root **unchanged** each is an invariant violation and fails closed (never a torn
    /// map), and with the root **moved on** each is a restart onto the generation that
    /// replaced it.
    #[test]
    fn a_damaged_row_of_a_retired_generation_restarts_rather_than_failing_closed() {
        let root = segmented_root_at(7, 2);
        let live = InodeRecord {
            size: 8,
            chunk_map: vec![chunk(42, 8, vec![0])].into(),
            state: InodeState::Committed,
            version: 3,
            ..Default::default()
        };
        for (what, stored) in [
            (
                "a row that does not decode at all",
                Bytes::from_static(b"{"),
            ),
            (
                "a row whose extents disagree with the root's table",
                encode(&SegmentRecord {
                    chunks: vec![chunk(1, 8, vec![0])],
                    byte_offset: 999,
                    byte_len: 8,
                }),
            ),
        ] {
            // The root still names this generation: corruption, and it fails closed.
            let store = mem_store();
            block_on(async {
                put(&store, inode_key(1), encode(&root)).await;
                put(&store, seg_key(&group(7), 0), stored.clone()).await;
                let err = resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .expect_err(what);
                assert!(
                    err.downcast_ref::<ChunkMapError>().is_some(),
                    "{what}: a live generation's damaged row is this object's fault, typed so \
                     every consumer contains it per object: {err}",
                );
            });

            // The root has moved on — the generation was retired while this resolve held it.
            // The same bytes are now a stale range, and the answer is the live map.
            let store = mem_store();
            block_on(async {
                put(&store, inode_key(1), encode(&live)).await;
                put(&store, seg_key(&group(7), 0), stored.clone()).await;
                assert!(
                    matches!(
                        resolve_chunk_map(&store, &inode_key(1), &root)
                            .await
                            .unwrap(),
                        MapResolution::Retired,
                    ),
                    "{what}: the root names another generation, so this range is a retired \
                     one's — a restart, never an error about bytes nobody will read again",
                );
                let restarted = resolve_live_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .unwrap()
                    .expect("the object is still live");
                assert_eq!(
                    restarted.chunks.as_ref(),
                    live.chunk_map.as_flat().unwrap(),
                    "{what}: …and the consumer resolves the generation that REPLACED it",
                );
                assert_eq!(
                    restarted.restarted.map(|r| r.version),
                    Some(3),
                    "{what}: …observably, so an operator sees a restart rather than inferring \
                     it",
                );
            });
        }
    }

    /// A store that answers a `seg:` range with a row that is **not** in that range — the
    /// backend-bug class the resolver's own nonce/epoch check exists for. The impostor's
    /// span matches the root's segment table exactly, so nothing downstream would notice:
    /// without the check its chunks are simply folded into another object's map.
    struct Impostor {
        inner: RedbMetadataStore,
        extra: (Vec<u8>, Bytes),
    }

    #[async_trait]
    impl MetadataStore for Impostor {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            let mut rows = self.inner.scan(prefix).await?;
            if prefix.starts_with(SEG_PREFIX) {
                rows.push(self.extra.clone());
            }
            Ok(rows)
        }

        // The required paginated read (#634): a test double needs *a* body, not a
        // backend's — the dev-only testkit helper pages over this store's own `scan`.
        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<wyrd_traits::ScanPage> {
            wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            self.inner.commit(batch).await
        }
    }

    /// A row from **another group or another epoch** must never be folded into this
    /// object's map, whichever of the two differs. Both spellings are asserted: a check
    /// that required them to differ *together* would admit each of these singly, and the
    /// stranger's chunks would resolve as this object's bytes.
    #[test]
    fn a_row_from_another_group_or_epoch_is_never_folded_into_this_map() {
        let root = segmented_root_at(7, 2);
        let real = SegmentRecord::new(vec![chunk(1, 8, vec![0])], 0).unwrap();
        let stranger = SegmentRecord::new(vec![chunk(999, 8, vec![9])], 0).unwrap();
        let other_nonce = SegmentGroup::new("ffffffffffffffffffffffffffffffff", 7).unwrap();
        for (what, key) in [
            ("another group's nonce", seg_key(&other_nonce, 0)),
            ("another epoch of this group", seg_key(&group(8), 0)),
        ] {
            let store = Impostor {
                inner: mem_store(),
                extra: (key, encode(&stranger)),
            };
            block_on(async {
                put(&store, inode_key(1), encode(&root)).await;
                put(&store, seg_key(&group(7), 0), encode(&real)).await;
                let err = resolve_chunk_map(&store, &inode_key(1), &root)
                    .await
                    .expect_err(what);
                assert!(
                    matches!(
                        err.downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::SegmentKeyMalformed { .. })
                    ),
                    "{what}: expected a typed rejection, got {err}",
                );
            });
        }
    }

    /// The repoint's **flat** arm, asserted on the batch it builds: the moved placement
    /// lands in the root's chunk map and the version advances by exactly one — the two
    /// writes that make a reader see the new generation. (The segmented arm is asserted
    /// end-to-end by the custodian consumer suite and the DST repoint property.)
    #[test]
    fn a_flat_repoint_rewrites_the_map_and_bumps_the_version_by_one() {
        let prior = InodeRecord {
            size: 16,
            chunk_map: vec![ec_chunk(1, 8, vec![0, 1, 2]), ec_chunk(2, 8, vec![0, 1, 2])].into(),
            state: InodeState::Committed,
            version: 4,
            ..Default::default()
        };
        let moved = ec_chunk(2, 8, vec![3, 1, 2]);
        let batch = repoint_chunk(1, &prior, &ChunkHome::Root { position: 1 }, vec![3, 1, 2])
            .expect("the position exists in the flat map");

        assert_eq!(batch.preconditions.len(), 1);
        assert_eq!(batch.preconditions[0].key, inode_key(1));
        assert_eq!(
            batch.preconditions[0].expected.as_deref(),
            Some(encode(&prior).as_ref()),
            "the CAS is on the prior root's exact bytes",
        );
        assert_eq!(batch.puts.len(), 1);
        let next: InodeRecord = decode(&batch.puts[0].1).unwrap();
        assert_eq!(
            next.chunk_map.as_flat().unwrap(),
            &[ec_chunk(1, 8, vec![0, 1, 2]), moved],
            "only the placement moved: the repointed chunk keeps its id, scheme and \
             length, and the other chunks are untouched",
        );
        assert_eq!(
            next.version,
            prior.version + 1,
            "the version advances by exactly one — a reader's proof it is a new generation",
        );
        assert_eq!(next.size, prior.size, "a repoint never changes the content");

        // A placement vector that is not the chunk's fragment count is refused: a short
        // one identity-fills its missing tail at the next read and sends a consumer to a
        // D-server that holds nothing (ADR-0040's strict maintenance boundary).
        assert_eq!(
            repoint_chunk(1, &prior, &ChunkHome::Root { position: 1 }, vec![3, 1])
                .expect_err("a short placement must be refused")
                .downcast_ref::<ChunkMapError>(),
            Some(&ChunkMapError::WritePlacementMalformed {
                position: 1,
                expected: 3,
                actual: 2,
            }),
        );

        // …and a stale plan is refused rather than applied to the wrong record.
        assert!(repoint_chunk(1, &prior, &ChunkHome::Root { position: 9 }, vec![0, 1, 2]).is_err());
        let segmented = segmented_root_at(7, 2);
        assert!(
            repoint_chunk(
                1,
                &segmented,
                &ChunkHome::Root { position: 0 },
                vec![0, 1, 2]
            )
            .is_err(),
            "a flat plan against a now-segmented root is a shape change, not a rewrite",
        );
    }

    /// **A repoint moves fragments, and moving them can make the record bigger.**
    ///
    /// The placement vector keeps its length and not its width: a `DServerId` renders in 1
    /// to 20 decimal digits, so evacuating a wide-EC chunk onto high-numbered servers grows
    /// every entry. The publication's 2x headroom is arithmetic about the *chunk* encoding
    /// (`0016:218-232`), not a bound on later repoints, so a segment written just inside the
    /// per-value ceiling can be pushed past it by a legitimate move — and the batch would
    /// then be rejected by the backend **permanently**, from a layer that cannot say which
    /// record was too big, failing every retry of that repair or evacuation.
    ///
    /// So the ceiling is re-checked where the record is rewritten, and the refusal is typed
    /// and names the `seg:` key. The control in the same fixture is a move of the same chunk
    /// to *narrow* ids, which must still commit — the rule is about the bytes, not about
    /// repointing a full segment.
    #[test]
    fn a_repoint_that_would_grow_a_segment_past_the_value_ceiling_is_refused() {
        // `ec_chunk`'s scheme has three fragments, so a placement vector is three ids long
        // (ADR-0040's strict maintenance boundary) — and each of them can render 20 digits.
        let narrow: Vec<DServerId> = (0..3).collect();
        let wide: Vec<DServerId> = (0..3).map(|_| DServerId::MAX).collect();
        // A record filled to just inside the ceiling with chunks whose placements are single
        // digits — the shape a publication writes.
        let per = encode(&ec_chunk(1, 8, narrow.clone())).len() + 1;
        let mut chunks: Vec<ChunkRef> = (1..=(MAX_VALUE_BYTES / per) as ChunkId)
            .map(|id| ec_chunk(id, 8, narrow.clone()))
            .collect();
        while encode(&SegmentRecord::new(chunks.clone(), 0).unwrap()).len() > MAX_VALUE_BYTES {
            chunks.pop();
        }
        let record = SegmentRecord::new(chunks, 0).unwrap();
        let prior = encode(&record);
        let headroom = MAX_VALUE_BYTES - prior.len();
        let growth = wide
            .iter()
            .map(|id| id.to_string().len() + 1)
            .sum::<usize>()
            - narrow
                .iter()
                .map(|id| id.to_string().len() + 1)
                .sum::<usize>();
        assert!(
            prior.len() <= MAX_VALUE_BYTES && headroom < growth,
            "fixture: the record must be storable ({} bytes) and one repoint must be enough \
             to push it over ({headroom} bytes of headroom, {growth} bytes of growth)",
            prior.len(),
        );

        let root = InodeRecord {
            size: record.byte_len(),
            chunk_map: ChunkMap::Segmented(
                SegmentedMap::new(
                    group(7),
                    vec![SegmentRef {
                        index: 0,
                        byte_offset: 0,
                        byte_len: record.byte_len(),
                    }],
                )
                .unwrap(),
            ),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        let key = seg_key(&group(7), 0);
        let home = ChunkHome::Segment {
            key: key.clone(),
            prior: prior.clone(),
            position: 0,
        };

        let err = repoint_chunk(1, &root, &home, wide)
            .expect_err("a repoint that would grow the record past the ceiling must refuse");
        assert!(
            matches!(
                err.downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::ValueOverCeiling { key: rendered, ceiling, .. })
                    if rendered.starts_with("seg:") && *ceiling == MAX_VALUE_BYTES
            ),
            "the refusal names the record and the ceiling it passed, before anything is \
             written: {err}",
        );

        // The control: the same chunk, moved to ids as narrow as the ones it had.
        let batch = repoint_chunk(1, &root, &home, vec![2, 1, 0])
            .expect("a move that does not grow the record still repoints");
        assert_eq!(batch.puts[0].0, key);
        assert!(batch.puts[0].1.len() <= MAX_VALUE_BYTES);
    }

    /// **A segment repoint is bound to the root's own generation.**
    ///
    /// `require(seg == prior)` and `require(inode == prior_root)` are each satisfiable by
    /// a home resolved from a generation that has since been *retired*: the segment record
    /// still holds its old bytes until the reaper reaches it, and the root is whatever the
    /// caller passed. Paired, they let a stale repoint commit — reporting success while
    /// writing into a record no consumer will read again, so the repair is silently lost
    /// and the caller has been told its fragment moved. The generation is therefore
    /// checked, not assumed.
    #[test]
    fn a_segment_repoint_refuses_a_home_from_another_generation() {
        let root = segmented_root_at(7, 2);
        let record = SegmentRecord::new(vec![ec_chunk(1, 8, vec![0, 1, 2])], 0).unwrap();
        let prior = encode(&record);
        let moved = vec![3, 1, 2];
        let home = |key: Vec<u8>| ChunkHome::Segment {
            key,
            prior: prior.clone(),
            position: 0,
        };

        // The matching generation repoints, and it CASes both the segment and the root.
        let batch = repoint_chunk(1, &root, &home(seg_key(&group(7), 0)), moved.clone())
            .expect("the home names this root's generation");
        assert_eq!(batch.puts.len(), 1);
        assert_eq!(batch.puts[0].0, seg_key(&group(7), 0));
        assert!(batch.preconditions.iter().any(
            |p| p.key == inode_key(1) && p.expected.as_deref() == Some(encode(&root).as_ref())
        ));

        // A home from the SAME nonce at an earlier epoch — the rolled-back attempt's
        // records, still present until the rollback drain removes them.
        let stale = repoint_chunk(1, &root, &home(seg_key(&group(6), 0)), moved.clone())
            .expect_err("a home from a retired epoch must be refused");
        assert!(
            matches!(
                stale.downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::SegmentGroupMismatch { root, home })
                    if root.ends_with(":7") && home.ends_with(":6")
            ),
            "the refusal names both generations: {stale}",
        );

        // …and a home whose index this root's table does not name.
        let unknown = repoint_chunk(1, &root, &home(seg_key(&group(7), 5)), moved.clone())
            .expect_err("a segment index the root does not name must be refused");
        assert!(
            matches!(
                unknown.downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::SegmentUnknown { index: 5, .. })
            ),
            "{unknown}",
        );

        // …and a segmented home against a FLAT root is a shape change, not a rewrite.
        let flat = InodeRecord {
            size: 8,
            chunk_map: vec![chunk(1, 8, vec![0, 1, 2])].into(),
            state: InodeState::Committed,
            ..Default::default()
        };
        assert!(matches!(
            repoint_chunk(1, &flat, &home(seg_key(&group(7), 0)), moved)
                .expect_err("a segment home against a flat root")
                .downcast_ref::<ChunkMapError>(),
            Some(ChunkMapError::MapShapeChanged),
        ));
    }

    /// **A repoint moves fragments and can change nothing else — by construction.**
    ///
    /// A repoint that could swap in a whole `ChunkRef` could change the chunk's **id**
    /// (publishing different content under a committed identity, with no publication fence
    /// and no retirement for the bytes it displaces), its **scheme** (re-framing how the
    /// stored fragments are read) or its **len**. The last one also breaks the record's own
    /// structure: `byte_len` is the sum of the chunk lengths *and* is repeated in the
    /// root's `SegmentRef`, so a re-lengthed chunk leaves a record that fails its own
    /// decode under a root that disagrees with it — an object no later pass can repair,
    /// because the map it would need to read is the record that will not decode.
    ///
    /// Validating each of those would leave the wrong thing *expressible*. The mutator
    /// takes a **placement vector**, so identity, framing and extent are carried over from
    /// the record the CAS is preconditioned on and there is no door to check.
    #[test]
    fn a_repoint_can_move_a_chunk_and_change_nothing_else() {
        let root = segmented_root_at(7, 2);
        let original = ec_chunk(1, 8, vec![0, 1, 2]);
        let record = SegmentRecord::new(vec![original.clone()], 0).unwrap();
        let prior = encode(&record);
        let home = ChunkHome::Segment {
            key: seg_key(&group(7), 0),
            prior: prior.clone(),
            position: 0,
        };

        // The move lands, and the three identity fields come from the PRIOR record.
        let batch = repoint_chunk(1, &root, &home, vec![3, 1, 2]).expect("a well-formed move");
        let moved: SegmentRecord = decode(&batch.puts[0].1).unwrap();
        assert_eq!(moved.chunks()[0].placement, vec![3, 1, 2]);
        assert_eq!(
            moved.chunks()[0].id,
            original.id,
            "the chunk id is carried over"
        );
        assert_eq!(
            moved.chunks()[0].scheme,
            original.scheme,
            "the EC scheme is carried over",
        );
        assert_eq!(
            moved.chunks()[0].len,
            original.len,
            "the length is carried over"
        );
        assert_eq!(
            moved.byte_len(),
            record.byte_len(),
            "so the record's byte_len still matches its chunks and the root's SegmentRef",
        );
        assert_eq!(
            decode::<SegmentRecord>(&encode(&moved)).unwrap(),
            moved,
            "a repointed record still decodes",
        );

        // A placement that is not the chunk's fragment count is refused, and the refusal
        // is total: the record is left untouched, so a caller that ignored the error still
        // holds a valid value.
        let mut record = record;
        assert_eq!(
            record.repoint(0, vec![3, 1]),
            Err(ChunkMapError::WritePlacementMalformed {
                position: 0,
                expected: 3,
                actual: 2,
            }),
        );
        assert_eq!(
            encode(&record),
            prior,
            "the refused mutation changed nothing"
        );
        assert_eq!(
            record.repoint(1, vec![3, 1, 2]),
            Err(ChunkMapError::ChunkPositionOutOfRange { position: 1 }),
        );
        // …and the move it IS for still lands, and still decodes.
        record.repoint(0, vec![3, 1, 2]).unwrap();
        assert_eq!(record.chunks()[0].placement, vec![3, 1, 2]);
        assert_eq!(
            decode::<SegmentRecord>(&encode(&record)).unwrap(),
            record,
            "a repointed record still decodes — its byte_len still matches its chunks",
        );
    }

    /// **A whole-map re-commit cannot flatten a segmented generation.**
    /// [`commit_chunk_map`] installs a flat map, so running it over a segmented `prior`
    /// would leave that generation's `seg:` records named by no root — nothing deletes
    /// them, and the fragments they alone reference are stranded for good. Retiring a
    /// segmented generation is the staged obligation of proposal 0016 decision 4/7(f)
    /// (#636); a segmented map's own maintenance rewrite is [`repoint_chunk`], which never
    /// rewrites the root. So this refuses, and it refuses **before** committing anything.
    #[test]
    fn a_whole_map_recommit_refuses_a_segmented_prior_rather_than_stranding_it() {
        let store = mem_store();
        let prior = segmented_root_at(7, 2);
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            let err = commit_chunk_map(&store, 1, &prior, vec![chunk(1, 8, vec![0])], 8)
                .await
                .expect_err("flattening a segmented generation must be refused");
            assert!(
                matches!(
                    err.downcast_ref::<ChunkMapError>(),
                    Some(ChunkMapError::SegmentedRetirementUnsupported { operation })
                        if operation.contains("re-committing")
                ),
                "{err}",
            );
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap().unwrap(),
                encode(&prior),
                "…and the root is untouched: the refusal precedes the commit",
            );

            // **The permanent refusal outranks the retryable one.** The leased overwrite
            // reads a `pending:` lease before it commits, and an absent or lapsed lease is
            // an ordinary `Conflict` the caller retries. A segmented prior is not
            // retryable at all — this slice cannot retire a segmented generation — so
            // deciding the lease first would answer `Conflict` and leave the caller
            // looping for ever on a call that can never succeed. Driven here with NO lease
            // in the store, which is exactly the arrangement that used to hide it.
            let err = commit_chunk_map_superseding_leased(
                &store,
                1,
                &prior,
                vec![chunk(1, 8, vec![0])],
                8,
                10,
                &[42],
                20,
                &ObjectMeta::default(),
            )
            .await
            .expect_err("a segmented prior must refuse whatever the lease says");
            assert!(
                matches!(
                    err.downcast_ref::<ChunkMapError>(),
                    Some(ChunkMapError::SegmentedRetirementUnsupported { operation })
                        if operation.contains("overwrite")
                ),
                "the refusal must be the permanent one, not a Conflict a caller retries: {err}",
            );

            // The flat path is unchanged — this is a shape refusal, not a new failure mode.
            let flat = InodeRecord {
                size: 8,
                chunk_map: vec![chunk(1, 8, vec![0])].into(),
                state: InodeState::Committed,
                version: 1,
                ..Default::default()
            };
            put(&store, inode_key(2), encode(&flat)).await;
            assert_eq!(
                commit_chunk_map(&store, 2, &flat, vec![chunk(1, 8, vec![1])], 8)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
        });
    }

    /// **Every door that advances a root's version refuses to advance it past its last
    /// value — none of them wraps, and none of them panics.**
    ///
    /// `version` is the monotonic counter every compare-and-swap in this module orders
    /// writers by. At `u64::MAX` an unchecked `+ 1` panics with overflow checks on and wraps
    /// to **zero** without them, and the wrap is the worse of the two: a stale writer then
    /// looks *newer* than the generation it lost to, so a repair or an overwrite can be
    /// admitted over a record that already superseded it. The staged publication has refused
    /// this since it was written; the three flat committers and the flat repoint reached the
    /// same arithmetic by a different route, and they are asserted here **together**,
    /// because "one of the four doors is checked" is exactly the state that made this a
    /// finding.
    ///
    /// The store is asserted untouched at the end: the refusal is deterministic and
    /// zero-I/O, so nothing may be durable after it.
    #[test]
    fn every_root_rewrite_refuses_a_version_that_would_overflow() {
        let prior = InodeRecord {
            size: 8,
            chunk_map: vec![ec_chunk(1, 8, vec![0, 1, 2])].into(),
            state: InodeState::Committed,
            version: u64::MAX,
            ..Default::default()
        };
        let expected = Some(&ChunkMapError::RootVersionOverflow { version: u64::MAX });

        // The maintenance repoint, which rewrites the root in place.
        assert_eq!(
            repoint_chunk(1, &prior, &ChunkHome::Root { position: 0 }, vec![3, 1, 2])
                .expect_err("a repoint may not advance a version that has nowhere to go")
                .downcast_ref::<ChunkMapError>(),
            expected,
        );

        let store = mem_store();
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            let next = vec![chunk(9, 8, vec![0])];

            assert_eq!(
                commit_chunk_map(&store, 1, &prior, next.clone(), 8)
                    .await
                    .expect_err("the repair committer must refuse it too")
                    .downcast_ref::<ChunkMapError>(),
                expected,
            );
            assert_eq!(
                commit_chunk_map_superseding(
                    &store,
                    1,
                    &prior,
                    next.clone(),
                    8,
                    10,
                    &ObjectMeta::default(),
                )
                .await
                .expect_err("…and the overwrite committer")
                .downcast_ref::<ChunkMapError>(),
                expected,
            );
            assert_eq!(
                commit_chunk_map_superseding_leased(
                    &store,
                    1,
                    &prior,
                    next,
                    8,
                    10,
                    &[],
                    20,
                    &ObjectMeta::default(),
                )
                .await
                .expect_err("…and its leased twin")
                .downcast_ref::<ChunkMapError>(),
                expected,
            );

            assert_eq!(
                store.get(&inode_key(1)).await.unwrap().unwrap(),
                encode(&prior),
                "a refusal this deterministic may not leave anything durable behind it",
            );
        });
    }

    /// **A refusal a committer was always going to make is decided before the arithmetic
    /// that can panic.**
    ///
    /// Superseding a **segmented** generation is permanently unsupported in this slice
    /// (its retirement is #636's staged obligation). That answer does not depend on the
    /// prior's version — but it used to be *reached* after the next record was built, so a
    /// segmented root at `u64::MAX` crashed the caller with an overflow panic instead of
    /// being told the operation is not supported. Order is the fix, and the assertion is
    /// that all three flat committers refuse the shape at a version where the arithmetic
    /// would have blown up first.
    #[test]
    fn a_segmented_prior_is_refused_before_the_version_arithmetic() {
        let store = mem_store();
        let prior = InodeRecord {
            version: u64::MAX,
            ..segmented_root_at(7, 2)
        };
        block_on(async {
            put(&store, inode_key(1), encode(&prior)).await;
            let next = vec![chunk(1, 8, vec![0])];

            for (what, err) in [
                (
                    "re-committing",
                    commit_chunk_map(&store, 1, &prior, next.clone(), 8)
                        .await
                        .expect_err("a segmented prior is refused, not overflowed"),
                ),
                (
                    "overwrite",
                    commit_chunk_map_superseding(
                        &store,
                        1,
                        &prior,
                        next.clone(),
                        8,
                        10,
                        &ObjectMeta::default(),
                    )
                    .await
                    .expect_err("a segmented prior is refused, not overflowed"),
                ),
                (
                    "overwrite",
                    commit_chunk_map_superseding_leased(
                        &store,
                        1,
                        &prior,
                        next.clone(),
                        8,
                        10,
                        &[],
                        20,
                        &ObjectMeta::default(),
                    )
                    .await
                    .expect_err("a segmented prior is refused, not overflowed"),
                ),
            ] {
                assert!(
                    matches!(
                        err.downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::SegmentedRetirementUnsupported { operation })
                            if operation.contains(what)
                    ),
                    "the permanent, typed refusal — not a panic and not a version error: {err}",
                );
            }
            assert_eq!(
                store.get(&inode_key(1)).await.unwrap().unwrap(),
                encode(&prior),
                "…and nothing was written on the way to deciding it",
            );
        });
    }

    /// **A repoint of a FLAT map is charged against the per-record ceiling too.**
    ///
    /// The segmented arm has been charged since it was written; the flat arm — the older
    /// one, the path every pre-#635 evacuation and reconstruction repair takes — was not.
    /// It is the same hazard in the same shape: a repoint keeps the placement vector's
    /// length and not its **width** (a `DServerId` renders in 1 to 20 decimal digits), so
    /// moving a wide-EC chunk onto high-numbered servers grows the record, and a flat root
    /// sitting just under [`MAX_VALUE_BYTES`] is pushed past it. Handed to the backend that
    /// batch is a **permanent** rejection — no retry and no finer split can shrink one
    /// record — failing every future attempt at that repair, from a layer that cannot say
    /// which record was too big.
    ///
    /// Two controls in the same fixture, and the second is the reason the charge is on
    /// **growth** rather than on the record:
    ///
    /// * the same chunk moved to ids as narrow as the ones it had must still repoint — the
    ///   rule is about the bytes, not about repointing a full record;
    /// * a root that is **already** past the ceiling must still repoint. Such a record can
    ///   exist: `origin/main`'s `commit_chunk_map` writes a flat map of any size and redb,
    ///   TiKV and etcd store it, so an object over the FoundationDB-tightest ceiling lives
    ///   on any store that was never on FoundationDB. Refusing to repair it would turn a
    ///   record one backend would reject into an object no maintenance pass can ever fix.
    #[test]
    fn a_flat_repoint_that_would_grow_the_root_past_the_value_ceiling_is_refused() {
        let narrow: Vec<DServerId> = (0..3).collect();
        let wide: Vec<DServerId> = (0..3).map(|_| DServerId::MAX).collect();
        // A record eight bytes under the ceiling. The padding is a metadata field rather
        // than more chunks so the distance is exact rather than whatever the last whole
        // chunk left over — a fixture that lands the record 60 bytes under the ceiling
        // proves nothing about a 57-byte repoint.
        const SLACK: usize = 8;
        let mut prior = InodeRecord {
            size: 16,
            chunk_map: vec![
                ec_chunk(1, 8, narrow.clone()),
                ec_chunk(2, 8, narrow.clone()),
            ]
            .into(),
            state: InodeState::Committed,
            version: 4,
            content_type: Some(String::new()),
            ..Default::default()
        };
        let filler = MAX_VALUE_BYTES - encode(&prior).len() - SLACK;
        prior.content_type = Some("x".repeat(filler));
        let growth = wide
            .iter()
            .map(|id| id.to_string().len() + 1)
            .sum::<usize>()
            - narrow
                .iter()
                .map(|id| id.to_string().len() + 1)
                .sum::<usize>();
        assert_eq!(
            encode(&prior).len(),
            MAX_VALUE_BYTES - SLACK,
            "fixture: the record must be storable and one repoint must be enough to push it \
             over ({SLACK} bytes of headroom, {growth} bytes of growth)",
        );
        assert!(
            SLACK < growth,
            "fixture: {SLACK} bytes is not enough headroom"
        );

        let err = repoint_chunk(1, &prior, &ChunkHome::Root { position: 0 }, wide)
            .expect_err("a repoint that would grow the root past the ceiling must refuse");
        assert!(
            matches!(
                err.downcast_ref::<ChunkMapError>(),
                Some(ChunkMapError::ValueOverCeiling { key: rendered, ceiling, .. })
                    if rendered.starts_with("inode:") && *ceiling == MAX_VALUE_BYTES
            ),
            "the refusal names the record and the ceiling it passed, before anything is \
             written: {err}",
        );

        let batch = repoint_chunk(1, &prior, &ChunkHome::Root { position: 0 }, vec![2, 1, 0])
            .expect("a move that does not grow the record still repoints");
        assert_eq!(batch.puts[0].0, inode_key(1));
        assert!(batch.puts[0].1.len() <= MAX_VALUE_BYTES);

        // …and a root that was ALREADY over the ceiling is still repairable, as long as the
        // repair does not make it worse: the charge is on what this repoint adds.
        let mut legacy = prior.clone();
        legacy.content_type = Some("x".repeat(filler + MAX_VALUE_BYTES));
        assert!(
            encode(&legacy).len() > MAX_VALUE_BYTES,
            "fixture: the legacy record must really be over the ceiling",
        );
        let batch = repoint_chunk(1, &legacy, &ChunkHome::Root { position: 0 }, vec![2, 1, 0])
            .expect("a repoint that does not grow an over-ceiling record must still repair it");
        assert!(
            batch.puts[0].1.len() > MAX_VALUE_BYTES,
            "…and it really is the over-ceiling record that was repointed",
        );
        assert_eq!(
            repoint_chunk(
                1,
                &legacy,
                &ChunkHome::Root { position: 0 },
                (0..3).map(|_| DServerId::MAX).collect(),
            )
            .expect_err("…but growing it further is still refused")
            .downcast_ref::<ChunkMapError>()
            .map(std::mem::discriminant),
            Some(std::mem::discriminant(&ChunkMapError::ValueOverCeiling {
                key: String::new(),
                bytes: 0,
                ceiling: 0,
            })),
        );
    }

    /// **No committer in this module publishes a segmented root the staged publication did
    /// not write, and none puts an inode record its own decode would reject.**
    ///
    /// [`InodeRecord`]'s fields are public — the `Flat | Segmented` change had to stay a
    /// one-line edit at its ~150 construction sites — so both illegal records are
    /// *representable*, and this is where they are stopped: at the committers, which are the
    /// only path from a record in this module to the store.
    ///
    /// Two faults, one boundary:
    ///
    /// * a **segmented root offered to a create**. Its `seg:` records are written and
    ///   verified only by [`SegmentedPublication`], so a create that accepted the shape would
    ///   commit an object no consumer could ever resolve — and no rollback could reclaim,
    ///   because the root is live. Refused with
    ///   [`ChunkMapError::SegmentedPublicationBypassed`], and refused *before* the leased
    ///   create's lease read, so a permanent refusal is not reported as a retryable
    ///   `Conflict`.
    /// * a record whose **segment table does not span its `size`** — the invariant the decode
    ///   enforces (ADR-0045). Stored, it would be a poison record: unreadable by the gateway,
    ///   unresolvable by every pass, and un-overwritable, because each overwrite is
    ///   `require(key, encode(prior))` against a prior nobody can decode. One body enforces
    ///   it in both directions ([`InodeRecord::checked_shape`]), so the two can never
    ///   disagree about what is storable.
    #[test]
    fn no_committer_publishes_a_root_its_own_decode_would_reject() {
        /// The directory every create in this test binds its name under.
        const PARENT: InodeId = 0;
        let store = mem_store();
        let segmented = segmented_root_at(7, 1);
        block_on(async {
            // (a) the create paths refuse the segmented shape, and write nothing.
            for (what, outcome) in [
                ("create", create(&store, PARENT, "obj", 1, &segmented).await),
                (
                    "create_leased",
                    create_leased(&store, PARENT, "obj", 1, &segmented, &[], 0).await,
                ),
            ] {
                let err = outcome.expect_err("a create may not publish a segmented root");
                assert!(
                    matches!(
                        err.downcast_ref::<ChunkMapError>(),
                        Some(ChunkMapError::SegmentedPublicationBypassed {
                            operation: "create"
                        }),
                    ),
                    "{what}: the refusal must be the permanent, typed one: {err}",
                );
                assert_eq!(
                    store.get(&inode_key(1)).await.unwrap(),
                    None,
                    "{what}: …and nothing may be durable — not the root, and not the dirent",
                );
                assert_eq!(store.get(&dirent_key(PARENT, "obj")).await.unwrap(), None,);
            }
            // The flat control through the same door: an ordinary create still creates.
            assert_eq!(
                create(&store, PARENT, "flat", 2, &InodeRecord::new_empty())
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
        });

        // (b) a record whose table and size disagree is not encodable for a put — checked by
        // the same body the decode uses, so a value this module refuses to READ is one it
        // cannot WRITE.
        let mut poison = segmented.clone();
        poison.size += 1;
        assert_eq!(
            encode_inode(&poison).unwrap_err(),
            ChunkMapError::SizeSpanMismatch {
                size: poison.size,
                span: segmented.size,
            },
        );
        assert!(
            decode::<InodeRecord>(&encode(&poison)).is_err(),
            "fixture: those bytes really are unreadable — which is what makes writing them \
             unrepairable rather than merely wrong",
        );
        // …and for every record the decode accepts, the checked encoder is byte-for-byte
        // `encode`: the CAS shape (`require(key, encode(prior))`) depends on it.
        for record in [segmented, InodeRecord::new_empty()] {
            assert_eq!(encode_inode(&record).unwrap(), encode(&record));
        }
    }

    /// The **segment** packing boundary (`0016:220-223`): a segment is filled until the
    /// next chunk would take its encoded size PAST the target, so a chunk that fits
    /// exactly still shares the segment.
    #[test]
    fn a_chunk_that_exactly_fills_a_segment_still_shares_it() {
        let prior = InodeRecord::new_empty();
        let chunks = many_chunks(2);
        let pubn = publication(&prior, chunks);
        let one = encode(&chunk(ChunkId::from(1u64), 8, vec![0])).len() + 1;
        // Two chunks fit exactly at this target…
        let exact = SEGMENT_ENVELOPE_BYTES + 2 * one;
        assert_eq!(
            pubn.plan_with(exact, MAX_ROOT_SEGMENTS).unwrap().len(),
            1,
            "a chunk that fills the segment exactly must not spill into a new one",
        );
        // …and one byte less is a spill.
        assert_eq!(
            pubn.plan_with(exact - 1, MAX_ROOT_SEGMENTS).unwrap().len(),
            2,
            "one byte over the target starts the next segment",
        );
    }

    // ---- the retired arm, as every CONSUMER must see it ----

    /// Decision 7(h) says a maintenance pass "drops the stale resolution" — the stale
    /// **resolution**, not the object. A consumer that answered "no chunks" for a
    /// generation superseded under it would take a LIVE object out of GC's reference set,
    /// after which restore manufactures `orphan:` evidence for its fragments and the next
    /// GC pass past grace deletes them: #508-attempt-4's data loss in a new spelling. So
    /// the shared entry re-reads the root and resolves the generation that replaced it.
    #[test]
    fn a_retired_generation_resolves_against_the_root_that_replaced_it() {
        let store = mem_store();
        let stale_root = InodeRecord {
            size: 8,
            chunk_map: ChunkMap::Segmented(
                SegmentedMap::new(
                    group(7),
                    vec![SegmentRef {
                        index: 0,
                        byte_offset: 0,
                        byte_len: 8,
                    }],
                )
                .unwrap(),
            ),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        let live_chunks = vec![chunk(42, 8, vec![1])];
        block_on(async {
            // The store holds the generation that SUPERSEDED the caller's, and the
            // caller's segments are already drained.
            let current = InodeRecord {
                chunk_map: live_chunks.clone().into(),
                version: 3,
                ..stale_root.clone()
            };
            put(&store, inode_key(1), encode(&current)).await;

            let live = resolve_live_chunk_map(&store, &inode_key(1), &stale_root)
                .await
                .unwrap()
                .expect("a superseded generation is not an absent object");
            assert_eq!(
                live.chunks.as_ref(),
                live_chunks.as_slice(),
                "the consumer must see the LIVE generation's chunks, not none at all",
            );
            assert_eq!(
                live.restarted.as_ref().map(|r| r.version),
                Some(3),
                "…and the root it restarted against, which a repoint CAS must name",
            );

            // A genuinely deleted object is the one case that answers `None`.
            let empty = mem_store();
            assert!(resolve_live_chunk_map(&empty, &inode_key(1), &stale_root)
                .await
                .unwrap()
                .is_none());

            // …and the same for the homed variant, which reconstruction/rebalance use.
            let homes = resolve_live_chunk_homes(&store, &inode_key(1), &stale_root)
                .await
                .unwrap()
                .expect("superseded, not absent");
            assert_eq!(homes.chunks.len(), 1);
            assert_eq!(homes.restarted.map(|r| r.version), Some(3));
        });
    }

    /// A segmented root naming `group(epoch)` with one 8-byte segment.
    fn segmented_root_at(epoch: u64, version: u64) -> InodeRecord {
        InodeRecord {
            size: 8,
            chunk_map: ChunkMap::Segmented(
                SegmentedMap::new(
                    group(epoch),
                    vec![SegmentRef {
                        index: 0,
                        byte_offset: 0,
                        byte_len: 8,
                    }],
                )
                .unwrap(),
            ),
            state: InodeState::Committed,
            version,
            ..Default::default()
        }
    }

    /// A store whose root names a **different** generation on every read and whose
    /// `seg:` ranges are always empty — a publisher looping faster than the resolver can
    /// settle. Each restart therefore meets another retired generation.
    struct MovingRoot {
        reads: Mutex<u64>,
    }

    #[async_trait]
    impl MetadataStore for MovingRoot {
        async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
            let mut reads = self.reads.lock().unwrap();
            *reads += 1;
            Ok(Some(encode(&segmented_root_at(100 + *reads, *reads))))
        }

        async fn scan(&self, _prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            Ok(Vec::new())
        }

        // The required paginated read (#634): a test double needs *a* body, not a
        // backend's — the dev-only testkit helper pages over this store's own `scan`.
        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<wyrd_traits::ScanPage> {
            wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
        }

        async fn commit(&self, _batch: WriteBatch) -> Result<CommitOutcome> {
            Ok(CommitOutcome::Conflict)
        }
    }

    /// A resolve that keeps meeting a retired generation must **fail closed**, never
    /// eventually answer "this object owns no bytes" — the answer that takes a live
    /// object out of GC's reference set.
    #[test]
    fn a_root_that_keeps_being_retired_fails_closed_rather_than_answering_none() {
        let store = MovingRoot {
            reads: Mutex::new(0),
        };
        let held = segmented_root_at(7, 2);
        block_on(async {
            let err = resolve_live_chunk_map(&store, &inode_key(1), &held)
                .await
                .expect_err("an unstable resolve must fail closed, never answer None");
            assert_eq!(
                err.downcast_ref::<ChunkMapError>(),
                Some(&ChunkMapError::MapResolutionUnstable {
                    attempts: MAX_RESOLVE_RESTARTS
                }),
            );
        });
    }

    /// The id high-water scan is a `.chunk_map` consumer too, and it must see a
    /// **segmented** map's ids: a gateway that resumed its allocator below a live chunk id
    /// would re-mint it and clobber that object's fragments (issue #364). It must also
    /// keep ignoring the coordination-free ids at or above `2^64` (ADR-0019), which no
    /// in-process counter mints and which would otherwise pin the allocator at the top of
    /// the range forever.
    #[test]
    fn the_high_water_scan_sees_segmented_ids_and_ignores_the_out_of_range_ones() {
        let store = mem_store();
        let in_process = ChunkId::from(7u64);
        let ceiling: ChunkId = 1 << 64;
        block_on(async {
            // A segmented object whose chunk lives in a `seg:` record, plus an id at the
            // ceiling and one above it (a different gateway's coordination-free range).
            let root = segmented_root_at(7, 2);
            put(&store, inode_key(1), encode(&root)).await;
            // The segment carries an in-process id AND one exactly AT the ceiling: the
            // bound is exclusive, so the ceiling itself is out of range.
            put(
                &store,
                seg_key(&group(7), 0),
                encode(
                    &SegmentRecord::new(
                        vec![chunk(in_process, 4, vec![0]), chunk(ceiling, 4, vec![0])],
                        0,
                    )
                    .unwrap(),
                ),
            )
            .await;
            put(&store, pending_key(ceiling), Bytes::from_static(b"{}")).await;
            put(&store, pending_key(ceiling + 5), Bytes::from_static(b"{}")).await;
            put(
                &store,
                orphan_key(
                    0,
                    FragmentId {
                        chunk: ceiling + 9,
                        index: 0,
                    },
                ),
                Bytes::from_static(b"0"),
            )
            .await;

            let (max_inode, max_chunk) = high_water_marks(&store).await.unwrap();
            assert_eq!(max_inode, 1);
            assert_eq!(
                max_chunk, in_process,
                "the segmented map's chunk id counts, and only ids BELOW the \
                 coordination-free ceiling do",
            );

            // …and an in-process id in the `pending:` / `orphan:` ledgers counts too, so
            // a deleted-but-not-yet-reclaimed id is never re-minted.
            put(&store, pending_key(11), Bytes::from_static(b"{}")).await;
            put(
                &store,
                orphan_key(
                    0,
                    FragmentId {
                        chunk: 13,
                        index: 0,
                    },
                ),
                Bytes::from_static(b"0"),
            )
            .await;
            assert_eq!(high_water_marks(&store).await.unwrap().1, 13);
        });
    }

    /// **The id floor is total, and one damaged object cannot make it fail.**
    ///
    /// `Gateway::recover` calls this before the gateway serves anything
    /// (`crates/server/src/lib.rs:123-124`), so an `Err` here is not one object's problem:
    /// it is every *healthy* object in the store becoming unavailable because one root
    /// lost a segment. The floor has no safety need to resolve a root — it needs ids, and
    /// the ids are in the `seg:` records themselves — so a damaged object costs the floor
    /// nothing and costs availability nothing.
    ///
    /// Both halves are asserted, because totality alone would be satisfied by a floor of
    /// zero: the answer is `Ok`, **and** it is at or above every chunk id any `seg:`
    /// record in the store names, including the damaged object's readable segment. A
    /// floor that skipped the damaged object's ids would let the allocator re-mint one and
    /// clobber fragments that are still on disk (issue #364).
    #[test]
    fn the_id_floor_is_total_over_a_damaged_segmented_object() {
        let store = mem_store();
        block_on(async {
            // A healthy segmented object…
            put(&store, inode_key(1), encode(&segmented_root_at(7, 2))).await;
            put(
                &store,
                seg_key(&group(7), 0),
                encode(&SegmentRecord::new(vec![chunk(4, 8, vec![0])], 0).unwrap()),
            )
            .await;
            // …and a DAMAGED one: its root names two segments, and the second is absent.
            // Nothing can resolve this object's map — that is the point of the fixture.
            let damaged = InodeRecord {
                size: 16,
                chunk_map: ChunkMap::Segmented(
                    SegmentedMap::new(
                        group(9),
                        vec![
                            SegmentRef {
                                index: 0,
                                byte_offset: 0,
                                byte_len: 8,
                            },
                            SegmentRef {
                                index: 1,
                                byte_offset: 8,
                                byte_len: 8,
                            },
                        ],
                    )
                    .unwrap(),
                ),
                state: InodeState::Committed,
                version: 1,
                ..Default::default()
            };
            put(&store, inode_key(2), encode(&damaged)).await;
            put(
                &store,
                seg_key(&group(9), 0),
                encode(&SegmentRecord::new(vec![chunk(21, 8, vec![0])], 0).unwrap()),
            )
            .await;

            // The map itself is unresolvable — fail-closed, per object (decision 7(h)).
            assert!(resolve_chunk_map(&store, &inode_key(2), &damaged)
                .await
                .is_err());

            // The floor is nonetheless answered, and it is above the damaged object's
            // readable ids rather than below them.
            let (max_inode, max_chunk) = high_water_marks(&store)
                .await
                .expect("one damaged object may not fail the whole id floor");
            assert_eq!(max_inode, 2);
            assert_eq!(
                max_chunk, 21,
                "the floor covers every id in every seg: record, damaged object included",
            );

            // …and a `seg:` value that does not decode at all is contained the same way —
            // no refusal to start — but it may not cost the floor the ids it was NAMING
            // either. Nobody can read them, and a record whose ids nobody can read is a
            // record whose fragments are still on disk under ids the allocator would
            // otherwise be free to re-mint (issue #364). So the answer is the top of the
            // in-process range: total, attributed, and never below a live id.
            put(
                &store,
                seg_key(&group(9), 1),
                Bytes::from_static(b"not a segment record"),
            )
            .await;
            assert_eq!(
                high_water_marks(&store)
                    .await
                    .expect("an unreadable seg: value may not fail the id floor either")
                    .1,
                (1 << 64) - 1,
                "bytes that spell no id at all are ids nobody can account for, not ids that \
                 are not there: skipping them leaves the floor below live fragments",
            );
        });
    }

    /// **An unreadable record costs the floor its typed reading, not its ids.**
    ///
    /// The containment rule has two halves and they pull against each other: the floor
    /// must never fail (`Gateway::recover` calls it before the gateway serves anything,
    /// `crates/server/src/lib.rs:123-124`) and it must never sit **below** an id whose
    /// fragments are on disk (issue #364 — the allocator would re-mint it and clobber
    /// them). Skipping a record that does not decode buys the first by giving up the
    /// second: silently, at exactly the moment the store is already damaged.
    ///
    /// So a value this module cannot decode is attributed and read **leniently** for ids
    /// alone. Both classes are covered, because this slice widens what can fail to decode
    /// in both: a `seg:` record (a new record class) and an `inode:` root (whose segmented
    /// shape now enforces structural invariants at decode).
    #[test]
    fn the_id_floor_recovers_ids_from_records_that_do_not_decode() {
        let store = mem_store();
        block_on(async {
            // A structurally invalid segmented root: `segment_count` disagrees with the
            // table, so this slice's decode rejects it (parse-don't-validate) where the
            // base would have accepted the value. It names no chunk id of its own — its
            // job here is the totality half: one damaged root may not fail the floor.
            put(
                &store,
                inode_key(4),
                Bytes::from_static(
                    br#"{"size":8,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":9,"segments":[{"index":0,"byte_offset":0,"byte_len":8}]},"state":"Committed","version":1}"#,
                ),
            )
            .await;
            // An `inode:` value that decodes to nothing at all, whose flat map still spells
            // its chunk ids.
            put(
                &store,
                inode_key(5),
                Bytes::from_static(
                    br#"{"size":8,"chunk_map":[{"id":41,"scheme":"None","len":8,"placement":[1]}],"state":"NotAState","version":1}"#,
                ),
            )
            .await;
            assert!(
                decode::<InodeRecord>(&store.get(&inode_key(5)).await.unwrap().unwrap()).is_err(),
                "fixture: the value must really be undecodable, or this proves nothing",
            );
            // A `seg:` record whose chunks do not sum to its declared span — rejected at
            // decode, and its ids are live on disk all the same.
            put(
                &store,
                seg_key(&group(9), 0),
                Bytes::from_static(
                    br#"{"chunks":[{"id":57,"scheme":"None","len":8,"placement":[1]}],"byte_offset":0,"byte_len":99}"#,
                ),
            )
            .await;

            let (max_inode, max_chunk) = high_water_marks(&store)
                .await
                .expect("an unreadable record may not fail the floor the gateway starts from");
            assert_eq!(max_inode, 5, "the inode mark comes from the KEY, always");
            assert_eq!(
                max_chunk, 57,
                "every id an unreadable record's bytes still spell is at or below the \
                 floor — skipping them would re-mint live ids over live fragments",
            );

            // …and the recovery is bounded by the same in-process ceiling as the typed
            // path: a coordination-free id in an unreadable record is still not this
            // allocator's to resume from.
            put(
                &store,
                seg_key(&group(9), 1),
                Bytes::from(format!(
                    r#"{{"chunks":[{{"id":{},"scheme":"None","len":8,"placement":[1]}}],"byte_offset":0,"byte_len":99}}"#,
                    1u128 << 64
                )),
            )
            .await;
            assert_eq!(high_water_marks(&store).await.unwrap().1, 57);
        });
    }

    /// **A record whose ids nobody can read raises the floor; it does not leave it where it
    /// was.**
    ///
    /// The recovery above reads the ids a damaged value still *spells*, and for a value that
    /// parses — the common case this slice creates, since a structural invariant enforced at
    /// decode rejects records whose JSON is perfectly well formed — that is every id there
    /// is. For a value nobody can structure it is not: bytes naming ids 5 and 900 whose
    /// second token was destroyed read as 5, and a floor of 5 is a floor **below** 900,
    /// whose fragments are still on disk. The allocator would re-mint it (issue #364).
    ///
    /// So the reading reports whether it could account for every id, and one that could not
    /// contributes the top of the in-process range. Both walks are asserted — the `inode:`
    /// roots and the `seg:` records go through one helper, and a fix applied to one of them
    /// only is how this came back a second time — and so is the **control**: an ordinary
    /// structurally-invalid record still gets an exact floor, because a rule that escalated
    /// on every failed decode would park the allocator on a record it can read perfectly
    /// well.
    #[test]
    fn a_record_whose_ids_cannot_be_read_raises_the_floor_rather_than_lowering_it() {
        let top = (1u128 << 64) - 1;
        block_on(async {
            // (1) the `seg:` walk: a value whose second id token the damage cut short. The
            // first chunk's id reads as 5; the second was spelling an id of at least 90 and
            // nobody can say which — the arrangement a floor of "the largest id we managed
            // to read" gets wrong, and gets wrong silently.
            let store = mem_store();
            put(
                &store,
                seg_key(&group(9), 0),
                Bytes::from_static(br#"{"chunks":[{"id":5},{"id":90"#),
            )
            .await;
            assert_eq!(
                high_water_marks(&store)
                    .await
                    .expect("one unreadable record may not stop the gateway starting")
                    .1,
                top,
                "the floor must cover the id the bytes were spelling, not the one they \
                 happened to finish",
            );

            // (2) the `inode:` walk, through the same helper: bytes that are not text at
            // all, under a key whose inode id is still read.
            let store = mem_store();
            put(
                &store,
                inode_key(7),
                Bytes::from_static(&[0xFF, 0x00, 0xFE]),
            )
            .await;
            assert_eq!(
                high_water_marks(&store)
                    .await
                    .expect("…and neither may an unreadable root"),
                (7, top),
            );

            // (3) THE CONTROL. A record whose JSON parses and whose every id reads is
            // covered exactly, escalation or no escalation — this one fails `decode` only
            // because its declared span disagrees with its chunks.
            let store = mem_store();
            put(
                &store,
                seg_key(&group(9), 0),
                Bytes::from_static(
                    br#"{"chunks":[{"id":57,"scheme":"None","len":8,"placement":[1]}],"byte_offset":0,"byte_len":99}"#,
                ),
            )
            .await;
            assert_eq!(
                high_water_marks(&store).await.unwrap().1,
                57,
                "a reading that accounted for every id the record names is the floor: the \
                 conservative answer is for damage nobody can read, not for every record \
                 this module refuses to decode",
            );
        });
    }

    /// **The in-process id space ends *below* `2^64`, and both readers of the bound agree.**
    ///
    /// `2^64` is the first coordination-free id (`(inode << 64) | seq`, ADR-0019), not the
    /// last in-process one, so the comparison is strictly `<`. Both directions are
    /// load-bearing and both are one comparator away from a real fault:
    ///
    /// * counting `2^64` (or above) pins the floor at the top of the range, and the next
    ///   in-process allocation is a chunk id nothing can mint — a gateway that starts and
    ///   then cannot write;
    /// * skipping `2^64 - 1` under-approximates the floor at its very last value, and the
    ///   allocator re-mints an id whose fragments are on disk (issue #364).
    ///
    /// The pair is asserted at **every** reader of the bound — the flat map on the inode
    /// record, the `seg:` namespace walk, the lenient recovery for a record that does not
    /// decode, and the `pending:` and `orphan:` ledgers — because they are five separate
    /// comparisons and each one is its own way to lose the property.
    #[test]
    fn the_id_floor_is_exclusive_at_the_coordination_free_boundary() {
        let ceiling: ChunkId = 1 << 64;
        let last_in_process = ceiling - 1;
        block_on(async {
            // (1) The flat map on an `inode:` record.
            let store = mem_store();
            let flat = InodeRecord {
                size: 16,
                chunk_map: ChunkMap::Flat(vec![
                    chunk(last_in_process, 8, vec![0]),
                    chunk(ceiling, 8, vec![0]),
                ]),
                state: InodeState::Committed,
                version: 1,
                ..Default::default()
            };
            put(&store, inode_key(1), encode(&flat)).await;
            assert_eq!(
                high_water_marks(&store).await.unwrap().1,
                last_in_process,
                "a flat map's last in-process id counts and the first coordination-free \
                 id does not",
            );

            // (2) The `seg:` namespace walk.
            let store = mem_store();
            put(
                &store,
                seg_key(&group(7), 0),
                encode(
                    &SegmentRecord::new(
                        vec![
                            chunk(last_in_process, 8, vec![0]),
                            chunk(ceiling, 8, vec![0]),
                        ],
                        0,
                    )
                    .unwrap(),
                ),
            )
            .await;
            assert_eq!(high_water_marks(&store).await.unwrap().1, last_in_process);

            // (3) The lenient recovery for a record that does not decode.
            let store = mem_store();
            put(
                &store,
                seg_key(&group(7), 0),
                Bytes::from(format!(
                    r#"{{"chunks":[{{"id":{last_in_process}}},{{"id":{ceiling}}}],"byte_len":99}}"#
                )),
            )
            .await;
            assert_eq!(high_water_marks(&store).await.unwrap().1, last_in_process);

            // (4) The `pending:` ledger and (5) the `orphan:` grace ledger, which derive
            // their ids from the KEY. An orphan record is the longest-lived evidence that
            // an id's fragments are still on disk (the reader-safe grace window), so a
            // floor that counted the first coordination-free id here would pin the
            // allocator at the top of the range from the next restart onwards.
            let store = mem_store();
            put(
                &store,
                pending_key(last_in_process),
                Bytes::from_static(b"{}"),
            )
            .await;
            put(&store, pending_key(ceiling), Bytes::from_static(b"{}")).await;
            assert_eq!(high_water_marks(&store).await.unwrap().1, last_in_process);

            let store = mem_store();
            for chunk in [last_in_process, ceiling] {
                put(
                    &store,
                    orphan_key(0, FragmentId { chunk, index: 0 }),
                    Bytes::from_static(b"0"),
                )
                .await;
            }
            assert_eq!(high_water_marks(&store).await.unwrap().1, last_in_process);
        });
    }

    /// **The digit grammar the byte scanner uses is the whole one**, checked against
    /// `serde_json`'s own unescaping rather than argued in a comment.
    ///
    /// [`decimal_digit`] claims that a decimal digit is spelled literally or as
    /// `\u003X` and in no other way, and the id floor's promise never to *lose* an id
    /// rests on that claim: a spelling it did not know would read as no digit at all, and
    /// an id missing from the floor is one the allocator re-mints over live fragments. So
    /// every escape JSON has is put to it — the eight two-character ones and the whole
    /// `\u00XX` range — and the reader must agree with a real JSON parse about which of
    /// them are digits, both ways.
    #[test]
    fn every_digit_escape_reads_as_that_digit() {
        let mut spellings: Vec<String> =
            (0..=0xFFu32).map(|code| format!(r"\u{code:04x}")).collect();
        spellings.extend((0..=0xFFu32).map(|code| format!(r"\u{code:04X}")));
        spellings
            .extend([r#"\""#, r"\\", r"\/", r"\b", r"\f", r"\n", r"\r", r"\t"].map(str::to_string));
        spellings.extend((0..=9u32).map(|digit| digit.to_string()));
        for spelling in spellings {
            // What a JSON parser makes of this token, if anything: the oracle.
            let parsed: Option<String> =
                serde_json::from_str::<String>(&format!(r#""{spelling}""#)).ok();
            let expected = parsed
                .as_deref()
                .filter(|text| text.len() == 1 && text.as_bytes()[0].is_ascii_digit())
                .map(|text| (text.as_bytes()[0], spelling.len()));
            assert_eq!(
                decimal_digit(spelling.as_bytes()),
                expected,
                "{spelling:?}: the scanner and a real JSON parse must agree about whether \
                 this spells a digit — a spelling the scanner cannot read is an id it \
                 cannot recover",
            );
        }
    }

    /// The lenient id recovery reads **fields**, folds a maximum, and never guesses: a
    /// number too wide for a chunk id, a `"id"` that is not a field, and a field with no
    /// number are all skipped rather than counted as something else. Over-counting is safe
    /// for an allocator floor; inventing a value is not.
    #[test]
    fn the_lenient_id_recovery_reads_fields_and_skips_what_it_cannot_read() {
        let ceiling: ChunkId = 1 << 64;
        assert_eq!(raw_chunk_id_floor(b"", ceiling).floor, 0);
        assert_eq!(raw_chunk_id_floor(br#"{"id":7,"id":3}"#, ceiling).floor, 7);
        assert_eq!(
            raw_chunk_id_floor(br#"{"id": 12 ,"id":9}"#, ceiling).floor,
            12
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":5,"truncated":[{"id":"#, ceiling).floor,
            5,
            "a field whose number never arrived is skipped, not read as zero",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":4,"nested":{"identity":99}}"#, ceiling).floor,
            4,
            "`\"id\"` must be a field: a longer key that merely starts with it is not one",
        );
        assert_eq!(
            raw_chunk_id_floor(
                br#"{"id":6,"id":999999999999999999999999999999999999999999999999}"#,
                ceiling,
            )
            .floor,
            6,
            "a number past the width of a chunk id is not one",
        );
    }

    /// **A reading that cannot account for every id the record names contributes the top of
    /// the in-process range — the floor may not rest on what the damage happened to leave
    /// legible.**
    ///
    /// This is the property that survived four rounds of getting a *completion rule* wrong.
    /// While a truncated `"id":18` was answered by arithmetic over its surviving digits, the
    /// arithmetic and the assertions that spelled it back agreed with each other and were a
    /// factor of ten below live ids; and a token nobody could read at all
    /// (`"id":<binary rubbish>`) contributed **nothing**, so a store holding it resumed its
    /// allocator below ids whose fragments are on disk (issue #364). Both are the same
    /// mistake — treating "what we could read" as "what is there".
    ///
    /// So the rule needs no arithmetic and no completion: an id the reading could not
    /// account for makes the record's whole reading incomplete
    /// ([`RecoveredIds::complete`]), and an incomplete reading contributes `ceiling - 1`
    /// ([`RecoveredIds::contribution`]) — at or above every id the record could possibly
    /// name. The oracle here is the definition itself: for every arrangement below, the
    /// contribution must be **at or above** the largest in-process id the bytes could have
    /// been spelling.
    #[test]
    fn an_incomplete_reading_contributes_the_top_of_the_in_process_range() {
        let ceiling: ChunkId = 1 << 64;
        let top = ceiling - 1;
        for (what, value, could_name) in [
            (
                "a token cut short by the end of the value: `12` may have been 12, or \
                 12_999_999_999_999_999_999, or anything between",
                br#"{"chunks":[{"id":12"#.as_slice(),
                12_999_999_999_999_999_999,
            ),
            (
                "…and digits that are a prefix of the CEILING's own are the worst case: \
                 every id from 18000…0 to 2^64-1 starts with `18`",
                br#"{"chunks":[{"id":18"#.as_slice(),
                top,
            ),
            (
                "a token cut short inside a digit's escape is a torn digit, not the id 12",
                br#"{"chunks":[{"id":"12\u003"#.as_slice(),
                12_999_999_999_999_999_999,
            ),
            (
                "an id field naming nothing a reader can turn into a number",
                br#"{"id":null,"x":[{"id":{}}]}"#.as_slice(),
                top,
            ),
            (
                "bytes that are not JSON and name no id at all — the record's ids are gone, \
                 not absent",
                b"not a segment record".as_slice(),
                top,
            ),
            (
                "…including bytes that are not text at all",
                &[0xFF, 0x00, 0xFE, 0x01],
                top,
            ),
            (
                "a duplicate key the parse collapses whose earlier copy no reader can turn \
                 into a number — the parse sees only the readable 3 and calls itself \
                 complete; the scan is what breaks that claim",
                br#"{"id":null,"id":3}"#.as_slice(),
                top,
            ),
        ] {
            let recovered = raw_chunk_id_floor(value, ceiling);
            assert!(
                !recovered.complete,
                "{what}: a reading that could not account for every id must say so",
            );
            assert_eq!(
                recovered.contribution(ceiling, ClassIds::Optional),
                top,
                "{what}: the contribution must cover every id the record could name (at \
                 least {could_name} here), and with no evidence to narrow it the only \
                 honest answer is the top of the in-process range",
            );
            assert!(
                recovered.contribution(ceiling, ClassIds::Optional) >= could_name,
                "{what}: …and it must never sit below one",
            );
        }

        // The bound in the other direction, asserted so the escalation cannot quietly grow
        // into the cluster's space: `2^64` is the FIRST coordination-free id, and those ids
        // are not this allocator's to resume from (ADR-0019).
        assert!(
            raw_chunk_id_floor(b"not a record", ceiling).contribution(ceiling, ClassIds::Optional)
                < ceiling,
            "the widest answer is still an in-process id",
        );
    }

    /// **What the byte scan reads WRONG, the parse has to correct — in both directions.**
    ///
    /// The pair of readers is not "one exact reader plus a fallback": the scan is a *lenient*
    /// reader by construction (it folds whatever decimal follows an `id` field), so on some
    /// shapes it does not merely miss an id, it reads a **different** one. Only a real parse
    /// can say what the value actually spells, and each direction of the disagreement is a
    /// live hazard:
    ///
    /// * a **fraction** where an id belongs (`"id":1.5`) — the scan reads `1` and calls it
    ///   readable, so a floor resting on the scan alone would sit at 1 for a record whose ids
    ///   nobody can read. The parse counts it unreadable, which makes the whole reading
    ///   incomplete and the contribution the top of the in-process range;
    /// * a **cluster-space** id spelled as a string (`"id":"18446744073709551620"`) — read
    ///   exactly, and *outside* this floor's population (ADR-0019). Taking it for an
    ///   in-process id would park the in-process allocator above its own range.
    ///
    /// Both fixtures put the id where a chunk sits — inside an array — because that is where
    /// every real record carries it, and a walk that does not descend into arrays sees neither.
    #[test]
    fn the_parsed_id_reader_corrects_what_the_byte_scan_reads_differently() {
        let ceiling: ChunkId = 1 << 64;
        let fraction = raw_chunk_id_floor(br#"{"chunks":[{"id":1.5,"len":8}]}"#, ceiling);
        assert_eq!(
            fraction,
            RecoveredIds {
                // The scan's lenient reading of the digits before the point: kept, because the
                // floor may over-approximate, and worthless on its own.
                floor: 1,
                unreadable: 1,
                found: 1,
                complete: false,
            },
            "a fraction is no chunk id: the reading that counted it is the only one that can \
             stop the floor resting on the digits before the point",
        );
        assert_eq!(
            fraction.contribution(ceiling, ClassIds::Optional),
            ceiling - 1,
            "…so the record contributes the top of the in-process range, not 1",
        );
        assert_eq!(
            raw_chunk_id_floor(
                br#"{"chunks":[{"id":"18446744073709551620","len":8}]}"#,
                ceiling,
            ),
            RecoveredIds {
                floor: 0,
                unreadable: 0,
                found: 1,
                complete: true,
            },
            "a coordination-free id reads exactly and is outside this floor's population: not \
             a miss, not an in-process id, and not a reason to raise the floor",
        );
    }

    /// **A value that names no id at all is a LOSS for a record class whose content is a
    /// chunk list — and an ordinary state for one whose content is not.**
    ///
    /// The reading's own `complete` flag counts the id *tokens* it could not read, which is
    /// the only loss a token can show. `{}` under a `seg:` key shows none: it is perfect
    /// JSON, it names no `id`, and it therefore reads as a **complete** reading with an empty
    /// answer — contributing **zero** while the chunk ids that record was holding sit on disk
    /// as fragments, which is precisely the re-mint that clobbers them (issue #364). A
    /// `seg:` record cannot legitimately name no chunk id (an empty one is refused at decode),
    /// so the answer for that class is the conservative one.
    ///
    /// The two controls are what keep it from being a blunt "damaged means park the
    /// allocator", which would cost a store with one damaged root the whole in-process id
    /// range:
    ///
    /// * a damaged **root** that names no chunk id is an ordinary root — an empty flat map,
    ///   or a segmented one whose ids live in its `seg:` records — so it contributes what it
    ///   read;
    /// * a damaged **`seg:`** record whose ids are the cluster's coordination-free ones read
    ///   perfectly well; they are simply above this floor's range, and a class minimum keyed
    ///   on the *in-process* floor rather than on ids **found** would call that record's list
    ///   destroyed.
    #[test]
    fn a_class_that_must_name_a_chunk_id_is_not_certified_by_a_value_that_names_none() {
        let ceiling: ChunkId = 1 << 64;
        let top = ceiling - 1;

        let empty = raw_chunk_id_floor(b"{}", ceiling);
        assert_eq!(
            empty,
            RecoveredIds {
                floor: 0,
                unreadable: 0,
                found: 0,
                complete: true,
            },
            "the reading itself is honest: nothing unreadable, and nothing found",
        );
        assert_eq!(
            empty.contribution(ceiling, ClassIds::Required),
            top,
            "a `seg:` value naming no id has LOST its chunk list — its ids must still be \
             covered, or the allocator re-mints over live fragments",
        );
        assert_eq!(
            empty.contribution(ceiling, ClassIds::Optional),
            0,
            "…while a root naming no chunk id is an ordinary root, and parking the allocator \
             for one would be the store-wide cost this containment exists to avoid",
        );

        // End to end, over the walk that decides the class: a healthy flat object names
        // chunk 9, and the damaged `seg:` value is the only other thing in the store.
        let store = mem_store();
        block_on(async {
            put(
                &store,
                inode_key(1),
                encode(&InodeRecord {
                    size: 8,
                    chunk_map: vec![chunk(9, 8, vec![0])].into(),
                    state: InodeState::Committed,
                    version: 1,
                    ..Default::default()
                }),
            )
            .await;
            put(&store, seg_key(&group(7), 0), Bytes::from_static(b"{}")).await;
            assert!(
                decode::<SegmentRecord>(b"{}").is_err(),
                "fixture: the seeded `seg:` value must genuinely fail to decode",
            );
            assert_eq!(
                high_water_marks(&store).await.unwrap(),
                (1, top),
                "a `seg:` record whose chunk list is gone must not leave the floor at the \
                 ids the rest of the store happens to name",
            );
        });

        // Control 1: the same walk over a `seg:` record that did not decode but whose ids
        // read perfectly well — above the in-process ceiling, so outside this floor.
        let store = mem_store();
        block_on(async {
            put(
                &store,
                inode_key(1),
                encode(&InodeRecord {
                    size: 8,
                    chunk_map: vec![chunk(9, 8, vec![0])].into(),
                    state: InodeState::Committed,
                    version: 1,
                    ..Default::default()
                }),
            )
            .await;
            let cluster_ids = br#"{"chunks":[{"id":"18446744073709551620","len":8}]}"#;
            assert!(
                decode::<SegmentRecord>(cluster_ids).is_err(),
                "fixture: the seeded `seg:` value must genuinely fail to decode",
            );
            put(
                &store,
                seg_key(&group(7), 0),
                Bytes::copy_from_slice(cluster_ids),
            )
            .await;
            assert_eq!(
                high_water_marks(&store).await.unwrap(),
                (1, 9),
                "a damaged record whose ids were READ is covered by what it names; only a \
                 record whose ids nobody can see is covered conservatively",
            );
        });

        // Control 2: a damaged ROOT that names no chunk id. It claims the segmented shape
        // and its table disagrees with `size`, so it fails to decode — and its ids, if it
        // has any, are in `seg:` records the walk covers without it.
        let store = mem_store();
        block_on(async {
            put(
                &store,
                inode_key(1),
                encode(&InodeRecord {
                    size: 8,
                    chunk_map: vec![chunk(9, 8, vec![0])].into(),
                    state: InodeState::Committed,
                    version: 1,
                    ..Default::default()
                }),
            )
            .await;
            let damaged_root = format!(
                r#"{{"size":99,"chunk_map":{},"state":"Committed","version":1}}"#,
                serde_json::to_string(
                    &SegmentedMap::new(
                        group(7),
                        vec![SegmentRef {
                            index: 0,
                            byte_offset: 0,
                            byte_len: 16,
                        }],
                    )
                    .unwrap()
                )
                .unwrap(),
            );
            assert!(
                decode::<InodeRecord>(damaged_root.as_bytes()).is_err(),
                "fixture: the seeded root must genuinely fail to decode",
            );
            put(&store, inode_key(2), damaged_root).await;
            assert_eq!(
                high_water_marks(&store).await.unwrap(),
                (2, 9),
                "one damaged root may not park the in-process chunk allocator at the top of \
                 its range: naming no chunk id is an ordinary state for a root",
            );
        });
    }

    /// **An `id` the record still spells in a shape the byte scan alone cannot read is
    /// recovered, not lost.**
    ///
    /// The floor may over-approximate and may not under-approximate: a floor below an id
    /// whose fragments are on disk is the re-mint hazard (issue #364), and the allocator
    /// has no second line of defence. Three shapes that a "digits straight after the colon"
    /// scan reads as **zero**, each of which is a live id in the bytes:
    ///
    /// * a **quoted** id — `{"id":"57"}` is 57, not nothing;
    /// * an id in a record whose JSON is otherwise fine but whose *structure* this module
    ///   rejects, nested wherever the shape puts it;
    /// * a **truncated** id — the digits ran out with the value, so the id was *at least*
    ///   what is left of it and may have been anything longer. It is an id nobody could
    ///   read, which makes the reading incomplete rather than smaller
    ///   (`an_incomplete_reading_contributes_the_top_of_the_in_process_range` is the other
    ///   half of this pair).
    #[test]
    fn the_id_recovery_reads_every_shape_the_bytes_still_spell() {
        let ceiling: ChunkId = 1 << 64;
        assert_eq!(
            raw_chunk_id_floor(br#"{"chunks":[{"id":"57","len":8}]}"#, ceiling),
            RecoveredIds {
                floor: 57,
                unreadable: 0,
                found: 1,
                complete: true,
            },
            "a quoted id is an id: reading it as zero under-reports the floor — and a value \
             that parses, all of whose ids were read, is covered EXACTLY, without the \
             conservative escalation an unreadable one gets",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"a":{"b":[{"c":{"id":41}}]},"id":"9"}"#, ceiling),
            RecoveredIds {
                floor: 41,
                unreadable: 0,
                found: 2,
                complete: true,
            },
            "an id nested anywhere in the value counts, and the walk that found it saw the \
             whole value",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"chunks":[{"id":12"#, ceiling),
            RecoveredIds {
                floor: 0,
                unreadable: 1,
                found: 0,
                complete: false,
            },
            "a truncated id is not the digits that survived it: it is an id nobody could \
             read, counted as such",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":null,"x":[{"id":{}}]}"#, ceiling),
            RecoveredIds {
                floor: 0,
                unreadable: 2,
                found: 0,
                complete: false,
            },
            "an id field naming no number at all is attributed, never silently skipped",
        );
    }

    /// **Each of the two readers is load-bearing on its own**, so each is asserted on
    /// input the *other* cannot read.
    ///
    /// The parse reads a JSON value exactly, wherever the shape puts an `id` and however it
    /// is spelled. The scan sees ids the parse **discards**: duplicate object keys collapse
    /// to the last one in a parsed value. Take either away and the floor drops **below** an
    /// id whose fragments are on disk, which is the re-mint hazard (issue #364) — so the
    /// pair is asserted, not their sum.
    ///
    /// **And neither reader may be blind to a JSON escape**, because the two blind spots
    /// compose: `"\u0069d"` is the same field name as `"id"`, so a record can carry both,
    /// the parse keeps only the last of them, and a scanner matching literal bytes sees only
    /// that same last one. An id in the *first* spelling is then invisible to both — the one
    /// arrangement of bytes that defeats the pair, and the reason the scan compares what a
    /// token unescapes to rather than its bytes.
    #[test]
    fn each_id_reader_recovers_what_the_other_cannot() {
        let ceiling: ChunkId = 1 << 64;
        // --- an escaped field name is the same field, to BOTH readers -----------------
        assert_eq!(
            raw_chunk_id_floor(br#"{"chunks":[{"\u0069d":57,"len":8}]}"#, ceiling).floor,
            57,
            "an escaped id, inside an array the walk has to descend",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"chunks":[{"\u0069d":"58"}]}"#, ceiling).floor,
            58,
            "…and the same id spelled as a string",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"\u0069d":"18446744073709551616"}"#, ceiling).floor,
            0,
            "the in-process bound is exclusive here too: the first coordination-free id is \
             not this allocator's to resume from, and a floor pinned there would park it at \
             the top of its range for ever",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"\u0069d":null}"#, ceiling),
            RecoveredIds {
                floor: 0,
                unreadable: 1,
                found: 0,
                complete: false,
            },
            "an id field the parse cannot read is counted, so the operator sees what the \
             floor is not covering — and the reading is not complete, so the record is \
             covered conservatively rather than left at zero",
        );

        // --- the scan's own ground: what a parsed value throws away --------------------
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":77,"id":3}"#, ceiling).floor,
            77,
            "a duplicate key collapses to the last one in a parsed value; the larger id is \
             still in the bytes and still names fragments on disk",
        );

        // --- the arrangement that defeats a LITERAL-bytes scan and the parse together ---
        for (what, value) in [
            ("a bare number", br#"{"\u0069d":999,"id":3}"#.as_slice()),
            ("a quoted one", br#"{"\u0069d":"999","id":3}"#.as_slice()),
            (
                "both characters escaped, the widest spelling there is",
                br#"{"\u0069\u0064":999,"id":3}"#.as_slice(),
            ),
            (
                "the escaped duplicate nested where a chunk sits",
                br#"{"chunks":[{"\u0069d":999,"id":3,"len":8}]}"#.as_slice(),
            ),
        ] {
            assert_eq!(
                raw_chunk_id_floor(value, ceiling).floor,
                999,
                "{what}: the parse keeps the LAST duplicate (3) and a scan matching the four \
                 bytes `\"id\"` finds only that same last one, so 999 — an id whose fragments \
                 are on disk — would be hidden from the allocator floor by both readers at \
                 once, and the next restart would re-mint it (issue #364)",
            );
        }

        // --- the scan's attribution, on bytes that are not JSON at all -----------------
        for (what, value, expected) in [
            (
                "an id field with no number after it",
                br#"{"id":}"#.as_slice(),
                RecoveredIds {
                    floor: 0,
                    unreadable: 1,
                    found: 0,
                    complete: false,
                },
            ),
            (
                "a number too wide for any chunk id",
                br#"{"id":99999999999999999999999999999999999999999999999999,}"#.as_slice(),
                RecoveredIds {
                    floor: 0,
                    unreadable: 1,
                    found: 0,
                    complete: false,
                },
            ),
            (
                "a token the value cut short: the digits that survived are not the id",
                br#"{"id":99999999999999999999"#.as_slice(),
                RecoveredIds {
                    floor: 0,
                    unreadable: 1,
                    found: 0,
                    complete: false,
                },
            ),
        ] {
            assert_eq!(
                raw_chunk_id_floor(value, ceiling),
                expected,
                "{what}: unreadable, and counted as such",
            );
        }

        // …and the other half of that count: an id READ perfectly well, at or above the
        // in-process ceiling, is not a miss and is not reported as one. Those ids are the
        // cluster's coordination-free space, which this floor never covers, and an alarm
        // about them would point an operator at a record that is fine. In a value that
        // **parses**, that reading is also complete — so the record contributes nothing at
        // all, and the conservative escalation stays where it belongs.
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":18446744073709551616}"#, ceiling),
            RecoveredIds {
                floor: 0,
                unreadable: 0,
                // READ, and above the ceiling: not a miss, and not "this value names no id"
                // either — the distinction `RecoveredIds::covers` turns on.
                found: 1,
                complete: true,
            },
            "a complete id past the ceiling, in a value that parses: outside this floor's \
             population, not unreadable, and not a reason to raise the floor",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":18446744073709551616,}"#, ceiling),
            RecoveredIds {
                floor: 0,
                unreadable: 0,
                found: 1,
                complete: false,
            },
            "…and the same id in bytes that are NOT JSON reads just as well, but the scan \
             that read it cannot say whether the damage hid another field, so the reading \
             is not complete",
        );

        // --- a value NO byte window could have lexed: padding, and escaped digits -------
        // The bytes are not JSON (the trailing comma), so the parse contributes nothing
        // and the scan is the only reader — and the id it must find is spelled the widest
        // way JSON allows: insignificant zeros, each escaped to six bytes, in front of an
        // id whose fragments are on disk. A reader that lexed the value inside a fixed
        // byte window found no closing quote, fell back to bare digits, met a backslash
        // and reported NOTHING — an id missing from the floor is one the allocator
        // re-mints over live fragments (issue #364).
        // `\u0030` is the escaped spelling of `0`: six bytes of padding per digit, so a
        // token can be arbitrarily wider than any window a lexer might allow itself. The
        // fixture asserts the spelling itself, because a padding string that quietly lost
        // its backslashes would leave both legs testing the same literal bytes.
        let padded = |zeros: usize, pad: &str, id: &str| {
            format!(r#"{{"id":"{}{id}",}}"#, pad.repeat(zeros)).into_bytes()
        };
        for (what, pad) in [("escaped", r"\u0030"), ("literal", "0")] {
            assert_eq!(
                serde_json::from_str::<String>(&format!(r#""{pad}""#)).unwrap(),
                "0",
                "fixture: {what} padding must actually spell a zero to a JSON parser",
            );
            for zeros in [1usize, 8, 64, 512] {
                assert_eq!(
                    raw_chunk_id_floor(&padded(zeros, pad, "18446744073709551615"), ceiling).floor,
                    18_446_744_073_709_551_615,
                    "{zeros} {what} leading zeros must not hide an id from the floor",
                );
            }
            assert_eq!(
                raw_chunk_id_floor(&padded(600, pad, ""), ceiling),
                RecoveredIds {
                    floor: 0,
                    unreadable: 0,
                    // The id zero was READ — worth nothing to the floor, and still an id
                    // this value carries.
                    found: 1,
                    complete: false,
                },
                "{what} padding and nothing else is the id ZERO — readable, worth nothing \
                 to the floor, and not something to report as unreadable (the reading is \
                 still not complete: these bytes are not JSON, so no reader saw the whole \
                 field set)",
            );
            // …but padding the value ran out of mid-way lost the digits that follow it, so
            // it is an id nobody could read: counted, and NOT guessed at — no id renders
            // with a leading zero, so there is nothing here even to complete.
            let truncated = format!(r#"{{"id":"{}"#, pad.repeat(8)).into_bytes();
            assert_eq!(
                raw_chunk_id_floor(&truncated, ceiling),
                RecoveredIds {
                    floor: 0,
                    unreadable: 1,
                    found: 0,
                    complete: false,
                },
                "{what} padding cut short is an id this reader could not recover, and the \
                 operator has to be told rather than shown a floor of zero",
            );
        }
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":"\u0035\u0037",}"#, ceiling).floor,
            57,
            "…and the significant digits may be escaped too",
        );
        assert_eq!(
            raw_chunk_id_floor(br#"{"id":"\u00305\u0037",}"#, ceiling).floor,
            57,
            "…in any mixture with literal ones, behind mixed padding",
        );
        // A value cut short **inside** a digit escape is a truncated DIGIT, so the token is
        // truncated exactly as a run of literal digits cut short is. Reading it as the
        // digits already seen reports an id BELOW the one the bytes were spelling, and a
        // floor below a live id is the re-mint that clobbers its fragments — so it is an
        // id nobody could read, and the record is covered conservatively for it.
        for torn in [r"\", r"\u", r"\u0", r"\u00", r"\u003"] {
            let value = format!(r#"{{"id":"12{torn}"#).into_bytes();
            let recovered = raw_chunk_id_floor(&value, ceiling);
            assert_eq!(
                recovered,
                RecoveredIds {
                    floor: 0,
                    unreadable: 1,
                    found: 0,
                    complete: false,
                },
                "a token ending in the half-written escape {torn:?} is truncated, not the \
                 id 12",
            );
            assert_eq!(
                recovered.contribution(ceiling, ClassIds::Optional),
                ceiling - 1,
                "…and what it contributes covers every id it could have been",
            );
        }
    }

    /// **The `seg:` namespace is walked in PAGES, and the walk does not stop at the first
    /// one.** The floor's whole reason to page rather than scan is that `scan` is
    /// complete-or-fail-loud at `SCAN_CAP` while `seg:` is the namespace one object can put
    /// [`MAX_ROOT_SEGMENTS`] records into — but a paged walk that never advanced its cursor
    /// would silently answer from the first page alone, which is an id floor *below* live
    /// ids: the re-mint hazard the whole function exists to prevent (issue #364).
    ///
    /// So the fixture is deliberately wider than one page and the largest id is in the
    /// **last** record: the assertion can only hold if the cursor advanced.
    #[test]
    fn the_id_floor_pages_through_the_whole_segment_namespace() {
        let store = mem_store();
        let records = RECORD_FLOOR_PAGE * 2 + 3;
        block_on(async {
            for index in 0..records {
                put(
                    &store,
                    seg_key(&group(7), index as u32),
                    encode(
                        &SegmentRecord::new(
                            vec![chunk(ChunkId::from(index as u64 + 1), 8, vec![0])],
                            index as u64 * 8,
                        )
                        .unwrap(),
                    ),
                )
                .await;
            }
            assert_eq!(
                high_water_marks(&store).await.unwrap().1,
                ChunkId::from(records as u64),
                "the floor stopped at a page boundary instead of walking the namespace",
            );
        });
    }

    /// **Totality is not only about a damaged record: a store simply too LARGE to `scan`
    /// may not stop the gateway either.**
    ///
    /// `MetadataStore::scan` is complete-or-fail-loud at `SCAN_CAP`
    /// (`crates/traits/src/lib.rs:1019-1023`), so every walk this function makes over a
    /// namespace that can grow — `inode:`, `seg:`, `pending:`, `orphan:` — is a way for a
    /// big store to make `Gateway::recover` return `Err` before the gateway serves anything
    /// (`crates/server/src/lib.rs:123-124`). That is the same blast radius the containment
    /// rule was written against, arriving through the size of the store rather than through
    /// one bad value: every healthy object unavailable. And it is not hypothetical at the
    /// stated ceilings — one maximum segmented retirement installs ~1.78 M orphan marks
    /// against a cap of 1 048 576 (`crates/traits/src/lib.rs:1030-1036`).
    ///
    /// So the fixture *includes the fault*: a store whose scan cap is 1, with more than one
    /// row in each of the four namespaces. A `scan` of any of them fails; the floor must
    /// still be exact.
    #[test]
    fn the_id_floor_is_total_over_a_store_too_large_to_scan() {
        let store = RedbMetadataStore::in_memory().unwrap().with_scan_cap(1);
        block_on(async {
            for id in [1u64, 2] {
                let record = InodeRecord {
                    size: 8,
                    chunk_map: ChunkMap::Flat(vec![chunk(ChunkId::from(10 + id), 8, vec![0])]),
                    state: InodeState::Committed,
                    version: 1,
                    ..Default::default()
                };
                put(&store, inode_key(id), encode(&record)).await;
            }
            for index in 0..2u32 {
                put(
                    &store,
                    seg_key(&group(7), index),
                    encode(
                        &SegmentRecord::new(
                            vec![chunk(ChunkId::from(20 + u64::from(index)), 8, vec![0])],
                            u64::from(index) * 8,
                        )
                        .unwrap(),
                    ),
                )
                .await;
            }
            for id in [30u128, 31] {
                put(&store, pending_key(id), Bytes::from_static(b"{}")).await;
            }
            for chunk_id in [40u128, 41] {
                put(
                    &store,
                    orphan_key(
                        0,
                        FragmentId {
                            chunk: chunk_id,
                            index: 0,
                        },
                    ),
                    Bytes::from_static(b"0"),
                )
                .await;
            }

            // The fault is in the fixture, not asserted around: every one of these four
            // walks would have failed as a `scan`.
            for prefix in [b"inode:".as_slice(), SEG_PREFIX, b"pending:", ORPHAN_PREFIX] {
                assert!(
                    store.scan(prefix).await.is_err(),
                    "fixture: a scan of {} must breach the cap, or this test proves nothing",
                    String::from_utf8_lossy(prefix),
                );
            }

            assert_eq!(
                high_water_marks(&store).await.unwrap(),
                (2, 41),
                "the id floor must be total over a store no scan can read, and exact: \
                 the largest inode id and the largest chunk id across all four namespaces",
            );
        });
    }

    /// The group-adoption predicate the `seggrp:` marker's two-arm lifetime rule gates on
    /// (`0016:513-527`) — one bounded range read, no session machinery.
    ///
    /// **Bounded by a page, not by the range's size**, and the fixture includes the fault
    /// that distinguishes the two: the predicate's prefix spans *every epoch this group
    /// ever attempted*, so a session that crashed and retried often enough leaves a range
    /// past `SCAN_CAP`, where `scan` answers `ScanCapExceeded` instead of "yes"
    /// (`crates/traits/src/lib.rs:1019-1023`). That error would land on the terminal delete
    /// this predicate gates — leaving the marker unreclaimable for ever, and the more
    /// rubbish the group had accumulated, the less able the session would be to clear it.
    #[test]
    fn segment_group_adoption_is_one_bounded_range_read() {
        let store = RedbMetadataStore::in_memory().unwrap().with_scan_cap(1);
        block_on(async {
            assert!(!segment_group_adopted(&store, NONCE).await.unwrap());
            for epoch in 3..6u64 {
                put(
                    &store,
                    seg_key(&group(epoch), 0),
                    encode(&SegmentRecord::new(vec![chunk(1, 8, vec![0])], 0).unwrap()),
                )
                .await;
            }
            assert!(
                store.scan(&seg_group_prefix(NONCE)).await.is_err(),
                "fixture: the group's whole-range `scan` must breach the cap, or this test \
                 proves nothing about what the predicate reads instead",
            );
            assert!(
                segment_group_adopted(&store, NONCE).await.unwrap(),
                "a group ANY of whose epochs has a segment is adopted, so the session \
                 teardown must leave its marker standing — and an accumulated range may \
                 not turn the predicate into an error that strands the marker for ever",
            );
        });
    }

    /// The other half of the `seggrp:` marker's rule: the **reservation** that mints a
    /// nonce, which is a mutation the session's Create batch carries rather than a
    /// transaction of its own (`0016:502-527`).
    #[test]
    fn a_group_reservation_is_a_mutation_the_minting_batch_carries() {
        let store = mem_store();
        block_on(async {
            assert_eq!(
                store
                    .commit(reserve_segment_group(NONCE).unwrap())
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
            assert_eq!(
                store.get(&seggrp_key(NONCE)).await.unwrap().as_deref(),
                Some(SEGGRP_MARKER),
                "the marker's PRESENCE is its whole meaning, so its value stays the empty \
                 object a later slice can CAS on",
            );
            assert_eq!(
                store
                    .commit(reserve_segment_group(NONCE).unwrap())
                    .await
                    .unwrap(),
                CommitOutcome::Conflict,
                "a second session minting the same nonce must LOSE — unreserved, it would \
                 later overwrite the first object's segment records",
            );
            assert!(
                !segment_group_adopted(&store, NONCE).await.unwrap(),
                "a reserved group is not an ADOPTED one: the two-arm lifetime rule turns \
                 on exactly that difference, so the terminal delete of a session that \
                 published nothing still reclaims its marker",
            );
            assert_eq!(
                reserve_segment_group("nonsense").unwrap_err(),
                ChunkMapError::NonceNotHex {
                    nonce: "nonsense".to_string(),
                },
                "a nonce no `seg:` key could reproduce reserves nothing, so it is refused \
                 where it is minted rather than where it is resolved",
            );
        });
    }
}
