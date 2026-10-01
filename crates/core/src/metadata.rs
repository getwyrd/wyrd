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
use std::collections::BTreeMap;
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
/// `0005:288-295`; the reader-safe window `0005:291-294`). The value is an
/// [`OrphanMark`]: the logical-millis instant the fragment became orphaned, in one of
/// the three shapes [`decode_orphan_mark`] reads.
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

/// The longest unreference-event identity an [`OrphanMark`] may carry, in bytes.
///
/// Every identity proposal 0016 enumerates is short ASCII (`0016:1174-1187`): a `retire:`
/// token (the longest, a per-part session token, is 94 bytes), an `<upload-id>:<epoch>`, an
/// `{inode, version}` pair, or a per-move nonce. The bound is what caps a decodable mark's
/// size, and so what a commit carrying marks costs: GC's reclaim intent carries each mark
/// twice — the exact-value precondition and the `reclaiming` value — for a whole batch of
/// marks in one commit, and an event free to grow to the value ceiling would put that commit
/// past the backend's transaction envelope on every pass. At this bound a mark encodes to at
/// most 328 bytes (pinned by this module's codec tests).
pub const MAX_ORPHAN_EVENT_LEN: usize = 256;

/// The value of an `orphan:` mark, decoded: when its fragment became orphaned, which
/// unreference event wrote the mark, and whether GC has recorded its decision to reclaim the
/// fragment.
///
/// **Three shapes, one codec** ([`encode_orphan_mark`], [`decode_orphan_mark`]; proposal 0016,
/// `0016:1190-1216` and `:1321-1338`), defined here beside [`orphan_key`] for the reason the key
/// is: every writer and every reader of a mark goes through one definition, so no two of them
/// can spell a shape differently.
///
/// | Shape | Stored bytes | Carries |
/// |---|---|---|
/// | legacy | `1700000000000` | `orphaned_at_millis` alone: the bare decimal every writer before 0016 writes |
/// | structured | `{"orphaned_at_millis":N,"event":"E"}` | the identity of the unreference event that wrote it, too |
/// | reclaiming | `{"orphaned_at_millis":N,"event":"E","reclaiming":true}` | GC's recorded decision to reclaim; `event` is absent when the mark GC replaced was legacy |
///
/// **Decoding accepts exactly what encoding writes** (ADR-0045). The shapes are told apart by
/// the bytes' own form — ASCII digits or a JSON object — and a decoded value is re-encoded and
/// refused unless it comes back byte for byte. So a legacy value round-trips unchanged, a reader
/// that only reads the ledger never rewrites a mark or restarts a grace clock
/// (`0016:1208-1211`), and an exact-value compare-and-swap may be built from a decoded mark and
/// match the stored bytes. Every other value — a non-canonical decimal (`007`, `+7`), fields
/// reordered, unknown or `null`, `"reclaiming":false`, an event outside [`Self::structured`]'s
/// grammar — is a [`RecordError`](crate::multipart::RecordError), never a value: a maintenance
/// pass keeps such a mark and its fragment and names it, and never acts on it (ADR-0045 decision
/// 3).
///
/// **No writer overwrites a `reclaiming` mark.** GC writes one by an exact-value
/// compare-and-swap from the bytes it read, commits it before it deletes the fragment, and
/// deletes the key after — blind (`0016:1312-1320`). From the instant it lands, any commit
/// preconditioned on the mark's earlier bytes (an adoption of a pre-marked position) fails.
/// The writer side of that, which 0016 leaves implicit: a writer that finds a `reclaiming` mark
/// must not replace it, neither with a fresh stamp nor with its own identity. GC's blind key
/// delete would take the new value with it, leaving whatever that writer meant it to evidence
/// with no evidence at all; and a position whose fragment GC is deleting is no place for a new
/// write either. A writer that needs the position marked waits until the key is gone and writes
/// under `require_absent`. Every mark writer that reads before it writes — the three-arm drain
/// (#659), the repoint pre-mark (#663) — inherits this rule. The writers that mark a position in
/// the same commit that dereferences it (a delete, a supersede) do not read first: GC swaps a
/// mark to `reclaiming` only on a position nothing referenced when it looked, and once every move
/// that re-places a chunk adopts under its mark's precondition (#663), nothing can reference that
/// position again before its key is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanMark {
    orphaned_at_millis: u64,
    event: Option<String>,
    reclaiming: bool,
}

impl OrphanMark {
    /// A legacy mark: the instant alone, stored as the bare decimal every writer before proposal
    /// 0016 writes.
    pub fn legacy(orphaned_at_millis: u64) -> Self {
        Self {
            orphaned_at_millis,
            event: None,
            reclaiming: false,
        }
    }

    /// A structured mark, naming the unreference event that wrote it.
    ///
    /// The identity grammar is the one [`decode_orphan_mark`] holds a stored value to: 1 to
    /// [`MAX_ORPHAN_EVENT_LEN`] bytes of visible ASCII other than `"` and `\` — every identity
    /// 0016 enumerates, and nothing JSON would escape, so a mark's encoded size is its event's
    /// size plus a constant. Anything else is refused here, so no writer can encode a mark the
    /// decoder would refuse.
    pub fn structured(
        orphaned_at_millis: u64,
        event: impl Into<String>,
    ) -> std::result::Result<Self, crate::multipart::RecordError> {
        Ok(Self {
            orphaned_at_millis,
            event: Some(checked_orphan_event(event.into())?),
            reclaiming: false,
        })
    }

    /// This mark in its terminal `reclaiming` state: the same stamp and the same event (none,
    /// for a legacy mark), so the compare-and-swap that writes it disturbs nothing the grace
    /// test was measured from (`0016:1331-1332`).
    pub fn into_reclaiming(self) -> Self {
        Self {
            reclaiming: true,
            ..self
        }
    }

    /// The instant the fragment became orphaned — where its grace window starts.
    pub fn orphaned_at_millis(&self) -> u64 {
        self.orphaned_at_millis
    }

    /// The identity of the unreference event that wrote the mark; `None` for a legacy mark, and
    /// for a `reclaiming` mark that replaced one.
    pub fn event(&self) -> Option<&str> {
        self.event.as_deref()
    }

    /// Whether GC has recorded its decision to reclaim the fragment: the reclamation is already
    /// decided, and what remains is to finish it (`0016:1321-1333`).
    pub fn is_reclaiming(&self) -> bool {
        self.reclaiming
    }

    /// The retirement token this mark's event spells, if it spells one: the obligation whose
    /// `retire:bytes:` key ([`crate::multipart::retire_key`]) is still present while that
    /// retirement drains, and whose absence says it has drained (`0016:1242-1247`).
    ///
    /// Read through the `retire:` key grammar's own parser ([`crate::multipart::parse_retire_key`]),
    /// so an event is a token only in the one spelling a retirement is installed under. An event
    /// that is not a token — a repoint pre-mark's per-move nonce, an owned-staging walk's
    /// `<upload-id>:<epoch>` — names no obligation, and answers `None`.
    pub fn retire_token(&self) -> Option<crate::multipart::RetireToken> {
        let event = self.event.as_deref()?;
        let mut key = crate::multipart::RETIRE_BYTES_PREFIX.to_vec();
        key.extend_from_slice(event.as_bytes());
        match crate::multipart::parse_retire_key(&key) {
            Ok((crate::multipart::RetireMode::Bytes, token)) => Some(token),
            _ => None,
        }
    }
}

/// The JSON form of the structured and `reclaiming` shapes. Field order is the stored order;
/// `event` and `reclaiming` are omitted when absent or false, never written as defaults, so a
/// structured mark and a `reclaiming` one differ by exactly the `"reclaiming":true` suffix.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrphanMarkWire {
    orphaned_at_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    reclaiming: bool,
}

/// An event identity [`OrphanMark::structured`] and [`decode_orphan_mark`] both accept: 1 to
/// [`MAX_ORPHAN_EVENT_LEN`] bytes of visible ASCII other than `"` and `\`.
fn checked_orphan_event(
    event: String,
) -> std::result::Result<String, crate::multipart::RecordError> {
    let visible = |b: &u8| matches!(b, 0x21..=0x7e) && *b != b'"' && *b != b'\\';
    if (1..=MAX_ORPHAN_EVENT_LEN).contains(&event.len()) && event.bytes().all(|b| visible(&b)) {
        Ok(event)
    } else {
        Err(crate::multipart::RecordError::MalformedRecordValue {
            namespace: "orphan:",
            detail: format!(
                "event {event:?} is not 1 to {MAX_ORPHAN_EVENT_LEN} bytes of visible ASCII \
                 other than `\"` and `\\`"
            ),
        })
    }
}

/// Encode an [`OrphanMark`] in its shape: the bare decimal for a legacy mark — byte for byte
/// what every writer before proposal 0016 writes — and the JSON object otherwise.
pub fn encode_orphan_mark(mark: &OrphanMark) -> Bytes {
    if mark.event.is_none() && !mark.reclaiming {
        return Bytes::from(mark.orphaned_at_millis.to_string());
    }
    encode(&OrphanMarkWire {
        orphaned_at_millis: mark.orphaned_at_millis,
        event: mark.event.clone(),
        reclaiming: mark.reclaiming,
    })
}

/// Decode an `orphan:` value into its [`OrphanMark`], accepting exactly the bytes
/// [`encode_orphan_mark`] writes and refusing everything else (see [`OrphanMark`]).
///
/// A value that is not a mark of any shape is
/// [`RecordError::MalformedRecordValue`](crate::multipart::RecordError::MalformedRecordValue);
/// one that reads as a mark in some other spelling than this codec's — a leading zero,
/// whitespace inside the object, reordered fields, a default written out — is
/// [`RecordError::NoncanonicalRecordValue`](crate::multipart::RecordError::NoncanonicalRecordValue),
/// because a compare-and-swap built from the decoded mark could never match those bytes.
pub fn decode_orphan_mark(
    value: &[u8],
) -> std::result::Result<OrphanMark, crate::multipart::RecordError> {
    let malformed = |detail: String| crate::multipart::RecordError::MalformedRecordValue {
        namespace: "orphan:",
        detail,
    };
    let mark = if !value.is_empty() && value.iter().all(u8::is_ascii_digit) {
        // The legacy shape. Digits alone reach `from_str`, so no sign can; a leading zero reads
        // as a number here and is refused as non-canonical below.
        let at = std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or_else(|| malformed("a bare decimal past u64".to_string()))?;
        OrphanMark::legacy(at)
    } else {
        let wire: OrphanMarkWire =
            serde_json::from_slice(value).map_err(|err| malformed(err.to_string()))?;
        OrphanMark {
            orphaned_at_millis: wire.orphaned_at_millis,
            event: wire.event.map(checked_orphan_event).transpose()?,
            reclaiming: wire.reclaiming,
        }
    };
    if encode_orphan_mark(&mark).as_ref() != value {
        return Err(crate::multipart::RecordError::NoncanonicalRecordValue {
            namespace: "orphan:",
        });
    }
    Ok(mark)
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

/// Whether an already-encoded **flat** record crosses the value ceiling every backend
/// inherits ([`MAX_VALUE_BYTES`]): `Some` names the ceiling it crossed (the caller's audit
/// line reports it beside the record's own length), `None` means the write may proceed.
///
/// The check a placement-maintenance write path (reconstruction / rebalance) makes on its
/// own `encode(&next)` **before it writes anything at all**, so a repoint that would not
/// survive on the tightest backend is REFUSED — classified, persisting nothing, and
/// distinguishable by the caller from a lost CAS ([`CommitOutcome::Conflict`]): a lost CAS
/// is worth retrying next pass, this shape never is until something shrinks the record.
/// Without it the write either reaches [`MetadataStore::commit`] as a raw backend `Err`
/// indistinguishable from a transient fault, or — on a store with no native enforcement —
/// commits a record that can then never be re-written, and `:333-341` above is what that
/// costs: *a root that cannot be re-written is an object whose placement can never be
/// repaired* (every repair is `require(inode, encode(prior)) + put(inode, encode(next))`).
///
/// Bound by the FULL ceiling ([`MAX_VALUE_BYTES`]), not the [`MAX_ROOT_VALUE_BYTES`] half:
/// that half exists to budget a **segmented** root's segment table against the reserve its
/// object metadata is spent from, and a flat record has no segment table — its whole value
/// *is* the record. The segmented arm of [`repoint_chunk`] weighs its record here too, for
/// the same reason: what it rewrites is one **segment** record, whose whole value is
/// likewise the record and which the resolver reads up to exactly this ceiling
/// (`read_group_range`). A placement move never re-encodes a segmented root, so
/// [`MAX_ROOT_VALUE_BYTES`] bounds only the publication that writes a segment table.
///
/// A record landing EXACTLY on [`MAX_VALUE_BYTES`] is admissible and is **not** refused —
/// the same `>` boundary the resolver's read side refuses a stored row on (`:2493`), so
/// nothing refused here is a record a conforming write could have stored.
pub fn flat_value_ceiling_crossed(encoded: &[u8]) -> Option<usize> {
    (encoded.len() > MAX_VALUE_BYTES).then_some(MAX_VALUE_BYTES)
}

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
    /// A root's segment table names more segments than [`MAX_ROOT_SEGMENTS`] allows,
    /// **while the root still names that generation**.
    ///
    /// Refused at **resolve** time (where the table becomes a bounded read,
    /// [`resolve_chunk_map`]), not at decode: the ceiling is a derived capacity constant
    /// this deployment chooses, not a structural invariant of the stored format
    /// (ADR-0045's liberal-on-read boundary — see [`MAX_ROOT_SEGMENTS`]'s own doc). The
    /// table is the root's own claim, so one past the ceiling is refused **before a
    /// single row of its range is read**, never clamped to the ceiling and read partway.
    ///
    /// Like every other resolve anomaly it goes through the resolve-retry arbiter first:
    /// a caller resolving a snapshot the store has since moved off restarts onto the live
    /// root instead, so an over-ceiling *retired* root can never fail the read of an
    /// object whose live generation is fine.
    TooManySegments {
        /// How many segments the table declares.
        segments: usize,
    },
    /// A record under the group's `seg:` range at an index the root's segment table
    /// does not name — an index at or past `segment_count`.
    SegmentUnknown {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The unnamed index.
        index: u32,
    },
    /// A segment the root's table names is **absent** from its group's `seg:` range
    /// while the root still names that exact generation: a live generation never loses
    /// a segment, so this is an invariant violation and is **fail-closed**
    /// (`0016:2463-2471`) — never a torn or partial read.
    SegmentAbsent {
        /// The group nonce.
        nonce: String,
        /// The group's fence epoch.
        epoch: u64,
        /// The missing segment's index.
        index: u32,
    },
    /// A segment record's own byte extent disagrees with the [`SegmentRef`] the root's
    /// table carries for it, while the root still names that generation.
    SegmentBoundsMismatch {
        /// The segment's index.
        index: u32,
        /// The `(byte_offset, byte_len)` the root's table names.
        root: (u64, u64),
        /// The `(byte_offset, byte_len)` the segment record itself carries.
        segment: (u64, u64),
    },
    /// A record under the group's `seg:` range whose **value** is larger than the value
    /// ceiling every backend inherits ([`MAX_VALUE_BYTES`]), while the root still names
    /// that generation.
    ///
    /// What this refusal *is*: the resolver will neither **decode** such a row nor
    /// **retain** it, so nothing downstream of here is sized by it. What it is **not**:
    /// a claim that no memory was spent on it. Values arrive from the seam already
    /// materialised — [`MetadataStore::scan_page`] hands back `Vec<(Vec<u8>, Bytes)>`
    /// and [`MetadataStore::get`] an `Option<Bytes>` — so the byte a store commits to a
    /// caller's heap is committed before any caller can look at it. Bounding *that* is
    /// the seam's, not this function's (the trait inherits each backend's native value
    /// limit and surfaces it as `Err`, `crates/traits/src/lib.rs:995-999`); it is
    /// tracked as getwyrd/wyrd#674 and is deliberately not chased with a mechanism here.
    ///
    /// The ceiling is the **value** ceiling `V`, twice the `V / 2` a publishable segment
    /// value is bounded by (`0016:1467`, the `MAX_SEG_CHUNKS` row), so no record a
    /// conforming publication could have written is ever refused by it — and like every
    /// other resolve anomaly it goes through the resolve-retry arbiter first, so an
    /// oversized row in a *retired* generation restarts the read instead of failing it.
    SegmentValueOverCeiling {
        /// The segment's index.
        index: u32,
        /// The value's size in bytes.
        bytes: usize,
        /// The ceiling it passed ([`MAX_VALUE_BYTES`]).
        ceiling: usize,
    },
    /// A record under the group's `seg:` range could not be decoded at all, while the
    /// root still names that generation.
    SegmentRecordUndecodable {
        /// The segment's index.
        index: u32,
        /// What the decode could not do with the bytes.
        detail: String,
    },
    /// The **root record itself** could not be decoded, on one of the re-reads a resolve
    /// makes of it: the settle-read that decides whether an anomaly is a concurrent
    /// retirement, or the re-read a restart begins with ([`resolve_current_chunk_map`]).
    ///
    /// The [`Self::SegmentRecordUndecodable`] rule one level up, and it exists for the same
    /// reason: a record whose own bytes will not parse is **this object's** fault and no
    /// store's, so it is described as a chunk-map anomaly rather than escaping as whatever
    /// error the decoder happened to raise. A maintenance pass classifies by that
    /// distinction — an object-local fault contains to the object, a store fault ends the
    /// pass — and an untyped decode error reaching it reads as a fleet-wide store outage,
    /// which stops every *healthy* object's protection over one damaged record
    /// (`crates/custodian/src/gc.rs`'s reference build).
    ///
    /// Fail closed, never "this object owns no bytes": the caller's own first decode of a
    /// root is its own to classify, and this covers only the re-reads the resolver makes
    /// underneath it.
    RootRecordUndecodable {
        /// What the decode could not do with the bytes.
        detail: String,
    },
    /// A resolve that kept meeting a **superseded** generation: each restart re-read the
    /// root and found the generation it named already replaced again, past
    /// [`MAX_RESOLVE_RESTARTS`] attempts. Fail closed — answering "this object owns no
    /// bytes" after giving up is exactly the data-loss shape decision 7(h) forbids.
    ///
    /// Only a **supersede** spends an attempt. A root the re-read finds **absent** is a
    /// deletion, not churn: nothing later exists to restart onto, so it answers "no such
    /// object" where it is seen and never reaches this variant. Collapsing the two would
    /// report a plain delete as an unsettled map whenever it landed on the last allowed
    /// attempt (`0016:2452-2471`).
    MapResolutionUnstable {
        /// How many restarts were spent.
        attempts: usize,
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
            Self::TooManySegments { segments } => write!(
                f,
                "segment table names {segments} segments, over the {MAX_ROOT_SEGMENTS}-segment resolve ceiling: refused before its range was read"
            ),
            Self::SegmentUnknown {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "seg:{nonce}:{epoch}:{index:0width$} exists but the root's table does not name it",
                width = SEG_INDEX_WIDTH
            ),
            Self::SegmentAbsent {
                nonce,
                epoch,
                index,
            } => write!(
                f,
                "seg:{nonce}:{epoch}:{index:0width$} is absent while the root still names this generation",
                width = SEG_INDEX_WIDTH
            ),
            Self::SegmentBoundsMismatch {
                index,
                root,
                segment,
            } => write!(
                f,
                "segment {index}: the root's table names ({}, {}) but the record carries ({}, {})",
                root.0, root.1, segment.0, segment.1
            ),
            Self::SegmentValueOverCeiling {
                index,
                bytes,
                ceiling,
            } => write!(
                f,
                "segment {index}'s record is {bytes} bytes, over the {ceiling}-byte value ceiling: neither decoded nor retained"
            ),
            Self::SegmentRecordUndecodable { index, detail } => {
                write!(f, "segment {index} could not be decoded: {detail}")
            }
            Self::RootRecordUndecodable { detail } => {
                write!(f, "the object's root record could not be decoded: {detail}")
            }
            Self::MapResolutionUnstable { attempts } => write!(
                f,
                "the chunk map kept resolving to a retired generation after {attempts} restarts"
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
/// `Deserialize`: each decode path — [`SegmentGroup`]'s, and the session record's
/// `segment_nonce` (`multipart.rs`) — routes through the validating constructor, so no
/// derive can produce one of these unvalidated.
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
    /// The validating constructor from a raw string — with [`SegmentGroup::from_nonce`], the
    /// only way to obtain a `SegmentGroup`, so a nonce that could not key a reproducible
    /// `seg:` range is never representable (ADR-0045, parse-don't-validate).
    pub fn new(nonce: impl Into<String>, epoch: u64) -> std::result::Result<Self, ChunkMapError> {
        Ok(Self {
            nonce: SegmentNonce::new(nonce)?,
            epoch,
        })
    }

    /// A group from a nonce that is **already** a [`SegmentNonce`] — infallible, because the
    /// only way to hold one is [`SegmentNonce::new`], so the rule has run exactly once. For a
    /// holder that names a group from a nonce it validated at its own decode (a `Completing`
    /// session's attempt group, `multipart.rs`) and must not re-parse it.
    pub fn from_nonce(nonce: SegmentNonce, epoch: u64) -> Self {
        Self { nonce, epoch }
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
    /// [`resolve_chunk_map`] (or [`resolve_current_chunk_map`], for a caller whose own
    /// snapshot may already be stale): that is the ONE way a consumer able to reach the
    /// store turns this field into an ordered chunk list (decision 7(e)). A consumer
    /// that cannot — no store in hand — uses [`ChunkMap::as_flat`] and fails closed on a
    /// segmented map; never treat [`ChunkMap::Segmented`] as an empty list.
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
///
/// The same value shape serves two key spaces (proposal 0016, `:442-457`): an **ordinary**
/// streaming-write lease under `pending:<chunk-id>`, carrying neither ownership field, and an
/// **owned** multipart staging entry under `sidx:<upload-id>:<part-number>:<chunk-id>`, carrying
/// both — the owning upload id and the chunk's planned placement. Both or neither is the only
/// valid shape, refused at decode otherwise ([`crate::multipart::RecordError::TornOwnedEntry`],
/// the cross-field rule [`InodeRecord`]'s wire applies to its own record); **which** of the two a
/// value may be is decided against its key, by one decode entry point per namespace —
/// [`decode_pending_entry`] here, [`crate::multipart::decode_owned_entry`] for `sidx:`. The
/// validated owned view, and the checked way to mint one, is [`crate::multipart::OwnedEntry`].
///
/// The write side mirrors the decode, as [`InodeRecord::checked_for_publication`] does for its
/// record: the fields are public, so a caller *can* hand [`put_pending`] or [`renew_pending`] a
/// torn or owned value, and storing it would leave under `pending:` bytes every reader of that
/// namespace refuses. Both writers therefore apply the same rule as [`decode_pending_entry`]
/// ([`Self::checked_ordinary_lease`]) and refuse such a value before touching the store.
///
/// # Serialization identity, and the CAS shape it protects
///
/// `skip_serializing_if` on both fields is load-bearing, as it is for [`InodeRecord`]'s optional
/// trio — but this record rides the **other** of the two CAS shapes in this module, and 0016's
/// account of it (`0016:475-485`, "compare the *re-encoded* prior entry") does not match the
/// code. [`renew_pending`] preconditions on the **raw bytes it read** and puts the entry **its
/// caller handed it**, freshly encoded (`require(key, current)` + `put(key, encode(entry))`), and
/// the lease guards pin those same raw bytes. So an encoder that spelled an absent field
/// `"owner":null` would not wedge those CASes on a permanent `Conflict`, as it would an `inode:`
/// CAS on `require(key, encode(prior))`: the CAS would *win*, and the renewal would durably rewrite
/// every legacy entry's shape with no error anywhere. Omitting `None` is what keeps a both-absent
/// entry's encoding byte-identical to what every earlier build wrote, so a renewal changes only the
/// lease it came to extend.
///
/// The wire stays **open**, as [`InodeRecordWire`] is and as this record's derive always was: the
/// `pending:` namespace has a stored corpus and live readers across a mixed-version fleet, so
/// making an unknown field fatal here is a format change of its own. The owned shape, which has no
/// corpus yet, is decoded closed by the `sidx:` entry point.
///
/// Not `Copy`: `owner` is a [`crate::multipart::UploadId`], a `String` newtype.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PendingEntryWire")]
pub struct PendingEntry {
    /// When the lease expires (logical milliseconds); a custodian sweep may
    /// reclaim the chunk after this.
    pub lease_expiry_millis: u64,
    /// The owning upload session — `Some(..)` only on an owned `sidx:` entry (`0016:442-457`).
    /// A validated token, so a malformed owner is a decode error rather than an id no
    /// per-session `sidx:<upload-id>:` range could be derived from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<crate::multipart::UploadId>,
    /// The chunk's planned EC placement, written at intent time so a record-only reaper can
    /// compute its `orphan:<dserver>:<chunk>:<index>` keys (`0016:459-473`) — `Some(..)` only on
    /// an owned `sidx:` entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged: Option<crate::multipart::StagedPlacement>,
}

/// The wire shape of [`PendingEntry`] — identical field-for-field, so every record ever written
/// still decodes (both new fields default to absent); it exists to give the decode a place to
/// refuse a torn value before the value exists, as [`InodeRecordWire`] does for its record.
///
/// Absence is the **only** spelling of an absent ownership field. `Option<T>`'s own
/// `Deserialize` would also read an explicit `"owner":null` / `"staged":null` as `None`, and the
/// encoder (`skip_serializing_if`) can never emit that spelling back — so a lease stored with a
/// `null` would decode, pass every rule, and be rewritten field-for-field by the next
/// [`renew_pending`], which preconditions on the raw bytes it read and puts a fresh encoding:
/// the CAS wins and the stored shape changes with no error anywhere (PR #793 review). Requiring
/// a value when the key is present makes the accepted set exactly the encoder's image, the same
/// closure `de_content_type` gives the session record's one optional field.
#[derive(Deserialize)]
struct PendingEntryWire {
    lease_expiry_millis: u64,
    #[serde(default, deserialize_with = "de_present_ownership")]
    owner: Option<crate::multipart::UploadId>,
    #[serde(default, deserialize_with = "de_present_ownership")]
    staged: Option<crate::multipart::StagedPlacement>,
}

/// Read a **present** ownership field of [`PendingEntryWire`] as a value, never as `null`.
///
/// It never returns `None`: serde calls it only for a key that is present, and absence is the
/// `#[serde(default)]` beside it. The `Option` in the signature is the field's type, not a
/// second absence channel — `de_content_type`'s shape, for the same reason.
fn de_present_ownership<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some).map_err(|err| {
        DeError::custom(format!(
            "{err} (an absent ownership field is spelled by omitting it, never as null)"
        ))
    })
}

impl TryFrom<PendingEntryWire> for PendingEntry {
    type Error = crate::multipart::RecordError;

    fn try_from(wire: PendingEntryWire) -> std::result::Result<Self, Self::Error> {
        crate::multipart::checked_ownership_pairing(wire.owner.is_some(), wire.staged.is_some())?;
        Ok(Self {
            lease_expiry_millis: wire.lease_expiry_millis,
            owner: wire.owner,
            staged: wire.staged,
        })
    }
}

impl PendingEntry {
    /// The `pending:` namespace's shape rule, in one place: an entry under `pending:` is an
    /// **ordinary** lease, carrying neither ownership field. Both directions apply it —
    /// [`decode_pending_entry`] to what it reads, [`put_pending`] and [`renew_pending`] to what
    /// they are asked to store — so no value one side refuses can pass the other.
    ///
    /// The pairing rule runs first, so a torn value is refused as torn
    /// ([`crate::multipart::RecordError::TornOwnedEntry`]) wherever it is met, and only a value
    /// carrying **both** fields reaches the namespace check — which, after the pairing, is the
    /// presence of an `owner`.
    fn checked_ordinary_lease(&self) -> std::result::Result<(), crate::multipart::RecordError> {
        crate::multipart::checked_ownership_pairing(self.owner.is_some(), self.staged.is_some())?;
        if self.owner.is_some() {
            return Err(
                crate::multipart::RecordError::PendingEntryNamespaceMismatch {
                    namespace: "pending:",
                    shape: crate::multipart::OWNED_SHAPE,
                },
            );
        }
        Ok(())
    }
}

/// Decode a `pending:<chunk-id>` value — the `pending:` namespace's **one** decode entry point,
/// through which every reader of that namespace goes ([`renew_pending`], [`live_lease_guards`],
/// `write::sweep_expired_leases`, the custodian GC's expired-lease scan).
///
/// It accepts only an **ordinary** lease. A torn value is refused by the record's own decode, and
/// an owned staging entry — both ownership fields present, a `sidx:` value filed under the wrong
/// key — is refused here, as
/// [`PendingEntryNamespaceMismatch`](crate::multipart::RecordError::PendingEntryNamespaceMismatch):
/// read as an ordinary lease it would be renewed with its ownership erased ([`renew_pending`]
/// puts its caller's entry) or reclaimed by an expiry sweep that believes it holds an abandoned
/// write. The store-wide [`decode`] of a `PendingEntry` still reads either valid shape; it cannot
/// see which namespace a value came from, which is why this entry point exists.
pub fn decode_pending_entry(
    value: &[u8],
) -> std::result::Result<PendingEntry, crate::multipart::RecordError> {
    let wire: PendingEntryWire =
        decode(value).map_err(|err| crate::multipart::RecordError::MalformedRecordValue {
            namespace: "pending:",
            detail: err.to_string(),
        })?;
    let entry = PendingEntry::try_from(wire)?;
    entry.checked_ordinary_lease()?;
    Ok(entry)
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
///
/// Errors (before touching the store) on an entry that is not an ordinary lease — a torn value
/// or an owned `sidx:` entry — which every `pending:` reader would refuse
/// ([`PendingEntry::checked_ordinary_lease`]).
pub async fn put_pending(
    store: &impl MetadataStore,
    chunk: ChunkId,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    entry.checked_ordinary_lease()?;
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
///
/// Both values it handles must be ordinary leases ([`PendingEntry::checked_ordinary_lease`]):
/// an `entry` that is not one is refused before the store is touched, as [`put_pending`]
/// refuses it, and a stored value that is not one — an owned `sidx:` entry misfiled under
/// `pending:`, a torn or malformed value — is an error rather than a lease to renew, since the
/// put would replace it with the caller's entry and erase its ownership fields.
pub async fn renew_pending(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
    now_millis: u64,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    if chunks.is_empty() {
        return Ok(CommitOutcome::Committed);
    }
    entry.checked_ordinary_lease()?;
    let mut batch = WriteBatch::new();
    for &chunk in chunks {
        let key = pending_key(chunk);
        let current = match store.get(&key).await? {
            // Swept out from under the upload — refuse rather than resurrect.
            None => return Ok(CommitOutcome::Conflict),
            Some(bytes) => bytes,
        };
        let existing = decode_pending_entry(&current)?;
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
        let entry = decode_pending_entry(&current)?;
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

/// The `action` of an [`attribute_unaccounted_inode_row`] event for a row whose **value**
/// does not decode into an [`InodeRecord`] at all.
const UNDECODABLE_INODE_RECORD: &str = "undecodable-inode-record";
/// The `action` for a row whose value decodes into a **segmented** root — a shape this walk
/// has no resolver for, so it can account for the record's id but for nothing inside it
/// (the [`ChunkMapError::SegmentedMapUnsupported`] this walk used to *refuse* over).
///
/// It has none *and needs none*: with the chunk mark gone this walk derives nothing from any
/// record's map, so reaching for [`resolve_chunk_map`] here would spend a store round trip
/// per segmented record at startup to produce a value nobody reads — the exact shape issue
/// #652 removes. The record is named instead, which is what the walk owes it.
const UNRESOLVED_SEGMENTED_ROOT: &str = "unresolved-segmented-inode-root";
/// The `action` for a row under the `inode:` prefix whose **key** is not an `inode:<id>`
/// key, so it names no id [`high_water_marks`] could recover.
const UNPARSABLE_INODE_KEY: &str = "unparsable-inode-key";

/// Attribute a row of the `inode:` namespace that startup recovery could not fully account
/// for, on the durability plane's audit seam (ADR-0011/ADR-0012): a counter the
/// `tracing`→OTel bridge aggregates plus an event naming the offending key. Same split
/// between metric event and audit event as the read path's fault reporter
/// (`crates/core/src/read.rs:212-240`).
///
/// This is the other half of containment. [`high_water_marks`] does not stop on such a row —
/// stopping costs every *healthy* object its availability — but "does not stop" must never
/// mean "silently skipped": the row is named, so an operator holds an explicit repair
/// obligation (`docs/principles.md` §5 C-1; the same shape the custodian's GC walk gives the
/// same namespace through `ReferenceSet::unresolvable`, `crates/custodian/src/gc.rs:378-382`).
///
/// Emitted **at most once per row**, and deliberately: records are repaired one at a time, so
/// a summary count would say a store is damaged without saying which key to look at, while a
/// row named twice would inflate the counter beside it and hand an operator two repair
/// obligations for one stored row. [`high_water_marks`] therefore stops at a row's *first*
/// unaccountable fact — a key that names no id makes its value moot, since nothing in that
/// value could raise the mark. The counter beside the event is the aggregate view, and it
/// counts **rows**.
///
/// The key is rendered with [`slice::escape_ascii`], never `String::from_utf8_lossy`: lossy
/// rendering collapses every invalid byte to one replacement character, so two distinct
/// damaged keys can print identically — and a repair obligation an operator cannot resolve
/// to a single row is not one. Escaping is injective, so the event names exactly one row.
fn attribute_unaccounted_inode_row(action: &str, key: &[u8], detail: impl std::fmt::Display) {
    tracing::warn!(
        monotonic_counter.recovery_unaccounted_inode_row = 1_u64,
        action
    );
    tracing::warn!(
        target: "wyrd.metadata.recovery.audit",
        action,
        key = %key.escape_ascii(),
        detail = %detail,
        "startup recovery could not account for a row of the `inode:` namespace: it is \
         attributed here and the walk continues, so one damaged record does not cost every \
         healthy object its availability",
    );
}

/// The high-water mark of the persisted **inode allocator** over the stored metadata: the
/// largest inode id the `inode:` namespace still names.
///
/// A gateway allocates inode ids from the shared, persisted `meta:next_inode` counter
/// (`server::cli::alloc_inode`); on a restart over a **non-empty** store — or an in-place
/// upgrade from a store an older single-process gateway wrote with no such counter — that
/// counter must resume *above* everything already on disk. Otherwise a new-key PUT reuses a
/// committed inode id, which [`create`]'s `require_absent` on the inode key turns into a
/// bogus "concurrent writer won" conflict (issue #364 durability finding 1). This walk
/// supplies the mark so `Gateway::recover` can seed the counter
/// (`crates/server/src/lib.rs:133-141`). An empty store yields `0` and allocation starts at
/// 1, unchanged.
///
/// **It is total over the content it walks: no arrangement of stored records makes it
/// refuse** (issue #652). `Gateway::recover` runs it *before the gateway serves anything*,
/// so an `Err` here is not one damaged object's failure — it costs **every healthy object**
/// its availability, and nothing but manual repair leaves that state (`docs/principles.md`
/// §5 C-1). Hence:
///
/// * The mark comes from each row's **key**, read *before* the value beside it is looked at,
///   so a record this walk cannot read **still contributes its own floor**. The two legal
///   answers for an unreadable record are *fail closed for that record* or *contribute its
///   true floor* — never a quiet zero, which is the one answer an allocator would trust and
///   act on.
/// * A value this walk cannot account for is **attributed**
///   ([`attribute_unaccounted_inode_row`], above) and the walk **continues**, instead of
///   `?`-propagating: bytes that do not decode at all, and a structurally valid **segmented**
///   root, which this walk has no resolver for. #648 enforces the segmented root's structural
///   invariants at decode (ADR-0045), which widens the set of values that can fail the
///   decode, so the hazard is live rather than latent. This is the containment this same
///   `inode:` namespace already gets from the custodian's GC walk
///   (`crates/custodian/src/gc.rs:378-382`), for the reason its module doc gives at
///   `gc.rs:22-31`: "the one object's fault is contained: it is attributed, and the walk —
///   and every other object's protection — continues".
/// * A fault that is **not** a record's own — the `scan` itself failing — still propagates
///   (`?`). A walk that cannot read the metadata store has no mark at all, and containing
///   that as "one record is unreadable" would be the wrong answer for every record in it
///   (the split `gc.rs:355-359` already draws).
///
/// One residual is **not** this walk's to close, and predates it: a store that already holds
/// `inode:<u64::MAX>` names an id with **no successor**, so the floor its caller derives
/// (`mark + 1`, `crates/server/src/lib.rs:141`) saturates and the allocator is left at the
/// ceiling with `alloc_inode`'s `id + 1` unguarded (`crates/server/src/cli.rs:1662`). That is
/// the *exhausted allocator*, not an unreadable record: the mark has always come from the key
/// ([`parse_inode_key`]), so a perfectly decodable record at that key reaches the identical
/// state on this tree today. Making the allocator fail closed there rather than wrap is
/// allocator safety and belongs to the function that hands ids out —
/// deferred: getwyrd/wyrd#687.
///
/// **There is no chunk-id mark, deliberately — it is not to be restored, re-derived or
/// wired** (issue #652). This function used to return one beside the inode mark, and
/// `Gateway::recover` bound it to a discarded, unused name. Both halves were written by the
/// *same* commit, `fdd34f1` (#487, 2026-07-08): before it, `mint_chunk_id` was a plain
/// counter from 0 and `recover` genuinely consumed the floor; after it, chunk ids are
/// `(chunk_epoch << 64) | seq` over a per-gateway random epoch whose top bit is set, so every
/// minted id is ≥ 2^127, two processes draw disjoint ranges with no shared counter, and
/// `next_chunk_seq` is seeded from nothing (`crates/server/src/lib.rs:246-258`, ADR-0019).
/// The cluster path never needed it either: `server::cli::chunk_id_minter` mints
/// `(inode_id << 64) | seq` with `inode_id ≥ 1`, so every id it produces is ≥ 2^64
/// (`crates/server/src/cli.rs:1809-1816`). Nothing in the tree mints into the `< 2^64` space
/// the old mark recovered, so there is no consumer to wire it to — and a number nobody reads
/// is not a safety property. Removing it also removes the two further complete scans
/// (`pending:`, `orphan:`) whose only product was that discarded value, and with them the
/// last reason for this walk to resolve any record's chunk map at all.
pub async fn high_water_marks(store: &impl MetadataStore) -> Result<InodeId> {
    let mut max_inode: InodeId = 0;
    for (key, value) in store.scan(b"inode:").await? {
        // The KEY first and unconditionally: the mark must never depend on anything this
        // walk might fail to read out of the value beside it.
        let Some(id) = parse_inode_key(&key) else {
            // A row under this prefix that is not `inode:<id>` names no id the allocator
            // can mint ([`inode_key`] is the sole writer of the prefix and ids are `u64`),
            // so skipping it cannot put the mark below a live id — but it is still a row
            // this walk cannot account for, so it is named rather than dropped in silence.
            //
            // Named ONCE, then the row is done: whatever its value holds, no id inside it
            // could raise the mark, so reading it further could only add a second event and
            // a second counter tick for one stored row — one repair obligation per row.
            attribute_unaccounted_inode_row(
                UNPARSABLE_INODE_KEY,
                &key,
                "not an `inode:<id>` key, so it names no recoverable id",
            );
            continue;
        };
        max_inode = max_inode.max(id);
        // The VALUE is read ONLY to attribute what this walk cannot account for — never to
        // derive the mark. Startup is the one pass that sees every record, so a record no
        // reader can decode, and one whose chunk map this build has no resolver for, are
        // both named here instead of surfacing later as one object's unexplained failure.
        // Failing on either, as this walk used to, is what let one record stop the gateway.
        match decode::<InodeRecord>(&value) {
            Ok(record) if record.chunk_map.as_flat().is_none() => attribute_unaccounted_inode_row(
                UNRESOLVED_SEGMENTED_ROOT,
                &key,
                ChunkMapError::SegmentedMapUnsupported {
                    operation: "high_water_marks",
                },
            ),
            Ok(_) => {}
            Err(fault) => attribute_unaccounted_inode_row(UNDECODABLE_INODE_RECORD, &key, fault),
        }
    }
    Ok(max_inode)
}

// ---------------------------------------------------------------------------
// Chunk-map RESOLUTION (proposal 0016 decision 7(e)/(h), issue #649)
// ---------------------------------------------------------------------------
//
// #648 landed the segmented SHAPE with no reader: every pre-existing `.chunk_map` site
// above treats `ChunkMap::Segmented` as `ChunkMapError::SegmentedMapUnsupported`. This
// section is the ONE way a consumer that can reach the store turns a committed inode
// into its ordered chunk list (decision 7(e)) — `read.rs`'s placement-aware entries and
// the gateway's streaming/ranged reads go through it; the custodian's maintenance passes
// still fail closed until #650/#651 adopt it.
//
// WHAT IS BOUNDED HERE, AND WHAT IS NOT.
//
// * The WORK a record can demand of a reader IS this caller's, and is bounded three
//   ways: a table naming more than `MAX_ROOT_SEGMENTS` segments is refused *before its
//   range is read at all*; the range read is the group's own `seg:<nonce>:<epoch>:`
//   prefix, never a global `seg:` scan (`0016:2393-2400`); and each page asks for
//   `SEGMENT_PAGE_LIMIT` rows — this reader's constant, never the root's claim, so no
//   record sizes a page.
// * The BYTES a read materialises are NOT bounded here, and cannot be: `scan_page`
//   returns `Vec<(Vec<u8>, Bytes)>` and `get` returns `Option<Bytes>`, so a value is
//   already in the caller's heap when it arrives (`crates/traits/src/lib.rs:1105`,
//   `:1017`). The trait assigns that bound to the seam — a backend's native limits are
//   inherited and surface as `Err` (`crates/traits/src/lib.rs:995-999`) — and it is
//   tracked as getwyrd/wyrd#674. `SegmentValueOverCeiling` below is therefore exactly
//   what its doc says: this resolver will not decode or retain an over-ceiling row. It
//   is NOT a claim that no memory was spent on one.
// * The TIME an await may take is the BACKEND's: "a backend must bound its own waiting
//   rather than block a caller forever on an unreachable cluster"
//   (`crates/traits/src/lib.rs:1000-1012`), which is why each networked driver imposes
//   its own (`crates/metadata-fdb/src/lib.rs:78-89`,
//   `crates/metadata-tikv/src/lib.rs:143-172`) and the embedded one needs none.
//   `wyrd-core` holds no runtime dependency to spend a caller-side deadline from
//   (ADR-0009; `crates/core/Cargo.toml:11-15` keeps core executor-free), and no other
//   metadata call in this module wraps one.

/// The most rows one page of a group's `seg:` range asks for — **this reader's
/// constant**, not the root's claim.
///
/// A root's segment table may not size the page spent on its behalf: passing the
/// claimed segment count would let a record set the transient cost of one round trip,
/// which is the record bounding the reader rather than the other way round. A fixed
/// bound caps that cost at a constant and costs at most
/// `MAX_ROOT_SEGMENTS / SEGMENT_PAGE_LIMIT` extra round trips for the largest table the
/// ceiling admits.
///
/// The walk terminates on the paging contract, not on this number: the cursor is
/// exclusive and strictly advancing (clause 2/3, `crates/traits/src/lib.rs:1105`) and
/// every row this resolver keeps has a distinct parsed index below the root's claim, so
/// a group's range costs at most `claim + 1` rows however they are paged — the first row
/// the table cannot account for ends the walk where it is found. Those clauses are not
/// taken on trust: the shared `wyrd-metadata-conformance` suite asserts each of them on
/// every backend (`crates/traits/src/lib.rs:1105`, `0016:2653-2666`), which is why this
/// resolver needs no page counter of its own on top of them.
pub const SEGMENT_PAGE_LIMIT: usize = 128;

/// How many times [`resolve_current_chunk_map`] re-reads the root before giving up. Each
/// restart means the generation just read was retired *again* under it; past this many
/// the honest answer is a typed error, never "this object owns no bytes" (decision 7(h)).
pub const MAX_RESOLVE_RESTARTS: usize = 3;

/// One resolved chunk map: the **ordered** chunk list, and the root generation it was
/// resolved from (proposal 0016 decision 7(e)).
///
/// The record rides along because it is the only honest source of a read's framing: a
/// resolve that restarted onto the live root ([`resolve_chunk_map`], decision 7(h))
/// answers chunks the caller's own snapshot does not describe, so a caller taking
/// `size`/`etag`/`modified` from its snapshot would frame the new bytes with the retired
/// generation's headers. Both fields are [`Cow`]s so the ordinary case — a flat map on a
/// snapshot that is still live — copies neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedChunkMap<'a> {
    /// The generation the chunks came from: the caller's own snapshot, or the live root
    /// a retired resolution restarted onto.
    pub record: Cow<'a, InodeRecord>,
    /// That generation's ordered chunk list.
    pub chunks: Cow<'a, [ChunkRef]>,
}

/// What resolving one generation produced: its answer, or the reason the resolution was
/// dropped without one (decision 7(h), `0016:2452-2471`).
///
/// The two dropped arms are **not** the same answer, and collapsing them is a data-shape
/// bug rather than a nicety: a superseded generation has a live successor to restart onto,
/// a deleted one has none. Only the first is churn worth spending a restart on; the second
/// is already the final answer every consumer reads as "no such object". Told apart here,
/// once, so no caller has to re-derive it from a bare `None`.
enum Resolution<T> {
    /// The generation resolved.
    Answer(T),
    /// The root has moved on to a **different** generation: the read restarts onto it. An
    /// overwrite is not a deletion, so answering "no such object" here would 404 an object
    /// that never stopped existing.
    Superseded,
    /// The root is **absent**: the object has no live committed generation, and there is
    /// no later one to restart onto. Terminal — it never spends a restart, so a delete
    /// racing the last allowed attempt still answers "no such object" instead of
    /// [`ChunkMapError::MapResolutionUnstable`].
    Gone,
}

impl<T> Resolution<T> {
    /// Carry a dropped arm across a change of answer type, mapping the answer with `f` —
    /// so a stage that reshapes what was resolved cannot accidentally reshape *why* a
    /// resolution was dropped.
    fn map<U>(self, f: impl FnOnce(T) -> U) -> Resolution<U> {
        match self {
            Self::Answer(answer) => Resolution::Answer(f(answer)),
            Self::Superseded => Resolution::Superseded,
            Self::Gone => Resolution::Gone,
        }
    }
}

/// What the **current** root says about `group` — the re-read the resolve-retry rule turns
/// on (`0016:2463-2471`). `Ok(None)` means the root still names this exact generation, so
/// an anomaly under it is that generation's own fault; otherwise the generation has been
/// dropped and the arm says **how**, because the two answers differ: overwritten
/// ([`Resolution::Superseded`], restart) or deleted ([`Resolution::Gone`], terminal).
///
/// A root whose own bytes will not **decode** is neither answer: nothing can be said about
/// which generation it names. That is the object's own fault and no store's, so it fails
/// closed as [`ChunkMapError::RootRecordUndecodable`] ([`decode_root_record`]) — typed, so
/// a maintenance pass contains it to this object instead of reading it as a store outage.
///
/// Generic in the answer type it never produces: with no `T` in scope this cannot build a
/// [`Resolution::Answer`], so "the root re-read decided the map" is unrepresentable, and
/// the arm composes straight into whichever stage asked.
async fn root_dropped<T>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    group: &SegmentGroup,
) -> Result<Option<Resolution<T>>> {
    let Some(bytes) = store.get(root_key).await? else {
        // Deleted, not overwritten. A restart would re-read the same absent root, so
        // spending one buys nothing — and on the last allowed attempt it would report an
        // ordinary delete as a map that will not settle.
        return Ok(Some(Resolution::Gone));
    };
    let current = decode_root_record(&bytes)?;
    if current.chunk_map.segmented().map(SegmentedMap::group) == Some(group) {
        return Ok(None);
    }
    // The root still exists but names something else — a different group, or a flat map.
    Ok(Some(Resolution::Superseded))
}

/// The resolve-retry rule's arbiter (`0016:2463-2471`, decision 7(h)): given an anomaly
/// in a group's `seg:` range, decide whether it is a **concurrent retirement** (the root
/// moved off this generation — the dropped arm [`root_dropped`] names, restart or
/// not-found) or an **invariant violation** on a generation the root still names
/// (`Err(fault)`, fail closed, never a torn map).
///
/// Every anomaly [`read_segments`] can meet — a table past the reader's ceiling, a row
/// the root does not name, a key the `seg:` grammar refuses, a value past the per-record
/// ceiling, a named segment that is absent, one that will not decode, one whose extent
/// disagrees with the root — comes through here rather than being judged where it was
/// noticed: they share one benign cause (the generation the caller holds was retired,
/// its records mid-deletion), and deciding any of them locally would turn an ordinary
/// overwrite into a hard read failure for the reader racing it. That is why
/// [`read_group_range`] hands back a described [`GroupRange::Anomaly`] instead of raising
/// one: there is exactly ONE place in this module that answers "retired, or corrupt?".
async fn retired_or<T>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    group: &SegmentGroup,
    fault: ChunkMapError,
) -> Result<Resolution<T>> {
    match root_dropped(store, root_key, group).await? {
        // The root moved off this generation while it was being read: not this reader's
        // fault to raise, and which dropped arm it is was decided by what the re-read saw.
        Some(dropped) => Ok(dropped),
        // The root still names THIS generation, and either its own table is past the
        // reader's ceiling or its range does not hold exactly what that table says it
        // does. A live generation neither loses a segment nor grows one, so this is
        // corruption or a protocol violation — fail closed, for this object only.
        None => Err(fault.into()),
    }
}

/// The outcome of walking a segment group's own range.
enum GroupRange {
    /// Every row under the range, keyed by its **parsed** index — none past the root's
    /// claim.
    Rows(BTreeMap<u32, Bytes>),
    /// The range cannot be answered as the root's table describes it. Carried back as a
    /// *described* anomaly rather than raised here, so [`read_segments`] settles it
    /// through the one resolve-retry arbiter ([`retired_or`]).
    Anomaly(ChunkMapError),
}

/// Read one segment group's own range, `seg:<nonce>:<epoch>:`, in bounded pages — the
/// single place anything in this module reads a group's `seg:` records
/// (`0016:2393-2400`; never a global `seg:` scan, never another group's or another
/// epoch's).
///
/// [`MetadataStore::scan`] is complete-or-fail-loud at `SCAN_CAP`
/// (`crates/traits/src/lib.rs:286`): a damaged or half-retired generation whose range
/// holds more records than any root names would either buffer all of them or fail the
/// whole call with a STORE error, which per-object containment cannot catch — one
/// damaged object would then end a fleet-wide maintenance pass. So the range is read
/// with [`MetadataStore::scan_page`] (`crates/traits/src/lib.rs:1105`, #634/PR #645),
/// [`SEGMENT_PAGE_LIMIT`] rows at a time.
///
/// `accounted` — the root's own claim — is refused above [`MAX_ROOT_SEGMENTS`] **before
/// the first page is asked for** (`0016:2432-2440`). That refusal is *described* like
/// every other anomaly, not raised here: an over-ceiling table is one more shape a reader
/// can meet on a generation retired under it, and deciding it locally would answer
/// [`ChunkMapError::TooManySegments`] for an object whose live generation resolves
/// perfectly. Settling it costs one **root** `get` ([`retired_or`]) and never a range
/// read, so the refusal stays unread either way.
///
/// A row past the claim, a key this module's grammar refuses, or a value past
/// [`MAX_VALUE_BYTES`] ends the walk where it is met — so the rows this function keeps
/// are at most the root's claim, and the pages it asks for at most one more than
/// `claim / SEGMENT_PAGE_LIMIT`. (What the store has already spent materialising a page
/// it hands back is the seam's bound, not this walk's — getwyrd/wyrd#674.)
///
/// Rows are collected into a [`BTreeMap`] keyed by their **parsed** index — never the
/// order a page returned them in: the paging contract states byte-lexicographic key
/// order (`crates/traits/src/lib.rs:1105` clause 1), but the fixed-width key is a
/// debuggability property, not a licence for this resolver to lean on it.
async fn read_group_range(
    store: &dyn MetadataStore,
    group: &SegmentGroup,
    accounted: usize,
) -> Result<GroupRange> {
    if accounted > MAX_ROOT_SEGMENTS {
        return Ok(GroupRange::Anomaly(ChunkMapError::TooManySegments {
            segments: accounted,
        }));
    }
    let prefix = seg_range_prefix(group);
    let mut rows: BTreeMap<u32, Bytes> = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = store
            .scan_page(&prefix, after.as_deref(), SEGMENT_PAGE_LIMIT)
            .await?;
        for (key, value) in page {
            // A key under the range this module's own grammar cannot parse is an anomaly
            // of the range, NOT a verdict: `Ok(Anomaly)`, so the retired-versus-corrupt
            // call is made once, by `read_segments`, for every shape alike.
            let (nonce, epoch, index) = match parse_seg_key(&key) {
                Ok(parsed) => parsed,
                Err(malformed) => return Ok(GroupRange::Anomaly(malformed)),
            };
            // The range prefix already pins both, so a mismatch means the store answered
            // with a row outside the prefix it was asked for: the resolver pins the group
            // itself rather than trusting a backend's prefix handling, or another epoch's
            // segment could be spliced into this generation's map.
            if nonce != *group.nonce() || epoch != group.epoch() {
                return Ok(GroupRange::Anomaly(ChunkMapError::SegmentKeyMalformed {
                    key: String::from_utf8_lossy(&key).into_owned(),
                }));
            }
            // Checked BEFORE the row is kept, so the walk ends at the first row the
            // root's table cannot account for instead of reading the rest of the range.
            if usize::try_from(index).unwrap_or(usize::MAX) >= accounted {
                return Ok(GroupRange::Anomaly(ChunkMapError::SegmentUnknown {
                    nonce: group.nonce().to_string(),
                    epoch: group.epoch(),
                    index,
                }));
            }
            // A row no conforming publication wrote (`0016:1467` bounds a segment value
            // to V/2) and the tightest backend in play would have refused to store
            // (`crates/traits/src/lib.rs:995-999`): this resolver neither decodes nor
            // retains it. Described, not raised — an oversized row is one more shape a
            // retired generation can show a reader.
            if value.len() > MAX_VALUE_BYTES {
                return Ok(GroupRange::Anomaly(
                    ChunkMapError::SegmentValueOverCeiling {
                        index,
                        bytes: value.len(),
                        ceiling: MAX_VALUE_BYTES,
                    },
                ));
            }
            rows.insert(index, value);
        }
        // Clause 3 of the paging contract: `next` is `None` only when the prefix is
        // exhausted, and the cursor is exclusive, so the walk strictly advances.
        match next {
            Some(cursor) => after = Some(cursor),
            None => return Ok(GroupRange::Rows(rows)),
        }
    }
}

/// Decode a **root** record the resolver re-read for itself, attributing any failure —
/// structural or plain-unparsable bytes alike — to that object via
/// [`ChunkMapError::RootRecordUndecodable`].
///
/// The [`decode_segment_record`] rule for the other record a resolve reads, and it is here
/// for the same reason: every fault this module raises about an object's own bytes leaves
/// it as a *typed* chunk-map anomaly, so a consumer that must tell "this object is
/// unreadable" (contain it, keep walking) from "the store is failing" (end the pass) can.
/// A raw decoder error escaping here is indistinguishable from a backend outage at that
/// consumer, and reads as the wider fault — the whole store's, not the one record's.
fn decode_root_record(value: &[u8]) -> std::result::Result<InodeRecord, ChunkMapError> {
    decode::<InodeRecord>(value).map_err(|err| ChunkMapError::RootRecordUndecodable {
        detail: match err.downcast::<ChunkMapError>() {
            Ok(typed) => typed.to_string(),
            Err(err) => err.to_string(),
        },
    })
}

/// Decode one `seg:` value into its [`SegmentRecord`], attributing any failure —
/// structural or plain-unparsable bytes alike — to this one segment's index via
/// [`ChunkMapError::SegmentRecordUndecodable`], so [`retired_or`] has one typed anomaly
/// to arbitrate regardless of *why* the record could not be read.
fn decode_segment_record(
    index: u32,
    value: &[u8],
) -> std::result::Result<SegmentRecord, ChunkMapError> {
    decode::<SegmentRecord>(value).map_err(|err| {
        let detail = match err.downcast::<ChunkMapError>() {
            Ok(typed) => typed.to_string(),
            Err(err) => err.to_string(),
        };
        ChunkMapError::SegmentRecordUndecodable { index, detail }
    })
}

/// Read a segmented map's segments through the bounded per-group range
/// ([`read_group_range`]), settling every anomaly it can show through the single
/// resolve-retry arbiter ([`retired_or`]) rather than a bare refusal at whichever check
/// noticed it first. [`Resolution::Superseded`] / [`Resolution::Gone`] are the retired
/// arms, kept apart all the way out (a delete is not churn).
async fn read_segments(
    store: &dyn MetadataStore,
    root_key: &[u8],
    map: &SegmentedMap,
) -> Result<Resolution<BTreeMap<u32, SegmentRecord>>> {
    let group = map.group();
    let rows = match read_group_range(store, group, map.segment_count() as usize).await? {
        GroupRange::Anomaly(fault) => return retired_or(store, root_key, group, fault).await,
        GroupRange::Rows(rows) => rows,
    };
    let mut found: BTreeMap<u32, SegmentRecord> = BTreeMap::new();
    for (index, value) in rows {
        match decode_segment_record(index, &value) {
            Ok(record) => {
                found.insert(index, record);
            }
            Err(fault) => return retired_or(store, root_key, group, fault).await,
        }
    }
    for segment in map.segments() {
        let Some(record) = found.get(&segment.index) else {
            let absent = ChunkMapError::SegmentAbsent {
                nonce: group.nonce().to_string(),
                epoch: group.epoch(),
                index: segment.index,
            };
            return retired_or(store, root_key, group, absent).await;
        };
        if record.byte_offset() != segment.byte_offset || record.byte_len() != segment.byte_len {
            let mismatch = ChunkMapError::SegmentBoundsMismatch {
                index: segment.index,
                root: (segment.byte_offset, segment.byte_len),
                segment: (record.byte_offset(), record.byte_len()),
            };
            return retired_or(store, root_key, group, mismatch).await;
        }
    }
    // A resolution that read every segment can still be STALE: the root is read at one
    // instant and the `seg:` range at another, and a supersede that flipped the root in
    // between leaves the retired generation's records in place until the drain reaches
    // them (`0016:2452-2462` — the root always moves first). So the same re-read that
    // settles an absent segment also settles a COMPLETE read — one extra `get` per
    // segmented resolve, and only for the segmented shape (a flat map is one value, read
    // atomically with the root).
    if let Some(dropped) = root_dropped(store, root_key, group).await? {
        return Ok(dropped);
    }
    Ok(Resolution::Answer(found))
}

/// Resolve one **snapshot's** chunk map to its ordered chunk list, or the arm saying that
/// generation was retired under the read (decision 7(h)). A flat map resolves to a borrow
/// of the record and reads nothing.
async fn resolve_snapshot<'a>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &'a InodeRecord,
) -> Result<Resolution<Cow<'a, [ChunkRef]>>> {
    let map = match &record.chunk_map {
        ChunkMap::Flat(chunks) => return Ok(Resolution::Answer(Cow::Borrowed(chunks))),
        ChunkMap::Segmented(map) => map,
    };
    Ok(read_segments(store, root_key, map).await?.map(|segments| {
        let mut chunks = Vec::new();
        // Ordered by the segment's own parsed index — `BTreeMap`'s iteration order — so
        // the object's bytes are assembled in the order its table names, never the order a
        // page happened to answer in.
        for (_index, segment) in segments {
            chunks.extend(segment.into_chunks());
        }
        Cow::Owned(chunks)
    }))
}

/// Resolve a caller's own **committed snapshot** to its ordered chunk list — the entry
/// for a consumer that has just read the root itself
/// ([`read_object`](crate::read::read_object), the gateway's streaming and ranged reads).
///
/// A flat map costs no read at all; a segmented one costs the bounded range
/// `seg:<nonce>:<epoch>:` plus the one re-read that settles it (decision 7(e)).
/// `root_key` is the inode's own key ([`inode_key`]): the resolver needs the root's
/// *identity*, not just its decoded value, because an anomaly is settled by re-reading it
/// — an API taking only a store and an already-decoded record could not tell a concurrent
/// retirement from corruption.
///
/// If that generation was **superseded mid-resolve** the resolution is dropped and the
/// read restarts against the live root ([`resolve_current_chunk_map`]): an overwrite is
/// not a deletion, so answering "no such object" — or, worse, a torn half-map — for one is
/// the data loss decision 7(h) exists to prevent. If instead the root turned out to be
/// **gone**, the object was deleted under the read: that is already the final answer, so it
/// is returned as one rather than restarted onto a root just observed absent. `Ok(None)`
/// therefore means exactly one thing either way: the object has no live committed
/// generation.
pub async fn resolve_chunk_map<'a>(
    store: &dyn MetadataStore,
    root_key: &[u8],
    record: &'a InodeRecord,
) -> Result<Option<ResolvedChunkMap<'a>>> {
    match resolve_snapshot(store, root_key, record).await? {
        Resolution::Answer(chunks) => Ok(Some(ResolvedChunkMap {
            record: Cow::Borrowed(record),
            chunks,
        })),
        Resolution::Superseded => resolve_current_chunk_map(store, root_key).await,
        Resolution::Gone => Ok(None),
    }
}

/// Resolve the chunk map of the root the store holds **now**, reading the root itself —
/// for a caller whose own record may already be stale (a maintenance pass's scan
/// snapshot), and the restart [`resolve_chunk_map`] takes when its caller's generation
/// was retired under it.
///
/// `Ok(None)` means there is no **live committed generation**: the root is absent
/// (deleted) or not `Committed` — the same condition every consumer already reads as "no
/// such object". A generation **superseded** mid-resolve is retried up to
/// [`MAX_RESOLVE_RESTARTS`] times, then fails closed with a typed error: giving up must
/// never be spelled "this object owns no bytes". A **deletion** met mid-resolve is not
/// churn and spends no attempt — there is no successor generation to restart onto, so it
/// answers `Ok(None)` where it is seen, on the last allowed attempt exactly as on the
/// first.
///
/// A root the restart lands on whose bytes will not **decode** — the object was replaced
/// under the read by a record this build cannot parse — is likewise typed
/// ([`ChunkMapError::RootRecordUndecodable`]), so it stays this object's fault at every
/// consumer rather than arriving as an unattributable error.
pub async fn resolve_current_chunk_map(
    store: &dyn MetadataStore,
    root_key: &[u8],
) -> Result<Option<ResolvedChunkMap<'static>>> {
    for _ in 0..MAX_RESOLVE_RESTARTS {
        let Some(bytes) = store.get(root_key).await? else {
            return Ok(None);
        };
        // Typed, not raw: the root this restart landed on is one object's record, so bytes
        // that will not parse are that object's fault — see [`decode_root_record`].
        let record = decode_root_record(&bytes)?;
        if record.state != InodeState::Committed {
            return Ok(None);
        }
        match resolve_snapshot(store, root_key, &record).await? {
            Resolution::Answer(chunks) => {
                return Ok(Some(ResolvedChunkMap {
                    chunks: Cow::Owned(chunks.into_owned()),
                    record: Cow::Owned(record),
                }))
            }
            // Deleted under the resolve: the same answer the absent-root check above
            // gives, and it must be given HERE — deferring it to another attempt would
            // spend a restart that cannot succeed, and on the last one would report a
            // plain delete as `MapResolutionUnstable`.
            Resolution::Gone => return Ok(None),
            // Overwritten under the resolve: re-read the root and resolve whatever
            // generation replaced it.
            Resolution::Superseded => {}
        }
    }
    Err(ChunkMapError::MapResolutionUnstable {
        attempts: MAX_RESOLVE_RESTARTS,
    }
    .into())
}

// ---------------------------------------------------------------------------
// The placement move — the maintenance WRITE side of a resolved chunk map
// (proposal 0016 decision 7(f))
// ---------------------------------------------------------------------------

/// What preparing a placement move produced ([`repoint_chunk`]).
///
/// **No arm has written anything.** The move reads, weighs and builds; only the caller's
/// own [`MetadataStore::commit`] of a [`Self::Prepared`] batch changes the store, so a
/// refusal and a conflict alike leave every record byte-identical.
#[derive(Debug)]
pub enum Repoint {
    /// The move as ONE compare-and-swap batch, carrying the three pins [`repoint_chunk`]
    /// documents.
    ///
    /// Handed back rather than committed so the caller can add **its own evidence for the
    /// same move** — an obligation's delete, an orphan mark per displaced fragment — and
    /// land all of it in ONE atomic mutation (`0005:277`, ADR-0015). Committing the
    /// placement and the evidence separately would leave a window in which a fragment has
    /// moved and nothing records where it went.
    Prepared(WriteBatch),
    /// The record the move would leave behind crosses the value ceiling every backend
    /// inherits ([`flat_value_ceiling_crossed`]): refused before anything was written.
    /// Weighed on the generation the caller planned from alone — a refusal carries no
    /// batch, so no pin confirms the root still names it. While it does, this is not
    /// transient: it fails every pass until the record shrinks, an operator signal rather
    /// than a retry — so confirm the generation is still current (a fresh resolve) before
    /// escalating; on one the root has since left, the next plan ends it.
    Refused {
        /// The re-encoded record's own length.
        bytes: usize,
        /// The ceiling it crossed.
        ceiling: usize,
    },
    /// The flat record's `version` is already `u64::MAX`: refused before anything was
    /// written rather than wrapped, since a wrapped version is a smaller number a stale
    /// reader could mistake for an older generation. Like [`Self::Refused`], judged on the
    /// planned generation alone: the object's own state rather than a race, so not
    /// transient while that generation is live.
    VersionExhausted {
        /// The version that cannot be advanced.
        version: u64,
    },
    /// The chunk is no longer where, or what, the caller planned from: no chunk starting at
    /// the offset equals the planned [`ChunkRef`], or the generation was retired under the
    /// plan. Nothing is written — a stale plan never lands over a newer placement; the
    /// caller keeps its obligation and re-plans next pass.
    Conflict,
}

/// [`repoint_chunk`] was handed a replacement placement that does not name exactly one D
/// server per fragment of the chunk it moves. A fault of the **call**, not of the object —
/// so deliberately not a [`ChunkMapError`], which a maintenance loop contains as "this
/// object is unreadable": a planner that builds a wrong-length vector surfaces as a bug.
/// Stricter than [`ChunkRef::placement_is_valid`], which admits the empty vector for pre-M3
/// records: a move says where every fragment now lives, and an empty or short vector would
/// be identity-filled on read ([`ChunkRef::placed_dserver`]) to servers the fragments are
/// not on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedReplacement {
    /// The chunk's fragment count ([`ChunkRef::fragment_count`]).
    pub expected: u16,
    /// The length of the placement the move was handed.
    pub actual: usize,
}

impl fmt::Display for MalformedReplacement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { expected, actual } = self;
        write!(f, "replacement needs {expected} D servers, got {actual}")
    }
}

impl std::error::Error for MalformedReplacement {}

/// Move ONE chunk's fragment `placement` in whichever record holds its [`ChunkRef`] — the
/// flat inode record, or the one `seg:` record of a segmented map — and hand back the
/// compare-and-swap batch that lands it ([`Repoint`]). The write counterpart of
/// [`resolve_chunk_map`]: without it a chunk living in a `seg:` record can be read by every
/// consumer and repaired by none.
///
/// # Addressing
///
/// `byte_offset` is the first object byte the chunk covers and `prior` is the reference the
/// caller planned from. The chunk moved is the one that **begins at that offset and equals
/// `prior`** (`chunk_at`); anything else is [`Repoint::Conflict`]. A zero-length chunk can
/// begin exactly where a segment ends, so both segments touching that offset are candidates
/// (`segment_may_hold`) and equality, not the offset, decides which record is written.
///
/// # The three pins
///
/// 1. The **root generation's** bytes, `encode(generation)`, in both arms: a supersede
///    moves the root first (`0016:2452-2462`), so a move racing one loses the CAS. The
///    segmented arm never `put`s the root — its placement lives in the segment — so the
///    root's bytes are unchanged by the move.
/// 2. In the segmented arm, the **segment record's own bytes as read here** — a fresh
///    read, not the bytes the caller's resolve saw ([`ResolvedChunkMap`] keeps only the
///    flattened chunks). A concurrent edit to a *sibling* chunk that landed before this
///    read is therefore merged; one that lands after it fails the CAS.
/// 3. The **`ChunkRef` itself**: the moved chunk must equal `prior`, placement included,
///    so an edit to the planned chunk is a [`Repoint::Conflict`] rather than an overwrite.
///
/// The flat arm is [`commit_chunk_map`]'s idiom — the next version and `..generation.clone()`,
/// so ADR-0047 metadata is preserved — with the version advanced **checked**
/// ([`Repoint::VersionExhausted`]) and `state` left as it was: a move is not a publication,
/// so unlike [`commit_chunk_map`] it never marks a generation `Committed`.
///
/// The re-encoded record — flat root or segment — is weighed against the full
/// [`MAX_VALUE_BYTES`] through [`flat_value_ceiling_crossed`] before anything is built.
///
/// `placement` must name one D server per fragment of `prior`, or the call fails with
/// [`MalformedReplacement`] before anything is read.
///
/// # Anomalies
///
/// A candidate segment record that is absent, is larger than [`MAX_VALUE_BYTES`] (weighed
/// before it is decoded, as `read_group_range` weighs a row), will not decode, or disagrees
/// with the root's table goes through the resolver's own arbiter (`retired_or`): a retired
/// generation is a [`Repoint::Conflict`], one the root still names is structural
/// corruption and surfaces as the typed [`ChunkMapError`] — never a silent skip, and never
/// a rewrite of a record the read side refuses.
///
/// # Bounded
///
/// Segments are found in the root's own table; no `seg:` range is walked. At most two
/// records are read (two only for a zero-length chunk on a segment boundary), one at a time.
///
/// deferred: #777 — the living architecture doc (`06-runtime-view.md` §6.3,
/// `08-crosscutting-concepts.md` §8.7) describes what the maintenance loops **do**, and
/// nothing calls this yet. It moves with the custodian wiring in #777, which changes that.
pub async fn repoint_chunk(
    store: &dyn MetadataStore,
    inode: InodeId,
    generation: &InodeRecord,
    byte_offset: u64,
    prior: &ChunkRef,
    placement: Vec<DServerId>,
) -> Result<Repoint> {
    let expected = prior.fragment_count();
    if placement.len() != usize::from(expected) {
        return Err(MalformedReplacement {
            expected,
            actual: placement.len(),
        }
        .into());
    }
    let root_key = inode_key(inode);
    let root_pin = WriteBatch::new().require(root_key.clone(), encode(generation));
    let map = match &generation.chunk_map {
        ChunkMap::Flat(chunks) => {
            let Some(at) = chunk_at(chunks, byte_offset, prior) else {
                return Ok(Repoint::Conflict);
            };
            let Some(version) = generation.version.checked_add(1) else {
                return Ok(Repoint::VersionExhausted {
                    version: generation.version,
                });
            };
            let mut next_chunks = chunks.clone();
            next_chunks[at].placement = placement;
            let next = InodeRecord {
                chunk_map: ChunkMap::Flat(next_chunks),
                version,
                ..generation.clone()
            };
            return Ok(weighed(root_pin, root_key, encode(&next)));
        }
        ChunkMap::Segmented(map) => map,
    };
    let group = map.group();
    for segment in map
        .segments()
        .iter()
        .filter(|segment| segment_may_hold(segment, byte_offset, prior.len))
    {
        // Infallible for a table that decoded: `SegmentedMap::new` refused every index
        // this could reject.
        let key = seg_key(group, segment.index)?;
        let fault = match store.get(&key).await? {
            None => ChunkMapError::SegmentAbsent {
                nonce: group.nonce().to_string(),
                epoch: group.epoch(),
                index: segment.index,
            },
            // `read_group_range`'s own refusal: never decoded, so never rewritten either.
            Some(bytes) if bytes.len() > MAX_VALUE_BYTES => {
                ChunkMapError::SegmentValueOverCeiling {
                    index: segment.index,
                    bytes: bytes.len(),
                    ceiling: MAX_VALUE_BYTES,
                }
            }
            Some(bytes) => match decode_segment_record(segment.index, &bytes) {
                Err(undecodable) => undecodable,
                Ok(record)
                    if record.byte_offset() != segment.byte_offset
                        || record.byte_len() != segment.byte_len =>
                {
                    ChunkMapError::SegmentBoundsMismatch {
                        index: segment.index,
                        root: (segment.byte_offset, segment.byte_len),
                        segment: (record.byte_offset(), record.byte_len()),
                    }
                }
                Ok(record) => {
                    // `segment_may_hold` admitted only offsets at or past the start.
                    let within = byte_offset - segment.byte_offset;
                    let Some(at) = chunk_at(record.chunks(), within, prior) else {
                        continue;
                    };
                    let mut chunks = record.into_chunks();
                    chunks[at].placement = placement;
                    let next = SegmentRecord::new(chunks, segment.byte_offset)?;
                    return Ok(weighed(
                        root_pin.require(key.clone(), bytes),
                        key,
                        encode(&next),
                    ));
                }
            },
        };
        // Only a dropped generation comes back `Ok`; one the root still names is `Err`.
        retired_or::<()>(store, &root_key, group, fault).await?;
        return Ok(Repoint::Conflict);
    }
    Ok(Repoint::Conflict)
}

/// Finish a move: refuse `next` past the value ceiling, else add its `put` to `pins`.
fn weighed(pins: WriteBatch, key: Vec<u8>, next: Bytes) -> Repoint {
    match flat_value_ceiling_crossed(&next) {
        Some(ceiling) => Repoint::Refused {
            bytes: next.len(),
            ceiling,
        },
        None => Repoint::Prepared(pins.put(key, next)),
    }
}

/// Whether `segment` can hold a chunk of length `len` that begins at object byte
/// `byte_offset`: the offset falls inside the segment, or — for a zero-length chunk
/// only — exactly on its end, where a trailing empty chunk begins. A chunk with bytes
/// lives only in the segment its first byte falls in. Total: the table's spans are
/// checked at decode, so nothing here overflows.
fn segment_may_hold(segment: &SegmentRef, byte_offset: u64, len: u64) -> bool {
    let Some(within) = byte_offset.checked_sub(segment.byte_offset) else {
        return false;
    };
    within < segment.byte_len || (len == 0 && within == segment.byte_len)
}

/// The position in `chunks` of the chunk that **begins at `byte_offset`** and **equals
/// `prior`** — the whole addressing rule of a placement move, shared by both shapes. The
/// offset is an address a caller can carry across a segment boundary it never saw; the
/// equality is the pin that turns a map rewritten under the plan into a conflict instead
/// of an overwrite. Several chunks can begin at one offset only when some are empty, so
/// equality — not position — picks among them.
fn chunk_at(chunks: &[ChunkRef], byte_offset: u64, prior: &ChunkRef) -> Option<usize> {
    let mut at = 0u64;
    for (index, chunk) in chunks.iter().enumerate() {
        if at > byte_offset {
            break;
        }
        if at == byte_offset && chunk == prior {
            return Some(index);
        }
        at = at.checked_add(chunk.len)?;
    }
    None
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
    fn the_value_ceiling_admits_the_boundary_and_refuses_only_past_it() {
        // BOTH sides pinned here, in the crate that owns the constant — the maintenance
        // loops that call this live in another crate, so a boundary drifting by one byte
        // would be invisible to this crate's own tests, and either direction of that drift
        // is a durability fault. Refusing AT the ceiling makes a legal record unwritable (an
        // object whose placement can never be repaired, `:333-341`); admitting one byte past
        // it commits a value the tightest backend refuses, the same fault by the other road.
        assert_eq!(
            flat_value_ceiling_crossed(&vec![b'x'; MAX_VALUE_BYTES]),
            None
        );
        assert_eq!(
            flat_value_ceiling_crossed(&vec![b'x'; MAX_VALUE_BYTES + 1]),
            Some(MAX_VALUE_BYTES)
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
                        owner: None,
                        staged: None,
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
        // GC reclaims from. A map's chunks live in `seg:` records nothing in this build can
        // enumerate (#649), so an unlink that proceeded would unbind the object while
        // orphaning NOTHING: every fragment it owns would end up referenced by no chunk map
        // and deadlined by no grace record, which is exactly the
        // unreferenced-but-undeadlined state GC keeps forever. Permanently leaked bytes
        // that no record names — the failure mode C-1 forbids. So the shape is judged
        // BEFORE the commit and the binding survives.
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

    /// The unit-level statement of [`high_water_marks`]'s totality — the acceptance target
    /// `crates/server/tests/gateway_recover_totality.rs` binds the same property end to end
    /// through `Gateway::recover()`, including the attribution this test does not read.
    ///
    /// This **replaces** `high_water_marks_refuses_a_segmented_root_rather_than_re_mint_its_
    /// chunk_ids`, and the replacement is deliberate rather than a dropped guard. That test
    /// reasoned that a segmented root read as "owns no chunks" would contribute nothing to
    /// `max_chunk`, letting the next PUT mint an id the root's fragments already occupy. The
    /// premise expired with #487 (`fdd34f1`, 2026-07-08, merged): the hazard needs a minter
    /// allocating in the `< 2^64` space that scan recovered, and #487 removed the last one —
    /// `mint_chunk_id` now mints ≥ 2^127 (`crates/server/src/lib.rs:246-258`) and the cluster
    /// path's `chunk_id_minter` ≥ 2^64 (`crates/server/src/cli.rs:1809-1816`), while the same
    /// commit rewrote `recover` to discard the floor. The chunk mark itself is now gone
    /// (issue #652) — a number nobody reads is not a safety property. Its live half is kept
    /// here, inverted to what totality requires: the segmented root must be **contained**,
    /// contributing its key-derived id rather than ending the walk.
    #[test]
    fn high_water_marks_is_total_over_records_it_cannot_read() {
        // An empty store still yields 0, so the mark below is the records' own and not a
        // constant this walk invents.
        assert_eq!(pollster::block_on(high_water_marks(&store())).unwrap(), 0);

        // A healthy flat record, a structurally valid SEGMENTED root this function has no
        // resolver for, raw bytes that are not a record at all, and a row under the prefix
        // whose key names no id — the segmented root and the undecodable value both at ids
        // ABOVE the healthy one, so the mark can only be right if each contributed its own
        // key-derived id, and the unparsable key last, so a walk that stopped there would
        // still be caught.
        let damaged = store();
        for (key, value) in [
            (inode_key(2), FLAT_ROOT),
            (inode_key(17), SEGMENTED_ROOT_OK),
            (inode_key(41), b"not a metadata record".as_slice()),
            (b"inode:not-an-id".to_vec(), FLAT_ROOT),
        ] {
            pollster::block_on(damaged.commit(WriteBatch::new().put(key, value))).unwrap();
        }
        assert_eq!(
            pollster::block_on(high_water_marks(&damaged)).unwrap(),
            41,
            "every stored record contributes its key-derived id and none ends the walk — a \
             record this build cannot read must raise the floor, never silently lower it",
        );
    }
}

/// The `orphan:` value codec ([`OrphanMark`]): each of the three shapes round-trips byte for
/// byte, the legacy shape is exactly what every existing writer spells, and every value outside
/// the encoder's image is refused rather than read. Co-located because the custodian's
/// discriminator (`crates/custodian/tests/gc_reclaim_intent.rs`) names no symbol this codec adds.
#[cfg(test)]
mod orphan_mark_codec {
    use super::*;
    use crate::multipart::{RecordError, RetireToken, UploadId};

    fn round_trips(mark: &OrphanMark, stored: &[u8]) {
        assert_eq!(
            encode_orphan_mark(mark).as_ref(),
            stored,
            "{mark:?} encodes"
        );
        assert_eq!(
            decode_orphan_mark(stored).as_ref(),
            Ok(mark),
            "{:?} decodes",
            String::from_utf8_lossy(stored)
        );
    }

    #[test]
    fn the_legacy_shape_is_every_existing_writers_bare_decimal() {
        for at in [0, 1, 60_000, 1_700_000_000_000, u64::MAX] {
            // `unlink`, the superseding commits, `mark_orphaned`, restore and the repair loops
            // all write `orphaned_at_millis.to_string()`.
            round_trips(&OrphanMark::legacy(at), at.to_string().as_bytes());
        }
        let mark = decode_orphan_mark(b"42").unwrap();
        assert_eq!(
            (
                mark.orphaned_at_millis(),
                mark.event(),
                mark.is_reclaiming()
            ),
            (42, None, false)
        );
    }

    #[test]
    fn the_structured_and_reclaiming_shapes_round_trip_byte_for_byte() {
        let structured = OrphanMark::structured(7, "g:1:2").unwrap();
        round_trips(&structured, br#"{"orphaned_at_millis":7,"event":"g:1:2"}"#);
        round_trips(
            &structured.clone().into_reclaiming(),
            br#"{"orphaned_at_millis":7,"event":"g:1:2","reclaiming":true}"#,
        );
        // A legacy mark GC replaces keeps its stamp and has no event to carry.
        round_trips(
            &OrphanMark::legacy(7).into_reclaiming(),
            br#"{"orphaned_at_millis":7,"reclaiming":true}"#,
        );
        let reclaiming = structured.into_reclaiming();
        assert_eq!(
            (
                reclaiming.orphaned_at_millis(),
                reclaiming.event(),
                reclaiming.is_reclaiming()
            ),
            (7, Some("g:1:2"), true),
            "the reclaiming state disturbs neither the stamp nor the event"
        );
    }

    #[test]
    fn a_value_outside_the_encoders_image_is_refused() {
        let over_long = format!(
            r#"{{"orphaned_at_millis":7,"event":"{}"}}"#,
            "e".repeat(MAX_ORPHAN_EVENT_LEN + 1)
        );
        // The event `g:1:2` with its first letter written as a JSON unicode escape (a backslash,
        // `u`, then `0067`): JSON reads the same string, the codec does not write it.
        let escaped = format!(
            r#"{{"orphaned_at_millis":7,"event":"{}u0067:1:2"}}"#,
            char::from(92)
        );
        for value in [
            // Not a mark of any shape.
            b"".as_slice(),
            b"not an instant",
            b"-1",
            b"+7",
            b"1.5",
            b" 7",
            b"18446744073709551616",
            br#"{"orphaned_at_millis":"7","event":"g:1:2"}"#,
            br#"{"orphaned_at_millis":7,"event":"g:1:2","extra":1}"#,
            br#"{"event":"g:1:2"}"#,
            br#"[7,"g:1:2"]"#,
            br#"{"orphaned_at_millis":7,"event":""}"#,
            br#"{"orphaned_at_millis":7,"event":"g 1"}"#,
            br#"{"orphaned_at_millis":7,"event":"g\"1"}"#,
            over_long.as_bytes(),
            // A mark, but not in this codec's spelling.
            b"007",
            br#"{"orphaned_at_millis":7}"#,
            br#"{"orphaned_at_millis":7,"event":null}"#,
            br#"{"orphaned_at_millis":7,"event":"g:1:2","reclaiming":false}"#,
            br#"{"event":"g:1:2","orphaned_at_millis":7}"#,
            br#"{"orphaned_at_millis": 7,"event":"g:1:2"}"#,
            escaped.as_bytes(),
            br#"{"orphaned_at_millis":7,"reclaiming":true,"event":"g:1:2"}"#,
        ] {
            assert!(
                matches!(
                    decode_orphan_mark(value),
                    Err(RecordError::MalformedRecordValue {
                        namespace: "orphan:",
                        ..
                    } | RecordError::NoncanonicalRecordValue {
                        namespace: "orphan:"
                    })
                ),
                "{:?} decoded as a mark: {:?}",
                String::from_utf8_lossy(value),
                decode_orphan_mark(value)
            );
        }
        assert_eq!(
            decode_orphan_mark(b"007"),
            Err(RecordError::NoncanonicalRecordValue {
                namespace: "orphan:"
            }),
            "a leading zero reads as 7 and is refused as a second spelling of it"
        );
    }

    #[test]
    fn a_writer_cannot_encode_what_the_decoder_refuses() {
        for event in [
            String::new(),
            "g 1".to_string(),
            "g\"1".to_string(),
            "g\\1".to_string(),
            "é".to_string(),
            "e".repeat(MAX_ORPHAN_EVENT_LEN + 1),
        ] {
            assert!(
                OrphanMark::structured(7, event.clone()).is_err(),
                "event {event:?} must be refused at construction"
            );
        }
        // The largest mark any writer can produce: the bound `MAX_ORPHAN_EVENT_LEN` states.
        let largest = OrphanMark::structured(u64::MAX, "e".repeat(MAX_ORPHAN_EVENT_LEN))
            .unwrap()
            .into_reclaiming();
        let bytes = encode_orphan_mark(&largest);
        assert_eq!(bytes.len(), 328);
        assert_eq!(decode_orphan_mark(&bytes), Ok(largest));
    }

    #[test]
    fn an_event_names_a_retirement_only_in_the_retire_key_grammars_spelling() {
        let token = |event: &str| OrphanMark::structured(0, event).unwrap().retire_token();
        assert_eq!(
            token("g:9:2"),
            Some(RetireToken::Generation {
                inode: 9,
                version: 2
            })
        );
        let upload = "0123456789abcdef0123456789abcdef";
        assert_eq!(
            token(&format!("s:{upload}:3")),
            Some(RetireToken::Session {
                upload_id: UploadId::new(upload).unwrap(),
                epoch: 3,
                part: None,
            })
        );
        // A per-move nonce, an owned-staging walk's identity, a second spelling of a token.
        for event in ["move-8045", &format!("{upload}:3"), "g:09:2", "g:9:2:1"] {
            assert_eq!(token(event), None, "{event:?} names no retirement");
        }
        assert_eq!(OrphanMark::legacy(0).retire_token(), None);
        assert_eq!(
            OrphanMark::structured(0, "g:9:2")
                .unwrap()
                .into_reclaiming()
                .retire_token(),
            Some(RetireToken::Generation {
                inode: 9,
                version: 2
            })
        );
    }
}

/// [`repoint_chunk`]: each arm, each pin, the ceiling, and the two addressing helpers.
/// Every test builds the batch and applies it to a real (in-memory redb) store that the
/// test mutates by hand in between, so "nothing written" is a byte comparison of the whole
/// store rather than an assumption about the batch.
#[cfg(test)]
mod placement_move {
    use super::*;
    use wyrd_testkit::Sim;

    const NONCE: &str = "0123456789abcdef0123456789abcdef";
    const INODE: InodeId = 7;

    fn store() -> wyrd_metadata_redb::RedbMetadataStore {
        wyrd_metadata_redb::RedbMetadataStore::in_memory().expect("in-memory redb store")
    }

    fn chunk(id: ChunkId, len: u64, placement: Vec<DServerId>) -> ChunkRef {
        ChunkRef {
            id,
            scheme: EcScheme::None,
            len,
            placement,
        }
    }

    fn put(store: &dyn MetadataStore, key: Vec<u8>, value: impl Into<Bytes>) {
        let outcome = pollster::block_on(store.commit(WriteBatch::new().put(key, value)));
        assert_eq!(outcome.unwrap(), CommitOutcome::Committed);
    }

    fn get(store: &dyn MetadataStore, key: &[u8]) -> Option<Bytes> {
        pollster::block_on(store.get(key)).unwrap()
    }

    /// Every key and value in the store — "nothing was written" is this, unchanged.
    fn snapshot(store: &dyn MetadataStore) -> Vec<(Vec<u8>, Bytes)> {
        pollster::block_on(store.scan(b"")).unwrap()
    }

    fn repoint(
        store: &dyn MetadataStore,
        generation: &InodeRecord,
        byte_offset: u64,
        prior: &ChunkRef,
        placement: Vec<DServerId>,
    ) -> Result<Repoint> {
        pollster::block_on(repoint_chunk(
            store,
            INODE,
            generation,
            byte_offset,
            prior,
            placement,
        ))
    }

    fn prepared(repoint: Result<Repoint>) -> WriteBatch {
        match repoint.expect("the move is prepared") {
            Repoint::Prepared(batch) => batch,
            other => panic!("expected a prepared batch, got {other:?}"),
        }
    }

    fn commit(store: &dyn MetadataStore, batch: WriteBatch) -> CommitOutcome {
        pollster::block_on(store.commit(batch)).unwrap()
    }

    fn chunk_map_error(result: Result<Repoint>) -> ChunkMapError {
        let err = result.expect_err("the move must fail closed");
        *err.downcast::<ChunkMapError>()
            .expect("a structural fault surfaces as a typed ChunkMapError")
    }

    fn flat_root(chunks: Vec<ChunkRef>, version: u64) -> InodeRecord {
        InodeRecord {
            size: checked_chunk_bytes(&chunks).unwrap(),
            chunk_map: ChunkMap::Flat(chunks),
            state: InodeState::Committed,
            version,
            etag: Some("\"e\"".into()),
            content_type: Some("text/plain".into()),
            modified: Some(1_700_000_000_000),
        }
    }

    fn group() -> SegmentGroup {
        SegmentGroup::new(NONCE, 1).unwrap()
    }

    /// A segmented root over `segments`, each `(byte_offset, chunks)`, with every segment
    /// record stored. Returns the root as stored.
    fn seed_segmented(store: &dyn MetadataStore, segments: &[(u64, Vec<ChunkRef>)]) -> InodeRecord {
        let mut table = Vec::new();
        for (index, (byte_offset, chunks)) in segments.iter().enumerate() {
            let record = SegmentRecord::new(chunks.clone(), *byte_offset).unwrap();
            table.push(SegmentRef {
                index: index as u32,
                byte_offset: *byte_offset,
                byte_len: record.byte_len(),
            });
            put(
                store,
                seg_key(&group(), index as u32).unwrap(),
                encode(&record),
            );
        }
        let map = SegmentedMap::new(group(), table).unwrap();
        let root = InodeRecord {
            size: map.span(),
            chunk_map: ChunkMap::Segmented(map),
            ..flat_root(Vec::new(), 3)
        };
        put(store, inode_key(INODE), encode(&root));
        root
    }

    fn segment(store: &dyn MetadataStore, index: u32) -> SegmentRecord {
        decode(&get(store, &seg_key(&group(), index).unwrap()).unwrap()).unwrap()
    }

    /// seg 0 = [A] over 0..5; seg 1 = [B, C] over 5..12 (C begins at 8).
    fn a() -> ChunkRef {
        chunk(1, 5, vec![10])
    }
    fn b() -> ChunkRef {
        chunk(2, 3, vec![20])
    }
    fn c() -> ChunkRef {
        chunk(3, 4, vec![30])
    }
    fn two_segments(store: &dyn MetadataStore) -> InodeRecord {
        seed_segmented(store, &[(0, vec![a()]), (5, vec![b(), c()])])
    }

    #[test]
    fn flat_arm_moves_the_placement_and_advances_the_version_preserving_metadata() {
        let store = store();
        // `Pending`, and it stays so: a move is not a publication.
        let root = InodeRecord {
            state: InodeState::Pending,
            ..flat_root(vec![a(), b()], 3)
        };
        put(&store, inode_key(INODE), encode(&root));

        let batch = prepared(repoint(&store, &root, 5, &b(), vec![99]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);

        let stored: InodeRecord = decode(&get(&store, &inode_key(INODE)).unwrap()).unwrap();
        assert_eq!(
            stored,
            InodeRecord {
                chunk_map: ChunkMap::Flat(vec![a(), chunk(2, 3, vec![99])]),
                version: 4,
                ..root
            },
            "only B's placement and the version move; size, state and ADR-0047 metadata stay"
        );
    }

    #[test]
    fn flat_arm_refuses_a_version_it_cannot_advance() {
        let store = store();
        let root = flat_root(vec![a()], u64::MAX);
        put(&store, inode_key(INODE), encode(&root));
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 0, &a(), vec![99]),
            Ok(Repoint::VersionExhausted { version: u64::MAX })
        ));
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn flat_arm_conflicts_on_a_changed_chunk_and_on_a_superseded_root() {
        let store = store();
        let root = flat_root(vec![a(), b()], 3);
        put(&store, inode_key(INODE), encode(&root));
        // The planned chunk is not the one at that offset (placement differs, or wrong offset).
        let edited = chunk(2, 3, vec![21]);
        assert!(matches!(
            repoint(&store, &root, 5, &edited, vec![99]),
            Ok(Repoint::Conflict)
        ));
        assert!(matches!(
            repoint(&store, &root, 0, &b(), vec![99]),
            Ok(Repoint::Conflict)
        ));
        // Planned against `root`, which is then superseded: the root pin fails the batch.
        let batch = prepared(repoint(&store, &root, 5, &b(), vec![99]));
        put(&store, inode_key(INODE), encode(&flat_root(vec![a()], 4)));
        let before = snapshot(&store);
        assert_eq!(commit(&store, batch), CommitOutcome::Conflict);
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn segmented_arm_rewrites_only_the_covering_segment_and_never_the_root() {
        let store = store();
        let root = two_segments(&store);
        let root_bytes = get(&store, &inode_key(INODE)).unwrap();
        // Segment 0 is garbage: were it read or decoded, the move would fail.
        put(
            &store,
            seg_key(&group(), 0).unwrap(),
            b"not a segment".to_vec(),
        );

        // C inside segment 1; then B at object byte 5 — segment 1's first byte, and exactly
        // where segment 0 ends: a chunk with bytes is looked for in segment 1 alone.
        for (offset, prior) in [(8, c()), (5, b())] {
            let batch = prepared(repoint(&store, &root, offset, &prior, vec![99]));
            assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        }

        assert_eq!(
            segment(&store, 1).chunks(),
            &[chunk(2, 3, vec![99]), chunk(3, 4, vec![99])][..]
        );
        assert_eq!(get(&store, &inode_key(INODE)).unwrap(), root_bytes);
        assert_eq!(
            get(&store, &seg_key(&group(), 0).unwrap())
                .unwrap()
                .as_ref(),
            b"not a segment"
        );
    }

    #[test]
    fn a_sibling_edit_is_merged_and_an_edit_to_the_planned_chunk_conflicts() {
        let store = store();
        let root = two_segments(&store);
        let key = seg_key(&group(), 1).unwrap();
        let sibling = chunk(2, 3, vec![21]);
        // After the plan, a concurrent writer moved sibling B: the move reads the record
        // fresh, so it lands on top of that edit.
        put(
            &store,
            key.clone(),
            encode(&SegmentRecord::new(vec![sibling.clone(), c()], 5).unwrap()),
        );
        let batch = prepared(repoint(&store, &root, 8, &c(), vec![99]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        assert_eq!(
            segment(&store, 1).chunks(),
            &[sibling.clone(), chunk(3, 4, vec![99])][..]
        );

        // The planned chunk itself was edited: conflict, nothing written.
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 8, &c(), vec![98]),
            Ok(Repoint::Conflict)
        ));
        assert_eq!(snapshot(&store), before);

        // An edit landing between the read and the commit fails the segment pin.
        let planned = chunk(3, 4, vec![99]);
        let batch = prepared(repoint(&store, &root, 8, &planned, vec![97]));
        put(
            &store,
            key,
            encode(&SegmentRecord::new(vec![b(), planned], 5).unwrap()),
        );
        let before = snapshot(&store);
        assert_eq!(commit(&store, batch), CommitOutcome::Conflict);
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn a_superseded_root_fails_the_batch_or_the_move() {
        let store = store();
        let root = two_segments(&store);
        let batch = prepared(repoint(&store, &root, 8, &c(), vec![99]));
        // The root flips to another generation; the old records are still there.
        put(&store, inode_key(INODE), encode(&flat_root(vec![a()], 4)));
        let before = snapshot(&store);
        assert_eq!(commit(&store, batch), CommitOutcome::Conflict);
        assert_eq!(snapshot(&store), before);

        // Its segment record already reclaimed: a retirement, answered as a conflict.
        let gone = WriteBatch::new().delete(seg_key(&group(), 1).unwrap());
        assert_eq!(commit(&store, gone), CommitOutcome::Committed);
        assert!(matches!(
            repoint(&store, &root, 8, &c(), vec![99]),
            Ok(Repoint::Conflict)
        ));
        // The object deleted outright: likewise.
        let deleted = WriteBatch::new().delete(inode_key(INODE));
        assert_eq!(commit(&store, deleted), CommitOutcome::Committed);
        assert!(matches!(
            repoint(&store, &root, 8, &c(), vec![99]),
            Ok(Repoint::Conflict)
        ));
    }

    #[test]
    fn a_damaged_segment_of_the_live_generation_is_structural_corruption() {
        let store = store();
        let root = two_segments(&store);
        let key = seg_key(&group(), 1).unwrap();

        put(&store, key.clone(), b"{\"chunks\":".to_vec());
        assert!(matches!(
            chunk_map_error(repoint(&store, &root, 8, &c(), vec![99])),
            ChunkMapError::SegmentRecordUndecodable { index: 1, .. }
        ));

        // Same length, shifted start.
        put(
            &store,
            key.clone(),
            encode(&SegmentRecord::new(vec![b(), c()], 6).unwrap()),
        );
        assert_eq!(
            chunk_map_error(repoint(&store, &root, 8, &c(), vec![99])),
            ChunkMapError::SegmentBoundsMismatch {
                index: 1,
                root: (5, 7),
                segment: (6, 7),
            }
        );

        // Same start, different length.
        put(
            &store,
            key.clone(),
            encode(&SegmentRecord::new(vec![b(), chunk(3, 5, vec![30])], 5).unwrap()),
        );
        assert_eq!(
            chunk_map_error(repoint(&store, &root, 8, &c(), vec![99])),
            ChunkMapError::SegmentBoundsMismatch {
                index: 1,
                root: (5, 7),
                segment: (5, 8),
            }
        );

        let absent = WriteBatch::new().delete(key);
        assert_eq!(commit(&store, absent), CommitOutcome::Committed);
        let before = snapshot(&store);
        assert_eq!(
            chunk_map_error(repoint(&store, &root, 8, &c(), vec![99])),
            ChunkMapError::SegmentAbsent {
                nonce: NONCE.into(),
                epoch: 1,
                index: 1,
            }
        );
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn a_zero_length_chunk_on_a_segment_boundary_is_found_by_equality() {
        // seg 0 = [A, Z] over 0..5 — Z is empty and begins at 5, exactly where seg 1's
        // B also begins. seg 1 = [Y, B] over 5..8 — Y is empty and begins at 5 too.
        let z = chunk(8, 0, vec![80]);
        let y = chunk(9, 0, vec![90]);
        let store = store();
        let root = seed_segmented(
            &store,
            &[(0, vec![a(), z.clone()]), (5, vec![y.clone(), b()])],
        );

        // Z lives in seg 0 even though seg 1 is the one covering byte 5.
        let batch = prepared(repoint(&store, &root, 5, &z, vec![81]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        assert_eq!(
            segment(&store, 0).chunks(),
            &[a(), chunk(8, 0, vec![81])][..]
        );
        assert_eq!(segment(&store, 1).chunks(), &[y.clone(), b()][..]);

        // Y lives in seg 1: seg 0 is a candidate too, but equality rules it out.
        let batch = prepared(repoint(&store, &root, 5, &y, vec![91]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        assert_eq!(
            segment(&store, 1).chunks(),
            &[chunk(9, 0, vec![91]), b()][..]
        );

        // A planned empty chunk no candidate holds: conflict in both, nothing written.
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 5, &z, vec![82]),
            Ok(Repoint::Conflict)
        ));
        assert_eq!(snapshot(&store), before);
    }

    /// `lead` then zero-length fillers, sized so `measure` of the list is exactly `target`.
    /// Built programmatically: tuned by a last filler's placement width and id digits.
    fn padded(
        lead: ChunkRef,
        target: usize,
        measure: impl Fn(&[ChunkRef]) -> usize,
    ) -> Vec<ChunkRef> {
        let filler = |id: ChunkId, placement: Vec<DServerId>| chunk(id, 0, placement);
        let base = measure(&[lead.clone(), filler(0, vec![])]);
        let per = measure(&[lead.clone(), filler(0, vec![0]), filler(0, vec![])]) - base;
        let mut chunks = vec![lead];
        chunks.extend((0..(target - base - 200) / per).map(|_| filler(0, vec![0])));
        chunks.push(filler(0, vec![]));
        // `p` zeros of placement add `2p - 1` bytes; an id of `d` digits adds `d - 1`.
        let gap = target - measure(&chunks);
        let p = if gap <= 19 { 0 } else { (gap - 17) / 2 };
        let digits = gap - if p == 0 { 0 } else { 2 * p - 1 };
        *chunks.last_mut().unwrap() = filler(ChunkId::pow(10, digits as u32), vec![0; p]);
        assert_eq!(measure(&chunks), target);
        chunks
    }

    /// Moving the lead from `[0]` to `[u64::MAX]` grows its record by 19 bytes.
    const GROWTH: usize = 19;

    #[test]
    fn flat_arm_refuses_a_record_the_move_would_push_past_the_value_ceiling() {
        let lead = chunk(1, 5, vec![0]);
        let root_of = |chunks: &[ChunkRef]| flat_root(chunks.to_vec(), 3);
        let chunks = padded(lead.clone(), MAX_VALUE_BYTES - GROWTH + 1, |c| {
            encode(&root_of(c)).len()
        });
        let store = store();
        let root = root_of(&chunks);
        put(&store, inode_key(INODE), encode(&root));
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 0, &lead, vec![u64::MAX]),
            Ok(Repoint::Refused { bytes, ceiling: MAX_VALUE_BYTES }) if bytes == MAX_VALUE_BYTES + 1
        ));
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn segmented_arm_weighs_its_record_against_the_full_value_ceiling() {
        let lead = chunk(1, 5, vec![0]);
        let measure = |c: &[ChunkRef]| encode(&SegmentRecord::new(c.to_vec(), 0).unwrap()).len();

        // One byte under the reach of the ceiling: refused, and the record byte-identical.
        let store = store();
        let over = padded(lead.clone(), MAX_VALUE_BYTES - GROWTH + 1, measure);
        let root = seed_segmented(&store, &[(0, over)]);
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 0, &lead, vec![u64::MAX]),
            Ok(Repoint::Refused { bytes, ceiling: MAX_VALUE_BYTES }) if bytes == MAX_VALUE_BYTES + 1
        ));
        assert_eq!(snapshot(&store), before);

        // Landing exactly on the full ceiling — well past V/2 — is admitted and applies.
        let store = self::store();
        let exact = padded(lead.clone(), MAX_VALUE_BYTES - GROWTH, measure);
        let root = seed_segmented(&store, &[(0, exact)]);
        let batch = prepared(repoint(&store, &root, 0, &lead, vec![u64::MAX]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        let stored = get(&store, &seg_key(&group(), 0).unwrap()).unwrap();
        assert_eq!(stored.len(), MAX_VALUE_BYTES);

        // A stored row exactly at the ceiling is read, not refused: move it again, same width.
        let moved = chunk(1, 5, vec![u64::MAX]);
        let batch = prepared(repoint(&store, &root, 0, &moved, vec![u64::MAX - 1]));
        assert_eq!(commit(&store, batch), CommitOutcome::Committed);
        let stored = get(&store, &seg_key(&group(), 0).unwrap()).unwrap();
        assert_eq!(stored.len(), MAX_VALUE_BYTES);
    }

    #[test]
    fn a_segment_row_over_the_ceiling_is_never_decoded_nor_rewritten() {
        // One byte over V, and the move would SHRINK it under V: were the row decoded, the
        // move would "repair" a record the resolver refuses as corrupt.
        let lead = chunk(1, 5, vec![u64::MAX]);
        let measure = |c: &[ChunkRef]| encode(&SegmentRecord::new(c.to_vec(), 0).unwrap()).len();
        let store = store();
        let root = seed_segmented(
            &store,
            &[(0, padded(lead.clone(), MAX_VALUE_BYTES + 1, measure))],
        );
        let before = snapshot(&store);
        assert_eq!(
            chunk_map_error(repoint(&store, &root, 0, &lead, vec![0])),
            ChunkMapError::SegmentValueOverCeiling {
                index: 0,
                bytes: MAX_VALUE_BYTES + 1,
                ceiling: MAX_VALUE_BYTES,
            }
        );
        assert_eq!(snapshot(&store), before);

        // The same row in a generation the root has left: a retirement, not corruption.
        put(&store, inode_key(INODE), encode(&flat_root(vec![a()], 4)));
        let before = snapshot(&store);
        assert!(matches!(
            repoint(&store, &root, 0, &lead, vec![0]),
            Ok(Repoint::Conflict)
        ));
        assert_eq!(snapshot(&store), before);
    }

    #[test]
    fn a_replacement_placement_must_name_every_fragment() {
        // RS(2,1): three fragments, so exactly three servers — empty is refused too, since a
        // read would identity-fill it to servers 0, 1 and 2.
        let rs = ChunkRef {
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            ..chunk(4, 6, vec![10, 11, 12])
        };
        let flat = self::store();
        let flat_gen = flat_root(vec![a(), rs.clone()], 3);
        put(&flat, inode_key(INODE), encode(&flat_gen));
        let segmented = self::store();
        let seg_gen = seed_segmented(&segmented, &[(0, vec![a()]), (5, vec![rs.clone()])]);
        for (store, generation) in [(&flat, &flat_gen), (&segmented, &seg_gen)] {
            let before = snapshot(store);
            for wrong in [vec![], vec![20], vec![20, 21], vec![20, 21, 22, 23]] {
                let actual = wrong.len();
                let err = repoint(store, generation, 5, &rs, wrong).unwrap_err();
                // The call's fault, typed apart from the object's `ChunkMapError`.
                let malformed = MalformedReplacement {
                    expected: 3,
                    actual,
                };
                assert_eq!(err.downcast_ref(), Some(&malformed));
                let shown = format!("replacement needs 3 D servers, got {actual}");
                assert_eq!(err.to_string(), shown);
                assert_eq!(snapshot(store), before);
            }
            let batch = prepared(repoint(store, generation, 5, &rs, vec![20, 21, 22]));
            assert_eq!(commit(store, batch), CommitOutcome::Committed);
        }
    }

    #[test]
    fn segment_may_hold_is_the_half_open_span_plus_an_empty_chunks_end() {
        let segment = SegmentRef {
            index: 1,
            byte_offset: 5,
            byte_len: 7,
        };
        for (offset, len, holds) in [
            (4, 1, false),
            (4, 0, false),
            (5, 1, true),
            (5, 0, true),
            (11, 1, true),
            (12, 1, false),
            (12, 0, true),
            (13, 0, false),
        ] {
            assert_eq!(
                segment_may_hold(&segment, offset, len),
                holds,
                "offset {offset}, len {len}"
            );
        }
    }

    #[test]
    fn chunk_at_needs_both_the_offset_and_the_reference() {
        let z = chunk(8, 0, vec![80]);
        let chunks = [a(), z.clone(), b(), c()];
        assert_eq!(chunk_at(&chunks, 0, &a()), Some(0));
        assert_eq!(chunk_at(&chunks, 5, &z), Some(1));
        assert_eq!(chunk_at(&chunks, 5, &b()), Some(2));
        assert_eq!(chunk_at(&chunks, 8, &c()), Some(3));
        // Right reference, wrong offset — before, inside, and past it.
        assert_eq!(chunk_at(&chunks, 0, &c()), None);
        assert_eq!(chunk_at(&chunks, 9, &c()), None);
        assert_eq!(chunk_at(&chunks, 12, &c()), None);
        // Right offset, different reference (same id, moved placement).
        assert_eq!(chunk_at(&chunks, 8, &chunk(3, 4, vec![31])), None);
        // Lengths that cannot be summed are no address at all: chunk 2 begins at `u64::MAX`,
        // and `c()` would begin one byte past it — a sum that overflows, not a match.
        let huge = [chunk(1, u64::MAX, vec![]), chunk(2, 1, vec![]), c()];
        assert_eq!(chunk_at(&huge, u64::MAX, &chunk(2, 1, vec![])), Some(1));
        assert_eq!(chunk_at(&huge, u64::MAX, &c()), None);
    }

    /// The object as a campaign believes it is: blind to a batch's pins, it knows only
    /// which records were written since a mover last looked.
    struct World {
        segmented: bool,
        /// Each record's chunks: the flat map's one list, or one list per segment.
        records: Vec<Vec<ChunkRef>>,
        /// Moves landed per record (a flat map's one record is the root itself).
        writes: Vec<u64>,
        /// The root as last written; `None` once deleted.
        root: Option<InodeRecord>,
        root_writes: u64,
        /// The root still names the drawn generation.
        live: bool,
        reclaimed: bool,
    }

    /// What a mover planned from, and the writes it had seen when it did.
    struct Plan {
        generation: InodeRecord,
        offset: u64,
        prior: ChunkRef,
        record: usize,
        root_seen: u64,
        record_seen: u64,
    }

    enum Mover {
        Unplanned,
        Planned(Plan),
        /// The batch, the placement it lands, and the record's writes at the prepare.
        Prepared(Plan, WriteBatch, Vec<DServerId>, u64),
    }

    /// Every server id a campaign draws is new, so no record's bytes ever recur and no CAS
    /// can pass on an A-B-A.
    fn fresh(server: &mut DServerId, fragments: u16) -> Vec<DServerId> {
        (0..fragments)
            .map(|_| {
                *server += 1;
                *server
            })
            .collect()
    }

    /// The store holds exactly the model: the root as last written and every segment's
    /// chunks — each landed move, and nothing a losing batch carried.
    fn assert_matches(store: &dyn MetadataStore, world: &World, seed: u64) {
        let root = get(store, &inode_key(INODE)).map(|b| decode::<InodeRecord>(&b).unwrap());
        assert_eq!(root, world.root, "seed {seed}: the root");
        if world.segmented {
            for (index, chunks) in world.records.iter().enumerate() {
                let stored = get(store, &seg_key(&group(), index as u32).unwrap());
                let stored = stored.map(|b| decode::<SegmentRecord>(&b).unwrap().into_chunks());
                let expected = (!world.reclaimed).then(|| chunks.clone());
                assert_eq!(stored, expected, "seed {seed}: segment {index}");
            }
        }
    }

    /// One seed: an object — flat or segmented, empty chunks included — two to five movers,
    /// and a root that stays, is overwritten, or is deleted (a segmented one's records then
    /// reclaimed), every actor's steps interleaved by the seed. A move writes nothing until
    /// its batch commits, so a race lands between its steps: plan (the production resolver),
    /// prepare ([`repoint_chunk`]), commit. `tally` counts the races that ran: merged,
    /// stale, lost to a move, lost to the retirement.
    fn campaign(seed: u64, tally: &mut [usize; 4]) {
        let mut sim = Sim::new(seed);
        let store = store();
        let root_key = inode_key(INODE);
        let mut server: DServerId = 100;
        let segmented = sim.gen::<bool>();
        let (mut records, mut id) = (Vec::new(), 0);
        for _ in 0..1 + sim.gen::<u32>() % 3 {
            let mut chunks = Vec::new();
            for _ in 0..1 + sim.gen::<u32>() % 3 {
                id += 1;
                let mut made = chunk(id, sim.gen::<u64>() % 4, Vec::new());
                if sim.gen::<bool>() {
                    made.scheme = EcScheme::ReedSolomon { k: 2, m: 1 };
                }
                made.placement = fresh(&mut server, made.fragment_count());
                chunks.push(made);
            }
            if chunks.iter().all(|chunk| chunk.len == 0) {
                chunks[0].len = 1;
            }
            records.push(chunks);
        }
        let root = if segmented {
            let mut offset = 0;
            let mut segments = Vec::new();
            for chunks in &records {
                segments.push((offset, chunks.clone()));
                offset += chunks.iter().map(|chunk| chunk.len).sum::<u64>();
            }
            seed_segmented(&store, &segments)
        } else {
            records = vec![records.concat()];
            let root = flat_root(records[0].clone(), 3);
            put(&store, root_key.clone(), encode(&root));
            root
        };
        let mut world = World {
            segmented,
            writes: vec![0; records.len()],
            records,
            root: Some(root),
            root_writes: 0,
            live: true,
            reclaimed: false,
        };
        let mut movers: Vec<Option<Mover>> = (0..2 + sim.gen::<u32>() % 4)
            .map(|_| Some(Mover::Unplanned))
            .collect();
        // 0: the root stays; 1: it is overwritten; 2: it is deleted.
        let retire = sim.gen::<u32>() % 3;
        let mut retirement = u32::from(retire > 0) * (1 + u32::from(segmented));
        loop {
            let mut ready: Vec<usize> =
                (0..movers.len()).filter(|&m| movers[m].is_some()).collect();
            if retirement > 0 {
                ready.push(movers.len());
            }
            if ready.is_empty() {
                break;
            }
            let actor = ready[sim.gen::<u32>() as usize % ready.len()];
            if actor == movers.len() {
                retirement -= 1;
                let batch = if world.live {
                    world.live = false;
                    world.root_writes += 1;
                    world.root = (retire == 1).then(|| flat_root(vec![chunk(999, 1, vec![1])], 9));
                    match &world.root {
                        Some(next) => WriteBatch::new().put(root_key.clone(), encode(next)),
                        None => WriteBatch::new().delete(root_key.clone()),
                    }
                } else {
                    world.reclaimed = true;
                    (0..world.records.len() as u32).fold(WriteBatch::new(), |batch, index| {
                        batch.delete(seg_key(&group(), index).unwrap())
                    })
                };
                assert_eq!(commit(&store, batch), CommitOutcome::Committed);
                assert_matches(&store, &world, seed);
                continue;
            }
            movers[actor] = match movers[actor].take().unwrap() {
                // Planning after the retirement would plan the successor: another campaign.
                Mover::Unplanned if !world.live => None,
                Mover::Unplanned => {
                    let resolved = resolve_current_chunk_map(&store, &root_key);
                    let resolved = pollster::block_on(resolved).unwrap().unwrap();
                    let flat = world.records.concat();
                    assert_eq!(*resolved.chunks, flat[..], "seed {seed}: every landed move");
                    let pick = sim.gen::<u32>() as usize % flat.len();
                    let prior = flat[pick].clone();
                    let record = world.records.iter().position(|r| r.contains(&prior));
                    let record = record.unwrap();
                    Some(Mover::Planned(Plan {
                        generation: resolved.record.into_owned(),
                        offset: flat[..pick].iter().map(|chunk| chunk.len).sum(),
                        prior,
                        record,
                        root_seen: world.root_writes,
                        record_seen: world.writes[record],
                    }))
                }
                Mover::Planned(plan) => {
                    let placement = fresh(&mut server, plan.prior.fragment_count());
                    let before = snapshot(&store);
                    let (generation, prior) = (&plan.generation, &plan.prior);
                    let prepared =
                        repoint(&store, generation, plan.offset, prior, placement.clone());
                    assert_eq!(snapshot(&store), before, "seed {seed}: a prepare wrote");
                    // The flat arm reads nothing past the plan; the segmented arm reads its
                    // record fresh, so it sees an edit to the planned chunk, or a reclaim.
                    let current = world.records[plan.record].contains(&plan.prior);
                    let stale = segmented && (!current || world.reclaimed);
                    match prepared {
                        Ok(Repoint::Prepared(batch)) if !stale => {
                            let seen = world.writes[plan.record];
                            Some(Mover::Prepared(plan, batch, placement, seen))
                        }
                        Ok(Repoint::Conflict) if stale => {
                            tally[1] += 1;
                            None
                        }
                        other => panic!("seed {seed}: the prepare answered {other:?}"),
                    }
                }
                Mover::Prepared(plan, batch, placement, seen) => {
                    let before = snapshot(&store);
                    let outcome = commit(&store, batch);
                    let at = plan.record;
                    if world.root_writes == plan.root_seen && world.writes[at] == seen {
                        assert_eq!(outcome, CommitOutcome::Committed, "seed {seed}");
                        tally[0] += usize::from(seen != plan.record_seen);
                        let moved = world.records[at].iter_mut().find(|c| **c == plan.prior);
                        moved.unwrap().placement = placement;
                        world.writes[at] += 1;
                        if !segmented {
                            world.root_writes += 1;
                            world.root = Some(InodeRecord {
                                chunk_map: ChunkMap::Flat(world.records[0].clone()),
                                version: plan.generation.version + 1,
                                ..plan.generation
                            });
                        }
                    } else {
                        assert_eq!(outcome, CommitOutcome::Conflict, "seed {seed}");
                        assert_eq!(snapshot(&store), before, "seed {seed}: a lost batch wrote");
                        tally[2 + usize::from(!world.live)] += 1;
                    }
                    None
                }
            };
            assert_matches(&store, &world, seed);
        }
    }

    /// Seeded Tier 0 (ADR-0009), `wyrd_testkit::Sim` drawing every campaign, with the seed
    /// in each failure. Each step is judged against [`World`], a model blind to the pins: a
    /// prepare writes nothing and conflicts exactly when its planned chunk has moved or been
    /// reclaimed; a batch commits exactly when its root is untouched since the plan and its
    /// record since the prepare — so a sibling's move before the read is merged — and a
    /// losing batch leaves the store byte-identical; the store always holds the model.
    #[test]
    fn seeded_moves_race_each_other_and_the_roots_retirement() {
        let mut tally = [0; 4];
        for seed in 0..256 {
            campaign(seed, &mut tally);
        }
        // Every race ran, so no seed range can pass vacuously.
        assert!(tally.iter().all(|&count| count > 0), "{tally:?}");
    }
}
