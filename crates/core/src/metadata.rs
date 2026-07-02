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

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, MetadataStore, Result, WriteBatch};

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

/// An inode: attributes, the ordered chunk map, state, and version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    /// Logical content length in bytes.
    pub size: u64,
    /// The ordered chunks making up the content.
    pub chunk_map: Vec<ChunkRef>,
    /// Commit state.
    pub state: InodeState,
    /// Monotonic per-inode version; the commit point bumps it under CAS.
    pub version: u64,
}

impl InodeRecord {
    /// A freshly-created, empty inode at version 1, awaiting content.
    pub fn new_empty() -> Self {
        Self {
            size: 0,
            chunk_map: Vec::new(),
            state: InodeState::Pending,
            version: 1,
        }
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
        .put(inode_key(id), encode(record))
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
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

/// Commit a chunk map and size onto an inode at the commit point, bumping its
/// version **conditional on the prior record** (full-value compare-and-set). A
/// writer holding a stale `prior` loses with [`CommitOutcome::Conflict`];
/// exactly one concurrent writer wins.
pub async fn commit_chunk_map(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
) -> Result<CommitOutcome> {
    let next = InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version: prior.version + 1,
    };
    let key = inode_key(id);
    let batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
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
