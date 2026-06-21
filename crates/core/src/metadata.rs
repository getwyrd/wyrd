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
