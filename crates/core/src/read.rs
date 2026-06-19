//! The client read path: resolve a name, read the inode's chunk map from the
//! [`MetadataStore`], fetch each fragment from the [`ChunkStore`], and return the
//! reassembled bytes (architecture §6.2).
//!
//! Two integrity properties hold by construction:
//! - **Never a hybrid.** A read takes one inode snapshot (a single atomic `get`),
//!   and chunks are immutable (a new version mints new chunk ids), so a
//!   reassembled object is always one whole version — never a mix.
//! - **Never bad data.** The chunk store verifies each fragment's checksum on
//!   read ([`ChunkStore::get_fragment`]); a mismatch propagates as an error
//!   rather than returning corrupt bytes. At M0 (`replication(1)`) there is a
//!   single fragment, so a corrupt or missing one errors; multi-replica re-read
//!   is a later milestone.

use wyrd_chunk_format::decode;
use wyrd_traits::{ChunkId, ChunkStore, MetadataStore, Result};

use crate::metadata::{self, DirentRecord, InodeId, InodeRecord, InodeState};

/// Resolve `name` under `parent` to its inode id, or `None` if the name is
/// unbound.
pub async fn resolve(
    meta: &impl MetadataStore,
    parent: InodeId,
    name: &str,
) -> Result<Option<InodeId>> {
    match meta.get(&metadata::dirent_key(parent, name)).await? {
        Some(bytes) => {
            let dirent: DirentRecord = metadata::decode(&bytes)?;
            Ok(Some(dirent.inode))
        }
        None => Ok(None),
    }
}

/// Read an inode record by id, or `None` if absent.
pub async fn read_inode(
    meta: &impl MetadataStore,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>> {
    match meta.get(&metadata::inode_key(inode_id)).await? {
        Some(bytes) => Ok(Some(metadata::decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Reassemble an object's bytes from a specific inode snapshot. Reading from an
/// explicit snapshot is what makes a read see one whole version. Each fragment's
/// checksum is verified by the chunk store; a mismatch or a missing fragment is
/// an error, never a short or corrupt read.
pub async fn read_object_from(chunks: &impl ChunkStore, inode: &InodeRecord) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(inode.size as usize);
    for &chunk_id in &inode.chunk_map {
        let fragment = chunks
            .get_fragment(chunk_id)
            .await?
            .ok_or(ReadError::MissingFragment { chunk_id })?;
        // The chunk store already verified integrity; decode to take the payload.
        bytes.extend_from_slice(&decode(&fragment)?.payload);
    }
    if bytes.len() as u64 != inode.size {
        return Err(ReadError::SizeMismatch {
            expected: inode.size,
            found: bytes.len() as u64,
        }
        .into());
    }
    Ok(bytes)
}

/// Read a committed object by inode id. `None` if the inode is absent or not yet
/// `COMMITTED`.
pub async fn read_object(
    meta: &impl MetadataStore,
    chunks: &impl ChunkStore,
    inode_id: InodeId,
) -> Result<Option<Vec<u8>>> {
    let Some(inode) = read_inode(meta, inode_id).await? else {
        return Ok(None);
    };
    if inode.state != InodeState::Committed {
        return Ok(None);
    }
    Ok(Some(read_object_from(chunks, &inode).await?))
}

/// Read a committed object by path. `None` if the name is unbound or its inode is
/// not committed.
pub async fn read_path(
    meta: &impl MetadataStore,
    chunks: &impl ChunkStore,
    parent: InodeId,
    name: &str,
) -> Result<Option<Vec<u8>>> {
    match resolve(meta, parent, name).await? {
        Some(inode_id) => read_object(meta, chunks, inode_id).await,
        None => Ok(None),
    }
}

/// Errors specific to the read path; surfaced through the trait's boxed error.
#[derive(Debug)]
pub enum ReadError {
    /// A committed chunk map references a fragment the chunk store does not hold.
    MissingFragment {
        /// The referenced chunk id.
        chunk_id: ChunkId,
    },
    /// The reassembled bytes do not match the inode's recorded size.
    SizeMismatch {
        /// The size the inode records.
        expected: u64,
        /// The size actually reassembled.
        found: u64,
    },
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::MissingFragment { chunk_id } => {
                write!(
                    f,
                    "committed chunk map references missing fragment {chunk_id:032x}"
                )
            }
            ReadError::SizeMismatch { expected, found } => {
                write!(
                    f,
                    "reassembled {found} bytes but the inode records {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ReadError {}
