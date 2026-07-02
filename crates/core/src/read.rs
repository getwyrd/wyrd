//! The client read path: resolve a name, read the inode's chunk map from the
//! [`MetadataStore`], fetch the chunk's fragments from the [`ChunkStore`], and
//! return the reassembled bytes (architecture §6.2). An erasure-coded chunk
//! fetches all `n` fragments **in parallel** and reconstructs from whichever `k`
//! verify their checksums first — it never waits on the slow `m` (§6.2, §6.6).
//!
//! Two integrity properties hold by construction:
//! - **Never a hybrid.** A read takes one inode snapshot (a single atomic `get`),
//!   and chunks are immutable (a new version mints new chunk ids), so a
//!   reassembled object is always one whole version — never a mix.
//! - **Never bad data.** The chunk store verifies each fragment's checksum on
//!   read ([`ChunkStore::get_fragment`]); a mismatch never returns corrupt bytes.
//!   For an erasure-coded chunk a missing or checksum-failing fragment is
//!   excluded and reconstructed around (up to `m` of them); below `k` survivors
//!   the read fails with a typed error. A `replication(1)`/`none` chunk has a
//!   single fragment, so a corrupt or missing one simply errors.

use futures_util::stream::{FuturesUnordered, StreamExt};
use wyrd_chunk_format::decode;
use wyrd_traits::{ChunkId, DServerId, FragmentId, MetadataStore, PlacementChunkStore, Result};

use crate::erasure;
use crate::metadata::{self, ChunkRef, DirentRecord, EcScheme, InodeId, InodeRecord, InodeState};
use crate::repair;

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
pub async fn read_object_from(
    chunks: &impl PlacementChunkStore,
    inode: &InodeRecord,
) -> Result<Vec<u8>> {
    // No metadata store at this entry, so a corruption finding cannot be recorded
    // on the repair queue; the placement-aware entries ([`read_object`] /
    // [`read_path`]) thread the store and feed the queue. Findings are still
    // computed (and dropped) here so this path's behaviour is otherwise unchanged.
    let mut corrupt = Vec::new();
    read_object_collecting(chunks, inode, &mut corrupt).await
}

/// Reassemble an object's bytes, **collecting** the ids of chunks whose read had to
/// exclude a checksum-failing fragment, so the caller can enqueue them for repair on
/// the shared queue (`0005:174-176`). `corrupt` is appended to as the read proceeds —
/// it carries the findings even when the read ultimately fails (a chunk below `k`
/// survivors is still a durable repair obligation).
async fn read_object_collecting(
    chunks: &impl PlacementChunkStore,
    inode: &InodeRecord,
    corrupt: &mut Vec<ChunkId>,
) -> Result<Vec<u8>> {
    // `inode.size` is untrusted metadata (arbitrary `u64` from stored JSON, ADR-0002)
    // and must not size an allocation before that many bytes are actually backed by
    // the chunk map. Grow the buffer only from bytes a chunk read has *already*
    // produced and checksum-verified (`read_chunk`) — never from the recorded size
    // up front. A `size: u64::MAX` inode with an empty/short chunk map then falls
    // through to the mismatch check below instead of attempting a
    // size-proportional (or overflowing) allocation.
    let mut bytes = Vec::new();
    for chunk in &inode.chunk_map {
        bytes.extend_from_slice(&read_chunk(chunks, chunk, corrupt).await?);
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

/// The D server holding fragment `index` of `chunk`, per the committed placement
/// record (proposal 0005, M3.1). The read resolves each fragment **from the record**
/// — retiring M2's stateless `index % n` — so a fragment a custodian has *moved* is
/// still found. A pre-M3 record carries no placement (or a short one); the fragment
/// then resolves to its own index, which the single-authority store routes exactly as
/// M2 did, so mixed-era data reads through the same path.
///
/// Delegates to [`ChunkRef::placed_dserver`] — the single authoritative
/// placement-resolution definition shared by the read path, GC, scrub, and
/// reconstruction, so placement semantics cannot drift across callers.
fn fragment_dserver(chunk: &ChunkRef, index: u16) -> DServerId {
    chunk.placed_dserver(index)
}

/// Read and decode one chunk's bytes, dispatching on its durability scheme. A
/// per-chunk scheme is what lets one read path serve mixed-era data (ADR-0008).
///
/// For an erasure-coded chunk the read is resilient *and* parallel (§6.2, §6.6):
/// all `n = k + m` fragments are fetched at once and the chunk is reconstructed
/// from whichever `k` verify their checksums **first**, so a missing, corrupt,
/// slow, or unreachable fragment is read *around* — the read waits only on the
/// `k` fastest valid fragments, never on the slowest `m`. Below `k` valid
/// fragments it returns a clean typed error rather than a short or corrupt read.
///
/// Each fragment is fetched from the D server the **placement record** names
/// ([`fragment_dserver`]), not from `index % n` — the location authority is the
/// committed chunk map, not the fan-out.
async fn read_chunk(
    chunks: &impl PlacementChunkStore,
    chunk: &ChunkRef,
    corrupt: &mut Vec<ChunkId>,
) -> Result<Vec<u8>> {
    match chunk.scheme {
        EcScheme::None => {
            // A single fragment at index 0; there is nothing to reconstruct around.
            let frag_id = FragmentId {
                chunk: chunk.id,
                index: 0,
            };
            let fetch = chunks
                .get_fragment_at(fragment_dserver(chunk, 0), frag_id)
                .await;
            let fragment = match fetch {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Err(ReadError::MissingFragment { chunk_id: chunk.id }.into()),
                // A verifying store (FsChunkStore, gRPC DATA_LOSS mapping) surfaced
                // corruption as a typed fault instead of returning raw corrupt bytes.
                // Mirror the raw-bytes corrupt arm: record as a durable repair
                // obligation before surfacing the error (`0005:174-176`).
                Err(e) if wyrd_traits::is_integrity_fault(e.as_ref()) => {
                    corrupt.push(chunk.id);
                    return Err(e);
                }
                // A transient / non-integrity error: propagate it so the caller's
                // retry policy decides — do NOT record as a corruption finding.
                Err(e) => return Err(e),
            };
            match decode(&fragment) {
                // Admit the fragment only if it decodes cleanly AND its header names
                // the chunk being read — the same gate the shared verify enforces
                // (`repair::fragment_intact`, `repair.rs`). This is the inline decode
                // that verify is documented to mirror (`0005:262-267`, `0005:174-176`).
                Ok(decoded) if decoded.header.chunk_id == chunk.id => Ok(decoded.payload),
                Ok(_) => {
                    // A misplaced-but-intact single fragment: it decodes cleanly but
                    // its header names a DIFFERENT chunk (a misrouted / placement-
                    // confused fragment). Never return its foreign bytes as this
                    // chunk; record the chunk as a durable repair obligation and
                    // surface a missing-fragment error — this chunk has no usable
                    // fragment here, exactly as scrub/reconstruction exclude it.
                    corrupt.push(chunk.id);
                    Err(ReadError::MissingFragment { chunk_id: chunk.id }.into())
                }
                Err(e) => {
                    // A present-but-corrupt single fragment: never return its bytes,
                    // and record the chunk as a durable repair obligation before
                    // surfacing the error (there is nothing to reconstruct around).
                    corrupt.push(chunk.id);
                    Err(e.into())
                }
            }
        }
        EcScheme::ReedSolomon { k, m } => {
            let (k, m) = (k as usize, m as usize);
            let n = (k + m) as u16;
            // Any-`k`-arrive-first (§6.2): fire `get_fragment_at` at all `n` indices
            // at once — each resolved to its placed D server — and reconstruct from
            // the first `k` that verify their checksums. A fragment that is missing
            // (`Ok(None)`), fails its checksum or cannot be decoded (`Err`), or is
            // slow/unreachable (its future has simply not resolved) is treated as
            // **absent** and read around — a corrupt shard is never handed to the
            // decoder, and the read never blocks on the slow `m`. The futures are
            // polled cooperatively on this one task (no spawn), so their completion
            // ordering is seed-driven and the read stays deterministic under
            // simulation (ADR-0009).
            let mut inflight: FuturesUnordered<_> = (0..n)
                .map(|index| {
                    let id = FragmentId {
                        chunk: chunk.id,
                        index,
                    };
                    let dserver = fragment_dserver(chunk, index);
                    async move { (index, chunks.get_fragment_at(dserver, id).await) }
                })
                .collect();

            let mut shards: Vec<(usize, Vec<u8>)> = Vec::with_capacity(k);
            while let Some((index, fetched)) = inflight.next().await {
                match fetched {
                    Ok(Some(fragment)) => {
                        match decode(&fragment) {
                            // Admit a survivor only if it decodes cleanly AND its header
                            // names this chunk — the same gate `repair::intact_shard`
                            // applies in reconstruction (`0005:262-267`). A misplaced-but-
                            // intact fragment (valid checksum, foreign `chunk_id`) is
                            // excluded from the decoder, never fed as a shard at `index`.
                            Ok(decoded) if decoded.header.chunk_id == chunk.id => {
                                shards.push((index as usize, decoded.payload));
                                if shards.len() == k {
                                    // `k` verified: drop the outstanding fetches, which
                                    // abandons (cancels) them.
                                    break;
                                }
                            }
                            // A present fragment that fails its checksum (decode `Err`) or
                            // names a different chunk (misplaced) is bit rot / a misrouted
                            // fragment: excluded from the decoder (read around) AND its
                            // chunk recorded as a repair obligation, never silently
                            // absorbed (`0005:174-176`, `0005:262-264`).
                            _ => {
                                corrupt.push(chunk.id);
                            }
                        }
                    }
                    // Absent — read around.
                    Ok(None) => {}
                    // A verifying store (FsChunkStore, gRPC DATA_LOSS mapping) surfaced
                    // corruption as a typed fault instead of raw bytes.  Mirror the
                    // corrupt-bytes arm: record as a repair obligation and read around
                    // (never absorbed silently, `0005:174-176`,
                    // `crates/custodian/src/scrub.rs:102`).
                    Err(e) if wyrd_traits::is_integrity_fault(e.as_ref()) => {
                        corrupt.push(chunk.id);
                    }
                    // A transient / non-integrity error: silently drop (treat as absent,
                    // existing behaviour) — do NOT record as a corruption finding
                    // (reclassifying non-integrity errors is out of scope).
                    Err(_) => {}
                }
            }
            if shards.len() < k {
                return Err(ReadError::InsufficientFragments {
                    chunk_id: chunk.id,
                    have: shards.len(),
                    need: k,
                }
                .into());
            }
            Ok(erasure::reconstruct(k, m, chunk.len as usize, &shards)?)
        }
    }
}

/// Read a committed object by inode id. `None` if the inode is absent or not yet
/// `COMMITTED`.
pub async fn read_object(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    inode_id: InodeId,
) -> Result<Option<Vec<u8>>> {
    let Some(inode) = read_inode(meta, inode_id).await? else {
        return Ok(None);
    };
    if inode.state != InodeState::Committed {
        return Ok(None);
    }
    // Read the object, collecting any chunk whose read excluded a checksum-failing
    // fragment, then enqueue each onto the SAME repair queue scrub feeds
    // (`0005:174-176`) — whether or not the read itself recovered. The enqueue runs
    // before the read result is surfaced, so a read that fails below `k` survivors
    // still leaves a durable repair obligation behind.
    let mut corrupt = Vec::new();
    let result = read_object_collecting(chunks, &inode, &mut corrupt).await;
    corrupt.sort_unstable();
    corrupt.dedup();
    for chunk in corrupt {
        repair::enqueue_repair(meta, chunk, "read").await?;
    }
    Ok(Some(result?))
}

/// Read a committed object by path. `None` if the name is unbound or its inode is
/// not committed.
pub async fn read_path(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
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
    /// Fewer than `k` fragments of an erasure-coded chunk were readable, so it
    /// cannot be reconstructed (more than `m` were missing or corrupt).
    InsufficientFragments {
        /// The chunk that could not be reconstructed.
        chunk_id: ChunkId,
        /// How many valid fragments were available.
        have: usize,
        /// How many (`k`) the scheme needs.
        need: usize,
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
            ReadError::InsufficientFragments {
                chunk_id,
                have,
                need,
            } => {
                write!(
                    f,
                    "chunk {chunk_id:032x}: only {have} of {need} fragments readable; \
                     cannot reconstruct"
                )
            }
        }
    }
}

impl std::error::Error for ReadError {}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use wyrd_traits::{ChunkStore, FragmentId, Health};

    use super::*;

    /// A chunk store that must never be called: the regression below feeds a
    /// `chunk_map` too short to ever reach a fragment fetch, so any call in here
    /// would itself indicate the fix regressed.
    struct UnreachableStore;

    #[async_trait]
    impl ChunkStore for UnreachableStore {
        async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
            unreachable!("no write path is exercised by this read-only regression")
        }

        async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
            unreachable!("an empty chunk_map must fail on the size check, never fetch a fragment")
        }

        async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
            unreachable!("not exercised by this regression")
        }

        async fn delete_fragment(&self, _id: FragmentId) -> Result<()> {
            unreachable!("not exercised by this regression")
        }

        async fn health(&self) -> Result<Health> {
            Ok(Health::Healthy)
        }
    }

    impl PlacementChunkStore for UnreachableStore {}

    /// `read.rs:79` (pre-fix) — `Vec::with_capacity(inode.size as usize)` trusted
    /// the inode's recorded `size` (an arbitrary `u64` from stored JSON) as an
    /// allocation hint *before* validating it against the chunk map. A committed
    /// inode with `size: u64::MAX` and an empty chunk map turned an ordinary read
    /// into a capacity-overflow panic (`with_capacity` panics past
    /// `isize::MAX` bytes) instead of the typed `SizeMismatch` the size check at
    /// `read.rs:83` was meant to produce. This must return a clean `Err` and must
    /// not panic — proving the allocation no longer scales with the untrusted
    /// `size` field.
    #[tokio::test]
    async fn oversized_inode_size_with_empty_chunk_map_errors_cleanly_not_panics() {
        let inode = InodeRecord {
            size: u64::MAX,
            chunk_map: Vec::new(),
            state: InodeState::Committed,
            version: 1,
        };

        let result = read_object_from(&UnreachableStore, &inode).await;

        match result {
            Err(err) => {
                // The variant is illustrative; what's binding is a typed error,
                // not a panic and not a size-proportional allocation attempt.
                let msg = err.to_string();
                assert!(
                    msg.contains("18446744073709551615") || msg.contains("0"),
                    "expected a size-mismatch-flavoured error, got: {msg}"
                );
            }
            Ok(bytes) => panic!(
                "an empty chunk_map cannot back a u64::MAX-sized object; got Ok({} bytes)",
                bytes.len()
            ),
        }
    }
}
