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

use bytes::Bytes;
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
/// How a fragment fetch failed on the read path.
///
/// The read path already *distinguishes* these four — it reads around all of them but records
/// a repair obligation only for the two corruption classes. What it never did was **say**
/// which one happened, on which fragment, on which D-server, though it held all three in hand
/// (#530). The classes are exactly the trichotomy the trait seam defines (`IntegrityFault` /
/// transient / absent), so the log agrees with the type system.
#[derive(Debug, Clone, Copy)]
enum FaultClass {
    /// A present fragment whose checksum does not verify — bit rot.
    Corrupt,
    /// A present, intact fragment whose header names a *different* chunk — a misrouted write.
    Misplaced,
    /// The D-server answered, and holds no such fragment.
    Absent,
    /// A verifying store surfaced corruption as a typed [`wyrd_traits::IntegrityFault`]
    /// instead of handing back bad bytes.
    IntegrityFault,
    /// Unreachable, timed out, or an RPC error — the D-server may be perfectly healthy a
    /// second later. **Not** a corruption finding.
    Transient,
}

impl FaultClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Corrupt => "corrupt",
            Self::Misplaced => "misplaced",
            Self::Absent => "absent",
            Self::IntegrityFault => "integrity-fault",
            Self::Transient => "transient",
        }
    }
}

/// Record a fragment fault with the identity the read path has always had and never reported:
/// **which D-server, which fragment index, which chunk, and what went wrong**.
///
/// Shaped to match `custodian::scrub::emit_corruption` (`crates/custodian/src/scrub.rs`) —
/// same `dserver` / `chunk` / `index` fields, same split between the metric event and the
/// audit event — so the read and scrub audit planes read alike and one collector query spans
/// both. A per-class counter (rather than one counter for all faults) is what lets an operator
/// see *transient faults rising on one node* — the single blindest spot in the old code, where
/// a D-server failing every RPC looked exactly like a healthy one.
fn emit_fragment_fault(
    class: FaultClass,
    dserver: DServerId,
    frag: FragmentId,
    detail: impl std::fmt::Display,
) {
    match class {
        FaultClass::Corrupt => tracing::warn!(monotonic_counter.read_fragment_corrupt = 1_u64),
        FaultClass::Misplaced => tracing::warn!(monotonic_counter.read_fragment_misplaced = 1_u64),
        FaultClass::Absent => tracing::warn!(monotonic_counter.read_fragment_absent = 1_u64),
        FaultClass::IntegrityFault => {
            tracing::warn!(monotonic_counter.read_fragment_integrity_fault = 1_u64)
        }
        FaultClass::Transient => tracing::warn!(monotonic_counter.read_fragment_transient = 1_u64),
    }
    tracing::warn!(
        target: "wyrd.read.fragment.audit",
        action = "fragment-fault",
        class = class.as_str(),
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        detail = %detail,
        "read fetched a fragment it could not use: excluded from the decoder, read around",
    );
}

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
                // Admit the fragment only if it decodes cleanly AND its header proves
                // the FULL identity the chunk map expects — chunk id, ec_fragment_index,
                // and the EC tuple — the same gate the shared verify enforces
                // (`repair::header_matches_identity` / `repair::fragment_intact`,
                // `repair.rs`). This is the inline decode that verify mirrors
                // (`0005:262-267`, `0005:174-176`).
                Ok(decoded)
                    if repair::header_matches_identity(&decoded.header, frag_id, chunk.scheme) =>
                {
                    Ok(decoded.payload)
                }
                Ok(_) => {
                    // A misplaced / mis-encoded single fragment: it decodes cleanly but
                    // its header does not prove the requested identity (a DIFFERENT
                    // chunk id, a wrong ec_fragment_index, or an EC tuple disagreeing
                    // with the committed scheme — a misrouted / placement-confused
                    // fragment). Never return its foreign bytes as this chunk; record
                    // the chunk as a durable repair obligation and surface a
                    // missing-fragment error — this chunk has no usable fragment here,
                    // exactly as scrub/reconstruction exclude it.
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
            // Validate the stored scheme itself before it drives any fan-out or
            // shard indexing. A committed inode's chunk scheme is untrusted
            // metadata (corruption/tampering, or a future decode of a record this
            // build predates) — the CLI already rejects `rs(0,m)` at parse time
            // (`crates/server/src/cli.rs:110`), but nothing re-checked it here, so
            // `k == 0` reached `erasure::reconstruct` and panicked indexing an
            // empty shard list (issue #285). Reject any scheme the erasure coder
            // itself would refuse — not just `k == 0` — using the SAME predicate
            // `erasure::reconstruct` applies (`erasure::supported`), so a stored
            // `rs(k, 0)` (no recovery shards were ever a legal *encode* target for
            // that scheme) is rejected before it can drive read fan-out, even
            // though a full `k`-of-`k` `available` set would otherwise never reach
            // the coder at all and could silently return bytes for a scheme no
            // commit could ever have produced. Reject here, before firing a single
            // fragment fetch.
            if !erasure::supported(k, m) {
                return Err(ReadError::InvalidEcScheme {
                    chunk_id: chunk.id,
                    k: k as u8,
                    m: m as u8,
                }
                .into());
            }
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
                    // `dserver` rides out with the result. It was computed here and dropped on
                    // the floor: every failure arm below recorded only `chunk.id`, so an
                    // operator could learn that *something* was rotting and never *which disk*
                    // (#530). It costs nothing to carry.
                    async move { (index, dserver, chunks.get_fragment_at(dserver, id).await) }
                })
                .collect();

            let mut shards: Vec<(usize, Vec<u8>)> = Vec::with_capacity(k);
            while let Some((index, dserver, fetched)) = inflight.next().await {
                let frag = FragmentId {
                    chunk: chunk.id,
                    index,
                };
                match fetched {
                    Ok(Some(fragment)) => {
                        match decode(&fragment) {
                            // Admit a survivor only if it decodes cleanly AND its header
                            // proves the FULL identity this slot expects — chunk id,
                            // ec_fragment_index, and the EC tuple — the same gate
                            // `repair::intact_shard` applies in reconstruction
                            // (`0005:262-267`). A valid same-chunk shard for the WRONG
                            // index, or one whose EC tuple disagrees with the committed
                            // scheme, is excluded from the decoder — NEVER fed as a shard
                            // at `index`, where it would be wrong reconstruction input.
                            Ok(decoded)
                                if repair::header_matches_identity(
                                    &decoded.header,
                                    frag,
                                    chunk.scheme,
                                ) =>
                            {
                                shards.push((index as usize, decoded.payload));
                                if shards.len() == k {
                                    // `k` verified: drop the outstanding fetches, which
                                    // abandons (cancels) them.
                                    break;
                                }
                            }
                            // A present fragment that fails its checksum (decode `Err`) or
                            // whose header does not prove the requested identity (foreign
                            // `chunk_id`, wrong `ec_fragment_index`, or a disagreeing EC
                            // tuple) is bit rot / a misplaced / mis-encoded fragment:
                            // excluded from the decoder (read around) AND its chunk
                            // recorded as a repair obligation, never silently absorbed
                            // (`0005:174-176`, `0005:262-264`).
                            Ok(decoded) => {
                                emit_fragment_fault(
                                    FaultClass::Misplaced,
                                    dserver,
                                    frag,
                                    format_args!(
                                        "fragment decodes but its header identity \
                                         (chunk {}, index {}) does not match the requested \
                                         chunk {} index {}",
                                        wyrd_traits::chunk_hex(decoded.header.chunk_id),
                                        decoded.header.ec_fragment_index,
                                        wyrd_traits::chunk_hex(chunk.id),
                                        index,
                                    ),
                                );
                                corrupt.push(chunk.id);
                            }
                            Err(e) => {
                                emit_fragment_fault(FaultClass::Corrupt, dserver, frag, e);
                                corrupt.push(chunk.id);
                            }
                        }
                    }
                    // Absent — read around.
                    Ok(None) => emit_fragment_fault(
                        FaultClass::Absent,
                        dserver,
                        frag,
                        format_args!("the D-server holds no such fragment"),
                    ),
                    // A verifying store (FsChunkStore, gRPC DATA_LOSS mapping) surfaced
                    // corruption as a typed fault instead of raw bytes.  Mirror the
                    // corrupt-bytes arm: record as a repair obligation and read around
                    // (never absorbed silently, `0005:174-176`,
                    // `crates/custodian/src/scrub.rs:102`).
                    Err(e) if wyrd_traits::is_integrity_fault(e.as_ref()) => {
                        emit_fragment_fault(FaultClass::IntegrityFault, dserver, frag, &e);
                        corrupt.push(chunk.id);
                    }
                    // A transient / non-integrity error: read around (treat as absent) — do
                    // NOT record as a corruption finding (reclassifying non-integrity errors
                    // is out of scope, and #431 owns the block-fault repair question).
                    //
                    // But SAY SO. This arm was `Err(_) => {}`: a D-server that is up but
                    // failing, timing out, or returning `Rpc(Status::internal(..))` on every
                    // single read produced no counter, no log and no trace, and so was
                    // indistinguishable from a healthy one from every available vantage point.
                    // Reads quietly got slower and leaned on parity, and nobody found out
                    // until the chunk dropped below `k`. `chunkstore-grpc` classifies the
                    // fault (`Unavailable`/`Timeout`/`Rpc`/`Connect`) and that classification
                    // was thrown in the bin by its one consumer. The control flow is
                    // unchanged; only the silence is.
                    Err(e) => emit_fragment_fault(FaultClass::Transient, dserver, frag, &e),
                }
            }
            if shards.len() < k {
                // The "why did this GET die" line. It did not exist: `InsufficientFragments`
                // carries the chunk and exactly how many of how many fragments were readable,
                // and on the streaming path it is raised inside a DETACHED task whose `Err` is
                // sent down a channel and dropped — so the client saw a broken connection and
                // the operator saw nothing at all (#464, confirmed in the field).
                tracing::error!(
                    target: "wyrd.read.audit",
                    action = "insufficient-fragments",
                    chunk = %wyrd_traits::chunk_hex(chunk.id),
                    have = shards.len(),
                    need = k,
                    total = n,
                    "read cannot reconstruct the chunk: fewer than k fragments verified",
                );
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

/// Resolve `name` under `parent` to its **committed** inode snapshot, or `None` if
/// the name is unbound or its inode is not yet `COMMITTED`. The returned record
/// carries only the chunk *map* (ids/scheme/len/placement) — small metadata, not the
/// object bytes — so a caller can then stream the object one chunk at a time via
/// [`read_chunk_verified`] without ever materialising the whole object (the
/// "stream, don't buffer" invariant, issue #364 / `0015:789`).
pub async fn committed_inode(
    meta: &impl MetadataStore,
    parent: InodeId,
    name: &str,
) -> Result<Option<InodeRecord>> {
    let Some(inode_id) = resolve(meta, parent, name).await? else {
        return Ok(None);
    };
    let Some(inode) = read_inode(meta, inode_id).await? else {
        return Ok(None);
    };
    if inode.state != InodeState::Committed {
        return Ok(None);
    }
    Ok(Some(inode))
}

/// Read and verify a **single** chunk of a committed object, enqueuing any chunk
/// whose read had to exclude a checksum-failing fragment onto the shared repair
/// queue (`0005:174-176`) — exactly as the whole-object [`read_object`] does, but for
/// one chunk. This is the per-chunk primitive a streaming GET walks over
/// [`InodeRecord::chunk_map`] with, so only one chunk (never the whole object) is
/// resident at a time (issue #364). Fragment checksums are verified on the way out,
/// so a streamed read still never yields corrupt bytes.
pub async fn read_chunk_verified(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    chunk: &ChunkRef,
) -> Result<Bytes> {
    let mut corrupt = Vec::new();
    let result = read_chunk(chunks, chunk, &mut corrupt).await;
    corrupt.sort_unstable();
    corrupt.dedup();
    for chunk_id in corrupt {
        repair::enqueue_repair(meta, chunk_id, "read").await?;
    }
    Ok(Bytes::from(result?))
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
    /// A committed chunk's stored `EcScheme::ReedSolomon` is itself invalid —
    /// `k == 0`, `m == 0`, or any other `k`/`m` pair the erasure coder does not
    /// support (`erasure::supported`, the same predicate `erasure::reconstruct`
    /// applies) — untrusted metadata (corruption/tampering) rather than anything
    /// the CLI's own `rs(k,m)` parse would have accepted
    /// (`crates/server/src/cli.rs:110`). Rejected before it can drive fragment
    /// fan-out or shard indexing.
    InvalidEcScheme {
        /// The chunk whose stored scheme is invalid.
        chunk_id: ChunkId,
        /// The rejected data-fragment count.
        k: u8,
        /// The parity-fragment count that accompanied it.
        m: u8,
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
            ReadError::InvalidEcScheme { chunk_id, k, m } => {
                write!(
                    f,
                    "chunk {chunk_id:032x}: invalid stored EC scheme rs({k},{m}); \
                     unsupported by the erasure coder"
                )
            }
        }
    }
}

impl std::error::Error for ReadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use wyrd_traits::{ChunkStore, FragmentId, Health};

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

    /// A chunk store holding nothing — every fetch resolves `Ok(None)`. Sufficient
    /// to prove an invalid stored scheme is rejected before any fragment is even
    /// requested (issue #285's read-path boundary), since a real fetch is never
    /// needed to trigger the (pre-fix) panic / silent pass-through.
    struct EmptyChunks;

    #[async_trait]
    impl ChunkStore for EmptyChunks {
        async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
            Ok(())
        }
        async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
            Ok(None)
        }
        async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
            Ok(Vec::new())
        }
        async fn delete_fragment(&self, _id: FragmentId) -> Result<()> {
            Ok(())
        }
        async fn health(&self) -> Result<Health> {
            Ok(Health::Healthy)
        }
    }

    impl PlacementChunkStore for EmptyChunks {}

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

    /// Issue #285: a committed inode's stored `EcScheme::ReedSolomon { k: 0, .. }`
    /// (corrupted/tampered metadata — untrusted input reaching the read path) must
    /// fail as a typed `ReadError`, never panic. Pre-fix this reached
    /// `erasure::reconstruct(0, m, ..)` and panicked indexing an empty shard list
    /// (matches the brief's read-path repro).
    #[test]
    fn read_chunk_rejects_a_stored_k_zero_scheme() {
        let chunk = ChunkRef {
            id: 42,
            scheme: EcScheme::ReedSolomon { k: 0, m: 1 },
            len: 0,
            placement: Vec::new(),
        };
        let mut corrupt = Vec::new();
        let err = pollster::block_on(read_chunk(&EmptyChunks, &chunk, &mut corrupt))
            .expect_err("a k == 0 stored scheme must be a typed error, not a panic");
        let read_err = err
            .downcast_ref::<ReadError>()
            .expect("expected a wyrd_core::read::ReadError");
        assert!(
            matches!(
                read_err,
                ReadError::InvalidEcScheme {
                    chunk_id: 42,
                    k: 0,
                    m: 1
                }
            ),
            "expected InvalidEcScheme {{ chunk_id: 42, k: 0, m: 1 }}, got {read_err:?}"
        );
        assert!(
            corrupt.is_empty(),
            "an invalid stored scheme is a validation rejection, not a corruption finding"
        );
    }

    /// Issue #285 (iteration 2 — carry-forward / codex finding): the read-boundary
    /// guard must reject ALL unsupported stored schemes, not just `k == 0`. A
    /// stored `rs(k, 0)` is exactly as untrusted — `reed_solomon_simd::encode`
    /// itself refuses to produce a `rs(k, 0)` chunk, so no committed chunk could
    /// ever legitimately carry that scheme. Pre- this iteration's fix (the
    /// `k == 0`-only guard), a tampered `rs(k, 0)` inode whose `k` fragments all
    /// happened to be present would fan out, fetch, and return bytes without ever
    /// being rejected — this proves it is now rejected before a single fragment
    /// fetch fires.
    #[test]
    fn read_chunk_rejects_a_stored_m_zero_scheme() {
        let chunk = ChunkRef {
            id: 7,
            scheme: EcScheme::ReedSolomon { k: 3, m: 0 },
            len: 0,
            placement: Vec::new(),
        };
        let mut corrupt = Vec::new();
        let err = pollster::block_on(read_chunk(&EmptyChunks, &chunk, &mut corrupt))
            .expect_err("an m == 0 stored scheme must be a typed error, not silently accepted");
        let read_err = err
            .downcast_ref::<ReadError>()
            .expect("expected a wyrd_core::read::ReadError");
        assert!(
            matches!(
                read_err,
                ReadError::InvalidEcScheme {
                    chunk_id: 7,
                    k: 3,
                    m: 0
                }
            ),
            "expected InvalidEcScheme {{ chunk_id: 7, k: 3, m: 0 }}, got {read_err:?}"
        );
        assert!(
            corrupt.is_empty(),
            "an invalid stored scheme is a validation rejection, not a corruption finding"
        );
    }
}
