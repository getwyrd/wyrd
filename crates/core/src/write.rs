//! The client write path: the four-phase commit protocol (architecture §5/§6.1)
//! over the [`MetadataStore`] and [`ChunkStore`] seams.
//!
//! 1. **Intent** — register the chunk ids in the pending ledger with a lease.
//! 2. **Data** — write each fragment to the chunk store (it verifies checksums).
//! 3. **Commit** — one atomic metadata mutation writes the chunk map, sets state
//!    `COMMITTED`, and bumps the version conditional on the prior. *This is the
//!    atomicity*: the file does not exist until it and fully exists after it;
//!    concurrent writers conflict here and exactly one wins.
//! 4. **Release** — delete the ledger entries.
//!
//! A crash before step 3 leaves leased garbage; a crash between 3 and 4 leaves
//! ledger entries. Both are reclaimed by [`sweep_expired_leases`], the
//! test-invoked stand-in for the custodian GC. The steps are exposed
//! individually so a test can stop after any phase to model a crash.
//!
//! Time enters as `now_millis` and chunk ids via a caller-supplied generator, so
//! this module is backend- and runtime-agnostic and a run is reproducible from a
//! seed (ADR-0009).

use bytes::Bytes;
use wyrd_chunk_format::{encode, EcSchemeType, FragmentHeader};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::erasure;
use crate::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, PendingEntry};

/// One planned chunk: its id, durability scheme, logical length, and the encoded
/// fragments to store — each `(ec_fragment_index, fragment bytes)`.
#[derive(Debug, Clone)]
pub struct PlannedChunk {
    /// The chunk id minted for this chunk (shared by all its fragments).
    pub id: ChunkId,
    /// How the chunk is fragmented.
    pub scheme: EcScheme,
    /// The chunk's logical (pre-coding) length.
    pub len: u64,
    /// The fragments to write, by `ec_fragment_index`.
    pub fragments: Vec<(u16, Bytes)>,
}

/// The pure result of chunking and encoding an object — no store access yet.
#[derive(Debug, Clone, Default)]
pub struct WritePlan {
    /// The chunks in object order.
    pub chunks: Vec<PlannedChunk>,
    /// Total object length in bytes.
    pub size: u64,
}

impl WritePlan {
    /// The chunk ids in object order — the keys of the pending ledger.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.id).collect()
    }

    /// The chunk map for the inode record — id, scheme, logical length, and the
    /// **placement record** per chunk. Placement is the identity vector
    /// (`index` → D-server `index`): the write fan-out routes each fragment by index
    /// (`index % n`), so the committed record mirrors that placement and the read
    /// path resolves each fragment **from the record** instead of recomputing the
    /// route (proposal 0005, M3.1). A custodian that later *moves* a fragment rewrites
    /// this entry, and the read follows the record rather than the stale `index % n`.
    pub fn chunk_refs(&self) -> Vec<ChunkRef> {
        self.chunks
            .iter()
            .map(|c| ChunkRef {
                id: c.id,
                scheme: c.scheme,
                len: c.len,
                placement: (0..c.fragments.len() as u64).collect(),
            })
            .collect()
    }
}

/// Encode one chunk's `piece` into its fragments under `scheme`.
fn encode_chunk(
    scheme: EcScheme,
    id: ChunkId,
    piece: &[u8],
) -> std::result::Result<Vec<(u16, Bytes)>, erasure::ErasureError> {
    match scheme {
        EcScheme::None => {
            let header = FragmentHeader::new_v1(id, piece.len() as u64);
            Ok(vec![(0, Bytes::from(encode(&header, piece)))])
        }
        EcScheme::ReedSolomon { k, m } => {
            let shards = erasure::encode(k as usize, m as usize, piece)?;
            Ok(shards
                .into_iter()
                .enumerate()
                .map(|(i, shard)| {
                    let mut header = FragmentHeader::new_v1(id, shard.len() as u64);
                    header.ec_scheme_type = EcSchemeType::ReedSolomon;
                    header.ec_k = k;
                    header.ec_m = m;
                    header.ec_fragment_index = i as u16;
                    (i as u16, Bytes::from(encode(&header, &shard)))
                })
                .collect())
        }
    }
}

/// Chunk `data` into `chunk_size`-byte pieces, mint a chunk id per piece via
/// `next_id`, and erasure-code each into its fragments under `scheme`. Pure and
/// deterministic given `next_id`. An empty object yields an empty plan.
pub fn plan_write(
    data: &[u8],
    chunk_size: usize,
    scheme: EcScheme,
    mut next_id: impl FnMut() -> ChunkId,
) -> std::result::Result<WritePlan, erasure::ErasureError> {
    let chunk_size = chunk_size.max(1);
    let mut chunks = Vec::new();
    for piece in data.chunks(chunk_size) {
        let id = next_id();
        chunks.push(PlannedChunk {
            id,
            scheme,
            len: piece.len() as u64,
            fragments: encode_chunk(scheme, id, piece)?,
        });
    }
    Ok(WritePlan {
        chunks,
        size: data.len() as u64,
    })
}

/// Phase 1 — Intent: register every chunk id in the pending ledger with a lease
/// expiring at `lease_expiry_millis`.
pub async fn intent(
    meta: &impl MetadataStore,
    plan: &WritePlan,
    lease_expiry_millis: u64,
) -> Result<()> {
    for chunk in &plan.chunks {
        metadata::put_pending(
            meta,
            chunk.id,
            &PendingEntry {
                lease_expiry_millis,
            },
        )
        .await?;
    }
    Ok(())
}

/// Phase 2 — Data: write every fragment to the chunk store, which verifies its
/// checksums.
///
/// The puts are fired as a **concurrent fan-out** and joined on every one: with a
/// fan-out store (M2.4) a chunk's `n` fragments stream to distinct D servers in
/// parallel rather than one after another, so the data phase costs one round trip,
/// not `n`. The join is single-task cooperative concurrency (it polls the futures,
/// it does not spawn), so it stays deterministic under simulation (ADR-0009).
///
/// **Fail-closed:** if any put fails or times out the whole phase returns that
/// error, so the four-phase protocol aborts *before* the commit — there is never a
/// half-committed chunk. The fragments that did land are harmless **leased
/// garbage** the pending-ledger sweep reclaims. Degraded-write tolerance
/// (committing with fewer than `n` and letting custodians backfill) is deferred to
/// M3; M2 commits only after **all `n`** ack.
pub async fn write_fragments(chunks: &impl ChunkStore, plan: &WritePlan) -> Result<()> {
    let puts = plan.chunks.iter().flat_map(|chunk| {
        chunk.fragments.iter().map(move |(index, fragment)| {
            let id = FragmentId {
                chunk: chunk.id,
                index: *index,
            };
            chunks.put_fragment(id, fragment.clone())
        })
    });
    futures_util::future::try_join_all(puts).await?;
    Ok(())
}

/// Phase 3 — Commit (new file): atomically create the inode (state `COMMITTED`,
/// the chunk map, version 1) and its dirent. `Conflict` if the name exists.
pub async fn commit_create(
    meta: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    inode_id: InodeId,
    plan: &WritePlan,
) -> Result<CommitOutcome> {
    let record = InodeRecord {
        size: plan.size,
        chunk_map: plan.chunk_refs(),
        state: InodeState::Committed,
        version: 1,
    };
    metadata::create(meta, parent, name, inode_id, &record).await
}

/// Phase 3 — Commit (overwrite): CAS the inode's chunk map + size onto `prior`,
/// bumping the version. `Conflict` rejects a stale writer; exactly one wins.
pub async fn commit_overwrite(
    meta: &impl MetadataStore,
    inode_id: InodeId,
    prior: &InodeRecord,
    plan: &WritePlan,
) -> Result<CommitOutcome> {
    metadata::commit_chunk_map(meta, inode_id, prior, plan.chunk_refs(), plan.size).await
}

/// Phase 4 — Release: delete the pending-ledger entries for a committed write.
pub async fn release(meta: &impl MetadataStore, plan: &WritePlan) -> Result<()> {
    metadata::sweep_pending(meta, &plan.chunk_ids()).await?;
    Ok(())
}

/// Write a brand-new object end to end (the four phases in order). `now_millis`
/// stamps the lease and `lease_ttl_millis` is its lifetime. The ledger is
/// released only on a winning commit; a losing commit leaves leased garbage for
/// the sweep.
#[allow(clippy::too_many_arguments)]
pub async fn write_new_object(
    meta: &impl MetadataStore,
    chunks: &impl ChunkStore,
    parent: InodeId,
    name: &str,
    inode_id: InodeId,
    data: &[u8],
    chunk_size: usize,
    scheme: EcScheme,
    now_millis: u64,
    lease_ttl_millis: u64,
    next_id: impl FnMut() -> ChunkId,
) -> Result<CommitOutcome> {
    let plan = plan_write(data, chunk_size, scheme, next_id)?;
    intent(meta, &plan, now_millis + lease_ttl_millis).await?;
    write_fragments(chunks, &plan).await?;
    let outcome = commit_create(meta, parent, name, inode_id, &plan).await?;
    if outcome == CommitOutcome::Committed {
        release(meta, &plan).await?;
    }
    Ok(outcome)
}

/// Reclaim expired pending-ledger entries — the custodian sweep stand-in
/// (architecture §6.2). Deletes every `pending:` entry whose lease has expired
/// as of `now_millis` in one atomic commit, and returns the reclaimed chunk ids.
/// Orphaned *fragments* are collectable garbage; reclaiming them needs a
/// chunk-store delete (a later milestone).
pub async fn sweep_expired_leases(
    meta: &impl MetadataStore,
    now_millis: u64,
) -> Result<Vec<ChunkId>> {
    let pending = meta.scan(b"pending:").await?;
    let mut batch = WriteBatch::new();
    let mut reclaimed = Vec::new();
    for (key, value) in pending {
        let entry: PendingEntry = metadata::decode(&value)?;
        if entry.lease_expiry_millis <= now_millis {
            if let Some(id) = parse_pending_key(&key) {
                reclaimed.push(id);
            }
            batch = batch.delete(key);
        }
    }
    if !reclaimed.is_empty() {
        meta.commit(batch).await?;
    }
    Ok(reclaimed)
}

/// Parse the chunk id out of a `pending:<id>` key.
fn parse_pending_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}
