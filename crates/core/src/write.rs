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
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::metadata::{self, InodeId, InodeRecord, InodeState, PendingEntry};

/// One planned chunk: its minted id and the encoded v1 fragment bytes.
#[derive(Debug, Clone)]
pub struct PlannedChunk {
    /// The chunk id minted for this chunk.
    pub id: ChunkId,
    /// The encoded fragment (header + payload + checksum) to store.
    pub fragment: Bytes,
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
    /// The chunk ids in object order — the inode's chunk map.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.id).collect()
    }
}

/// Chunk `data` into `chunk_size`-byte pieces, mint a chunk id per piece via
/// `next_id`, and encode each as a v1 fragment. Pure and deterministic given
/// `next_id`. An empty object yields an empty plan.
pub fn plan_write(
    data: &[u8],
    chunk_size: usize,
    mut next_id: impl FnMut() -> ChunkId,
) -> WritePlan {
    let chunk_size = chunk_size.max(1);
    let chunks = data
        .chunks(chunk_size)
        .map(|piece| {
            let id = next_id();
            let fragment = Bytes::from(encode(
                &FragmentHeader::new_v1(id, piece.len() as u64),
                piece,
            ));
            PlannedChunk { id, fragment }
        })
        .collect();
    WritePlan {
        chunks,
        size: data.len() as u64,
    }
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
/// checksums. Failures here are harmless leased garbage.
pub async fn write_fragments(chunks: &impl ChunkStore, plan: &WritePlan) -> Result<()> {
    for chunk in &plan.chunks {
        // replication(1)/none: one fragment per chunk at index 0. Erasure coding
        // will vary the index across the chunk's n fragments here.
        let id = FragmentId {
            chunk: chunk.id,
            index: 0,
        };
        chunks.put_fragment(id, chunk.fragment.clone()).await?;
    }
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
        chunk_map: plan.chunk_ids(),
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
    metadata::commit_chunk_map(meta, inode_id, prior, plan.chunk_ids(), plan.size).await
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
    now_millis: u64,
    lease_ttl_millis: u64,
    next_id: impl FnMut() -> ChunkId,
) -> Result<CommitOutcome> {
    let plan = plan_write(data, chunk_size, next_id);
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
