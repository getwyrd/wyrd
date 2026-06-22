//! The shared **reconstruction / repair queue** — the durable seam a corruption
//! finding lands on, whether discovered **proactively by scrub** (the custodian's
//! scrub loop) or **reactively on read** (the client read path's read-time checksum
//! verification). Proposal 0005 §Scrub (`0005:262-267`) and the read-time-failure
//! feed (`0005:174-176`) require **one shared queue** that both producers enqueue
//! onto and the reconstruction loop (slice 6, `0005:531-536`) later drains.
//!
//! The queue lives in `core` rather than `custodian` because the read path is in
//! `core` and `core` must not depend on `custodian` (the dependency rule runs
//! `custodian → core`, ADR-0010 `0005:421-422`). Placing the key + the enqueue +
//! the verify here makes "the same queue" true **by construction**: scrub
//! (`custodian`, which depends on `core`) and the read path call the *same*
//! [`enqueue_repair`] against the *same* [`repair_key`].
//!
//! This slice only **produces** repair obligations; dequeuing and rebuilding is the
//! reconstruction custodian (slice 6, out of scope here). The concrete ledger
//! representation (key/encoding) is ILLUSTRATIVE; the shared-queue feed is BINDING.

use wyrd_traits::{ChunkId, MetadataStore, Result, WriteBatch};

/// Key prefix for the **repair queue** ledger — the chunks a corruption finding has
/// flagged for reconstruction. Mirrors the `pending:` / `orphan:` ledger pattern
/// (architecture §5).
pub const REPAIR_PREFIX: &[u8] = b"repair:";

/// Key for a repair-queue entry: `repair:<chunk_id>`. Keyed by chunk so a repair
/// obligation is a **set** — enqueuing the same chunk twice (scrub and a read both
/// catching it) collapses to one obligation, never a duplicate rebuild.
pub fn repair_key(chunk: ChunkId) -> Vec<u8> {
    format!("repair:{chunk}").into_bytes()
}

/// Parse a repair-queue key back to the chunk id it enqueues.
pub fn parse_repair_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("repair:")?
        .parse()
        .ok()
}

/// Verify a stored fragment's **self-describing checksum** and that it belongs to
/// `chunk` (the committed chunk map's id). Returns `true` only when the bytes decode
/// cleanly (header + payload crc32c verified by [`wyrd_chunk_format::decode`]) **and**
/// the decoded header names `chunk`. A `false` is bit rot / a misplaced fragment: the
/// fragment must be **excluded** from the decoder and its `chunk` enqueued for
/// reconstruction ([`enqueue_repair`]) — the load-bearing invariant
/// (`0005:262-267`, `0005:174-176`).
///
/// This is the one verify both producers share: the read path decodes for the same
/// effect inline (`crates/core/src/read.rs`), and scrub calls this against each
/// referenced fragment it walks.
pub fn fragment_intact(bytes: &[u8], chunk: ChunkId) -> bool {
    matches!(wyrd_chunk_format::decode(bytes), Ok(decoded) if decoded.header.chunk_id == chunk)
}

/// Enqueue `chunk` for reconstruction onto the shared, durable repair queue.
/// **Idempotent** — a chunk already queued stays a single obligation (the key
/// dedups). `detected_by` records the producer (`"scrub"` | `"read"`) for the
/// durability-plane audit trail (`0005:336-340`); the reconstruction loop reads only
/// the key set.
pub async fn enqueue_repair(
    meta: &dyn MetadataStore,
    chunk: ChunkId,
    detected_by: &str,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(repair_key(chunk), detected_by.as_bytes().to_vec()))
        .await?;
    Ok(())
}

/// Every chunk currently enqueued on the repair queue. The reconstruction loop's
/// future entry point; here it is the in-process read-back that proves both
/// producers feed the **same** queue.
pub async fn queued_repairs(meta: &dyn MetadataStore) -> Result<Vec<ChunkId>> {
    let mut chunks = Vec::new();
    for (key, _value) in meta.scan(REPAIR_PREFIX).await? {
        if let Some(chunk) = parse_repair_key(&key) {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}
