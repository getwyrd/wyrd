//! Issue #431: a foreground RS read that reads AROUND a **permanent block-layer read
//! fault** (`wyrd_traits::BlockReadFault`, `traits/src/lib.rs:164-199` — documented
//! PERMANENT damage; retrying the same fetch cannot help) must still land a repair
//! obligation on the SAME shared repair queue scrub feeds (`0005:174-176`), not just
//! read around it and emit telemetry.
//!
//! BINDING leg: an RS(2,1) read over a store where one fragment's fetch returns
//! `Err(BlockReadFault)` (`wyrd_traits::is_block_read_fault` — the system's single
//! decision point for permanence, `traits/src/lib.rs:339` — matches it) while the
//! other two fragments are intact:
//!   * the read still returns the correct bytes (read around the fault), AND
//!   * `repair::queued_repairs` contains the chunk, with a **non-corruption**
//!     `detected_by` reason recorded at `repair::repair_key(chunk)` (read back through
//!     the `MetadataStore`, since `queued_repairs` itself returns chunk ids only).
//!
//! A companion test proves the no-corruption-signal leg holds structurally: the
//! recorded `detected_by` reason differs from the corruption producers' `"read"`.
//!
//! Flippable: on `origin/main` the RS fan-out's final error arm classifies EVERY
//! non-integrity error — including a `BlockReadFault` — as `FaultClass::Transient` and
//! enqueues nothing (`read.rs:379`, whose own comment reads "#431 owns the block-fault
//! repair question"). Pre-fix, the queue-emptiness assertion below fails (RED). Revert
//! the `read.rs` production change and this test fails again while the object still
//! reads back correctly — proving the enqueue, not the read, is what this test pins.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::{erasure, read, repair};
use wyrd_traits::{
    BlockReadFault, ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (mirrors crates/core/tests/read_repair.rs:32-153) ----

#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// A dumb in-memory chunk store holding the real stored fragment bytes; the default
/// `PlacementChunkStore::get_fragment_at` routes straight through by `FragmentId`.
#[derive(Default)]
struct MemChunks {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for MemChunks {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

impl PlacementChunkStore for MemChunks {}

/// A chunk store that returns [`BlockReadFault`] for one specific fragment, simulating a
/// D-server whose block layer cannot read that fragment's sector (a dead sector /
/// `dm-error`) — the shape `chunkstore-fs` surfaces directly, and the gRPC client
/// reconstructs client-side from `FAILED_PRECONDITION` (ADR-0010). All other fragments
/// are served normally from the inner [`MemChunks`].
struct BlockFaultingStore {
    inner: MemChunks,
    /// The one fragment whose fetch returns `BlockReadFault`.
    fault_id: FragmentId,
}

#[async_trait]
impl ChunkStore for BlockFaultingStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        if id == self.fault_id {
            return Err(Box::new(BlockReadFault::new(
                id,
                "dead sector injected by test double (issue #431)",
            )));
        }
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// The default `get_fragment_at` delegates to `get_fragment`, so a store whose
/// `get_fragment` returns `BlockReadFault` for a fragment also returns it from
/// `get_fragment_at` — exactly the shape a real backend emits.
impl PlacementChunkStore for BlockFaultingStore {}

// ---- helpers ----

/// Wrap a shard's bytes in a valid, self-describing v1 fragment for `chunk`.
fn fragment(chunk: ChunkId, payload: &[u8]) -> Bytes {
    Bytes::from(encode(
        &FragmentHeader::new_v1(chunk, payload.len() as u64),
        payload,
    ))
}

/// Commit a single-chunk inode into the metadata store, returning its id.
async fn commit_inode(meta: &MemMeta, inode: u64, chunk: ChunkRef, size: u64) {
    let record = InodeRecord {
        size,
        chunk_map: vec![chunk],
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = meta
        .commit(WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&record)))
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

// ---- #431: an RS read around a permanent block-layer read fault still enqueues repair --

/// A foreground RS(2,1) read over a store where one fragment's fetch fails with
/// `BlockReadFault` (while the other two remain readable, k = 2) must:
///   1. still return the correct reconstructed bytes (read around the fault), and
///   2. land the chunk on the shared repair queue with a NON-corruption `detected_by`
///      reason — distinct from the corruption producers' `"read"`.
///
/// Pre-fix (`origin/main`): the fault falls into the RS fan-out's final `Err(e) =>` arm,
/// classified `FaultClass::Transient` and enqueued nowhere (`read.rs:379`) — the queue
/// assertion below is RED. Post-fix: a dedicated arm recognizes
/// `wyrd_traits::is_block_read_fault`, reads around it, and enqueues the chunk with a
/// distinct reason — GREEN.
#[tokio::test]
async fn ec_read_around_block_fault_still_enqueues_repair_with_non_corruption_reason() {
    let meta = MemMeta::default();

    // RS(2,1): 3 fragments, any k = 2 suffice to reconstruct.
    let (k, m) = (2u8, 1u8);
    let data = b"a dead sector must not be absorbed silently";
    let chunk_id: ChunkId = 0xB10C_FA17;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();
    assert_eq!(shards.len(), 3);

    let inner = MemChunks::default();
    for (index, shard) in shards.iter().enumerate() {
        inner
            .put_fragment(
                FragmentId {
                    chunk: chunk_id,
                    index: index as u16,
                },
                fragment(chunk_id, shard),
            )
            .await
            .unwrap();
    }

    // Fragment index 0 comes from a D-server whose block layer cannot read that sector;
    // indices 1 and 2 are intact and sufficient (k = 2) to reconstruct.
    let fault_id = FragmentId {
        chunk: chunk_id,
        index: 0,
    };
    let chunks = BlockFaultingStore { inner, fault_id };

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    // Pre-condition: nothing is queued before the read.
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair queue starts empty"
    );

    // The read reconstructs byte-identical from the two surviving fragments...
    let got = read::read_object(&meta, &chunks, 1).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "a permanent block-layer read fault on one shard (k others readable) is read around; \
         the object still reconstructs"
    );

    // ...AND the chunk it excluded is now a durable repair obligation on the SAME queue
    // scrub feeds (`0005:174-176`) — this is the binding assertion; pre-fix it is empty.
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "a permanent block-layer read fault must land a repair obligation on the shared \
         queue, exactly as a checksum-corruption finding does — pre-fix (#431) it silently \
         read around with no obligation"
    );

    // `queued_repairs` returns chunk ids only (`repair.rs:91-98`); read the VALUE back
    // through the MetadataStore to prove the recorded reason is a non-corruption one —
    // distinct from the corruption producers' `"read"` / integrity classes (`read.rs:432`,
    // `:477`). The concrete spelling is illustrative; what's binding is that it differs.
    let recorded = meta
        .get(&repair::repair_key(chunk_id))
        .await
        .unwrap()
        .expect("the repair-queue entry's value must be readable back");
    let reason = String::from_utf8(recorded.to_vec()).unwrap();
    assert_ne!(
        reason, "read",
        "a block-layer read fault is NOT checksum corruption (`traits/src/lib.rs:164-199`) \
         and must not be recorded with the corruption producers' `\"read\"` reason; got \
         detected_by = {reason:?}"
    );
    assert_eq!(
        reason, "read-block-fault",
        "expected the read-path block-fault producer's own detected_by reason; got {reason:?}"
    );
}

// ---- #431: a block fault on a NO-redundancy chunk still enqueues repair ----------------

/// An `EcScheme::None` chunk has a single fragment and nothing to reconstruct around, so
/// a permanent block-layer read fault on it means the read itself MUST fail — but the
/// damage is exactly as permanent as in the RS case, and the failed read must still
/// leave a durable repair obligation behind (the enqueue runs before the read result is
/// surfaced, `read.rs`'s `read_object`). Otherwise the no-redundancy chunk's dead sector
/// is forgotten with the error and reconstruction never gets the queued obligation that
/// would surface it as unrepairable.
///
/// Pre-fix: the single-fragment arm's generic `Err(e) => return Err(e)` swallows the
/// classification — the read fails and the queue stays empty (RED). Post-fix: a
/// dedicated arm mirrors the adjacent integrity-fault arm — records the obligation,
/// then surfaces the error (GREEN).
#[tokio::test]
async fn none_scheme_block_fault_fails_the_read_but_still_enqueues_repair() {
    let meta = MemMeta::default();

    let data = b"a dead sector under a no-redundancy chunk still owes a repair obligation";
    let chunk_id: ChunkId = 0x0DEAD5EC;
    let frag_id = FragmentId {
        chunk: chunk_id,
        index: 0,
    };

    // The chunk's ONLY fragment is stored intact, but its D-server's block layer
    // cannot read the sector: every fetch returns `BlockReadFault`.
    let inner = MemChunks::default();
    inner
        .put_fragment(frag_id, fragment(chunk_id, data))
        .await
        .unwrap();
    let chunks = BlockFaultingStore {
        inner,
        fault_id: frag_id,
    };

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::None,
            len: data.len() as u64,
            placement: vec![0],
        },
        data.len() as u64,
    )
    .await;

    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair queue starts empty"
    );

    // With no redundancy there is nothing to read around: the read must fail...
    let result = read::read_object(&meta, &chunks, 1).await;
    assert!(
        result.is_err(),
        "a block-layer read fault on a no-redundancy chunk's only fragment cannot be \
         read around; the read must surface the error"
    );

    // ...but the permanent damage must still be a durable repair obligation, recorded
    // with the block-fault producer's own non-corruption reason — pre-fix the generic
    // error arm returns without recording anything and this queue is empty.
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "a failed no-redundancy read over a permanent block fault must still leave a \
         repair obligation on the shared queue — pre-fix the error was surfaced and the \
         damage forgotten"
    );
    let recorded = meta
        .get(&repair::repair_key(chunk_id))
        .await
        .unwrap()
        .expect("the repair-queue entry's value must be readable back");
    let reason = String::from_utf8(recorded.to_vec()).unwrap();
    assert_eq!(
        reason, "read-block-fault",
        "expected the read-path block-fault producer's own detected_by reason; got {reason:?}"
    );
}
