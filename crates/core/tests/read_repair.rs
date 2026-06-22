//! M3.5 (issue #143, proposal 0005 slice 5): the **read path feeds the shared repair
//! queue** (`0005:174-176`). The scrub loop and the read path land repair obligations
//! on **one** durable queue ([`wyrd_core::repair`]); this is the read-path producer's
//! regression home (the enqueue seam lands in `core`, where the read path lives).
//!
//! BINDING leg 4 of the success criterion: a read that **excludes** a checksum-failing
//! fragment also **enqueues** its chunk for repair onto the same queue scrub feeds — a
//! corruption finding discovered reactively on read is never absorbed silently. Proven
//! in-process over the trait stores. The enqueue is keyed by the very
//! [`repair::repair_key`] the scrub loop also enqueues onto, so "the same queue" holds
//! by construction.
//!
//! Flippable demonstration: drop the enqueue loop in [`read::read_object`] and the
//! repair-queue assertions below fire while the bytes still read back — proving the
//! enqueue, not the read, is what these tests pin.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{encode, FragmentHeader, CORE_HEADER_LEN};
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::{erasure, read, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore,
    Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the path is proven over the seams) ----

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

// ---- leg 4: an EC read excludes a corrupt fragment AND enqueues its chunk ----

#[tokio::test]
async fn ec_read_excludes_corrupt_fragment_and_enqueues_for_repair() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    // A real RS(2,1) chunk: 3 fragments, reconstructible from any k = 2.
    let (k, m) = (2u8, 1u8);
    let data = b"the read path catches bit rot too";
    let chunk_id: ChunkId = 0xF00D;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();
    assert_eq!(shards.len(), 3);

    // Store all three fragments; then inject a bit-flip into fragment index 0's
    // payload so its checksum fails — the read must exclude it and reconstruct from
    // the surviving two.
    for (index, shard) in shards.iter().enumerate() {
        chunks
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
    let mut rotten = fragment(chunk_id, &shards[0]).to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(rotten),
        )
        .await
        .unwrap();

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

    // The read reconstructs byte-identical from the two surviving fragments...
    let got = read::read_object(&meta, &chunks, 1).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "the corrupt fragment is read around; the object reconstructs"
    );

    // ...AND the chunk it excluded is now a durable repair obligation on the SAME
    // queue scrub feeds — keyed by the shared `repair::repair_key` (`0005:174-176`).
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "the read-time checksum failure enqueued its chunk for reconstruction"
    );
    assert_eq!(
        meta.get(&repair::repair_key(chunk_id)).await.unwrap(),
        Some(Bytes::from_static(b"read")),
        "the obligation records the read-path producer"
    );
}

// ---- leg 4 (unrecoverable case): a corrupt single fragment still enqueues ----

#[tokio::test]
async fn unrecoverable_read_still_enqueues_the_corrupt_chunk() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    // A `none`-scheme chunk has a single fragment; a corrupt one cannot be read
    // around, so the read fails — but the corruption is still a durable repair
    // obligation, never silently absorbed.
    let chunk_id: ChunkId = 0xBEEF;
    let payload = b"lonely fragment";
    let mut rotten = fragment(chunk_id, payload).to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(rotten),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::None,
            len: payload.len() as u64,
            placement: vec![0],
        },
        payload.len() as u64,
    )
    .await;

    let result = read::read_object(&meta, &chunks, 1).await;
    assert!(
        result.is_err(),
        "a corrupt single fragment cannot be read around: the read fails"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "even a failed read leaves the corrupt chunk enqueued for reconstruction"
    );
}
