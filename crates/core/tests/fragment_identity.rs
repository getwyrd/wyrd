//! Issue #430: the shared read/repair validation must admit a fragment only when its
//! decoded header proves the **FULL identity** the committed chunk map requested —
//! `chunk_id`, `ec_fragment_index`, and (for Reed-Solomon) an EC tuple consistent with
//! the committed `ChunkRef.scheme` — not the `chunk_id` alone (`0005:262-267`; the
//! store-level precedent is `FsChunkStore::verify`, chunk **and** index).
//!
//! Before the fix the read path admitted a same-chunk shard for the WRONG
//! `ec_fragment_index` (and one whose header EC tuple disagreed with the committed
//! scheme) on `chunk_id` alone, pushing its payload under the requested index — wrong
//! reconstruction input, so the RS read returned silently WRONG bytes and enqueued
//! nothing. Both tests below drive the PUBLIC surface ([`read::read_object`] +
//! [`repair::queued_repairs`]) over a test-double store that serves such a fragment, so
//! reverting the production change makes them fail by ASSERTION (wrong bytes / no
//! enqueue), not by compile error.
//!
//! DETERMINISTIC RED (the RS fan-out stops as soon as `k` shards are accepted): each
//! test serves only `k` fragments total — ONE of them wrong-identity, one correct, the
//! rest absent — so the decoder necessarily consumes the wrong shard pre-fix (silently
//! wrong bytes, no enqueue) and rejects it post-fix (typed error + enqueue: below `k`
//! intact fragments remain, so the read fails rather than returning wrong bytes).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, read, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore,
    Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the path is proven over the seams,
// ---- mirroring `crates/core/tests/read_repair.rs`) --------------------------------

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
/// `PlacementChunkStore::get_fragment_at` routes straight through by `FragmentId`, so a
/// deliberately mismatched fragment stored under a slot is served verbatim — exactly the
/// shape an adversarial / corrupted backend returns.
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

/// Commit a single-chunk inode into the metadata store, returning nothing; the inode id
/// is `1` throughout.
async fn commit_inode(meta: &MemMeta, chunk: ChunkRef, size: u64) {
    let record = InodeRecord {
        size,
        chunk_map: vec![chunk],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let outcome = meta
        .commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&record)))
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

/// A payload that spans BOTH RS(2,1) data shards, so a wrong shard fed at a data index
/// corrupts the live reconstruction output — not padding a truncation would discard.
fn spanning_data() -> Vec<u8> {
    (0..200u32).map(|i| (i % 199) as u8).collect()
}

// === case 1: a same-chunk fragment served at the WRONG ec_fragment_index ============

/// A store returns a validly-encoded fragment of the SAME chunk but a DIFFERENT
/// `ec_fragment_index` at the requested slot. It must be rejected — never fed to the RS
/// decoder under the requested index — so the read NEVER returns wrong bytes (it fails
/// with a typed error, fewer than `k` intact fragments remaining) and the chunk is
/// enqueued on the shared repair queue.
///
/// Serve only `k = 2` fragments: slot 0 answers with index 1's fragment (header
/// `ec_fragment_index = 1`), slot 1 is correct, slot 2 is absent. Pre-fix the decoder
/// consumes index-1 bytes at BOTH data positions and returns silently wrong bytes with
/// no enqueue (red); post-fix slot 0 is rejected, leaving 1 < k survivors → typed error
/// + enqueue (green).
#[tokio::test]
async fn ec_read_rejects_a_same_chunk_wrong_index_fragment() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    let (k, m) = (2u8, 1u8);
    let data = spanning_data();
    let chunk_id: ChunkId = 0x0430_0001;
    let shards = erasure::encode(k as usize, m as usize, &data).unwrap();
    assert_eq!(shards.len(), 3);

    // Slot 0: a WRONG-index fragment — index 1's bytes carrying `ec_fragment_index = 1`,
    // stored under the slot the chunk map places fragment index 0 on. Its checksum and
    // chunk id are perfectly valid; only its identity index is wrong.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            encode_ec_fragment(chunk_id, 1, k, m, &shards[1]),
        )
        .await
        .unwrap();
    // Slot 1: the genuine fragment for index 1.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 1,
            },
            encode_ec_fragment(chunk_id, 1, k, m, &shards[1]),
        )
        .await
        .unwrap();
    // Slot 2: absent — so only k = 2 fragments are ever available, one of them wrong.

    commit_inode(
        &meta,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    let got = read::read_object(&meta, &chunks, 1).await;
    // The read must NEVER return wrong bytes: either it reconstructs the true object, or
    // it fails with a typed error. It must never hand back a wrong-index shard's bytes.
    match &got {
        Ok(Some(bytes)) => assert_eq!(
            bytes.as_slice(),
            data.as_slice(),
            "read returned WRONG bytes: a same-chunk wrong-index shard was fed to the decoder"
        ),
        Ok(None) => panic!("a committed object read back as absent"),
        Err(_) => { /* typed error: fewer than k intact fragments remained — acceptable */ }
    }

    // ...AND the affected chunk is enqueued on the shared repair queue, as the existing
    // misplaced-fragment arm already does (`read.rs`, `0005:174-176`).
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "the wrong-index fragment's chunk is enqueued for reconstruction"
    );
}

// === case 2: a fragment whose header EC tuple disagrees with the committed scheme =====

/// A store returns a validly-encoded fragment of the SAME chunk at the correct index but
/// whose header EC tuple (`ec_scheme_type`/`ec_k`/`ec_m`) disagrees with the committed
/// `ChunkRef.scheme` — here a `none`-scheme header (`ec_scheme_type = None`, `ec_k = 1`,
/// `ec_m = 0`) against a committed RS(2,1) chunk. Such a shard belongs to a different
/// stripe geometry and must be rejected, never fed to the RS decoder.
///
/// Same deterministic-red shape: slot 0 carries a `none`-scheme header over index 1's
/// bytes, slot 1 is correct, slot 2 absent. Pre-fix slot 0 is admitted on `chunk_id`
/// alone and the decoder returns wrong bytes with no enqueue (red); post-fix slot 0 is
/// rejected on the EC-tuple mismatch → 1 < k survivors → typed error + enqueue (green).
#[tokio::test]
async fn ec_read_rejects_a_fragment_whose_ec_tuple_disagrees_with_scheme() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    let (k, m) = (2u8, 1u8);
    let data = spanning_data();
    let chunk_id: ChunkId = 0x0430_0002;
    let shards = erasure::encode(k as usize, m as usize, &data).unwrap();
    assert_eq!(shards.len(), 3);

    // Slot 0: correct chunk id and index 0, but a `none`-scheme header (what `new_v1`
    // stamps) — its EC tuple disagrees with the committed RS(2,1) scheme. Its payload is
    // index 1's bytes, so a pre-fix admit corrupts the reconstruction.
    let wrong_ec = encode(
        &FragmentHeader::new_v1(chunk_id, shards[1].len() as u64),
        &shards[1],
    );
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(wrong_ec),
        )
        .await
        .unwrap();
    // Slot 1: the genuine RS fragment for index 1.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 1,
            },
            encode_ec_fragment(chunk_id, 1, k, m, &shards[1]),
        )
        .await
        .unwrap();
    // Slot 2: absent.

    commit_inode(
        &meta,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    let got = read::read_object(&meta, &chunks, 1).await;
    match &got {
        Ok(Some(bytes)) => assert_eq!(
            bytes.as_slice(),
            data.as_slice(),
            "read returned WRONG bytes: a shard whose EC tuple disagrees with the scheme was decoded"
        ),
        Ok(None) => panic!("a committed object read back as absent"),
        Err(_) => { /* typed error: fewer than k intact fragments remained — acceptable */ }
    }

    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "the EC-tuple-mismatched fragment's chunk is enqueued for reconstruction"
    );
}

// === case 3: a same-SCHEME-TYPE fragment whose STRIPE GEOMETRY (k/m) disagrees ========

/// A store returns a validly-encoded Reed-Solomon fragment of the SAME chunk at the
/// correct index whose scheme TYPE also matches (`ec_scheme_type = ReedSolomon`), but
/// whose stripe geometry disagrees with the committed `ChunkRef.scheme` — here an RS(3,1)
/// header against a committed RS(2,1) chunk. It belongs to a different, incompatible
/// stripe and must be rejected, never fed to the RS decoder.
///
/// This case exists specifically to pin the `ec_k`/`ec_m` comparison: because the scheme
/// TYPE matches, the `ec_scheme_type` check passes and ONLY the k/m compare can reject
/// the shard. (Case 2's `none`-type header trips the type check first, leaving the k/m
/// conjuncts unexercised.) Same deterministic-red shape: slot 0 carries an RS(3,1) header
/// over index 1's bytes at the correct index 0, slot 1 is correct, slot 2 absent. Pre-fix
/// slot 0 is admitted on `chunk_id` alone and the decoder returns wrong bytes with no
/// enqueue (red); post-fix slot 0 is rejected on the k/m mismatch → 1 < k survivors →
/// typed error + enqueue (green).
#[tokio::test]
async fn ec_read_rejects_a_same_scheme_type_wrong_geometry_fragment() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    let (k, m) = (2u8, 1u8);
    let data = spanning_data();
    let chunk_id: ChunkId = 0x0430_0003;
    let shards = erasure::encode(k as usize, m as usize, &data).unwrap();
    assert_eq!(shards.len(), 3);

    // Slot 0: correct chunk id and index 0, and an RS scheme type — but the header stamps
    // the WRONG stripe geometry RS(3,1) against the committed RS(2,1). Its payload is
    // index 1's bytes, so a pre-fix admit corrupts the reconstruction. Only the `ec_k`
    // (3 != 2) compare distinguishes it from a genuine survivor.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            encode_ec_fragment(chunk_id, 0, 3, 1, &shards[1]),
        )
        .await
        .unwrap();
    // Slot 1: the genuine RS(2,1) fragment for index 1.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 1,
            },
            encode_ec_fragment(chunk_id, 1, k, m, &shards[1]),
        )
        .await
        .unwrap();
    // Slot 2: absent.

    commit_inode(
        &meta,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    let got = read::read_object(&meta, &chunks, 1).await;
    match &got {
        Ok(Some(bytes)) => assert_eq!(
            bytes.as_slice(),
            data.as_slice(),
            "read returned WRONG bytes: a shard whose stripe geometry disagrees with the scheme was decoded"
        ),
        Ok(None) => panic!("a committed object read back as absent"),
        Err(_) => { /* typed error: fewer than k intact fragments remained — acceptable */ }
    }

    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "the wrong-geometry fragment's chunk is enqueued for reconstruction"
    );
}
