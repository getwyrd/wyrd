//! Regression tests pinning the wyrd-core durability-path mutants the nightly
//! `cargo mutants` sweep reported as surviving (issue #225, Tier 1). Each test
//! names the mutant it kills by `file:line`; a `cargo mutants` run over those
//! source files must now report the cited line as `caught`, not `MISSED`.
//!
//! Proven in-process over the trait stores — the same backend-agnostic style as
//! `read_repair.rs` / `write_fanout.rs`.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, ObjectMeta, PendingEntry,
};
use wyrd_core::placement::Topology;
use wyrd_core::read::ReadError;
use wyrd_core::{erasure, read, write};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore,
    Result, WriteBatch,
};

const ROOT: InodeId = 0;
const CHUNK: usize = 1 << 16; // one chunk per test payload

// ---- in-memory trait stores (backend-agnostic; mirrors read_repair.rs) ----

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

/// Commit a single-chunk inode into the metadata store.
async fn commit_inode(meta: &MemMeta, inode: InodeId, chunk: ChunkRef, size: u64) {
    let record = InodeRecord {
        size,
        chunk_map: vec![chunk],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let outcome = meta
        .commit(WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&record)))
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

// === write.rs:302 (`== -> !=`) — a winning write RELEASES its lease ====================

#[tokio::test]
async fn write_new_object_releases_pending_on_commit() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    let mut next = 0x42u128;

    let outcome = write::write_new_object(
        &meta,
        &chunks,
        ROOT,
        "obj",
        1,
        b"a freshly written object",
        CHUNK,
        EcScheme::ReedSolomon { k: 2, m: 1 },
        || 1_000,
        5_000,
        || {
            next += 1;
            next
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    // The release runs only when the commit WON (`outcome == Committed`). Flipping
    // that to `!=` skips the release on a winning commit, leaving leased garbage.
    let pending = meta.scan(b"pending:").await.unwrap();
    assert!(
        pending.is_empty(),
        "a committed write releases its pending ledger entry, leaving none behind"
    );
}

// === write.rs:337 (`== -> !=`) — the placed write RELEASES its lease too ===============

#[tokio::test]
async fn write_new_object_placed_releases_pending_on_commit() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    // RS(2,1) -> n = 3 fragments, so the placement needs 3 distinct failure domains.
    let mut topo = Topology::default();
    topo.register(0, "A").register(1, "B").register(2, "C");
    let mut next = 0x42u128;

    let outcome = write::write_new_object_placed(
        &meta,
        &chunks,
        ROOT,
        "obj",
        1,
        b"a placed object across distinct domains",
        CHUNK,
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        || 1_000,
        5_000,
        || {
            next += 1;
            next
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    let pending = meta.scan(b"pending:").await.unwrap();
    assert!(
        pending.is_empty(),
        "a committed placed write releases its pending ledger entry"
    );
}

// === write.rs:357 (`<= -> >`) AND :364 (`delete !`) — the sweep reclaims AT the lease
// expiry boundary and actually COMMITS the deletion ===================================

#[tokio::test]
async fn sweep_reclaims_at_expiry_boundary_and_deletes() {
    let meta = MemMeta::default();
    // One lease expiring exactly at `now` (the boundary), one still in the future.
    metadata::put_pending(
        &meta,
        0xAAAA,
        &PendingEntry {
            lease_expiry_millis: 100,
        },
    )
    .await
    .unwrap();
    metadata::put_pending(
        &meta,
        0xBBBB,
        &PendingEntry {
            lease_expiry_millis: 200,
        },
    )
    .await
    .unwrap();

    let reclaimed = write::sweep_expired_leases(&meta, 100).await.unwrap();

    // `:357` boundary — `expiry <= now` is expired, so the lease expiring exactly at
    // `now` IS reclaimed and the future one is NOT. Turning `<=` into `>` flips both.
    assert_eq!(
        reclaimed,
        vec![0xAAAA],
        "the lease expiring at `now` is reclaimed; the unexpired one is left alone"
    );
    // `:364` — a non-empty reclaim must COMMIT the batch of deletes. Dropping the `!`
    // skips the commit, so the entry is returned as reclaimed yet never removed.
    let remaining = meta.scan(b"pending:").await.unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "the reclaimed lease is deleted from the store; only the unexpired one remains"
    );
    assert_eq!(
        remaining[0].0,
        metadata::pending_key(0xBBBB),
        "the survivor is exactly the unexpired lease"
    );
}

// === read.rs:195 (`< -> >`) — too few readable fragments is InsufficientFragments ======

#[tokio::test]
async fn read_with_fewer_than_k_fragments_reports_insufficient() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    let (k, m) = (2u8, 1u8);
    let data = b"reconstruction needs at least k fragments";
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();
    let chunk_id: ChunkId = 0xF11D;

    // Store only ONE fragment (k - 1 = 1, fewer than k): unreconstructible. Stamp its
    // FULL RS header identity (`encode_ec_fragment`) so the read path's full-identity
    // check admits it — the point under test is the below-`k` guard, not identity.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            write::encode_ec_fragment(chunk_id, 0, k, m, &shards[0]),
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

    let err = read::read_object(&meta, &chunks, 1).await.unwrap_err();
    // The guard must surface a `ReadError::InsufficientFragments`. Flipping `<` to `>`
    // makes the guard never fire, so reconstruct is called with < k shards and the
    // error is an erasure `TooFewShards` instead — which does NOT downcast to ReadError.
    let read_err = err
        .downcast_ref::<ReadError>()
        .expect("a too-few-fragments read is a ReadError, not a bare erasure error");
    assert!(
        matches!(
            read_err,
            ReadError::InsufficientFragments {
                have: 1,
                need: 2,
                ..
            }
        ),
        "1 of the 2 needed fragments yields InsufficientFragments{{have:1,need:2}}, got {read_err:?}"
    );
}

// === metadata.rs:207 (`+ -> -`) — a commit bumps the inode version by exactly one ======

#[tokio::test]
async fn commit_chunk_map_bumps_version_by_one() {
    let meta = MemMeta::default();
    let id: InodeId = 7;
    let prior = InodeRecord {
        size: 0,
        chunk_map: vec![],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    // Seed the prior record so the commit's CAS precondition matches.
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), metadata::encode(&prior)))
        .await
        .unwrap();

    let outcome = metadata::commit_chunk_map(&meta, id, &prior, vec![], 42)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    let stored = meta.get(&metadata::inode_key(id)).await.unwrap().unwrap();
    let next: InodeRecord = metadata::decode(&stored).unwrap();
    assert_eq!(
        next.version, 2,
        "commit_chunk_map writes version = prior.version + 1 (here 1 + 1)"
    );
}

// === metadata.rs `commit_chunk_map` `..prior.clone()` — a reconstruction/backfill
//     re-commit of the SAME content PRESERVES the object-metadata trio (ADR-0047):
//     a placement-maintenance commit must not move Last-Modified or drop the
//     ETag/content type. Guards the preservation `..prior.clone()` against silently
//     regressing to `..Default::default()` (which still compiles but drops the trio to
//     `None`). Every OTHER commit_chunk_map test seeds all-`None` metadata, so this
//     invariant is otherwise VACUOUSLY true. =====================================

#[tokio::test]
async fn commit_chunk_map_preserves_object_metadata_across_a_repair() {
    let meta = MemMeta::default();
    let id: InodeId = 9;
    // A published object carrying the full ADR-0047 metadata trio (the state a real
    // create/overwrite leaves behind — never all-`None` for a published object).
    let prior = InodeRecord {
        size: 42,
        chunk_map: vec![],
        state: InodeState::Committed,
        version: 3,
        etag: Some("d1f2e3c4b5a60718".to_string()),
        content_type: Some("text/plain; charset=utf-8".to_string()),
        modified: Some(1_700_000_000_123),
    };
    // Seed it so the reconstruction commit's CAS precondition matches.
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), metadata::encode(&prior)))
        .await
        .unwrap();

    // A reconstruction/backfill re-commit: the SAME content (size unchanged), re-placed.
    let outcome = metadata::commit_chunk_map(&meta, id, &prior, vec![], 42)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    let stored = meta.get(&metadata::inode_key(id)).await.unwrap().unwrap();
    let next: InodeRecord = metadata::decode(&stored).unwrap();
    assert_eq!(
        next.version, 4,
        "the repair commit bumped the version by one"
    );
    assert_eq!(
        next.etag, prior.etag,
        "a repair PRESERVES the ETag (ADR-0047): re-placing the same content does not \
         republish it, so it mints no new change-token"
    );
    assert_eq!(
        next.content_type, prior.content_type,
        "a repair PRESERVES the stored content type"
    );
    assert_eq!(
        next.modified, prior.modified,
        "a repair must NOT move Last-Modified"
    );
}

// === metadata.rs:581 / :641 (`modified: meta.modified`) — a content OVERWRITE is a fresh
//     publication (ADR-0047), so the superseding commit STAMPS the new publication time,
//     never carries the prior version's forward. Distinct from the preservation invariant
//     above: a repair keeps the old `modified`, an overwrite mints a new one. The wire
//     overwrite test cannot pin this — its two PUTs land in the same wall-clock second, so
//     regressing `meta.modified` to `prior.modified` still passes it. These unit tests seed a
//     prior with a DISTINCT `modified` and assert the fresh one lands, killing the mutant on
//     BOTH the plain (:581) and the leased (:641, the path the wire PUT drives) commits. ===

#[tokio::test]
async fn commit_chunk_map_superseding_stamps_a_fresh_modified() {
    let meta = MemMeta::default();
    let id: InodeId = 11;
    // A prior publication carrying its OWN distinct metadata trio.
    let prior = InodeRecord {
        size: 10,
        chunk_map: vec![],
        state: InodeState::Committed,
        version: 5,
        etag: Some("aaaaaaaaaaaaaaaa".to_string()),
        content_type: Some("text/plain; charset=utf-8".to_string()),
        modified: Some(1_700_000_000_000),
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), metadata::encode(&prior)))
        .await
        .unwrap();

    // The overwrite publishes DIFFERENT content with a DISTINCT publication time.
    let fresh = ObjectMeta {
        etag: Some("bbbbbbbbbbbbbbbb".to_string()),
        content_type: Some("application/json".to_string()),
        modified: Some(1_700_000_999_999),
    };
    assert_ne!(
        fresh.modified, prior.modified,
        "the test is only meaningful if the fresh time differs from the prior one"
    );

    let outcome = metadata::commit_chunk_map_superseding(&meta, id, &prior, vec![], 20, 0, &fresh)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    let stored = meta.get(&metadata::inode_key(id)).await.unwrap().unwrap();
    let next: InodeRecord = metadata::decode(&stored).unwrap();
    assert_eq!(
        next.modified, fresh.modified,
        "an overwrite STAMPS the new publication time (ADR-0047), not the prior version's — \
         guards `modified: meta.modified` against regressing to `prior.modified`"
    );
    assert_eq!(
        next.etag, fresh.etag,
        "the overwrite stamps the fresh ETag, not the prior one"
    );
    assert_eq!(
        next.content_type, fresh.content_type,
        "the overwrite stamps the fresh content type"
    );
}

#[tokio::test]
async fn commit_chunk_map_superseding_leased_stamps_a_fresh_modified() {
    let meta = MemMeta::default();
    let id: InodeId = 12;
    let prior = InodeRecord {
        size: 10,
        chunk_map: vec![],
        state: InodeState::Committed,
        version: 2,
        etag: Some("cccccccccccccccc".to_string()),
        content_type: Some("text/plain; charset=utf-8".to_string()),
        modified: Some(1_700_000_000_000),
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), metadata::encode(&prior)))
        .await
        .unwrap();

    // The new version's chunk holds a LIVE pending lease (expires after `now`), so the leased
    // overwrite CAS lands — this is the streaming-overwrite path the S3 wire PUT drives.
    let new_chunk: ChunkId = 0x00C0_FFEE;
    let now: u64 = 500;
    metadata::put_pending(
        &meta,
        new_chunk,
        &PendingEntry {
            lease_expiry_millis: 1_000,
        },
    )
    .await
    .unwrap();

    let fresh = ObjectMeta {
        etag: Some("dddddddddddddddd".to_string()),
        content_type: Some("application/json".to_string()),
        modified: Some(1_700_000_999_999),
    };
    assert_ne!(
        fresh.modified, prior.modified,
        "the test is only meaningful if the fresh time differs from the prior one"
    );

    let outcome = metadata::commit_chunk_map_superseding_leased(
        &meta,
        id,
        &prior,
        vec![],
        20,
        0,
        &[new_chunk],
        now,
        &fresh,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed,
        "the live pending lease lets the leased overwrite land"
    );

    let stored = meta.get(&metadata::inode_key(id)).await.unwrap().unwrap();
    let next: InodeRecord = metadata::decode(&stored).unwrap();
    assert_eq!(
        next.modified, fresh.modified,
        "the LEASED overwrite (the path the wire PUT uses) stamps the fresh publication time — \
         guards metadata.rs's `modified: meta.modified` at the leased commit against regressing \
         to `prior.modified`"
    );
    assert_eq!(
        next.etag, fresh.etag,
        "the leased overwrite stamps the fresh ETag"
    );
    assert_eq!(
        next.content_type, fresh.content_type,
        "the leased overwrite stamps the fresh content type"
    );
}
