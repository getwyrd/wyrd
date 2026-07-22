//! Integration tests for the four-phase write/commit path (architecture §5),
//! wired against the real backends `server` composes — an in-memory redb
//! `MetadataStore` and a filesystem `ChunkStore` — and seeded by `testkit`.
//!
//! `server` is the one crate allowed to know concretes (ADR-0010), so these
//! end-to-end tests of the protocol belong here. Sync backends never yield, so
//! `pollster::block_on` drives the async path deterministically.

#![forbid(unsafe_code)]

use pollster::block_on;
use wyrd_chunk_format::decode;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{self, DirentRecord, EcScheme, InodeRecord, InodeState};
use wyrd_core::write;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{ChunkStore, CommitOutcome, FragmentId, MetadataStore};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const AFTER_LEASE: u64 = NOW + TTL + 1;
const CHUNK: usize = 4;

/// A unique, deterministic chunk-id generator starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let meta = RedbMetadataStore::in_memory().expect("redb");
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = FsChunkStore::open(dir.path()).expect("fs store");
    (meta, chunks, dir)
}

async fn read_inode(meta: &RedbMetadataStore, id: u64) -> Option<InodeRecord> {
    let bytes = meta.get(&metadata::inode_key(id)).await.unwrap()?;
    Some(metadata::decode(&bytes).unwrap())
}

async fn pending_count(meta: &RedbMetadataStore) -> usize {
    meta.scan(b"pending:").await.unwrap().len()
}

/// Reassemble an object's bytes by fetching every fragment in its chunk map.
async fn read_object(meta: &RedbMetadataStore, chunks: &FsChunkStore, inode_id: u64) -> Vec<u8> {
    let inode = read_inode(meta, inode_id).await.expect("inode");
    let mut out = Vec::new();
    for chunk in &inode.chunk_map {
        let fragment = chunks
            .get_fragment(FragmentId {
                chunk: chunk.id,
                index: 0,
            })
            .await
            .unwrap()
            .expect("fragment present");
        out.extend_from_slice(&decode(&fragment).unwrap().payload);
    }
    out
}

#[test]
fn write_produces_an_atomically_committed_readable_file() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let data = b"hello wyrd, this object spans several chunks";

        let outcome = write::write_new_object(
            &meta,
            &chunks,
            ROOT,
            "file",
            1,
            data,
            CHUNK,
            EcScheme::None,
            || NOW,
            TTL,
            ids_from(0x10),
        )
        .await
        .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);

        let inode = read_inode(&meta, 1).await.unwrap();
        assert_eq!(inode.state, InodeState::Committed);
        assert_eq!(inode.version, 1);
        assert_eq!(inode.size, data.len() as u64);
        assert_eq!(inode.chunk_map.len(), data.len().div_ceil(CHUNK));

        let dirent: DirentRecord = metadata::decode(
            &meta
                .get(&metadata::dirent_key(ROOT, "file"))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(dirent.inode, 1);

        // Content round-trips through the chunk store, and the ledger is released.
        assert_eq!(read_object(&meta, &chunks, 1).await, data);
        assert_eq!(pending_count(&meta).await, 0);
    });
}

#[test]
fn crash_before_commit_leaves_only_collectable_garbage() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let mut next = ids_from(0x20);
        let plan = write::plan_write(
            b"data that is staged but never committed",
            CHUNK,
            EcScheme::None,
            &mut next,
        )
        .unwrap();

        write::intent(&meta, &plan, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        // --- crash: no commit ---

        // The file does not exist: no dirent, no inode.
        assert!(meta
            .get(&metadata::dirent_key(ROOT, "file"))
            .await
            .unwrap()
            .is_none());
        assert!(read_inode(&meta, 1).await.is_none());
        // The ledger holds the intent; the fragments are on disk (collectable).
        assert_eq!(pending_count(&meta).await, plan.chunks.len());
        for chunk in &plan.chunks {
            let id = FragmentId {
                chunk: chunk.id,
                index: 0,
            };
            assert!(chunks.get_fragment(id).await.unwrap().is_some());
        }

        // The sweep spares unexpired leases and reclaims expired ones.
        assert!(write::sweep_expired_leases(&meta, NOW)
            .await
            .unwrap()
            .is_empty());
        let reclaimed = write::sweep_expired_leases(&meta, AFTER_LEASE)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), plan.chunks.len());
        assert_eq!(pending_count(&meta).await, 0);
    });
}

#[test]
fn crash_between_commit_and_release_leaves_entries_the_sweep_reclaims() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let mut next = ids_from(0x30);
        let plan = write::plan_write(
            b"committed but not released",
            CHUNK,
            EcScheme::None,
            &mut next,
        )
        .unwrap();

        write::intent(&meta, &plan, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        let outcome = write::commit_create(&meta, ROOT, "file", 1, &plan, NOW)
            .await
            .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);
        // --- crash: no release ---

        // The file fully exists, yet its ledger entries linger.
        assert_eq!(
            read_inode(&meta, 1).await.unwrap().state,
            InodeState::Committed
        );
        assert_eq!(pending_count(&meta).await, plan.chunks.len());

        // The sweep reclaims the lingering entries; the committed file is intact.
        write::sweep_expired_leases(&meta, AFTER_LEASE)
            .await
            .unwrap();
        assert_eq!(pending_count(&meta).await, 0);
        assert_eq!(
            read_object(&meta, &chunks, 1).await,
            b"committed but not released"
        );
    });
}

#[test]
fn exactly_one_overwrite_wins_under_a_concurrent_writer() {
    block_on(async {
        for seed in 0..64u64 {
            let mut sim = Sim::new(seed);
            let (meta, chunks, _dir) = backends();

            // An existing file at version 1.
            write::write_new_object(
                &meta,
                &chunks,
                ROOT,
                "obj",
                1,
                b"v1",
                CHUNK,
                EcScheme::None,
                || NOW,
                TTL,
                ids_from(sim.gen()),
            )
            .await
            .unwrap();
            let prior = read_inode(&meta, 1).await.unwrap();

            // Two writers read the same prior and each stage a new version.
            let plan_a =
                write::plan_write(b"winner", CHUNK, EcScheme::None, ids_from(sim.gen())).unwrap();
            let plan_b =
                write::plan_write(b"loser too", CHUNK, EcScheme::None, ids_from(sim.gen()))
                    .unwrap();
            write::intent(&meta, &plan_a, NOW + TTL).await.unwrap();
            write::write_fragments(&chunks, &plan_a).await.unwrap();
            write::intent(&meta, &plan_b, NOW + TTL).await.unwrap();
            write::write_fragments(&chunks, &plan_b).await.unwrap();

            // They race to commit on the same prior; exactly one wins.
            let a = write::commit_overwrite(&meta, 1, &prior, &plan_a, 0)
                .await
                .unwrap();
            let b = write::commit_overwrite(&meta, 1, &prior, &plan_b, 0)
                .await
                .unwrap();
            assert_eq!(a, CommitOutcome::Committed, "seed {seed}");
            assert_eq!(b, CommitOutcome::Conflict, "seed {seed}");

            let committed = read_inode(&meta, 1).await.unwrap();
            assert_eq!(committed.version, 2, "seed {seed}: bumped once");
            assert_eq!(
                committed.chunk_map,
                plan_a.chunk_refs(),
                "seed {seed}: winner persisted"
            );

            // The loser left leased garbage; the sweep reclaims it.
            write::release(&meta, &plan_a).await.unwrap();
            assert!(
                pending_count(&meta).await > 0,
                "seed {seed}: loser's leases linger"
            );
            write::sweep_expired_leases(&meta, AFTER_LEASE)
                .await
                .unwrap();
            assert_eq!(pending_count(&meta).await, 0, "seed {seed}");
        }
    });
}

#[test]
fn concurrent_create_of_the_same_name_has_one_winner() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let plan_a = write::plan_write(b"a", CHUNK, EcScheme::None, ids_from(0x100)).unwrap();
        let plan_b = write::plan_write(b"b", CHUNK, EcScheme::None, ids_from(0x200)).unwrap();
        write::intent(&meta, &plan_a, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan_a).await.unwrap();
        write::intent(&meta, &plan_b, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan_b).await.unwrap();

        let a = write::commit_create(&meta, ROOT, "same", 10, &plan_a, NOW)
            .await
            .unwrap();
        let b = write::commit_create(&meta, ROOT, "same", 11, &plan_b, NOW)
            .await
            .unwrap();
        assert_eq!(a, CommitOutcome::Committed);
        assert_eq!(b, CommitOutcome::Conflict);
        // The loser's inode was never written (the create was atomic).
        assert!(read_inode(&meta, 11).await.is_none());
    });
}
