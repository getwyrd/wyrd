//! Integration tests for the erasure-coded write + read path (M1.3/M1.4,
//! architecture §5/§6.1/§6.2/§6.6, proposal 0003): a chunk is coded into n = k+m
//! fragments stored under one chunk id, the chunk map records the scheme + logical
//! length, and the read reconstructs from any k — excluding missing or
//! checksum-failing fragments and erroring cleanly below k. Wired against the real
//! redb + filesystem backends; `pollster::block_on` drives the sync path.

use pollster::block_on;
use wyrd_chunk_format::{decode, EcSchemeType};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_core::metadata::{ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{ChunkId, ChunkStore, CommitOutcome, FragmentId};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
// One chunk per object: larger than the test payloads so each object is a single
// chunk, making the per-chunk fragment count easy to assert.
const CHUNK: usize = 1 << 16;
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

fn ids_from(base: u128) -> impl FnMut() -> ChunkId {
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

/// The number of fragment files stored under a chunk id (its on-disk directory).
fn fragment_count(root: &std::path::Path, chunk: ChunkId) -> usize {
    let chunk_dir = fragment_path(root, FragmentId { chunk, index: 0 })
        .parent()
        .expect("fragment path has a chunk directory")
        .to_path_buf();
    std::fs::read_dir(chunk_dir)
        .expect("chunk directory exists")
        .filter(|e| {
            e.as_ref()
                .map(|e| e.path().extension().is_some_and(|x| x == "frag"))
                .unwrap_or(false)
        })
        .count()
}

#[test]
fn rs_put_stages_n_fragments_and_reads_back_byte_identical() {
    block_on(async {
        let (meta, chunks, dir) = backends();
        let data = b"the quick brown fox erasure-codes over the lazy dog";

        let outcome = write::write_new_object(
            &meta,
            &chunks,
            ROOT,
            "ec",
            1,
            data,
            CHUNK,
            RS,
            || NOW,
            TTL,
            ids_from(0x10),
        )
        .await
        .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);

        // The chunk map records one chunk under the RS(6,3) scheme with the
        // chunk's logical length.
        let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();
        assert_eq!(inode.chunk_map.len(), 1, "single-chunk object");
        let chunk = inode.chunk_map[0].clone();
        assert_eq!(chunk.scheme, RS, "scheme recorded in the chunk map");
        assert_eq!(chunk.len, data.len() as u64, "logical length recorded");

        // n = k + m = 9 fragments staged under the one chunk id, and each carries
        // EC header fields that agree with the chunk map's scheme + its index.
        assert_eq!(fragment_count(dir.path(), chunk.id), 9, "k + m fragments");
        for index in 0..9u16 {
            let frag = chunks
                .get_fragment(FragmentId {
                    chunk: chunk.id,
                    index,
                })
                .await
                .unwrap()
                .expect("fragment present");
            let header = decode(&frag).unwrap().header;
            assert_eq!(header.chunk_id, chunk.id, "fragments share the chunk id");
            assert_eq!(header.ec_scheme_type, EcSchemeType::ReedSolomon);
            assert_eq!(header.ec_k, 6);
            assert_eq!(header.ec_m, 3);
            assert_eq!(
                header.ec_fragment_index, index,
                "index stamped per fragment"
            );
        }
        // A tenth fragment was never written.
        assert!(chunks
            .get_fragment(FragmentId {
                chunk: chunk.id,
                index: 9
            })
            .await
            .unwrap()
            .is_none());

        // The object reconstructs byte-identical through the read path.
        assert_eq!(read::read_object_from(&chunks, &inode).await.unwrap(), data);
    });
}

#[test]
fn exactly_one_overwrite_wins_under_rs() {
    block_on(async {
        for seed in 0..32u64 {
            let mut sim = Sim::new(seed);
            let (meta, chunks, _dir) = backends();

            // An existing RS(6,3) object at version 1.
            write::write_new_object(
                &meta,
                &chunks,
                ROOT,
                "obj",
                1,
                b"v1",
                CHUNK,
                RS,
                || NOW,
                TTL,
                ids_from(sim.gen()),
            )
            .await
            .unwrap();
            let prior = read::read_inode(&meta, 1).await.unwrap().unwrap();

            // Two writers stage new RS versions and race to commit. Each runs the Intent phase
            // first so its chunks hold a live pending lease at the commit — phase 3 is now
            // lease-conditional (issue #490).
            let plan_a = write::plan_write(b"winner", CHUNK, RS, ids_from(sim.gen())).unwrap();
            let plan_b = write::plan_write(b"loser!", CHUNK, RS, ids_from(sim.gen())).unwrap();
            write::intent(&meta, &plan_a, NOW + TTL).await.unwrap();
            write::write_fragments(&chunks, &plan_a).await.unwrap();
            write::intent(&meta, &plan_b, NOW + TTL).await.unwrap();
            write::write_fragments(&chunks, &plan_b).await.unwrap();

            let a = write::commit_overwrite(&meta, 1, &prior, &plan_a, 0)
                .await
                .unwrap();
            let b = write::commit_overwrite(&meta, 1, &prior, &plan_b, 0)
                .await
                .unwrap();
            assert_eq!(a, CommitOutcome::Committed, "seed {seed}: first wins");
            assert_eq!(b, CommitOutcome::Conflict, "seed {seed}: stale rejected");

            let committed = read::read_inode(&meta, 1).await.unwrap().unwrap();
            assert_eq!(committed.version, 2, "seed {seed}: bumped once");
            assert_eq!(
                read::read_object_from(&chunks, &committed).await.unwrap(),
                b"winner"
            );
        }
    });
}

#[test]
fn mixed_era_chunks_read_through_one_path() {
    block_on(async {
        let (_meta, chunks, _dir) = backends();
        let part_none = b"chunk stored under replication(1)/none";
        let part_rs = b"chunk stored under reed-solomon rs(6,3)";

        // One chunk per scheme, distinct chunk ids, both staged.
        let plan_none =
            write::plan_write(part_none, CHUNK, EcScheme::None, ids_from(0x100)).unwrap();
        let plan_rs = write::plan_write(part_rs, CHUNK, RS, ids_from(0x200)).unwrap();
        write::write_fragments(&chunks, &plan_none).await.unwrap();
        write::write_fragments(&chunks, &plan_rs).await.unwrap();

        // An inode whose chunk map mixes the two eras (ADR-0008).
        let chunk_map: Vec<ChunkRef> = plan_none
            .chunk_refs()
            .into_iter()
            .chain(plan_rs.chunk_refs())
            .collect();
        let inode = InodeRecord {
            size: (part_none.len() + part_rs.len()) as u64,
            chunk_map,
            state: InodeState::Committed,
            version: 1,
            ..Default::default()
        };

        let mut expected = part_none.to_vec();
        expected.extend_from_slice(part_rs);
        assert_eq!(
            read::read_object_from(&chunks, &inode).await.unwrap(),
            expected
        );
    });
}

// --- M1.4: resilient read (reconstruct from any k) -------------------------------

/// A deterministic, non-trivial single-chunk payload (spans several shards).
fn sample() -> Vec<u8> {
    (0..500u32)
        .map(|i| i.wrapping_mul(31).wrapping_add(7) as u8)
        .collect()
}

/// Write a single-chunk rs(6,3) object and return its committed inode snapshot.
async fn put_rs(meta: &RedbMetadataStore, chunks: &FsChunkStore, data: &[u8]) -> InodeRecord {
    write::write_new_object(
        meta,
        chunks,
        ROOT,
        "obj",
        1,
        data,
        CHUNK,
        RS,
        || NOW,
        TTL,
        ids_from(0x10),
    )
    .await
    .unwrap();
    let inode = read::read_inode(meta, 1).await.unwrap().unwrap();
    assert_eq!(inode.chunk_map.len(), 1, "single-chunk object");
    inode
}

fn frag_path(root: &std::path::Path, chunk: ChunkId, index: u16) -> std::path::PathBuf {
    fragment_path(root, FragmentId { chunk, index })
}

/// Flip the last byte of a fragment on disk so its checksum fails verification.
fn corrupt(root: &std::path::Path, chunk: ChunkId, index: u16) {
    let path = frag_path(root, chunk, index);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
}

#[test]
fn reads_survive_any_m_fragment_losses() {
    block_on(async {
        let data = sample();
        // Every way to delete m = 3 of the n = 9 fragments (C(9,3) = 84): each keeps
        // exactly k = 6, so the chunk must still reconstruct byte-identical.
        for mask in 0u16..(1 << 9) {
            if mask.count_ones() != 3 {
                continue;
            }
            let (meta, chunks, dir) = backends();
            let inode = put_rs(&meta, &chunks, &data).await;
            let cid = inode.chunk_map[0].id;
            for index in 0..9u16 {
                if mask & (1 << index) != 0 {
                    std::fs::remove_file(frag_path(dir.path(), cid, index)).unwrap();
                }
            }
            assert_eq!(
                read::read_object_from(&chunks, &inode).await.unwrap(),
                data,
                "deleting fragments {mask:#011b} must still reconstruct"
            );
        }
    });
}

#[test]
fn fewer_than_k_fragments_is_a_clean_typed_error() {
    block_on(async {
        let data = sample();
        let (meta, chunks, dir) = backends();
        let inode = put_rs(&meta, &chunks, &data).await;
        let cid = inode.chunk_map[0].id;

        // Delete m + 1 = 4 fragments → only 5 < k = 6 remain.
        for index in [0u16, 1, 2, 3] {
            std::fs::remove_file(frag_path(dir.path(), cid, index)).unwrap();
        }
        let err = read::read_object_from(&chunks, &inode).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<read::ReadError>(),
                Some(read::ReadError::InsufficientFragments {
                    have: 5,
                    need: 6,
                    ..
                })
            ),
            "below k must surface InsufficientFragments, got: {err}"
        );
    });
}

#[test]
fn checksum_failing_fragments_are_excluded_and_reconstructed_around() {
    block_on(async {
        let data = sample();

        // Corrupting up to m = 3 fragments still reconstructs around them — a
        // checksum-failing shard is never fed to the decoder.
        for corrupt_count in 1..=3u16 {
            let (meta, chunks, dir) = backends();
            let inode = put_rs(&meta, &chunks, &data).await;
            let cid = inode.chunk_map[0].id;
            for index in 0..corrupt_count {
                corrupt(dir.path(), cid, index);
            }
            assert_eq!(
                read::read_object_from(&chunks, &inode).await.unwrap(),
                data,
                "corrupting {corrupt_count} fragment(s) must reconstruct around them"
            );
        }

        // Corrupting m + 1 = 4 leaves < k valid fragments → typed error.
        let (meta, chunks, dir) = backends();
        let inode = put_rs(&meta, &chunks, &data).await;
        let cid = inode.chunk_map[0].id;
        for index in 0..4u16 {
            corrupt(dir.path(), cid, index);
        }
        let err = read::read_object_from(&chunks, &inode).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<read::ReadError>(),
                Some(read::ReadError::InsufficientFragments { .. })
            ),
            "m+1 corruptions must surface InsufficientFragments, got: {err}"
        );
    });
}
