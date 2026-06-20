//! Tier 1 DST: seed-reproducible property tests for the erasure-coded data path
//! (M1.6, ADR-0009), driven by `wyrd_testkit::Sim` over the real redb + filesystem
//! backends. Where `dst_commit.rs` proves the commit protocol, this proves the
//! rs(6,3) read reconstructs correctly under randomized fragment loss and
//! corruption — and that mixed-era chunk maps read through one path (ADR-0008).
//!
//! Single-threaded and deterministic: re-running a seed reproduces the run
//! exactly, so any seed that ever surfaces a bug is pinned below as a permanent
//! regression guard.

use pollster::block_on;
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_core::metadata::{EcScheme, InodeRecord, InodeState};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{ChunkId, CommitOutcome, FragmentId};

const ROOT: u64 = 0;
const CHUNK: usize = 4;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const SEEDS: u64 = 64;

// The default scheme under test: rs(6,3) → n = 9 fragments per chunk, surviving
// up to m = 3 losses.
const K: u8 = 6;
const M: u8 = 3;
const N: u16 = (K + M) as u16;
const RS: EcScheme = EcScheme::ReedSolomon { k: K, m: M };

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    let chunks = FsChunkStore::open(dir.path()).expect("fs store");
    (meta, chunks, dir)
}

fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A random payload of up to `max` bytes, drawn from the seeded RNG.
fn payload(sim: &mut Sim, max: usize) -> Vec<u8> {
    let len = (sim.gen::<u16>() as usize) % (max + 1);
    (0..len).map(|_| sim.gen::<u8>()).collect()
}

/// A random payload of 1..=`max` bytes — guarantees at least one chunk.
fn nonempty_payload(sim: &mut Sim, max: usize) -> Vec<u8> {
    let len = 1 + (sim.gen::<u16>() as usize) % max;
    (0..len).map(|_| sim.gen::<u8>()).collect()
}

/// `count` distinct fragment indices in `0..n`, chosen by a partial Fisher-Yates
/// shuffle from the seeded RNG.
fn choose_indices(sim: &mut Sim, n: u16, count: usize) -> Vec<u16> {
    let mut pool: Vec<u16> = (0..n).collect();
    let count = count.min(pool.len());
    for i in 0..count {
        let j = i + (sim.gen::<u16>() as usize) % (pool.len() - i);
        pool.swap(i, j);
    }
    pool.truncate(count);
    pool
}

/// Write a new object end to end under `scheme` and return its committed inode.
async fn put(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    data: &[u8],
    base: u128,
    scheme: EcScheme,
) -> InodeRecord {
    assert_eq!(
        write::write_new_object(
            meta,
            chunks,
            ROOT,
            "obj",
            1,
            data,
            CHUNK,
            scheme,
            NOW,
            TTL,
            ids_from(base),
        )
        .await
        .unwrap(),
        CommitOutcome::Committed
    );
    read::read_inode(meta, 1).await.unwrap().unwrap()
}

fn delete(root: &std::path::Path, chunk: ChunkId, index: u16) {
    std::fs::remove_file(fragment_path(root, FragmentId { chunk, index })).unwrap();
}

/// Flip the last byte of a fragment so its payload checksum fails verification.
fn corrupt(root: &std::path::Path, chunk: ChunkId, index: u16) {
    let path = fragment_path(root, FragmentId { chunk, index });
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
}

/// Property: deleting up to `m` fragments from *every* chunk still reconstructs
/// the object byte-identical.
fn loss_survival(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = payload(&mut sim, 48);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;

        for chunk in &inode.chunk_map {
            let count = (sim.gen::<u8>() as usize) % (M as usize + 1); // 0..=m
            for index in choose_indices(&mut sim, N, count) {
                delete(dir.path(), chunk.id, index);
            }
        }
        assert_eq!(
            read::read_object_from(&chunks, &inode).await.unwrap(),
            data,
            "seed {seed}: <= m losses per chunk must reconstruct"
        );
    });
}

/// Property: deleting `m + 1` fragments of a chunk yields a clean typed error,
/// never a panic or corrupt bytes.
fn loss_beyond_m_is_clean_error(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = nonempty_payload(&mut sim, 48);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;

        let chunk = inode.chunk_map[0];
        for index in choose_indices(&mut sim, N, M as usize + 1) {
            delete(dir.path(), chunk.id, index);
        }
        let err = read::read_object_from(&chunks, &inode).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<read::ReadError>(),
                Some(read::ReadError::InsufficientFragments { .. })
            ),
            "seed {seed}: m+1 losses must surface InsufficientFragments, got: {err}"
        );
    });
}

/// Property: bit-flipping up to `m` fragments per chunk is excluded by the
/// checksum and reconstructed around — never garbage.
fn corruption_excluded(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = payload(&mut sim, 48);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;

        for chunk in &inode.chunk_map {
            let count = (sim.gen::<u8>() as usize) % (M as usize + 1); // 0..=m
            for index in choose_indices(&mut sim, N, count) {
                corrupt(dir.path(), chunk.id, index);
            }
        }
        assert_eq!(
            read::read_object_from(&chunks, &inode).await.unwrap(),
            data,
            "seed {seed}: <= m corruptions per chunk must reconstruct around them"
        );
    });
}

/// Property: corrupting `m + 1` fragments of a chunk leaves < k valid → a clean
/// typed error, never garbage.
fn corruption_beyond_m_is_clean_error(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = nonempty_payload(&mut sim, 48);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;

        let chunk = inode.chunk_map[0];
        for index in choose_indices(&mut sim, N, M as usize + 1) {
            corrupt(dir.path(), chunk.id, index);
        }
        let err = read::read_object_from(&chunks, &inode).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<read::ReadError>(),
                Some(read::ReadError::InsufficientFragments { .. })
            ),
            "seed {seed}: m+1 corruptions must surface InsufficientFragments, got: {err}"
        );
    });
}

/// Property: an inode whose chunk map mixes a `none` chunk and an `rs(6,3)` chunk
/// reads correctly through one path (ADR-0008 mixed-era).
fn mixed_era_read(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (_meta, chunks, _dir) = backends();
        let part_none = payload(&mut sim, 48);
        let part_rs = payload(&mut sim, 48);

        let plan_none =
            write::plan_write(&part_none, CHUNK, EcScheme::None, ids_from(0x100)).unwrap();
        let plan_rs = write::plan_write(&part_rs, CHUNK, RS, ids_from(0x200)).unwrap();
        write::write_fragments(&chunks, &plan_none).await.unwrap();
        write::write_fragments(&chunks, &plan_rs).await.unwrap();

        let inode = InodeRecord {
            size: (part_none.len() + part_rs.len()) as u64,
            chunk_map: plan_none
                .chunk_refs()
                .into_iter()
                .chain(plan_rs.chunk_refs())
                .collect(),
            state: InodeState::Committed,
            version: 1,
        };

        let mut expected = part_none.clone();
        expected.extend_from_slice(&part_rs);
        assert_eq!(
            read::read_object_from(&chunks, &inode).await.unwrap(),
            expected,
            "seed {seed}: mixed-era chunk map reads byte-identical"
        );
    });
}

#[test]
fn rs_reads_survive_losses_across_seeds() {
    for seed in 0..SEEDS {
        loss_survival(seed);
    }
}

#[test]
fn rs_loss_beyond_m_is_a_clean_error_across_seeds() {
    for seed in 0..SEEDS {
        loss_beyond_m_is_clean_error(seed);
    }
}

#[test]
fn rs_corruption_is_excluded_across_seeds() {
    for seed in 0..SEEDS {
        corruption_excluded(seed);
    }
}

#[test]
fn rs_corruption_beyond_m_is_a_clean_error_across_seeds() {
    for seed in 0..SEEDS {
        corruption_beyond_m_is_clean_error(seed);
    }
}

#[test]
fn mixed_era_reads_across_seeds() {
    for seed in 0..SEEDS {
        mixed_era_read(seed);
    }
}

/// A pinned seed kept as a permanent regression guard (ADR-0009): any seed that
/// ever surfaces a bug is added here so the exact scenario is replayed forever.
#[test]
fn ec_properties_hold_at_pinned_regression_seed() {
    const REGRESSION_SEED: u64 = 0x00C0_FFEE;
    loss_survival(REGRESSION_SEED);
    loss_beyond_m_is_clean_error(REGRESSION_SEED);
    corruption_excluded(REGRESSION_SEED);
    corruption_beyond_m_is_clean_error(REGRESSION_SEED);
    mixed_era_read(REGRESSION_SEED);
}
