//! Conformance and property tests for the redb `MetadataStore`.
//!
//! The generic trait-contract assertions now live in the **shared**
//! `wyrd-metadata-conformance` crate (proposal 0007, M4.1): redb drives the
//! identical suite the TiKV backend drives, so the contract is pinned by both
//! implementations rather than forked. redb is sync, so the async methods never
//! yield and `pollster::block_on` drives them deterministically.
//!
//! The backend-specific model/property tests (via `core::metadata`, seeded by
//! `testkit`) stay here — they exercise redb's serialized-write-transaction
//! guarantee directly and are not part of the backend-agnostic contract.

#![forbid(unsafe_code)]

use pollster::block_on;
use wyrd_core::metadata::{self, InodeRecord, InodeState, PendingEntry};
use wyrd_metadata_conformance as conformance;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{CommitOutcome, MetadataStore};

fn store() -> RedbMetadataStore {
    RedbMetadataStore::in_memory().expect("in-memory redb store")
}

// ---- Trait contract (the shared, not forked, suite) ------------------------

#[test]
fn trait_contract() {
    // The whole shared contract via the single `run_all` runner, so redb and TiKV drive
    // the identical clause set with no per-driver list to drift (#419 read-consistency
    // clauses previously ran here but not on TiKV). Each clause gets a fresh, empty
    // in-memory store — the same isolation the TiKV target provides per clause.
    block_on(conformance::run_all(|_tag| async { store() }));
}

// ---- DoD via the metadata model -------------------------------------------

#[test]
fn create_writes_inode_and_dirent_atomically() {
    block_on(async {
        let s = store();
        let outcome = metadata::create(&s, 1, "file", 2, &InodeRecord::new_empty())
            .await
            .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);

        let inode: InodeRecord =
            metadata::decode(&s.get(&metadata::inode_key(2)).await.unwrap().unwrap()).unwrap();
        assert_eq!(inode, InodeRecord::new_empty());
        let dirent: metadata::DirentRecord = metadata::decode(
            &s.get(&metadata::dirent_key(1, "file"))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(dirent.inode, 2);

        // A second create over the same name conflicts and writes nothing — the
        // new inode id must not be half-written.
        let dup = metadata::create(&s, 1, "file", 3, &InodeRecord::new_empty())
            .await
            .unwrap();
        assert_eq!(dup, CommitOutcome::Conflict);
        assert_eq!(s.get(&metadata::inode_key(3)).await.unwrap(), None);
    });
}

#[test]
fn rename_is_one_dirent_mutation() {
    block_on(async {
        let s = store();
        metadata::create(&s, 1, "file", 2, &InodeRecord::new_empty())
            .await
            .unwrap();
        let inode_before = s.get(&metadata::inode_key(2)).await.unwrap();

        let outcome = metadata::rename(&s, 1, "file", 1, "renamed").await.unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);

        assert_eq!(s.get(&metadata::dirent_key(1, "file")).await.unwrap(), None);
        let moved: metadata::DirentRecord = metadata::decode(
            &s.get(&metadata::dirent_key(1, "renamed"))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(moved.inode, 2);
        // The inode itself is untouched by a rename.
        assert_eq!(s.get(&metadata::inode_key(2)).await.unwrap(), inode_before);
    });
}

#[test]
fn pending_ledger_written_and_cleared() {
    block_on(async {
        let s = store();
        let chunk: u128 = 0xabcd_ef01_2345_6789_abcd_ef01_2345_6789;
        metadata::put_pending(
            &s,
            chunk,
            &PendingEntry {
                lease_expiry_millis: 5_000,
            },
        )
        .await
        .unwrap();
        assert!(s
            .get(&metadata::pending_key(chunk))
            .await
            .unwrap()
            .is_some());

        metadata::sweep_pending(&s, &[chunk]).await.unwrap();
        assert_eq!(s.get(&metadata::pending_key(chunk)).await.unwrap(), None);
    });
}

// ---- Version CAS: exactly one concurrent writer wins -----------------------

#[test]
fn version_cas_rejects_a_stale_writer() {
    block_on(async {
        for seed in 0..128u64 {
            let mut sim = Sim::new(seed);
            let s = store();
            let id: u64 = sim.gen();
            metadata::create(&s, 0, "obj", id, &InodeRecord::new_empty())
                .await
                .unwrap();

            // Both writers read the same prior inode, then race to commit a
            // chunk map. redb serializes the commits; exactly one wins.
            let prior: InodeRecord =
                metadata::decode(&s.get(&metadata::inode_key(id)).await.unwrap().unwrap()).unwrap();
            let chunk = |id: u128| metadata::ChunkRef {
                id,
                scheme: metadata::EcScheme::None,
                len: 0,
                placement: vec![0],
            };
            let map_a: Vec<metadata::ChunkRef> = vec![chunk(sim.gen()), chunk(sim.gen())];
            let map_b: Vec<metadata::ChunkRef> = vec![chunk(sim.gen())];

            let a = metadata::commit_chunk_map(&s, id, &prior, map_a.clone(), prior.size)
                .await
                .unwrap();
            let b = metadata::commit_chunk_map(&s, id, &prior, map_b, prior.size)
                .await
                .unwrap();
            assert_eq!(
                a,
                CommitOutcome::Committed,
                "seed {seed}: first writer wins"
            );
            assert_eq!(
                b,
                CommitOutcome::Conflict,
                "seed {seed}: stale writer rejected"
            );

            let committed: InodeRecord =
                metadata::decode(&s.get(&metadata::inode_key(id)).await.unwrap().unwrap()).unwrap();
            assert_eq!(
                committed.chunk_map, map_a,
                "seed {seed}: winner's map persisted"
            );
            assert_eq!(
                committed.version,
                prior.version + 1,
                "seed {seed}: version bumped once"
            );
            assert_eq!(committed.state, InodeState::Committed);
        }
    });
}
