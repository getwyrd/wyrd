//! Commit-protocol concurrency tests on madsim's deterministic, single-threaded
//! scheduler (ADR-0009). madsim interleaves the spawned writer tasks at their
//! `.await` boundaries from the run seed; each `commit()` is internally
//! synchronous (one redb write transaction, no await inside), so the only
//! question the scheduler decides is *which* writer commits first — and the
//! version compare-and-set guarantees exactly one does, under every interleaving.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`); without it this file
//! compiles to nothing, so a normal `cargo test` neither builds nor runs it.
#![cfg(madsim)]

use std::sync::Arc;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::CommitOutcome;

const CHUNK: usize = 4;
const LEASE_EXPIRY: u64 = 6_000;

/// A unique, deterministic chunk-id generator starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

#[madsim::test]
async fn exactly_one_concurrent_writer_wins() {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = Arc::new(RedbMetadataStore::in_memory().expect("redb"));
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    // An existing object at version 1.
    let v0 = write::plan_write(b"v0", CHUNK, EcScheme::None, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // Four writers read the same prior, stage independently, then race to commit.
    // madsim schedules their interleaving from the seed.
    let mut handles = Vec::new();
    for i in 0..4u128 {
        let meta = Arc::clone(&meta);
        let chunks = Arc::clone(&chunks);
        let prior = prior.clone();
        handles.push(madsim::task::spawn(async move {
            let plan = write::plan_write(
                b"contended",
                CHUNK,
                EcScheme::None,
                ids_from(0x1000 * (i + 1)),
            )
            .unwrap();
            write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
            write::write_fragments(&*chunks, &plan).await.unwrap();
            let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan)
                .await
                .unwrap();
            if outcome == CommitOutcome::Committed {
                write::release(&*meta, &plan).await.unwrap();
            }
            outcome
        }));
    }

    let mut winners = 0;
    for handle in handles {
        if handle.await.unwrap() == CommitOutcome::Committed {
            winners += 1;
        }
    }

    assert_eq!(
        winners, 1,
        "exactly one concurrent writer must win the commit"
    );
    let after = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    assert_eq!(
        after.version,
        prior.version + 1,
        "version bumped exactly once"
    );

    // The committed object is whole and readable (the winner's content).
    let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
    assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
}
