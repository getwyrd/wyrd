//! Concurrency regression for `FsChunkStore::put_fragment` (issue #203).
//!
//! Many writers racing on the **same** `FragmentId` must all succeed: each uses
//! a private scratch file and publishes via an atomic rename, so no writer can
//! observe or clobber another's partial bytes and none fails spuriously. Before
//! the fix the scratch path was keyed on the `FragmentId` alone (`<index>.tmp`)
//! and shared across calls; concurrent same-id writes raced on it and the second
//! `fs::rename` could hit `NotFound`, spuriously erroring a legitimate
//! duplicate/repair write.
//!
//! The store's I/O is synchronous (`std::fs`), so real OS threads driving
//! `pollster::block_on` give genuine concurrency without an async runtime; a
//! `Barrier` releases every writer at once to widen the write→rename race. The
//! load is import-light (no GUI / display / async runtime), so it is safe on a
//! headless runner. Per-write scratch *uniqueness* is also asserted structurally
//! in the crate's unit tests
//! (`scratch_names_are_unique_per_seq_and_invisible_to_listing`); this test is
//! the behavioural half — every concurrent put returns `Ok`.

#![forbid(unsafe_code)]

use std::sync::Barrier;
use std::thread;

use bytes::Bytes;
use pollster::block_on;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_traits::{ChunkStore, FragmentId};

/// Build a valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

/// Writers released together per round, and rounds repeated: the pre-fix race is
/// interleaving-dependent, so writers × rounds amplify it to a near-certain red,
/// while the post-fix green is deterministic (every write `Ok` regardless of
/// interleaving — each has private scratch, the rename is the only publish).
const WRITERS: usize = 64;
const ROUNDS: usize = 16;

/// Release `WRITERS` threads at once, each writing `frag` under `id`, and return
/// every call's outcome (the error rendered to a `String` so it crosses the
/// thread boundary cleanly).
fn race_same_id(
    store: &FsChunkStore,
    id: FragmentId,
    frag: &Bytes,
) -> Vec<std::result::Result<(), String>> {
    let barrier = Barrier::new(WRITERS);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..WRITERS)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    block_on(store.put_fragment(id, frag.clone())).map_err(|e| e.to_string())
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[test]
fn concurrent_same_id_writes_all_succeed_and_publish_one_verified_fragment() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");

    let id = FragmentId {
        chunk: 0x00c0_ffee_u128,
        index: 7,
    };
    let frag = fragment(id, b"a duplicate/repair fragment written concurrently");

    for round in 0..ROUNDS {
        let results = race_same_id(&store, id, &frag);
        for (writer, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "round {round} writer {writer}: a concurrent same-id put must not fail \
                 spuriously, got {:?}",
                result.as_ref().err()
            );
        }
    }

    // The atomic rename published exactly one complete, verifying fragment.
    let got = block_on(store.get_fragment(id))
        .expect("get must not error")
        .expect("the fragment was published");
    assert_eq!(
        got, frag,
        "the published fragment is byte-complete and verifies"
    );

    // Scratch files never surface as fragments: the store lists exactly the one id.
    let listed = block_on(store.list_fragments()).expect("list");
    assert_eq!(
        listed,
        vec![id],
        "list_fragments ignores temp scratch, reporting only the one published fragment"
    );
}
