//! Conformance and property tests for the redb `MetadataStore`.
//!
//! The assertions are written against the `MetadataStore` *trait* surface and
//! the backend-agnostic `core::metadata` model, so they lift to a shared suite
//! when a second backend (TiKV) arrives. redb is sync, so the async methods
//! never yield and `pollster::block_on` drives them deterministically.

use pollster::block_on;
use wyrd_core::metadata::{self, InodeRecord, InodeState, PendingEntry};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

fn store() -> RedbMetadataStore {
    RedbMetadataStore::in_memory().expect("in-memory redb store")
}

// ---- Trait contract (generic over any MetadataStore) -----------------------

async fn contract_commit_and_get(store: &impl MetadataStore) {
    let outcome = store
        .commit(
            WriteBatch::new()
                .put(b"a".to_vec(), "1")
                .put(b"b".to_vec(), "2"),
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(store.get(b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(store.get(b"b").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(store.get(b"missing").await.unwrap(), None);
}

async fn contract_scan_by_prefix(store: &impl MetadataStore) {
    store
        .commit(
            WriteBatch::new()
                .put(b"p:1".to_vec(), "x")
                .put(b"p:2".to_vec(), "y")
                .put(b"q:1".to_vec(), "z"),
        )
        .await
        .unwrap();
    let mut hits = store.scan(b"p:").await.unwrap();
    hits.sort();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, b"p:1");
    assert_eq!(hits[1].0, b"p:2");
}

async fn contract_require_absent_gates(store: &impl MetadataStore) {
    store
        .commit(WriteBatch::new().put(b"k".to_vec(), "v"))
        .await
        .unwrap();
    // The key now exists, so require_absent must reject — and write nothing.
    let outcome = store
        .commit(
            WriteBatch::new()
                .require_absent(b"k".to_vec())
                .put(b"side".to_vec(), "effect"),
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Conflict);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(
        store.get(b"side").await.unwrap(),
        None,
        "batch must be atomic"
    );
}

async fn contract_require_value_gates(store: &impl MetadataStore) {
    store
        .commit(WriteBatch::new().put(b"k".to_vec(), "v"))
        .await
        .unwrap();
    let stale = store
        .commit(
            WriteBatch::new()
                .require(b"k".to_vec(), "WRONG")
                .put(b"k".to_vec(), "v2"),
        )
        .await
        .unwrap();
    assert_eq!(stale, CommitOutcome::Conflict);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));

    let fresh = store
        .commit(
            WriteBatch::new()
                .require(b"k".to_vec(), "v")
                .put(b"k".to_vec(), "v2"),
        )
        .await
        .unwrap();
    assert_eq!(fresh, CommitOutcome::Committed);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v2"[..]));
}

#[test]
fn trait_contract() {
    block_on(async {
        contract_commit_and_get(&store()).await;
        contract_scan_by_prefix(&store()).await;
        contract_require_absent_gates(&store()).await;
        contract_require_value_gates(&store()).await;
    });
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
            let map_a: Vec<u128> = vec![sim.gen(), sim.gen()];
            let map_b: Vec<u128> = vec![sim.gen()];

            let a = metadata::commit_chunk_map(&s, id, &prior, map_a.clone())
                .await
                .unwrap();
            let b = metadata::commit_chunk_map(&s, id, &prior, map_b)
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
