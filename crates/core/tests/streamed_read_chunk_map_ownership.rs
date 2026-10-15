//! The streamed GET's chunk list is **moved** out of the committed record, not copied
//! (issue #686).
//!
//! The gateway's streamed read (`get_object_streaming`, `crates/server/src/lib.rs`) owns
//! the committed inode it just read and hands the resolved chunk list to its reader task.
//! It resolves through [`resolve_chunk_map_owned`], so these tests drive that same path:
//!
//! * a **flat** record on a live snapshot answers with the record's OWN allocation — the
//!   outer `Vec` buffer and the first chunk's `placement` buffer are the very pointers the
//!   record held, so no copy of the list (or of any placement vector) happened;
//! * a **segmented** record still answers its chunks assembled from its `seg:` records in
//!   table order, framed by its own record;
//! * a record **superseded mid-resolve** still restarts onto the live generation and is
//!   framed by THAT generation's record, not the caller's stale snapshot;
//! * a record **deleted mid-resolve** still answers "no such object" (`Ok(None)`), the
//!   final answer, rather than an error or a restart onto an absent root.
//!
//! The flat list is non-empty and every placement is non-empty on purpose: a copied empty
//! `Vec` keeps the same dangling pointer as the original, so an empty fixture could not
//! tell a move from a copy.

#![forbid(unsafe_code)]

use wyrd_core::metadata::{
    encode, inode_key, resolve_chunk_map_owned, seg_key, ChunkMap, ChunkRef, EcScheme, InodeRecord,
    InodeState, SegmentGroup, SegmentRecord, SegmentRef, SegmentedMap,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

const INODE: u64 = 7;
/// A 32-lowercase-hex nonce, exactly the length the `seg:` grammar requires.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 3;

/// A chunk of `len` bytes with a real, non-empty placement (one D server per fragment of
/// an RS(2,1) scheme).
fn chunk(id: u128, len: u64) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
        len,
        placement: vec![10 + id as u64, 20 + id as u64, 30 + id as u64],
    }
}

fn chunks(ids: std::ops::Range<u128>) -> Vec<ChunkRef> {
    ids.map(|id| chunk(id, 4)).collect()
}

/// A committed flat record over `list`, with framing fields distinct per `version` so a
/// test can tell which generation framed an answer.
fn flat_record(list: Vec<ChunkRef>, version: u64) -> InodeRecord {
    InodeRecord {
        size: list.iter().map(|c| c.len).sum(),
        chunk_map: ChunkMap::Flat(list),
        state: InodeState::Committed,
        version,
        etag: Some(format!("etag-v{version}")),
        content_type: Some(format!("application/x-v{version}")),
        modified: Some(1_000 + version),
    }
}

/// A two-segment spelling of `list` under `group`: the root's table and its `seg:` rows.
fn segmented(
    group: &SegmentGroup,
    list: &[ChunkRef],
    version: u64,
) -> (InodeRecord, Vec<(Vec<u8>, bytes::Bytes)>) {
    let (front, back) = list.split_at(list.len() / 2);
    let first = SegmentRecord::new(front.to_vec(), 0).expect("segment 0");
    let second = SegmentRecord::new(back.to_vec(), first.byte_len()).expect("segment 1");
    let refs = vec![
        SegmentRef {
            index: 0,
            byte_offset: 0,
            byte_len: first.byte_len(),
        },
        SegmentRef {
            index: 1,
            byte_offset: first.byte_len(),
            byte_len: second.byte_len(),
        },
    ];
    let table = SegmentedMap::new(group.clone(), refs).expect("segment table");
    let record = InodeRecord {
        size: table.span(),
        chunk_map: ChunkMap::Segmented(table),
        state: InodeState::Committed,
        version,
        etag: Some(format!("etag-v{version}")),
        content_type: Some(format!("application/x-v{version}")),
        modified: Some(1_000 + version),
    };
    let rows = vec![
        (seg_key(group, 0).expect("key 0"), encode(&first)),
        (seg_key(group, 1).expect("key 1"), encode(&second)),
    ];
    (record, rows)
}

async fn seed(meta: &RedbMetadataStore, rows: Vec<(Vec<u8>, bytes::Bytes)>) {
    let mut batch = WriteBatch::new();
    for (key, value) in rows {
        batch = batch.put(key, value);
    }
    assert_eq!(
        meta.commit(batch).await.expect("seed"),
        CommitOutcome::Committed
    );
}

#[tokio::test]
async fn a_flat_record_hands_over_its_own_chunk_list_by_move() {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let record = flat_record(chunks(1..4), 2);
    // The root the store holds IS this snapshot: the resolve does not restart.
    seed(&meta, vec![(inode_key(INODE), encode(&record))]).await;

    let expected = record.clone();
    let list = record.chunk_map.as_flat().expect("flat fixture");
    assert!(
        list.len() >= 2,
        "a non-empty list, or a copy is undetectable"
    );
    assert!(
        list.iter().all(|c| !c.placement.is_empty()),
        "non-empty placements, or a copy is undetectable"
    );
    let outer = list.as_ptr();
    let first_placement = list[0].placement.as_ptr();

    let resolved = resolve_chunk_map_owned(&meta, &inode_key(INODE), record)
        .await
        .expect("resolve")
        .expect("a committed object");

    assert_eq!(
        resolved.chunks.as_ptr(),
        outer,
        "the chunk list is the record's own allocation (moved), not a copy"
    );
    assert_eq!(
        resolved.chunks[0].placement.as_ptr(),
        first_placement,
        "the first placement vector is the record's own allocation (moved), not a copy"
    );
    assert_eq!(
        Some(&resolved.chunks[..]),
        expected.chunk_map.as_flat(),
        "contents unchanged"
    );
    assert_eq!(resolved.size, expected.size);
    assert_eq!(resolved.version, expected.version);
    assert_eq!(resolved.etag, expected.etag);
    assert_eq!(resolved.content_type, expected.content_type);
    assert_eq!(resolved.modified, expected.modified);
}

#[tokio::test]
async fn a_segmented_record_answers_its_segments_in_table_order() {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let group = SegmentGroup::new(NONCE, EPOCH).expect("group");
    let list = chunks(1..5);
    let (record, mut rows) = segmented(&group, &list, 2);
    rows.push((inode_key(INODE), encode(&record)));
    seed(&meta, rows).await;

    let resolved = resolve_chunk_map_owned(&meta, &inode_key(INODE), record.clone())
        .await
        .expect("resolve")
        .expect("a committed object");

    assert_eq!(resolved.chunks, list, "segments assembled in table order");
    assert_eq!(resolved.size, record.size);
    assert_eq!(resolved.version, record.version);
    assert_eq!(resolved.etag, record.etag);
    assert_eq!(resolved.content_type, record.content_type);
    assert_eq!(resolved.modified, record.modified);
}

#[tokio::test]
async fn a_record_superseded_mid_resolve_restarts_onto_the_live_generation() {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let group = SegmentGroup::new(NONCE, EPOCH).expect("group");
    // The caller's snapshot: a segmented generation whose `seg:` rows are already gone —
    // retired under the read. The store's root now names a NEW flat generation.
    let (stale, _retired_rows) = segmented(&group, &chunks(1..5), 2);
    let live_list = chunks(100..103);
    let live = flat_record(live_list.clone(), 3);
    seed(&meta, vec![(inode_key(INODE), encode(&live))]).await;

    let resolved = resolve_chunk_map_owned(&meta, &inode_key(INODE), stale)
        .await
        .expect("resolve")
        .expect("an overwrite is not a deletion");

    assert_eq!(resolved.chunks, live_list, "the live generation's chunks");
    assert_eq!(resolved.size, live.size, "framed by the live generation");
    assert_eq!(resolved.version, live.version);
    assert_eq!(resolved.etag, live.etag);
    assert_eq!(resolved.content_type, live.content_type);
    assert_eq!(resolved.modified, live.modified);
}

#[tokio::test]
async fn a_record_deleted_mid_resolve_answers_no_such_object() {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let group = SegmentGroup::new(NONCE, EPOCH).expect("group");
    // The caller's snapshot: a segmented generation whose root was deleted under the read.
    // The root always moves first, so its `seg:` rows are still there, not yet drained.
    let (snapshot, rows) = segmented(&group, &chunks(1..5), 2);
    seed(&meta, rows).await;

    let resolved = resolve_chunk_map_owned(&meta, &inode_key(INODE), snapshot)
        .await
        .expect("a deletion is an answer, not an error");

    assert_eq!(
        resolved, None,
        "a deleted object has no live committed generation"
    );
}
