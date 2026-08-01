//! The gateway's whole-object and ranged GET over a **segmented** object (issue #649,
//! proposal 0016 decision 7(e)) — through the base-visible [`ObjectGateway`] trait (its
//! streaming and ranged entries are trait methods, not inherent `pub fn`s, so the trait
//! must be in scope: the idiom already used at `crates/server/tests/s3_http_wire.rs:44`
//! and `crates/server/tests/e2e.rs:85`).
//!
//! Every object here is seeded as raw `seg:` records plus a segmented root — a genuine
//! flat write (real fragments, via `wyrd_core::write`) re-spelled by a raw `WriteBatch`,
//! **never via a committer** (this slice ships no producer) — because `Gateway`'s fields
//! are private to its own crate, so an external integration test seeds through its own
//! store handle *before* handing it to `Gateway::new`.
//!
//! RED on this slice's base: `get_object_streaming` / `get_object_range` take the
//! record's `.chunk_map` directly (`crates/server/src/lib.rs:364-365`, `:459-460`) and
//! fail closed with `SegmentedMapUnsupported` for every segmented object — each case here
//! fails as an assertion, and the file still compiles because every symbol it imports is
//! base-visible from #648.

#![forbid(unsafe_code)]

use std::sync::Arc;

use futures_util::StreamExt;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    encode, inode_key, seg_key, ChunkMap, EcScheme, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord, SegmentRef, SegmentedMap,
};
use wyrd_core::write;
use wyrd_gateway_core::{ByteRange, ObjectGateway, ObjectStream, RangeOutcome};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 4;
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const OTHER_NONCE: &str = "fedcba9876543210fedcba9876543210";
/// The framing metadata (ADR-0047) the segmented generation carries — a segmented
/// object's own headers must ride out with its bytes exactly as a flat one's do.
const ETAG: &str = "etag-of-the-segmented-generation";
const CONTENT_TYPE: &str = "text/segmented";
const MODIFIED: u64 = 111;

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = FsChunkStore::open(dir.path()).expect("fs chunk store");
    (meta, chunks, dir)
}

/// Plan, fan out and commit a genuine **flat** object through the ordinary write path —
/// real fragments on disk, and the dirent the gateway resolves `key` through.
async fn write_flat(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    inode_id: u64,
    name: &str,
    data: &[u8],
    id_base: u128,
) -> write::WritePlan {
    let mut next = id_base;
    let plan = write::plan_write(data, CHUNK, EcScheme::None, || {
        next += 1;
        next
    })
    .expect("plan");
    write::intent(meta, &plan, NOW + TTL).await.expect("intent");
    write::write_fragments(chunks, &plan)
        .await
        .expect("fan out");
    assert_eq!(
        write::commit_create(meta, ROOT, name, inode_id, &plan, NOW)
            .await
            .expect("commit"),
        CommitOutcome::Committed
    );
    write::release(meta, &plan).await.expect("release");
    plan
}

/// Overwrite `inode_id`'s root with the **segmented** spelling of `plan`'s own chunks,
/// split into `n` segments keyed by `group` — a raw batch, never a committer. `keep` says
/// how many of those segment records are actually written, so a fixture can leave one the
/// root names unsatisfied.
async fn reshape_into_segments(
    meta: &RedbMetadataStore,
    inode_id: u64,
    plan: &write::WritePlan,
    group: &SegmentGroup,
    n: usize,
    keep: usize,
) {
    let chunks = plan.chunk_refs();
    assert!(n >= 2 && n <= chunks.len(), "fixture needs a real split");
    let mut refs = Vec::new();
    let mut offset = 0u64;
    let mut batch = WriteBatch::new();
    for (index, part) in chunks.chunks(chunks.len().div_ceil(n)).enumerate() {
        let record = SegmentRecord::new(part.to_vec(), offset).expect("segment record");
        refs.push(SegmentRef {
            index: index as u32,
            byte_offset: offset,
            byte_len: record.byte_len(),
        });
        offset += record.byte_len();
        if index < keep {
            let key = seg_key(group, index as u32).expect("addressable index");
            batch = batch.put(key, encode(&record));
        }
    }
    let table = SegmentedMap::new(group.clone(), refs).expect("segment table");
    let root = InodeRecord {
        size: table.span(),
        chunk_map: ChunkMap::Segmented(table),
        state: InodeState::Committed,
        version: 1,
        etag: Some(ETAG.to_owned()),
        content_type: Some(CONTENT_TYPE.to_owned()),
        modified: Some(MODIFIED),
    };
    batch = batch.put(inode_key(inode_id), encode(&root));
    assert_eq!(
        meta.commit(batch).await.expect("seed the segmented root"),
        CommitOutcome::Committed
    );
}

async fn drain(mut stream: ObjectStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(piece) = stream.next().await {
        out.extend_from_slice(&piece.expect("chunk read"));
    }
    out
}

/// **Both criterion-(1) gateway entries on one segmented fixture.** The whole-object
/// stream, and a range that **straddles a segment boundary** — the case a consumer
/// reading only the root's table, or only the first segment's record, cannot serve. Two
/// segments of 24 bytes (12 four-byte chunks, split 6/6), so `[20, 30)` crosses the seam.
/// The control is the same payload published and left flat: byte-identical is the claim,
/// so a byte-for-byte comparison against it is the assertion.
#[tokio::test]
async fn a_segmented_object_reads_byte_identical_whole_and_across_a_segment_boundary() {
    let (meta, chunks, _dir) = backends();
    let data: Vec<u8> = (0..48u32).map(|i| (i % 251) as u8).collect();
    let plan = write_flat(&meta, &chunks, 1, "segmented", &data, 0x100).await;
    let group = SegmentGroup::new(NONCE, 1).unwrap();
    reshape_into_segments(&meta, 1, &plan, &group, 2, 2).await;
    // The flat control, published the ordinary way and left alone.
    write_flat(&meta, &chunks, 2, "flat", &data, 0x200).await;
    let gateway =
        Arc::new(Gateway::new(meta, chunks, MemCoordination::new()).with_chunk_size(CHUNK));

    let segmented = Arc::clone(&gateway)
        .get_object_streaming("segmented")
        .await
        .unwrap()
        .expect("the segmented object is committed");
    assert_eq!(segmented.size, data.len() as u64);
    // A segmented object's own framing rides out with it exactly as a flat one's does.
    assert_eq!(segmented.etag.as_deref(), Some(ETAG));
    assert_eq!(segmented.content_type.as_deref(), Some(CONTENT_TYPE));
    assert_eq!(segmented.modified, Some(MODIFIED));
    let segmented_bytes = drain(segmented.stream).await;
    assert_eq!(segmented_bytes, data, "the source data");

    let flat = Arc::clone(&gateway)
        .get_object_streaming("flat")
        .await
        .unwrap()
        .expect("the flat control is committed");
    assert_eq!(
        segmented_bytes,
        drain(flat.stream).await,
        "segmented and flat stream the same bytes"
    );

    for (case, key) in [("segmented", "segmented"), ("flat", "flat")] {
        let range_read = Arc::clone(&gateway)
            .get_object_range(key, ByteRange::FromTo(20, 29))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{case}: the object is committed"));
        let RangeOutcome::Satisfiable {
            offset,
            len,
            stream,
        } = range_read.outcome
        else {
            panic!("{case}: a 10-byte range inside a 48-byte object must be satisfiable");
        };
        assert_eq!((offset, len), (20, 10));
        assert_eq!(
            drain(stream).await,
            &data[20..30],
            "{case}: a range spanning the segment boundary is the flat range's bytes"
        );
    }
}

/// **An object the gateway cannot resolve fails closed for ITSELF ALONE.** The root names
/// a segment nothing satisfies, so there is no whole answer for that object — but one
/// unreadable object must not end the read of any other, at either entry. (The typed
/// refusal itself is bound in `crates/core/tests/segmented_map_resolution.rs`; what this
/// case adds is the *scope* of it, at the gateway.)
#[tokio::test]
async fn an_unresolvable_object_fails_closed_without_ending_another_objects_read() {
    let (meta, chunks, _dir) = backends();
    let data: Vec<u8> = (0..32u32).map(|i| (i % 251) as u8).collect();

    let broken_plan = write_flat(&meta, &chunks, 1, "broken", &data, 0x300).await;
    let broken_group = SegmentGroup::new(NONCE, 1).unwrap();
    // Two segments named, one written.
    reshape_into_segments(&meta, 1, &broken_plan, &broken_group, 2, 1).await;

    let whole_plan = write_flat(&meta, &chunks, 2, "whole", &data, 0x400).await;
    let whole_group = SegmentGroup::new(OTHER_NONCE, 1).unwrap();
    reshape_into_segments(&meta, 2, &whole_plan, &whole_group, 2, 2).await;
    let gateway =
        Arc::new(Gateway::new(meta, chunks, MemCoordination::new()).with_chunk_size(CHUNK));

    let err = Arc::clone(&gateway)
        .get_object_streaming("broken")
        .await
        .err()
        .expect("a map that cannot be resolved is an error, never an empty body");
    assert!(
        !format!("{err}").contains("cannot yet resolve"),
        "must be the resolver's own refusal, not the base's blanket one: {err}"
    );
    assert!(
        Arc::clone(&gateway)
            .get_object_range("broken", ByteRange::FromTo(0, 7))
            .await
            .is_err(),
        "the ranged entry fails closed on the same object"
    );

    let whole = Arc::clone(&gateway)
        .get_object_streaming("whole")
        .await
        .unwrap()
        .expect("the well-formed object is still committed");
    assert_eq!(
        drain(whole.stream).await,
        data,
        "a second, well-formed object in the same store still reads"
    );
}
