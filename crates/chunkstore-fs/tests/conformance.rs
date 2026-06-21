//! Conformance tests for the filesystem `ChunkStore`.
//!
//! Round-trip and integrity assertions are written against the `ChunkStore`
//! trait surface (helpers over `&impl ChunkStore`) so they lift to a shared
//! suite when a second backend (S3) arrives. The corruption and id-guard tests
//! are filesystem-specific (they reach the bytes on disk). Filesystem I/O is
//! sync, so `pollster::block_on` drives the async methods deterministically.

use bytes::Bytes;
use pollster::block_on;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Build a valid v1 fragment carrying `payload`, whose header records `id`'s
/// chunk id and fragment index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

fn store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    (store, dir)
}

// ---- Round-trip (generic over any ChunkStore) ------------------------------

async fn round_trips(store: &impl ChunkStore, id: FragmentId, payload: &[u8]) {
    let frag = fragment(id, payload);
    store.put_fragment(id, frag.clone()).await.unwrap();
    let got = store.get_fragment(id).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(frag.as_ref()),
        "fragment must round-trip byte-identical"
    );
}

#[test]
fn put_then_get_is_byte_identical() {
    block_on(async {
        let (s, _dir) = store();
        round_trips(&s, fid(1, 0), b"").await;
        round_trips(
            &s,
            fid(0xdead_beef_cafe_babe_0000_0000_1234_5678, 0),
            b"a small payload",
        )
        .await;
        // A non-zero fragment index (an erasure-coding stripe position).
        round_trips(&s, fid(42, 3), b"a parity fragment").await;
    });
}

#[test]
fn fragments_of_one_chunk_are_addressed_independently_by_index() {
    block_on(async {
        let (s, _dir) = store();
        let chunk = 0x5151;
        s.put_fragment(fid(chunk, 0), fragment(fid(chunk, 0), b"index zero"))
            .await
            .unwrap();
        s.put_fragment(fid(chunk, 1), fragment(fid(chunk, 1), b"index one"))
            .await
            .unwrap();

        let zero = s.get_fragment(fid(chunk, 0)).await.unwrap().unwrap();
        let one = s.get_fragment(fid(chunk, 1)).await.unwrap().unwrap();
        assert_ne!(
            zero, one,
            "different indices of one chunk are distinct fragments"
        );
        // An index the chunk does not have reads as not-found.
        assert!(s.get_fragment(fid(chunk, 2)).await.unwrap().is_none());
    });
}

#[test]
fn get_unknown_id_is_none() {
    block_on(async {
        let (s, _dir) = store();
        assert!(s.get_fragment(fid(99, 0)).await.unwrap().is_none());
    });
}

#[test]
fn health_is_healthy_when_open() {
    block_on(async {
        let (s, _dir) = store();
        assert_eq!(s.health().await.unwrap(), Health::Healthy);
    });
}

// ---- Enumerate + delete (M3, proposal 0005) --------------------------------

#[test]
fn list_and_delete_walk_the_store() {
    block_on(async {
        let (s, _dir) = store();
        // An empty store walks to nothing.
        assert!(s.list_fragments().await.unwrap().is_empty());

        let ids = [fid(0x11, 0), fid(0x11, 4), fid(0x22, 0)];
        for &id in &ids {
            s.put_fragment(id, fragment(id, b"walked")).await.unwrap();
        }
        let listed: std::collections::HashSet<_> =
            s.list_fragments().await.unwrap().into_iter().collect();
        assert_eq!(
            listed,
            ids.into_iter().collect::<std::collections::HashSet<_>>(),
            "the directory walk recovers exactly the placed fragment ids"
        );

        // Delete one; it disappears from both get and the walk, siblings remain.
        s.delete_fragment(fid(0x11, 4)).await.unwrap();
        assert!(s.get_fragment(fid(0x11, 4)).await.unwrap().is_none());
        let listed: std::collections::HashSet<_> =
            s.list_fragments().await.unwrap().into_iter().collect();
        assert_eq!(
            listed,
            [fid(0x11, 0), fid(0x22, 0)]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        );

        // Deleting an absent fragment is an idempotent Ok(()).
        s.delete_fragment(fid(0x11, 4)).await.unwrap();
        s.delete_fragment(fid(0xdead, 9)).await.unwrap();
    });
}

#[test]
fn list_skips_foreign_and_temp_entries() {
    block_on(async {
        let (s, dir) = store();
        let id = fid(0x33, 0);
        s.put_fragment(id, fragment(id, b"real")).await.unwrap();

        // A leftover `.tmp` (an interrupted put) and a foreign directory/file
        // must not surface as phantom fragments — the walk parses names strictly.
        let chunk_dir = dir.path().join(format!("{:032x}", id.chunk));
        std::fs::write(chunk_dir.join("00001.tmp"), b"interrupted").unwrap();
        std::fs::write(chunk_dir.join("notes.txt"), b"foreign").unwrap();
        std::fs::create_dir_all(dir.path().join("not-a-chunk")).unwrap();

        assert_eq!(
            s.list_fragments().await.unwrap(),
            vec![id],
            "only the valid .frag under a 32-hex chunk dir is listed"
        );
    });
}

// ---- Integrity (filesystem-specific) ---------------------------------------

#[test]
fn corruption_is_detected_on_read() {
    block_on(async {
        let (s, dir) = store();
        let id = fid(7, 0);
        s.put_fragment(id, fragment(id, b"important"))
            .await
            .unwrap();

        // Flip a payload byte directly on disk, behind the store's back.
        let path = fragment_path(dir.path(), id);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1; // a payload-checksum byte
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            s.get_fragment(id).await.is_err(),
            "a corrupted fragment must not be returned"
        );
    });
}

#[test]
fn put_rejects_non_fragment_bytes() {
    block_on(async {
        let (s, _dir) = store();
        let err = s
            .put_fragment(fid(1, 0), Bytes::from_static(b"not a fragment"))
            .await;
        assert!(err.is_err(), "garbage must be rejected, not stored");
        assert!(s.get_fragment(fid(1, 0)).await.unwrap().is_none());
    });
}

#[test]
fn put_rejects_chunk_or_index_mismatch() {
    block_on(async {
        let (s, _dir) = store();
        // Header chunk id differs from the key's chunk.
        assert!(
            s.put_fragment(fid(0x2222, 0), fragment(fid(0x1111, 0), b"payload"))
                .await
                .is_err(),
            "a fragment must be filed under the chunk its header records"
        );
        // Header index differs from the key's index.
        assert!(
            s.put_fragment(fid(0x1111, 1), fragment(fid(0x1111, 0), b"payload"))
                .await
                .is_err(),
            "a fragment must be filed under the index its header records"
        );
    });
}
