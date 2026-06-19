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
use wyrd_traits::{ChunkId, ChunkStore, Health};

/// Build a valid v1 fragment carrying `payload` under `id`.
fn fragment(id: ChunkId, payload: &[u8]) -> Bytes {
    Bytes::from(encode(
        &FragmentHeader::new_v1(id, payload.len() as u64),
        payload,
    ))
}

fn store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    (store, dir)
}

// ---- Round-trip (generic over any ChunkStore) ------------------------------

async fn round_trips(store: &impl ChunkStore, id: ChunkId, payload: &[u8]) {
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
        round_trips(&s, 1, b"").await;
        round_trips(
            &s,
            0xdead_beef_cafe_babe_0000_0000_1234_5678,
            b"a small payload",
        )
        .await;
    });
}

#[test]
fn get_unknown_id_is_none() {
    block_on(async {
        let (s, _dir) = store();
        assert!(s.get_fragment(99).await.unwrap().is_none());
    });
}

#[test]
fn health_is_healthy_when_open() {
    block_on(async {
        let (s, _dir) = store();
        assert_eq!(s.health().await.unwrap(), Health::Healthy);
    });
}

// ---- Integrity (filesystem-specific) ---------------------------------------

#[test]
fn corruption_is_detected_on_read() {
    block_on(async {
        let (s, dir) = store();
        let id: ChunkId = 7;
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
            .put_fragment(1, Bytes::from_static(b"not a fragment"))
            .await;
        assert!(err.is_err(), "garbage must be rejected, not stored");
        // Nothing was written.
        assert!(s.get_fragment(1).await.unwrap().is_none());
    });
}

#[test]
fn put_rejects_id_mismatch() {
    block_on(async {
        let (s, _dir) = store();
        // A fragment whose header records id X, offered under id Y.
        let frag = fragment(0x1111, b"payload");
        assert!(
            s.put_fragment(0x2222, frag).await.is_err(),
            "a fragment must be filed under the id its header records"
        );
    });
}
