//! Integration tests for the client read path (architecture §6.2), wired against
//! the real redb + filesystem backends `server` composes and driven by
//! `pollster::block_on` (sync backends never yield).

use pollster::block_on;
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_core::metadata::{self, InodeRecord};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{CommitOutcome, FragmentId};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 4;

fn ids_from(base: u128) -> impl FnMut() -> u128 {
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

#[test]
fn committed_file_reads_back_byte_identical() {
    block_on(async {
        for (name, data, base) in [
            ("empty", &b""[..], 0x10u128),
            ("small", &b"abc"[..], 0x20),
            ("multi", &b"spans several four-byte chunks!"[..], 0x30),
        ] {
            let (meta, chunks, _dir) = backends();
            let outcome = write::write_new_object(
                &meta,
                &chunks,
                ROOT,
                name,
                1,
                data,
                CHUNK,
                NOW,
                TTL,
                ids_from(base),
            )
            .await
            .unwrap();
            assert_eq!(outcome, CommitOutcome::Committed, "{name}");

            let got = read::read_path(&meta, &chunks, ROOT, name).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(data),
                "{name}: read back byte-identical"
            );
        }
    });
}

#[test]
fn reader_sees_old_or_new_version_never_a_hybrid() {
    block_on(async {
        let (meta, chunks, _dir) = backends();

        // v1.
        write::write_new_object(
            &meta,
            &chunks,
            ROOT,
            "obj",
            1,
            b"version one is here",
            CHUNK,
            NOW,
            TTL,
            ids_from(0x100),
        )
        .await
        .unwrap();
        let v1 = read::read_inode(&meta, 1).await.unwrap().unwrap();
        assert_eq!(
            read::read_path(&meta, &chunks, ROOT, "obj")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"version one is here"[..])
        );

        // Overwrite to v2 (new chunk ids; v1's fragments stay in the store).
        let plan2 = write::plan_write(b"VERSION 2!", CHUNK, ids_from(0x500));
        write::intent(&meta, &plan2, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan2).await.unwrap();
        assert_eq!(
            write::commit_overwrite(&meta, 1, &v1, &plan2)
                .await
                .unwrap(),
            CommitOutcome::Committed
        );

        // The current read resolves the new version, whole.
        assert_eq!(
            read::read_path(&meta, &chunks, ROOT, "obj")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"VERSION 2!"[..])
        );
        // A reader holding the v1 snapshot still reassembles all of v1 — never a mix.
        assert_eq!(
            read::read_object_from(&chunks, &v1).await.unwrap(),
            b"version one is here"
        );
    });
}

#[test]
fn checksum_mismatch_surfaces_as_an_error() {
    block_on(async {
        let (meta, chunks, dir) = backends();
        write::write_new_object(
            &meta,
            &chunks,
            ROOT,
            "f",
            1,
            b"important data, do not corrupt",
            CHUNK,
            NOW,
            TTL,
            ids_from(0x40),
        )
        .await
        .unwrap();

        // Corrupt the first fragment on disk, behind the store's back.
        let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();
        let path = fragment_path(
            dir.path(),
            FragmentId {
                chunk: inode.chunk_map[0],
                index: 0,
            },
        );
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            read::read_path(&meta, &chunks, ROOT, "f").await.is_err(),
            "a checksum mismatch must surface as an error, not corrupt bytes"
        );
    });
}

#[test]
fn unbound_name_and_uncommitted_inode_read_as_none() {
    block_on(async {
        let (meta, chunks, _dir) = backends();

        // An unbound name.
        assert!(read::read_path(&meta, &chunks, ROOT, "missing")
            .await
            .unwrap()
            .is_none());

        // A bound name whose inode is still Pending (not yet committed).
        metadata::create(&meta, ROOT, "pending", 7, &InodeRecord::new_empty())
            .await
            .unwrap();
        assert!(read::read_path(&meta, &chunks, ROOT, "pending")
            .await
            .unwrap()
            .is_none());
    });
}
