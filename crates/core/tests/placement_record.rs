//! M3.1 (issue #139): the placement record — recorded at the write commit point,
//! consumed by the read path in place of M2's stateless `index % n`.
//!
//! Proposal 0005 ("The placement record"): the committed chunk map records, per
//! fragment index, the **stable D-server id** holding that fragment, and the read
//! reconstructs the chunk by resolving each fragment **from that record**. These are
//! the backend-agnostic, in-process properties; the over-the-wire `rs(6,3)`-over-tonic
//! and the DST seed sweep are supplementary (`cargo xtask ci`), not the regression
//! home. Two properties, both surviving a metadata-store **reopen** (the in-process
//! process-restart equivalent):
//!
//! 1. the write path records a length-`n` placement vector at the commit point, and a
//!    read after reopen reconstructs the object by resolving each fragment from it;
//! 2. the BINDING regression — a fragment placed at a D server that `index % n` would
//!    **not** select is still read correctly, because the read consumes the record.
//!    (Red against today's `index % n` read path — it has neither the record nor the
//!    placement-aware resolution; green once the read consumes the record.)

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{
    ChunkStore, CommitOutcome, DServerId, FragmentId, Health, PlacementChunkStore, Result,
};

const CHUNK: usize = 1 << 16; // one chunk per payload
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };
const N: u16 = 9; // k + m fragments per chunk
const ROOT: u64 = 0;

fn rs_plan(payload: &[u8]) -> write::WritePlan {
    let mut next = 0x139u128;
    write::plan_write(payload, CHUNK, RS, || {
        next += 1;
        next
    })
    .unwrap()
}

/// A fleet of `n` independent in-process D servers, addressed by stable id. Unlike a
/// single store or the `index % n` fan-out, a fragment physically lives on exactly one
/// D server, so a read that does **not** consult the placement record looks at the
/// wrong server and finds nothing — which is precisely the M2 gap M3.1 closes.
struct Fleet {
    servers: Vec<Mutex<HashMap<FragmentId, Bytes>>>,
}

impl Fleet {
    fn new(n: usize) -> Self {
        Self {
            servers: (0..n).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn server(&self, dserver: DServerId) -> &Mutex<HashMap<FragmentId, Bytes>> {
        &self.servers[dserver as usize]
    }

    fn index_route(&self, index: u16) -> DServerId {
        u64::from(index) % self.servers.len() as u64
    }
}

#[async_trait]
impl ChunkStore for Fleet {
    // Supertrait obligation. The placement-aware read never calls these (it uses
    // `*_at`); a stateless `index % n` caller would route here — and find nothing for a
    // moved fragment, the gap the placement record exists to close.
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.server(self.index_route(id.index))
            .lock()
            .unwrap()
            .insert(id, fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self
            .server(self.index_route(id.index))
            .lock()
            .unwrap()
            .get(&id)
            .cloned())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

#[async_trait]
impl PlacementChunkStore for Fleet {
    async fn get_fragment_at(&self, dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.server(dserver).lock().unwrap().get(&id).cloned())
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
    ) -> Result<()> {
        self.server(dserver).lock().unwrap().insert(id, fragment);
        Ok(())
    }
}

/// Property 1: the four-phase write records a length-`n` placement vector at the commit
/// point, and a read after the metadata store is **reopened** reconstructs the object
/// by resolving each fragment from that record.
#[test]
fn write_records_placement_read_resolves_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("meta.redb");
        let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
        let payload = b"rs(6,3) records a placement vector at the commit point; ".repeat(8);

        // Write end to end (intent -> data -> commit -> release). The commit records
        // the placement into the chunk map.
        {
            let meta = RedbMetadataStore::open(&db_path).unwrap();
            let mut next = 0x42u128;
            let outcome = write::write_new_object(
                &meta,
                &chunks,
                ROOT,
                "obj",
                1,
                &payload,
                CHUNK,
                RS,
                0,
                10_000,
                || {
                    next += 1;
                    next
                },
            )
            .await
            .unwrap();
            assert_eq!(outcome, CommitOutcome::Committed);
        } // drop the store: the process-restart equivalent

        // Reopen the persisted metadata store.
        let meta = RedbMetadataStore::open(&db_path).unwrap();
        let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();

        assert_eq!(inode.chunk_map.len(), 1, "single-chunk object");
        assert_eq!(
            inode.chunk_map[0].placement.len(),
            N as usize,
            "commit records one stable D-server id per fragment index"
        );

        // The read resolves every fragment from the record and reconstructs.
        let got = read::read_object(&meta, &chunks, 1).await.unwrap().unwrap();
        assert_eq!(got, payload, "object reassembled from the placement record");
    });
}

/// Property 2 (BINDING): every fragment is placed at a D server that `index % n` would
/// not select (a custodian-move world), committed into the chunk map, and — after the
/// metadata store is reopened — the read still reconstructs the chunk because it
/// resolves each fragment from the record rather than from `index % n`.
#[test]
fn moved_fragment_resolved_from_record_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("meta.redb");

        let payload = b"a fragment moved off index % n is still found via the record; ".repeat(8);
        let plan = rs_plan(&payload);
        let chunk = &plan.chunks[0];
        let chunk_id = chunk.id;
        assert_eq!(chunk.fragments.len(), N as usize, "rs(6,3) -> 9 fragments");

        let fleet = Fleet::new(N as usize);

        // Place fragment `i` on D server `(i + SHIFT) % N`: a rotation, so EVERY
        // fragment lives at a server that `index % n` (== i) would not select.
        const SHIFT: u16 = 4;
        let placement: Vec<DServerId> = (0..N).map(|i| u64::from((i + SHIFT) % N)).collect();
        for (index, bytes) in &chunk.fragments {
            let dserver = placement[*index as usize];
            fleet
                .put_fragment_at(
                    dserver,
                    FragmentId {
                        chunk: chunk_id,
                        index: *index,
                    },
                    bytes.clone(),
                )
                .await
                .unwrap();
        }

        // Commit an inode whose chunk map carries that placement (the commit point).
        let record = InodeRecord {
            size: plan.size,
            chunk_map: vec![ChunkRef {
                id: chunk_id,
                scheme: RS,
                len: chunk.len,
                placement: placement.clone(),
            }],
            state: InodeState::Committed,
            version: 1,
        };
        {
            let meta = RedbMetadataStore::open(&db_path).unwrap();
            assert_eq!(
                metadata::create(&meta, ROOT, "obj", 1, &record)
                    .await
                    .unwrap(),
                CommitOutcome::Committed,
            );
        } // drop the store: the process-restart equivalent

        // Reopen: the placement record survives, and the read consumes it.
        let meta = RedbMetadataStore::open(&db_path).unwrap();
        let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();
        assert_eq!(
            inode.chunk_map[0].placement, placement,
            "placement record survived the metadata-store reopen"
        );
        // Guard: every recorded location genuinely diverges from `index % n`, so a green
        // read can only come from resolving the record (not from `index % n`).
        assert!(
            (0..N).all(|i| inode.chunk_map[0].placement[i as usize] != u64::from(i)),
            "every fragment is moved off its index % n home"
        );

        let got = read::read_object_from(&fleet, &inode).await.unwrap();
        assert_eq!(
            got, payload,
            "moved-fragment chunk reconstructed by resolving each fragment from the record"
        );
    });
}
