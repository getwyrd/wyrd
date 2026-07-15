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

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        // Supertrait obligation: the union across the fleet. A fragment lives on
        // exactly one server (placed via `*_at`), so the keys are disjoint.
        Ok(self
            .servers
            .iter()
            .flat_map(|s| s.lock().unwrap().keys().copied().collect::<Vec<_>>())
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        // Supertrait obligation: remove wherever it physically lives.
        for s in &self.servers {
            s.lock().unwrap().remove(&id);
        }
        Ok(())
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
                || 0,
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

// ---- issue #349 (mixed-era placement matrix): Read — empty/short placement, both
//      schemes, identity-placement fallback (`ChunkRef::placed_dserver`,
//      `metadata.rs:119-124`). Property 1+2 above already pin FULL rs(6,3) placement;
//      these three close the empty/short gaps the #292 audit found unguarded. Each
//      commits a record in the genuine pre-M3 / mixed-era shape directly (no live
//      writer ever emits a short or empty vector, `write.rs:171`) and proves the read
//      path resolves every fragment from `placed_dserver`, not a raw vector index.
//
// Flippable (recorded in build-notes): revert `read.rs:103-105` (`fragment_dserver`)
// from `chunk.placed_dserver(index)` to raw `chunk.placement[index as usize]` — an
// empty/short vector then panics (out of bounds) on these three cases instead of
// resolving via the fallback, while Property 1/2's FULL-length cases above are
// unaffected (their vectors are already length `n`).

/// Gap: `EcScheme::None` + `placement: vec![]` (pre-M3). The single fragment at index 0
/// must resolve via identity fallback (D-server 0) and read byte-identical.
#[test]
fn empty_placement_ec_none_resolves_via_identity_fallback() {
    block_on(async {
        let payload = b"a pre-M3 EcScheme::None chunk: empty placement, identity fallback".to_vec();
        let mut next = 0x34900u128;
        let plan = write::plan_write(&payload, CHUNK, EcScheme::None, || {
            next += 1;
            next
        })
        .unwrap();
        assert_eq!(
            plan.chunks.len(),
            1,
            "single chunk for this small an object"
        );
        let chunk = &plan.chunks[0];
        assert_eq!(chunk.fragments.len(), 1, "EcScheme::None -> one fragment");

        // Place the one fragment at its IDENTITY D server (0) — exactly where the
        // empty-placement fallback (`placed_dserver(0) == 0`) resolves it.
        let fleet = Fleet::new(1);
        let (index, bytes) = &chunk.fragments[0];
        fleet
            .put_fragment_at(
                u64::from(*index),
                FragmentId {
                    chunk: chunk.id,
                    index: *index,
                },
                bytes.clone(),
            )
            .await
            .unwrap();

        let record = InodeRecord {
            size: plan.size,
            chunk_map: vec![ChunkRef {
                id: chunk.id,
                scheme: EcScheme::None,
                len: chunk.len,
                placement: Vec::new(), // pre-M3: the genuine empty shape
            }],
            state: InodeState::Committed,
            version: 1,
        };
        assert!(
            record.chunk_map[0].placement.is_empty(),
            "pre-M3 record: an empty, not full-length, placement vector"
        );

        let got = read::read_object_from(&fleet, &record).await.unwrap();
        assert_eq!(
            got, payload,
            "EcScheme::None chunk with empty placement reads via identity fallback"
        );
    });
}

/// Gap + the brief's required RS{6,3} cell: rs(6,3) + `placement: vec![]` (pre-M3).
/// All 9 fragments resolve via identity fallback (index `i` -> D-server `i`).
#[test]
fn empty_placement_rs_6_3_resolves_via_identity_fallback() {
    block_on(async {
        let payload =
            b"a pre-M3 rs(6,3) chunk: empty placement, identity-resolved fragments; ".repeat(4);
        let plan = rs_plan(&payload);
        let chunk = &plan.chunks[0];
        assert_eq!(chunk.fragments.len(), N as usize, "rs(6,3) -> 9 fragments");

        let fleet = Fleet::new(N as usize);
        for (index, bytes) in &chunk.fragments {
            fleet
                .put_fragment_at(
                    u64::from(*index), // identity fallback target
                    FragmentId {
                        chunk: chunk.id,
                        index: *index,
                    },
                    bytes.clone(),
                )
                .await
                .unwrap();
        }

        let record = InodeRecord {
            size: plan.size,
            chunk_map: vec![ChunkRef {
                id: chunk.id,
                scheme: RS,
                len: chunk.len,
                placement: Vec::new(), // pre-M3: the genuine empty shape
            }],
            state: InodeState::Committed,
            version: 1,
        };
        assert!(
            record.chunk_map[0].placement.is_empty(),
            "pre-M3 record: an empty, not full-length, placement vector"
        );

        let got = read::read_object_from(&fleet, &record).await.unwrap();
        assert_eq!(
            got, payload,
            "rs(6,3) chunk with empty placement reads via identity fallback"
        );
    });
}

/// Gap: a SHORT rs(6,3) placement vector — the first 3 indices explicit (moved off
/// their identity D server, onto servers OUTSIDE the identity range so a wrong
/// resolution finds nothing there), the remaining 6 absent (identity fallback).
/// Proves the mixed explicit/fallback `.get(i).unwrap_or(i)` resolution
/// (`ChunkRef::placed_dserver`, `metadata.rs:119-124`) over a real rs(6,3) chunk.
#[test]
fn short_placement_rs_6_3_mixed_explicit_and_fallback() {
    block_on(async {
        let payload =
            b"a short rs(6,3) placement vector: 3 explicit, 6 identity-fallback fragments; "
                .repeat(4);
        let plan = rs_plan(&payload);
        let chunk = &plan.chunks[0];
        assert_eq!(chunk.fragments.len(), N as usize, "rs(6,3) -> 9 fragments");

        // Explicit indices 0,1,2 move to servers N..N+3 — strictly outside the
        // identity range, so a wrong (raw / unresolved) read finds nothing there.
        // Indices 3..9 stay unrecorded and resolve via identity fallback.
        let explicit: Vec<DServerId> = (0..3).map(|i| u64::from(N) + i).collect();
        let fleet = Fleet::new((N + 3) as usize);
        for (index, bytes) in &chunk.fragments {
            let dserver = if (*index as usize) < explicit.len() {
                explicit[*index as usize]
            } else {
                u64::from(*index) // identity fallback target
            };
            fleet
                .put_fragment_at(
                    dserver,
                    FragmentId {
                        chunk: chunk.id,
                        index: *index,
                    },
                    bytes.clone(),
                )
                .await
                .unwrap();
        }

        let record = InodeRecord {
            size: plan.size,
            chunk_map: vec![ChunkRef {
                id: chunk.id,
                scheme: RS,
                len: chunk.len,
                placement: explicit.clone(), // short: length 3, not 9
            }],
            state: InodeState::Committed,
            version: 1,
        };
        assert_eq!(
            record.chunk_map[0].placement.len(),
            3,
            "a genuinely SHORT vector: 3 explicit entries, not the full 9"
        );
        // Guard: every explicit entry diverges from identity, so a green read can only
        // come from honouring the recorded override (not silently falling back to i).
        assert!(
            (0..3).all(|i| record.chunk_map[0].placement[i] != i as u64),
            "every explicit index is moved off its identity D server"
        );

        let got = read::read_object_from(&fleet, &record).await.unwrap();
        assert_eq!(
            got, payload,
            "short-placement rs(6,3) chunk reads via mixed explicit + identity fallback"
        );
    });
}

/// `ChunkRef::fragments()` (issue #347, ADR-0040 decision 2): the one placement-
/// expansion helper GC, reconstruction, and rebalance must now route through instead
/// of open-coding `(0..fragment_count()).map(|i| placed_dserver(i))` themselves. This
/// matrix asserts `fragments()` yields *exactly* the per-index `placed_dserver`
/// resolution — never more, never fewer, never a different value — across
/// `EcScheme::None` and `ReedSolomon { k, m }`, for empty, full (`len ==
/// fragment_count()`), and malformed (`len != fragment_count()`, non-empty) placement
/// vectors. `fragments()` is deliberately liberal (ADR-0040 decision 2): it applies
/// the identity fallback unconditionally and never validates `placement`'s length —
/// that stays a synchronous, no-code, purely-mechanical proxy for the read path.
mod fragments_matrix {
    use wyrd_core::metadata::{ChunkRef, EcScheme};
    use wyrd_traits::{ChunkId, DServerId};

    const CHUNK_ID: ChunkId = 0x347;
    const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 }; // fragment_count() == 9

    fn chunk(scheme: EcScheme, placement: Vec<DServerId>) -> ChunkRef {
        ChunkRef {
            id: CHUNK_ID,
            scheme,
            len: 4096,
            placement,
        }
    }

    /// The ADR-0040 decision-2 contract, checked structurally: `fragments()` covers
    /// exactly `0..fragment_count()` and each entry is `(i, chunk.placed_dserver(i))`
    /// — i.e. `fragments()` cannot drift from `placed_dserver`, by construction.
    fn assert_matches_placed_dserver(chunk: &ChunkRef) {
        let want: Vec<(u16, DServerId)> = (0..chunk.fragment_count())
            .map(|i| (i, chunk.placed_dserver(i)))
            .collect();
        let got: Vec<(u16, DServerId)> = chunk.fragments().collect();
        assert_eq!(
            got, want,
            "fragments() must equal the per-index placed_dserver walk"
        );
        assert_eq!(
            got.len(),
            chunk.fragment_count() as usize,
            "fragments() must cover the full 0..fragment_count() index space"
        );
    }

    #[test]
    fn none_empty_placement_is_pure_identity() {
        let c = chunk(EcScheme::None, vec![]);
        assert_matches_placed_dserver(&c);
        assert_eq!(c.fragments().collect::<Vec<_>>(), vec![(0, 0)]);
    }

    #[test]
    fn none_full_placement_resolves_from_record() {
        let c = chunk(EcScheme::None, vec![7]);
        assert_matches_placed_dserver(&c);
        assert_eq!(c.fragments().collect::<Vec<_>>(), vec![(0, 7)]);
    }

    #[test]
    fn none_malformed_length_placement_still_resolves_liberally() {
        // `EcScheme::None` has `fragment_count() == 1`, so the only non-empty
        // "wrong length" (ADR-0040 decision 3: malformed) shape reachable is a
        // vector LONGER than the index space — a shorter-but-nonempty vector is
        // impossible below length 1. `fragments()` does not validate length
        // (decision 2): it walks only `0..fragment_count()` and ignores the extra
        // trailing entry, exactly as `placed_dserver` would for the same index.
        let c = chunk(EcScheme::None, vec![7, 8]);
        assert_matches_placed_dserver(&c);
        assert_eq!(c.fragments().collect::<Vec<_>>(), vec![(0, 7)]);
    }

    #[test]
    fn rs_empty_placement_is_pure_identity() {
        let c = chunk(RS, vec![]);
        assert_matches_placed_dserver(&c);
        let want: Vec<(u16, DServerId)> = (0..9u16).map(|i| (i, u64::from(i))).collect();
        assert_eq!(c.fragments().collect::<Vec<_>>(), want);
    }

    #[test]
    fn rs_full_placement_resolves_from_record() {
        let placement: Vec<DServerId> = (0..9u16).map(|i| 100 + u64::from(i)).collect();
        let c = chunk(RS, placement.clone());
        assert_matches_placed_dserver(&c);
        let want: Vec<(u16, DServerId)> = (0..9u16).map(|i| (i, placement[i as usize])).collect();
        assert_eq!(c.fragments().collect::<Vec<_>>(), want);
    }

    #[test]
    fn rs_short_placement_mixes_record_and_identity_fallback() {
        // A short (non-empty, `len < fragment_count()`) vector: malformed per
        // ADR-0040 decision 3. `fragments()` itself does not classify or reject it
        // (that is the strict-maintenance companion's job, #348) — indices within
        // `placement` resolve from the record, indices beyond it fall back to
        // identity (decision 1), exactly like `placed_dserver`.
        let placement: Vec<DServerId> = vec![50, 51, 52, 53]; // len 4 < fragment_count 9
        let c = chunk(RS, placement.clone());
        assert_matches_placed_dserver(&c);
        let want: Vec<(u16, DServerId)> = (0..9u16)
            .map(|i| {
                (
                    i,
                    placement.get(i as usize).copied().unwrap_or(u64::from(i)),
                )
            })
            .collect();
        assert_eq!(c.fragments().collect::<Vec<_>>(), want);
    }
}
