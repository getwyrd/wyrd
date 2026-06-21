//! M2.4 (issue #114): `write_fragments` is a concurrent, fail-closed fan-out.
//!
//! These are the backend-agnostic properties, proven against an in-process
//! fault-injecting `ChunkStore` so they are deterministic (the over-the-wire,
//! multi-D-server fan-out is proven in `wyrd-server`'s integration test). The
//! point M2.4 retires: a partial fan-out must abort the data phase — never a
//! half-written set silently treated as complete.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_core::metadata::EcScheme;
use wyrd_core::write;
use wyrd_traits::{ChunkStore, FragmentId, Health, PlacementChunkStore, Result};

const CHUNK: usize = 1 << 16; // one chunk per test payload
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// A `ChunkStore` that records what it stored and can be told to fail the put for
/// one fragment index — the injected partial fan-out.
struct FaultStore {
    stored: Mutex<HashSet<FragmentId>>,
    fail_index: Option<u16>,
}

impl FaultStore {
    fn new(fail_index: Option<u16>) -> Self {
        Self {
            stored: Mutex::new(HashSet::new()),
            fail_index,
        }
    }

    fn stored(&self) -> HashSet<FragmentId> {
        self.stored.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChunkStore for FaultStore {
    async fn put_fragment(&self, id: FragmentId, _fragment: Bytes) -> Result<()> {
        if self.fail_index == Some(id.index) {
            return Err(format!("injected put failure at fragment {}", id.index).into());
        }
        self.stored.lock().unwrap().insert(id);
        Ok(())
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.stored.lock().unwrap().iter().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.stored.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

// A single-location store: its `*_at` defaults delegate to the index-routed
// `put_fragment` / `get_fragment`, so the identity-placement fan-out routes exactly
// as in M2 (the write path now addresses fragments via `put_fragment_at`).
impl PlacementChunkStore for FaultStore {}

fn rs_plan(payload: &[u8]) -> write::WritePlan {
    let mut next = 0x42u128;
    write::plan_write(payload, CHUNK, RS, || {
        next += 1;
        next
    })
    .unwrap()
}

#[test]
fn full_fan_out_acks_every_fragment() {
    block_on(async {
        let plan = rs_plan(b"a chunk that codes into nine fragments");
        let chunk_id = plan.chunks[0].id;
        let store = FaultStore::new(None);

        write::write_fragments(&store, &plan).await.unwrap();

        // All n = k + m = 9 fragments were stored, addressed by index.
        let stored = store.stored();
        assert_eq!(stored.len(), 9, "every fragment acked");
        for index in 0..9u16 {
            assert!(
                stored.contains(&FragmentId {
                    chunk: chunk_id,
                    index
                }),
                "fragment {index} stored"
            );
        }
    });
}

#[test]
fn partial_fan_out_fails_closed() {
    block_on(async {
        let plan = rs_plan(b"a chunk whose fan-out loses one fragment");
        let chunk_id = plan.chunks[0].id;
        // One D server rejects its fragment (a drop / timeout / integrity failure).
        let store = FaultStore::new(Some(4));

        let result = write::write_fragments(&store, &plan).await;

        // The whole data phase fails, so the four-phase protocol aborts *before*
        // the commit — there is no half-committed chunk.
        assert!(result.is_err(), "a partial fan-out must fail closed");

        // The fragments that did land are leased garbage (the pending-ledger sweep
        // reclaims them); the failing index is, of course, not among them.
        let stored = store.stored();
        assert!(
            !stored.contains(&FragmentId {
                chunk: chunk_id,
                index: 4
            }),
            "the failed fragment was not stored"
        );
        assert!(
            !stored.is_empty() && stored.len() < 9,
            "a partial set landed as leased garbage, not the full chunk"
        );
    });
}
