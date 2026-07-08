//! Issue #477: **two active-active gateways over one shared fleet** must mint globally
//! unique object ids. The M4 blueprint (#465) is "one shared front door, N gateways, no LB
//! affinity, plain round-robin", so two `Gateway` processes can serve the SAME
//! `MetadataStore` + `ChunkStore` **concurrently**. Both recover from the same baseline, so
//! any id minted from *per-process* state collides across the two processes:
//!
//! * Inodes: two per-process counters seeded from the same high-water mark both mint inode 1,
//!   so the second gateway's `commit_create` fails `require_absent(inode:1)` with a bogus
//!   `Conflict` and its new-key PUT is spuriously rejected.
//! * Chunk ids: two per-process chunk counters both mint chunk id 1, so the second gateway's
//!   fragment write clobbers the first object's fragments on the shared chunk store under the
//!   colliding id — silent corruption of a committed object.
//!
//! This is the two-active-gateways analogue of the two-sequential-CLI-invocations test at
//! `gateway_cluster.rs`. The fix routes inodes through the shared store's `meta:next_inode`
//! CAS allocator (`cli::alloc_inode`, exactly as the CLI cluster path does) and mints chunk
//! ids coordination-free (a per-gateway random epoch, ADR-0019), so neither collides.
//!
//! **Genuinely concurrent (iteration-2 carry-forward).** The two gateways' PUTs are driven
//! under `tokio::spawn` on a multi-thread runtime and released together from a `Barrier`, so
//! they *race* on the shared `meta:next_inode` CAS allocator — the contended path the fix
//! rests on. A sequential "A then B" would let the allocator hand out 1 then 2 uncontended and
//! never exercise the CAS retry, so this test drives several writers per gateway all at once.
//! A companion test contends `cli::alloc_inode` directly and asserts it never hands out a
//! duplicate id under that contention.
//!
//! It drives the **production** `Gateway::{recover, put_object, get_object}` (and the
//! production `cli::alloc_inode`) over a shared, **read-back-observable** in-memory metadata +
//! chunk store (the `Arc<Mutex<…>>`-backed `MemMeta`/`MemChunks` the other gateway tests use).
//! An in-memory store — not redb — is used precisely because the two gateways are alive
//! **concurrently**: redb takes an exclusive file lock, so it cannot model two live processes
//! sharing one store; the shared `Arc` backing does.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_server::cli::alloc_inode;
use wyrd_server::Gateway;
use wyrd_traits::{
    ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore, Result,
    WriteBatch,
};

/// An in-memory `MetadataStore` whose state is shared by every clone (the `Arc<Mutex<…>>`
/// backing). Two `Gateway` instances built over clones of one `MemMeta` therefore read and
/// write the SAME metadata — the coordination point two active-active gateways collide on.
#[derive(Clone, Default)]
struct MemMeta {
    kv: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// An in-memory `ChunkStore` whose fragments are shared by every clone — **read-back
/// observable**: a fragment one gateway writes is visible to the other, and a colliding
/// `FragmentId` overwrites, so a chunk-id collision is a real, observable clobber.
#[derive(Clone, Default)]
struct MemChunks {
    frags: Arc<Mutex<HashMap<FragmentId, Bytes>>>,
}

#[async_trait]
impl ChunkStore for MemChunks {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
        Ok(())
    }
    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }
    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }
    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }
    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

impl PlacementChunkStore for MemChunks {}

/// A distinct, deterministic payload per object, large enough to erasure-code into several
/// fragments under the default RS(6,3) scheme. `seed` makes each object's bytes differ, so a
/// clobber (one gateway's fragments overwriting another's under a colliding chunk id) is
/// detectable as a byte mismatch on read-back.
fn payload(seed: u8) -> Vec<u8> {
    (0..4096u32)
        .map(|i| (i as u8).wrapping_mul(37).wrapping_add(seed))
        .collect()
}

/// Parse the inode id out of an `inode:<id>` metadata key (mirrors `metadata::inode_key`'s
/// `inode:{id}` format; the parser there is crate-private, so the test reads the key shape
/// directly to count the DISTINCT inodes the shared allocator handed out).
fn inode_id_of(key: &[u8]) -> Option<u64> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// Two `Gateway` instances over ONE shared metadata store and ONE shared, read-back-observable
/// chunk store — the active-active case — both recovered from the same empty baseline. Several
/// distinct objects are stored **concurrently**, split across the two gateways and released
/// together from a barrier so their PUTs genuinely race on the shared `meta:next_inode` CAS
/// allocator. Every object must read back byte-identical and every object must get its own
/// inode.
///
/// Pre-fix (per-process `AtomicU64` counters): each gateway seeds its counter at 1
/// independently, so gateway B re-mints inodes 1..N that gateway A already committed — each
/// such create loses `require_absent(inode:k)` with a bogus `Conflict` and its PUT is rejected
/// — and re-mints chunk ids 1..N that clobber A's fragments on the shared store. Either way at
/// least one object no longer round-trips → RED. Post-fix, the shared-CAS inode allocator and
/// coordination-free chunk ids keep every id disjoint even under the concurrent contention →
/// every object round-trips → GREEN.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_active_gateways_concurrently_store_distinct_objects_without_collision() {
    // One shared metadata store and one shared chunk store, cloned into each gateway — a single
    // fleet, two front doors (the M4 shape).
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    let gw_a = Arc::new(Gateway::new(
        meta.clone(),
        chunks.clone(),
        MemCoordination::new(),
    ));
    let gw_b = Arc::new(Gateway::new(
        meta.clone(),
        chunks.clone(),
        MemCoordination::new(),
    ));

    // Both recover from the SAME (empty) baseline, so any per-process counter would seed
    // identically — exactly the condition that makes uncoordinated ids collide.
    gw_a.recover().await.expect("gateway A recovers");
    gw_b.recover().await.expect("gateway B recovers");

    // A handful of distinct objects, evenly split across the two gateways, each stored on its
    // own task and released together — so the PUTs run at the same time and the id allocations
    // genuinely contend (not "A fully completes, then B").
    const WRITERS: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));

    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        // Even writers go through gateway A, odd through gateway B — so BOTH gateways are
        // minting ids concurrently over the one shared store.
        let gw = if i % 2 == 0 {
            Arc::clone(&gw_a)
        } else {
            Arc::clone(&gw_b)
        };
        let barrier = Arc::clone(&barrier);
        let key = format!("obj-{i}");
        let body = payload(i as u8);
        handles.push(tokio::spawn(async move {
            // Align the start so the allocations actually race on the shared CAS allocator.
            barrier.wait().await;
            gw.put_object(&key, &body).await.map(|()| (key, body))
        }));
    }

    // Every concurrent PUT must commit. Pre-fix the two gateways re-mint each other's inodes,
    // so at least one create loses `require_absent(inode:k)` and its PUT errors here.
    let mut stored = Vec::with_capacity(WRITERS);
    for h in handles {
        let (key, body) = h.await.expect("writer task must not panic").expect(
            "every concurrent PUT over the shared fleet must commit — a bogus `Conflict` here \
             is the second gateway re-minting an inode the first already committed (issue #477)",
        );
        stored.push((key, body));
    }

    // Every object round-trips byte-identical through BOTH gateways (one shared fleet). Reading
    // through the OTHER gateway also proves the stores are genuinely shared. A chunk-id
    // collision would have clobbered some object's fragments — caught here as a byte mismatch.
    for (key, body) in &stored {
        assert_eq!(
            gw_a.get_object(key).await.expect("GET via A").as_deref(),
            Some(&body[..]),
            "object `{key}` must round-trip byte-identical through gateway A — no chunk-id \
             collision clobbered its fragments",
        );
        assert_eq!(
            gw_b.get_object(key).await.expect("GET via B").as_deref(),
            Some(&body[..]),
            "object `{key}` must round-trip byte-identical through gateway B — the stores are \
             shared and its id is disjoint from every other writer's",
        );
    }

    // The shared allocator handed out WRITERS **distinct** inodes under the concurrent
    // contention: exactly one inode record per object, all different. Two objects sharing an
    // inode would have failed a create above; this is the positive evidence that they did not.
    let inode_keys = meta.scan(b"inode:").await.expect("scan inode records");
    let inodes: HashSet<u64> = inode_keys
        .iter()
        .filter_map(|(k, _)| inode_id_of(k))
        .collect();
    assert_eq!(
        inodes.len(),
        WRITERS,
        "each of the {WRITERS} concurrent objects must have its own inode — the shared \
         `meta:next_inode` CAS allocator handed out {WRITERS} distinct ids under contention",
    );
}

/// The mechanism the fix rests on, exercised directly and under genuine contention: many tasks
/// call the production `cli::alloc_inode` against ONE shared store at once (released together
/// from a barrier), and the allocator must hand each a **distinct** id — never two the same.
/// This is the CAS-retry path the active-active gateway relies on; it is what makes two
/// gateways minting inodes concurrently over the shared `meta:next_inode` counter safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_inode_allocator_hands_out_distinct_ids_under_contention() {
    const ALLOCATORS: usize = 16;
    let meta = MemMeta::default();
    let barrier = Arc::new(tokio::sync::Barrier::new(ALLOCATORS));

    let mut handles = Vec::with_capacity(ALLOCATORS);
    for _ in 0..ALLOCATORS {
        let meta = meta.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            alloc_inode(&meta)
                .await
                .expect("alloc_inode under contention")
        }));
    }

    let mut ids = Vec::with_capacity(ALLOCATORS);
    for h in handles {
        ids.push(h.await.expect("allocator task must not panic"));
    }

    let distinct: HashSet<u64> = ids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        ALLOCATORS,
        "the shared CAS allocator must never hand two concurrent callers the same inode id \
         (got {ids:?}) — the contended path two active-active gateways rely on",
    );
}
