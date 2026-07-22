//! Tier-1 DST: seed-reproducible property tests for the **any-*k*-arrive-first**
//! erasure-coded read (M2.5, issue #115), driven by `wyrd_testkit::Sim` in the
//! same style as `dst_erasure.rs`. Where `dst_erasure.rs` proves the read
//! reconstructs under randomized fragment *loss/corruption*, this proves the
//! parallel read **fans out all `n` fragment fetches at once** and reconstructs
//! from whichever `k` complete first — independent of arrival/index order —
//! cancelling the rest (proposal 0004, "Read — any-*k*-arrive-first", lines
//! 259-265 / PR step 5, lines 442-444; architecture §6.2 read path, §6.6).
//!
//! Determinism under simulation (ADR-0009): the run is a pure function of the
//! seed. Each run draws a seeded **arrival permutation** and feeds it to
//! [`ArrivalStore`], a `ChunkStore` whose `get_fragment` future yields `Pending`
//! a seed-assigned number of times before delegating — so the `n` fragment
//! futures complete in a seed-driven order, and re-running a seed replays it
//! exactly. The store also records, with no timing or wall clock, **how many
//! fetches are in flight at once** and **how many reach the inner store**. Those
//! two counters are the oracle:
//!
//! - The parallel fan-out issues all `n` fetches before any completes, so the
//!   peak in-flight count is `n`. `main`'s in-index-order serial read awaits each
//!   fetch before issuing the next, so its peak in-flight count is `1`. Asserting
//!   the peak is `n` is therefore **red on the serial read, green on the fix** —
//!   the discriminating property, checked deterministically and without a hang.
//! - Once `k` fragments verify, the read reconstructs and drops the rest, so
//!   exactly `k` fetches ever reach the inner store: the cancellation invariant.

#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_core::metadata::{EcScheme, InodeRecord};
use wyrd_core::read::ReadError;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, Result};

const ROOT: u64 = 0;
// One chunk per object: a payload that fits a single chunk keeps the any-`k`
// arrival property crisp — exactly `n` fragment futures fan out, once.
const CHUNK: usize = 4096;
const MAX_PAYLOAD: usize = 512;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const SEEDS: u64 = 64;

// The scheme under test: rs(6,3) → n = 9 fragments, any k = 6 reconstruct.
const K: u8 = 6;
const M: u8 = 3;
const N: u16 = (K + M) as u16;
const RS: EcScheme = EcScheme::ReedSolomon { k: K, m: M };

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    let chunks = FsChunkStore::open(dir.path()).expect("fs store");
    (meta, chunks, dir)
}

fn ids_from(base: u128) -> impl FnMut() -> ChunkId {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A random payload of 1..=`max` bytes — guarantees at least one chunk and, with
/// `max <= CHUNK`, exactly one.
fn nonempty_payload(sim: &mut Sim, max: usize) -> Vec<u8> {
    let len = 1 + (sim.gen::<u16>() as usize) % max;
    (0..len).map(|_| sim.gen::<u8>()).collect()
}

/// `count` distinct fragment indices in `0..n`, chosen by a partial Fisher-Yates
/// shuffle from the seeded RNG (mirrors `dst_erasure.rs`).
fn choose_indices(sim: &mut Sim, n: u16, count: usize) -> Vec<u16> {
    let mut pool: Vec<u16> = (0..n).collect();
    let count = count.min(pool.len());
    for i in 0..count {
        let j = i + (sim.gen::<u16>() as usize) % (pool.len() - i);
        pool.swap(i, j);
    }
    pool.truncate(count);
    pool
}

/// A seed-driven **arrival schedule**: `yields[index]` is the number of times
/// that fragment's fetch yields `Pending` before completing. It is `rank + 1`
/// where `rank` is a uniform permutation of `0..n` drawn from the RNG — so every
/// fetch yields at least once (all `n` are in flight together before any
/// completes) and they complete in strict, seed-determined rank order. Varying
/// the seed varies which `k` fragments win the race, which is precisely the
/// arrival-order independence the read must honor (ADR-0009 reproducibility).
fn arrival_yields(sim: &mut Sim, n: usize) -> Vec<usize> {
    let mut rank: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = (sim.gen::<u32>() as usize) % (i + 1);
        rank.swap(i, j);
    }
    rank.into_iter().map(|r| r + 1).collect()
}

/// A future that returns `Pending` (re-waking itself) `remaining` times, then
/// resolves — a deterministic, runtime-free way to make one fetch complete after
/// another. Driven cooperatively by `block_on`, it makes "fragment X completes
/// before fragment Y" a pure function of the seed.
struct Yield {
    remaining: usize,
}

impl Future for Yield {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.remaining == 0 {
            Poll::Ready(())
        } else {
            this.remaining -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Decrements the in-flight counter when a fetch ends — whether it completes
/// normally or is **dropped (cancelled)** mid-yield once the read has its `k`.
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Per-run instrumentation shared with the read under test.
#[derive(Clone, Default)]
struct Probe {
    /// Fetches currently issued-but-not-finished.
    in_flight: Arc<AtomicUsize>,
    /// The peak concurrent in-flight count — `n` for the parallel fan-out, `1`
    /// for the serial read.
    peak_in_flight: Arc<AtomicUsize>,
    /// Fetches that got past their delay and reached the inner store — `k` once
    /// the slow `m` are cancelled.
    reached_inner: Arc<AtomicUsize>,
}

impl Probe {
    fn peak(&self) -> usize {
        self.peak_in_flight.load(Ordering::SeqCst)
    }

    fn reached(&self) -> usize {
        self.reached_inner.load(Ordering::SeqCst)
    }
}

/// A `ChunkStore` that delays each `get_fragment` by a seed-assigned number of
/// poll yields before delegating to an inner [`FsChunkStore`], recording the
/// concurrency and the count of fetches that reach the store. `put_fragment` and
/// `health` delegate unchanged.
struct ArrivalStore {
    inner: FsChunkStore,
    yields: Vec<usize>,
    probe: Probe,
}

impl ArrivalStore {
    fn new(dir: &std::path::Path, yields: Vec<usize>) -> (Self, Probe) {
        let probe = Probe::default();
        let store = ArrivalStore {
            inner: FsChunkStore::open(dir).expect("fs store"),
            yields,
            probe: probe.clone(),
        };
        (store, probe)
    }
}

#[async_trait]
impl ChunkStore for ArrivalStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        // This fetch is now in flight; record the running peak.
        let now = self.probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.probe.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        // Decrement on completion *or* cancellation (drop while still yielding).
        let _guard = InFlightGuard {
            in_flight: self.probe.in_flight.clone(),
        };

        let remaining = self.yields.get(id.index as usize).copied().unwrap_or(0);
        Yield { remaining }.await;

        // Past the delay: this fetch was not cancelled — it reaches the store.
        self.probe.reached_inner.fetch_add(1, Ordering::SeqCst);
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
    }
}

// A single fault-injecting store is its own location authority; it uses the
// `PlacementChunkStore` identity defaults (0005, M3.1).
impl wyrd_traits::PlacementChunkStore for ArrivalStore {}

/// Write a new object end to end under `scheme` and return its committed inode
/// (fragments land on disk with no delay — only the read is raced).
async fn put(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    data: &[u8],
    base: u128,
    scheme: EcScheme,
) -> InodeRecord {
    assert_eq!(
        write::write_new_object(
            meta,
            chunks,
            ROOT,
            "obj",
            1,
            data,
            CHUNK,
            scheme,
            || NOW,
            TTL,
            ids_from(base),
        )
        .await
        .unwrap(),
        CommitOutcome::Committed
    );
    read::read_inode(meta, 1).await.unwrap().unwrap()
}

fn delete(root: &std::path::Path, chunk: ChunkId, index: u16) {
    std::fs::remove_file(fragment_path(root, FragmentId { chunk, index })).unwrap();
}

/// Property: an `rs(6,3)` read **fans out all `n` fetches concurrently** and
/// reconstructs byte-identically from whichever `k` complete first, then cancels
/// the rest — independent of the seed-driven arrival order.
///
/// The peak-concurrency assertion is the discriminator: against `main`'s
/// in-index-order serial fetch only one fetch is ever in flight, so the peak is
/// `1`, not `n`, and the assertion fails.
fn any_k_arrive_first(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = nonempty_payload(&mut sim, MAX_PAYLOAD);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;
        assert_eq!(
            inode.chunk_map.len(),
            1,
            "single chunk keeps the race crisp"
        );

        let (store, probe) = ArrivalStore::new(dir.path(), arrival_yields(&mut sim, N as usize));
        let got = read::read_object_from(&store, &inode).await.unwrap();

        assert_eq!(
            got, data,
            "seed {seed}: reconstructs byte-identical from whichever k arrived first"
        );
        assert_eq!(
            probe.peak(),
            N as usize,
            "seed {seed}: all n fetches fan out concurrently (serial read peaks at 1)"
        );
        assert_eq!(
            probe.reached(),
            K as usize,
            "seed {seed}: exactly k fetches reach the store; the outstanding m are cancelled"
        );
    });
}

/// Property: with `m + 1` fragments deleted only `k - 1` survive, so the read
/// returns a clean typed [`ReadError::InsufficientFragments`] — no panic, no
/// short/corrupt read — regardless of the surviving fragments' arrival order. The
/// fan-out still issues all `n` fetches before giving up.
fn below_k_is_a_clean_typed_error(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = nonempty_payload(&mut sim, MAX_PAYLOAD);
        let inode = put(&meta, &chunks, &data, 0x10, RS).await;

        let chunk = inode.chunk_map[0].clone();
        for index in choose_indices(&mut sim, N, M as usize + 1) {
            delete(dir.path(), chunk.id, index);
        }

        let (store, probe) = ArrivalStore::new(dir.path(), arrival_yields(&mut sim, N as usize));
        let err = read::read_object_from(&store, &inode)
            .await
            .expect_err("fewer than k readable fragments cannot reconstruct");

        assert!(
            matches!(
                err.downcast_ref::<ReadError>(),
                Some(ReadError::InsufficientFragments { have, need: 6, .. })
                    if *have == (K - 1) as usize
            ),
            "seed {seed}: below k must surface InsufficientFragments {{ have: 5, need: 6 }}, got {err}"
        );
        assert_eq!(
            probe.peak(),
            N as usize,
            "seed {seed}: the read still fans out all n fetches before failing closed"
        );
    });
}

/// Property: an `EcScheme::None` chunk **stays a single fetch** — the unchanged
/// arm issues exactly one `get_fragment`, never a fan-out.
fn none_scheme_is_a_single_fetch(seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, dir) = backends();
        let data = nonempty_payload(&mut sim, MAX_PAYLOAD);
        let inode = put(&meta, &chunks, &data, 0x20, EcScheme::None).await;
        assert_eq!(inode.chunk_map.len(), 1, "single chunk");

        let (store, probe) = ArrivalStore::new(dir.path(), arrival_yields(&mut sim, N as usize));
        let got = read::read_object_from(&store, &inode).await.unwrap();

        assert_eq!(got, data, "seed {seed}: none-scheme reads its one fragment");
        assert_eq!(
            probe.peak(),
            1,
            "seed {seed}: none-scheme issues exactly one fetch, never a fan-out"
        );
        assert_eq!(
            probe.reached(),
            1,
            "seed {seed}: exactly one fetch reaches the store"
        );
    });
}

#[test]
fn any_k_arrive_first_across_seeds() {
    for seed in 0..SEEDS {
        any_k_arrive_first(seed);
    }
}

#[test]
fn below_k_is_a_clean_typed_error_across_seeds() {
    for seed in 0..SEEDS {
        below_k_is_a_clean_typed_error(seed);
    }
}

#[test]
fn none_scheme_is_a_single_fetch_across_seeds() {
    for seed in 0..SEEDS {
        none_scheme_is_a_single_fetch(seed);
    }
}

/// A pinned seed kept as a permanent regression guard (ADR-0009): any seed that
/// ever surfaces a bug is added here so the exact scenario replays forever.
#[test]
fn read_fanout_properties_hold_at_pinned_regression_seed() {
    const REGRESSION_SEED: u64 = 0x00C0_FFEE;
    any_k_arrive_first(REGRESSION_SEED);
    below_k_is_a_clean_typed_error(REGRESSION_SEED);
    none_scheme_is_a_single_fetch(REGRESSION_SEED);
}
