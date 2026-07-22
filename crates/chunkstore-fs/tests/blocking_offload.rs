//! Reactor-liveness regression for `FsChunkStore`'s async `ChunkStore` methods
//! (issue #204).
//!
//! `FsChunkStore` implements the **async** `ChunkStore` trait, but its method
//! bodies are synchronous blocking `std::fs` syscalls. The d-server hosts the
//! store on a multi-threaded tokio runtime and dispatches each storage RPC as a
//! task; if a method runs its syscall directly on the reactor worker thread, that
//! thread is **pinned for the whole syscall** and every other task on the runtime
//! is starved — including the lease-renew heartbeat, whose missed tick past the
//! lease TTL drops the server out of discovery.
//!
//! This test makes that starvation observable on a **constrained** runtime: a
//! single worker thread, with a co-scheduled timer task and a burst of storage
//! I/O (`list_fragments`, the O(N) walk that is the worst starvation source). Both
//! the timer and the burst are spawned **tasks on the one worker**, so they share
//! that worker cooperatively:
//!
//!   * Before the fix — the burst runs every `list_fragments` walk synchronously
//!     on the worker and never yields, so it monopolises the single worker for the
//!     whole burst; the timer task cannot be polled and makes **zero** progress
//!     while the I/O is in flight.
//!   * After the fix — each walk is offloaded to tokio's blocking pool, so the
//!     worker is free between awaits; the timer task interleaves and keeps ticking
//!     while the I/O runs.
//!
//! We assert the timer advanced *during* the burst. `list_fragments` only parses
//! directory-entry names (it never reads file contents), so the store is populated
//! with empty `.frag` files directly — no fragment encoding needed — keeping the
//! unit under test import-light (no GUI / display) and safe on a headless runner.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_traits::ChunkStore;

/// Chunk directories to lay down under the store root.
const CHUNK_DIRS: u128 = 64;
/// Fragment files per chunk directory — `CHUNK_DIRS * FRAGS_PER_DIR` entries make
/// each `list_fragments` walk a non-trivial, observably-slow syscall sequence.
const FRAGS_PER_DIR: u16 = 16;
/// How many `list_fragments` walks the burst performs back-to-back.
const BURST_WALKS: usize = 500;
/// The timer ticks every millisecond; the burst takes far longer than a few
/// milliseconds, so a freed reactor accumulates many ticks. Before the fix the
/// pinned worker yields **zero** ticks during the burst — this threshold sits well
/// inside that gap so the test flips red→green on the offload, not on timing noise.
const MIN_TICKS_DURING_BURST: u64 = 5;

/// Populate `root` with `CHUNK_DIRS` chunk directories, each holding
/// `FRAGS_PER_DIR` empty `<index>.frag` files, mirroring the on-disk layout
/// `root/<32-hex chunk>/<05-index>.frag` that `list_fragments` walks.
fn populate(root: &std::path::Path) {
    for chunk in 0..CHUNK_DIRS {
        let dir = root.join(format!("{chunk:032x}"));
        std::fs::create_dir_all(&dir).expect("create chunk dir");
        for index in 0..FRAGS_PER_DIR {
            std::fs::write(dir.join(format!("{index:05}.frag")), b"").expect("write frag file");
        }
    }
}

#[test]
fn storage_io_burst_does_not_starve_a_co_scheduled_timer() {
    let dir = tempfile::tempdir().expect("temp dir");
    populate(dir.path());
    let store = Arc::new(FsChunkStore::open(dir.path()).expect("open store"));

    // A single reactor worker thread is the constrained runtime that makes the
    // starvation observable: with only one worker, a method that blocks it leaves
    // nothing to run the co-scheduled timer.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build runtime");

    let ticks = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let during = rt.block_on({
        let ticks = Arc::clone(&ticks);
        let stop = Arc::clone(&stop);
        async move {
            // Co-scheduled timer task: tick, then sleep a millisecond, until told
            // to stop. Spawned (not the block_on root) so it lives on the single
            // worker thread alongside the burst and competes for it.
            let timer = tokio::spawn({
                let ticks = Arc::clone(&ticks);
                let stop = Arc::clone(&stop);
                async move {
                    while !stop.load(Ordering::Relaxed) {
                        ticks.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            });

            // The storage-I/O burst, also a task on the one worker.
            let burst = tokio::spawn({
                let store = Arc::clone(&store);
                let ticks = Arc::clone(&ticks);
                async move {
                    let start = ticks.load(Ordering::Relaxed);
                    for _ in 0..BURST_WALKS {
                        store.list_fragments().await.expect("list_fragments");
                    }
                    // Ticks the timer managed to fire *while the burst was in
                    // flight*. Zero ⇒ the worker was pinned by the blocking walk.
                    ticks.load(Ordering::Relaxed) - start
                }
            });

            let during = burst.await.expect("burst task");
            stop.store(true, Ordering::Relaxed);
            timer.abort();
            during
        }
    });

    assert!(
        during >= MIN_TICKS_DURING_BURST,
        "a co-scheduled timer made only {during} tick(s) during the storage-I/O burst \
         (expected >= {MIN_TICKS_DURING_BURST}): the blocking filesystem syscalls are \
         pinning the reactor worker thread instead of running off the reactor, so the \
         heartbeat/timer is starved while storage I/O is in flight (issue #204)"
    );
}
