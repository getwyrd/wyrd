//! Issue #490, buffered-helper leg (Codex, PR #565): the whole-object helpers
//! (`write_new_object` / `write_new_object_placed`) must evaluate the lease-conditional
//! commit against the COMMIT instant, not the lease-stamp instant.
//!
//! Both helpers stamp their leases from the caller's clock and then drive phase 3
//! themselves. When they reused the SAME fixed `now_millis` for the stamp and the
//! commit's lease-liveness check, the guard was a tautology — `lease_expiry = t + ttl`
//! compared against the same `t` can never read as lapsed, no matter how long the data
//! phase or the caller stalled — so a stalled buffered create could still publish an
//! object over bytes the custodian GC is already free to reclaim (the exact hole the
//! lease-conditional commit exists to close). The helpers now take a clock CLOSURE
//! (exactly as `stream_write_data` does) and read it twice: once for the stamp, again
//! at the commit point.
//!
//! BINDING assertions: with a clock that advances past the TTL between the lease stamp
//! and the commit, the helper refuses (`Conflict`), the key never resolves, and the
//! pending entries are left for the sweep (not released). A clock still inside the TTL
//! at the commit instant publishes normally (the boundary leg).

use std::cell::Cell;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::EcScheme;
use wyrd_core::placement::Topology;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore};

const ROOT: u64 = 0;
const TTL: u64 = 30_000;
const CHUNK: usize = 1 << 16;
const KEY: &str = "obj";
const DATA: &[u8] = b"a stalled buffered create must never publish over reclaimable bytes";

/// A fresh chunk-id minter starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> ChunkId {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A logical clock read once per call: the FIRST read (the lease stamp) returns 0, every
/// later read (the commit instant) returns `commit_at` — simulating a data phase / caller
/// stall of exactly `commit_at` milliseconds between phase 1 and phase 3.
fn stalling_clock(commit_at: u64) -> impl FnMut() -> u64 {
    let calls = Cell::new(0u32);
    move || {
        let t = if calls.get() == 0 { 0 } else { commit_at };
        calls.set(calls.get() + 1);
        t
    }
}

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
    let meta = RedbMetadataStore::in_memory().unwrap();
    (meta, chunks, dir)
}

/// A buffered create whose commit instant lands ON the lease-expiry boundary
/// (`lease_expiry_millis <= now`, the sweep's own reap boundary) must fail closed with
/// `Conflict`, resolve nothing, and leave its pending entries for the sweep.
#[tokio::test]
async fn stalled_buffered_create_fails_closed_when_its_lease_lapses() {
    let (meta, chunks, _dir) = backends();

    let outcome = write::write_new_object(
        &meta,
        &chunks,
        ROOT,
        KEY,
        1,
        DATA,
        CHUNK,
        EcScheme::None,
        stalling_clock(TTL),
        TTL,
        ids_from(100),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        CommitOutcome::Conflict,
        "a buffered create that outran its lease (commit instant == expiry) must refuse \
         — pre-fix the guard compared the expiry against the stamp time and published"
    );
    assert_eq!(
        read::resolve(&meta, ROOT, KEY).await.unwrap(),
        None,
        "the refused create must publish nothing under the key"
    );
    assert!(
        !meta.scan(b"pending:").await.unwrap().is_empty(),
        "a refused commit must NOT release the pending ledger — the leased garbage \
         stays visible for the sweep to reap"
    );
}

/// The same seam through the failure-domain-placed helper.
#[tokio::test]
async fn stalled_buffered_placed_create_fails_closed_when_its_lease_lapses() {
    let (meta, chunks, _dir) = backends();
    // A trivial one-server topology: EcScheme::None places a single fragment.
    let mut topo = Topology::default();
    topo.register(0, "A");

    let outcome = write::write_new_object_placed(
        &meta,
        &chunks,
        ROOT,
        KEY,
        1,
        DATA,
        CHUNK,
        EcScheme::None,
        &topo,
        stalling_clock(TTL),
        TTL,
        ids_from(200),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        CommitOutcome::Conflict,
        "the placed helper shares the commit-instant lease check"
    );
    assert_eq!(read::resolve(&meta, ROOT, KEY).await.unwrap(), None);
}

/// The boundary's other side: a commit instant still INSIDE the TTL (expiry - 1)
/// publishes — proving the refusal above is the lapse, not the two-read clock itself.
#[tokio::test]
async fn buffered_create_inside_the_ttl_still_publishes() {
    let (meta, chunks, _dir) = backends();

    let outcome = write::write_new_object(
        &meta,
        &chunks,
        ROOT,
        KEY,
        1,
        DATA,
        CHUNK,
        EcScheme::None,
        stalling_clock(TTL - 1),
        TTL,
        ids_from(300),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        CommitOutcome::Committed,
        "one millisecond inside the lease the commit must still land"
    );
    assert_eq!(
        read::read_path(&meta, &chunks, ROOT, KEY)
            .await
            .unwrap()
            .as_deref(),
        Some(DATA),
        "the committed object reads back byte-identical"
    );
}
