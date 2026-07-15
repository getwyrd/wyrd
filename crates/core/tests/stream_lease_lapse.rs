//! Issue #490 (streaming-write carry-forward from #364): a streaming PUT whose in-flight
//! pending lease **LAPSES** must never publish an object over fragments the custodian GC is
//! free to reclaim — through EITHER seam of the pending-lease lifecycle.
//!
//! A streaming PUT commits only after its *last* chunk arrives, so until then an early chunk's
//! fragments are protected from the custodian sweep/GC solely by its pending-ledger lease
//! (`crates/core/src/write.rs:409-415`). If the upload stalls past the TTL and the sweep runs,
//! that chunk's `pending:<id>` entry is reclaimed — a genuinely dead upload the sweep is
//! entitled to reap (`write.rs:417-418`); and GC reclaims the fragment BYTES keyed on expiry
//! even while the entry is merely present-but-expired (`crates/custodian/src/gc.rs:142-144`).
//! Two seams could still publish over such reclaimable bytes:
//!
//!   * **renewal seam** — the next chunk's blind renewal RE-CREATED a swept `pending:<id>`
//!     entry (`metadata::renew_pending`), so the upload proceeded and committed; and
//!   * **commit seam** — phase 3 (`commit_create` / `commit_overwrite`) was UNCONDITIONAL on
//!     the pending ledger, so a lapse the renewal never observed (a stall after the last chunk
//!     but before end-of-stream, or between `stream_write_data` returning and the caller
//!     driving the commit, or a present-but-expired lease at commit) still published.
//!
//! Every scenario is OVERWRITE-shaped: an initial object is published under the key, then a new
//! version is streamed and phase 3 is driven through the signature-stable
//! `write::commit_overwrite`. The binding, red-pre-fix / green-post-fix assertions are that the
//! lapsed upload is REFUSED (the stream errors, or the commit does not return `Committed`) and
//! the key still reads back the ORIGINAL bytes — never the lapsed version.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use bytes::Bytes;
use futures_util::stream;
use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{self, EcScheme};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore};

const ROOT: u64 = 0;
/// The pending-lease lifetime the streaming write stamps.
const TTL: u64 = 30_000;
const CHUNK: usize = 4;
const KEY: &str = "obj";
/// The bytes the initial object holds — a lapsed streaming overwrite must never replace them.
const ORIGINAL: &[u8] = b"AAAAAAAA"; // 8 bytes → two chunks
/// The bytes a lapsed streaming overwrite would (wrongly) publish.
const NEW_VERSION: &[u8] = b"BBBBBBBBBBBB"; // 12 bytes → three chunks

/// A fresh chunk-id minter starting just above `base`, so the initial object and the streamed
/// overwrite draw disjoint id ranges.
fn ids_from(base: u128) -> impl FnMut() -> ChunkId {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// Publish the initial object under `KEY` and resolve the `(inode_id, prior)` a streaming
/// overwrite commits against — exactly as the gateway does (`server/src/lib.rs:179-183`).
/// `write_new_object` runs the full four-phase protocol at `now=0` and releases its leases, so
/// the ledger is clean before the overwrite streams its own pending entries.
async fn publish_initial(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
) -> (u64, wyrd_core::metadata::InodeRecord) {
    let inode_id = 1u64;
    let outcome = write::write_new_object(
        meta,
        chunks,
        ROOT,
        KEY,
        inode_id,
        ORIGINAL,
        CHUNK,
        EcScheme::None,
        0,
        TTL,
        ids_from(100),
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed,
        "initial publish must land"
    );
    let resolved = read::resolve(meta, ROOT, KEY).await.unwrap().unwrap();
    let prior = read::read_inode(meta, resolved).await.unwrap().unwrap();
    (resolved, prior)
}

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
    let meta = RedbMetadataStore::in_memory().unwrap();
    (meta, chunks, dir)
}

/// Assert the key still resolves to the ORIGINAL bytes — nothing was published over it.
async fn assert_key_unchanged(meta: &RedbMetadataStore, chunks: &FsChunkStore) {
    let got = read::read_path(meta, chunks, ROOT, KEY).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(ORIGINAL),
        "the key must still read back the ORIGINAL object — a lapsed upload published over it",
    );
}

/// Split a payload into `CHUNK`-sized `Bytes` items, so a stream generator can yield one chunk
/// per step (and inject a sweep between two of them).
fn chunk_items(payload: &[u8]) -> Vec<Bytes> {
    payload.chunks(CHUNK).map(Bytes::copy_from_slice).collect()
}

/// Seam 1 — **renewal**, Repro (A): the input stream sweeps a mid-flight lease between two
/// chunks. The next chunk's renewal must REFUSE to resurrect it and the upload must abort; on
/// the `Ok` arm only (the base tree) phase 3 is driven, which makes "nothing published"
/// genuinely falsifiable.
#[test]
fn mid_upload_lapse_aborts_the_stream_and_publishes_nothing() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let (inode_id, prior) = publish_initial(&meta, &chunks).await;

        // A shared logical clock: read per chunk by `now_fn`, and jumped by the input stream
        // between chunk 0 and chunk 1. Single-threaded (`block_on`), so `Rc<Cell<..>>` is sound.
        let clock = Rc::new(Cell::new(0u64));
        let reclaimed: Rc<RefCell<Vec<ChunkId>>> = Rc::new(RefCell::new(Vec::new()));

        let items = chunk_items(NEW_VERSION);
        let meta_ref = &meta;
        let stream_clock = clock.clone();
        let reclaimed_log = reclaimed.clone();
        // Between yielding chunk 0's and chunk 1's bytes (on step i==1), jump the clock a full
        // 2·TTL ahead and run the custodian sweep — reclaiming chunk 0's in-flight pending entry
        // while the upload is still running.
        let source = Box::pin(stream::unfold(0usize, move |i| {
            let stream_clock = stream_clock.clone();
            let reclaimed_log = reclaimed_log.clone();
            let item = items.get(i).cloned();
            async move {
                let bytes = item?;
                if i == 1 {
                    stream_clock.set(2 * TTL);
                    let swept = write::sweep_expired_leases(meta_ref, 2 * TTL)
                        .await
                        .unwrap();
                    reclaimed_log.borrow_mut().extend(swept);
                }
                Some((Ok(bytes), i + 1))
            }
        }));

        let now_clock = clock.clone();
        let now_fn = move || now_clock.get();

        let result = write::stream_write_data(
            &meta,
            &chunks,
            source,
            CHUNK,
            EcScheme::None,
            now_fn,
            TTL,
            ids_from(200),
        )
        .await;

        // Fault-injection guard: the mid-upload sweep must actually have torn out chunk 0's
        // in-flight lease (id 201). If empty, the test proves nothing.
        assert_eq!(
            *reclaimed.borrow(),
            vec![201u128],
            "the mid-upload sweep must reclaim chunk 0's in-flight pending lease",
        );

        // ON THE `Ok` ARM ONLY, drive phase 3 — so the "nothing published" assertion below is
        // genuinely falsifiable: the base tree resurrects the lease, returns `Ok`, and this
        // commit publishes the new version.
        if let Ok(plan) = &result {
            let _ = write::commit_overwrite(&meta, inode_id, &prior, plan, 2 * TTL).await;
        }

        // BINDING #1 — the upload must FAIL: the renewal cannot resurrect the swept lease.
        assert!(
            result.is_err(),
            "a streaming upload whose in-flight lease lapsed mid-upload must abort, not produce \
             a commit plan over reclaimed fragments",
        );
        // BINDING #2 — the swept `pending:201` must NOT be resurrected by the renewal.
        let swept_key = metadata::pending_key(201);
        let pending = meta.scan(b"pending:").await.unwrap();
        assert!(
            !pending.iter().any(|(k, _)| *k == swept_key),
            "the renewal must not re-create the pending entry the sweep reclaimed",
        );
        // BINDING #3 — nothing was published over the key.
        assert_key_unchanged(&meta, &chunks).await;
    });
}

/// Seam 2 — **commit**, Repro (B): the stall lands in the end-of-stream window the renewal never
/// observes. The generator sweeps every lease on the EOF pull (after the last chunk), so
/// `stream_write_data` returns `Ok(plan)` on both trees — then the commit must REFUSE.
#[test]
fn eof_window_lapse_refuses_at_commit_and_publishes_nothing() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let (inode_id, prior) = publish_initial(&meta, &chunks).await;

        // The clock stays at 0 for every chunk (no renewal fires: renew_at is half a TTL out),
        // so all leases are stamped to expire at TTL and are present when the stream ends.
        let reclaimed: Rc<RefCell<Vec<ChunkId>>> = Rc::new(RefCell::new(Vec::new()));
        let items = chunk_items(NEW_VERSION);
        let meta_ref = &meta;
        let reclaimed_log = reclaimed.clone();
        // On the EOF pull (i == items.len(), the step that returns `None`), jump the clock and
        // sweep — reclaiming every lease AFTER the last chunk is written but BEFORE the caller
        // drives phase 3. No renewal ever observes this lapse.
        let source = Box::pin(stream::unfold(0usize, move |i| {
            let reclaimed_log = reclaimed_log.clone();
            let item = items.get(i).cloned();
            async move {
                match item {
                    Some(bytes) => Some((Ok(bytes), i + 1)),
                    None => {
                        if reclaimed_log.borrow().is_empty() {
                            let swept = write::sweep_expired_leases(meta_ref, 2 * TTL)
                                .await
                                .unwrap();
                            reclaimed_log.borrow_mut().extend(swept);
                        }
                        None
                    }
                }
            }
        }));

        let plan = write::stream_write_data(
            &meta,
            &chunks,
            source,
            CHUNK,
            EcScheme::None,
            || 0u64,
            TTL,
            ids_from(200),
        )
        .await
        .expect("the EOF-window stall is invisible to the stream: it returns Ok on both trees");

        // Fault-injection guard: the EOF sweep must have reclaimed the streamed leases.
        assert!(
            !reclaimed.borrow().is_empty(),
            "the EOF-window sweep must reclaim the streamed pending leases",
        );

        // The commit must REFUSE (Conflict, not Committed) — the leases are gone.
        let outcome = write::commit_overwrite(&meta, inode_id, &prior, &plan, 2 * TTL)
            .await
            .unwrap();
        assert_ne!(
            outcome,
            CommitOutcome::Committed,
            "phase-3 commit must refuse when a chunk's pending lease was swept before the commit",
        );
        assert_key_unchanged(&meta, &chunks).await;
    });
}

/// Seam 2 — **commit**, Repro (C): no sweep at all. The leases are merely present-but-expired at
/// the commit's own instant — bytes `gc.rs:142-144` already treats as reclaimable — so the
/// commit must refuse on the `<=`-boundary expiry check.
#[test]
fn present_but_expired_leases_refuse_at_commit_and_publish_nothing() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let (inode_id, prior) = publish_initial(&meta, &chunks).await;

        // Stream completes normally at t=0: every lease is present, stamped to expire at TTL.
        let plan = write::stream_write_data(
            &meta,
            &chunks,
            stream::iter(vec![Ok(Bytes::from(NEW_VERSION))]),
            CHUNK,
            EcScheme::None,
            || 0u64,
            TTL,
            ids_from(200),
        )
        .await
        .unwrap();
        // The leases really are still present (no sweep) — the refusal is purely the expiry
        // check, not an absent entry.
        assert_eq!(
            meta.scan(b"pending:").await.unwrap().len(),
            plan.chunks.len(),
            "every streamed lease must be present at commit time — this scenario tests expiry",
        );

        // Commit at an instant past every lease's expiry (TTL): expiry <= now → refuse.
        let outcome = write::commit_overwrite(&meta, inode_id, &prior, &plan, 2 * TTL)
            .await
            .unwrap();
        assert_ne!(
            outcome,
            CommitOutcome::Committed,
            "phase-3 commit must refuse when a chunk's pending lease is present but expired \
             (lease_expiry_millis <= now)",
        );
        assert_key_unchanged(&meta, &chunks).await;
    });
}

/// Seam 2 — **commit**, the `<=` BOUNDARY, Repro (C) sharpened. Repro (C) above commits a full
/// TTL past expiry (`2·TTL` vs expiry `TTL`), so it cannot distinguish the reaper-aligned `<=`
/// from a `<` regression — it stays red under both. This scenario commits at **exactly**
/// `now == lease_expiry_millis == TTL`: the sweep (`write.rs:572`, `<=`) and GC (`gc.rs`, `<=`)
/// are already entitled to reap at this instant, so the commit MUST refuse. Under `<=` (correct)
/// it refuses; under a `<` mutant (iteration 1's exact rejected bug, obligation (b)) `TTL < TTL`
/// is false and it would publish — so this test is the one that pins the boundary. It is
/// red-provable on base too (unconditional commit → `Committed`).
#[test]
fn commit_refuses_at_exact_lease_deadline() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let (inode_id, prior) = publish_initial(&meta, &chunks).await;

        // Stream completes at t=0: every lease is present, stamped to expire at exactly TTL.
        let plan = write::stream_write_data(
            &meta,
            &chunks,
            stream::iter(vec![Ok(Bytes::from(NEW_VERSION))]),
            CHUNK,
            EcScheme::None,
            || 0u64,
            TTL,
            ids_from(200),
        )
        .await
        .unwrap();
        // The leases are all still present (no sweep) — the refusal is purely the `<=`-boundary
        // expiry check, evaluated at the exact deadline.
        assert_eq!(
            meta.scan(b"pending:").await.unwrap().len(),
            plan.chunks.len(),
            "every streamed lease must be present at commit time — this scenario tests the \
             exact-deadline expiry boundary, not an absent entry",
        );

        // Commit at EXACTLY the lease deadline (now == expiry == TTL). expiry <= now is true, so
        // a lease is dead at its deadline — the reaper's own contract — and the commit refuses.
        let outcome = write::commit_overwrite(&meta, inode_id, &prior, &plan, TTL)
            .await
            .unwrap();
        assert_ne!(
            outcome,
            CommitOutcome::Committed,
            "phase-3 commit must refuse at the EXACT lease deadline (now == lease_expiry_millis): \
             the sweep and GC are entitled to reap at this instant, so a `<` boundary that let it \
             publish would resurrect authority already revoked (issue #490 obligation (b))",
        );
        assert_key_unchanged(&meta, &chunks).await;
    });
}

/// Healthy control (green on both trees): a normal streaming overwrite whose leases are live and
/// unexpired at commit still publishes and reads back the NEW bytes. This pins the fail-closed
/// guards against overreach — they must not refuse a live upload.
#[test]
fn live_overwrite_still_commits_and_reads_back_the_new_version() {
    block_on(async {
        let (meta, chunks, _dir) = backends();
        let (inode_id, prior) = publish_initial(&meta, &chunks).await;

        let plan = write::stream_write_data(
            &meta,
            &chunks,
            stream::iter(vec![Ok(Bytes::from(NEW_VERSION))]),
            CHUNK,
            EcScheme::None,
            || 0u64,
            TTL,
            ids_from(200),
        )
        .await
        .unwrap();

        // Commit while the leases are live (now = TTL/2 < expiry = TTL): the guard must pass.
        let outcome = write::commit_overwrite(&meta, inode_id, &prior, &plan, TTL / 2)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            CommitOutcome::Committed,
            "a live, unexpired streaming overwrite must still commit",
        );
        write::release(&meta, &plan).await.unwrap();

        let got = read::read_path(&meta, &chunks, ROOT, KEY).await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some(NEW_VERSION),
            "the committed object must read back the NEW version, byte-identical",
        );
    });
}
