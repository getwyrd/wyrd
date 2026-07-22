//! Durability finding 2 (issue #364 carry-forward): a **slow streaming PUT** must not let
//! the custodian sweep reclaim its already-written, not-yet-committed chunks.
//!
//! A streaming PUT commits only after its *last* chunk arrives, so until then an early
//! chunk's fragments are protected from the custodian GC solely by its pending-ledger lease
//! (they are in no committed chunk map, so GC's committed reference set does not cover them
//! — `custodian::gc::reconcile`). The prior code stamped a single start-of-upload deadline
//! on every chunk, so a slow authenticated upload running past that deadline let the sweep
//! reclaim its early chunks as expired garbage before the commit — publishing an object with
//! missing fragments. This drives the production `write::stream_write_data` with a logical
//! clock that advances a full lease-TTL between chunks and asserts:
//!
//!  * every in-flight chunk lease is **renewed** past the sweep horizon (not left at its
//!    original deadline), so
//!  * `sweep_expired_leases` at that horizon reclaims **nothing**, and
//!  * the object commits and reads back **byte-identical** over the surviving fragments.
//!
//! Under the old single-stamp behaviour the sweep would reclaim the first two chunks — this
//! is the behavioural red the renewal turns green.

#![forbid(unsafe_code)]

use bytes::Bytes;
use futures_util::stream;
use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{self, EcScheme, PendingEntry};
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{CommitOutcome, MetadataStore};

const ROOT: u64 = 0;
/// The pending-lease lifetime the streaming write stamps.
const TTL: u64 = 30_000;

#[test]
fn slow_streaming_put_renews_in_flight_leases_before_commit() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
        let meta = RedbMetadataStore::in_memory().unwrap();

        // 12 bytes at chunk_size 4 → exactly three single-fragment chunks (scheme None), so
        // the streaming path leases three chunks in sequence.
        let payload: Vec<u8> = (0u8..12).collect();
        let source = stream::iter(vec![Ok(Bytes::from(payload.clone()))]);

        // A logical clock that advances JUST UNDER a full lease-TTL between chunk writes:
        // chunk 0 at t=0, chunk 1 at t=TTL-1, chunk 2 at t=2·(TTL-1). Each renewal therefore
        // fires while the prior chunk's lease is still alive (its expiry sits one tick ahead of
        // the renewal instant) — under the reaper-aligned `<=` boundary a renewal at exactly
        // `now == expiry` is refused (issue #490 obligation (b)), so the healthy slow-upload
        // path must renew strictly BEFORE expiry, which this clock does. Under the OLD
        // single-stamp behaviour every chunk's lease would still expire near t=TTL, so a sweep
        // at t=2·TTL reclaims the early chunks BEFORE the commit publishes the object.
        let mut tick = 0usize;
        let clock = move || {
            let t = [0u64, TTL - 1, 2 * (TTL - 1)][tick.min(2)];
            tick += 1;
            t
        };
        let mut cid = 0u128;
        let mint = move || {
            cid += 1;
            cid
        };

        let plan =
            write::stream_write_data(&meta, &chunks, source, 4, EcScheme::None, clock, TTL, mint)
                .await
                .unwrap();
        assert_eq!(plan.chunks.len(), 3, "three chunks over the streaming path");

        // Every in-flight lease has been renewed past the sweep horizon (t=2·TTL): the early
        // chunks are no longer sitting at their original t=TTL deadline.
        let pending = meta.scan(b"pending:").await.unwrap();
        assert_eq!(pending.len(), 3, "one pending lease per in-flight chunk");
        for (_key, value) in &pending {
            let entry: PendingEntry = metadata::decode(value).unwrap();
            assert!(
                entry.lease_expiry_millis > 2 * TTL,
                "an in-flight chunk lease was renewed past the sweep time, not left at its \
                 start-of-upload deadline (expiry={})",
                entry.lease_expiry_millis,
            );
        }

        // The binding regression: a custodian sweep at t=2·TTL (well past the old single
        // deadline) reclaims NOTHING — no in-flight chunk is torn out before the commit.
        let reclaimed = write::sweep_expired_leases(&meta, 2 * TTL).await.unwrap();
        assert!(
            reclaimed.is_empty(),
            "renewed in-flight leases survive the mid-upload sweep; reclaimed={reclaimed:?}",
        );

        // Publish the object (commit + release), then read it back over the fragments that
        // survived the sweep.
        let outcome = write::commit_create(&meta, ROOT, "slow", 1, &plan, 2 * TTL)
            .await
            .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);
        write::release(&meta, &plan).await.unwrap();

        let got = read::read_path(&meta, &chunks, ROOT, "slow").await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some(&payload[..]),
            "byte-identical after a slow, sweep-surviving PUT",
        );
    });
}

/// Renewal seam, the `<=` BOUNDARY (issue #490 obligation (b)). The reaper-aligned boundary
/// says a lease is dead at exactly `now == lease_expiry_millis`. This drives a streaming upload
/// whose renewal fires at **exactly** the prior chunk's deadline: chunk 0 is leased at t=0
/// (expiry = TTL) and the renewal clock arms half a TTL out, so chunk 1 at t=TTL triggers a
/// renewal evaluated at `now == expiry == TTL`. Under the correct `<=` boundary the renewal
/// refuses (`CommitOutcome::Conflict`) and `lease_write_chunk` aborts the upload; under a `<`
/// regression (`TTL < TTL` is false) the renewal would resurrect a lease the sweep is already
/// entitled to reap and the upload would proceed. This is the boundary case the healthy test
/// above deliberately steps around (it advances `TTL-1`), so nothing else pins it.
///
/// This test lives beside the renewal contract and is reverted on C4-verify's red leg (which
/// keeps only the added `stream_lease_lapse.rs`); it exists to pin the renewal boundary against
/// a `<`-regression mutation, not as part of the primary red→green.
#[test]
fn renewal_at_exact_deadline_aborts_the_upload() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
        let meta = RedbMetadataStore::in_memory().unwrap();

        // 8 bytes at chunk_size 4 → two chunks, so the second chunk fires a renewal of the first.
        let payload: Vec<u8> = (0u8..8).collect();
        let source = stream::iter(vec![Ok(Bytes::from(payload))]);

        // chunk 0 at t=0 (lease expiry = TTL, renew clock armed at TTL/2), chunk 1 at t=TTL —
        // so the renewal is evaluated at exactly `now == expiry` of chunk 0.
        let mut tick = 0usize;
        let clock = move || {
            let t = [0u64, TTL][tick.min(1)];
            tick += 1;
            t
        };
        let mut cid = 0u128;
        let mint = move || {
            cid += 1;
            cid
        };

        let result =
            write::stream_write_data(&meta, &chunks, source, 4, EcScheme::None, clock, TTL, mint)
                .await;

        assert!(
            result.is_err(),
            "a renewal at exactly the prior chunk's deadline (now == lease_expiry_millis) must \
             refuse and abort the upload — the sweep is entitled to reap at this instant, so the \
             boundary is `<=`, not `<` (issue #490 obligation (b))",
        );
    });
}

/// Create seam, refusal on a SWEPT lease (issue #490 obligation (d)). Every scenario in the
/// added `stream_lease_lapse.rs` is overwrite-shaped (its compile-against-base constraint forbids
/// the changed `commit_create` signature), so nothing there exercises `commit_create`'s own
/// lease guard with a lapsed lease — a mutant that dropped the guard from `create_leased` alone
/// would survive. This drives the create seam directly: stream a new object's chunks, let the
/// sweep reclaim their pending leases, then commit — which must refuse (fail closed) and publish
/// no dirent. Reverted on C4-verify's red leg; a post-fix regression guard.
#[test]
fn create_commit_refuses_when_lease_swept() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
        let meta = RedbMetadataStore::in_memory().unwrap();

        let mut cid = 0u128;
        let mint = move || {
            cid += 1;
            cid
        };
        // Stream at t=0: leases stamped to expire at TTL, still present when the stream ends.
        let plan = write::stream_write_data(
            &meta,
            &chunks,
            stream::iter(vec![Ok(Bytes::from((0u8..8).collect::<Vec<u8>>()))]),
            4,
            EcScheme::None,
            || 0u64,
            TTL,
            mint,
        )
        .await
        .unwrap();

        // Sweep reclaims every in-flight lease AFTER the stream returned but BEFORE the commit —
        // the create seam's own end-of-stream window (no renewal ever observes it).
        let reclaimed = write::sweep_expired_leases(&meta, 2 * TTL).await.unwrap();
        assert!(
            !reclaimed.is_empty(),
            "the sweep must reclaim the streamed create's pending leases",
        );

        let outcome = write::commit_create(&meta, ROOT, "created", 1, &plan, 2 * TTL)
            .await
            .unwrap();
        assert_ne!(
            outcome,
            CommitOutcome::Committed,
            "phase-3 create must refuse when a chunk's pending lease was swept before the commit",
        );
        assert!(
            read::resolve(&meta, ROOT, "created")
                .await
                .unwrap()
                .is_none(),
            "a refused create must publish no dirent",
        );
    });
}

/// Create seam, the `<=` BOUNDARY (issue #490 obligations (b)+(d)). Companion to
/// `create_commit_refuses_when_lease_swept`, but the leases are merely present-but-expired at
/// EXACTLY their deadline (no sweep): `commit_create` at `now == lease_expiry_millis == TTL` must
/// refuse on the expiry check, so a `<` regression in the create seam's guard is pinned too.
/// Reverted on C4-verify's red leg; a post-fix regression guard.
#[test]
fn create_commit_refuses_at_exact_lease_deadline() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let chunks = FsChunkStore::open(dir.path().join("frags")).unwrap();
        let meta = RedbMetadataStore::in_memory().unwrap();

        let mut cid = 0u128;
        let mint = move || {
            cid += 1;
            cid
        };
        let plan = write::stream_write_data(
            &meta,
            &chunks,
            stream::iter(vec![Ok(Bytes::from((0u8..8).collect::<Vec<u8>>()))]),
            4,
            EcScheme::None,
            || 0u64,
            TTL,
            mint,
        )
        .await
        .unwrap();
        // The leases are still present (no sweep) — the refusal is purely the exact-deadline
        // expiry check.
        assert_eq!(
            meta.scan(b"pending:").await.unwrap().len(),
            plan.chunks.len(),
            "every streamed lease must be present — this pins the create seam's expiry boundary",
        );

        let outcome = write::commit_create(&meta, ROOT, "created", 1, &plan, TTL)
            .await
            .unwrap();
        assert_ne!(
            outcome,
            CommitOutcome::Committed,
            "phase-3 create must refuse at the EXACT lease deadline (now == lease_expiry_millis)",
        );
        assert!(
            read::resolve(&meta, ROOT, "created")
                .await
                .unwrap()
                .is_none(),
            "a refused create must publish no dirent",
        );
    });
}
