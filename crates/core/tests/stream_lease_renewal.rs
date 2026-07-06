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

        // A logical clock that advances a FULL lease-TTL between chunk writes: chunk 0 at
        // t=0, chunk 1 at t=TTL, chunk 2 at t=2·TTL. Under the OLD single-stamp behaviour
        // every chunk's lease would expire at t=TTL, so a sweep at t=2·TTL reclaims chunks
        // 0 and 1 BEFORE the commit publishes the object.
        let mut tick = 0usize;
        let clock = move || {
            let t = [0u64, TTL, 2 * TTL][tick.min(2)];
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
        let outcome = write::commit_create(&meta, ROOT, "slow", 1, &plan)
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
