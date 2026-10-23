//! **Seeded publication atomicity** for the staged-publication committer (issue #653,
//! proposal 0016 decision 7(b)–(d)), on the simulated-FoundationDB store with the
//! commit-ambiguity nemesis armed (`support/mod.rs`, `arm_mixed_commit_ambiguity`).
//!
//! Two tasks race on one segmented publication:
//!
//! * the **publisher** runs [`publish`] — one full segment per batch on odd seeds, the whole
//!   phase in one batch on even ones — and on every
//!   `CommitUnknownResult` or lost precondition recovers the way #658's Complete does —
//!   re-reads its own fence record and publishes again **at the same epoch** from the
//!   cursor it finds. Each attempt mints a fresh inode id, as a Complete that re-resolves
//!   its target may, so a second root is representable: only the flip's pins on the fence
//!   and the dirent keep one out;
//! * the **rival rollback** has the target's shape
//!   (`SessionRecord::completing_teardown`): it first wins a fence move off the publisher's
//!   value, and only then deletes the epoch's `seg:` range.
//!
//! The seed also picks how many commits the nemesis strikes (1–7) and, for half the seeds,
//! holds a timed-out batch in flight until the run settles.
//!
//! After every seed: at most one root exists, and the name binds it; a root that names the
//! group resolves, through the shared resolver, to the plan with every segment present; and
//! the fence agrees with whichever side won. The demonstrated red is a rival that drains on a
//! fence value it only **read** — the caller obligation the committer's doc states — and the
//! same invariant catches the root it leaves over a drained range.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`); without it this file compiles to
//! nothing.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use wyrd_core::metadata::{
    decode, dirent_key, inode_key, resolve_chunk_map, seg_range_prefix, ChunkRef, DirentRecord,
    EcScheme, InodeRecord, ObjectMeta, SegmentGroup,
};
use wyrd_core::multipart::MAX_MAP_CHUNKS;
use wyrd_core::staged_publication::{
    planned_segments, publish, Publication, PublicationError, RootTarget, SegmentBatchInfo,
};
use wyrd_traits::{BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, WriteBatch};

#[path = "support/mod.rs"]
mod support;
use support::{FdbFidelity, FdbObservations, SimFdbMetadataStore};

/// The inode id the publisher's first attempt mints; attempt `n` mints `FIRST_ID + n`.
const FIRST_ID: u64 = 8;
const NAME: &str = "big";
const FENCE: &[u8] = b"test:fence";
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 4;
/// Four full segments and a short tail.
const CHUNKS: usize = 4 * MAX_MAP_CHUNKS as usize + 10;
/// One full segment row per batch (a full row is about 8.5 KB), so the phase is four
/// segment commits and the nemesis lands mid-phase. Even seeds use the default budget
/// instead: the phase is one commit, and more strikes reach the flip.
const ONE_FULL_ROW_PER_BATCH: u64 = 10_000;
/// The most commits the nemesis strikes, starting with the publisher's first segment batch.
/// A seed strikes `1 + seed % MAX_STRIKES` of them: few strikes land mid-phase, and the
/// publication is four segment commits and a flip, so more of them reach the flip, where an
/// ambiguous flip still in flight sends the publisher round again with a fresh inode id.
const MAX_STRIKES: u64 = 7;
/// How many times either side re-reads and tries again before it stops.
const ATTEMPTS: usize = 16;
/// Seeds the plain-`#[test]` sweeps drive.
const SWEEP_SEEDS: u64 = 64;

fn cursor(n: u32) -> Bytes {
    Bytes::from(format!("cursor:{n}"))
}

fn group() -> SegmentGroup {
    SegmentGroup::new(NONCE, EPOCH).unwrap()
}

fn chunks() -> Vec<ChunkRef> {
    (0..CHUNKS)
        .map(|i| ChunkRef {
            id: 1 + i as u128,
            scheme: EcScheme::None,
            len: 4096,
            placement: vec![(i % 3) as u64 + 1],
        })
        .collect()
}

fn cursor_move(info: SegmentBatchInfo) -> WriteBatch {
    WriteBatch::new()
        .require(FENCE, cursor(info.first_index))
        .put(FENCE, cursor(info.written))
}

fn is_unknown(err: &BoxError) -> bool {
    err.downcast_ref::<CommitUnknownResult>().is_some()
}

/// How the publisher's run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    /// It read its own fence at `published`.
    Published,
    /// It read the fence at the rival's `aborting`.
    RolledBack,
    /// It ran out of attempts.
    GaveUp,
}

/// The publisher and its same-epoch recovery: re-read the fence, publish from the cursor it
/// names, repeat until the fence says the publication or the rollback won. Returns how it
/// ended, how many of its publishes came back `CommitUnknownResult`, and how many attempts
/// resumed mid-phase (from a cursor strictly between 0 and the segment count).
async fn publisher(
    meta: Arc<SimFdbMetadataStore>,
    chunks: Arc<Vec<ChunkRef>>,
    byte_budget: Option<u64>,
) -> (Ended, usize, usize) {
    let (mut unknowns, mut mid_phase) = (0, 0);
    let object = ObjectMeta {
        modified: Some(1),
        ..ObjectMeta::default()
    };
    let segments = planned_segments(&chunks).unwrap();
    for attempt in 0..ATTEMPTS as u64 {
        let fence = meta.get(FENCE).await.unwrap().expect("the fence exists");
        let resume_from = match fence.as_ref() {
            b"published" => return (Ended::Published, unknowns, mid_phase),
            b"aborting" => return (Ended::RolledBack, unknowns, mid_phase),
            value => std::str::from_utf8(value)
                .ok()
                .and_then(|v| v.strip_prefix("cursor:"))
                .and_then(|n| n.parse().ok())
                .expect("a cursor value"),
        };
        if 0 < resume_from && resume_from < segments {
            mid_phase += 1;
        }
        let publication = Publication {
            parent: 0,
            name: NAME,
            root: RootTarget::Fresh {
                id: FIRST_ID + attempt,
            },
            group: group(),
            chunks: &chunks,
            meta: &object,
            fence_key: FENCE,
            segment_contribution: &cursor_move,
            resume_from,
            byte_budget,
            ops_budget: None,
            flip_contribution: WriteBatch::new()
                .require(FENCE, cursor(segments))
                .put(FENCE, Bytes::from_static(b"published")),
        };
        match publish(&*meta, &publication).await {
            // Committed or Conflict alike: the next read of the fence says which.
            Ok(_) => {}
            Err(err) if is_unknown(&err) => unknowns += 1,
            // A rival drained under this attempt; its fence move is what the next read sees.
            Err(err)
                if matches!(
                    err.downcast_ref::<PublicationError>(),
                    Some(PublicationError::SegmentNotDurable { .. })
                ) => {}
            Err(err) => panic!("the publisher met an unexpected error: {err}"),
        }
    }
    (Ended::GaveUp, unknowns, mid_phase)
}

/// Which rollback the rival runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rival {
    /// The target's shape: commit a fence move off the publisher's value, then drain.
    FenceThenDrain,
    /// Violating: drain on a publisher value it only read, without moving the fence.
    DrainOnRead,
}

/// Delete the group's whole `seg:` range, page by page, until it reads empty.
async fn drain(meta: &SimFdbMetadataStore) {
    let prefix = seg_range_prefix(&group());
    for _ in 0..ATTEMPTS {
        let (page, _) = meta.scan_page(&prefix, None, 128).await.unwrap();
        if page.is_empty() {
            return;
        }
        let mut batch = WriteBatch::new();
        for (key, _) in page {
            batch = batch.delete(key);
        }
        match meta.commit(batch).await {
            Ok(_) => {}
            Err(err) if is_unknown(&err) => {}
            Err(err) => panic!("the drain met an unexpected error: {err}"),
        }
    }
}

/// The rival rollback. Returns whether it drained.
async fn rival(meta: Arc<SimFdbMetadataStore>, delay_ms: u64, shape: Rival) -> bool {
    madsim::time::sleep(Duration::from_millis(delay_ms)).await;
    for _ in 0..ATTEMPTS {
        let read = meta.get(FENCE).await.unwrap().expect("the fence exists");
        match read.as_ref() {
            b"published" => return false,
            // Only this rival writes `aborting`: an earlier ambiguous move of its own landed.
            b"aborting" => {
                drain(&meta).await;
                return true;
            }
            _ => {}
        }
        if shape == Rival::DrainOnRead {
            drain(&meta).await;
            madsim::time::sleep(Duration::from_millis(1)).await;
            continue;
        }
        let fence_move = WriteBatch::new()
            .require(FENCE, read)
            .put(FENCE, Bytes::from_static(b"aborting"));
        match meta.commit(fence_move).await {
            Ok(CommitOutcome::Committed) => {
                drain(&meta).await;
                return true;
            }
            // The publisher moved the fence first, or the move's outcome is unknown: the next
            // read decides. (A 1031 move still in flight may land later, leaving the range
            // undrained — reclaimable residue, never a published root over it.)
            Ok(CommitOutcome::Conflict) => {}
            Err(err) if is_unknown(&err) => {}
            Err(err) => panic!("the rival met an unexpected error: {err}"),
        }
    }
    false
}

/// What one seed produced.
#[derive(Debug)]
struct Report {
    ended: Ended,
    unknowns: usize,
    mid_phase: usize,
    published: bool,
    obs: FdbObservations,
}

/// The invariant, on the store's settled state. Returns whether a root was published.
async fn assert_atomic(meta: &SimFdbMetadataStore, chunks: &[ChunkRef]) -> bool {
    meta.quiesce();
    let mut roots = meta.scan(b"inode:").await.unwrap();
    assert!(roots.len() <= 1, "more than one root: {}", roots.len());
    let fence = meta.get(FENCE).await.unwrap();
    let Some((key, bytes)) = roots.pop() else {
        assert_ne!(
            fence.as_deref(),
            Some(&b"published"[..]),
            "the fence says published but no root exists"
        );
        return false;
    };
    let name = meta.get(&dirent_key(0, NAME)).await.unwrap();
    let dirent: DirentRecord = decode(&name.expect("a root no name binds")).unwrap();
    assert_eq!(key, inode_key(dirent.inode), "the name binds another inode");
    let root: InodeRecord = decode(&bytes).unwrap();
    let map = root.chunk_map.segmented().expect("a segmented root");
    assert_eq!(map.group(), &group(), "the root names another group");
    let resolved = resolve_chunk_map(meta, &key, &root)
        .await
        .expect("a published root names a missing or different seg: record")
        .expect("the published root is live");
    assert_eq!(
        resolved.chunks.as_ref(),
        chunks,
        "the root resolves to another map"
    );
    let rows = meta.scan(&seg_range_prefix(&group())).await.unwrap();
    assert_eq!(rows.len(), map.segment_count() as usize);
    assert_eq!(fence.as_deref(), Some(&b"published"[..]));
    true
}

async fn scenario(seed: u64, shape: Rival) -> Report {
    let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
        FdbFidelity::CommitUnknownResult,
    ));
    meta.commit(WriteBatch::new().put(FENCE, cursor(0)))
        .await
        .unwrap();
    meta.arm_mixed_commit_ambiguity(1 + seed % MAX_STRIKES);
    // Every other block of seeds holds a timed-out batch in flight until the run settles
    // (the latest instant FDB allows), so an ambiguous flip can still land after the
    // publisher has gone round again — the case only the flip's pins decide.
    if (seed / 24) % 2 == 1 {
        meta.defer_in_flight_landings();
    }
    let chunks = Arc::new(chunks());
    // Each network hop is one millisecond and a clean publication takes about a dozen, so
    // the seed spreads the rival from before the first segment to after the flip.
    let delay_ms = seed % 24;
    let byte_budget = (seed % 2 == 1).then_some(ONE_FULL_ROW_PER_BATCH);
    let publishing = madsim::task::spawn(publisher(
        Arc::clone(&meta),
        Arc::clone(&chunks),
        byte_budget,
    ));
    let rolling_back = madsim::task::spawn(rival(Arc::clone(&meta), delay_ms, shape));
    let (ended, unknowns, mid_phase) = publishing.await.unwrap();
    rolling_back.await.unwrap();
    let published = assert_atomic(&meta, &chunks).await;
    Report {
        ended,
        unknowns,
        mid_phase,
        published,
        obs: meta.observations(),
    }
}

fn run(seed: u64, shape: Rival) -> Report {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(scenario(seed, shape))
}

/// **The seed sweep `cargo xtask dst` drives**: at every seed the publication is atomic,
/// and the nemesis struck at least once, so no seed passes on a fault-free run.
#[madsim::test]
async fn segmented_publication_is_atomic_under_ambiguity_and_a_rival_rollback() {
    let seed = madsim::runtime::Handle::current().seed();
    let report = scenario(seed, Rival::FenceThenDrain).await;
    assert!(
        report.obs.ambiguous_commits >= 1,
        "the nemesis never struck: {report:?}"
    );
}

/// The sweep explores both winners and the recovery it exists for: some seeds publish,
/// some roll back, and some publish only after recovering at the same epoch from a
/// `CommitUnknownResult`, resuming mid-phase.
#[test]
fn the_sweep_reaches_both_winners_and_a_mid_phase_recovery() {
    let (mut published, mut rolled_back, mut recovered) = (0, 0, 0);
    for seed in 0..SWEEP_SEEDS {
        let report = run(seed, Rival::FenceThenDrain);
        assert!(report.obs.ambiguous_commits >= 1, "seed {seed}: no strike");
        assert_eq!(report.published, report.ended == Ended::Published);
        match report.ended {
            Ended::Published if report.unknowns >= 1 && report.mid_phase >= 1 => {
                published += 1;
                recovered += 1;
            }
            Ended::Published => published += 1,
            Ended::RolledBack => rolled_back += 1,
            Ended::GaveUp => panic!("seed {seed}: the publisher gave up: {report:?}"),
        }
    }
    assert!(published >= 1, "no seed published");
    assert!(rolled_back >= 1, "no seed rolled back");
    assert!(
        recovered >= 1,
        "no seed published after a mid-phase recovery from an unknown commit result"
    );
}

/// **Demonstrated red:** a rival that drains on a fence value it only read — no fence move
/// first — leaves a published root over a drained range at some seed, and the invariant
/// above catches it. Without such a seed this test fails, so it also guards the sweep
/// against reaching that window by luck alone.
#[test]
#[should_panic(expected = "a published root names a missing or different seg: record")]
fn a_rival_draining_on_a_read_value_is_caught() {
    for seed in 0..SWEEP_SEEDS {
        run(seed, Rival::DrainOnRead);
    }
}
