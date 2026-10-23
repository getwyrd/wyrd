//! The staged-publication committer (issue #653, proposal 0016 decision 7(b)–(d)), driven
//! for real over [`RedbMetadataStore::in_memory`] through [`Recorder`], a wrapper that logs
//! every batch the committer hands the store and can answer one with `Conflict`, or run a
//! rival's batch just before or just after it.
//!
//! The fence record is test-local (`FENCE`); the committer must not care what it is. Its
//! values model the multipart session: `cursor:<n>` while `n` segments are durable,
//! `published` after the flip, `aborting` after a rollback's fence move.
//!
//! One test per acceptance leg of the brief: (A) round trip, (B) bounded batches, (C)
//! refusals before any commit, (D1)–(D3) the whole range verified, (E) the flip carries the
//! caller's batch, (F) epoch disjointness, fence cycles and both rollback orders.

#![forbid(unsafe_code)]

use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{
    dirent_key, encode, inode_key, resolve_chunk_map, seg_key, seg_range_prefix, ChunkMap,
    ChunkRef, DirentRecord, EcScheme, InodeRecord, ObjectMeta, SegmentGroup, MAX_ROOT_SEGMENTS,
    MAX_ROOT_VALUE_BYTES, SEGMENT_PAGE_LIMIT,
};
use wyrd_core::multipart::{MAX_BATCH_OPS, MAX_MAP_CHUNKS, MAX_SEG_CHUNKS};
use wyrd_core::staged_publication::{
    planned_segments, publish, Phase, Publication, PublicationError, RootTarget, SegmentBatchInfo,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{CommitOutcome, MetadataStore, Result, ScanPage, WriteBatch};

const ROOT: u64 = 0;
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 4;
/// The test-local fence record.
const FENCE: &[u8] = b"test:fence";
/// A key of the caller's own that its flip contribution writes (leg E).
const FLIP_MARK: &[u8] = b"test:flip-mark";
/// What [`BIG`] chunks plan to: four full segments and a short tail (checked by
/// `the_fixture_plans_the_segment_count_the_legs_assume`).
const SEGMENTS: u32 = 5;
const BIG: usize = 4 * MAX_MAP_CHUNKS as usize + 10;

// ---------------------------------------------------------------------------
// The recording wrapper
// ---------------------------------------------------------------------------

/// A rival batch, built from the fence record's value as the inner store holds it at that
/// instant, so a rival can require what it read.
type Hook = Box<dyn Fn(Option<Bytes>) -> WriteBatch + Send + Sync>;

/// What [`Recorder`] does to the one commit a [`Rule`] matches.
enum Action {
    /// Answer `Conflict` without forwarding the batch.
    Conflict,
    /// Read the fence, commit the hook's batch, then forward the matched batch.
    Before(Hook),
    /// Read the fence, forward the matched batch, then commit the hook's batch.
    ReadBeforeCommitAfter(Hook),
    /// Forward the matched batch, then read the fence and commit the hook's batch.
    After(Hook),
}

/// A one-shot rule: the first commit `when` matches gets `action`.
struct Rule {
    when: Box<dyn Fn(&WriteBatch) -> bool + Send + Sync>,
    action: Action,
}

#[derive(Default)]
struct Log {
    /// Every batch the committer handed `commit`, in order.
    batches: Vec<WriteBatch>,
    /// The outcome of every rival batch a hook committed.
    rivals: Vec<CommitOutcome>,
    rules: Vec<Rule>,
}

struct Recorder {
    inner: RedbMetadataStore,
    log: Mutex<Log>,
}

impl Recorder {
    /// A fresh store whose fence reads `cursor:0`.
    fn new() -> Self {
        let recorder = Self {
            inner: RedbMetadataStore::in_memory().unwrap(),
            log: Mutex::new(Log::default()),
        };
        recorder.set_fence(cursor(0));
        recorder
    }

    fn rule(&self, when: impl Fn(&WriteBatch) -> bool + Send + Sync + 'static, action: Action) {
        self.log.lock().unwrap().rules.push(Rule {
            when: Box::new(when),
            action,
        });
    }

    fn batches(&self) -> Vec<WriteBatch> {
        self.log.lock().unwrap().batches.clone()
    }

    fn rivals(&self) -> Vec<CommitOutcome> {
        self.log.lock().unwrap().rivals.clone()
    }

    fn clear_log(&self) {
        let mut log = self.log.lock().unwrap();
        log.batches.clear();
        log.rivals.clear();
    }

    /// Commit straight to the inner store, unlogged: a fixture step or another writer.
    fn direct(&self, batch: WriteBatch) {
        let outcome = pollster::block_on(self.inner.commit(batch)).unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);
    }

    /// Set the fence directly, as a new attempt's own fence would leave it.
    fn set_fence(&self, value: Bytes) {
        self.direct(WriteBatch::new().put(FENCE, value));
    }

    fn get(&self, key: &[u8]) -> Option<Bytes> {
        pollster::block_on(self.inner.get(key)).unwrap()
    }

    /// Every key and value in the store.
    fn snapshot(&self) -> Vec<(Vec<u8>, Bytes)> {
        pollster::block_on(self.inner.scan(b"")).unwrap()
    }

    fn keys_under(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let rows = pollster::block_on(self.inner.scan(prefix)).unwrap();
        rows.into_iter().map(|(key, _)| key).collect()
    }

    /// What a rollback's drain does once its fence move committed: delete the whole range.
    fn drain(&self, group: &SegmentGroup) {
        let mut batch = WriteBatch::new();
        for key in self.keys_under(&seg_range_prefix(group)) {
            batch = batch.delete(key);
        }
        self.direct(batch);
    }

    async fn rival(&self, hook: &Hook) -> Result<WriteBatch> {
        Ok(hook(self.inner.get(FENCE).await?))
    }

    async fn commit_rival(&self, batch: WriteBatch) -> Result<()> {
        let outcome = self.inner.commit(batch).await?;
        self.log.lock().unwrap().rivals.push(outcome);
        Ok(())
    }
}

#[async_trait]
impl MetadataStore for Recorder {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let action = {
            let mut log = self.log.lock().unwrap();
            log.batches.push(batch.clone());
            let matched = log.rules.iter().position(|rule| (rule.when)(&batch));
            matched.map(|at| log.rules.remove(at).action)
        };
        match action {
            None => self.inner.commit(batch).await,
            Some(Action::Conflict) => Ok(CommitOutcome::Conflict),
            Some(Action::Before(hook)) => {
                let rival = self.rival(&hook).await?;
                self.commit_rival(rival).await?;
                self.inner.commit(batch).await
            }
            Some(Action::ReadBeforeCommitAfter(hook)) => {
                let rival = self.rival(&hook).await?;
                let outcome = self.inner.commit(batch).await?;
                self.commit_rival(rival).await?;
                Ok(outcome)
            }
            Some(Action::After(hook)) => {
                let outcome = self.inner.commit(batch).await?;
                let rival = self.rival(&hook).await?;
                self.commit_rival(rival).await?;
                Ok(outcome)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn cursor(n: u32) -> Bytes {
    Bytes::from(format!("cursor:{n}"))
}

fn group(epoch: u64) -> SegmentGroup {
    SegmentGroup::new(NONCE, epoch).unwrap()
}

/// `n` distinct one-fragment chunks, ids from `base`.
fn chunks(n: usize, base: u128) -> Vec<ChunkRef> {
    (0..n)
        .map(|i| ChunkRef {
            id: base + i as u128,
            scheme: EcScheme::None,
            len: 4096 + (i as u64 % 7),
            placement: vec![(i % 3) as u64 + 1],
        })
        .collect()
}

fn meta(modified: u64) -> ObjectMeta {
    ObjectMeta {
        etag: Some("e".repeat(64)),
        content_type: Some("application/octet-stream".into()),
        modified: Some(modified),
    }
}

/// The segment contribution #658 makes: require the cursor the batch starts at, move it to
/// the one it leaves.
fn cursor_move(info: SegmentBatchInfo) -> WriteBatch {
    WriteBatch::new()
        .require(FENCE, cursor(info.first_index))
        .put(FENCE, cursor(info.written))
}

/// The flip contribution: require the fully written cursor, move the fence to `published`,
/// and write a key of the caller's own in the same batch.
fn flip_move(segments: u32) -> WriteBatch {
    WriteBatch::new()
        .require(FENCE, cursor(segments))
        .put(FENCE, Bytes::from_static(b"published"))
        .put(FLIP_MARK, Bytes::from_static(b"flipped"))
}

/// A rollback's fence move: require the value it read, move it to `aborting`.
fn rollback() -> Hook {
    Box::new(|read| {
        WriteBatch::new()
            .require(FENCE, read.expect("the fence record exists"))
            .put(FENCE, Bytes::from_static(b"aborting"))
    })
}

/// A fresh publication of `chunks` at inode `id`, named `name`, under the cursor fence, its
/// flip requiring the cursor at the count the committer plans.
fn fresh<'a>(
    id: u64,
    name: &'a str,
    epoch: u64,
    chunks: &'a [ChunkRef],
    meta: &'a ObjectMeta,
) -> Publication<'a> {
    let segments = planned_segments(chunks).unwrap_or(0);
    Publication {
        parent: ROOT,
        name,
        root: RootTarget::Fresh { id },
        group: group(epoch),
        chunks,
        meta,
        fence_key: FENCE,
        segment_contribution: &cursor_move,
        resume_from: 0,
        byte_budget: None,
        ops_budget: None,
        flip_contribution: flip_move(segments),
    }
}

/// A byte budget one full segment row fits and two do not (a full row is about 8.5 KB),
/// so the phase commits one batch per full segment.
const ONE_FULL_ROW_PER_BATCH: u64 = 10_000;

fn run(store: &Recorder, publication: &Publication<'_>) -> Result<CommitOutcome> {
    pollster::block_on(publish(store, publication))
}

fn refusal(result: Result<CommitOutcome>) -> PublicationError {
    match result {
        Ok(outcome) => panic!("expected a refusal, got {outcome:?}"),
        Err(err) => err
            .downcast_ref::<PublicationError>()
            .unwrap_or_else(|| panic!("expected a PublicationError, got {err}"))
            .clone(),
    }
}

/// Run `publication` and require a refusal that committed nothing and changed nothing.
fn refused_before_any_commit(store: &Recorder, publication: &Publication<'_>) -> PublicationError {
    let before = store.snapshot();
    store.clear_log();
    let err = refusal(run(store, publication));
    assert!(
        store.batches().is_empty(),
        "{err}: a refusal committed a batch"
    );
    assert_eq!(
        store.snapshot(),
        before,
        "{err}: a refusal changed the store"
    );
    err
}

fn root_of(store: &Recorder, id: u64) -> Option<InodeRecord> {
    store
        .get(&inode_key(id))
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
}

/// The chunk list the shared resolver answers for inode `id`.
fn resolved(store: &Recorder, id: u64) -> Vec<ChunkRef> {
    let record = root_of(store, id).expect("a root");
    let key = inode_key(id);
    let answer = pollster::block_on(resolve_chunk_map(store, &key, &record))
        .unwrap()
        .expect("a live generation");
    answer.chunks.into_owned()
}

fn puts_key(batch: &WriteBatch, key: &[u8]) -> bool {
    batch.puts.iter().any(|(k, _)| k == key)
}

fn touches_seg(batch: &WriteBatch) -> bool {
    batch.puts.iter().any(|(k, _)| k.starts_with(b"seg:"))
        || batch.deletes.iter().any(|k| k.starts_with(b"seg:"))
        || batch
            .preconditions
            .iter()
            .any(|p| p.key.starts_with(b"seg:"))
}

fn requires(batch: &WriteBatch, key: &[u8], value: &Bytes) -> bool {
    batch
        .preconditions
        .iter()
        .any(|p| p.key == key && p.expected.as_ref() == Some(value))
}

/// The encoded bytes of the **whole** transaction, measured here rather than asked of the
/// committer: every precondition's key and expected value, every put's key and value, every
/// delete's key. Its mutation bytes are the part of it without the preconditions.
fn transaction_bytes(batch: &WriteBatch) -> usize {
    let preconditions: usize = batch
        .preconditions
        .iter()
        .map(|p| p.key.len() + p.expected.as_ref().map_or(0, Bytes::len))
        .sum();
    let puts: usize = batch.puts.iter().map(|(k, v)| k.len() + v.len()).sum();
    preconditions + puts + batch.deletes.iter().map(Vec::len).sum::<usize>()
}

fn operations(batch: &WriteBatch) -> usize {
    batch.preconditions.len() + batch.puts.len() + batch.deletes.len()
}

/// Leave `publication`'s segments durable up to where the fence reads `stop`: run it and
/// answer the first batch that requires `stop` — a segment batch, or the flip — `Conflict`.
fn plant(store: &Recorder, publication: &Publication<'_>, stop: Bytes) {
    let at = stop.clone();
    store.rule(move |b| requires(b, FENCE, &at), Action::Conflict);
    assert_eq!(run(store, publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(store.get(FENCE), Some(stop));
    store.clear_log();
}

/// [`plant`] for the cursor fence, one full row per batch: segments `0..n` durable and the
/// fence at `cursor:n`.
fn plant_cursor(store: &Recorder, epoch: u64, chunks: &[ChunkRef], meta: &ObjectMeta, n: u32) {
    let mut publication = fresh(8, "planted", epoch, chunks, meta);
    publication.byte_budget = Some(ONE_FULL_ROW_PER_BATCH);
    plant(store, &publication, cursor(n));
}

// ---------------------------------------------------------------------------
// (A) Round trip
// ---------------------------------------------------------------------------

#[test]
fn a_segmented_and_flat_maps_round_trip_through_the_resolver() {
    let store = Recorder::new();
    let meta = meta(10_000);

    // Fresh key, segmented: the resolver answers the identical ordered list.
    let big = chunks(BIG, 1_000_000);
    let publication = fresh(8, "big", EPOCH, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let root = root_of(&store, 8).unwrap();
    let map = root.chunk_map.segmented().expect("a segmented root");
    assert_eq!(
        map.group(),
        &group(EPOCH),
        "the root names the group it was given"
    );
    assert_eq!(map.segment_count(), planned_segments(&big).unwrap());
    assert_eq!(root.version, 1);
    assert_eq!(resolved(&store, 8), big);

    // At most MAX_MAP_CHUNKS: a flat root, and no `seg:` record written.
    store.set_fence(cursor(0));
    store.clear_log();
    let before = store.keys_under(b"seg:");
    let small = chunks(MAX_MAP_CHUNKS as usize, 2_000_000);
    let publication = fresh(9, "small", EPOCH + 1, &small, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let root = root_of(&store, 9).unwrap();
    assert_eq!(root.chunk_map, ChunkMap::Flat(small.clone()));
    assert_eq!(root.version, 1);
    assert!(!store.batches().iter().any(touches_seg));
    assert_eq!(store.keys_under(b"seg:"), before);

    // Superseding that flat generation with a segmented one: version prior + 1, and the
    // stamp ordered after the prior's (#768) though the caller's clock reads earlier.
    store.set_fence(cursor(0));
    let prior = root_of(&store, 9).unwrap();
    let late = chunks(BIG, 3_000_000);
    let early = ObjectMeta {
        modified: Some(5_000),
        ..meta.clone()
    };
    let mut publication = fresh(9, "small", EPOCH + 2, &late, &early);
    publication.root = RootTarget::Supersede {
        id: 9,
        prior: &prior,
    };
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let root = root_of(&store, 9).unwrap();
    assert_eq!(root.version, prior.version + 1);
    assert_eq!(root.modified, Some(10_001));
    assert_eq!(
        root.chunk_map.segmented().map(|m| m.group().clone()),
        Some(group(EPOCH + 2))
    );
    assert_eq!(resolved(&store, 9), late);
}

/// `planned_segments` is the count `publish` writes, also where the byte rule closes
/// segments before the count rule does: wide refs (RS(10,4), twenty-digit placement ids)
/// plan more than `⌈chunks / MAX_SEG_CHUNKS⌉` segments, and the flip built from it
/// publishes.
#[test]
fn a_planned_segments_is_the_count_publish_writes_for_wide_refs_too() {
    let wide: Vec<ChunkRef> = (0..2 * MAX_SEG_CHUNKS as usize)
        .map(|i| ChunkRef {
            id: u128::MAX - i as u128,
            scheme: EcScheme::ReedSolomon { k: 10, m: 4 },
            len: 1 << 20,
            placement: vec![u64::MAX - i as u64; 14],
        })
        .collect();
    let planned = planned_segments(&wide).unwrap();
    assert!(
        planned > wide.len().div_ceil(MAX_SEG_CHUNKS as usize) as u32,
        "wide refs close segments early: {planned}"
    );
    let store = Recorder::new();
    let meta = meta(1);
    let publication = fresh(8, "wide", EPOCH, &wide, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let root = root_of(&store, 8).unwrap();
    assert_eq!(root.chunk_map.segmented().unwrap().segment_count(), planned);
    assert_eq!(resolved(&store, 8), wide);
    // Every segment value written fits the `V/2` budget the byte rule closes segments
    // against: wide refs fill a segment long before the count rule would close it.
    let written: Vec<usize> = store
        .batches()
        .iter()
        .flat_map(|batch| batch.puts.iter())
        .filter(|(key, _)| key.starts_with(b"seg:"))
        .map(|(_, value)| value.len())
        .collect();
    assert_eq!(written.len(), planned as usize);
    for bytes in written {
        assert!(
            bytes <= MAX_ROOT_VALUE_BYTES,
            "a segment value of {bytes} bytes, over {MAX_ROOT_VALUE_BYTES}"
        );
    }

    // The bounds: flat up to MAX_MAP_CHUNKS, and exactly MAX_ROOT_SEGMENTS full segments
    // is the largest plan.
    assert_eq!(planned_segments(&chunks(MAX_MAP_CHUNKS as usize, 1)), Ok(0));
    assert_eq!(
        planned_segments(&chunks(MAX_MAP_CHUNKS as usize + 1, 1)),
        Ok(2)
    );
    let most = MAX_ROOT_SEGMENTS * MAX_SEG_CHUNKS as usize;
    assert_eq!(
        planned_segments(&chunks(most, 1)),
        Ok(MAX_ROOT_SEGMENTS as u32)
    );
    assert_eq!(
        planned_segments(&chunks(most + 1, 1)),
        Err(PublicationError::TooManySegments {
            segments: MAX_ROOT_SEGMENTS + 1
        })
    );
}

/// The flip pins the target dirent on both branches (`0016:663`): a fresh key over a name
/// already bound, and a supersede after a rename moved the name, each lose with `Conflict`
/// and leave the dirent as it was.
#[test]
fn a_the_flip_pins_the_dirent_on_both_branches() {
    let store = Recorder::new();
    let meta = meta(1);
    let taken = dirent_key(ROOT, "taken");
    let small = chunks(10, 1);
    assert_eq!(
        run(&store, &fresh(8, "taken", EPOCH, &small, &meta)).unwrap(),
        CommitOutcome::Committed
    );
    let bound = store.get(&taken);
    assert!(bound.is_some());

    // A fresh inode 9 for the name inode 8 holds.
    store.set_fence(cursor(0));
    let big = chunks(BIG, 1);
    let publication = fresh(9, "taken", EPOCH, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(store.get(&taken), bound, "the name was rebound");
    assert_eq!(root_of(&store, 9), None);

    // A supersede of inode 8 at "taken" after a rename moved inode 8 to "moved".
    store.set_fence(cursor(0));
    let prior = root_of(&store, 8).unwrap();
    let moved = dirent_key(ROOT, "moved");
    store.direct(
        WriteBatch::new()
            .delete(taken.clone())
            .put(moved.clone(), encode(&DirentRecord { inode: 8 })),
    );
    let mut publication = fresh(8, "taken", EPOCH + 1, &big, &meta);
    publication.root = RootTarget::Supersede {
        id: 8,
        prior: &prior,
    };
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(
        root_of(&store, 8),
        Some(prior),
        "the inode now bound at another name was overwritten"
    );
    assert_eq!(store.get(&taken), None);
    assert_eq!(store.get(&moved), Some(encode(&DirentRecord { inode: 8 })));
}

/// The flip pins the target inode on both branches (`0016:663`): a supersede whose prior a
/// concurrent PUT already replaced, and a fresh key whose inode id already holds a record,
/// each lose with `Conflict` and leave that record as it was. The dirent pin cannot catch
/// either: the name still names inode 8 in the first, and the fresh name is unbound in the
/// second.
#[test]
fn a_the_flip_pins_the_inode_on_both_branches() {
    let store = Recorder::new();
    let meta = meta(1);
    let small = chunks(10, 1);
    assert_eq!(
        run(&store, &fresh(8, "obj", EPOCH, &small, &meta)).unwrap(),
        CommitOutcome::Committed
    );
    let prior = root_of(&store, 8).unwrap();

    // A concurrent PUT replaces the generation the supersede read, under the same name.
    let put = InodeRecord {
        version: prior.version + 1,
        modified: Some(2),
        ..prior.clone()
    };
    store.direct(
        WriteBatch::new()
            .require(inode_key(8), encode(&prior))
            .put(inode_key(8), encode(&put)),
    );
    store.set_fence(cursor(0));
    let big = chunks(BIG, 1);
    let mut publication = fresh(8, "obj", EPOCH + 1, &big, &meta);
    publication.root = RootTarget::Supersede {
        id: 8,
        prior: &prior,
    };
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(
        root_of(&store, 8),
        Some(put.clone()),
        "the concurrent PUT's generation was overwritten"
    );

    // A fresh key whose inode id already holds a record no dirent names.
    store.direct(WriteBatch::new().put(inode_key(9), encode(&put)));
    store.set_fence(cursor(0));
    let publication = fresh(9, "unbound", EPOCH + 2, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(
        root_of(&store, 9),
        Some(put),
        "an existing inode record was overwritten"
    );
    assert_eq!(store.get(&dirent_key(ROOT, "unbound")), None);
}

/// A map of more segments than one range-read page holds (`SEGMENT_PAGE_LIMIT`): the
/// committer's verify read pages through all of them, and the root publishes and resolves.
#[test]
fn a_a_map_over_one_page_of_segments_publishes_and_resolves() {
    let many = chunks((SEGMENT_PAGE_LIMIT + 2) * MAX_SEG_CHUNKS as usize, 1);
    let planned = planned_segments(&many).unwrap();
    assert!(
        planned as usize > SEGMENT_PAGE_LIMIT,
        "{planned} segments fit one page"
    );
    let store = Recorder::new();
    let meta = meta(1);
    let publication = fresh(8, "many", EPOCH, &many, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let root = root_of(&store, 8).unwrap();
    assert_eq!(root.chunk_map.segmented().unwrap().segment_count(), planned);
    assert_eq!(resolved(&store, 8), many);
}

// ---------------------------------------------------------------------------
// (B) Bounded batches
// ---------------------------------------------------------------------------

/// Fence values 4,000 bytes wide, all the same width, so each batch's contribution carries
/// about 8 KB — half of it a precondition's expected value — and costs the same in every
/// batch.
fn padded(n: u32) -> Bytes {
    Bytes::from(format!("{n:0>4000}"))
}

fn padded_move(info: SegmentBatchInfo) -> WriteBatch {
    WriteBatch::new()
        .require(FENCE, padded(info.first_index))
        .put(FENCE, padded(info.written))
}

/// Every batch fits the byte budget **as a whole transaction**, and no segment batch left
/// room for the next row (the split fills each batch rather than under-packing it).
///
/// The budget is set where the precondition bytes decide the split. A full row costs about
/// 8.6 KB and the contribution 8,020 bytes, so one row and the contribution are about
/// 16.6 KB and two about 25.3 KB: 23,000 bytes fits one (the last batch also takes the
/// 10-chunk tail). A committer that dropped the expected value from its count would see two
/// rows at about 21.3 KB, pack them, and commit a 25 KB transaction.
#[test]
fn b_every_batch_fits_the_byte_envelope_counting_its_preconditions() {
    const BUDGET: u64 = 23_000;
    let within_budget = |batches: &[WriteBatch]| {
        for (n, batch) in batches.iter().enumerate() {
            let bytes = transaction_bytes(batch);
            assert!(
                bytes as u64 <= BUDGET,
                "batch {n} carries {bytes} bytes, over {BUDGET}"
            );
            assert!(operations(batch) <= MAX_BATCH_OPS as usize);
        }
    };
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.segment_contribution = &padded_move;
    publication.byte_budget = Some(BUDGET);
    publication.flip_contribution = WriteBatch::new()
        .require(FENCE, padded(SEGMENTS))
        .put(FENCE, Bytes::from_static(b"published"));

    // A fresh run, which writes every row.
    let store = Recorder::new();
    store.set_fence(padded(0));
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let batches = store.batches();
    within_budget(&batches);
    let segment_batches: Vec<&WriteBatch> = batches.iter().filter(|b| touches_seg(b)).collect();
    assert!(
        segment_batches.len() >= 3,
        "the budget must force at least three segment batches, got {}",
        segment_batches.len()
    );
    for pair in segment_batches.windows(2) {
        let (key, value) = pair[1]
            .puts
            .iter()
            .find(|(k, _)| k.starts_with(b"seg:"))
            .unwrap();
        // The next row in the shape the committer charges: `require_absent` + `put`.
        let next_row = 2 * key.len() + value.len();
        assert!(
            (transaction_bytes(pair[0]) + next_row) as u64 > BUDGET,
            "a segment batch left room for the next row"
        );
    }
    let flips: Vec<usize> = (0..batches.len())
        .filter(|&n| puts_key(&batches[n], &inode_key(8)))
        .collect();
    assert_eq!(
        flips,
        vec![batches.len() - 1],
        "the root is written once, last"
    );
    assert_eq!(resolved(&store, 8), big);

    // A recovery: rows 0..2 are durable, and the recovery from 0 pins them
    // (`require(key, value)`, no put) instead of rewriting them.
    let store = Recorder::new();
    store.set_fence(padded(0));
    plant(&store, &publication, padded(2));
    store.set_fence(padded(0));
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let batches = store.batches();
    assert!(
        batches[0].puts.iter().all(|(k, _)| !k.starts_with(b"seg:")),
        "the recovery's first batch pins the durable rows"
    );
    within_budget(&batches);
}

/// The operation envelope binds on its own: at seven operations a segment batch holds two
/// rows (two operations each, plus the contribution's two) whatever the byte budget allows.
#[test]
fn b_the_operation_envelope_binds_where_bytes_do_not() {
    const OPS: u32 = 7;
    let store = Recorder::new();
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.ops_budget = Some(OPS);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);

    let batches = store.batches();
    for (n, batch) in batches.iter().enumerate() {
        let ops = operations(batch);
        assert!(
            ops <= OPS as usize,
            "batch {n} carries {ops} operations, over {OPS}"
        );
    }
    let segment_batches: Vec<&WriteBatch> = batches.iter().filter(|b| touches_seg(b)).collect();
    assert!(segment_batches.len() >= 3);
    for batch in &segment_batches[..segment_batches.len() - 1] {
        assert!(
            operations(batch) + 2 > OPS as usize,
            "a segment batch left room for the next row"
        );
    }
    assert_eq!(
        batches
            .iter()
            .filter(|b| puts_key(b, &inode_key(8)))
            .count(),
        1
    );
    assert!(puts_key(batches.last().unwrap(), &inode_key(8)));
    assert_eq!(resolved(&store, 8), big);
}

// ---------------------------------------------------------------------------
// (C) Refusals precede durability
// ---------------------------------------------------------------------------

#[test]
fn c_every_deterministic_refusal_leaves_zero_commits() {
    let big = chunks(BIG, 1);
    let too_many = chunks(MAX_ROOT_SEGMENTS * MAX_MAP_CHUNKS as usize + 1, 1);
    let meta = meta(1);
    let seg0 = seg_key(&group(EPOCH), 0).unwrap();
    let seg0_rendered = String::from_utf8(seg0.clone()).unwrap();
    let at = |first_index| Phase::Segments { first_index };

    fn unfenced(info: SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new().put(FENCE, cursor(info.written))
    }
    fn deletes_seg0(info: SegmentBatchInfo) -> WriteBatch {
        let seg0 = seg_key(&group(EPOCH), 0).unwrap();
        cursor_move(info).delete(seg0)
    }
    fn deletes_fence(info: SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new()
            .require(FENCE, cursor(info.first_index))
            .delete(FENCE)
    }
    /// Leaves a value the next one-segment batch does not require.
    fn skips_a_cursor(info: SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new()
            .require(FENCE, cursor(info.first_index))
            .put(FENCE, cursor(info.written + 1))
    }
    /// One-segment batches agree with each other, a wider batch leaves something else.
    fn wide_batches_disagree(info: SegmentBatchInfo) -> WriteBatch {
        let left = if info.written == info.first_index + 1 {
            cursor(info.written)
        } else {
            Bytes::from_static(b"skipped")
        };
        WriteBatch::new()
            .require(FENCE, cursor(info.first_index))
            .put(FENCE, left)
    }

    type Edit<'a> = Box<dyn Fn(&mut Publication<'a>) + 'a>;
    let cases: Vec<(Edit<'_>, PublicationError)> = vec![
        (
            Box::new(|p| p.chunks = &too_many),
            PublicationError::TooManySegments {
                segments: MAX_ROOT_SEGMENTS + 1,
            },
        ),
        (
            Box::new(|p| p.resume_from = SEGMENTS + 1),
            PublicationError::ResumePastPlan {
                resume_from: SEGMENTS + 1,
                segments: SEGMENTS,
            },
        ),
        (
            Box::new(|p| p.byte_budget = Some(5_000)),
            PublicationError::BatchOverBytes {
                phase: at(0),
                bytes: 0,
                budget: 5_000,
            },
        ),
        (
            Box::new(|p| p.ops_budget = Some(3)),
            PublicationError::BatchOverOps {
                phase: at(0),
                ops: 4,
                budget: 3,
            },
        ),
        (
            Box::new(|p| {
                p.byte_budget = Some(60_000);
                p.flip_contribution =
                    flip_move(SEGMENTS).put(b"test:big".to_vec(), vec![0u8; 70_000]);
            }),
            PublicationError::BatchOverBytes {
                phase: Phase::Flip,
                bytes: 0,
                budget: 60_000,
            },
        ),
        (
            Box::new(|p| {
                let mut flip = flip_move(SEGMENTS);
                for n in 0..600 {
                    flip = flip.put(format!("test:op:{n}").into_bytes(), Bytes::new());
                }
                p.flip_contribution = flip;
            }),
            PublicationError::BatchOverOps {
                phase: Phase::Flip,
                ops: 607,
                budget: 500,
            },
        ),
        (
            Box::new(|p| p.segment_contribution = &unfenced),
            PublicationError::Unfenced { phase: at(0) },
        ),
        (
            Box::new(|p| p.segment_contribution = &deletes_fence),
            PublicationError::FenceDeleted { phase: at(0) },
        ),
        (
            Box::new(|p| p.segment_contribution = &skips_a_cursor),
            PublicationError::FenceOffCursor { phase: at(1) },
        ),
        (
            Box::new(|p| p.segment_contribution = &wide_batches_disagree),
            PublicationError::FenceOffCursor { phase: at(0) },
        ),
        (
            Box::new(|p| p.flip_contribution = flip_move(SEGMENTS - 1)),
            PublicationError::FenceOffCursor { phase: Phase::Flip },
        ),
        (
            Box::new(|p| {
                p.flip_contribution = WriteBatch::new()
                    .require(FENCE, cursor(SEGMENTS))
                    .put(FENCE, cursor(SEGMENTS));
            }),
            PublicationError::FenceNotMoved,
        ),
        (
            // Deleting a key of the caller's own is not a move of the fence.
            Box::new(|p| {
                p.flip_contribution = WriteBatch::new()
                    .require(FENCE, cursor(SEGMENTS))
                    .delete(b"test:unrelated".to_vec());
            }),
            PublicationError::FenceNotMoved,
        ),
        (
            // The flip's deletes count toward its bytes: about 51 KB without them, 63 KB
            // with them.
            Box::new(|p| {
                p.byte_budget = Some(60_000);
                p.flip_contribution = flip_move(SEGMENTS)
                    .put(b"test:big".to_vec(), vec![0u8; 50_000])
                    .delete(format!("test:del:{}", "a".repeat(6_000)).into_bytes())
                    .delete(format!("test:del:{}", "b".repeat(6_000)).into_bytes());
            }),
            PublicationError::BatchOverBytes {
                phase: Phase::Flip,
                bytes: 0,
                budget: 60_000,
            },
        ),
        (
            // ...and toward its operations: the root's four, `flip_move`'s three, four
            // deletes.
            Box::new(|p| {
                p.ops_budget = Some(10);
                let mut flip = flip_move(SEGMENTS);
                for n in 0..4 {
                    flip = flip.delete(format!("test:del:{n}").into_bytes());
                }
                p.flip_contribution = flip;
            }),
            PublicationError::BatchOverOps {
                phase: Phase::Flip,
                ops: 11,
                budget: 10,
            },
        ),
        (
            Box::new(|p| p.segment_contribution = &deletes_seg0),
            PublicationError::OwnedKey {
                phase: at(0),
                key: seg0_rendered.clone(),
            },
        ),
        (
            Box::new(|p| p.flip_contribution = flip_move(SEGMENTS).delete(seg0.clone())),
            PublicationError::OwnedKey {
                phase: Phase::Flip,
                key: seg0_rendered.clone(),
            },
        ),
        (
            Box::new(|p| {
                p.flip_contribution = flip_move(SEGMENTS).require_absent(dirent_key(ROOT, "big"));
            }),
            PublicationError::OwnedKey {
                phase: Phase::Flip,
                key: String::from_utf8_lossy(&dirent_key(ROOT, "big")).into_owned(),
            },
        ),
        (
            Box::new(|p| p.flip_contribution = flip_move(SEGMENTS).put(inode_key(8), Bytes::new())),
            PublicationError::OwnedKey {
                phase: Phase::Flip,
                key: String::from_utf8_lossy(&inode_key(8)).into_owned(),
            },
        ),
        (
            Box::new(|p| {
                p.flip_contribution = flip_move(SEGMENTS).put(FLIP_MARK, Bytes::new());
            }),
            PublicationError::DuplicateMutation {
                phase: Phase::Flip,
                key: String::from_utf8_lossy(FLIP_MARK).into_owned(),
            },
        ),
        (
            Box::new(|p| {
                p.flip_contribution =
                    flip_move(SEGMENTS).put(b"test:huge".to_vec(), vec![0u8; 100_001]);
            }),
            PublicationError::ValueOverCeiling {
                phase: Phase::Flip,
                key: "test:huge".into(),
                bytes: 100_001,
            },
        ),
    ];
    for (edit, expected) in cases {
        let store = Recorder::new();
        let mut publication = fresh(8, "big", EPOCH, &big, &meta);
        edit(&mut publication);
        let err = refused_before_any_commit(&store, &publication);
        // A byte refusal is matched on where and against what; its size is the
        // committer's own measure.
        let err = match err {
            PublicationError::BatchOverBytes { phase, budget, .. } => {
                PublicationError::BatchOverBytes {
                    phase,
                    bytes: 0,
                    budget,
                }
            }
            other => other,
        };
        assert_eq!(err, expected);
    }
}

/// A flip that deletes the fence record moves it as surely as one that puts a new value:
/// no rollback can require a record that is gone. The publication commits.
#[test]
fn c_a_flip_that_deletes_the_fence_record_moves_it() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.flip_contribution = WriteBatch::new()
        .require(FENCE, cursor(SEGMENTS))
        .delete(FENCE);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    assert_eq!(store.get(FENCE), None);
    assert_eq!(resolved(&store, 8), big);
}

// ---------------------------------------------------------------------------
// (D) A resume verifies the whole range
// ---------------------------------------------------------------------------

/// Plant one row the plan does not have under the group's range, then publish: the range
/// read refuses it before any commit, and no root is written.
fn refused_over_planted_row(key: Vec<u8>) -> PublicationError {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    store.direct(WriteBatch::new().put(key, Bytes::from_static(b"{}")));
    let err = refused_before_any_commit(&store, &fresh(8, "big", EPOCH, &big, &meta));
    assert_eq!(root_of(&store, 8), None);
    err
}

/// A durable segment record one past the plan's last index, as a longer plan at this epoch
/// would leave: a root over the range would not resolve.
#[test]
fn d_a_durable_row_past_the_plan_refuses_the_publish() {
    let stranded = seg_key(&group(EPOCH), SEGMENTS).unwrap();
    assert_eq!(
        refused_over_planted_row(stranded),
        PublicationError::StrandedRow { index: SEGMENTS }
    );
}

/// A key under the group's `seg:<nonce>:<epoch>:` prefix that is not a segment key.
#[test]
fn d_a_foreign_key_in_the_groups_range_refuses_the_publish() {
    let mut foreign = seg_range_prefix(&group(EPOCH));
    foreign.extend_from_slice(b"junk");
    let key = String::from_utf8(foreign.clone()).unwrap();
    assert_eq!(
        refused_over_planted_row(foreign),
        PublicationError::ForeignRow { key }
    );
}

#[test]
fn d1_a_record_below_the_cursor_missing_or_rewritten_refuses_the_publish() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let damage: [(u32, Option<Bytes>); 2] = [(1, None), (2, Some(Bytes::from_static(b"{}")))];
    for (index, replacement) in damage {
        let store = Recorder::new();
        plant_cursor(&store, EPOCH, &big, &meta, 3);
        let key = seg_key(&group(EPOCH), index).unwrap();
        store.direct(match &replacement {
            None => WriteBatch::new().delete(key),
            Some(bytes) => WriteBatch::new().put(key, bytes.clone()),
        });

        let mut publication = fresh(8, "big", EPOCH, &big, &meta);
        publication.resume_from = 3;
        let err = refusal(run(&store, &publication));
        let expected = match replacement {
            None => PublicationError::SegmentNotDurable { index },
            Some(_) => PublicationError::SegmentMismatch { index },
        };
        assert_eq!(err, expected);
        assert!(store.batches().is_empty(), "refused before any commit");
        assert_eq!(root_of(&store, 8), None, "no root names the group");
    }
}

/// With `resume_from = 2`, a record lost right after the LAST segment commit returns is
/// caught by the check before the flip, at an index past the cursor (which a check of
/// `0..resume_from` alone would pass) and at one below it (which a check of
/// `resume_from..` alone would pass).
#[test]
fn d2_a_record_lost_after_the_last_segment_commit_refuses_the_flip() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    for lost in [3, 0] {
        let store = Recorder::new();
        plant_cursor(&store, EPOCH, &big, &meta, 2);
        let last = seg_key(&group(EPOCH), SEGMENTS - 1).unwrap();
        store.rule(
            move |b| b.preconditions.iter().any(|p| p.key == last),
            Action::After(Box::new(move |_| {
                WriteBatch::new().delete(seg_key(&group(EPOCH), lost).unwrap())
            })),
        );
        let mut publication = fresh(8, "big", EPOCH, &big, &meta);
        publication.resume_from = 2;
        let err = refusal(run(&store, &publication));
        assert_eq!(err, PublicationError::SegmentNotDurable { index: lost });
        assert_eq!(store.rivals(), vec![CommitOutcome::Committed]);
        assert!(
            !store.batches().iter().any(|b| puts_key(b, &inode_key(8))),
            "no flip commit was handed to the store"
        );
        assert_eq!(root_of(&store, 8), None, "no root names the group");
    }
}

#[test]
fn d3_a_fully_durable_range_publishes_without_rewriting_it() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    plant_cursor(&store, EPOCH, &big, &meta, SEGMENTS);
    let durable = store.snapshot();

    store.set_fence(cursor(0));
    let publication = fresh(8, "big", EPOCH, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    for batch in store.batches() {
        assert!(
            !batch.puts.iter().any(|(k, _)| k.starts_with(b"seg:")),
            "a durable, identical segment was rewritten"
        );
    }
    let segs = |rows: &[(Vec<u8>, Bytes)]| -> Vec<(Vec<u8>, Bytes)> {
        rows.iter()
            .filter(|(k, _)| k.starts_with(b"seg:"))
            .cloned()
            .collect()
    };
    assert_eq!(segs(&store.snapshot()), segs(&durable));
    assert_eq!(resolved(&store, 8), big);
}

// ---------------------------------------------------------------------------
// (E) The flip carries the caller's batch atomically
// ---------------------------------------------------------------------------

#[test]
fn e_the_flip_commits_the_callers_contribution_with_the_root_or_neither() {
    let big = chunks(BIG, 1);
    let meta = meta(1);

    let store = Recorder::new();
    let publication = fresh(8, "big", EPOCH, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let flip = store.batches().pop().unwrap();
    assert!(puts_key(&flip, &inode_key(8)) && puts_key(&flip, FLIP_MARK));
    assert!(store.get(FLIP_MARK).is_some());

    let store = Recorder::new();
    store.rule(|b| puts_key(b, &inode_key(8)), Action::Conflict);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Conflict);
    assert_eq!(root_of(&store, 8), None);
    assert_eq!(
        store.get(FLIP_MARK),
        None,
        "the caller's put landed without the root"
    );
}

// ---------------------------------------------------------------------------
// (F) Epoch disjointness, fence cycles, and the rollback race
// ---------------------------------------------------------------------------

#[test]
fn f_a_drained_rolled_back_epoch_never_touches_the_published_one() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    // Epoch E writes its segments and loses its flip: a rolled-back attempt.
    plant_cursor(&store, EPOCH, &big, &meta, SEGMENTS);
    let rolled_back = store.keys_under(&seg_range_prefix(&group(EPOCH)));
    assert_eq!(rolled_back.len(), SEGMENTS as usize);

    // Epoch E' > E publishes, over a part set that changed meanwhile.
    store.set_fence(cursor(0));
    let changed = chunks(BIG, 7_000_000);
    let publication = fresh(8, "big", EPOCH + 1, &changed, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    let live = store.keys_under(&seg_range_prefix(&group(EPOCH + 1)));
    assert!(
        live.iter().all(|k| !rolled_back.contains(k)),
        "the ranges overlap"
    );

    // The stale epoch-E obligation drains AFTER the publication.
    store.drain(&group(EPOCH));
    assert!(store
        .keys_under(&seg_range_prefix(&group(EPOCH)))
        .is_empty());
    assert_eq!(store.keys_under(&seg_range_prefix(&group(EPOCH + 1))), live);
    assert_eq!(resolved(&store, 8), changed);
}

/// A fence that returns to a value it left is refused before any commit: across the
/// segment phase and the flip (`A → B → A`), at a cursor position only a resumed attempt's
/// durable prefix held, and at one this call's own split never stops at.
#[test]
fn f_a_fence_that_returns_to_a_value_it_left_is_refused() {
    /// `A` before the first segment, `B` from then on.
    fn a_to_b(info: SegmentBatchInfo) -> WriteBatch {
        let at = |n| Bytes::from_static(if n == 0 { b"A" } else { b"B" });
        WriteBatch::new()
            .require(FENCE, at(info.first_index))
            .put(FENCE, at(info.written))
    }
    /// The cursor fence reduced to its parity: `0 → 1 → 0 → …`.
    fn parity(info: SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new()
            .require(FENCE, cursor(info.first_index % 2))
            .put(FENCE, cursor(info.written % 2))
    }
    let big = chunks(BIG, 1);
    let meta = meta(1);

    // A → B over the phase, then a flip B → A.
    let store = Recorder::new();
    store.set_fence(Bytes::from_static(b"A"));
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.segment_contribution = &a_to_b;
    publication.flip_contribution = WriteBatch::new()
        .require(FENCE, Bytes::from_static(b"B"))
        .put(FENCE, Bytes::from_static(b"A"));
    let err = refused_before_any_commit(&store, &publication);
    assert_eq!(err, PublicationError::FenceCycled { phase: Phase::Flip });
    assert_eq!(
        err.to_string(),
        "the root flip: the fence record returns to a value it already left"
    );

    // A resume from 3 whose flip moves the fence back to `cursor:1`, a value only the
    // durable prefix held.
    let store = Recorder::new();
    plant_cursor(&store, EPOCH, &big, &meta, 3);
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.resume_from = 3;
    publication.flip_contribution = WriteBatch::new()
        .require(FENCE, cursor(SEGMENTS))
        .put(FENCE, cursor(1));
    let err = refused_before_any_commit(&store, &publication);
    assert_eq!(err, PublicationError::FenceCycled { phase: Phase::Flip });

    // One batch at the default budget holds the whole phase (`0 → 1`), but a one-row
    // split would go `0 → 1 → 0`: refused whatever split this call happens to use.
    let store = Recorder::new();
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.segment_contribution = &parity;
    publication.flip_contribution = WriteBatch::new()
        .require(FENCE, cursor(SEGMENTS % 2))
        .put(FENCE, Bytes::from_static(b"published"));
    let err = refused_before_any_commit(&store, &publication);
    assert_eq!(
        err,
        PublicationError::FenceCycled {
            phase: Phase::Segments { first_index: 1 }
        }
    );
    assert_eq!(
        err.to_string(),
        "the segment batch starting at segment 1: the fence record returns to a value it already left"
    );
}

/// The fence verdict depends on the hook, the plan and the flip, never on how far an earlier
/// attempt got or how it split. With fence values `A C D E F B` at cursor positions `0..=5`:
/// a flip `B → C` is refused on the **first** attempt, before anything is durable (not only
/// on a recovery, after); a flip `B → G` publishes on a recovery whose earlier attempt
/// committed the whole phase as one `A → B` batch.
#[test]
fn f_the_fence_verdict_is_the_same_on_every_attempt() {
    fn letter(k: u32) -> Bytes {
        Bytes::copy_from_slice(&b"ACDEFB"[k as usize..=k as usize])
    }
    fn lettered(info: SegmentBatchInfo) -> WriteBatch {
        WriteBatch::new()
            .require(FENCE, letter(info.first_index))
            .put(FENCE, letter(info.written))
    }
    let flip_to = |to: &'static [u8]| {
        WriteBatch::new()
            .require(FENCE, Bytes::from_static(b"B"))
            .put(FENCE, Bytes::from_static(to))
    };
    let big = chunks(BIG, 1);
    let meta = meta(1);

    let store = Recorder::new();
    store.set_fence(Bytes::from_static(b"A"));
    let mut publication = fresh(8, "big", EPOCH, &big, &meta);
    publication.segment_contribution = &lettered;
    publication.flip_contribution = flip_to(b"C");
    let err = refused_before_any_commit(&store, &publication);
    assert_eq!(err, PublicationError::FenceCycled { phase: Phase::Flip });

    publication.flip_contribution = flip_to(b"G");
    plant(&store, &publication, Bytes::from_static(b"B"));
    publication.resume_from = SEGMENTS;
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    assert_eq!(store.get(FENCE), Some(Bytes::from_static(b"G")));
    assert_eq!(resolved(&store, 8), big);
}

#[test]
fn f_r_a_rollback_whose_fence_move_lands_first_loses_the_flip_and_drains_nothing_published() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    store.rule(|b| puts_key(b, &inode_key(8)), Action::Before(rollback()));
    let publication = fresh(8, "big", EPOCH, &big, &meta);
    let outcome = run(&store, &publication).unwrap();
    assert_eq!(store.rivals(), vec![CommitOutcome::Committed]);

    // The rollback's fence move committed, so its drain may now delete the range.
    store.drain(&group(EPOCH));
    assert_eq!(root_of(&store, 8), None, "a root names a drained range");
    assert_eq!(outcome, CommitOutcome::Conflict);
    assert_eq!(store.get(&dirent_key(ROOT, "big")), None);
    assert_eq!(store.get(FENCE), Some(Bytes::from_static(b"aborting")));
}

#[test]
fn f_p_a_flip_that_lands_first_makes_the_rollbacks_fence_move_lose() {
    let big = chunks(BIG, 1);
    let meta = meta(1);
    let store = Recorder::new();
    store.rule(
        |b| puts_key(b, &inode_key(8)),
        Action::ReadBeforeCommitAfter(rollback()),
    );
    let publication = fresh(8, "big", EPOCH, &big, &meta);
    assert_eq!(run(&store, &publication).unwrap(), CommitOutcome::Committed);
    assert_eq!(store.rivals(), vec![CommitOutcome::Conflict]);

    // The rollback lost, so it deletes nothing; the published root resolves whole.
    assert_eq!(store.get(FENCE), Some(Bytes::from_static(b"published")));
    assert_eq!(
        store.keys_under(&seg_range_prefix(&group(EPOCH))).len(),
        SEGMENTS as usize
    );
    assert_eq!(resolved(&store, 8), big);
}

/// The fixture plans the segment count the legs assume, and its refs are narrow enough that
/// the count rule, not the byte rule, closes each segment.
#[test]
fn the_fixture_plans_the_segment_count_the_legs_assume() {
    let big = chunks(BIG, 1);
    assert_eq!(planned_segments(&big), Ok(SEGMENTS));
    assert_eq!(BIG.div_ceil(MAX_SEG_CHUNKS as usize), SEGMENTS as usize);
    assert!(encode(&big[0]).len() * MAX_SEG_CHUNKS as usize <= 25_000);
}
