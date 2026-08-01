//! The shared chunk-map resolver (issue #649, proposal 0016 decision 7(e)/(h)), proved
//! through the **base-visible** read entries — [`read_object`] / [`read_path`] — over
//! objects seeded as raw `seg:` records plus a segmented root, **never via a committer**
//! (this slice ships no producer): a genuine flat write (real fragments, via
//! `wyrd_core::write`) re-spelled as segments by a raw batch, so `.chunk_map` is
//! segmented before a single read touches it.
//!
//! RED on this slice's base (`crates/core/src/metadata.rs` and `read.rs` reverted): a
//! segmented root still **decodes** (#648's contribution, so every fixture still builds)
//! but no read can resolve one — every case below fails as an assertion, and this file
//! still compiles because every symbol it imports is base-visible from #648. Nothing this
//! patch adds is named here: criteria (2) and (3) are observed through the read path plus
//! the fake store's request log, never by calling the resolver directly.
//!
//! Which store backs which criterion:
//! * **(1)** runs over the real redb backend — bytes compared against the flat
//!   equivalent of the same payload need no instrumentation.
//! * **(2)/(3)** run over [`FakeStore`], a **self-contained** `MetadataStore` that owns
//!   its rows and answers every read from them. It wraps no backend on purpose: a double
//!   that delegated could only report what it forwarded, never what the resolver asked
//!   for, and what the resolver asked for **is** the oracle for "the work a read demands
//!   is bounded by the reader, not by the record".

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{
    encode, inode_key, seg_group_prefix, seg_key, seg_range_prefix, ChunkMap, ChunkMapError,
    EcScheme, InodeRecord, InodeState, SegmentGroup, SegmentRecord, SegmentRef, SegmentedMap,
    MAX_ROOT_SEGMENTS, MAX_VALUE_BYTES,
};
use wyrd_core::read::{read_object, read_path};
use wyrd_core::write;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Result, ScanPage, WriteBatch};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 4;
/// A 32-lowercase-hex nonce, exactly `SEG_NONCE_HEX_LEN` characters.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
/// A second, distinct nonce — a group that must never be touched resolving the first.
const OTHER_NONCE: &str = "fedcba9876543210fedcba9876543210";
/// The epoch every live fixture's group is keyed by.
const EPOCH: u64 = 5;

/// The page size the resolver asks a group's range for: **the reader's own constant**,
/// spelled here as a literal because the property is that it is *not* a function of the
/// record. Pinned rather than imported: a test that read the number out of the code
/// under test would pass however that number moved, including all the way up to "the
/// root's claim", which is exactly the thing criterion 2(c) forbids.
const READER_PAGE_LIMIT: usize = 128;

/// How many root `get`s ONE clean segmented resolve costs, whatever the root's table
/// names: the read's own root read, plus the ONE re-read that settles whether the
/// generation just resolved is still live. A constant of the reader's — asserted on a
/// two-segment object *and* on a `MAX_ROOT_SEGMENTS` one, because "every get is the
/// root" alone would still allow one re-ask per segment or per page.
const CLEAN_RESOLVE_ROOT_GETS: usize = 2;

// ---------------------------------------------------------------------------
// Fixtures — real chunks, hand-written `seg:` rows.
// ---------------------------------------------------------------------------

fn chunk_store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = FsChunkStore::open(dir.path()).expect("fs chunk store");
    (chunks, dir)
}

/// `n` deterministic bytes from `base` — two payloads built with different `base` values
/// share no byte, so *which* generation answered a read is decidable from the bytes.
fn payload(n: u32, base: u32) -> Vec<u8> {
    (0..n).map(|i| ((i + base) % 251) as u8).collect()
}

/// Plan a real object and write its fragments — no metadata store involved, so the same
/// helper serves the redb fixtures and the fake-store ones.
async fn fragments_for(chunks: &FsChunkStore, data: &[u8], id_base: u128) -> write::WritePlan {
    let mut next = id_base;
    let plan = write::plan_write(data, CHUNK, EcScheme::None, || {
        next += 1;
        next
    })
    .expect("plan");
    write::write_fragments(chunks, &plan)
        .await
        .expect("fan out");
    plan
}

/// Plan, fan out and commit a genuine **flat** object at `inode_id`/`name` through the
/// ordinary write path — exactly as every pre-existing flat object is built.
async fn write_flat(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    inode_id: u64,
    name: &str,
    data: &[u8],
    id_base: u128,
) -> write::WritePlan {
    let plan = fragments_for(chunks, data, id_base).await;
    write::intent(meta, &plan, NOW + TTL).await.expect("intent");
    let outcome = write::commit_create(meta, ROOT, name, inode_id, &plan, NOW)
        .await
        .expect("commit");
    assert_eq!(outcome, CommitOutcome::Committed);
    write::release(meta, &plan).await.expect("release");
    plan
}

fn sref(index: u32, byte_offset: u64, byte_len: u64) -> SegmentRef {
    SegmentRef {
        index,
        byte_offset,
        byte_len,
    }
}

/// The **correct** `n`-segment spelling of `plan`: the root's segment table and the
/// records that satisfy it. Every bent fixture below starts here and changes exactly one
/// thing, so what a case differs by *is* the property it binds.
fn split(plan: &write::WritePlan, n: usize) -> (Vec<SegmentRef>, Vec<SegmentRecord>) {
    let chunks = plan.chunk_refs();
    assert!(n >= 2 && n <= chunks.len(), "fixture needs a real split");
    let (mut refs, mut records, mut offset) = (Vec::new(), Vec::new(), 0u64);
    for (index, part) in chunks.chunks(chunks.len().div_ceil(n)).enumerate() {
        let record = SegmentRecord::new(part.to_vec(), offset).expect("segment record");
        refs.push(sref(index as u32, offset, record.byte_len()));
        offset += record.byte_len();
        records.push(record);
    }
    (refs, records)
}

/// One raw `seg:` row: the canonical key for `index` and the encoded record.
fn row(group: &SegmentGroup, index: u32, record: &SegmentRecord) -> (Vec<u8>, Bytes) {
    (
        seg_key(group, index).expect("addressable index"),
        encode(record),
    )
}

fn rows_of(group: &SegmentGroup, records: &[SegmentRecord]) -> Vec<(Vec<u8>, Bytes)> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| row(group, index as u32, record))
        .collect()
}

/// A committed root's encoded bytes over `refs` — the only inode shape any fixture here
/// publishes. `size` is the table's own span, so an object's size is never what a case
/// is about.
fn segmented_root(group: &SegmentGroup, refs: Vec<SegmentRef>, version: u64) -> Bytes {
    let table = SegmentedMap::new(group.clone(), refs).expect("segment table");
    encode(&InodeRecord {
        size: table.span(),
        chunk_map: ChunkMap::Segmented(table),
        state: InodeState::Committed,
        version,
        ..Default::default()
    })
}

fn flat_root(plan: &write::WritePlan, version: u64) -> Bytes {
    encode(&InodeRecord {
        size: plan.size,
        chunk_map: plan.chunk_refs().into(),
        state: InodeState::Committed,
        version,
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// The self-contained fake store: its request log is the oracle for (2) and (3).
// ---------------------------------------------------------------------------

/// One recorded request: which channel it came through, the key or prefix it was about,
/// and — for a page — the cursor it was anchored after and the limit it asked for.
///
/// Every channel is recorded, not only the paged one: a resolver that reached another
/// generation's records with a direct `get`, or walked the whole `seg:` namespace with
/// one unpaged `scan`, would be invisible to a log that watched paged prefixes alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    channel: Channel,
    target: Vec<u8>,
    after: Option<Vec<u8>>,
    limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Channel {
    Get,
    Scan,
    Page,
}

/// When a [`Pending`] mutation lands — the two windows a resolve can be retired in
/// (`0016:2452-2462`: the root always moves first, its records are deleted after).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum When {
    /// The moment a root read returns: the reader holds the old root, the store already
    /// holds the new one. The only window a resolve refused *before* any range read has.
    RootRead,
    /// At a `seg:` page: the root moves on (or is deleted) while this resolve is in
    /// flight.
    SegPage,
}

/// Rows a fixture writes over the store's own map: `Some` puts, `None` deletes.
type Mutation = Vec<(Vec<u8>, Option<Bytes>)>;

/// A mutation the store applies to itself mid-resolve.
struct Pending {
    when: When,
    rows: Mutation,
}

/// A `MetadataStore` that owns its rows: `get`/`scan`/`scan_page` are answered from its
/// own ordered map, and every request is recorded. It delegates to nothing.
#[derive(Default)]
struct FakeStore {
    rows: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    log: Mutex<Vec<Request>>,
    pending: Mutex<VecDeque<Pending>>,
    /// Hand every `seg:` page back **reversed** — the paging contract's byte-order clause
    /// broken on purpose, so a chunk order that still comes out right can only have come
    /// from the resolver's own parse of the key.
    reverse_pages: bool,
    /// Rows handed back on every `seg:` page **on top of** the ones the asked-for prefix
    /// actually matched — a backend whose prefix handling leaks a neighbouring range into
    /// an answer. Deliberate, like [`FakeStore::reverse_pages`]: it is the one fault a
    /// resolver that trusted the seam's filtering could not survive, so the fake has to be
    /// able to commit it. The page stays far inside `limit` either way (three rows at a
    /// 128-row bound), so what a case built on this bends is *which range answered*, not
    /// how much of it.
    bleed: Vec<(Vec<u8>, Bytes)>,
}

impl FakeStore {
    /// Seed rows directly — no write path, no CAS: this slice ships no committer, so
    /// every segmented fixture in the tree is written by hand.
    fn seed(&self, rows: impl IntoIterator<Item = (Vec<u8>, Bytes)>) -> &Self {
        let mut owned = self.rows.lock().unwrap();
        for (key, value) in rows {
            owned.insert(key, value);
        }
        drop(owned);
        self
    }

    /// Queue a mutation the store applies to itself at `when`, once.
    fn retire_at(&self, when: When, rows: Mutation) -> &Self {
        self.pending
            .lock()
            .unwrap()
            .push_back(Pending { when, rows });
        self
    }

    /// How many queued mutations were never reached — how much of a scripted campaign the
    /// reader stopped short of, which is how a case reads the reader's own restart budget
    /// off its behaviour instead of pinning a number the resolver owns.
    fn pending_left(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Apply the next queued mutation if it is due at `when`.
    fn fire(&self, when: When) {
        let due = {
            let mut pending = self.pending.lock().unwrap();
            match pending.front() {
                Some(next) if next.when == when => pending.pop_front(),
                _ => None,
            }
        };
        let Some(due) = due else { return };
        let mut rows = self.rows.lock().unwrap();
        for (key, value) in due.rows {
            match value {
                Some(value) => rows.insert(key, value),
                None => rows.remove(&key),
            };
        }
    }

    fn record(&self, channel: Channel, target: &[u8], after: Option<&[u8]>, limit: usize) {
        self.log.lock().unwrap().push(Request {
            channel,
            target: target.to_vec(),
            after: after.map(<[u8]>::to_vec),
            limit,
        });
    }

    fn log(&self) -> Vec<Request> {
        self.log.lock().unwrap().clone()
    }

    /// Every request that came through `channel`.
    fn on(&self, channel: Channel) -> Vec<Request> {
        self.log()
            .into_iter()
            .filter(|request| request.channel == channel)
            .collect()
    }

    /// `channel` was never used at all. Two properties ride on this one assertion: a
    /// resolve refused on the root's own claim spends **no** range read (`Channel::Page`),
    /// and no resolve ever takes an unpaged `scan` (`Channel::Scan`) — which is
    /// complete-or-fail-loud at `SCAN_CAP` (`crates/traits/src/lib.rs:286`), so a damaged
    /// range would cost the caller the whole namespace or the whole call, neither of which
    /// per-object containment can catch.
    fn assert_never_used(&self, channel: Channel, why: &str) {
        let seen = self.on(channel);
        assert!(seen.is_empty(), "{why}: {seen:?}");
    }

    /// Every key this store was asked for **by key**, asserting each was `inode_id`'s own
    /// root; hands back how many.
    ///
    /// A whitelist, not a blacklist, and that is the point. Criterion (2) is "the root
    /// **plus only** the group's range", so any *other* metadata key a resolve consults —
    /// a sibling object's root, a dirent, an index — is out of budget even though it is no
    /// `seg:` key and touches no second group. Forbidding only `seg:` gets would wave
    /// every one of those through.
    fn assert_gets_only_the_root_of(&self, inode_id: u64) -> usize {
        let root = inode_key(inode_id);
        let gets = self.on(Channel::Get);
        assert!(!gets.is_empty(), "a read must read that object's own root");
        let stray = gets.iter().find(|request| request.target != root);
        let stray = stray.map(|request| String::from_utf8_lossy(&request.target));
        assert!(stray.is_none(), "a resolve also fetched {stray:?}");
        gets.len()
    }
}

#[async_trait]
impl MetadataStore for FakeStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.record(Channel::Get, key, None, 0);
        let value = self.rows.lock().unwrap().get(key).cloned();
        // Fired AFTER the value is taken: the reader already holds the old bytes when the
        // overwrite lands, which is exactly the window the resolve-retry rule is about.
        self.fire(When::RootRead);
        Ok(value)
    }

    /// Recorded, then **refused**. No resolve may take an unpaged `scan`: it is
    /// complete-or-fail-loud at `SCAN_CAP` (`crates/traits/src/lib.rs:286`), so a damaged
    /// range would cost the caller the whole namespace or the whole call — neither of which
    /// per-object containment can catch. Answering one here would let that call look like a
    /// success; refusing makes it a visible failure of whichever read attempted it, on top
    /// of the log assertion.
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.record(Channel::Scan, prefix, None, 0);
        Err(format!(
            "a read took an unpaged scan of {}",
            String::from_utf8_lossy(prefix)
        )
        .into())
    }

    /// The four paging clauses (`crates/traits/src/lib.rs:1105`), honestly: byte-ordered,
    /// exclusive cursor, `next` `None` only at exhaustion, no skips — with
    /// [`FakeStore::reverse_pages`] the one deliberate exception, so a test can break a
    /// clause on purpose.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        self.record(Channel::Page, prefix, after, limit);
        assert!(
            limit > 0,
            "a page bound of 0 is `ZeroPageLimit`, never a page"
        );
        self.fire(When::SegPage);
        let rows = self.rows.lock().unwrap();
        let mut matching = rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|cursor| key.as_slice() > cursor))
            .map(|(key, value)| (key.clone(), value.clone()));
        let mut items: Vec<(Vec<u8>, Bytes)> = matching.by_ref().take(limit).collect();
        // Clause 3: `Some(last key returned)` while more may remain, `None` only when the
        // prefix is exhausted at this instant.
        let next = matching
            .next()
            .and_then(|_| items.last().map(|(key, _)| key.clone()));
        if self.reverse_pages && prefix.starts_with(b"seg:") {
            items.reverse();
        }
        if prefix.starts_with(b"seg:") {
            items.extend(self.bleed.iter().cloned());
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        // No read path here drives a conditional batch (the repair enqueue a corrupt
        // fragment would trigger is not reachable from these fixtures), so preconditions
        // are not modelled — this body exists to satisfy the trait.
        assert!(
            batch.preconditions.is_empty(),
            "this fake models no compare-and-set; a test needing one wants the real store"
        );
        let mut rows = self.rows.lock().unwrap();
        for (key, value) in batch.puts {
            rows.insert(key, value);
        }
        for key in batch.deletes {
            rows.remove(&key);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// Assert a read failed **closed** with a typed, *resolved* refusal — never the base's
/// blanket "nothing can resolve a segmented map yet"
/// ([`ChunkMapError::SegmentedMapUnsupported`], the only error a `.chunk_map` site raised
/// before this patch).
///
/// That variant is base-visible (#648) and named here deliberately: without this check a
/// bare "the read must fail" assertion would pass equally well pre-fix (every segmented
/// map is refused, unconditionally) and post-fix (this ONE anomaly is refused, resolved)
/// — not a fix-discriminating assertion. Which variant an anomaly raises is *not* named:
/// nothing this patch adds is imported, so the discriminator is "resolved AND typed AND
/// not the blanket refusal". It also separates a refusal made by the resolver from one a
/// later stage makes on its behalf — a downstream size check (`ReadError::SizeMismatch`)
/// is no `ChunkMapError` and fails this assertion, so a resolution that leaked an
/// under-described map to the byte layer cannot pass as "failed closed".
fn assert_fails_closed(case: &str, read: Result<Option<Vec<u8>>>) {
    match read {
        Ok(read) => panic!("{case}: must fail closed; the read answered {read:?}"),
        Err(err) => assert_resolved_typed_refusal(case, err),
    }
}

fn assert_resolved_typed_refusal(case: &str, err: BoxError) {
    let typed = err.downcast_ref::<ChunkMapError>().unwrap_or_else(|| {
        panic!("{case}: a resolve failure must surface as a typed ChunkMapError, got: {err}")
    });
    assert!(
        !matches!(typed, ChunkMapError::SegmentedMapUnsupported { .. }),
        "{case}: must be refused by the resolver's own typed anomaly, not the base's \
         blanket 'this build cannot yet resolve a segmented map': {err}"
    );
}

// ---------------------------------------------------------------------------
// (1) Byte-identical reads, through the core read path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_segmented_object_reads_byte_identical_to_its_flat_equivalent() {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let (chunks, _dir) = chunk_store();
    let data = payload(48, 0);

    // Publish a genuine flat object, then re-spell THAT object's own chunks as a
    // two-segment generation — so the two reads below differ in the map's shape and in
    // nothing else.
    let plan = write_flat(&meta, &chunks, 1, "segmented", &data, 0x100).await;
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let (refs, records) = split(&plan, 2);
    let mut batch = WriteBatch::new().put(inode_key(1), segmented_root(&group, refs, 2));
    for (key, value) in rows_of(&group, &records) {
        batch = batch.put(key, value);
    }
    assert_eq!(
        meta.commit(batch).await.expect("seed the segmented root"),
        CommitOutcome::Committed
    );
    // The byte-for-byte control: the same payload, published and left flat.
    write_flat(&meta, &chunks, 2, "flat", &data, 0x200).await;

    let segmented = read_object(&meta, &chunks, 1).await.unwrap();
    let flat = read_object(&meta, &chunks, 2).await.unwrap();
    assert_eq!(segmented.as_deref(), Some(&data[..]), "the source data");
    assert_eq!(segmented, flat, "segmented and flat agree byte-for-byte");
    let by_name = read_path(&meta, &chunks, ROOT, "segmented").await.unwrap();
    assert_eq!(by_name, Some(data), "read_path resolves the same bytes");
}

// ---------------------------------------------------------------------------
// (2) The work a read demands is bounded by the reader, not by the record.
// ---------------------------------------------------------------------------

/// Seed `store` with a correct `n`-segment generation of `plan` at `inode_id`, keyed by
/// `group`.
async fn seed_segmented(
    store: &FakeStore,
    inode_id: u64,
    plan: &write::WritePlan,
    group: &SegmentGroup,
    n: usize,
) {
    let (refs, records) = split(plan, n);
    store
        .seed([(inode_key(inode_id), segmented_root(group, refs, 1))])
        .seed(rows_of(group, &records));
}

/// **(2a) A resolve reads this object's root and this group's own range — nothing else.**
/// The decoys are the two ranges a resolver could plausibly widen into: another nonce, and
/// **the same nonce at a different epoch** (an older, not-yet-reclaimed generation of this
/// very object). Neither is named by any live root, and reading either is how a reader
/// splices a retired generation's segment into a live map.
#[tokio::test]
async fn a_resolve_reads_only_its_own_root_and_its_own_group_range() {
    let (chunks, _dir) = chunk_store();
    let data = payload(40, 0);
    let plan = fragments_for(&chunks, &data, 0x300).await;
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let store = FakeStore::default();
    seed_segmented(&store, 1, &plan, &group, 2).await;

    let decoys = [
        SegmentGroup::new(OTHER_NONCE, EPOCH).unwrap(),
        SegmentGroup::new(NONCE, EPOCH - 1).unwrap(),
    ];
    let stray = SegmentRecord::new(vec![plan.chunk_refs()[0].clone()], 0).unwrap();
    for decoy in &decoys {
        store.seed([row(decoy, 0, &stray)]);
    }

    let got = read_object(&store, &chunks, 1).await.unwrap();
    assert_eq!(got.as_deref(), Some(&data[..]), "the live object reads");

    // Paged, over its own group's range and no other, and every page asks for the
    // READER's own limit — a two-segment root sizes nothing (criterion 2(c)'s lower half;
    // its upper half, a `MAX_ROOT_SEGMENTS` root asking for the same number, is below).
    let own_range = seg_range_prefix(&group);
    let pages = store.on(Channel::Page);
    assert!(!pages.is_empty(), "a resolve must read its own range");
    for page in &pages {
        assert_eq!(
            page.target, own_range,
            "paged outside its own group's range"
        );
        assert_eq!(
            page.limit, READER_PAGE_LIMIT,
            "a 2-segment root sized the page: the claim, not the reader's constant"
        );
    }
    // No unpaged scan — including none over `seg:<nonce>:`, the whole-group prefix a
    // cleanup pass sweeps, which would take every epoch at once.
    store.assert_never_used(Channel::Scan, "an unpaged scan was taken");
    for request in store.log() {
        for range in decoys.iter().map(seg_range_prefix) {
            assert!(
                !request.target.starts_with(&range[..]) && !range.starts_with(&request.target),
                "a group no live root names was read: {}",
                String::from_utf8_lossy(&request.target)
            );
        }
        assert_ne!(
            request.target,
            seg_group_prefix(group.nonce()),
            "the whole-group prefix spans every epoch, including retired ones"
        );
    }
    // And the `get` channel bounded positively: this object's own root, no other key, and
    // no more often than the reader's own constant.
    assert_eq!(
        store.assert_gets_only_the_root_of(1),
        CLEAN_RESOLVE_ROOT_GETS,
        "the root-read count is the READER's, not a function of the table"
    );
}

/// Seed a decodable root naming ONE MORE segment than a resolver may ever spend a read on
/// (`MAX_ROOT_SEGMENTS` is a resolve-time ceiling, not a decode invariant — so the record
/// exists), and no `seg:` records for it at all.
fn seed_over_ceiling_root(store: &FakeStore, inode_id: u64, group: &SegmentGroup) {
    let refs = (0..(MAX_ROOT_SEGMENTS + 1) as u32)
        .map(|i| sref(i, i as u64, 1))
        .collect();
    store.seed([(inode_key(inode_id), segmented_root(group, refs, 1))]);
}

/// **(2b) A live root over the segment ceiling is refused UNREAD.** The root the store
/// holds is the one the reader met, so this is the object's own fault: refused before a
/// single row of its range is asked for, and without reaching for any other key to decide
/// it. A ceiling enforced *after* the range read would let the record spend the very
/// budget the ceiling exists to cap.
#[tokio::test]
async fn a_root_over_the_segment_ceiling_is_refused_before_any_range_read() {
    let (chunks, _dir) = chunk_store();
    let store = FakeStore::default();
    seed_over_ceiling_root(&store, 1, &SegmentGroup::new(NONCE, EPOCH).unwrap());

    assert_fails_closed("over the ceiling", read_object(&store, &chunks, 1).await);
    store.assert_never_used(Channel::Page, "a range was read anyway");
    store.assert_never_used(Channel::Scan, "an unpaged scan was taken");
    store.assert_gets_only_the_root_of(1);
}

/// **(2c) The widest table the ceiling admits still sizes no page — and reads back
/// whole.** Its `MAX_ROOT_SEGMENTS` claim is 4x the reader's page bound and 256x the
/// two-segment root above, yet every page it produces asks for exactly the same number
/// the small one did: the page size is demonstrably not a function of the record. The same
/// fixture is the segment ceiling's other side — the most segments this deployment admits
/// is **inside** it — and the only one that exercises the multi-page walk, on a strictly
/// advancing continuation cursor.
#[tokio::test]
async fn the_widest_admissible_root_still_sizes_no_page_and_reads_back_whole() {
    let (chunks, _dir) = chunk_store();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    // One chunk per segment, so the table is exactly MAX_ROOT_SEGMENTS entries long.
    let wide = payload((MAX_ROOT_SEGMENTS * CHUNK) as u32, 7);
    let plan = fragments_for(&chunks, &wide, 0x1_000).await;
    let store = FakeStore::default();
    seed_segmented(&store, 1, &plan, &group, MAX_ROOT_SEGMENTS).await;

    assert_eq!(
        read_object(&store, &chunks, 1).await.unwrap().as_deref(),
        Some(&wide[..]),
        "exactly MAX_ROOT_SEGMENTS is INSIDE the ceiling and reads back whole"
    );
    let pages = store.on(Channel::Page);
    assert!(
        pages.len() > 1,
        "a {MAX_ROOT_SEGMENTS}-segment range must be a multi-page walk at a \
         {READER_PAGE_LIMIT}-row page, not one giant read: {pages:?}"
    );
    let mut cursors = Vec::new();
    for page in &pages {
        assert_eq!(
            page.limit, READER_PAGE_LIMIT,
            "the widest root the ceiling admits still sizes no page"
        );
        cursors.push(page.after.clone());
    }
    assert_eq!(cursors[0], None, "the first page anchors at the prefix");
    assert!(
        cursors.windows(2).all(|pair| pair[0] < pair[1]),
        "the walk must advance on the continuation cursor it was handed: {cursors:?}"
    );
    // The reader's root reads stay its own constant across a 256x wider table.
    assert_eq!(
        store.assert_gets_only_the_root_of(1),
        CLEAN_RESOLVE_ROOT_GETS,
        "a wider table bought the record no extra root reads"
    );
}

/// **(2d) An unresolvable object fails closed for ITSELF ALONE.** One damaged object may
/// not end the read of another — that is the difference between a per-object refusal and
/// a store-wide availability loss (C-1).
#[tokio::test]
async fn an_object_that_cannot_resolve_fails_closed_for_itself_alone() {
    let (chunks, _dir) = chunk_store();
    let store = FakeStore::default();
    // Each object owns its own group, as each publishing session mints its own nonce —
    // so the well-formed object's rows cannot stand in for the damaged one's.
    let broken_group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let whole_group = SegmentGroup::new(OTHER_NONCE, EPOCH).unwrap();

    let broken = payload(24, 3);
    let broken_plan = fragments_for(&chunks, &broken, 0x500).await;
    let (refs, records) = split(&broken_plan, 2);
    store
        .seed([(inode_key(1), segmented_root(&broken_group, refs, 1))])
        // Segment 1 is simply not there — the root names it, nothing satisfies it.
        .seed(rows_of(&broken_group, &records[..1]));

    let whole = payload(24, 9);
    let whole_plan = fragments_for(&chunks, &whole, 0x600).await;
    seed_segmented(&store, 2, &whole_plan, &whole_group, 2).await;

    assert_fails_closed("a missing segment", read_object(&store, &chunks, 1).await);
    assert_eq!(
        read_object(&store, &chunks, 2).await.unwrap().as_deref(),
        Some(&whole[..]),
        "a second, well-formed object in the same store still reads"
    );
}

// ---------------------------------------------------------------------------
// (3) Resolution is total and never tears.
// ---------------------------------------------------------------------------

/// **A generation the root STILL NAMES that cannot be resolved is fail-closed, typed —
/// whichever way it is broken.** Every case starts from the same correct two-segment
/// generation and bends exactly one thing, so the bend *is* the property. None may be
/// answered with a short read, a half map, or "this object owns no chunks": an
/// under-approximation of which bytes an object owns is what lets one process reclaim
/// what another still needs (C-1).
#[tokio::test]
async fn every_anomaly_on_a_generation_the_root_still_names_fails_closed() {
    let (chunks, _dir) = chunk_store();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let data = payload(32, 5);
    let plan = fragments_for(&chunks, &data, 0x700).await;
    let (refs, records) = split(&plan, 2);
    let correct = rows_of(&group, &records);

    // A record whose stored value is one byte past the ceiling every backend inherits,
    // padded with the JSON whitespace `decode` ignores: it would parse back to exactly
    // the right record, so its SIZE is the only thing wrong with it.
    let mut oversized = correct[1].1.to_vec();
    oversized.resize(MAX_VALUE_BYTES + 1, b' ');
    // A key under this group's own range that the `seg:` grammar refuses — six non-digit
    // characters where the zero-padded index belongs. Any writer can leave one.
    let mut malformed_key = seg_range_prefix(&group);
    malformed_key.extend_from_slice(b"abcdef");
    // A record whose own extent disagrees with the table the root carries for it.
    let shifted = SegmentRecord::new(records[1].chunks().to_vec(), records[1].byte_offset() + 1)
        .expect("a record at the wrong offset");
    // A row at an index the root's two-entry table cannot account for.
    let unnamed = row(&group, 2, &records[0]);

    let second = correct[1].0.clone();
    let bends: Mutation = vec![
        (second.clone(), None),
        (second.clone(), Some(Bytes::from_static(b"{oops"))),
        (second.clone(), Some(Bytes::from(oversized))),
        (malformed_key, Some(Bytes::from_static(b"{}"))),
        (unnamed.0, Some(unnamed.1)),
        (second, Some(encode(&shifted))),
    ];
    let cases = [
        "a named segment is absent",
        "a named segment will not decode",
        "a segment's value is past the value ceiling",
        "the range holds a key this grammar refuses",
        "the range holds a row the root's table does not name",
        "a segment's extent disagrees with the root's table",
    ];

    for (case, (key, value)) in cases.into_iter().zip(bends) {
        let store = FakeStore::default();
        store
            .seed([(inode_key(1), segmented_root(&group, refs.clone(), 1))])
            .seed(correct.clone());
        match value {
            Some(value) => {
                store.seed([(key, value)]);
            }
            None => {
                store.rows.lock().unwrap().remove(&key);
            }
        }
        assert_fails_closed(case, read_object(&store, &chunks, 1).await);
        // And settled by the ONE re-read the rule calls for. The root that answered it is
        // the root the reader already met, so re-asking it can only produce the same
        // verdict: a refusal that instead spent the whole restart budget would cost this
        // object's own churn-free fault a multiple of the reader's constant, and would
        // report a plain corruption as a map that will not settle.
        assert_eq!(
            store.assert_gets_only_the_root_of(1),
            CLEAN_RESOLVE_ROOT_GETS,
            "{case}: a fault on a generation the root STILL NAMES is one re-read, not a \
             restart campaign"
        );
    }
}

/// **The value ceiling's admissible side: a record of exactly [`MAX_VALUE_BYTES`] still
/// resolves.** The bend above refuses a value ONE byte past the ceiling; this is the same
/// fixture one byte smaller, and it must read back whole. `MAX_VALUE_BYTES` is the largest
/// value the tightest backend in play accepts, not the first it refuses
/// (`crates/traits/src/lib.rs:995-999`), so a record sized exactly to it is a conforming
/// publication — refusing it would invent a permanent, unrecoverable read failure out of a
/// legal record, which is precisely the failure mode C-1 forbids. Without this case the
/// refusal is only known to be *some* bound at or below the ceiling: an off-by-one that
/// moved it a byte down would pass every other test in this file.
#[tokio::test]
async fn a_segment_value_at_exactly_the_ceiling_still_resolves() {
    let (chunks, _dir) = chunk_store();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let data = payload(32, 41);
    let plan = fragments_for(&chunks, &data, 0xe00).await;
    let (refs, records) = split(&plan, 2);
    let rows = rows_of(&group, &records);

    // The same trick the over-ceiling bend uses, stopped one byte earlier: padded with the
    // JSON whitespace `decode` ignores, so the record parses back to exactly the right
    // segment and its SIZE is the only thing that differs from the correct row.
    let (key, encoded) = rows[1].clone();
    assert!(
        encoded.len() < MAX_VALUE_BYTES,
        "the fixture must PAD to the ceiling, never truncate to it"
    );
    let mut at_ceiling = encoded.to_vec();
    at_ceiling.resize(MAX_VALUE_BYTES, b' ');

    let store = FakeStore::default();
    store
        .seed([(inode_key(1), segmented_root(&group, refs, 1))])
        .seed(rows)
        .seed([(key, Bytes::from(at_ceiling))]);

    assert_eq!(
        read_object(&store, &chunks, 1).await.unwrap().as_deref(),
        Some(&data[..]),
        "a value AT the ceiling is inside it — only one byte MORE may be refused"
    );
}

/// **A row from another group, answered inside this group's range, is refused — on either
/// component ALONE.** The range prefix pins nonce and epoch together, so a row that comes
/// back differing in one of them is a row from outside the prefix the store was asked for.
/// The resolver pins the group itself rather than trusting a backend's prefix handling,
/// and each case here bends exactly ONE component: a resolver that only refused a row
/// differing in BOTH would splice a neighbouring range's record into a live map.
///
/// The bled row carries **the record the root's table names for that index**, byte-
/// identical to the legitimate one — so a resolver that admitted it would resolve cleanly
/// and answer the object's bytes. Nothing about the *contents* can fail this case: the
/// refusal can only come from the key's own group.
///
/// The same-nonce arm is the live risk. `seg:<nonce>:<epoch>:` ranges of one group's
/// epochs are byte-adjacent, and a retired generation's records outlive its root until the
/// drain reaches them (`0016:2452-2462`), so the row a leaky prefix answer would splice in
/// is exactly a superseded generation's segment: the same object, the wrong bytes, at full
/// length and with no error (C-1).
#[tokio::test]
async fn a_row_from_another_group_answered_inside_this_range_is_refused() {
    let (chunks, _dir) = chunk_store();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let data = payload(32, 29);
    let plan = fragments_for(&chunks, &data, 0xe80).await;
    let (refs, records) = split(&plan, 2);

    let foreign = [
        (
            "a row from another EPOCH of this very group",
            SegmentGroup::new(NONCE, EPOCH - 1).unwrap(),
        ),
        (
            "a row from another GROUP at this epoch",
            SegmentGroup::new(OTHER_NONCE, EPOCH).unwrap(),
        ),
    ];
    for (case, other) in foreign {
        let store = FakeStore {
            bleed: vec![row(&other, 1, &records[1])],
            ..FakeStore::default()
        };
        store
            .seed([(inode_key(1), segmented_root(&group, refs.clone(), 1))])
            .seed(rows_of(&group, &records));

        assert_fails_closed(case, read_object(&store, &chunks, 1).await);
    }
}

/// **A generation retired MID-RESOLVE is dropped, never torn.** The root moves first and
/// its records are deleted after (`0016:2452-2462`), so a reader that has read the old
/// root and is paging its range meets a range that can no longer be completed. Both arms
/// of what the rule then allows are here, on the same interleaving:
///
/// * the root **moved on** — the read restarts and answers the WHOLE live generation.
///   `NoSuchKey` would 404 an object that never stopped existing, and the half-map it did
///   read would tear the object across two generations;
/// * the root is **gone** — the object is being reclaimed with its generation, so the
///   honest answer is the one every consumer already reads as "no such object".
///
/// Both fixtures also take a record of the retired generation, as the drain does: with the
/// old map genuinely incompletable, a reader that did NOT re-read the root has no
/// old-generation answer left to succeed with by accident.
#[tokio::test]
async fn a_generation_retired_mid_resolve_restarts_onto_the_live_root_or_reads_as_absent() {
    let (chunks, _dir) = chunk_store();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let old = payload(32, 11);
    let old_plan = fragments_for(&chunks, &old, 0x800).await;
    // The generation that replaces it while the reader's range read is in flight: a
    // brand-new flat object of a different length, whose fragments are already on disk. Its
    // bytes share none of the old one's, so a torn answer cannot look like a whole one.
    let new = payload(20, 200);
    let new_plan = fragments_for(&chunks, &new, 0x900).await;
    let (first_row, _) = row(&group, 0, &split(&old_plan, 2).1[0]);

    // The root the retirement installs, what the read must then answer — the live
    // generation's whole bytes when it moved on, absent when it is gone — and what
    // answering it costs in root reads. That cost is the two arms' other difference: a
    // supersede has a successor to go and read (the re-read that settled it, plus the live
    // root it restarts onto), while a deletion has none — the re-read that saw the root
    // absent IS the answer, and asking a third time can only hear the same thing.
    let arms = [
        (
            "superseded",
            Some(flat_root(&new_plan, 2)),
            Some(new),
            CLEAN_RESOLVE_ROOT_GETS + 1,
        ),
        ("deleted", None, None, CLEAN_RESOLVE_ROOT_GETS),
    ];
    for (case, root, expected, root_gets) in arms {
        let store = FakeStore::default();
        seed_segmented(&store, 1, &old_plan, &group, 2).await;
        store.retire_at(
            When::SegPage,
            vec![(inode_key(1), root), (first_row.clone(), None)],
        );
        assert_eq!(
            read_object(&store, &chunks, 1).await.unwrap(),
            expected,
            "{case}: a resolve retired mid-read is dropped, never torn and never short"
        );
        assert_eq!(
            store.assert_gets_only_the_root_of(1),
            root_gets,
            "{case}: a dropped resolution costs the reads its OWN arm needs"
        );
    }
}

/// **An over-ceiling root the store has already moved off is a RETIRED generation, not a
/// fault.** The ceiling still refuses before any range read — but *which answer* that
/// refusal becomes is the resolve-retry rule's call, exactly as it is for an absent row.
/// The reader here holds a snapshot whose root was overwritten the instant after it read
/// it (the only window a resolve refused before its first page ever has), so the live
/// generation is what it must read. Refusing where the ceiling is noticed would fail a
/// perfectly healthy object over bytes nothing points at any more — a permanent read
/// failure invented out of an ordinary overwrite, which is what C-1 forbids.
#[tokio::test]
async fn an_over_ceiling_root_superseded_mid_read_restarts_onto_the_live_generation() {
    let (chunks, _dir) = chunk_store();
    let store = FakeStore::default();
    seed_over_ceiling_root(&store, 1, &SegmentGroup::new(NONCE, EPOCH).unwrap());

    let new = payload(36, 170);
    let new_plan = fragments_for(&chunks, &new, 0xb00).await;
    store.retire_at(
        When::RootRead,
        vec![(inode_key(1), Some(flat_root(&new_plan, 2)))],
    );

    assert_eq!(
        read_object(&store, &chunks, 1).await.unwrap(),
        Some(new),
        "a retired over-ceiling table must not fail a live generation that resolves"
    );
    // …and the retired table authorises no range read whatsoever on its way out.
    store.assert_never_used(Channel::Page, "a range was read anyway");
    store.assert_gets_only_the_root_of(1);
}

/// **Chunks come back in the segment table's order, not the page's.** The store hands
/// every `seg:` page back reversed — the paging contract's byte-order clause broken on
/// purpose — so a read that still returns the payload can only have ordered the segments
/// by their own **parsed index**. A resolver that concatenated in arrival order would
/// answer this object's bytes with two halves swapped: the same length, the same chunks,
/// and silently wrong content.
#[tokio::test]
async fn chunks_are_ordered_by_the_parsed_index_not_by_page_order() {
    let (chunks, _dir) = chunk_store();
    let data = payload(48, 17);
    let plan = fragments_for(&chunks, &data, 0xc00).await;
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let store = FakeStore {
        reverse_pages: true,
        ..FakeStore::default()
    };
    seed_segmented(&store, 1, &plan, &group, 4).await;

    assert_eq!(
        read_object(&store, &chunks, 1).await.unwrap().as_deref(),
        Some(&data[..]),
        "segments are assembled by parsed index, whatever order the page arrived in"
    );
}

/// One retirement, as the drain performs it (`0016:2452-2462`): `next`'s whole generation
/// published over the root, and `prev`'s records taken with it. Every generation is
/// complete and resolvable in itself — only the epoch moves — so nothing about the DATA
/// can explain what a reader racing this ends up answering.
fn supersede(plan: &write::WritePlan, prev: &SegmentGroup, next: &SegmentGroup) -> Mutation {
    let (refs, records) = split(plan, 2);
    let mut mutation: Mutation = rows_of(next, &records)
        .into_iter()
        .map(|(key, value)| (key, Some(value)))
        .collect();
    mutation.push((inode_key(1), Some(segmented_root(next, refs, 2))));
    mutation.extend(rows_of(prev, &records).into_iter().map(|(k, _)| (k, None)));
    mutation
}

/// A DELETE landing in the same window: the root gone and the generation it named taken
/// with it — no successor anywhere in the store.
fn delete_object(plan: &write::WritePlan, group: &SegmentGroup) -> Mutation {
    let (_refs, records) = split(plan, 2);
    let mut mutation: Mutation = rows_of(group, &records)
        .into_iter()
        .map(|(key, _)| (key, None))
        .collect();
    mutation.push((inode_key(1), None));
    mutation
}

/// One [`supersede`] per adjacent pair in `generations`, queued in order — so a campaign of
/// `generations.len() - 1` retirements lands one per attempt, each at the range read of the
/// attempt that meets it.
fn queue_supersedes(store: &FakeStore, plan: &write::WritePlan, generations: &[SegmentGroup]) {
    for pair in generations.windows(2) {
        store.retire_at(When::SegPage, supersede(plan, &pair[0], &pair[1]));
    }
}

/// `n` successive generations of one object: the same nonce at ascending epochs, which is
/// how a republished object's groups are keyed.
fn epochs(n: u64) -> Vec<SegmentGroup> {
    (0..n)
        .map(|epoch| SegmentGroup::new(NONCE, EPOCH + epoch).unwrap())
        .collect()
}

/// **A resolution that never settles is a typed refusal, and it terminates.** Restarting
/// is the right answer to *one* retirement; it cannot be the answer to every retirement,
/// or a hot object spins a reader forever. Past the budget the honest answer is a typed
/// error about this object: `Ok(None)` would tell every consumer "no such object" about an
/// object that is whole in every generation — a maintenance pass would then protect none
/// of it, and a reader would 404 it (decision 7(h); C-1).
///
/// Hands back **how many** retirements the reader absorbed before it gave up — its own
/// restart budget, measured off its behaviour. The case below needs that number and must
/// not pin it: the budget is the resolver's constant, and a pinned copy would either drift
/// or, worse, silently stop testing the last attempt.
async fn assert_retired_under_every_restart_refuses(chunks: &FsChunkStore) -> usize {
    let data = payload(32, 23);
    let plan = fragments_for(chunks, &data, 0xd00).await;
    let store = FakeStore::default();
    // Far more retirements than any reader may absorb, so the reader's own budget — never
    // the length of this script — is what ends the campaign.
    let generations = epochs(12);
    seed_segmented(&store, 1, &plan, &generations[0], 2).await;
    // Each attempt's range read republishes the object at the NEXT epoch and takes the
    // generation the reader is mid-way through with it: every attempt finds its own
    // snapshot retired under it.
    queue_supersedes(&store, &plan, &generations);

    let err = read_object(&store, chunks, 1)
        .await
        .expect_err("a resolution that never settles is a refusal, never `Ok`");
    assert_resolved_typed_refusal("retired under every restart", err);
    // Bounded work, not an unbounded chase: the reader's restart budget is its own, so a
    // record's churn buys it no extra rounds.
    let root_gets = store.assert_gets_only_the_root_of(1);
    assert!(
        root_gets <= 4 * CLEAN_RESOLVE_ROOT_GETS,
        "giving up must cost a bounded number of root reads, spent {root_gets}"
    );
    // One retirement lands per attempt (each attempt reads the group's range once), so the
    // mutations this campaign got through count the attempts the reader had.
    let attempts = (generations.len() - 1) - store.pending_left();
    assert!(
        (2..generations.len() - 1).contains(&attempts),
        "the reader must stop of its own accord, after more than one attempt: {attempts}"
    );
    attempts
}

#[tokio::test]
async fn a_generation_retired_under_every_restart_is_a_typed_refusal_that_terminates() {
    let (chunks, _dir) = chunk_store();
    assert_retired_under_every_restart_refuses(&chunks).await;
}

/// **A DELETE met on the reader's LAST attempt is still "no such object".** A resolution
/// gives up only against *churn* — a generation that keeps being replaced — and a deletion
/// is not churn: there is no successor to go and read, so the re-read that found the root
/// absent has already produced the final answer. Which attempt it lands on cannot change
/// what it means.
///
/// The interleaving is the point. Every attempt but the last meets an overwrite, so the
/// reader arrives at its final attempt with its budget spent; that attempt meets the
/// delete. A resolver that files "gone" under the same drop as "moved on" answers this
/// with the give-up refusal — telling every consumer that a plainly deleted object's map
/// *will not settle*, when the store has already said the object is not there. A reader
/// would surface that as a hard error instead of a 404, and every maintenance consumer to
/// come (#650/#651) would read "unresolvable" — the reclaim-blocking answer — for an object
/// whose bytes are genuinely nobody's.
#[tokio::test]
async fn a_delete_met_on_the_readers_last_attempt_is_no_such_object() {
    let (chunks, _dir) = chunk_store();
    // Measured from the reader itself, on the same fixtures: how many retirements it
    // absorbs before giving up.
    let attempts = assert_retired_under_every_restart_refuses(&chunks).await;

    let data = payload(32, 73);
    let plan = fragments_for(&chunks, &data, 0x1_200).await;
    let store = FakeStore::default();
    let generations = epochs(attempts as u64);
    seed_segmented(&store, 1, &plan, &generations[0], 2).await;
    // `attempts - 1` overwrites, one per attempt…
    queue_supersedes(&store, &plan, &generations);
    // …then, on the attempt the reader has left, the object is deleted outright.
    store.retire_at(
        When::SegPage,
        delete_object(&plan, generations.last().expect("a live generation")),
    );

    assert_eq!(
        read_object(&store, &chunks, 1).await.unwrap(),
        None,
        "a delete is not churn: the last attempt answers no-such-object, not `the map will \
         not settle`"
    );
    assert_eq!(
        store.pending_left(),
        0,
        "the fixture must actually reach the delete, or this case proves nothing"
    );
}
