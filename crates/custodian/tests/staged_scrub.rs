//! Issue #813 (663.1, split from #663) — scrub checks a session's COMMITTED `part:` fragments,
//! with the scheme each record's own `ChunkRef` carries, exactly as it already checks a
//! committed chunk map's — but never reads an owned, in-flight `sidx:` entry (proposal 0016
//! decision 2, `0016:824-825`). Legs D-F — reconstruction's own read of the same staged
//! classes — are appended to `crates/custodian/tests/staged_protection.rs` instead (as legs
//! G-I there): they build a `ReconstructionContext`, whose two new fields do not exist on this
//! leg's red base, so a file that built one here would make the whole red run UNVERIFIABLE
//! (brief §Verification posture).
//!
//! Every leg drives the production entry point — `reconcile_step` with a `ScrubContext` — over
//! in-memory doubles. No client creates a session before the S3 verbs (#508), so every staged
//! record is seeded as raw JSON the base decoders accept (the shapes of
//! `crates/core/tests/multipart_session_records.rs:81-141`), each round-tripped through
//! `decode_session_record` / `decode_part_record` / `decode_owned_entry` before a pass reads
//! it — the same discipline `staged_protection.rs` already follows for GC and restore.
//!
//! The legs:
//! - **A** a committed part's fragment, corrupt or missing, is detected, excluded and enqueued
//!   for reconstruction; so is an intact fragment whose header names a DIFFERENT EC scheme from
//!   the part record's own `ChunkRef` (proving the part's scheme, not merely "does it decode",
//!   is what is checked). A control with every fragment intact queues nothing and the pass
//!   answers `Satisfied`.
//! - **A'** once a committed map names a published chunk, scrub checks it where that map places
//!   it, not where the upload's leftover part record does: after reconstruction has moved a lost
//!   fragment, the part record's old position is empty, and checking it would have scrub
//!   re-enqueue a chunk reconstruction then finds whole and drains, every pass. A control with
//!   the fragment lost at the committed position proves the chunk is still checked.
//!   **A'-malformed**: a committed map that names the chunk with a MALFORMED placement
//!   supersedes the part record too.
//! - **B** a chunk named only by an owned `sidx:` entry — no `part:` record yet — queues
//!   nothing: checking needs the COMMITTED scheme, which an in-flight chunk does not carry yet
//!   (`0016:776-781`).
//! - **C** an unreadable `part:` record fails closed: the pass still checks every OTHER
//!   fragment (leg A's corrupt chunk is still queued), names the unreadable record on the audit
//!   seam, and answers `Blocked` — scrub's existing rule for an unreadable committed chunk map,
//!   applied to the staged read (`scrub.rs:99-116`, `:205-215`). A store fault while reading a
//!   session's `part:` range fails the pass with `Err`, as it fails GC.
//! - **C-order** staged damage is named on the audit seam BEFORE the committed read: an `inode:`
//!   store fault right after the staged reading ends the pass with `Err`, and the malformed and
//!   the unreadable `part:` record are both already named (`scrub.rs:144-163`). Reconstruction
//!   pins the same promise in `staged_protection.rs` (leg I).
//! - **A-paged** the session listing is walked to its END, not to the end of its first page: with
//!   more `mpu:` sessions than one page holds (`gc.rs`'s `STAGED_PAGE`), a committed part under a
//!   session on a LATER page is still checked. Scrub's session loop
//!   (`gc.rs:staged_committed_parts`) is its own, not the one GC's paged legs cover.
//! - **C'** **source before destination**: a publication landing between scrub's two reads
//!   leaves the chunk checked. The flip (committed inode written, part record kept) and the
//!   retirement drain (the part record deleted, a later batch — `0016:793-800`) land during the
//!   pass, at the store's own choosing; the chunk's fragment is missing, so scrub must enqueue
//!   it. A pass that read `inode:` before `part:` sees the chunk in NEITHER class and answers
//!   `Satisfied` over a lost fragment. GC pins the same handoff the same way
//!   (`staged_protection.rs`'s (C)(ii)).
//! - **C''** a committed part record whose placement is EMPTY or the wrong length is malformed,
//!   not identity-filled: every staged record is born with a full placement (`0016:828`), so
//!   scrub enqueues NO phantom repair against a D server no record ever named — the exact-length
//!   rule `StagedSet::place` already applies (`gc.rs:staged_placement`) — names the `part:`
//!   record on its audit seam as a staged record (C''-audit), and answers `Blocked`: the chunk
//!   was never checked. The counter-case: when a committed map names the chunk too, that map's
//!   rule alone decides, and the damaged leftover does not block.
//! - **C'''** an `mpu:` key naming no upload is contained like an undecodable part value — named
//!   on the audit seam, `Blocked`, every readable session still checked.
//!
//! Falsifiability (brief §Falsifiability): RED in-process on `origin/main` @ `4ab2b28`, no
//! container — scrub never reads `part:`, so A, A-paged, C, C-order and every C-prime leg fail by
//! assertion (a corrupt/missing/wrong-scheme committed-part fragment is never enqueued, on any
//! page of the session listing; an unreadable part record or session key never blocks
//! certification, nor is staged damage named at all; a published-mid-pass chunk is never checked;
//! and a malformed staged placement is never reported or refused certification). B passes there
//! by construction: it guards against scrub over-reaching a `sidx:`-only chunk, not a red leg. So
//! do the A' legs, A'-malformed included, which guard the other way: base reads no part record
//! at all, and A' goes red only on a scrub that reads part records without letting a committed
//! map supersede them.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::{encode, EcSchemeType, FragmentHeader, CORE_HEADER_LEN};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, dirent_key, inode_key, ChunkRef, DirentRecord, EcScheme, InodeId, InodeRecord, InodeState,
};
use wyrd_core::multipart::{
    decode_owned_entry, decode_part_record, decode_session_record, mpu_key, parse_mpu_key,
    part_key, part_range, sidx_key, OwnedEntry, PartNumber, StagedPlacement, UploadId,
};
use wyrd_core::repair::repair_key;
use wyrd_custodian::{
    reconcile_step, Custodian, FencedZone, ReconcileError, Reconciled, ScrubContext,
};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId,
    FragmentId, Health, MetadataStore, PageStart, Result, ScanPage, WriteBatch, SCAN_CAP,
};

// ---- the metadata double -------------------------------------------------------------------------

/// A batch the double commits itself, right after the `fire_after`-th completed read of any of
/// `triggers` — so **when** the concurrent writer lands is the double's decision, not the pass's.
/// `staged_protection.rs`'s `Hook` (`:143-152`), unchanged in shape.
struct Hook {
    triggers: Vec<Vec<u8>>,
    fire_after: usize,
    seen: usize,
    batch: Option<WriteBatch>,
    outcome: Option<CommitOutcome>,
    /// Every key the store held the instant this hook's batch applied, before anything else ran.
    keys_after: Option<Vec<Vec<u8>>>,
}

/// An in-memory `MetadataStore` with armable read faults and read-triggered writers —
/// `staged_protection.rs`'s `Meta` (`:147-302`), narrowed to what these legs need: no lowered scan
/// cap (leg A-paged fills a real page instead) and, for a read log, only the prefix of each
/// `scan_page` call.
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// The prefix of every `scan_page` call that was let through, in order.
    pages: Mutex<Vec<Vec<u8>>>,
    /// Key ranges whose reads fail: any read whose key or prefix overlaps one of them.
    faults: Mutex<Vec<Vec<u8>>>,
    hooks: Mutex<Vec<Hook>>,
}

impl Meta {
    fn new() -> Self {
        Self {
            kv: Mutex::new(BTreeMap::new()),
            pages: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
        }
    }

    /// Commit `batch` right after the `fire_after`-th completed read of any of `triggers`.
    fn hook(&self, triggers: &[&[u8]], fire_after: usize, batch: WriteBatch) {
        self.hooks.lock().unwrap().push(Hook {
            triggers: triggers.iter().map(|t| t.to_vec()).collect(),
            fire_after,
            seen: 0,
            batch: Some(batch),
            outcome: None,
            keys_after: None,
        });
    }

    /// Each hook's commit outcome, in the order the hooks were armed — `None` for one whose
    /// trigger count was never reached.
    fn hook_outcomes(&self) -> Vec<Option<CommitOutcome>> {
        self.hooks
            .lock()
            .unwrap()
            .iter()
            .map(|hook| hook.outcome)
            .collect()
    }

    /// Every key the store held right after each hook's batch applied, in the order the hooks
    /// were armed — `None` for one whose trigger count was never reached.
    fn keys_after_hooks(&self) -> Vec<Option<Vec<Vec<u8>>>> {
        self.hooks
            .lock()
            .unwrap()
            .iter()
            .map(|hook| hook.keys_after.clone())
            .collect()
    }

    /// Count a completed read of `subject` against every hook it triggers, committing a hook's
    /// batch the moment its count is reached.
    fn completed(&self, subject: &[u8]) {
        let mut hooks = self.hooks.lock().unwrap();
        for hook in hooks.iter_mut() {
            if hook
                .triggers
                .iter()
                .any(|trigger| trigger.as_slice() == subject)
            {
                hook.seen += 1;
                if hook.seen == hook.fire_after {
                    if let Some(batch) = hook.batch.take() {
                        hook.outcome = Some(self.apply(batch));
                        hook.keys_after = Some(self.kv.lock().unwrap().keys().cloned().collect());
                    }
                }
            }
        }
    }

    /// Apply `batch` atomically: every precondition holds, or nothing changes.
    fn apply(&self, batch: WriteBatch) -> CommitOutcome {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key) != pre.expected.as_ref() {
                return CommitOutcome::Conflict;
            }
        }
        for key in batch.deletes {
            kv.remove(&key);
        }
        for (key, value) in batch.puts {
            kv.insert(key, value);
        }
        CommitOutcome::Committed
    }

    /// Put a fixture record in place — not a pass's write.
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    /// Whether `key` is present.
    fn holds(&self, key: &[u8]) -> bool {
        self.kv.lock().unwrap().contains_key(key)
    }

    /// How many pages a pass read under exactly `prefix`.
    fn pages_read(&self, prefix: &[u8]) -> usize {
        let pages = self.pages.lock().unwrap();
        pages
            .iter()
            .filter(|page| page.as_slice() == prefix)
            .count()
    }

    /// Fail every later read that overlaps `range`.
    fn fail_reads_of(&self, range: &[u8]) {
        self.faults.lock().unwrap().push(range.to_vec());
    }

    /// Refuse a read of `subject` if it overlaps an armed fault. The refusal names no key
    /// range: naming the failed read is the pass's job, not the store's.
    fn check(&self, subject: &[u8]) -> Result<()> {
        let faulted = self
            .faults
            .lock()
            .unwrap()
            .iter()
            .any(|range| range.starts_with(subject) || subject.starts_with(range));
        if faulted {
            return Err(BoxError::from("injected metadata-store fault"));
        }
        Ok(())
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check(key)?;
        let value = self.kv.lock().unwrap().get(key).cloned();
        self.completed(key);
        Ok(value)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.check(prefix)?;
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.completed(prefix);
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        self.check(prefix)?;
        self.pages.lock().unwrap().push(prefix.to_vec());
        let limit = page_limit(limit, SCAN_CAP, prefix)?;
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Excluded(cursor),
            PageStart::Prefix => Bound::Included(prefix),
            PageStart::PastPrefix => {
                self.completed(prefix);
                return Ok((Vec::new(), None));
            }
        };
        let items: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let next = page_cursor(&items, limit);
        self.completed(prefix);
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.apply(batch))
    }
}

// ---- the D-server double and the fleet -------------------------------------------------------

#[derive(Default)]
struct Disk {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
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

fn disks() -> [Disk; 4] {
    Default::default()
}

fn fleet(d: &[Disk; 4]) -> [(DServerId, &dyn ChunkStore); 4] {
    [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])]
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

fn place(d: &[Disk; 4], dserver: DServerId, frag: FragmentId, bytes: Bytes) {
    d[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .insert(frag, bytes);
}

/// A valid, self-describing v1 (`EcScheme::None`) fragment for `chunk` — what `fragment_intact`
/// must accept.
fn valid_fragment(chunk: ChunkId) -> Bytes {
    let payload = b"scrub target bytes";
    Bytes::from(encode(
        &FragmentHeader::new_v1(chunk, payload.len() as u64),
        payload,
    ))
}

/// The same fragment with a single bit flipped in the payload (`crates/custodian/tests/scrub.rs`,
/// `corrupt_fragment`, `:157-162`) — its trailing payload checksum no longer matches, so
/// `decode` fails its crc32c gate: injected bit rot.
fn corrupt_fragment(chunk: ChunkId) -> Bytes {
    let mut bytes = valid_fragment(chunk).to_vec();
    bytes[CORE_HEADER_LEN as usize] ^= 0xff;
    Bytes::from(bytes)
}

/// An intact fragment (its checksum verifies) whose header names a DIFFERENT EC scheme —
/// RS(3, 1) — from the `EcScheme::None` every part record here declares. Proves scrub checks
/// the PART record's own scheme, not merely "does the fragment decode" (`repair::fragment_intact`
/// verifies the full identity: chunk id, index, AND the EC tuple).
fn wrong_scheme_fragment(chunk: ChunkId) -> Bytes {
    let payload = b"scrub target bytes";
    let mut header = FragmentHeader::new_v1(chunk, payload.len() as u64);
    header.ec_scheme_type = EcSchemeType::ReedSolomon;
    header.ec_k = 3;
    header.ec_m = 1;
    Bytes::from(encode(&header, payload))
}

// ---- the records ----------------------------------------------------------------------------

const PARENT: u64 = 42;
const OBJECT: &str = "staged-scrub/object";
const EPOCH: u64 = 1;
/// The inode a publication's root flip writes.
const PUBLISHED: InodeId = 7;
/// An unrelated committed object, present for a whole pass: its own chunk's fragment is missing,
/// so scrub queues it in every schedule — the positive observable that the pass walked and
/// enqueued at all (leg C''s control, GC's `control` fragment in the same role).
const WITNESS: InodeId = 8;

/// An upload id: 32 lowercase-hex characters from a 2-character pair, so every leg's records
/// key under their own upload and never collide.
fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn part_no(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number in range")
}

/// A session record whose `state` is `state_json`, spelled as the base decoder's own encoding
/// and round-tripped through it (the shapes of
/// `crates/core/tests/multipart_session_records.rs:81-99` and `:298-304`).
fn session(state_json: &str) -> Bytes {
    let bytes = format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":{EPOCH},\"attempts\":1,\"state\":{state_json}}}"
    )
    .into_bytes();
    let record = decode_session_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded session {state_json} must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded session {state_json} must be the decoder's own spelling"
    );
    Bytes::from(bytes)
}

fn session_open() -> Bytes {
    session("{\"kind\":\"Open\"}")
}

/// The session a publication's root flip conditions on: fenced, with its publish target.
fn session_completing() -> Bytes {
    session(&format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":0,\
         \"publish_target\":{{\"parent\":{PARENT},\"name\":\"{OBJECT}\",\"epoch\":{EPOCH}}}}}"
    ))
}

/// The session the same flip leaves behind.
fn session_completed() -> Bytes {
    session(&format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{PUBLISHED},\"version\":1,\
         \"etag\":\"{}-1\",\"completed_at_millis\":950,\"complete_fingerprint\":\"{}\"}}}}",
        "ab".repeat(32),
        "cd".repeat(32)
    ))
}

/// A committed inode whose flat chunk map names `chunks` — the committed class scrub has always
/// walked, seeded here as the destination a publication hands a staged chunk to.
fn committed_inode(chunks: &[ChunkRef]) -> Bytes {
    metadata::encode(&InodeRecord {
        size: chunks.iter().map(|chunk| chunk.len).sum(),
        chunk_map: chunks.to_vec().into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    })
}

fn chunk_ref(id: ChunkId, scheme: EcScheme, placement: &[DServerId]) -> ChunkRef {
    ChunkRef {
        id,
        scheme,
        len: 5,
        placement: placement.to_vec(),
    }
}

/// A committed part record naming `chunks`, spelled as the base decoder's own encoding and
/// round-tripped through it (the shape of `crates/core/tests/multipart_session_records.rs:135-141`).
fn part_record(chunks: &[ChunkRef]) -> Bytes {
    let refs: Vec<String> = chunks
        .iter()
        .map(|chunk| String::from_utf8(metadata::encode(chunk).to_vec()).unwrap())
        .collect();
    let len: u64 = chunks.iter().map(|chunk| chunk.len).sum();
    let bytes = format!(
        "{{\"chunks\":[{}],\"len\":{len},\"digest\":\"{}\",\"committed_at_millis\":800,\
         \"session_epoch\":{EPOCH}}}",
        refs.join(","),
        "ab".repeat(32)
    )
    .into_bytes();
    let record = decode_part_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded part record must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded part record must be the decoder's own spelling"
    );
    Bytes::from(bytes)
}

/// An owned staging entry of `owner`, planned as `placement` under `scheme`, round-tripped
/// through `decode_owned_entry` under `key`.
fn owned_entry(owner: &UploadId, key: &[u8], scheme: EcScheme, placement: &[DServerId]) -> Bytes {
    let staged = StagedPlacement::new(scheme, placement.to_vec()).expect("a supported scheme");
    // Far past every pass's clock, so no leg here turns on it — scrub reads no lease anyway.
    let lease = 1_000_000_000;
    let value = metadata::encode(&OwnedEntry::new(owner.clone(), lease, staged).to_pending());
    decode_owned_entry(key, &value)
        .unwrap_or_else(|fault| panic!("the seeded owned entry must decode: {fault}"));
    value
}

// ---- the pass ---------------------------------------------------------------------------------

async fn elect() -> (FencedZone, Custodian) {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-staged-scrub")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    (zone, custodian)
}

/// One scrub pass through the fenced control point.
async fn scrub_pass(meta: &Meta, d: &[Disk; 4]) -> std::result::Result<Reconciled, ReconcileError> {
    let (zone, custodian) = elect().await;
    let fleet = fleet(d);
    let ctx = ScrubContext {
        meta,
        fleet: &fleet,
    };
    reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0).await
}

// ---- the audit seam -----------------------------------------------------------------------------

/// One custodian audit event, as the thread that emitted it saw it.
struct AuditEvent {
    thread: ThreadId,
    target: String,
    fields: Vec<String>,
}

fn audit_log() -> &'static Mutex<Vec<AuditEvent>> {
    static LOG: OnceLock<Mutex<Vec<AuditEvent>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

/// Install the capture once for the whole test binary, before any pass on any thread runs, so
/// no audit callsite is ever first met with no subscriber in place.
fn capture_audit() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing_subscriber::registry()
            .with(AuditCapture)
            .try_init()
            .expect("this test binary installs the only global subscriber");
    });
}

struct AuditCapture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if !target.starts_with("wyrd.custodian.") {
            return;
        }
        let mut fields = FieldText(Vec::new());
        event.record(&mut fields);
        audit_log().lock().unwrap().push(AuditEvent {
            thread: std::thread::current().id(),
            target: target.to_owned(),
            fields: fields.0,
        });
    }
}

struct FieldText(Vec<String>);

impl Visit for FieldText {
    fn record_str(&mut self, _field: &Field, value: &str) {
        self.0.push(value.to_owned());
    }

    fn record_debug(&mut self, _field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push(format!("{value:?}"));
    }
}

/// Whether a pass on this thread named `record` in an event on the audit seam `target`.
fn named_on_audit_seam(target: &str, record: &[u8]) -> bool {
    let record = String::from_utf8_lossy(record);
    let thread = std::thread::current().id();
    audit_log().lock().unwrap().iter().any(|event| {
        event.thread == thread
            && event.target == target
            && event
                .fields
                .iter()
                .any(|field| field.contains(record.as_ref()))
    })
}

const SCRUB_AUDIT: &str = "wyrd.custodian.scrub.audit";

// ---- (A) scrub checks committed-part fragments ---------------------------------------------------

/// **(A) harness:** an `Open` session with one committed `part:` record naming `chunk`, placed
/// on `dserver` — its fragment on disk as `fragment` returns, or absent entirely when `None` —
/// then one scrub pass.
async fn scrub_over_one_committed_part(
    pair: &str,
    chunk: ChunkId,
    dserver: DServerId,
    fragment: Option<Bytes>,
) -> (Meta, std::result::Result<Reconciled, ReconcileError>) {
    let meta = Meta::new();
    let d = disks();
    let id = upload(pair);
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(&[chunk_ref(chunk, EcScheme::None, &[dserver])]),
    );
    if let Some(bytes) = fragment {
        place(&d, dserver, frag(chunk, 0), bytes);
    }
    let outcome = scrub_pass(&meta, &d).await;
    (meta, outcome)
}

/// **(A)** A corrupt committed-part fragment (one flipped bit) is detected, excluded, and its
/// chunk enqueued for reconstruction; the pass answers `Changed`.
#[tokio::test]
async fn scrub_detects_a_corrupt_committed_part_fragment() {
    capture_audit();
    let chunk: ChunkId = 0xA01;
    let (meta, outcome) =
        scrub_over_one_committed_part("a1", chunk, 0, Some(corrupt_fragment(chunk))).await;
    let outcome = outcome.expect("scrub returns Ok over one corrupt fragment");

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "scrub must enqueue a repair for the corrupt committed-part fragment: {outcome:?}"
    );
    assert!(
        meta.holds(&repair_key(chunk)),
        "the corrupt chunk was not left in `wyrd_core::repair::queued_repairs`"
    );
}

/// **(A)** A committed-part fragment simply ABSENT from its placed D server (issue #330) is
/// detected the same way: excluded and enqueued, the pass answers `Changed`.
#[tokio::test]
async fn scrub_detects_a_missing_committed_part_fragment() {
    capture_audit();
    let chunk: ChunkId = 0xA02;
    let (meta, outcome) = scrub_over_one_committed_part("a2", chunk, 0, None).await;
    let outcome = outcome.expect("scrub returns Ok over one missing fragment");

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "scrub must enqueue a repair for the missing committed-part fragment: {outcome:?}"
    );
    assert!(
        meta.holds(&repair_key(chunk)),
        "the chunk with the missing fragment was not left in `queued_repairs`"
    );
}

/// **(A)** An INTACT fragment whose header names a DIFFERENT EC scheme from the part record's
/// own `ChunkRef` is a misplaced/misencoded fragment — excluded and enqueued exactly as
/// corruption is. This proves the PART record's scheme is what scrub checks, not the mere
/// presence of a decodable fragment.
#[tokio::test]
async fn scrub_detects_a_committed_part_fragment_with_the_wrong_ec_scheme() {
    capture_audit();
    let chunk: ChunkId = 0xA03;
    let (meta, outcome) =
        scrub_over_one_committed_part("a3", chunk, 0, Some(wrong_scheme_fragment(chunk))).await;
    let outcome = outcome.expect("scrub returns Ok over one wrong-scheme fragment");

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "scrub must enqueue a repair for a fragment whose header disagrees with the part \
         record's own EC scheme: {outcome:?}"
    );
    assert!(
        meta.holds(&repair_key(chunk)),
        "the wrong-scheme chunk was not left in `queued_repairs`"
    );
}

/// **(A) control:** every fragment intact — nothing is queued, and the pass answers
/// `Satisfied`. Without this, a scrub that queued every chunk unconditionally would pass A's
/// three positive legs above for the wrong reason.
#[tokio::test]
async fn scrub_control_intact_committed_part_fragment_queues_nothing() {
    capture_audit();
    let chunk: ChunkId = 0xA04;
    let (meta, outcome) =
        scrub_over_one_committed_part("a4", chunk, 0, Some(valid_fragment(chunk))).await;
    let outcome = outcome.expect("scrub returns Ok");

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "an intact committed-part fragment must queue nothing: {outcome:?}"
    );
    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub enqueued a repair for an intact committed-part fragment"
    );
}

// ---- (A') a committed map supersedes the part record for the chunk it names ----------------------

/// **(A') harness:** a published upload whose `part:` record — kept until its retirement drain —
/// still places `chunk` on server 3, while the committed inode the publication wrote now places it
/// on server 0: reconstruction moved the fragment there after the copy on server 3 was lost, and
/// nothing updates a part record when it does. `on_part` / `on_committed` put an intact fragment
/// at either position; then one scrub pass.
async fn scrub_over_a_superseded_part(
    pair: &str,
    chunk: ChunkId,
    on_part: bool,
    on_committed: bool,
) -> (Meta, Reconciled) {
    let meta = Meta::new();
    let d = disks();
    let id = upload(pair);
    meta.seed(mpu_key(&id), session_completed());
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(&[chunk_ref(chunk, EcScheme::None, &[3])]),
    );
    meta.seed(
        inode_key(PUBLISHED),
        committed_inode(&[chunk_ref(chunk, EcScheme::None, &[0])]),
    );
    if on_part {
        place(&d, 3, frag(chunk, 0), valid_fragment(chunk));
    }
    if on_committed {
        place(&d, 0, frag(chunk, 0), valid_fragment(chunk));
    }
    let outcome = scrub_pass(&meta, &d).await.expect("the scrub pass runs");
    (meta, outcome)
}

/// **(A')** The part record's old position is empty and the committed one is intact: nothing is
/// queued and the pass answers `Satisfied`. A scrub that checked the part record's placement too
/// would enqueue the chunk here every pass, and reconstruction — which repairs against the
/// committed map — would find it whole and drain it every pass: the two loops undoing each other
/// for as long as the part record lives.
#[tokio::test]
async fn a_part_placement_a_committed_map_supersedes_is_not_checked() {
    capture_audit();
    let chunk: ChunkId = 0xA11;
    let (meta, outcome) = scrub_over_a_superseded_part("a5", chunk, false, true).await;

    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub enqueued a published chunk for the empty position its leftover part record still \
         names, though the committed map places it elsewhere and it is intact there"
    );
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the published chunk is intact where the committed map places it: {outcome:?}"
    );
}

/// **(A') control:** the same records, with the fragment lost at the committed position and a
/// copy still intact at the part record's old one. The chunk is enqueued: scrub checks a
/// superseded part's chunk where the committed map places it, rather than skipping the chunk,
/// and bytes left at the old position do not hide the loss.
#[tokio::test]
async fn a_superseded_part_is_checked_where_the_committed_map_places_it() {
    capture_audit();
    let chunk: ChunkId = 0xA12;
    let (meta, outcome) = scrub_over_a_superseded_part("a6", chunk, true, false).await;

    assert!(
        meta.holds(&repair_key(chunk)),
        "scrub did not enqueue a published chunk whose fragment is missing where the committed \
         map places it"
    );
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "scrub enqueued a repair, so it must answer `Changed`: {outcome:?}"
    );
}

// ---- (B) scrub leaves in-flight chunks alone -----------------------------------------------------

/// **(B)** A chunk named only by an owned `sidx:` entry — no `part:` record has been committed
/// for it yet — with its fragment missing on its planned server queues nothing: checking needs
/// the COMMITTED scheme, which an in-flight chunk does not carry (`0016:776-781`).
#[tokio::test]
async fn scrub_leaves_an_sidx_only_chunk_alone() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let id = upload("b1");
    let chunk: ChunkId = 0xB01;
    meta.seed(mpu_key(&id), session_open());
    let key = sidx_key(&id, part_no(2), chunk);
    meta.seed(key.clone(), owned_entry(&id, &key, EcScheme::None, &[0]));
    // No fragment placed anywhere — missing, exactly like leg A's positive, except this chunk
    // has no COMMITTED part record naming it yet.

    let outcome = scrub_pass(&meta, &d).await.expect("scrub returns Ok");

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "scrub must not treat an in-flight owned entry's fragment as a finding: {outcome:?}"
    );
    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub enqueued a repair for a chunk only an owned `sidx:` entry names"
    );
}

// ---- (C) scrub fails closed on what it cannot read -----------------------------------------------

/// **(C)** One `part:` record whose value will not decode. The pass still checks every OTHER
/// fragment — leg A's corrupt chunk, seeded here under a different part number of the SAME
/// session — names the unreadable record on the audit seam, and answers `Blocked`: scrub's rule
/// for an unreadable committed map (`scrub.rs:99-116`, `:205-215`), applied to the staged read.
#[tokio::test]
async fn scrub_fails_closed_on_an_unreadable_part_record_but_checks_the_rest() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let id = upload("c1");
    meta.seed(mpu_key(&id), session_open());

    // A readable, corrupt part — proves the unreadable record below does not stop scrub from
    // checking every other one.
    let corrupt_chunk: ChunkId = 0xC01;
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(&[chunk_ref(corrupt_chunk, EcScheme::None, &[0])]),
    );
    place(
        &d,
        0,
        frag(corrupt_chunk, 0),
        corrupt_fragment(corrupt_chunk),
    );

    let bad_key = part_key(&id, part_no(2));
    let bad_value = b"{\"chunks\":\"not a chunk list\"}";
    assert!(decode_part_record(bad_value).is_err());
    meta.seed(bad_key.clone(), Bytes::from_static(bad_value));

    let outcome = scrub_pass(&meta, &d)
        .await
        .expect("an unreadable part record is contained, never an Err");

    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "scrub must refuse to certify the store while a committed part record is unreadable: \
         {outcome:?}"
    );
    assert!(
        meta.holds(&repair_key(corrupt_chunk)),
        "scrub must still enqueue the corrupt fragment of a DIFFERENT, readable part record — \
         one damaged record must not abort the rest of the pass"
    );
    let name = String::from_utf8(bad_key).expect("a `part:` key is ASCII");
    assert!(
        named_on_audit_seam(SCRUB_AUDIT, name.as_bytes()),
        "scrub withheld certification over the unreadable part record {name} without naming it \
         on its audit seam"
    );
}

/// **(C)** A store fault while reading a session's `part:` range fails the whole pass with
/// `Err` — as it fails GC's own staged read of the same range.
#[tokio::test]
async fn scrub_fails_the_pass_on_a_store_fault_reading_a_part_range() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let id = upload("c2");
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(&[chunk_ref(0xC21, EcScheme::None, &[0])]),
    );
    meta.fail_reads_of(&part_range(&id));

    let fault = scrub_pass(&meta, &d).await.expect_err(
        "a store fault under a session's `part:` range must fail the pass, as it fails GC — a \
         pass that went on would enqueue or certify over a reading with a hole in it",
    );
    let ReconcileError::Store(store_fault) = &fault else {
        panic!("a store fault must surface as `ReconcileError::Store`: {fault}");
    };
    assert!(
        store_fault
            .to_string()
            .contains("injected metadata-store fault"),
        "the failure must wrap the store's own error: {store_fault}"
    );
}

/// **(C-order)** Staged damage is named BEFORE the committed read can fail. One session holds a
/// `part:` record with a malformed placement and one whose value will not decode; the `inode:`
/// read that follows the staged one FAILS. The pass ends with `Err` — a store fault under the
/// committed read is never contained — and both records must ALREADY be on the audit seam: scrub
/// emits them the moment the staged reading returns (`scrub.rs:144-163`), so the committed read's
/// `?` cannot carry the names away. A damaged staged record has no repair path yet, so its name is
/// all an operator gets. Reconstruction pins the same promise (`staged_protection.rs`, leg I).
#[tokio::test]
async fn staged_damage_is_named_even_when_the_committed_read_then_faults() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let id = upload("c3");
    meta.seed(mpu_key(&id), session_open());

    let chunk: ChunkId = 0xC31;
    let malformed_key = part_key(&id, part_no(1));
    meta.seed(
        malformed_key.clone(),
        part_record(&[chunk_ref(chunk, EcScheme::None, &[])]),
    );
    let unreadable_key = part_key(&id, part_no(2));
    let unreadable_value = b"{\"chunks\":\"not a chunk list\"}";
    assert!(decode_part_record(unreadable_value).is_err());
    meta.seed(unreadable_key.clone(), Bytes::from_static(unreadable_value));

    // The committed read is next after the staged one, and it fails.
    meta.fail_reads_of(b"inode:");

    let fault = scrub_pass(&meta, &d)
        .await
        .expect_err("a store fault under the committed read must fail the pass");
    let ReconcileError::Store(store_fault) = &fault else {
        panic!("a store fault must surface as `ReconcileError::Store`: {fault}");
    };
    assert!(
        store_fault
            .to_string()
            .contains("injected metadata-store fault"),
        "the failure must wrap the store's own error: {store_fault}"
    );
    assert!(
        one_audit_event_names(
            SCRUB_AUDIT,
            &[&malformed_key, b"malformed-staged-placement"]
        ),
        "the malformed part record {} was found and then lost: the `inode:` fault ended the pass \
         before its name reached the audit seam",
        String::from_utf8_lossy(&malformed_key)
    );
    assert!(
        named_on_audit_seam(SCRUB_AUDIT, &unreadable_key),
        "the unreadable part record {} was found and then lost: the `inode:` fault ended the pass \
         before its name reached the audit seam",
        String::from_utf8_lossy(&unreadable_key)
    );
}

// ---- (A-paged) sessions past the first page of the listing ---------------------------------------

/// The page size scrub's staged reader lists sessions by (`gc.rs:300`, `STAGED_PAGE`; private, so
/// restated here — leg A-paged fails loudly if the two drift apart).
const STAGED_PAGE: usize = 512;

/// The `n`-th of a run of uploads whose `mpu:` keys sort in `n` order.
fn numbered_upload(n: usize) -> UploadId {
    UploadId::new(format!("{n:032x}")).expect("32 lowercase-hex characters")
}

/// **(A-paged)** More `Open` sessions than one page of the listing holds: two full pages and one
/// more. Three of them have a committed part whose fragment is MISSING — the first session, the
/// first one of the SECOND page, and the one alone on the third. All three chunks must be queued.
/// A session loop that stopped at the end of its first page would check one chunk of the three
/// and certify the rest of the store unread. The loop under test is scrub's own
/// (`gc.rs:staged_committed_parts`); GC's paged legs walk `gc.rs:staged_fragments`' loop instead.
#[tokio::test]
async fn scrub_checks_committed_parts_of_sessions_past_the_first_page() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let sessions = 2 * STAGED_PAGE + 1;
    let open = session_open();
    for n in 0..sessions {
        meta.seed(mpu_key(&numbered_upload(n)), open.clone());
    }
    let lost: [(usize, ChunkId); 3] = [(0, 0xA91), (STAGED_PAGE, 0xA92), (2 * STAGED_PAGE, 0xA93)];
    for &(n, chunk) in &lost {
        // Placed on server 1, where nothing is: the fragment is missing.
        meta.seed(
            part_key(&numbered_upload(n), part_no(1)),
            part_record(&[chunk_ref(chunk, EcScheme::None, &[1])]),
        );
    }

    let outcome = scrub_pass(&meta, &d)
        .await
        .expect("a scrub pass over a long session listing");

    for &(n, chunk) in &lost {
        assert!(
            meta.holds(&repair_key(chunk)),
            "scrub never checked the committed part of session {n} of {sessions}, which is on page \
             {} of the session listing: its missing fragment was not queued",
            n / STAGED_PAGE + 1
        );
    }
    // Every chunk was checked — but only a listing longer than one page proves the walk.
    assert_eq!(
        meta.pages_read(b"mpu:"),
        sessions.div_ceil(STAGED_PAGE),
        "this leg needs the session listing to span more than one page: {sessions} sessions no \
         longer take {} pages, so resize it to the reader's page size (`gc.rs`, `STAGED_PAGE`)",
        sessions.div_ceil(STAGED_PAGE)
    );
    assert_eq!(outcome, Reconciled::Changed, "{outcome:?}");
}

// ---- (C') source before destination: a publication between scrub's two reads ---------------------

/// A publication in the making: a `Completing` session whose `part:` record names `chunk` on
/// server 2 with its fragment MISSING there, an unrelated committed `WITNESS` object whose own
/// chunk's fragment is missing on server 3, and the two batches that publish the chunk — the root
/// flip (the committed inode written, the part record KEPT) and the later retirement drain that
/// deletes it (`0016:793-800`, `:941-944`, `:964-966`), exactly as GC's own (C)(ii) fixture builds
/// them (`staged_protection.rs:1271-1312`). The `retire:records:` obligation is left out: scrub
/// reads no `retire:` key.
struct Publication {
    meta: Meta,
    d: [Disk; 4],
    id: UploadId,
    witness: ChunkId,
    flip: WriteBatch,
    drain: WriteBatch,
}

fn publication(pair: &str, chunk: ChunkId) -> Publication {
    let meta = Meta::new();
    let d = disks();
    let id = upload(pair);
    let completing = session_completing();
    meta.seed(mpu_key(&id), completing.clone());
    let placed = chunk_ref(chunk, EcScheme::None, &[2]);
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(std::slice::from_ref(&placed)),
    );
    // No fragment on server 2: the staged chunk is short the one fragment its part record places.

    // The witness: committed for the whole pass, its fragment likewise missing.
    let witness = chunk + 0xF;
    meta.seed(
        inode_key(WITNESS),
        committed_inode(&[chunk_ref(witness, EcScheme::None, &[3])]),
    );

    let flip = WriteBatch::new()
        .require(mpu_key(&id), completing)
        .require_absent(inode_key(PUBLISHED))
        .require_absent(dirent_key(PARENT, OBJECT))
        .put(inode_key(PUBLISHED), committed_inode(&[placed]))
        .put(
            dirent_key(PARENT, OBJECT),
            metadata::encode(&DirentRecord { inode: PUBLISHED }),
        )
        .put(mpu_key(&id), session_completed());
    let drain = WriteBatch::new().delete(part_key(&id, part_no(1)));
    Publication {
        meta,
        d,
        id,
        witness,
        flip,
        drain,
    }
}

/// The `publication` landed as the TWO batches it is, never collapsed into one: right after the
/// root flip applied the store held the committed inode AND the part record, and right after the
/// retirement drain applied, the inode without it (`staged_protection.rs:1318-1341`).
fn assert_published_in_two_batches(meta: &Meta, id: &UploadId) {
    let (inode, record) = (inode_key(PUBLISHED), part_key(id, part_no(1)));
    let after = meta.keys_after_hooks();
    let [Some(flipped), Some(drained)] = after.as_slice() else {
        panic!(
            "the flip and the drain must both land during the pass, or the handoff was not \
             exercised: {:?}",
            meta.hook_outcomes()
        );
    };
    let text = |keys: &[Vec<u8>]| -> Vec<String> {
        keys.iter()
            .map(|key| String::from_utf8_lossy(key).into_owned())
            .collect()
    };
    assert!(
        flipped.contains(&inode) && flipped.contains(&record),
        "right after the root flip the store must hold the committed inode AND the part record — \
         the flip keeps the record, and only the later retirement drain deletes it: {:?}",
        text(flipped)
    );
    assert!(
        drained.contains(&inode) && !drained.contains(&record),
        "right after the retirement drain the store must hold the committed inode and no part \
         record: {:?}",
        text(drained)
    );
}

/// **(C') harness:** the publication lands DURING one scrub pass. The root flip lands right after
/// the first of the pass's two reads of the part range (the source) and the `inode:` scan (the
/// destination) — whichever order the pass makes them in — and the retirement drain after read
/// `drain_after` of the two, as a batch of its own.
async fn publication_during_scrub(pair: &str, chunk: ChunkId, drain_after: usize) {
    let Publication {
        meta,
        d,
        id,
        witness,
        flip,
        drain,
    } = publication(pair, chunk);
    let triggers: [&[u8]; 2] = [&part_range(&id), b"inode:"];
    meta.hook(&triggers, 1, flip);
    meta.hook(&triggers, drain_after, drain);

    let outcome = scrub_pass(&meta, &d).await.expect("the scrub pass runs");

    assert!(
        meta.holds(&repair_key(chunk)),
        "a publication landing during scrub's reads (flip after the first, drain after read \
         {drain_after}) left the chunk's missing fragment unchecked: a pass that reads `inode:` \
         before `part:` sees it in NEITHER class and certifies a lost fragment (X67, \
         `0016:2596`)"
    );
    assert!(
        meta.holds(&repair_key(witness)),
        "the witness object's own missing fragment was not enqueued: this pass checked nothing, \
         so keeping the published chunk proves nothing"
    );
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "scrub enqueued repairs this pass, so it must answer `Changed`: {outcome:?}"
    );
    assert_eq!(
        meta.hook_outcomes(),
        vec![
            Some(CommitOutcome::Committed),
            Some(CommitOutcome::Committed)
        ],
        "the flip and the drain must both land during the pass, or the handoff was not exercised"
    );
    assert_published_in_two_batches(&meta, &id);
}

/// **(C')** first schedule: the flip AND the drain both land between the pass's two reads — the
/// one schedule a destination-first reading sees in neither class.
///
/// Base (and any `inode:`-before-`part:` reading): the missing fragment is never enqueued.
#[tokio::test]
async fn a_publication_flipped_and_drained_between_scrubs_reads_leaves_the_chunk_checked() {
    capture_audit();
    publication_during_scrub("d1", 0xD11, 1).await;
}

/// **(C')** second schedule: the flip lands between the two reads and the drain after the second,
/// so the part record is still there when the source is read whichever order the pass makes them
/// in. Green on a correct pass either way — it is the companion that keeps the first leg honest
/// about WHICH schedule discriminates.
#[tokio::test]
async fn a_publication_flipped_between_scrubs_reads_and_drained_after_leaves_the_chunk_checked() {
    capture_audit();
    publication_during_scrub("d2", 0xD21, 2).await;
}

// ---- (C'') a malformed committed part placement is never identity-filled, nor certified ---------

/// Whether ONE event a pass on this thread emitted on the audit seam `target` names every one of
/// `needles` — so "the record" and "what is wrong with it" are proven to be one signal.
fn one_audit_event_names(target: &str, needles: &[&[u8]]) -> bool {
    let thread = std::thread::current().id();
    audit_log().lock().unwrap().iter().any(|event| {
        event.thread == thread
            && event.target == target
            && needles.iter().all(|needle| {
                let needle = String::from_utf8_lossy(needle);
                event
                    .fields
                    .iter()
                    .any(|field| field.contains(needle.as_ref()))
            })
    })
}

/// **(C'') harness:** a session whose `part:` record places `chunk` on `part_placement` — and,
/// when `committed` is `Some`, a committed inode that places it too, published, with an intact
/// fragment where that placement puts fragment 0. Nothing else is on any server. One scrub pass;
/// returns the part record's key for the audit assertions.
async fn scrub_over_a_part_placement(
    pair: &str,
    chunk: ChunkId,
    part_placement: &[DServerId],
    committed: Option<&[DServerId]>,
) -> (Meta, Vec<u8>, Reconciled) {
    let meta = Meta::new();
    let d = disks();
    let id = upload(pair);
    let state = match committed {
        Some(_) => session_completed(),
        None => session_open(),
    };
    meta.seed(mpu_key(&id), state);
    let key = part_key(&id, part_no(1));
    meta.seed(
        key.clone(),
        part_record(&[chunk_ref(chunk, EcScheme::None, part_placement)]),
    );
    if let Some(placement) = committed {
        meta.seed(
            inode_key(PUBLISHED),
            committed_inode(&[chunk_ref(chunk, EcScheme::None, placement)]),
        );
        if let Some(&dserver) = placement.first() {
            place(&d, dserver, frag(chunk, 0), valid_fragment(chunk));
        }
    }
    let outcome = scrub_pass(&meta, &d)
        .await
        .expect("a malformed placement is contained, never an Err");
    (meta, key, outcome)
}

/// **(C'')** The staged chunk no committed map names, whose `part:` placement is malformed: no
/// repair queued, the record named on the audit seam as a STAGED record, by its `part:` key
/// (C''-audit), and the pass answers `Blocked` — `Satisfied` claims every referenced fragment was
/// checked (`reconciliation.rs:32`), and this chunk's were not.
async fn assert_malformed_part_placement_is_never_certified(
    pair: &str,
    chunk: ChunkId,
    placement: &[DServerId],
) {
    let (meta, key, outcome) = scrub_over_a_part_placement(pair, chunk, placement, None).await;

    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub enqueued a repair for a chunk whose committed part placement is malformed — a \
         filled-in staged placement fabricates positions no record ever held, so the obligation \
         points at a server that was never asked to store the fragment"
    );
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "scrub never checked this staged chunk — its only record's placement cannot be used — so \
         it must not certify the store: {outcome:?}"
    );
    assert!(
        one_audit_event_names(
            SCRUB_AUDIT,
            &[&key, b"malformed-staged-placement", wyrd_traits::chunk_hex(chunk).as_bytes()]
        ),
        "the malformed-placement signal must name the damaged `part:` record {} as a staged record \
         (and its chunk): there is no committed object for an operator to go and find",
        String::from_utf8_lossy(&key)
    );
}

/// **(C'')** An EMPTY part placement. A committed chunk MAP's empty placement is the pre-M3
/// identity fallback (`ChunkRef::checked_fragments`), but a staged record's never is — every one
/// is born with a full placement (`0016:828`) — so this is damage. Identity-filling it would have
/// the pass fetch fragment 0 from D server 0, which no record ever named, and enqueue a phantom
/// repair when that server holds no bytes for it. The same exact-length rule `StagedSet::place`
/// applies for GC (`staged_protection.rs:1922`).
#[tokio::test]
async fn an_empty_committed_part_placement_is_malformed_and_queues_no_phantom_repair() {
    capture_audit();
    assert_malformed_part_placement_is_never_certified("e1", 0xE11, &[]).await;
}

/// **(C'')** A part placement of the wrong length: two D servers for a one-fragment scheme.
#[tokio::test]
async fn a_wrong_length_committed_part_placement_is_never_certified() {
    capture_audit();
    assert_malformed_part_placement_is_never_certified("e2", 0xE12, &[0, 1]).await;
}

/// **(C'') counter-case:** the same damaged part record, but a published committed map also names
/// the chunk, validly, and the chunk is intact there. The chunk IS checked (leg A'), so the
/// leftover record's damage is still named but does not block the pass: whenever a committed map
/// names a chunk, its rule alone decides the answer.
#[tokio::test]
async fn a_malformed_part_placement_a_committed_map_supersedes_does_not_block() {
    capture_audit();
    let chunk: ChunkId = 0xE13;
    let (meta, key, outcome) = scrub_over_a_part_placement("e3", chunk, &[], Some(&[0])).await;

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the committed map places the chunk and it is intact there, so a leftover part record's \
         damage must not block the pass: {outcome:?}"
    );
    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub enqueued a repair for a chunk intact where its committed map places it"
    );
    assert!(
        one_audit_event_names(SCRUB_AUDIT, &[&key, b"malformed-staged-placement"]),
        "the leftover part record's damage must still be named on the audit seam"
    );
}

// ---- (A'-malformed) a MALFORMED committed map supersedes the part record too ---------------------

/// **(A'-malformed)** A committed map names the chunk with a malformed placement (two D servers
/// for a one-fragment scheme), and a part record places it validly on server 3, where nothing is.
/// Scrub does not check the part placement: the committed map names the chunk, so its own rule —
/// report the malformed placement, enqueue nothing (`scrub.rs:95-96` on base) — decides. A scrub
/// that let a MALFORMED committed map leave the part placement in play would enqueue the chunk
/// for server 3's empty position.
#[tokio::test]
async fn a_part_placement_a_malformed_committed_map_supersedes_is_not_checked() {
    capture_audit();
    let chunk: ChunkId = 0xE14;
    let (meta, _key, outcome) = scrub_over_a_part_placement("e4", chunk, &[3], Some(&[0, 1])).await;

    assert!(
        !meta.holds(&repair_key(chunk)),
        "scrub checked the part record's placement of a chunk a committed map names — malformed \
         or not, the committed map's rule decides"
    );
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "a malformed COMMITTED placement answers as it does on base: {outcome:?}"
    );
}

// ---- (C''') a session key the parser rejects ---------------------------------------------------

/// **(C''')** An `mpu:` key naming no upload at all. Its part range cannot even be addressed, so
/// scrub cannot say which chunks that session protects: the record is named on the audit seam and
/// the pass answers `Blocked`, the same containment an undecodable part value gets (leg C) and the
/// same one GC's own staged reader applies to the same key (`staged_protection.rs:1690`). The
/// readable session beside it is still checked in full.
#[tokio::test]
async fn scrub_fails_closed_on_a_session_key_naming_no_upload() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();

    // A readable session whose part's fragment is corrupt — still enqueued.
    let id = upload("0a");
    let corrupt_chunk: ChunkId = 0x0A1;
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part_record(&[chunk_ref(corrupt_chunk, EcScheme::None, &[0])]),
    );
    place(
        &d,
        0,
        frag(corrupt_chunk, 0),
        corrupt_fragment(corrupt_chunk),
    );

    let bad_key = b"mpu:not-an-upload-id".to_vec();
    assert!(parse_mpu_key(&bad_key).is_err());
    meta.seed(bad_key.clone(), session_open());

    let outcome = scrub_pass(&meta, &d)
        .await
        .expect("an unparsable session key is contained, never an Err");

    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "scrub must refuse to certify the store while a session key names no upload — its part \
         range is unaddressable, so the chunks it would have protected were never checked: \
         {outcome:?}"
    );
    assert!(
        meta.holds(&repair_key(corrupt_chunk)),
        "scrub must still check every READABLE session's parts — one unparsable session key must \
         not abort the rest of the pass"
    );
    let name = String::from_utf8(bad_key).expect("the seeded key is ASCII");
    assert!(
        named_on_audit_seam(SCRUB_AUDIT, name.as_bytes()),
        "scrub withheld certification over the unparsable session key {name} without naming it on \
         its audit seam"
    );
}
