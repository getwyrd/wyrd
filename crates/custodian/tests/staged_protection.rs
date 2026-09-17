//! Issue #803 (662.1) — the **staged protection class** for the two custodian passes that delete
//! or mark: GC's reclaim and the post-restore mark gate (proposal 0016 decision 2,
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md:765-893`).
//!
//! A multipart upload's bytes are durable long before they are published: a committed part's
//! fragments are named by its `part:` record, and a still-streaming part's by the owned `sidx:`
//! entries staged for it (#716, #772). Neither is a committed chunk map. On `main` no maintenance
//! pass reads either, so GC reclaims such a fragment as soon as it carries an `orphan:` mark past
//! grace, and the post-restore pass marks it stranded for the next GC pass to delete.
//!
//! Every leg drives the production entry points — `reconcile_step` (GC and scrub),
//! `reconcile_after_restore` and `reconciliation_status` — over in-memory doubles. No client
//! creates a session before the S3 verbs (#508), so every staged record is seeded, as raw JSON
//! the base decoders accept (the shapes of
//! `crates/core/tests/multipart_session_records.rs:81-141`), and each value is round-tripped
//! through `decode_session_record` / `decode_part_record` / `decode_owned_entry` before a pass
//! reads it. Every protection leg also seeds an unprotected
//! control that the pass does reclaim or mark, so a pass that did nothing can never pass for one
//! that protected the staged bytes.
//!
//! The legs:
//! - **A** GC keeps a committed part's and an owned entry's fragments, in every session state, both
//!   at a single-fragment placement and at a real erasure-coded one, and all of them for an upload
//!   whose own records span several pages — while reclaiming, for each staged class, a stray copy
//!   no placement names.
//! - **B** the post-restore pass marks none of them, in either fixture, and a GC pass past grace
//!   then keeps them.
//! - **C** the reads run source before destination: GC's across a part commit and a publication,
//!   and the post-restore pass's across a publication.
//! - **D** neither pass scans the bare `part:` or `sidx:` prefix (a guard).
//! - **E** a staged record a pass cannot read, one it cannot trust, and a store fault under a
//!   staged read each fail closed — while a session VALUE, which no pass decodes, changes neither
//!   pass's answer.
//! - **F** scrub and the drain-status query read no upload record (a guard).
//!
//! Nothing here names a symbol the fix adds: every type, function and string it uses is on
//! `main` already.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::FragmentHeader;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, dirent_key, inode_key, orphan_key, ChunkRef, DirentRecord, EcScheme, InodeId,
    InodeRecord, InodeState,
};
use wyrd_core::multipart::{
    decode_owned_entry, decode_part_record, decode_session_record, mpu_key, parse_mpu_key,
    parse_part_key, parse_sidx_key, part_key, part_range, sidx_key, sidx_range, OwnedEntry,
    PartNumber, StagedPlacement, UploadId, MPU_PREFIX, PART_PREFIX, SIDX_PREFIX,
};
use wyrd_core::repair::repair_key;
use wyrd_custodian::{
    mark_orphaned, reconcile_after_restore, reconcile_step, reconciliation_status, set_lifecycle,
    Custodian, DServerLifecycle, ExpiredPendingPolicy, FencedZone, GcContext, ReconcileError,
    Reconciled, ReconciliationStatus, RestoreReport, ScrubContext,
};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId,
    FragmentId, Health, MetadataStore, PageStart, Result, ScanCapExceeded, ScanPage, WriteBatch,
    SCAN_CAP,
};

/// The reader-safe grace window every pass here runs with.
const GRACE: u64 = 50;
/// The instant every pass runs at, unless a leg moves past a mark it wrote itself.
const NOW: u64 = 10_000;
/// When a leg's own `orphan:` marks were stamped: long past [`GRACE`] at [`NOW`].
const MARKED_AT: u64 = 0;
/// An owned entry's lease: far past every pass's clock, so no leg turns on it.
const LEASE: u64 = NOW * 1_000;
/// The bucket and object every seeded session targets.
const PARENT: InodeId = 42;
const OBJECT: &str = "staged/object";
/// Every seeded session's epoch.
const EPOCH: u64 = 3;
/// The inode a `Completed` session records, and that a publication flip writes.
const PUBLISHED: InodeId = 7;
/// The D server leg F drains.
const DRAINING: DServerId = 3;
/// What the metadata double answers a read it was armed to fail with. It names no key range:
/// naming the failed read is the pass's job, not the store's.
const INJECTED_FAULT: &str = "injected metadata-store fault";

// ---- the metadata double ------------------------------------------------------------------------

/// One read the metadata double was asked for, logged BEFORE it answers — so a read the double
/// was armed to fail is in the log too.
#[derive(Clone, PartialEq, Eq)]
enum Read {
    Get(Vec<u8>),
    Scan(Vec<u8>),
    ScanPage(Vec<u8>),
}

impl Read {
    /// The key or prefix the read named.
    fn subject(&self) -> &[u8] {
        match self {
            Read::Get(key) | Read::Scan(key) | Read::ScanPage(key) => key,
        }
    }
}

/// A read as a failure message shows it: the call, and its key or prefix as text.
impl Debug for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let call = match self {
            Read::Get(_) => "get",
            Read::Scan(_) => "scan",
            Read::ScanPage(_) => "scan_page",
        };
        write!(f, "{call}({})", String::from_utf8_lossy(self.subject()))
    }
}

/// A batch the double commits itself, right after the `fire_after`-th completed read of any of
/// `triggers` — so when a concurrent writer lands is the double's decision, not the pass's.
struct Hook {
    triggers: Vec<Vec<u8>>,
    fire_after: usize,
    seen: usize,
    batch: Option<WriteBatch>,
    outcome: Option<CommitOutcome>,
    /// Every key the store held the instant this hook's batch applied, before anything else ran.
    keys_after: Option<Vec<Vec<u8>>>,
}

/// An in-memory `MetadataStore` over an ordered map, with a read log, a scan cap, armable read
/// faults and read-triggered writers.
///
/// `scan` refuses a result past the cap, and `scan_page` clamps a page to it through the seam's
/// own `page_limit` / `page_start` / `page_cursor` — the shape of every backend's `with_scan_cap`
/// knob, so a lowered cap bites here exactly as it would there.
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    cap: usize,
    reads: Mutex<Vec<Read>>,
    /// Key ranges whose reads fail: any read whose key or prefix overlaps one of them.
    faults: Mutex<Vec<Vec<u8>>>,
    hooks: Mutex<Vec<Hook>>,
}

impl Meta {
    fn new() -> Self {
        Self::with_cap(SCAN_CAP)
    }

    fn with_cap(cap: usize) -> Self {
        Self {
            kv: Mutex::new(BTreeMap::new()),
            cap,
            reads: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
        }
    }

    /// Put a fixture record in place — not a read, and not a pass's write.
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    /// Whether `key` is present, looked up around the read log.
    fn holds(&self, key: &[u8]) -> bool {
        self.kv.lock().unwrap().contains_key(key)
    }

    /// Every key under `prefix`, looked up around the read log.
    fn keys_under(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.kv
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Fail every later read that overlaps `range`, until [`Self::heal`].
    fn fail_reads_of(&self, range: &[u8]) {
        self.faults.lock().unwrap().push(range.to_vec());
    }

    fn heal(&self) {
        self.faults.lock().unwrap().clear();
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

    /// Every key the store held right after each hook's batch applied, in the order the hooks were
    /// armed — `None` for one whose trigger count was never reached.
    fn keys_after_hooks(&self) -> Vec<Option<Vec<Vec<u8>>>> {
        self.hooks
            .lock()
            .unwrap()
            .iter()
            .map(|hook| hook.keys_after.clone())
            .collect()
    }

    fn reads(&self) -> Vec<Read> {
        self.reads.lock().unwrap().clone()
    }

    fn clear_reads(&self) {
        self.reads.lock().unwrap().clear();
    }

    /// Whether any read logged so far named exactly `subject`.
    fn read_of(&self, subject: &[u8]) -> bool {
        self.reads().iter().any(|read| read.subject() == subject)
    }

    /// Log `read`, then refuse it if it overlaps an armed fault. The refusal's text names no key
    /// range: naming the failed read is the pass's job, not the store's.
    fn issue(&self, read: Read) -> Result<()> {
        let subject = read.subject().to_vec();
        self.reads.lock().unwrap().push(read);
        let faulted = self
            .faults
            .lock()
            .unwrap()
            .iter()
            .any(|range| range.starts_with(&subject) || subject.starts_with(range));
        if faulted {
            return Err(BoxError::from(INJECTED_FAULT));
        }
        Ok(())
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
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.issue(Read::Get(key.to_vec()))?;
        let value = self.kv.lock().unwrap().get(key).cloned();
        self.completed(key);
        Ok(value)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.issue(Read::Scan(prefix.to_vec()))?;
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if hits.len() > self.cap {
            return Err(BoxError::from(ScanCapExceeded {
                cap: self.cap,
                prefix: prefix.to_vec(),
            }));
        }
        self.completed(prefix);
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        self.issue(Read::ScanPage(prefix.to_vec()))?;
        let limit = page_limit(limit, self.cap, prefix)?;
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

// ---- the D-server double and the fleet ----------------------------------------------------------

/// One D server's fragments.
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

/// Put a real v1 single-copy fragment of `frag`'s chunk on `dserver` — bytes scrub verifies
/// (leg F); the passes that delete or mark never read them.
fn place(d: &[Disk; 4], dserver: DServerId, frag: FragmentId) {
    let payload = b"staged";
    let bytes = wyrd_chunk_format::encode(
        &FragmentHeader::new_v1(frag.chunk, payload.len() as u64),
        payload,
    );
    d[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .insert(frag, Bytes::from(bytes));
}

fn on_disk(d: &[Disk; 4], dserver: DServerId, frag: FragmentId) -> bool {
    d[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .contains_key(&frag)
}

// ---- the passes ---------------------------------------------------------------------------------

async fn elect() -> (FencedZone, Custodian) {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-staged-protection")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    (zone, custodian)
}

/// One GC pass through the fenced control point.
async fn gc_pass(
    meta: &Meta,
    d: &[Disk; 4],
    now: u64,
) -> std::result::Result<Reconciled, ReconcileError> {
    let (zone, custodian) = elect().await;
    let fleet = fleet(d);
    let ctx = GcContext {
        meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, now).await
}

/// One post-restore pass at [`NOW`].
async fn restore_pass(meta: &Meta, d: &[Disk; 4]) -> Result<RestoreReport> {
    let fleet = fleet(d);
    let ctx = GcContext {
        meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_after_restore(&ctx, NOW).await
}

// ---- the records --------------------------------------------------------------------------------

/// An upload id: 32 lowercase-hex characters from a 2-character pair. Every leg uses its own, so
/// no two legs share a record name on the audit seam.
fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn part_no(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number in range")
}

/// The session states a listed session can be in (`0016:528-602`).
#[derive(Clone, Copy, Debug)]
enum State {
    Open,
    Completing,
    Aborting,
    Completed,
}

const STATES: [State; 4] = [
    State::Open,
    State::Completing,
    State::Aborting,
    State::Completed,
];

/// A session record in `state`, spelled as the base decoder's own encoding and round-tripped
/// through it (the `Completed` shape as `multipart_session_records.rs:298-304` spells it).
fn session(state: State) -> Bytes {
    let state_json = match state {
        State::Open => "{\"kind\":\"Open\"}".to_owned(),
        State::Aborting => "{\"kind\":\"Aborting\"}".to_owned(),
        State::Completing => format!(
            "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":0,\
             \"publish_target\":{{\"parent\":{PARENT},\"name\":\"{OBJECT}\",\"epoch\":{EPOCH}}}}}"
        ),
        State::Completed => format!(
            "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{PUBLISHED},\"version\":1,\
             \"etag\":\"{}-1\",\"completed_at_millis\":950,\"complete_fingerprint\":\"{}\"}}}}",
            "ab".repeat(32),
            "cd".repeat(32)
        ),
    };
    let bytes = format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":{EPOCH},\"attempts\":1,\"state\":{state_json}}}"
    )
    .into_bytes();
    let record = decode_session_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded {state:?} session must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded {state:?} session must be the decoder's own spelling"
    );
    Bytes::from(bytes)
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
/// round-tripped through it. A placement's length is not a decode-time rule, so a wrong-length
/// one decodes too (`multipart.rs:2542-2546`).
fn part(chunks: &[ChunkRef]) -> Bytes {
    let refs: Vec<String> = chunks
        .iter()
        .map(|chunk| String::from_utf8(metadata::encode(chunk).to_vec()).unwrap())
        .collect();
    let len: u64 = chunks.iter().map(|chunk| chunk.len).sum();
    let bytes = format!(
        "{{\"chunks\":[{}],\"len\":{len},\"digest\":\"{}\",\"committed_at_millis\":800,\
         \"session_epoch\":{EPOCH}}}",
        refs.join(","),
        "ef".repeat(32)
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

/// An owned staging entry of `owner`, planned as `placement` under `scheme`, round-tripped through
/// `decode_owned_entry` under `key`. A placement's length is not a decode-time rule here either
/// (`multipart.rs:3530-3538`).
fn owned(owner: &UploadId, key: &[u8], scheme: EcScheme, placement: &[DServerId]) -> Bytes {
    let staged = StagedPlacement::new(scheme, placement.to_vec()).expect("a supported scheme");
    let value = metadata::encode(&OwnedEntry::new(owner.clone(), LEASE, staged).to_pending());
    decode_owned_entry(key, &value)
        .unwrap_or_else(|fault| panic!("the seeded owned entry must decode: {fault}"));
    value
}

const RS_2_1: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };

// ---- the fixtures -------------------------------------------------------------------------------

/// One seeded staged fragment and what names it, for the failure message.
#[derive(Clone, Copy, Debug)]
struct Staged {
    state: State,
    class: &'static str,
    dserver: DServerId,
    frag: FragmentId,
}

/// A session in each of the four states under upload ids `<digit><i>`, each with one committed
/// part (chunk `base + 0x10 + i`) and one owned staging entry (chunk `base + 0x20 + i`), their
/// fragments on disk — nothing marked.
fn seed_every_state(meta: &Meta, d: &[Disk; 4], digit: char, base: ChunkId) -> Vec<Staged> {
    let mut staged = Vec::new();
    for (i, state) in STATES.into_iter().enumerate() {
        let id = upload(&format!("{digit}{i}"));
        meta.seed(mpu_key(&id), session(state));

        let part_chunk = base + 0x10 + i as ChunkId;
        let part_server = (i % 3) as DServerId;
        meta.seed(
            part_key(&id, part_no(1)),
            part(&[chunk_ref(part_chunk, EcScheme::None, &[part_server])]),
        );
        place(d, part_server, frag(part_chunk, 0));
        staged.push(Staged {
            state,
            class: "committed part (`part:`)",
            dserver: part_server,
            frag: frag(part_chunk, 0),
        });

        let owned_chunk = base + 0x20 + i as ChunkId;
        let owned_server = ((i + 1) % 3) as DServerId;
        let key = sidx_key(&id, part_no(2), owned_chunk);
        meta.seed(
            key.clone(),
            owned(&id, &key, EcScheme::None, &[owned_server]),
        );
        place(d, owned_server, frag(owned_chunk, 0));
        staged.push(Staged {
            state,
            class: "owned staging entry (`sidx:`)",
            dserver: owned_server,
            frag: frag(owned_chunk, 0),
        });
    }
    staged
}

/// One healthy `Open` session with a committed part (chunk `base + 1` on server 1) and an owned
/// staging entry (chunk `base + 2` on server 2), plus an unreferenced control fragment (chunk
/// `base + 3` on server 3) — nothing marked.
struct Healthy {
    id: UploadId,
    staged: [(DServerId, FragmentId); 2],
    control: (DServerId, FragmentId),
}

impl Healthy {
    fn seed(meta: &Meta, d: &[Disk; 4], pair: &str, base: ChunkId) -> Self {
        let id = upload(pair);
        meta.seed(mpu_key(&id), session(State::Open));
        meta.seed(
            part_key(&id, part_no(1)),
            part(&[chunk_ref(base + 1, EcScheme::None, &[1])]),
        );
        let key = sidx_key(&id, part_no(2), base + 2);
        meta.seed(key.clone(), owned(&id, &key, EcScheme::None, &[2]));
        let staged = [(1, frag(base + 1, 0)), (2, frag(base + 2, 0))];
        let control = (3, frag(base + 3, 0));
        for (dserver, frag) in staged.into_iter().chain([control]) {
            place(d, dserver, frag);
        }
        Self {
            id,
            staged,
            control,
        }
    }

    fn all(&self) -> impl Iterator<Item = (DServerId, FragmentId)> {
        self.staged.into_iter().chain([self.control])
    }
}

/// Where each fragment of an erasure-coded staged chunk is placed: a **full** placement for
/// `RS(2, 1)`'s three fragments that names a different D server at every index and never fragment
/// `i` on server `i`.
const PART_PLACEMENT: [DServerId; 3] = [2, 0, 1];
const OWNED_PLACEMENT: [DServerId; 3] = [1, 2, 0];

/// One healthy `Open` session whose two staged chunks are **erasure-coded**: a committed part
/// naming chunk `base + 1` and an owned staging entry naming chunk `base + 2`, both `RS(2, 1)` —
/// three fragments each — at the full, non-identity placements above, every fragment on its placed
/// server.
///
/// Every other fixture here stages `EcScheme::None` chunks, whose whole placement is one server at
/// index 0; this is the multi-fragment shape a real upload writes, and the only one where expanding
/// a staged placement can go wrong. A protection that dropped the fragment index, or that resolved
/// the placement through the identity fallback a committed map's empty one gets (fragment `i` on
/// server `i`, `crates/core/src/metadata.rs:164-169`), leaves at least two of these six fragments
/// unprotected — and each is a durable byte its record still names.
///
/// Beside them, a **stray** copy of fragment 0 of EACH staged chunk on server 3, which neither
/// staged placement names: a staged record protects the fragments it places, not every fragment
/// that carries its chunk id, so both passes must still judge these. (A protection that held a
/// chunk whole instead of placing it would keep its stray for as long as the record lives.) One
/// per class, because the two classes reach `place` through different readers — a committed part's
/// recorded placement (`gc.rs:742-744`) and an owned entry's planned one (`gc.rs:713-723`) — so a
/// stray for one says nothing about the other.
struct ErasureCoded {
    placed: Vec<Staged>,
    strays: [(DServerId, FragmentId); 2],
}

impl ErasureCoded {
    fn seed(meta: &Meta, d: &[Disk; 4], pair: &str, base: ChunkId) -> Self {
        let id = upload(pair);
        meta.seed(mpu_key(&id), session(State::Open));
        let part_chunk = base + 1;
        meta.seed(
            part_key(&id, part_no(1)),
            part(&[chunk_ref(part_chunk, RS_2_1, &PART_PLACEMENT)]),
        );
        let owned_chunk = base + 2;
        let key = sidx_key(&id, part_no(2), owned_chunk);
        meta.seed(key.clone(), owned(&id, &key, RS_2_1, &OWNED_PLACEMENT));

        let mut placed = Vec::new();
        for (chunk, class, placement) in [
            (
                part_chunk,
                "erasure-coded committed part (`part:`)",
                PART_PLACEMENT,
            ),
            (
                owned_chunk,
                "erasure-coded owned staging entry (`sidx:`)",
                OWNED_PLACEMENT,
            ),
        ] {
            for (index, dserver) in placement.into_iter().enumerate() {
                let fragment = frag(chunk, index as u16);
                place(d, dserver, fragment);
                placed.push(Staged {
                    state: State::Open,
                    class,
                    dserver,
                    frag: fragment,
                });
            }
        }

        // Server 3 is named by neither PART_PLACEMENT nor OWNED_PLACEMENT, so neither copy is a
        // fragment its record places.
        let strays = [(3, frag(part_chunk, 0)), (3, frag(owned_chunk, 0))];
        for (dserver, fragment) in strays {
            place(d, dserver, fragment);
        }
        Self { placed, strays }
    }
}

async fn mark_all(meta: &Meta, fragments: impl IntoIterator<Item = (DServerId, FragmentId)>) {
    for (dserver, frag) in fragments {
        mark_orphaned(meta, dserver, frag, MARKED_AT)
            .await
            .expect("seeding a mark");
    }
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

/// Install the capture once for the whole test binary, before any pass on any thread runs, so no
/// audit callsite is ever first met with no subscriber in place.
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

    fn record_debug(&mut self, _field: &Field, value: &dyn Debug) {
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

const GC_AUDIT: &str = "wyrd.custodian.gc.audit";
const RESTORE_AUDIT: &str = "wyrd.custodian.restore.audit";

// ---- (A) GC protects both staged classes, in every session state --------------------------------

/// **(A)** A committed part's fragment and an owned entry's, for a session in each of the four
/// states, each carrying an `orphan:` mark past grace: one GC pass keeps all eight of them, marks
/// and all, and reclaims the unprotected control beside them. The mark is what makes the leg bite —
/// unmarked, GC's conservative arm keeps any fragment (`gc.rs:307-310`).
///
/// A fifth session stages two **erasure-coded** chunks ([`ErasureCoded`]), whose six fragments sit
/// at full, non-identity placements: they pin how a staged placement expands, which a
/// single-fragment one cannot. The two stray copies beside them — one per staged class, neither
/// named by a placement — are reclaimed like the control.
///
/// The store's scan cap is 2, so the session listing takes more than one page: a session on a
/// later page is kept exactly as one on the first.
///
/// Base: all fourteen staged fragments reclaimed.
#[tokio::test]
async fn gc_keeps_every_staged_fragment_in_every_session_state() {
    capture_audit();
    let meta = Meta::with_cap(2);
    let d = disks();
    let mut staged = seed_every_state(&meta, &d, '1', 0xA00);
    let erasure_coded = ErasureCoded::seed(&meta, &d, "1a", 0xA40);
    staged.extend(erasure_coded.placed.iter().copied());
    let control = (3, frag(0xAFF, 0));
    place(&d, control.0, control.1);
    mark_all(
        &meta,
        staged
            .iter()
            .map(|s| (s.dserver, s.frag))
            .chain([control])
            .chain(erasure_coded.strays),
    )
    .await;

    let outcome = gc_pass(&meta, &d, NOW).await.expect("the GC pass runs");

    for s in &staged {
        assert!(
            on_disk(&d, s.dserver, s.frag),
            "GC reclaimed the {} fragment {:?} of a {:?} session on server {} — staged bytes \
             with a mark past grace are protected by their record, whatever the session's state",
            s.class,
            s.frag,
            s.state,
            s.dserver
        );
        assert!(
            meta.holds(&orphan_key(s.dserver, s.frag)),
            "GC consumed the mark of the protected {} fragment {:?} ({:?} session)",
            s.class,
            s.frag,
            s.state
        );
    }
    assert!(
        !on_disk(&d, control.0, control.1),
        "the unprotected control survived: this pass reclaimed nothing, so keeping the staged \
         fragments proves nothing"
    );
    for (dserver, fragment) in erasure_coded.strays {
        assert!(
            !on_disk(&d, dserver, fragment),
            "GC kept the stray copy {fragment:?} on server {dserver} — no staged placement names \
             it, and a staged record protects the fragments it places, not every fragment carrying \
             its chunk id"
        );
    }
    assert_eq!(outcome, Reconciled::Changed);
}

// ---- (B) the post-restore pass protects them through the same rule ------------------------------

/// **(B)** The same store, unmarked: the post-restore pass writes no `orphan:` key for any staged
/// fragment and its `stranded_marked` counts the control and the erasure-coded fixture's two stray
/// copies alone; a GC pass past grace then keeps every staged fragment and reclaims the three the
/// restore marked. (Staged counters are #664's.)
///
/// The fifth session's chunks are erasure-coded ([`ErasureCoded`]), for leg A's reason: the mark
/// gate reads the same staged placements, so the same expansion has to hold here.
///
/// Base: every staged fragment marked, then deleted.
#[tokio::test]
async fn restore_marks_no_staged_fragment_and_gc_then_keeps_them() {
    capture_audit();
    let meta = Meta::with_cap(2);
    let d = disks();
    let mut staged = seed_every_state(&meta, &d, '2', 0xB00);
    let erasure_coded = ErasureCoded::seed(&meta, &d, "2a", 0xB40);
    staged.extend(erasure_coded.placed.iter().copied());
    let control = (3, frag(0xBFF, 0));
    place(&d, control.0, control.1);

    let report = restore_pass(&meta, &d)
        .await
        .expect("the post-restore pass runs");

    for s in &staged {
        assert!(
            !meta.holds(&orphan_key(s.dserver, s.frag)),
            "the post-restore pass marked the {} fragment {:?} of a {:?} session stranded — the \
             next GC pass deletes a staged byte its record still names",
            s.class,
            s.frag,
            s.state
        );
    }
    assert!(
        meta.holds(&orphan_key(control.0, control.1)),
        "the unreferenced control was not marked: this pass marked nothing, so leaving the staged \
         fragments unmarked proves nothing: {report:?}"
    );
    for (dserver, fragment) in erasure_coded.strays {
        assert!(
            meta.holds(&orphan_key(dserver, fragment)),
            "the post-restore pass left the stray copy {fragment:?} on server {dserver} unmarked — \
             no staged placement names it, and a staged record protects the fragments it places, \
             not every fragment carrying its chunk id: {report:?}"
        );
    }
    assert_eq!(
        report.stranded_marked, 3,
        "`stranded_marked` must count the control and the two stray copies alone: {report:?}"
    );

    let outcome = gc_pass(&meta, &d, NOW + GRACE)
        .await
        .expect("the GC pass runs");
    for s in &staged {
        assert!(
            on_disk(&d, s.dserver, s.frag),
            "GC reclaimed the {} fragment {:?} of a {:?} session after the post-restore pass",
            s.class,
            s.frag,
            s.state
        );
    }
    assert!(
        !on_disk(&d, control.0, control.1),
        "GC did not reclaim the control the post-restore pass marked"
    );
    for (dserver, fragment) in erasure_coded.strays {
        assert!(
            !on_disk(&d, dserver, fragment),
            "GC did not reclaim the stray copy {fragment:?} on server {dserver} that the \
             post-restore pass marked"
        );
    }
    assert_eq!(outcome, Reconciled::Changed);
}

// ---- (A, B) one upload whose own records span several pages -------------------------------------

/// How many committed parts, and how many owned staging entries, the paging legs seed for their one
/// upload: more than two pages of each at a scan cap of 2.
const OWN_RECORDS: u32 = 5;

/// One `Open` upload holding `OWN_RECORDS` committed parts (part numbers 1-5, chunk
/// `base + 0x10 + n`) and `OWN_RECORDS` owned staging entries (part numbers 6-10, chunk
/// `base + 0x20 + n`), their fragments on servers 0-2, and an unreferenced control fragment (chunk
/// `base + 0xFF` on server 3) — nothing marked. Seeded into a store whose scan cap is 2, so each of
/// the upload's two ranges is three pages long; the fixture checks that no single read can return
/// either range whole.
async fn seed_one_upload_across_pages(
    meta: &Meta,
    d: &[Disk; 4],
    pair: &str,
    base: ChunkId,
) -> (Vec<Staged>, (DServerId, FragmentId)) {
    let id = upload(pair);
    meta.seed(mpu_key(&id), session(State::Open));
    let mut staged = Vec::new();
    for n in 1..=OWN_RECORDS {
        let part_chunk = base + 0x10 + n as ChunkId;
        let part_server = (n % 3) as DServerId;
        meta.seed(
            part_key(&id, part_no(n)),
            part(&[chunk_ref(part_chunk, EcScheme::None, &[part_server])]),
        );
        place(d, part_server, frag(part_chunk, 0));
        staged.push(Staged {
            state: State::Open,
            class: "committed part (`part:`)",
            dserver: part_server,
            frag: frag(part_chunk, 0),
        });

        let owned_chunk = base + 0x20 + n as ChunkId;
        let owned_server = ((n + 1) % 3) as DServerId;
        let key = sidx_key(&id, part_no(OWN_RECORDS + n), owned_chunk);
        meta.seed(
            key.clone(),
            owned(&id, &key, EcScheme::None, &[owned_server]),
        );
        place(d, owned_server, frag(owned_chunk, 0));
        staged.push(Staged {
            state: State::Open,
            class: "owned staging entry (`sidx:`)",
            dserver: owned_server,
            frag: frag(owned_chunk, 0),
        });
    }
    let control = (3, frag(base + 0xFF, 0));
    place(d, control.0, control.1);
    assert!(
        meta.scan(&part_range(&id)).await.is_err() && meta.scan(&sidx_range(&id)).await.is_err(),
        "the fixture must hold more of this upload's records than one read of either of its ranges \
         may return"
    );
    meta.clear_reads();
    (staged, control)
}

/// **(A)** One `Open` upload whose own records outnumber a page: five committed parts and five
/// owned staging entries, every fragment marked past grace, in a store whose scan cap of 2 makes
/// each of the upload's two ranges three pages long. One GC pass keeps all ten, marks and all, and
/// reclaims the control. A reading that stopped after the first page of a range would protect two
/// of its five records and leave the other three to be reclaimed.
///
/// Base: all ten reclaimed.
#[tokio::test]
async fn gc_keeps_every_staged_fragment_of_an_upload_whose_records_span_pages() {
    capture_audit();
    let meta = Meta::with_cap(2);
    let d = disks();
    let (staged, control) = seed_one_upload_across_pages(&meta, &d, "b1", 0x2A00).await;
    mark_all(
        &meta,
        staged.iter().map(|s| (s.dserver, s.frag)).chain([control]),
    )
    .await;

    let outcome = gc_pass(&meta, &d, NOW).await.expect("the GC pass runs");

    for s in &staged {
        assert!(
            on_disk(&d, s.dserver, s.frag),
            "GC reclaimed the {} fragment {:?} on server {} of an upload whose own records span \
             several pages — a staged reading must walk every page of each of the upload's ranges",
            s.class,
            s.frag,
            s.dserver
        );
        assert!(
            meta.holds(&orphan_key(s.dserver, s.frag)),
            "GC consumed the mark of the protected {} fragment {:?}",
            s.class,
            s.frag
        );
    }
    assert!(
        !on_disk(&d, control.0, control.1),
        "the unprotected control survived: this pass reclaimed nothing, so keeping the staged \
         fragments proves nothing"
    );
    assert_eq!(outcome, Reconciled::Changed);
}

/// **(B)** The same upload, unmarked: the post-restore pass marks none of its ten staged fragments
/// and its `stranded_marked` counts the control alone; a GC pass past grace then keeps all ten and
/// reclaims the control.
///
/// Base: all ten marked, then deleted.
#[tokio::test]
async fn restore_marks_no_staged_fragment_of_an_upload_whose_records_span_pages() {
    capture_audit();
    let meta = Meta::with_cap(2);
    let d = disks();
    let (staged, control) = seed_one_upload_across_pages(&meta, &d, "b2", 0x2B00).await;

    let report = restore_pass(&meta, &d)
        .await
        .expect("the post-restore pass runs");

    for s in &staged {
        assert!(
            !meta.holds(&orphan_key(s.dserver, s.frag)),
            "the post-restore pass marked the {} fragment {:?} on server {} of an upload whose own \
             records span several pages — a staged reading must walk every page of each of the \
             upload's ranges: {report:?}",
            s.class,
            s.frag,
            s.dserver
        );
    }
    assert!(
        meta.holds(&orphan_key(control.0, control.1)),
        "the unreferenced control was not marked: this pass marked nothing, so leaving the staged \
         fragments unmarked proves nothing: {report:?}"
    );
    assert_eq!(
        report.stranded_marked, 1,
        "`stranded_marked` must count the control alone: {report:?}"
    );

    let outcome = gc_pass(&meta, &d, NOW + GRACE)
        .await
        .expect("the GC pass runs");
    for s in &staged {
        assert!(
            on_disk(&d, s.dserver, s.frag),
            "GC reclaimed the {} fragment {:?} after the post-restore pass",
            s.class,
            s.frag
        );
    }
    assert!(
        !on_disk(&d, control.0, control.1),
        "GC did not reclaim the control the post-restore pass marked"
    );
    assert_eq!(outcome, Reconciled::Changed);
}

// ---- (C) source before destination, across both handoffs ----------------------------------------

/// **(C)(i)** A part commit — ONE batch that deletes the chunk's owned `sidx:` entry and writes its
/// `part:` record (`0016:782-784`) — lands right after the first of the two reads involved
/// completes, whichever GC issues first: the owned range (the source) or the part range (the
/// destination). Read source first, the chunk is in the owned snapshot; read destination first, it
/// is in neither. Its fragment carries a mark past grace throughout.
///
/// Base: reclaimed.
#[tokio::test]
async fn a_part_commit_between_its_two_reads_leaves_the_chunk_protected() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let id = upload("c1");
    let chunk: ChunkId = 0xC11;
    let open = session(State::Open);
    meta.seed(mpu_key(&id), open.clone());
    let owned_key = sidx_key(&id, part_no(1), chunk);
    meta.seed(
        owned_key.clone(),
        owned(&id, &owned_key, EcScheme::None, &[1]),
    );
    place(&d, 1, frag(chunk, 0));
    let control = (3, frag(0xC1F, 0));
    place(&d, control.0, control.1);
    mark_all(&meta, [(1, frag(chunk, 0)), control]).await;

    let commit = WriteBatch::new()
        .require(mpu_key(&id), open)
        .delete(owned_key.clone())
        .put(
            part_key(&id, part_no(1)),
            part(&[chunk_ref(chunk, EcScheme::None, &[1])]),
        );
    meta.hook(&[&sidx_range(&id), &part_range(&id)], 1, commit);

    let outcome = gc_pass(&meta, &d, NOW).await.expect("the GC pass runs");

    assert!(
        on_disk(&d, 1, frag(chunk, 0)),
        "a part commit landing between GC's reads of the owned range and the part range got the \
         chunk reclaimed — a reading that took the destination before the source sees it in \
         neither class (`0016:782-800`)"
    );
    assert!(
        !on_disk(&d, control.0, control.1),
        "the unprotected control survived: this pass reclaimed nothing"
    );
    assert_eq!(outcome, Reconciled::Changed);
    assert_eq!(
        meta.hook_outcomes(),
        vec![Some(CommitOutcome::Committed)],
        "the part commit never landed during the pass, so the handoff was not exercised"
    );
    assert!(
        !meta.holds(&owned_key) && meta.holds(&part_key(&id, part_no(1))),
        "the part commit must have moved the chunk from `sidx:` to `part:`"
    );
}

/// A publication in the making: a `Completing` session whose `part:` record names `chunk` (its
/// fragment on server 2), an unreferenced control (chunk `chunk + 0xF` on server 3), and the two
/// batches that publish the chunk — nothing marked. The root flip is ONE batch writing the
/// committed inode and moving the session to `Completed`, the part record left in place; the
/// retirement drain is a separate, later batch deleting that part record (`0016:793-800`,
/// `:941-944`, `:964-966`). The `retire:records:` obligation itself is left out: no pass in this
/// slice reads `retire:` (#804's), and the drain batch is what deletes the record.
struct Publication {
    meta: Meta,
    d: [Disk; 4],
    id: UploadId,
    control: (DServerId, FragmentId),
    flip: WriteBatch,
    drain: WriteBatch,
}

fn publication(pair: &str, chunk: ChunkId) -> Publication {
    let meta = Meta::new();
    let d = disks();
    let id = upload(pair);
    let completing = session(State::Completing);
    meta.seed(mpu_key(&id), completing.clone());
    let placed = chunk_ref(chunk, EcScheme::None, &[2]);
    meta.seed(
        part_key(&id, part_no(1)),
        part(std::slice::from_ref(&placed)),
    );
    place(&d, 2, frag(chunk, 0));
    let control = (3, frag(chunk + 0xF, 0));
    place(&d, control.0, control.1);

    let published = InodeRecord {
        size: placed.len,
        chunk_map: vec![placed].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let flip = WriteBatch::new()
        .require(mpu_key(&id), completing)
        .require_absent(inode_key(PUBLISHED))
        .require_absent(dirent_key(PARENT, OBJECT))
        .put(inode_key(PUBLISHED), metadata::encode(&published))
        .put(
            dirent_key(PARENT, OBJECT),
            metadata::encode(&DirentRecord { inode: PUBLISHED }),
        )
        .put(mpu_key(&id), session(State::Completed));
    let drain = WriteBatch::new().delete(part_key(&id, part_no(1)));
    Publication {
        meta,
        d,
        id,
        control,
        flip,
        drain,
    }
}

/// The `publication` landed as the TWO batches it is (`0016:793-800`, `:941-944`, `:964-966`),
/// never collapsed into one: right after the root flip applied, the store held the committed inode
/// AND the part record — the flip keeps the record — and right after the retirement drain applied,
/// the inode without it.
fn assert_published_in_two_batches(meta: &Meta, id: &UploadId) {
    let (inode, part_record) = (inode_key(PUBLISHED), part_key(id, part_no(1)));
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
        flipped.contains(&inode) && flipped.contains(&part_record),
        "right after the root flip the store must hold the committed inode AND the part record — \
         the flip keeps the record, and only the later retirement drain deletes it: {:?}",
        text(flipped)
    );
    assert!(
        drained.contains(&inode) && !drained.contains(&part_record),
        "right after the retirement drain the store must hold the committed inode and no part \
         record: {:?}",
        text(drained)
    );
}

/// **(C)(ii)** harness: the `publication` lands during a GC pass whose every fragment carries a
/// mark past grace. The root flip lands right after the first of GC's reads of the part range (the
/// source) and the `inode:` scan (the destination); the retirement drain lands after read
/// `drain_after` of the two, as a batch of its own (`assert_published_in_two_batches`).
async fn publication_during_gc(pair: &str, chunk: ChunkId, drain_after: usize) {
    let Publication {
        meta,
        d,
        id,
        control,
        flip,
        drain,
    } = publication(pair, chunk);
    mark_all(&meta, [(2, frag(chunk, 0)), control]).await;
    let triggers: [&[u8]; 2] = [&part_range(&id), b"inode:"];
    meta.hook(&triggers, 1, flip);
    meta.hook(&triggers, drain_after, drain);

    let outcome = gc_pass(&meta, &d, NOW).await.expect("the GC pass runs");

    assert!(
        on_disk(&d, 2, frag(chunk, 0)),
        "a publication landing during GC's reads (flip after the first, drain after read \
         {drain_after}) got the published chunk reclaimed — a reading that took the committed \
         inodes before the part records sees it in neither class (X67, `0016:2596`)"
    );
    assert!(
        !on_disk(&d, control.0, control.1),
        "the unprotected control survived: this pass reclaimed nothing"
    );
    assert_eq!(outcome, Reconciled::Changed);
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

/// **(C)(ii)** first schedule: the flip AND the drain both land between the two reads — the one
/// schedule a destination-first reading sees in neither class.
///
/// Base: reclaimed.
#[tokio::test]
async fn a_publication_flipped_and_drained_between_the_reads_leaves_the_chunk_protected() {
    capture_audit();
    publication_during_gc("c2", 0xC21, 1).await;
}

/// **(C)(ii)** second schedule: the flip lands between the two reads and the drain after the
/// second.
///
/// Base: reclaimed.
#[tokio::test]
async fn a_publication_flipped_between_the_reads_and_drained_after_leaves_the_chunk_protected() {
    capture_audit();
    publication_during_gc("c3", 0xC31, 2).await;
}

/// **(C)(ii)** harness, for the post-restore pass: the `publication` lands while that pass reads,
/// its fragment unmarked. The pass reads the committed namespace twice — its reference build, then
/// the report's reading — so three reads are involved: the upload's part range and the two
/// `inode:` scans. The root flip lands right after the `flip_after`-th of them to complete and the
/// drain, a batch of its own, right after the `drain_after`-th, whichever order the pass issues
/// them in (`assert_published_in_two_batches`). The published chunk's fragment must stay unmarked,
/// and the pass must have read the upload's owned range, then its part range, before its first
/// `inode:` scan — the order its docs claim.
async fn publication_during_restore(
    pair: &str,
    chunk: ChunkId,
    flip_after: usize,
    drain_after: usize,
) {
    let Publication {
        meta,
        d,
        id,
        control,
        flip,
        drain,
    } = publication(pair, chunk);
    let triggers: [&[u8]; 2] = [&part_range(&id), b"inode:"];
    meta.hook(&triggers, flip_after, flip);
    meta.hook(&triggers, drain_after, drain);

    let report = restore_pass(&meta, &d)
        .await
        .expect("the post-restore pass runs");

    assert!(
        !meta.holds(&orphan_key(2, frag(chunk, 0))),
        "a publication landing during the post-restore pass (flip after read {flip_after}, drain \
         after read {drain_after} of the part range and the two `inode:` scans) got the published \
         chunk marked stranded — a pass that reads the staged class after the committed namespace \
         sees it in no reading, and GC then deletes the object's bytes (X67, `0016:2596`): \
         {report:?}"
    );
    assert!(
        meta.holds(&orphan_key(control.0, control.1)),
        "the unreferenced control was not marked: this pass marked nothing: {report:?}"
    );
    assert_eq!(
        report.stranded_marked, 1,
        "`stranded_marked` must count the control alone: {report:?}"
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

    let reads = meta.reads();
    let first = |subject: &[u8]| reads.iter().position(|read| read.subject() == subject);
    let last = |subject: &[u8]| reads.iter().rposition(|read| read.subject() == subject);
    let (owned_range, parts_range) = (sidx_range(&id), part_range(&id));
    let in_order = matches!(
        (last(&owned_range), first(&parts_range), last(&parts_range), first(b"inode:")),
        (Some(owned_last), Some(parts_first), Some(parts_last), Some(inode_first))
            if owned_last < parts_first && parts_last < inode_first
    );
    assert!(
        in_order,
        "the post-restore pass must read the upload's owned range, then its part range, before \
         its first `inode:` scan: {reads:?}"
    );
}

/// **(C)(ii)** for the post-restore pass, first schedule: the flip and the drain both land right
/// after the second of the three reads to complete. For a pass that reads the staged class first,
/// that is between its two readings of the committed namespace; for one that read the staged class
/// after both of them, it is the one landing that pass sees in no reading.
///
/// Base: marked.
#[tokio::test]
async fn restore_leaves_unmarked_a_chunk_flipped_and_drained_after_read_2() {
    capture_audit();
    publication_during_restore("c4", 0xC41, 2, 2).await;
}

/// **(C)(ii)** for the post-restore pass, second schedule: the flip and the drain both land right
/// after the first of the three reads to complete.
///
/// Base: red at the read-order assertion (the base pass reads no upload record).
#[tokio::test]
async fn restore_leaves_unmarked_a_chunk_flipped_and_drained_after_read_1() {
    capture_audit();
    publication_during_restore("c5", 0xC51, 1, 1).await;
}

/// **(C)(ii)** for the post-restore pass, third schedule: the flip lands after the first of the
/// three reads and the drain after the second.
///
/// Base: red at the read-order assertion (the base pass reads no upload record).
#[tokio::test]
async fn restore_leaves_unmarked_a_chunk_flipped_after_read_1_and_drained_after_read_2() {
    capture_audit();
    publication_during_restore("c6", 0xC61, 1, 2).await;
}

// ---- (D) bounded per-session reads --------------------------------------------------------------

/// **(D)** With the store's scan cap lowered below the number of sessions holding parts, a global
/// `scan("part:")` fails — and GC and the post-restore pass both still succeed, issuing no `scan`
/// or `scan_page` of the bare `part:` or `sidx:` prefix: staged records are read per session,
/// through each session's bounded ranges (`0016:890`).
///
/// A guard: green on base.
#[tokio::test]
async fn gc_and_restore_never_scan_a_whole_staged_namespace() {
    capture_audit();
    const CAP: usize = 3;
    let meta = Meta::with_cap(CAP);
    let d = disks();
    for i in 0..=CAP {
        let id = upload(&format!("d{i}"));
        meta.seed(mpu_key(&id), session(State::Open));
        let part_chunk = 0xD10 + i as ChunkId;
        let part_server = (i % 3) as DServerId;
        meta.seed(
            part_key(&id, part_no(1)),
            part(&[chunk_ref(part_chunk, EcScheme::None, &[part_server])]),
        );
        place(&d, part_server, frag(part_chunk, 0));
        let owned_chunk = 0xD20 + i as ChunkId;
        let owned_server = ((i + 1) % 3) as DServerId;
        let key = sidx_key(&id, part_no(2), owned_chunk);
        meta.seed(
            key.clone(),
            owned(&id, &key, EcScheme::None, &[owned_server]),
        );
        place(&d, owned_server, frag(owned_chunk, 0));
    }
    assert!(
        meta.scan(PART_PREFIX).await.is_err() && meta.scan(SIDX_PREFIX).await.is_err(),
        "the fixture must hold more staged records than one scan of either namespace may return"
    );
    meta.clear_reads();

    let gc = gc_pass(&meta, &d, NOW).await;
    assert!(gc.is_ok(), "the GC pass failed: {:?}", gc.err());
    let restore = restore_pass(&meta, &d).await;
    assert!(
        restore.is_ok(),
        "the post-restore pass failed: {:?}",
        restore.err()
    );

    let whole: Vec<Read> = meta
        .reads()
        .into_iter()
        .filter(|read| {
            matches!(read, Read::Scan(_) | Read::ScanPage(_))
                && (read.subject() == PART_PREFIX || read.subject() == SIDX_PREFIX)
        })
        .collect();
    assert!(
        whole.is_empty(),
        "a pass read a whole staged namespace instead of each session's range: {whole:?}"
    );
}

// ---- (E)(i) a staged record the passes cannot read ----------------------------------------------

/// **(E)(i)** harness: a healthy session, plus one staged record that `damage` seeds and returns
/// the key of. The post-restore pass (over the unmarked store) marks nothing anywhere and names
/// the record in `RestoreReport::unresolvable`; GC (with every fragment then marked past grace)
/// reclaims nothing and answers `Blocked` — the incomplete-set containment `gc.rs:348-355` gives an
/// unreadable committed record (ADR-0045 decision 3,
/// `docs/design/adr/0045-metadata-validation-boundaries.md:55-59`). Each pass also names the
/// record on its own audit seam: for the GC loop, which has no report, that is the only place an
/// operator learns which record is holding every reclaim back.
///
/// Base: the restore marks the staged fragments and the control; GC reclaims them.
async fn an_unreadable_staged_record_withholds_both_passes(
    pair: &str,
    base: ChunkId,
    damage: impl FnOnce(&Meta, &UploadId) -> Vec<u8>,
) {
    let meta = Meta::new();
    let d = disks();
    let healthy = Healthy::seed(&meta, &d, pair, base);
    let damaged = damage(&meta, &healthy.id);
    let name = String::from_utf8(damaged).expect("every damaged key here is ASCII");

    let report = restore_pass(&meta, &d)
        .await
        .expect("an unreadable staged record is named, never an Err that blanks the report");
    for (dserver, frag) in healthy.all() {
        assert!(
            !meta.holds(&orphan_key(dserver, frag)),
            "the post-restore pass marked {frag:?} on server {dserver} while the staged record \
             {name} could not be read — the fragments it owns cannot be told from strays: \
             {report:?}"
        );
    }
    assert_eq!(report.stranded_marked, 0, "{report:?}");
    assert!(
        report.unresolvable.contains(&name),
        "the unreadable staged record {name} is not named in the report: {report:?}"
    );
    assert!(
        named_on_audit_seam(RESTORE_AUDIT, name.as_bytes()),
        "the post-restore pass withheld every mark over the unreadable staged record {name} \
         without naming it on its audit seam"
    );

    mark_all(&meta, healthy.all()).await;
    let outcome = gc_pass(&meta, &d, NOW)
        .await
        .expect("an unreadable staged record is contained, never an Err");
    for (dserver, frag) in healthy.all() {
        assert!(
            on_disk(&d, dserver, frag),
            "GC reclaimed {frag:?} on server {dserver} while the staged record {name} could not \
             be read"
        );
    }
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "GC must refuse to certify while the staged record {name} is unreadable"
    );
    // The unattended GC loop's only word on WHICH record stalls it fleet-wide: `Blocked` says that
    // something is unreadable, and the audit seam is where an operator finds what to repair.
    assert!(
        named_on_audit_seam(GC_AUDIT, name.as_bytes()),
        "GC withheld every reclaim over the unreadable staged record {name} without naming it on \
         its audit seam"
    );
}

/// **(E)(i)** A `part:` value that will not decode.
#[tokio::test]
async fn an_undecodable_part_record_withholds_both_passes() {
    capture_audit();
    an_unreadable_staged_record_withholds_both_passes("e1", 0xE10, |meta, id| {
        let key = part_key(id, part_no(2));
        let value = b"{\"chunks\":\"not a chunk list\"}";
        assert!(decode_part_record(value).is_err());
        meta.seed(key.clone(), Bytes::from_static(value));
        key
    })
    .await;
}

/// **(E)(i)** A key inside the session's `part:<id>:` range that `parse_part_key` rejects — an
/// unpadded part number — holding a value `decode_part_record` accepts: key and value are
/// validated separately (`multipart.rs:1279`, `:2578`).
#[tokio::test]
async fn a_part_key_the_parser_rejects_withholds_both_passes() {
    capture_audit();
    an_unreadable_staged_record_withholds_both_passes("e2", 0xE20, |meta, id| {
        let key = format!("part:{id}:2").into_bytes();
        assert!(key.starts_with(&part_range(id)) && parse_part_key(&key).is_err());
        meta.seed(key.clone(), part(&[chunk_ref(0xE2E, EcScheme::None, &[0])]));
        key
    })
    .await;
}

/// **(E)(i)** An `sidx:` key naming no chunk, holding a valid owned entry.
#[tokio::test]
async fn an_owned_key_naming_no_chunk_withholds_both_passes() {
    capture_audit();
    an_unreadable_staged_record_withholds_both_passes("e3", 0xE30, |meta, id| {
        let key = format!("sidx:{id}:000002:no-chunk").into_bytes();
        assert!(key.starts_with(&sidx_range(id)) && parse_sidx_key(&key).is_err());
        let value = owned(id, &sidx_key(id, part_no(2), 0xE3E), EcScheme::None, &[0]);
        meta.seed(key.clone(), value);
        key
    })
    .await;
}

/// **(E)(i)** An `mpu:` key naming no upload, holding a valid session record.
#[tokio::test]
async fn a_session_key_naming_no_upload_withholds_both_passes() {
    capture_audit();
    an_unreadable_staged_record_withholds_both_passes("e4", 0xE40, |meta, _id| {
        let key = b"mpu:not-an-upload-id".to_vec();
        assert!(parse_mpu_key(&key).is_err());
        meta.seed(key.clone(), session(State::Open));
        key
    })
    .await;
}

// ---- (E) a session VALUE no pass decodes --------------------------------------------------------

/// **(E)** A listed session whose **value** will not decode at all, under a key that parses.
///
/// Neither pass reads that value: the staged class is found through each session's KEY
/// (`gc.rs:820-836`), because protection does not depend on the upload's state, and a damaged value
/// still names its records' key ranges. So this session's committed part and owned staging entry are
/// protected exactly as a healthy session's, both ranges are still walked, the pass is not withheld
/// (GC answers `Changed`, not `Blocked`), the record is NOT named in `RestoreReport::unresolvable`,
/// and the unrelated control is still judged.
///
/// This is the boundary the operator text may claim and no more: the post-restore command's
/// UNREADABLE paragraph and the runbook name a session by its KEY alone
/// (`crates/server/src/cli.rs:1346-1356`, `m4-first-deployment-blueprint.md:609-614`) — telling an
/// operator a session's value was checked would send a repair at bytes no pass ever reads.
///
/// Base: both staged fragments marked, then reclaimed.
#[tokio::test]
async fn a_session_whose_value_will_not_decode_still_protects_its_records() {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let healthy = Healthy::seed(&meta, &d, "e5", 0xE50);
    let key = mpu_key(&healthy.id);
    let damaged = b"not a session record";
    assert!(
        parse_mpu_key(&key).is_ok() && decode_session_record(damaged).is_err(),
        "the damage must be in the session's VALUE alone — its key still parses"
    );
    meta.seed(key.clone(), Bytes::from_static(damaged));
    let name = String::from_utf8(key).expect("an `mpu:` key is ASCII");
    meta.clear_reads();

    let report = restore_pass(&meta, &d)
        .await
        .expect("a session value no pass decodes must not fail the post-restore pass");
    for (dserver, frag) in healthy.staged {
        assert!(
            !meta.holds(&orphan_key(dserver, frag)),
            "the post-restore pass marked {frag:?} on server {dserver} because the session {name} \
             carries an undecodable value — the value is not what names this upload's records: \
             {report:?}"
        );
    }
    assert!(
        meta.holds(&orphan_key(healthy.control.0, healthy.control.1)),
        "the unreferenced control was not marked: this pass marked nothing, so leaving the staged \
         fragments unmarked proves nothing: {report:?}"
    );
    assert_eq!(report.stranded_marked, 1, "{report:?}");
    assert!(
        report.unresolvable.is_empty(),
        "the post-restore pass reported the session {name} as unreadable although it never decodes \
         a session's value — the operator text names a session by its key alone: {report:?}"
    );
    assert!(
        !named_on_audit_seam(RESTORE_AUDIT, name.as_bytes()),
        "the post-restore pass named the session {name} on its audit seam although it never \
         decodes a session's value"
    );
    // Both of the session's ranges were walked from its key, as they are for a healthy session.
    let subjects: Vec<Vec<u8>> = meta
        .reads()
        .into_iter()
        .map(|read| read.subject().to_vec())
        .collect();
    for range in [sidx_range(&healthy.id), part_range(&healthy.id)] {
        assert!(
            subjects.contains(&range),
            "the post-restore pass never read {} — a session whose value will not decode still \
             names its records' key ranges through its key",
            String::from_utf8_lossy(&range)
        );
    }

    mark_all(&meta, healthy.staged).await;
    let outcome = gc_pass(&meta, &d, NOW + GRACE)
        .await
        .expect("a session value no pass decodes must not fail the GC pass");
    for (dserver, frag) in healthy.staged {
        assert!(
            on_disk(&d, dserver, frag),
            "GC reclaimed {frag:?} on server {dserver} because the session {name} carries an \
             undecodable value"
        );
    }
    assert!(
        !on_disk(&d, healthy.control.0, healthy.control.1),
        "GC did not reclaim the control the post-restore pass marked"
    );
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "a session value no pass decodes must not withhold the reclaim: only a record a pass \
         cannot READ does that"
    );
}

// ---- (E)(ii) a staged record the passes cannot trust --------------------------------------------

/// **(E)(ii)** harness: a healthy session, plus one staged record `untrusted` seeds that names
/// chunk `held` but cannot be trusted about where its fragments are. Every fragment of that chunk
/// is held in both passes and the record is named on each pass's audit seam, while unrelated
/// fragments are still judged: the post-restore pass marks the control, and GC reclaims it.
///
/// Base: the restore marks the held chunk's fragments, and GC reclaims them.
async fn an_untrusted_staged_record_holds_its_chunk(
    pair: &str,
    base: ChunkId,
    untrusted: impl FnOnce(&Meta, &UploadId, ChunkId) -> Vec<u8>,
) {
    capture_audit();
    let meta = Meta::new();
    let d = disks();
    let healthy = Healthy::seed(&meta, &d, pair, base);
    let held = base + 0xA;
    let record = untrusted(&meta, &healthy.id, held);
    let name = String::from_utf8_lossy(&record).into_owned();
    // Every fragment of the chunk: those a truncated placement still names, and the one past it —
    // on server 3, which neither that placement nor the identity fallback a short committed
    // placement gets (fragment `i` on server `i`, `metadata.rs:164-169`) names. So only holding
    // the chunk whole keeps it: a pass that filled the short placement by identity would not.
    let held_frags = [(0, frag(held, 0)), (1, frag(held, 1)), (3, frag(held, 2))];
    for (dserver, frag) in held_frags {
        place(&d, dserver, frag);
    }

    let report = restore_pass(&meta, &d)
        .await
        .expect("the post-restore pass runs");
    for (dserver, frag) in held_frags.into_iter().chain(healthy.staged) {
        assert!(
            !meta.holds(&orphan_key(dserver, frag)),
            "the post-restore pass marked {frag:?} on server {dserver} although the staged record \
             {name} names its chunk: {report:?}"
        );
    }
    assert!(
        meta.holds(&orphan_key(healthy.control.0, healthy.control.1)),
        "the unrelated control was not marked — one untrusted record withheld the whole fleet \
         instead of its own chunk: {report:?}"
    );
    assert_eq!(report.stranded_marked, 1, "{report:?}");
    assert!(
        named_on_audit_seam(RESTORE_AUDIT, &record),
        "the post-restore pass held the chunk of {name} without naming the record on its audit \
         seam"
    );

    mark_all(&meta, held_frags.into_iter().chain(healthy.staged)).await;
    let outcome = gc_pass(&meta, &d, NOW + GRACE)
        .await
        .expect("the GC pass runs");
    for (dserver, frag) in held_frags.into_iter().chain(healthy.staged) {
        assert!(
            on_disk(&d, dserver, frag),
            "GC reclaimed {frag:?} on server {dserver} although the staged record {name} names \
             its chunk"
        );
    }
    assert!(
        !on_disk(&d, healthy.control.0, healthy.control.1),
        "GC did not reclaim the unrelated control — one untrusted record held the whole fleet"
    );
    assert_eq!(outcome, Reconciled::Changed);
    assert!(
        named_on_audit_seam(GC_AUDIT, &record),
        "GC held the chunk of {name} without naming the record on its audit seam"
    );
}

/// **(E)(ii)** An owned entry whose staged placement names two D servers for a three-fragment
/// scheme.
#[tokio::test]
async fn an_owned_entry_with_a_wrong_length_placement_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f1", 0xF10, |meta, id, held| {
        let key = sidx_key(id, part_no(3), held);
        meta.seed(key.clone(), owned(id, &key, RS_2_1, &[0, 1]));
        key
    })
    .await;
}

/// **(E)(ii)** A committed part whose chunk's placement names one D server for a three-fragment
/// scheme.
#[tokio::test]
async fn a_part_with_a_wrong_length_placement_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f2", 0xF20, |meta, id, held| {
        let key = part_key(id, part_no(3));
        meta.seed(key.clone(), part(&[chunk_ref(held, RS_2_1, &[0])]));
        key
    })
    .await;
}

/// **(E)(ii)** A committed part whose chunk's placement names FOUR D servers for a three-fragment
/// scheme: too long is the wrong length too, not a full placement with a spare entry.
#[tokio::test]
async fn a_part_with_an_over_long_placement_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f4", 0xF40, |meta, id, held| {
        let key = part_key(id, part_no(3));
        meta.seed(key.clone(), part(&[chunk_ref(held, RS_2_1, &[0, 1, 2, 2])]));
        key
    })
    .await;
}

/// **(E)(ii)** A committed part whose chunk's placement is **empty** — no D server at all for a
/// three-fragment scheme.
///
/// Empty is the wrong length too, and this is the one length where the staged rule deliberately
/// parts from the committed one: a committed map's empty `placement` resolves through the identity
/// fallback (`ChunkRef::placement_is_valid`, `crates/core/src/metadata.rs:204-206`) because such a
/// record predates the field, while every staged record is born with a full placement
/// (`0016:828`), so an empty one can only be damage. A staged rule relaxed to the committed
/// classifier would identity-fill this chunk — fragment `i` on server `i` — and leave its fragment
/// 2, which sits on server 3, unprotected.
#[tokio::test]
async fn a_part_with_an_empty_placement_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f5", 0xF50, |meta, id, held| {
        let key = part_key(id, part_no(3));
        meta.seed(key.clone(), part(&[chunk_ref(held, RS_2_1, &[])]));
        key
    })
    .await;
}

/// **(E)(ii)** The `sidx:` twin of the leg above: an owned entry whose staged placement is
/// **empty**. Its placement is expanded by a reader of its own (`gc.rs:713-723`), so the part
/// record's leg says nothing about it.
#[tokio::test]
async fn an_owned_entry_with_an_empty_placement_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f6", 0xF60, |meta, id, held| {
        let key = sidx_key(id, part_no(3), held);
        meta.seed(key.clone(), owned(id, &key, RS_2_1, &[]));
        key
    })
    .await;
}

/// **(E)(ii)** An owned value that will not decode, under an `sidx:` key that names its chunk.
#[tokio::test]
async fn an_undecodable_owned_value_holds_its_chunk() {
    capture_audit();
    an_untrusted_staged_record_holds_its_chunk("f3", 0xF30, |meta, id, held| {
        let key = sidx_key(id, part_no(3), held);
        let value = b"not an owned staging entry";
        assert!(decode_owned_entry(&key, value).is_err() && parse_sidx_key(&key).is_ok());
        meta.seed(key.clone(), Bytes::from_static(value));
        key
    })
    .await;
}

// ---- (E)(iii) a store fault under a staged read -------------------------------------------------

/// **(E)(iii)** harness: the metadata double fails every read of `range` (a staged read the passes
/// must issue). The post-restore pass and GC both return `Err` whose text names the failed read —
/// wrapping the store's own error, still reachable through `source` — every staged fragment is
/// still on disk and unmarked by the pass, and the read log shows the faulted read was issued.
/// Healed, the same store runs a clean GC pass that still keeps them and reclaims the control.
///
/// Base: the post-restore pass returns `Ok`, having marked every staged fragment.
async fn a_store_fault_under_a_staged_read_fails_both_passes(
    pair: &str,
    base: ChunkId,
    range: impl FnOnce(&UploadId) -> Vec<u8>,
) {
    let meta = Meta::new();
    let d = disks();
    let healthy = Healthy::seed(&meta, &d, pair, base);
    let range = range(&healthy.id);
    let read = String::from_utf8(range.clone()).expect("every staged range is ASCII");
    meta.fail_reads_of(&range);

    let fault = restore_pass(&meta, &d).await.expect_err(
        "a store fault under a staged read must fail the post-restore pass — a pass that goes on \
         marks over a reading with a hole in it",
    );
    assert!(
        fault.to_string().contains(&read),
        "the post-restore pass failed without naming the read that failed ({read}): {fault}"
    );
    assert_eq!(
        fault.source().map(|store| store.to_string()).as_deref(),
        Some(INJECTED_FAULT),
        "the name must wrap the store's own error, never replace it: a chain-walking classifier \
         (`wyrd_traits::classify`) reads the store's fault class through `source` ({fault})"
    );
    for (dserver, frag) in healthy.all() {
        assert!(
            !meta.holds(&orphan_key(dserver, frag)),
            "the failed post-restore pass left a mark on {frag:?} (server {dserver})"
        );
    }
    assert!(
        meta.read_of(&range),
        "the post-restore pass never issued the faulted read of {read}: {:?}",
        meta.reads()
    );

    meta.clear_reads();
    mark_all(&meta, healthy.all()).await;
    let fault = gc_pass(&meta, &d, NOW).await.expect_err(
        "a store fault under a staged read must fail the GC pass — a pass that goes on reclaims \
         over a reading with a hole in it",
    );
    assert!(
        fault.to_string().contains(&read),
        "the GC pass failed without naming the read that failed ({read}): {fault}"
    );
    let ReconcileError::Store(store_fault) = &fault else {
        panic!("a store fault must surface as `ReconcileError::Store`: {fault}");
    };
    assert_eq!(
        store_fault
            .source()
            .map(|store| store.to_string())
            .as_deref(),
        Some(INJECTED_FAULT),
        "the name must wrap the store's own error, never replace it ({fault})"
    );
    for (dserver, frag) in healthy.all() {
        assert!(
            on_disk(&d, dserver, frag),
            "the failed GC pass reclaimed {frag:?} (server {dserver})"
        );
    }
    assert!(
        meta.read_of(&range),
        "the GC pass never issued the faulted read of {read}: {:?}",
        meta.reads()
    );

    meta.heal();
    let outcome = gc_pass(&meta, &d, NOW)
        .await
        .expect("the healed store runs a clean GC pass");
    for (dserver, frag) in healthy.staged {
        assert!(
            on_disk(&d, dserver, frag),
            "the healed GC pass reclaimed the staged fragment {frag:?} (server {dserver})"
        );
    }
    assert!(
        !on_disk(&d, healthy.control.0, healthy.control.1),
        "the healed GC pass did not reclaim the control"
    );
    assert_eq!(outcome, Reconciled::Changed);
}

/// **(E)(iii)** The session listing (`mpu:`) fails.
#[tokio::test]
async fn a_fault_reading_the_session_listing_fails_both_passes() {
    capture_audit();
    a_store_fault_under_a_staged_read_fails_both_passes("a7", 0x1710, |_id| MPU_PREFIX.to_vec())
        .await;
}

/// **(E)(iii)** A session's owned range (`sidx:<id>:`) fails.
#[tokio::test]
async fn a_fault_reading_a_session_owned_range_fails_both_passes() {
    capture_audit();
    a_store_fault_under_a_staged_read_fails_both_passes("a8", 0x1720, sidx_range).await;
}

/// **(E)(iii)** A session's part range (`part:<id>:`) fails.
#[tokio::test]
async fn a_fault_reading_a_session_part_range_fails_both_passes() {
    capture_audit();
    a_store_fault_under_a_staged_read_fails_both_passes("a9", 0x1730, part_range).await;
}

// ---- (F) scrub and drain status do not see upload records ---------------------------------------

/// What leg F adds to, or arms on, the store holding upload records.
#[derive(Clone, Copy, Debug)]
enum UploadRecords {
    Healthy,
    UndecodablePart,
    UnparsablePartKey,
    OwnedKeyNamingNoChunk,
    SessionKeyNamingNoUpload,
    SessionListingFails,
    OwnedRangeFails,
    PartRangeFails,
}

const F_LOST: ChunkId = 0x0F01;
const F_ON_DRAIN: ChunkId = 0x0F02;
const F_PART: ChunkId = 0x0F03;
const F_OWNED: ChunkId = 0x0F04;

/// Leg F's store: a committed object whose only fragment is missing from its D server, a committed
/// object with its fragment on the draining server, and that server marked draining — plus, when
/// `uploads` is `Some`, a listed session whose part and owned entry sit on other servers, in the
/// given condition. The fleet holds the upload's fragments either way.
async fn f_store(uploads: Option<UploadRecords>) -> (Meta, [Disk; 4]) {
    let meta = Meta::new();
    let d = disks();
    let lost = InodeRecord {
        size: 5,
        chunk_map: vec![chunk_ref(F_LOST, EcScheme::None, &[0])].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let on_drain = InodeRecord {
        size: 5,
        chunk_map: vec![chunk_ref(F_ON_DRAIN, EcScheme::None, &[DRAINING])].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    for (name, id, record) in [("lost", 1, &lost), ("on-drain", 2, &on_drain)] {
        let created = metadata::create(&meta, PARENT, name, id, record).await;
        assert_eq!(created.unwrap(), CommitOutcome::Committed);
    }
    place(&d, DRAINING, frag(F_ON_DRAIN, 0));
    place(&d, 1, frag(F_PART, 0));
    place(&d, 2, frag(F_OWNED, 0));
    set_lifecycle(&meta, DRAINING, DServerLifecycle::Draining)
        .await
        .unwrap();

    let Some(uploads) = uploads else {
        return (meta, d);
    };
    let id = upload("0f");
    meta.seed(mpu_key(&id), session(State::Open));
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[chunk_ref(F_PART, EcScheme::None, &[1])]),
    );
    let key = sidx_key(&id, part_no(2), F_OWNED);
    meta.seed(key.clone(), owned(&id, &key, EcScheme::None, &[2]));
    match uploads {
        UploadRecords::Healthy => {}
        UploadRecords::UndecodablePart => meta.seed(
            part_key(&id, part_no(3)),
            Bytes::from_static(b"{\"chunks\":\"not a chunk list\"}"),
        ),
        UploadRecords::UnparsablePartKey => meta.seed(
            format!("part:{id}:3"),
            part(&[chunk_ref(F_PART, EcScheme::None, &[1])]),
        ),
        UploadRecords::OwnedKeyNamingNoChunk => meta.seed(
            format!("sidx:{id}:000003:no-chunk"),
            owned(&id, &key, EcScheme::None, &[2]),
        ),
        UploadRecords::SessionKeyNamingNoUpload => {
            meta.seed(b"mpu:not-an-upload-id".to_vec(), session(State::Open));
        }
        UploadRecords::SessionListingFails => meta.fail_reads_of(MPU_PREFIX),
        UploadRecords::OwnedRangeFails => meta.fail_reads_of(&sidx_range(&id)),
        UploadRecords::PartRangeFails => meta.fail_reads_of(&part_range(&id)),
    }
    (meta, d)
}

/// What scrub and the drain-status query answered over one store, and the `repair:` keys scrub
/// left.
#[derive(Debug, PartialEq)]
struct Answers {
    scrub: Reconciled,
    drain: ReconciliationStatus,
    repairs: Vec<Vec<u8>>,
}

async fn scrub_and_drain_status(meta: &Meta, d: &[Disk; 4], label: &str) -> Answers {
    let (zone, custodian) = elect().await;
    let fleet = fleet(d);
    let scrub = ScrubContext {
        meta,
        fleet: &fleet,
    };
    let scrubbed = reconcile_step(&zone, &custodian, None, Some(&scrub), None, None, NOW)
        .await
        .unwrap_or_else(|fault| panic!("scrub returned Err over {label}: {fault}"));
    let drain = reconciliation_status(meta, DRAINING)
        .await
        .unwrap_or_else(|fault| panic!("drain status returned Err over {label}: {fault}"));
    Answers {
        scrub: scrubbed,
        drain,
        repairs: meta.keys_under(b"repair:"),
    }
}

/// **(F)** Scrub (`reconcile_step` with a `ScrubContext`) and `reconciliation_status` answer over a
/// store holding upload records exactly as over the same store without them — the same
/// `Reconciled`, the same `repair:` key for the missing chunk, the same drain status — with the
/// upload records healthy, with each (E)(i) damaged record in place, and with each (E)(iii) store
/// fault armed; and neither issues a read under `mpu:`, `sidx:` or `part:`. A consumer that does
/// not yet act on staged bytes inherits neither their reads, nor their damage, nor their faults
/// (`desired_state.rs:178-180`).
///
/// A guard: green on base.
// deferred: #663, #664 — those slices add upload records to scrub and to drain status, and own
// changing this leg.
#[tokio::test]
async fn scrub_and_drain_status_do_not_read_upload_records() {
    capture_audit();
    let (without, without_disks) = f_store(None).await;
    let expected =
        scrub_and_drain_status(&without, &without_disks, "the store without uploads").await;
    assert_eq!(
        expected.repairs,
        vec![repair_key(F_LOST)],
        "scrub must enqueue a repair for the missing chunk, or the comparison below compares \
         nothing"
    );
    assert_eq!(expected.drain, ReconciliationStatus::Pending);

    for uploads in [
        UploadRecords::Healthy,
        UploadRecords::UndecodablePart,
        UploadRecords::UnparsablePartKey,
        UploadRecords::OwnedKeyNamingNoChunk,
        UploadRecords::SessionKeyNamingNoUpload,
        UploadRecords::SessionListingFails,
        UploadRecords::OwnedRangeFails,
        UploadRecords::PartRangeFails,
    ] {
        let (with, with_disks) = f_store(Some(uploads)).await;
        with.clear_reads();
        let label = format!("upload records {uploads:?}");
        let answers = scrub_and_drain_status(&with, &with_disks, &label).await;
        assert_eq!(
            answers, expected,
            "scrub or drain status answered differently with {label}"
        );
        let reads = with.reads();
        assert!(!reads.is_empty(), "the read log recorded nothing");
        let staged: Vec<&Read> = reads
            .iter()
            .filter(|read| {
                [MPU_PREFIX, SIDX_PREFIX, PART_PREFIX]
                    .iter()
                    .any(|prefix| read.subject().starts_with(prefix))
            })
            .collect();
        assert!(
            staged.is_empty(),
            "scrub or drain status read upload records with {label}: {staged:?}"
        );
    }
}
