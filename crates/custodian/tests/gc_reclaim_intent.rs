//! Issue #804 (child 2 of #662): GC **records a marked fragment's reclamation before it destroys
//! the bytes**, and reads every shape an `orphan:` mark's value can take (proposal 0016,
//! `0016:1190-1216`, `:1293-1338`, `:1226-1248`).
//!
//! On `main`, GC calls `delete_fragment` inside its fleet walk and only queues the key's delete,
//! committed after the whole walk (`crates/custodian/src/gc.rs`, the fleet loop and `Cleanup`).
//! For as long as that takes, the ledger still holds the mark's original bytes over a fragment
//! that is gone — so a mover's adoption preconditioned on those bytes still commits, and publishes
//! a placement naming deleted bytes (0016's outcome (c)). A fault later in the walk drops the
//! queued deletes, leaving marks over nothing. And `main` reads only the bare decimal
//! `mark_orphaned` writes: 0016's structured and `reclaiming` shapes read as unreadable, and their
//! fragments are kept forever.
//!
//! Every leg runs the production `reconcile_step` — with a FRESH `GcContext` per pass, as the
//! deployed loop builds one (`crates/server/src/custodian.rs`) — or `reconcile_after_restore`,
//! over in-memory doubles built as `gc_ledger_walk.rs` builds them: an ordered metadata map whose
//! `scan_page` pages its own truth under a lowered cap, and D servers holding fragment bytes. The
//! two record what the pass did, in one log, in the order it happened. Structured and
//! `reclaiming` values are seeded as raw JSON, exactly as 0016 spells them; this file names no
//! symbol the change under test adds, so it compiles against `main` and fails there by
//! assertion. Every leg seeds a control the pass does reclaim (restore's guard: one it does
//! mark), so no leg passes on a pass that did nothing.
//!
//! The legs, and what `main` does to each:
//!
//! * **A** — three shapes decode, a fourth fails closed. A legacy, a structured and a
//!   `reclaiming` mark (with and without its event) are each honoured; a value that is none of
//!   them is left byte-identical, its fragment kept, and it is named on the audit seam. The
//!   guard, green on `main`: restore counts every marked fragment `already_marked`, whatever its
//!   value, and leaves each value byte-identical. `main`: a structured mark past grace licenses
//!   nothing.
//! * **B** — recorded before destroyed. (i) The commit recording the intent fails: the fragment
//!   is still present. (ii) A mark that changes between the pass's read and its intent loses only
//!   its own intent, and a pass whose only candidate lost is not `Satisfied`. (iii) An adoption
//!   CAS on the mark's original bytes, committed as GC deletes the fragment, gets `Conflict`.
//!   (iv) A `reclaiming` mark stamped recently is finished with no grace test — the fragment
//!   once, then the key. (v) A store fault after deletes leaves no consumed key behind, and the
//!   error still propagates. `main`: (i)/(iii) delete first, (ii) reclaims on a changed mark,
//!   (iv) does not decode, (v) drops the queued deletes.
//! * **C** — intents are batched: [`W`] + 1 reclaimable marks are recorded in exactly two
//!   commits, neither carrying more than [`W`]. `main`: no commit records anything.
//! * **D** — a draining retirement protects by keyed lookup: a structured mark past grace whose
//!   event is a retirement token keeps its fragment while `retire:bytes:<event>` exists, and the
//!   next pass after it is deleted reclaims it; no pass reads `retire:` by range. `main`: the
//!   second half.
//!
//! [`W`] is the production batch (`gc::CLEANUP_BATCH`), written as a number as
//! `gc_ledger_walk.rs` writes it, so a change to it fails here instead of moving the bound.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, orphan_key, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::multipart::{
    decode_retire_obligation, retire_key, AttemptId, PartNumber, RetireMode, RetireToken, UploadId,
};
use wyrd_custodian::{
    mark_orphaned, reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy,
    FencedZone, GcContext, ReconcileError, Reconciled, RestoreReport,
};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId,
    FragmentId, Health, MetadataStore, PageStart, Result, ScanCapExceeded, ScanPage, WriteBatch,
};

/// **W**, the production batch (`gc::CLEANUP_BATCH`): the most marks one commit of a pass moves
/// to `reclaiming`, and the most key deletes one cleanup commit carries.
const W: usize = 1_000;

/// The double's cap on one `scan` answer and one `scan_page` page, as `gc_ledger_walk.rs` lowers
/// it. Every ledger here fits one page.
const CAP: usize = 5_000;

/// The grace window every pass runs with.
const GRACE: u64 = 1_000;

/// An instant far from zero, so a stamp at it is inside the grace window and a stamp at zero is
/// long past it.
const NOW: u64 = 1_000_000;

const ORPHAN_PREFIX: &[u8] = b"orphan:";
const RETIRE_PREFIX: &[u8] = b"retire:";

// ---- what the doubles saw ----

/// One observation, in the order the pass produced it.
#[derive(Clone, Debug)]
enum Event {
    /// `delete_fragment` removed a fragment's bytes.
    FragmentDeleted(DServerId, FragmentId),
    /// A commit was answered.
    Commit(Commit),
}

/// One commit as the metadata double answered it.
#[derive(Clone, Debug)]
struct Commit {
    preconditions: Vec<Vec<u8>>,
    puts: Vec<Vec<u8>>,
    deletes: Vec<Vec<u8>>,
    /// `None` when the double failed it with an error.
    outcome: Option<CommitOutcome>,
}

impl Commit {
    /// Whether it carries a precondition or a put on an `orphan:` key — what recording a reclaim
    /// intent takes, and what no other commit of a GC pass does.
    fn records_on_the_ledger(&self) -> bool {
        self.preconditions
            .iter()
            .chain(&self.puts)
            .any(|key| key.starts_with(ORPHAN_PREFIX))
    }

    /// How many `orphan:` marks it moves: its puts on `orphan:` keys.
    fn marks_moved(&self) -> usize {
        self.puts
            .iter()
            .filter(|key| key.starts_with(ORPHAN_PREFIX))
            .count()
    }

    fn applied(&self) -> bool {
        self.outcome == Some(CommitOutcome::Committed)
    }
}

/// The log the metadata double and the D servers share.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<Event>>>);

impl Log {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }

    /// Everything since the last call, clearing it.
    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

fn commits(events: &[Event]) -> Vec<&Commit> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Commit(commit) => Some(commit),
            Event::FragmentDeleted(..) => None,
        })
        .collect()
}

// ---- the metadata double ----

/// Faults and interleavings the metadata double injects on request.
#[derive(Default)]
struct MetaFaults {
    /// Fail — `Err`, nothing applied — every commit that records on the ledger
    /// ([`Commit::records_on_the_ledger`]).
    fail_recording: bool,
    /// Once a ledger page has handed out this key, write this value to it: a writer landing
    /// between the pass's read of the mark and anything the pass commits on it.
    rewrite_after_read: Option<(Vec<u8>, Bytes)>,
}

/// An in-memory metadata store over an ORDERED map (see the module docs). Every read it answers
/// is recorded: `scan` and `scan_page` by prefix, `get` by key.
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    log: Log,
    faults: Mutex<MetaFaults>,
    scans: Mutex<Vec<Vec<u8>>>,
    gets: Mutex<Vec<Vec<u8>>>,
}

impl Meta {
    fn new(log: &Log) -> Self {
        Self {
            kv: Mutex::default(),
            log: log.clone(),
            faults: Mutex::default(),
            scans: Mutex::default(),
            gets: Mutex::default(),
        }
    }

    fn seed(&self, key: Vec<u8>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key, value.into());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, MetaFaults> {
        self.faults.lock().unwrap()
    }

    /// Every `scan` and `scan_page` prefix read since the last call, clearing them.
    fn take_scans(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.scans.lock().unwrap())
    }

    /// Every key read by `get` since the last call, clearing them.
    fn take_gets(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.gets.lock().unwrap())
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.gets.lock().unwrap().push(key.to_vec());
        Ok(self.value(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.scans.lock().unwrap().push(prefix.to_vec());
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(CAP + 1)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if hits.len() > CAP {
            return Err(Box::new(ScanCapExceeded {
                cap: CAP,
                prefix: prefix.to_vec(),
            }));
        }
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        self.scans.lock().unwrap().push(prefix.to_vec());
        let limit = page_limit(limit, CAP, prefix)?;
        let lower = match page_start(prefix, after) {
            PageStart::Prefix => Bound::Included(prefix.to_vec()),
            PageStart::After(cursor) => Bound::Excluded(cursor.to_vec()),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let items: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let next = page_cursor(&items, limit);
        // The page is answered as it stood; the concurrent writer lands right behind it.
        let mut faults = self.faults();
        if let Some((key, _)) = &faults.rewrite_after_read {
            if items.iter().any(|(read, _)| read == key) {
                let (key, value) = faults.rewrite_after_read.take().unwrap();
                self.seed(key, value);
            }
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut commit = Commit {
            preconditions: batch
                .preconditions
                .iter()
                .map(|pre| pre.key.clone())
                .collect(),
            puts: batch.puts.iter().map(|(key, _)| key.clone()).collect(),
            deletes: batch.deletes.clone(),
            outcome: None,
        };
        if self.faults().fail_recording && commit.records_on_the_ledger() {
            self.log.push(Event::Commit(commit));
            return Err(BoxError::from(
                "simulated metadata fault recording on the ledger",
            ));
        }
        let outcome = {
            let mut kv = self.kv.lock().unwrap();
            if batch
                .preconditions
                .iter()
                .all(|pre| kv.get(&pre.key) == pre.expected.as_ref())
            {
                for (key, value) in batch.puts {
                    kv.insert(key, value);
                }
                for key in &batch.deletes {
                    kv.remove(key);
                }
                CommitOutcome::Committed
            } else {
                CommitOutcome::Conflict
            }
        };
        commit.outcome = Some(outcome);
        self.log.push(Event::Commit(commit));
        Ok(outcome)
    }
}

// ---- the D-server double ----

/// A commit a mover makes the moment GC deletes one fragment — its adoption of that position —
/// and what the store answered it.
struct Adoption {
    meta: Arc<Meta>,
    frag: FragmentId,
    batch: WriteBatch,
    outcome: Arc<Mutex<Option<CommitOutcome>>>,
}

/// Faults and interleavings a D server injects on request.
#[derive(Default)]
struct DiskFaults {
    /// `list_fragments` fails.
    fail_list: bool,
    /// `delete_fragment` of this fragment fails, deleting nothing.
    fail_delete: Option<FragmentId>,
    /// Committed right after this D server deletes the adoption's fragment.
    adopt_on_delete: Option<Adoption>,
}

/// One D server's fragment bytes, listed in `(chunk, index)` order so a pass meets them in an
/// order a leg can name.
struct Disk {
    id: DServerId,
    frags: Mutex<BTreeMap<(ChunkId, u16), Bytes>>,
    log: Log,
    faults: Mutex<DiskFaults>,
}

impl Disk {
    fn put(&self, frag: FragmentId) {
        self.frags
            .lock()
            .unwrap()
            .insert(slot(frag), Bytes::from_static(b"bytes"));
    }

    fn holds(&self, frag: FragmentId) -> bool {
        self.frags.lock().unwrap().contains_key(&slot(frag))
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, DiskFaults> {
        self.faults.lock().unwrap()
    }
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.frags.lock().unwrap().insert(slot(id), fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&slot(id)).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        if self.faults().fail_list {
            return Err(BoxError::from("simulated D-server fault listing fragments"));
        }
        Ok(self
            .frags
            .lock()
            .unwrap()
            .keys()
            .map(|&(chunk, index)| FragmentId { chunk, index })
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        if self.faults().fail_delete == Some(id) {
            return Err(BoxError::from(
                "simulated D-server fault deleting a fragment",
            ));
        }
        self.frags.lock().unwrap().remove(&slot(id));
        self.log.push(Event::FragmentDeleted(self.id, id));
        let adoption = {
            let mut faults = self.faults();
            if faults
                .adopt_on_delete
                .as_ref()
                .is_some_and(|adoption| adoption.frag == id)
            {
                faults.adopt_on_delete.take()
            } else {
                None
            }
        };
        if let Some(adoption) = adoption {
            let outcome = adoption.meta.commit(adoption.batch).await?;
            *adoption.outcome.lock().unwrap() = Some(outcome);
        }
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// A fragment's place in a [`Disk`]'s ordered map.
fn slot(frag: FragmentId) -> (ChunkId, u16) {
    (frag.chunk, frag.index)
}

fn servers(log: &Log, count: DServerId) -> Vec<Disk> {
    (0..count)
        .map(|id| Disk {
            id,
            frags: Mutex::default(),
            log: log.clone(),
            faults: Mutex::default(),
        })
        .collect()
}

/// The fleet view a `GcContext` takes.
fn fleet_of(disks: &[Disk]) -> Vec<(DServerId, &dyn ChunkStore)> {
    disks
        .iter()
        .map(|disk| (disk.id, disk as &dyn ChunkStore))
        .collect()
}

// ---- helpers ----

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// The legacy shape: the bare decimal [`mark_orphaned`] writes (pinned by
/// [`assert_legacy_is_mark_orphaned`]).
fn legacy(at: u64) -> Bytes {
    Bytes::from(at.to_string())
}

/// The structured shape, spelled as 0016 spells it (`0016:1195`).
fn structured(at: u64, event: &str) -> Bytes {
    Bytes::from(format!(
        r#"{{"orphaned_at_millis":{at},"event":"{event}"}}"#
    ))
}

/// The `reclaiming` shape (`0016:1325`); `event` absent when the mark GC replaced was legacy.
fn reclaiming(at: u64, event: Option<&str>) -> Bytes {
    Bytes::from(match event {
        Some(event) => {
            format!(r#"{{"orphaned_at_millis":{at},"event":"{event}","reclaiming":true}}"#)
        }
        None => format!(r#"{{"orphaned_at_millis":{at},"reclaiming":true}}"#),
    })
}

/// The seeded legacy value is exactly what the production writer stores.
async fn assert_legacy_is_mark_orphaned() {
    let written = Meta::new(&Log::default());
    mark_orphaned(&written, 3, frag(77, 2), 4_242)
        .await
        .unwrap();
    assert_eq!(
        written.value(&orphan_key(3, frag(77, 2))),
        Some(legacy(4_242)),
        "fixture: a seeded legacy mark must be exactly what `mark_orphaned` writes"
    );
}

/// A permissive global default, installed before any callsite a pass fires is first hit, so none
/// can latch `Interest::never` under the parallel harness and leave a capture empty (#214; the
/// guard `crates/custodian/tests/gc.rs` installs).
fn install_global_default() {
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
}

/// One custodian, and one GC pass at a time the way the deployed loop runs one.
struct Gc {
    zone: FencedZone,
    custodian: Custodian,
}

impl Gc {
    async fn elect(coord: &MemCoordination) -> Self {
        let custodian = Custodian::elect(coord, "zone-gc-reclaim-intent")
            .await
            .unwrap();
        let mut zone = FencedZone::new();
        zone.install(custodian.leadership());
        Self { zone, custodian }
    }

    /// One GC pass at `now`, through the fenced `reconcile_step`, with a FRESH `GcContext`.
    async fn pass(
        &self,
        meta: &Meta,
        fleet: &[(DServerId, &dyn ChunkStore)],
        now: u64,
    ) -> std::result::Result<Reconciled, ReconcileError> {
        let ctx = GcContext {
            meta,
            fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        reconcile_step(
            &self.zone,
            &self.custodian,
            Some(&ctx),
            None,
            None,
            None,
            now,
        )
        .await
    }
}

/// Where in `events` the first applied commit deleting `key` sits.
fn key_deleted_at(events: &[Event], key: &[u8]) -> Option<usize> {
    events.iter().position(|event| match event {
        Event::Commit(commit) => commit.applied() && commit.deletes.iter().any(|k| k == key),
        Event::FragmentDeleted(..) => false,
    })
}

/// Every position in `events` at which `frag` on `dserver` was deleted.
fn fragment_deleted_at(events: &[Event], dserver: DServerId, frag: FragmentId) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            matches!(event, Event::FragmentDeleted(d, f) if *d == dserver && *f == frag)
        })
        .map(|(at, _)| at)
        .collect()
}

/// A committed flat record placing `chunk`'s only fragment on `dserver`.
fn placing(chunk: ChunkId, dserver: DServerId) -> InodeRecord {
    InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 5,
            placement: vec![dserver],
        }]
        .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    }
}

/// Collects what a `tracing` subscriber writes, so leg A reads back the audit lines the pass
/// actually emitted (`crates/custodian/tests/gc.rs`'s `Capture`).
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

// ---- leg A: three shapes decode; a fourth fails closed ----

/// What leg A seeds on D server 0: one fragment per mark, every one referenced by nothing, and a
/// stray with no mark at all.
struct Shapes {
    /// Marks GC honours: a legacy and two structured marks past grace (one naming a retirement
    /// token whose obligation is gone, one a per-move nonce), and two `reclaiming` marks stamped
    /// NOW, inside grace — with and without an event.
    honoured: Vec<(FragmentId, Bytes)>,
    /// Values that are none of the three shapes, every one old enough that any stamp read out of
    /// it would be past grace: garbage, a second spelling of a decimal, a JSON mark with neither
    /// event nor `reclaiming`, a `reclaiming` written out as false, fields reordered.
    fourth: Vec<(FragmentId, Bytes)>,
    /// No mark: restore's control.
    stray: FragmentId,
}

fn seed_shapes(meta: &Meta, disk: &Disk) -> Shapes {
    let honoured = vec![
        (frag(0xA1, 0), legacy(0)),
        (frag(0xA2, 0), structured(0, "g:1:1")),
        (frag(0xA3, 0), structured(0, "move-5a17")),
        (frag(0xA4, 0), reclaiming(NOW, Some("g:1:1"))),
        (frag(0xA5, 0), reclaiming(NOW, None)),
    ];
    let fourth: Vec<(FragmentId, Bytes)> = [
        b"not a mark".as_slice(),
        b"007",
        br#"{"orphaned_at_millis":0}"#,
        br#"{"orphaned_at_millis":0,"event":"g:1:1","reclaiming":false}"#,
        br#"{"event":"g:1:1","orphaned_at_millis":0}"#,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| (frag(0xB1 + i as ChunkId, 0), Bytes::copy_from_slice(value)))
    .collect();
    for (f, value) in honoured.iter().chain(&fourth) {
        disk.put(*f);
        meta.seed(orphan_key(disk.id, *f), value.clone());
    }
    let stray = frag(0xC0, 0);
    disk.put(stray);
    Shapes {
        honoured,
        fourth,
        stray,
    }
}

/// Every shape 0016 defines is honoured by one GC pass: the legacy and structured marks past grace
/// are reclaimed, and the `reclaiming` marks — stamped NOW, so any grace test would keep them — are
/// finished (B(iv)). Each value that is none of the shapes keeps its fragment and its exact bytes,
/// and is named on the GC audit seam carrying its key. Negation: decode only the bare decimal
/// (`main`) — the structured mark past grace is kept.
#[tokio::test]
async fn a_three_shapes_decode_and_a_fourth_fails_closed() {
    install_global_default();
    assert_legacy_is_mark_orphaned().await;
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let shapes = seed_shapes(&meta, &disks[0]);
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let audit = Capture::default();
    let outcome = gc
        .pass(&meta, &fleet, NOW)
        .with_subscriber(tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(audit.clone()),
            ),
        ))
        .await;
    assert!(outcome.is_ok(), "leg A: the pass failed: {outcome:?}");

    for (f, value) in &shapes.honoured {
        assert!(
            !disks[0].holds(*f) && meta.value(&orphan_key(0, *f)).is_none(),
            "leg A: {f:?}'s mark {:?} is one of 0016's three shapes and licenses its reclaim, but \
             the pass left the fragment (on disk: {}) or the mark",
            String::from_utf8_lossy(value),
            disks[0].holds(*f)
        );
    }
    for (f, value) in &shapes.fourth {
        assert!(
            disks[0].holds(*f),
            "leg A: {f:?}'s mark {:?} is none of the three shapes, and its fragment was reclaimed \
             on it",
            String::from_utf8_lossy(value)
        );
        assert_eq!(
            meta.value(&orphan_key(0, *f)).as_ref(),
            Some(value),
            "leg A: a value that is none of the shapes must be left byte-identical"
        );
    }
    assert!(
        disks[0].holds(shapes.stray),
        "leg A: a fragment with no mark is kept"
    );

    let logged = String::from_utf8(audit.0.lock().unwrap().clone()).unwrap();
    for (f, _) in &shapes.fourth {
        let named = format!(
            r#""mark":"{}""#,
            String::from_utf8_lossy(&orphan_key(0, *f))
        );
        assert!(
            logged
                .lines()
                .any(|line| line.contains(r#""action":"unreadable-orphan-mark""#)
                    && line.contains(r#""target":"wyrd.custodian.gc.audit""#)
                    && line.contains(&named)),
            "leg A: a mark that is none of the shapes must be named on the GC audit seam, \
             carrying its key ({named}). got: {logged}"
        );
    }
}

/// **The guard, green on `main`.** The post-restore pass judges a mark by existence alone
/// (`gc::marked_among`): every marked fragment — whatever its value holds, one of the three shapes
/// or none — is counted `already_marked` and left byte-identical, and no reader rewrites a mark
/// (`0016:1208-1211`). The stray with no mark is marked: the pass did run.
#[tokio::test]
async fn a_restore_counts_every_shape_already_marked_and_rewrites_none() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let shapes = seed_shapes(&meta, &disks[0]);
    let fleet = fleet_of(&disks);
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    let report: RestoreReport = reconcile_after_restore(&ctx, NOW)
        .await
        .expect("leg A guard: the post-restore pass");
    let marked = shapes.honoured.len() + shapes.fourth.len();
    assert_eq!(
        report.already_marked, marked,
        "leg A guard: every marked fragment, whatever its value, is already marked: {report:?}"
    );
    for (f, value) in shapes.honoured.iter().chain(&shapes.fourth) {
        assert_eq!(
            meta.value(&orphan_key(0, *f)).as_ref(),
            Some(value),
            "leg A guard: the post-restore pass rewrote {f:?}'s mark"
        );
    }
    assert_eq!(
        (
            report.stranded_marked,
            meta.value(&orphan_key(0, shapes.stray))
        ),
        (1, Some(legacy(NOW))),
        "leg A guard: the stray with no mark is marked, at the pass's clock: {report:?}"
    );
}

// ---- leg B: recorded before destroyed ----

/// **B(i).** The store fails the commit that records the reclaim intent. The fragment must still
/// be on disk, its mark unchanged, and the fault must reach the caller; once the store takes the
/// intent, the next pass reclaims it. Negation: delete the fragment before recording anything
/// (`main`) — it is gone although nothing recorded its reclamation.
#[tokio::test]
async fn b1_a_failed_intent_commit_destroys_nothing() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let x = frag(0xD1, 0);
    disks[0].put(x);
    meta.seed(orphan_key(0, x), legacy(0));
    meta.faults().fail_recording = true;
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert!(
        disks[0].holds(x),
        "leg B(i): the fragment was deleted although the commit recording its reclamation failed \
         — its bytes were destroyed with nothing in the metadata saying so ({:?})",
        commits(&log.take())
    );
    assert_eq!(
        meta.value(&orphan_key(0, x)),
        Some(legacy(0)),
        "leg B(i): the mark must be exactly as it was"
    );
    assert!(
        outcome.is_err(),
        "leg B(i): the failed commit must reach the caller, not be swallowed: {outcome:?}"
    );

    // Control: the store takes the intent, and the next pass reclaims.
    meta.faults().fail_recording = false;
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert_eq!(
        outcome.ok(),
        Some(Reconciled::Changed),
        "leg B(i): the next pass reclaims"
    );
    assert!(!disks[0].holds(x) && meta.value(&orphan_key(0, x)).is_none());
}

/// **B(ii).** A mark changes — a later unreference event re-stamps it — the instant after the
/// pass's ledger page hands it out, so the pass holds its old bytes. Its intent must lose: its
/// fragment survives and its new value is left as the writer wrote it, while the two other marks
/// the pass judged in the same batch, one on each side of it, are still reclaimed. And a pass
/// whose ONLY candidate loses must not answer `Satisfied`: it read a value that is no longer
/// there. Negation: reclaim on the value read (`main`), or let one lost precondition sink the
/// whole batch.
#[tokio::test]
async fn b2_a_mark_that_changes_after_the_read_loses_only_its_own_intent() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let (before, changed, after) = (frag(0xD2, 0), frag(0xD3, 0), frag(0xD4, 0));
    for f in [before, changed, after] {
        disks[0].put(f);
        meta.seed(orphan_key(0, f), legacy(0));
    }
    meta.faults().rewrite_after_read = Some((orphan_key(0, changed), legacy(NOW)));
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert!(outcome.is_ok(), "leg B(ii): the pass failed: {outcome:?}");
    assert!(
        meta.faults().rewrite_after_read.is_none(),
        "fixture: the concurrent re-stamp must have landed"
    );
    assert!(
        disks[0].holds(changed),
        "leg B(ii): the fragment was reclaimed on a mark value that had already been replaced — \
         the new stamp's grace window is the one a reader is relying on"
    );
    assert_eq!(
        meta.value(&orphan_key(0, changed)),
        Some(legacy(NOW)),
        "leg B(ii): the re-stamped mark must be left as its writer wrote it"
    );
    for f in [before, after] {
        assert!(
            !disks[0].holds(f) && meta.value(&orphan_key(0, f)).is_none(),
            "leg B(ii): {f:?} was judged in the same pass and nothing changed its mark, but a \
             lost precondition elsewhere cost it its reclaim"
        );
    }

    // The only candidate loses: nothing reclaimed, and not a certification.
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let only = frag(0xD5, 0);
    disks[0].put(only);
    meta.seed(orphan_key(0, only), legacy(0));
    meta.faults().rewrite_after_read = Some((orphan_key(0, only), legacy(NOW)));
    let fleet = fleet_of(&disks);
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert!(
        disks[0].holds(only),
        "leg B(ii), only candidate: reclaimed on a replaced mark"
    );
    let outcome = outcome.expect("leg B(ii), only candidate: the pass");
    assert_ne!(
        outcome,
        Reconciled::Satisfied,
        "leg B(ii): a pass whose only candidate lost its intent certified the store as converged, \
         over a mark value it never read"
    );
    // Control: once the new stamp's grace has run, the next pass reclaims.
    let outcome = gc.pass(&meta, &fleet, NOW + GRACE).await;
    assert_eq!(outcome.ok(), Some(Reconciled::Changed));
    assert!(!disks[0].holds(only) && meta.value(&orphan_key(0, only)).is_none());
}

/// **B(iii).** A mover pre-marked a position and wrote its fragment there, then paused past the
/// grace window; it resumes and commits its adoption — `require(orphan:<pos> == <the pre-mark's
/// bytes>)`, publishing a record that places the position — at the moment GC deletes that
/// fragment. It must get `Conflict`, and nothing may be published. Negation: delete the fragment
/// before recording anything (`main`) — the ledger still holds the pre-mark's exact bytes, the
/// adoption commits, and the published placement names deleted bytes (0016's outcome (c)).
#[tokio::test]
async fn b3_an_adoption_on_the_marks_original_bytes_conflicts_as_gc_deletes() {
    install_global_default();
    const ADOPTER: u64 = 77;
    let log = Log::default();
    let meta = Arc::new(Meta::new(&log));
    let disks = servers(&log, 2);
    let pos = frag(0xE1, 0);
    disks[1].put(pos);
    let premark = legacy(0);
    meta.seed(orphan_key(1, pos), premark.clone());
    let outcome = Arc::new(Mutex::new(None));
    let adopter = metadata::inode_key(ADOPTER);
    disks[1].faults().adopt_on_delete = Some(Adoption {
        meta: Arc::clone(&meta),
        frag: pos,
        batch: WriteBatch::new()
            .require_absent(adopter.clone())
            .put(adopter.clone(), metadata::encode(&placing(pos.chunk, 1)))
            .require(orphan_key(1, pos), premark)
            .delete(orphan_key(1, pos)),
        outcome: Arc::clone(&outcome),
    });
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let reconciled = gc.pass(&meta, &fleet, NOW).await;
    assert!(
        reconciled.is_ok(),
        "leg B(iii): the pass failed: {reconciled:?}"
    );
    assert_eq!(
        *outcome.lock().unwrap(),
        Some(CommitOutcome::Conflict),
        "leg B(iii): an adoption preconditioned on the mark's original bytes was answered as GC \
         deleted the fragment — it must lose, or it publishes a placement over deleted bytes"
    );
    assert!(
        meta.value(&adopter).is_none(),
        "leg B(iii): a record placing the deleted fragment was published"
    );
    // Control: GC did reclaim the position, and consumed its mark.
    assert!(!disks[1].holds(pos) && meta.value(&orphan_key(1, pos)).is_none());
}

/// **B(iv).** A `reclaiming` mark over a fragment still on disk — a pass that recorded its intent
/// and died before its deletes — stamped NOW, well inside its grace window. The next pass finishes
/// it with no grace test: the fragment is deleted exactly once, and only then the key, with and
/// without the event the mark carries. Negation: decode only the bare decimal (`main`) — the mark
/// reads as unreadable and the fragment is kept forever.
#[tokio::test]
async fn b4_a_reclaiming_mark_is_finished_with_no_grace_test() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let marks = [
        (frag(0xF1, 0), reclaiming(NOW, Some("g:5:1"))),
        (frag(0xF2, 0), reclaiming(NOW, None)),
    ];
    for (f, value) in &marks {
        disks[0].put(*f);
        meta.seed(orphan_key(0, *f), value.clone());
    }
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert!(outcome.is_ok(), "leg B(iv): the pass failed: {outcome:?}");
    let events = log.take();
    for (f, value) in &marks {
        let deleted = fragment_deleted_at(&events, 0, *f);
        assert_eq!(
            deleted.len(),
            1,
            "leg B(iv): {f:?} under the `reclaiming` mark {:?} must be deleted exactly once — the \
             reclamation was already decided, and a grace test would keep it until NOW + GRACE",
            String::from_utf8_lossy(value)
        );
        let key = key_deleted_at(&events, &orphan_key(0, *f));
        assert!(
            key.is_some_and(|key| key > deleted[0]),
            "leg B(iv): the key of {f:?} must be deleted after its fragment, never before or \
             never (fragment deleted at event {}, key at {key:?})",
            deleted[0]
        );
    }
    assert_eq!(
        outcome.ok(),
        Some(Reconciled::Changed),
        "leg B(iv): the pass"
    );
}

/// **B(v).** A store fault ends a pass after it has already deleted fragments. The keys of the
/// fragments it deleted must be gone afterwards — never left behind over bytes that are gone,
/// where no `list_fragments()`-driven walk visits again — and the fault must still reach the
/// caller. Two faults: the third delete of a recorded batch fails, and listing a later D server
/// fails after an earlier one's `reclaiming` mark was finished. Once each fault clears, the next
/// pass reclaims what is left. Negation: let the fault drop the queued deletes (`main`).
#[tokio::test]
async fn b5_a_store_fault_after_deletes_still_commits_their_key_deletes() {
    install_global_default();
    let gc = Gc::elect(&MemCoordination::new()).await;

    // A delete fault, third in its batch.
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 1);
    let batch = [frag(0x101, 0), frag(0x102, 0), frag(0x103, 0)];
    for f in batch {
        disks[0].put(f);
        meta.seed(orphan_key(0, f), legacy(0));
    }
    disks[0].faults().fail_delete = Some(batch[2]);
    let fleet = fleet_of(&disks);
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    let err = outcome.expect_err("leg B(v): the delete fault must reach the caller");
    assert!(
        err.to_string()
            .contains("simulated D-server fault deleting a fragment"),
        "leg B(v): the pass must report the store's own fault, got: {err}"
    );
    for f in &batch[..2] {
        assert!(
            !disks[0].holds(*f),
            "fixture: the pass must have deleted {f:?} before the fault"
        );
        assert!(
            meta.value(&orphan_key(0, *f)).is_none(),
            "leg B(v): {f:?} was deleted, and the fault that ended the pass took its key delete \
             with it — a mark left over bytes that are gone"
        );
    }
    assert!(
        disks[0].holds(batch[2]),
        "fixture: the failed delete deleted nothing"
    );
    disks[0].faults().fail_delete = None;
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert_eq!(outcome.ok(), Some(Reconciled::Changed));
    assert!(!disks[0].holds(batch[2]) && meta.value(&orphan_key(0, batch[2])).is_none());

    // A listing fault on a later D server, after an earlier one's `reclaiming` mark was finished.
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 2);
    let finished = frag(0x104, 0);
    disks[0].put(finished);
    meta.seed(orphan_key(0, finished), reclaiming(0, None));
    disks[1].faults().fail_list = true;
    let fleet = fleet_of(&disks);
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    let err = outcome.expect_err("leg B(v): the listing fault must reach the caller");
    assert!(
        err.to_string()
            .contains("simulated D-server fault listing fragments"),
        "leg B(v): the pass must report the store's own fault, got: {err}"
    );
    assert!(
        !disks[0].holds(finished),
        "leg B(v): the `reclaiming` mark's fragment on the first D server must be finished before \
         the second one's listing fails"
    );
    assert!(
        meta.value(&orphan_key(0, finished)).is_none(),
        "leg B(v): the finished fragment's key must not be left behind by the fault"
    );
    disks[1].faults().fail_list = false;
    assert!(gc.pass(&meta, &fleet, NOW).await.is_ok());
}

// ---- leg C: intents are batched ----

/// [`W`] + 1 marks past grace, spread over four D servers, all reclaimed in one pass. The commits
/// that carry a precondition or a put on an `orphan:` key — the reclaim intents — number exactly
/// two, neither moving more than [`W`] marks, together every mark once. Negation: record each
/// intent alone (v1's surviving mutant: 1,001 commits), all in one commit, or per D server; or
/// record none (`main`).
#[tokio::test]
async fn c_intents_are_recorded_in_batches_of_at_most_w() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 4);
    let population = W + 1;
    let reclaimable: Vec<(DServerId, FragmentId)> = (0..population)
        .map(|i| ((i % 4) as DServerId, frag(0x10_000 + i as ChunkId, 0)))
        .collect();
    for &(dserver, f) in &reclaimable {
        disks[dserver as usize].put(f);
        meta.seed(orphan_key(dserver, f), legacy(0));
    }
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;

    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert_eq!(outcome.ok(), Some(Reconciled::Changed), "leg C: the pass");
    let events = log.take();
    let answered = commits(&events);
    let recording: Vec<&&Commit> = answered
        .iter()
        .filter(|commit| commit.records_on_the_ledger())
        .collect();
    assert_eq!(
        recording.len(),
        2,
        "leg C: {population} reclaim intents must be recorded in exactly ⌈{population} / {W}⌉ = 2 \
         commits; {} commits carried a precondition or a put on an `orphan:` key",
        recording.len()
    );
    for commit in &recording {
        assert!(
            commit.marks_moved() <= W,
            "leg C: one commit recorded {} intents, over the batch of {W}",
            commit.marks_moved()
        );
    }
    assert_eq!(
        recording
            .iter()
            .map(|commit| commit.marks_moved())
            .sum::<usize>(),
        population,
        "leg C: every intent is recorded once"
    );
    for commit in &answered {
        assert!(
            commit.puts.len() + commit.deletes.len() <= W,
            "leg C: a commit carried {} writes, over the batch of {W}",
            commit.puts.len() + commit.deletes.len()
        );
    }
    for &(dserver, f) in &reclaimable {
        assert!(
            !disks[dserver as usize].holds(f) && meta.value(&orphan_key(dserver, f)).is_none(),
            "leg C: {f:?} on D server {dserver} was not reclaimed"
        );
    }
}

// ---- leg D: a draining retirement protects by keyed lookup ----

/// 32 lowercase-hex characters from a 2-character pair.
fn hex32(pair: &str) -> String {
    pair.repeat(16)
}

/// One chunk of an obligation's chunk list, spelled as `multipart`'s `ChunkRefWire` declares it.
fn chunk_json(chunk: ChunkId, dserver: DServerId) -> String {
    format!(r#"{{"id":{chunk},"scheme":"None","len":5,"placement":[{dserver}]}}"#)
}

/// A structured mark past grace whose event is a retirement token's canonical string keeps its
/// fragment while that retirement's `retire:bytes:` obligation exists — a generation retirement
/// and a per-part session one, each seeded as the drain that marks them would find it — and the
/// next pass after the obligation is deleted reclaims it, one retirement at a time. Every pass
/// reads each obligation it depends on by key, and no pass reads `retire:` by range. The controls,
/// reclaimed in the first pass: a legacy mark, and a structured one naming a retirement whose
/// obligation is already gone. Negation: never reclaim a structured mark (`main`); or protect by
/// expanding `retire:` (a range read).
#[tokio::test]
async fn d_a_draining_retirement_protects_by_keyed_lookup() {
    install_global_default();
    let log = Log::default();
    let meta = Meta::new(&log);
    let disks = servers(&log, 3);
    let generation = RetireToken::Generation {
        inode: 9,
        version: 2,
    };
    let session = RetireToken::Session {
        upload_id: UploadId::new(hex32("a1")).unwrap(),
        epoch: 3,
        part: Some((
            PartNumber::new(1).unwrap(),
            AttemptId::new(hex32("b2")).unwrap(),
        )),
    };
    let (by_generation, by_session) = (frag(0x901, 0), frag(0x902, 0));
    let draining: [(DServerId, FragmentId, RetireToken, String); 2] = [
        (0, by_generation, generation, {
            let chunks = chunk_json(by_generation.chunk, 0);
            format!(r#"{{"generation":{{"inode":9,"version":2,"chunks":[{chunks}]}}}}"#)
        }),
        (1, by_session, session, {
            let chunks = chunk_json(by_session.chunk, 1);
            format!(r#"{{"chunks":[{chunks}]}}"#)
        }),
    ];
    for (dserver, f, token, obligation) in &draining {
        let key = retire_key(RetireMode::Bytes, token);
        decode_retire_obligation(&key, obligation.as_bytes())
            .expect("fixture: the seeded obligation is one a writer installs");
        meta.seed(key, obligation.clone());
        disks[*dserver as usize].put(*f);
        meta.seed(orphan_key(*dserver, *f), structured(0, &token.to_string()));
    }
    let (plain, drained) = (frag(0x903, 0), frag(0x904, 0));
    for (f, value) in [(plain, legacy(0)), (drained, structured(0, "g:9:3"))] {
        disks[2].put(f);
        meta.seed(orphan_key(2, f), value);
    }
    let fleet = fleet_of(&disks);
    let gc = Gc::elect(&MemCoordination::new()).await;
    let no_retire_scan = |scans: &[Vec<u8>], pass: usize| {
        let reaching: Vec<String> = scans
            .iter()
            .filter(|prefix| RETIRE_PREFIX.starts_with(prefix) || prefix.starts_with(RETIRE_PREFIX))
            .map(|prefix| String::from_utf8_lossy(prefix).into_owned())
            .collect();
        assert!(
            reaching.is_empty(),
            "leg D, pass {pass}: a range read reached `retire:` ({reaching:?}) — that namespace is \
             not bounded by cardinality, and a draining retirement is looked up by key"
        );
    };

    // Pass 1: both retirements still draining.
    let outcome = gc.pass(&meta, &fleet, NOW).await;
    assert!(outcome.is_ok(), "leg D, pass 1: {outcome:?}");
    for (dserver, f, token, _) in &draining {
        assert!(
            disks[*dserver as usize].holds(*f),
            "leg D, pass 1: {f:?}'s mark names the retirement `{token}`, whose obligation is still \
             installed — its drain has not finished, and the fragment was reclaimed"
        );
        assert_eq!(
            meta.value(&orphan_key(*dserver, *f)),
            Some(structured(0, &token.to_string())),
            "leg D, pass 1: a mark a draining retirement protects is left as it is"
        );
    }
    assert!(
        !disks[2].holds(plain) && meta.value(&orphan_key(2, plain)).is_none(),
        "leg D, pass 1: the legacy control was not reclaimed"
    );
    assert!(
        !disks[2].holds(drained) && meta.value(&orphan_key(2, drained)).is_none(),
        "leg D, pass 1: a structured mark whose retirement's obligation is gone is judged on its \
         own grace, and was not reclaimed"
    );
    let gets = meta.take_gets();
    for (_, _, token, _) in &draining {
        assert!(
            gets.contains(&retire_key(RetireMode::Bytes, token)),
            "leg D, pass 1: the pass kept the fragment without reading its retirement's obligation \
             by key"
        );
    }
    no_retire_scan(&meta.take_scans(), 1);

    // Each retirement's drain finishes in turn; the next pass reclaims what it named, and only
    // that.
    for (pass, finished) in [(2, 0), (3, 1)] {
        let (dserver, f, token, _) = &draining[finished];
        let drain = WriteBatch::new().delete(retire_key(RetireMode::Bytes, token));
        assert_eq!(meta.commit(drain).await.unwrap(), CommitOutcome::Committed);
        let outcome = gc.pass(&meta, &fleet, NOW).await;
        assert_eq!(
            outcome.ok(),
            Some(Reconciled::Changed),
            "leg D, pass {pass}: the pass"
        );
        assert!(
            !disks[*dserver as usize].holds(*f) && meta.value(&orphan_key(*dserver, *f)).is_none(),
            "leg D, pass {pass}: `{token}` has drained, so {f:?}'s mark is judged on its own grace \
             — long past — and the fragment was not reclaimed"
        );
        for (other_dserver, other, other_token, _) in &draining[finished + 1..] {
            assert!(
                disks[*other_dserver as usize].holds(*other),
                "leg D, pass {pass}: {other:?} is still protected by `{other_token}`"
            );
        }
        no_retire_scan(&meta.take_scans(), pass);
    }
}
