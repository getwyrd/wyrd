//! Issue #800 (a child of #637, split from #661): GC **sweeps an `orphan:` mark whose position
//! holds no fragment**, once no fragment can still land under it (proposal 0016,
//! `0016:1359-1408`, X87, X91, X96).
//!
//! On `main`, GC consumes a mark only while it walks a `list_fragments()` result (the fleet walk in
//! `crates/custodian/src/gc.rs`), so a mark whose position holds no fragment is never visited and
//! never deleted. `main` already writes such marks — every repair of a missing fragment marks that
//! fragment's old position (`crates/custodian/src/reconstruction.rs`), where by definition nothing
//! is stored — and 0016 adds producers by design. The paged walk survives the ledger's size, but
//! every lap re-reads marks nothing will ever remove.
//!
//! **`D`, the late-write deadline, is hard-coded: 41 s** — `W_repoint + W_write + δ_clock`, that is
//! 10 s + 30 s + 1 s (`gc::LATE_WRITE_DEADLINE_MILLIS`). It is written as a number because this
//! file compiles against `main`, where no such constant exists, and so that a change to it fails
//! here instead of silently moving the bound this file pins. [`GRACE`] is the deployed grace window
//! (`GC_GRACE_WINDOW_MILLIS` = `LEASE_TTL_MILLIS`, `crates/server/src/custodian.rs`) and [`W`] the
//! production batch (`gc::CLEANUP_BATCH`), pinned the same way.
//!
//! Every leg runs the production `reconcile_step`, with a FRESH `GcContext` per pass, as the
//! deployed loop builds one (`crates/server/src/custodian.rs`), over in-memory doubles built as
//! `gc_ledger_walk.rs` builds them: a metadata double over an ordered map with a lowered cap, whose
//! `scan_page` pages its own truth, and D-server doubles whose `list_fragments()` the test
//! controls. The metadata double injects what the legs need — a writer landing between the pass's
//! read of a mark and its delete, a commit that fails — and logs every commit and every keyed read
//! in order. Structured and `reclaiming` values are seeded as raw JSON, as 0016 spells them, and
//! the audit trail is read back as the pass wrote it. The file names no symbol the change adds, so
//! it compiles against `main` and fails there by assertion. Every leg that asserts a mark survives
//! also seeds a control the pass does sweep, so no such leg passes on a pass that swept nothing.
//!
//! The legs:
//!
//! * **A** — a fragment-less mark aged exactly `D`, judged on this pass's own listing, is deleted,
//!   by a delete conditioned on the value read, and the delete is audited on the durability seam
//!   and counted. `main`: it survives every pass.
//! * **B** — only then. (i) The same mark aged `D − 1` survives. (ii) So does one whose D server is
//!   not in the pass's fleet. (iii) A listed position is the reclaim's, not the sweep's. (iv) A
//!   listing from an earlier pass never licenses a sweep (X96). (v) A fleet naming one server twice
//!   never makes a listed position look unlisted. (vi) An incomplete reference set sweeps nothing
//!   (the pass answers `Blocked`), and a referenced position is never swept. (vii) Each of the
//!   marks written after their fragment — legacy, and `reclaiming` with and without its event —
//!   is swept on its own stamp; a value that is none of the three shapes is never swept, stays
//!   byte-identical and is named. (viii) A structured mark that is not `reclaiming` — one that
//!   may have been written ahead of its fragment — is never swept, however old, and is named
//!   `event-may-await-write`: a write whose effect the store could not verify may still land under
//!   it (PR #821 review).
//! * **D** — a sweep never deletes a mark newer than the one it judged, and its accounting matches
//!   what landed. (i) A mark re-stamped between the pass's read and its delete survives, and is
//!   neither audited nor counted. (ii) A commit that fails partway through the sweep leaves claimed
//!   exactly the deletes that landed — across a batch boundary and inside the per-mark retry. (iii)
//!   After a lost precondition the pass concludes only what a fresh read of the mark shows: a mark
//!   rewritten and then deleted under it is claimed neither swept nor protecting, and one that
//!   still reads as it did is left for the next pass. (iv) A commit whose result is unknown but
//!   out of flight is settled by a fresh read: keys gone are claimed swept, marked as settled by a
//!   re-read; keys still there are left. (v) One that may still commit is named unsettled, claims
//!   nothing, and ends the pass on the fault (PR #821 review). No commit carries more than `CW`
//!   deletes.
//! * **E** — a differently spelled key never costs a mark: it lends the mark neither its stamp nor
//!   its listing, and is itself never deleted or rewritten, only named.
//!
//! Leg C — `D` strictly inside the deployed grace — is a compile-time assertion beside the deployed
//! constant (`crates/server/src/custodian.rs`), and the seeded DST property is property 15 of
//! `crates/dst/tests/custodian.rs`.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, orphan_key, parse_orphan_key, ChunkRef, EcScheme, InodeRecord, InodeState, ORPHAN_PREFIX,
};
use wyrd_custodian::{
    mark_orphaned, reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext,
    ReconcileError, Reconciled,
};
use wyrd_traits::{
    chunk_hex, page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome,
    CommitUnknownResult, DServerId, FragmentId, Health, MetadataStore, PageStart, Result,
    ScanCapExceeded, ScanPage, WriteBatch,
};

/// **D** — the late-write deadline, `W_repoint + W_write + δ_clock`
/// (`gc::LATE_WRITE_DEADLINE_MILLIS`, `0016:1381-1391`): 10 s + 30 s + 1 s. A mark at least this
/// old whose position this pass's own listing shows empty is swept; one a millisecond younger is
/// not.
const D: u64 = 41_000;

/// The grace window every pass runs with: the deployed one (`GC_GRACE_WINDOW_MILLIS` =
/// `LEASE_TTL_MILLIS` = 60 s). `D` sits strictly inside it, so a mark aged `D` is still inside its
/// grace — the reclaim of a listed fragment under it waits, while the sweep of a fragment-less one
/// does not.
const GRACE: u64 = 60_000;

/// **W**, the production blind batch (`gc::CLEANUP_BATCH`): the byte bound every GC commit keeps.
const W: usize = 1_000;
/// **CW**, the production conditional batch (`gc::CONDITIONAL_BATCH`, half of
/// `multipart::MAX_BATCH_OPS`): the most marks one commit of the sweep deletes — each delete a
/// precondition plus a mutation, two sequential operations on the networked backends, so the
/// bound is the transaction's operation budget, not its byte budget (PR #821 review). Pinned by
/// literal, as `W` is.
const CW: usize = 250;

/// The double's cap on one `scan` answer and on one `scan_page` page, lowered as
/// `gc_ledger_walk.rs` lowers it: a pass reads even these small ledgers in several pages.
const CAP: usize = 4;

/// Where the marks are stamped: far from zero, so a stamp a millisecond either side of it is valid.
const T0: u64 = 1_000_000;

/// The GC audit target every line this file reads is written under.
const AUDIT: &str = r#""target":"wyrd.custodian.gc.audit""#;

/// The counter a landed sweep ticks.
const SWEPT_COUNTER: &str = r#""monotonic_counter.gc_orphan_marks_swept":1"#;

// ---- what the metadata double saw ----

/// One observation, in the order the pass produced it.
#[derive(Clone, Debug)]
enum Event {
    /// A commit was answered.
    Commit(Commit),
    /// `get` read this key.
    Get(Vec<u8>),
}

/// One commit as the metadata double answered it.
#[derive(Clone, Debug)]
struct Commit {
    preconditions: Vec<Vec<u8>>,
    deletes: Vec<Vec<u8>>,
    /// `None` when the double failed it with an error.
    outcome: Option<CommitOutcome>,
}

impl Commit {
    /// Whether it is a conditional delete of `key`: a precondition on the key and a delete of it —
    /// the shape of the sweep's delete, and of no other commit a GC pass makes.
    fn conditionally_deletes(&self, key: &[u8]) -> bool {
        self.preconditions.iter().any(|k| k == key) && self.deletes.iter().any(|k| k == key)
    }
}

// ---- the metadata double ----

/// A writer that lands on a key the moment the pass's conditional delete of that key arrives,
/// before the store answers it — between the pass's read of the mark and its delete.
enum Racer {
    /// Rewrite the key to this value.
    Rewrite(Bytes),
    /// Rewrite the key to this value, and then delete it.
    RewriteThenDelete(Bytes),
    /// Hold the key: answer this many conditional deletes of it `Conflict`, changing nothing — a
    /// concurrent commit on the key, not a new value.
    Hold(usize),
}

/// A commit that deletes `key` answers `CommitUnknownResult` — after applying the batch or not,
/// and out of flight or possibly still to be applied. Taken once.
struct UnknownCommit {
    key: Vec<u8>,
    applied: bool,
    may_still_commit: bool,
}

/// Interleavings and faults the metadata double injects on request.
#[derive(Default)]
struct Faults {
    /// Per key, the writer that lands on it (see [`Racer`]).
    racers: BTreeMap<Vec<u8>, Racer>,
    /// Fail — `Err`, nothing applied — every commit that would otherwise land and deletes this key.
    fail_deleting: Option<Vec<u8>>,
    /// Answer an unknown result for the first commit that would otherwise land and deletes its key.
    unknown_deleting: Option<UnknownCommit>,
}

/// An in-memory metadata store over an ORDERED map, with a lowered cap (see the module docs).
#[derive(Default)]
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    events: Mutex<Vec<Event>>,
    faults: Mutex<Faults>,
}

impl Meta {
    fn seed(&self, key: Vec<u8>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key, value.into());
    }

    fn remove(&self, key: &[u8]) {
        self.kv.lock().unwrap().remove(key);
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().unwrap()
    }

    /// Everything since the last call, clearing it.
    fn take_events(&self) -> Vec<Event> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }

    fn log(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.log(Event::Get(key.to_vec()));
        Ok(self.value(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(CAP + 1)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        // `>` not `>=`: exactly `CAP` keys is a complete answer, as on every backend.
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
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut commit = Commit {
            preconditions: batch
                .preconditions
                .iter()
                .map(|pre| pre.key.clone())
                .collect(),
            deletes: batch.deletes.clone(),
            outcome: None,
        };
        // A racing writer lands first, on a key this commit deletes under a precondition.
        let mut held = false;
        {
            let mut faults = self.faults();
            let targets: Vec<Vec<u8>> = faults
                .racers
                .keys()
                .filter(|key| commit.conditionally_deletes(key))
                .cloned()
                .collect();
            for key in targets {
                match faults.racers.remove(&key) {
                    Some(Racer::Rewrite(value)) => self.seed(key, value),
                    Some(Racer::RewriteThenDelete(value)) => {
                        self.seed(key.clone(), value);
                        self.remove(&key);
                    }
                    Some(Racer::Hold(times)) => {
                        held = true;
                        if times > 1 {
                            faults.racers.insert(key, Racer::Hold(times - 1));
                        }
                    }
                    None => {}
                }
            }
        }
        let preconditions_hold = {
            let kv = self.kv.lock().unwrap();
            batch
                .preconditions
                .iter()
                .all(|pre| kv.get(&pre.key) == pre.expected.as_ref())
        };
        if held || !preconditions_hold {
            commit.outcome = Some(CommitOutcome::Conflict);
            self.log(Event::Commit(commit));
            return Ok(CommitOutcome::Conflict);
        }
        let fail = self
            .faults()
            .fail_deleting
            .as_ref()
            .is_some_and(|key| batch.deletes.contains(key));
        if fail {
            self.log(Event::Commit(commit));
            return Err(BoxError::from(
                "simulated metadata fault committing a batch of deletes",
            ));
        }
        // An unknown result: the batch is applied, or not, and the store says it cannot tell.
        let unknown = {
            let mut faults = self.faults();
            match &faults.unknown_deleting {
                Some(u) if batch.deletes.contains(&u.key) => faults.unknown_deleting.take(),
                _ => None,
            }
        };
        if let Some(u) = unknown {
            if u.applied {
                let mut kv = self.kv.lock().unwrap();
                for (key, value) in &batch.puts {
                    kv.insert(key.clone(), value.clone());
                }
                for key in &batch.deletes {
                    kv.remove(key);
                }
            }
            self.log(Event::Commit(commit));
            return Err(Box::new(CommitUnknownResult {
                backend: "simulated",
                code: None,
                detail: "simulated unknown commit result".to_owned(),
                may_still_commit: u.may_still_commit,
            }));
        }
        {
            let mut kv = self.kv.lock().unwrap();
            for (key, value) in batch.puts {
                kv.insert(key, value);
            }
            for key in &batch.deletes {
                kv.remove(key);
            }
        }
        commit.outcome = Some(CommitOutcome::Committed);
        self.log(Event::Commit(commit));
        Ok(CommitOutcome::Committed)
    }
}

// ---- the D-server double ----

/// One D server's fragment bytes — a deliberately dumb `ChunkStore` whose listing the test
/// controls.
#[derive(Default)]
struct Disk {
    frags: Mutex<BTreeSet<(ChunkId, u16)>>,
}

impl Disk {
    fn put(&self, frag: FragmentId) {
        self.frags.lock().unwrap().insert((frag.chunk, frag.index));
    }

    fn holds(&self, frag: FragmentId) -> bool {
        self.frags
            .lock()
            .unwrap()
            .contains(&(frag.chunk, frag.index))
    }
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(
        &self,
        id: FragmentId,
        _fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.put(id);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.holds(id).then(|| Bytes::from_static(b"bytes")))
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self
            .frags
            .lock()
            .unwrap()
            .iter()
            .map(|&(chunk, index)| FragmentId { chunk, index })
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&(id.chunk, id.index));
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// `count` D servers, D server `i` at index `i`.
fn disks(count: usize) -> Vec<Disk> {
    (0..count).map(|_| Disk::default()).collect()
}

/// The fleet view a `GcContext` takes: `disks[i]` is D server `i`.
fn fleet_of(disks: &[Disk]) -> Vec<(DServerId, &dyn ChunkStore)> {
    disks
        .iter()
        .enumerate()
        .map(|(id, disk)| (id as DServerId, disk as &dyn ChunkStore))
        .collect()
}

// ---- the pass, and what it wrote ----

/// A permissive global default, installed before any callsite a pass fires is first hit, so none
/// can latch `Interest::never` under the parallel harness and leave a capture empty (#214; the
/// guard `crates/custodian/tests/gc.rs` installs).
fn install_global_default() {
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
}

/// Collects what a `tracing` subscriber writes, so a leg reads back the audit lines the pass
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

/// One custodian, and one GC pass at a time the way the deployed loop runs one.
struct Gc {
    zone: FencedZone,
    custodian: Custodian,
}

impl Gc {
    async fn elect() -> Self {
        let custodian = Custodian::elect(&MemCoordination::new(), "zone-gc-mark-sweep")
            .await
            .unwrap();
        let mut zone = FencedZone::new();
        zone.install(custodian.leadership());
        Self { zone, custodian }
    }

    /// One GC pass at `now`, through the fenced `reconcile_step`, with a FRESH `GcContext` built
    /// for it alone — as the deployed loop builds one — under a capture of everything it emits.
    async fn pass(&self, meta: &Meta, fleet: &[(DServerId, &dyn ChunkStore)], now: u64) -> Pass {
        let capture = Capture::default();
        let ctx = GcContext {
            meta,
            fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let outcome = reconcile_step(
            &self.zone,
            &self.custodian,
            Some(&ctx),
            None,
            None,
            None,
            now,
        )
        .with_subscriber(tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(capture.clone()),
            ),
        ))
        .await;
        let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        Pass { outcome, log }
    }
}

/// What one pass answered and wrote.
struct Pass {
    outcome: std::result::Result<Reconciled, ReconcileError>,
    log: String,
}

impl Pass {
    /// The pass's answer, failing the leg with the error if it had none.
    fn answer(&self, leg: &str) -> Reconciled {
        match &self.outcome {
            Ok(answer) => *answer,
            Err(err) => panic!("{leg}: the GC pass failed: {err}"),
        }
    }

    /// Every audit line of `action` naming `frag` on `dserver`.
    fn audit_of(&self, action: &str, dserver: DServerId, frag: FragmentId) -> Vec<&str> {
        let action = format!(r#""action":"{action}""#);
        self.log
            .lines()
            .filter(|line| {
                line.contains(AUDIT) && line.contains(&action) && names(line, dserver, frag)
            })
            .collect()
    }

    /// How many audit lines of `action` this pass wrote, whatever they name.
    fn audits(&self, action: &str) -> usize {
        let action = format!(r#""action":"{action}""#);
        self.log
            .lines()
            .filter(|line| line.contains(AUDIT) && line.contains(&action))
            .count()
    }

    /// Whether this pass audited a sweep of `frag` on `dserver`.
    fn swept(&self, dserver: DServerId, frag: FragmentId) -> bool {
        !self.audit_of("sweep-mark", dserver, frag).is_empty()
    }

    /// The reasons of every `skip-mark` audit line naming `frag` on `dserver`.
    fn mark_skips(&self, dserver: DServerId, frag: FragmentId) -> Vec<String> {
        self.audit_of("skip-mark", dserver, frag)
            .into_iter()
            .filter_map(|line| string_field(line, "reason"))
            .collect()
    }

    /// How many times the sweep counter ticked.
    fn counted(&self) -> usize {
        self.log.matches(SWEPT_COUNTER).count()
    }
}

/// Whether an audit line names `frag` on `dserver`.
fn names(line: &str, dserver: DServerId, frag: FragmentId) -> bool {
    number_field(line, "dserver", dserver)
        && number_field(line, "index", u64::from(frag.index))
        && line.contains(&format!(r#""chunk":"{}""#, chunk_hex(frag.chunk)))
}

/// Whether `line` carries the numeric JSON field `name` holding exactly `value` (not a longer
/// number it is a prefix of).
fn number_field(line: &str, name: &str, value: u64) -> bool {
    let needle = format!(r#""{name}":{value}"#);
    line.match_indices(&needle)
        .any(|(at, _)| !line[at + needle.len()..].starts_with(|c: char| c.is_ascii_digit()))
}

/// The string JSON field `name` of `line`, if it has one.
fn string_field(line: &str, name: &str) -> Option<String> {
    let needle = format!(r#""{name}":""#);
    let start = line.find(&needle)? + needle.len();
    let end = line[start..].find('"')?;
    Some(line[start..start + end].to_string())
}

// ---- fixture helpers ----

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
    let written = Meta::default();
    mark_orphaned(&written, 3, frag(77, 2), 4_242)
        .await
        .unwrap();
    assert_eq!(
        written.value(&orphan_key(3, frag(77, 2))),
        Some(legacy(4_242)),
        "fixture: a seeded legacy mark must be exactly what `mark_orphaned` writes"
    );
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

/// Seed a control: a fragment-less mark on D server `dserver`, stamped so it is exactly `D` old at
/// `now` — one the pass must sweep, so a leg asserting survival cannot pass on a pass that swept
/// nothing.
fn seed_control(meta: &Meta, dserver: DServerId, chunk: ChunkId, now: u64) -> FragmentId {
    let control = frag(chunk, 0);
    meta.seed(orphan_key(dserver, control), legacy(now - D));
    control
}

/// Assert the control seeded by [`seed_control`] was swept by `pass`, once, and audited.
fn assert_control_swept(
    leg: &str,
    meta: &Meta,
    pass: &Pass,
    dserver: DServerId,
    control: FragmentId,
) {
    assert!(
        meta.value(&orphan_key(dserver, control)).is_none(),
        "{leg}: the control — a fragment-less mark aged exactly D — was not swept, so this pass \
         swept nothing and the leg proves nothing"
    );
    assert_eq!(
        pass.audit_of("sweep-mark", dserver, control).len(),
        1,
        "{leg}: the control's sweep must be audited exactly once. got: {}",
        pass.log
    );
}

// ---- leg A: a fragment-less mark is swept once that is safe ----

/// A mark at a position no listed server reports, aged exactly `D`, under a pass whose listing was
/// taken at `orphaned_at + D`: deleted, by a delete conditioned on the mark's value; audited on the
/// GC audit seam naming its position; counted once; and the pass answers `Changed`. A fragment
/// under a young mark beside it is the walk's, and untouched. The next pass finds nothing left to
/// sweep. `main`: the mark survives every pass.
#[tokio::test]
async fn a_a_fragment_less_mark_aged_exactly_d_is_swept_audited_and_counted() {
    install_global_default();
    assert_legacy_is_mark_orphaned().await;
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let mark = frag(0xA1, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    // A neighbour: a fragment on disk under a mark stamped now, inside its grace window.
    let neighbour = frag(0xA2, 0);
    disks[1].put(neighbour);
    meta.seed(orphan_key(1, neighbour), legacy(T0 + D));
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, T0 + D).await;
    let answer = pass.answer("leg A");
    assert!(
        meta.value(&key).is_none(),
        "leg A: a mark with no fragment beneath it, aged exactly D, whose position this pass's \
         own listing showed empty, must be swept — it survived. On main GC visits a mark only \
         through a listed fragment, so this mark is never visited and never deleted"
    );
    let swept = pass.audit_of("sweep-mark", 1, mark);
    assert_eq!(
        swept.len(),
        1,
        "leg A: the sweep must be audited once, on the GC audit seam, naming the mark's position. \
         got: {}",
        pass.log
    );
    assert_eq!(
        pass.counted(),
        1,
        "leg A: the sweep must be counted once ({SWEPT_COUNTER}). got: {}",
        pass.log
    );
    assert_eq!(
        answer,
        Reconciled::Changed,
        "leg A: a pass that swept a mark converged something"
    );
    let events = meta.take_events();
    let deleting: Vec<&Commit> = events
        .iter()
        .filter_map(|event| match event {
            Event::Commit(commit)
                if commit.outcome == Some(CommitOutcome::Committed)
                    && commit.deletes.contains(&key) =>
            {
                Some(commit)
            }
            _ => None,
        })
        .collect();
    assert!(
        deleting.len() == 1 && deleting[0].preconditions.contains(&key),
        "leg A: the mark must be deleted by exactly one commit, conditioned on the mark's value \
         — a blind delete loses a concurrent refresh: {deleting:?}"
    );
    assert!(
        disks[1].holds(neighbour) && meta.value(&orphan_key(1, neighbour)) == Some(legacy(T0 + D)),
        "leg A: the listed fragment and its mark, inside grace, are the walk's and untouched"
    );

    let again = gc.pass(&meta, &fleet, T0 + D).await;
    assert_eq!(
        (again.answer("leg A, again"), again.audits("sweep-mark")),
        (Reconciled::Satisfied, 0),
        "leg A: with the mark gone the next pass has nothing to sweep. got: {}",
        again.log
    );
}

// ---- leg B: only then ----

/// **B(i).** A mark aged `D − 1` survives byte-identical, kept on its deadline, while a control
/// aged exactly `D` in the same pass is swept: the boundary is `D`, inclusive.
#[tokio::test]
async fn b1_a_mark_aged_one_millisecond_short_of_d_survives() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let young = frag(0xB1, 0);
    meta.seed(orphan_key(1, young), legacy(T0 + 1));
    let control = seed_control(&meta, 1, 0xB2, now);
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(i)");
    assert_eq!(
        meta.value(&orphan_key(1, young)),
        Some(legacy(T0 + 1)),
        "leg B(i): a mark aged D − 1 ms was swept or rewritten: a fragment may still land under it"
    );
    assert!(
        !pass.swept(1, young),
        "leg B(i): the young mark's sweep was claimed"
    );
    assert_eq!(
        pass.mark_skips(1, young),
        ["within-late-write-deadline"],
        "leg B(i): the young mark must be kept on its late-write deadline. got: {}",
        pass.log
    );
    assert_control_swept("leg B(i)", &meta, &pass, 1, control);
    assert_eq!(pass.counted(), 1, "leg B(i): only the control is counted");
}

/// **B(ii).** A mark on a D server that is not in this pass's fleet — so no listing of this pass
/// could show its position empty — survives byte-identical however old, and is named as such.
#[tokio::test]
async fn b2_a_mark_on_a_d_server_outside_the_fleet_survives() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let stray = frag(0xB3, 0);
    meta.seed(orphan_key(7, stray), legacy(0));
    let control = seed_control(&meta, 1, 0xB4, now);
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(ii)");
    assert_eq!(
        meta.value(&orphan_key(7, stray)),
        Some(legacy(0)),
        "leg B(ii): a mark on D server 7, which this pass did not list, was swept — its position \
         was never observed empty"
    );
    assert!(!pass.swept(7, stray), "leg B(ii): its sweep was claimed");
    assert_eq!(
        pass.mark_skips(7, stray),
        ["server-not-in-fleet"],
        "leg B(ii): the mark must be kept because its D server is not in the fleet. got: {}",
        pass.log
    );
    assert_control_swept("leg B(ii)", &meta, &pass, 1, control);
}

/// **B(iii).** A mark whose position IS listed — a fragment is on disk under it — is the reclaim
/// path's, not the sweep's: aged `D` it is inside grace, so the walk keeps both the fragment and
/// the mark and the sweep never touches either; once its grace elapses the walk reclaims it, as a
/// reclaim and not as a sweep.
#[tokio::test]
async fn b3_a_listed_position_is_the_reclaims_not_the_sweeps() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let listed = frag(0xB5, 0);
    disks[1].put(listed);
    meta.seed(orphan_key(1, listed), legacy(T0));
    let control = seed_control(&meta, 1, 0xB6, now);
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(iii)");
    assert!(
        disks[1].holds(listed) && meta.value(&orphan_key(1, listed)) == Some(legacy(T0)),
        "leg B(iii): a mark aged D over a listed fragment, inside grace, lost its fragment or was \
         swept"
    );
    assert!(
        !pass.swept(1, listed) && pass.mark_skips(1, listed).is_empty(),
        "leg B(iii): the sweep judged a listed position. got: {}",
        pass.log
    );
    assert_eq!(
        pass.audit_of("skip", 1, listed).len(),
        1,
        "leg B(iii): the walk must have judged the listed fragment (within grace). got: {}",
        pass.log
    );
    assert_control_swept("leg B(iii)", &meta, &pass, 1, control);

    // Its grace elapses: the walk reclaims it, fragment then mark — never the sweep.
    let later = gc.pass(&meta, &fleet, T0 + GRACE).await;
    later.answer("leg B(iii), grace elapsed");
    assert!(
        !disks[1].holds(listed) && meta.value(&orphan_key(1, listed)).is_none(),
        "leg B(iii): past grace the walk reclaims the fragment and consumes its mark"
    );
    assert!(
        !later.audit_of("reclaim", 1, listed).is_empty() && !later.swept(1, listed),
        "leg B(iii): the listed position is reclaimed by the walk, not swept. got: {}",
        later.log
    );
}

/// **B(iv) — a listing from an earlier pass never licenses a sweep (X96, `0016:2625`).** Pass 1
/// runs with the mark aged `D − 1` and sees its position empty. A fragment then lands there, still
/// inside its writer's deadline. Pass 2 runs with the mark aged exactly `D`, and pass 3 just short
/// of its grace: only a pass's own listing may show the position empty, and theirs do not, so the
/// mark and the fragment survive both. A sweep that trusted pass 1's observation — or checked the
/// mark's age alone — deletes the evidence for a fragment that now exists.
#[tokio::test]
async fn b4_a_listing_from_an_earlier_pass_never_licenses_a_sweep() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let mark = frag(0xB7, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    let gc = Gc::elect().await;

    let first = gc.pass(&meta, &fleet, T0 + D - 1).await;
    first.answer("leg B(iv), pass 1");
    assert_eq!(
        meta.value(&key),
        Some(legacy(T0)),
        "leg B(iv): aged D − 1, the mark must survive pass 1"
    );
    // The fragment lands after pass 1's listing, inside its writer's deadline.
    disks[1].put(mark);

    for (pass_no, now) in [(2, T0 + D), (3, T0 + GRACE - 1)] {
        let control = seed_control(&meta, 2, 0xB8 + pass_no, now);
        let pass = gc.pass(&meta, &fleet, now).await;
        pass.answer("leg B(iv)");
        assert!(
            meta.value(&key) == Some(legacy(T0)) && disks[1].holds(mark),
            "leg B(iv), pass {pass_no}: the mark was deleted, or its fragment reclaimed, although \
             this pass's own listing shows the fragment — a listing an earlier pass took licensed \
             the delete"
        );
        assert!(
            !pass.swept(1, mark),
            "leg B(iv), pass {pass_no}: a sweep of the mark was claimed"
        );
        assert_control_swept("leg B(iv)", &meta, &pass, 2, control);
    }
}

/// **B(v).** A fleet naming one D server twice — one of its stores holding the fragment, the other
/// not — never makes the listed position look unlisted, whichever of the two is listed last: the
/// mark aged `D`, inside grace, survives with its fragment.
#[tokio::test]
async fn b5_a_fleet_naming_one_server_twice_never_unlists_a_listed_position() {
    install_global_default();
    let gc = Gc::elect().await;
    for holder_first in [true, false] {
        let meta = Meta::default();
        let other = Disk::default();
        let holder = Disk::default();
        let empty = Disk::default();
        let mark = frag(0xB9, 0);
        holder.put(mark);
        meta.seed(orphan_key(1, mark), legacy(T0));
        let now = T0 + D;
        let control = seed_control(&meta, 0, 0xBA, now);
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = if holder_first {
            vec![(0, &other), (1, &holder), (1, &empty)]
        } else {
            vec![(0, &other), (1, &empty), (1, &holder)]
        };

        let pass = gc.pass(&meta, &fleet, now).await;
        pass.answer("leg B(v)");
        assert!(
            meta.value(&orphan_key(1, mark)) == Some(legacy(T0)) && holder.holds(mark),
            "leg B(v) (holder listed {}): a fleet naming D server 1 twice made its listed position \
             look unlisted, and the mark over the fragment was swept",
            if holder_first { "first" } else { "last" }
        );
        assert!(
            !pass.swept(1, mark),
            "leg B(v): a sweep of the mark was claimed"
        );
        assert_control_swept("leg B(v)", &meta, &pass, 0, control);
    }
}

/// **B(vi), incomplete.** While a committed record cannot be read, the reference set is incomplete:
/// the pass answers `Blocked`, and the sweep — answering to the reclaim's own gate — deletes
/// nothing, and says so. Once the record is repaired, the next pass sweeps the mark.
#[tokio::test]
async fn b6_an_incomplete_reference_set_sweeps_nothing() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let mark = frag(0xBB, 0);
    meta.seed(orphan_key(1, mark), legacy(T0));
    let unreadable = metadata::inode_key(9);
    meta.seed(
        unreadable.clone(),
        Bytes::from_static(b"not an inode record"),
    );
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert_eq!(
        pass.answer("leg B(vi), incomplete"),
        Reconciled::Blocked,
        "leg B(vi): an unreadable committed record makes the reference set incomplete"
    );
    assert_eq!(
        meta.value(&orphan_key(1, mark)),
        Some(legacy(T0)),
        "leg B(vi): the sweep deleted a mark while the reference set was incomplete"
    );
    assert_eq!(
        (pass.counted(), pass.mark_skips(1, mark)),
        (0, vec!["incomplete-reference-set".to_string()]),
        "leg B(vi): an incomplete set sweeps nothing, and names why the mark was kept. got: {}",
        pass.log
    );

    // The record is repaired: the same mark, the next pass, swept.
    meta.remove(&unreadable);
    let repaired = gc.pass(&meta, &fleet, now).await;
    assert_eq!(
        repaired.answer("leg B(vi), repaired"),
        Reconciled::Changed,
        "leg B(vi): with the set complete again the pass sweeps"
    );
    assert!(
        meta.value(&orphan_key(1, mark)).is_none() && repaired.swept(1, mark),
        "leg B(vi): once the record is repaired the mark is swept. got: {}",
        repaired.log
    );
}

/// **B(vi), referenced.** A mark at a position a committed chunk map places — its fragment lost,
/// so nothing is listed there — is never swept, however old: the reference set protects the
/// position, exactly as it protects a fragment from a reclaim.
#[tokio::test]
async fn b6_a_referenced_position_is_never_swept() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let placed = frag(0xBC, 0);
    meta.seed(
        metadata::inode_key(10),
        metadata::encode(&placing(placed.chunk, 1)),
    );
    meta.seed(orphan_key(1, placed), legacy(0));
    let control = seed_control(&meta, 1, 0xBD, now);
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(vi), referenced");
    assert_eq!(
        meta.value(&orphan_key(1, placed)),
        Some(legacy(0)),
        "leg B(vi): a mark at a position a committed chunk map places was swept"
    );
    assert_eq!(
        pass.mark_skips(1, placed),
        ["referenced"],
        "leg B(vi): the mark must be kept by the reference set. got: {}",
        pass.log
    );
    assert_control_swept("leg B(vi), referenced", &meta, &pass, 1, control);
}

/// **B(vii).** Each shape a mark written after its fragment takes is swept on its own stamp: aged
/// exactly `D` — a legacy decimal, a `reclaiming` mark with and without its event — it is swept;
/// stamped a millisecond later it is kept, byte-identical. A value that is none of the three
/// shapes, however old any stamp read out of it would be, is never swept, is left byte-identical,
/// and is named on the audit seam carrying its key (ADR-0045 decision 3). The structured,
/// non-`reclaiming` shape is leg B(viii)'s.
#[tokio::test]
async fn b7_each_shape_is_swept_on_its_stamp_and_a_fourth_is_never_swept() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let shapes = |at: u64| {
        [
            legacy(at),
            reclaiming(at, Some("move-8007")),
            reclaiming(at, None),
        ]
    };
    let old: Vec<(FragmentId, Bytes)> = shapes(T0)
        .into_iter()
        .enumerate()
        .map(|(i, value)| (frag(0xC0 + i as ChunkId, 0), value))
        .collect();
    let young: Vec<(FragmentId, Bytes)> = shapes(T0 + 1)
        .into_iter()
        .enumerate()
        .map(|(i, value)| (frag(0xD0 + i as ChunkId, 0), value))
        .collect();
    let fourth: Vec<(FragmentId, Bytes)> = [
        b"not a mark".as_slice(),
        b"007",
        br#"{"orphaned_at_millis":0}"#,
        br#"{"orphaned_at_millis":0,"event":"move-8007","reclaiming":false}"#,
        br#"{"event":"move-8007","orphaned_at_millis":0}"#,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| (frag(0xE0 + i as ChunkId, 0), Bytes::copy_from_slice(value)))
    .collect();
    for (f, value) in old.iter().chain(&young).chain(&fourth) {
        meta.seed(orphan_key(1, *f), value.clone());
    }
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(vii)");
    for (f, value) in &old {
        assert!(
            meta.value(&orphan_key(1, *f)).is_none() && pass.swept(1, *f),
            "leg B(vii): {:?}, aged exactly D, is a shape written after its fragment and must be \
             swept on its stamp. got: {}",
            String::from_utf8_lossy(value),
            pass.log
        );
    }
    for (f, value) in &young {
        assert_eq!(
            meta.value(&orphan_key(1, *f)).as_ref(),
            Some(value),
            "leg B(vii): {:?}, aged D − 1 ms, was swept or rewritten — its stamp was not the one \
             judged",
            String::from_utf8_lossy(value)
        );
        assert_eq!(
            pass.mark_skips(1, *f),
            ["within-late-write-deadline"],
            "leg B(vii): {:?} must be kept on its own stamp. got: {}",
            String::from_utf8_lossy(value),
            pass.log
        );
    }
    for (f, value) in &fourth {
        let key = orphan_key(1, *f);
        assert_eq!(
            meta.value(&key).as_ref(),
            Some(value),
            "leg B(vii): a value that is none of the shapes must be left byte-identical"
        );
        assert!(
            !pass.swept(1, *f) && pass.mark_skips(1, *f).is_empty(),
            "leg B(vii): the sweep acted on {:?}, which has no stamp it can read",
            String::from_utf8_lossy(value)
        );
        let named = format!(r#""mark":"{}""#, String::from_utf8_lossy(&key));
        assert!(
            pass.log.lines().any(|line| line.contains(AUDIT)
                && line.contains(r#""action":"unreadable-orphan-mark""#)
                && line.contains(&named)),
            "leg B(vii): a value that is none of the shapes must be named on the GC audit seam, \
             carrying its key ({named}). got: {}",
            pass.log
        );
    }
    assert_eq!(
        pass.counted(),
        old.len(),
        "leg B(vii): exactly the swept marks are counted"
    );
}

/// **B(viii).** A structured mark that is not `reclaiming` may have been written ahead of its
/// fragment — a repoint's pre-mark, a teardown's planned placement — and `D` bounds when a write is
/// accepted, not when it takes effect: a publication straddling the deadline is `WriteEffect::
/// Unknown` with the bytes possibly landed. So such a mark is never swept on `D`, however old:
/// with a move nonce or a retirement token for its event, aged `D` and aged far past it, it is
/// kept byte-identical and named `event-may-await-write`, while the legacy control beside it is
/// swept. Negation: drop the event-class arm — the first assertion fails.
#[tokio::test]
async fn b8_a_structured_mark_that_may_await_its_write_is_never_swept_on_the_deadline() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + 10 * D;
    let awaiting: Vec<(FragmentId, Bytes)> = [
        structured(now - D, "move-8007"),
        structured(T0, "move-8007"),
        structured(now - D, "g:1:1"),
        structured(T0, "s:0123456789abcdef0123456789abcdef:7"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| (frag(0xF0 + i as ChunkId, 0), value))
    .collect();
    for (f, value) in &awaiting {
        meta.seed(orphan_key(1, *f), value.clone());
    }
    let control = seed_control(&meta, 1, 0xF8, now);
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg B(viii)");
    for (f, value) in &awaiting {
        assert_eq!(
            meta.value(&orphan_key(1, *f)).as_ref(),
            Some(value),
            "leg B(viii): {:?} is a structured mark that is not reclaiming; its writer may still \
             have a write in flight whose effect no deadline bounds, so it must never be swept on \
             the deadline — it was swept or rewritten",
            String::from_utf8_lossy(value)
        );
        assert!(
            !pass.swept(1, *f),
            "leg B(viii): audited as swept: {}",
            pass.log
        );
        assert_eq!(
            pass.mark_skips(1, *f),
            ["event-may-await-write"],
            "leg B(viii): {:?} must be kept for that reason and named. got: {}",
            String::from_utf8_lossy(value),
            pass.log
        );
    }
    assert_control_swept("leg B(viii)", &meta, &pass, 1, control);
    assert_eq!(
        pass.counted(),
        1,
        "leg B(viii): only the control is counted"
    );
}

// ---- leg D: never a newer mark, and the accounting matches what landed ----

/// **D(i).** A writer re-stamps the mark after the pass read it and before its delete lands. The
/// refreshed mark survives with its new value, and its sweep is neither audited nor counted; the
/// control beside it in the same batch is still swept, audited and counted — the lost precondition
/// costs only its own mark. The next pass judges the new value, which is young, and keeps it.
#[tokio::test]
async fn d1_a_mark_restamped_after_the_read_survives_unclaimed() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let mark = frag(0xF1, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    let control = seed_control(&meta, 1, 0xF2, now);
    let refreshed = structured(now, "move-f1");
    meta.faults()
        .racers
        .insert(key.clone(), Racer::Rewrite(refreshed.clone()));
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    pass.answer("leg D(i)");
    assert!(
        meta.faults().racers.is_empty(),
        "fixture: the re-stamp must land between the pass's read and its delete — the pass never \
         tried to delete the mark"
    );
    assert_eq!(
        meta.value(&key),
        Some(refreshed.clone()),
        "leg D(i): the mark was re-stamped after the pass read it; its delete must lose, and the \
         refreshed mark survive"
    );
    assert!(
        !pass.swept(1, mark),
        "leg D(i): the refreshed mark survived but its sweep was audited. got: {}",
        pass.log
    );
    assert_eq!(
        pass.mark_skips(1, mark),
        ["mark-changed"],
        "leg D(i): the mark must be named as changed under the pass. got: {}",
        pass.log
    );
    assert_control_swept("leg D(i)", &meta, &pass, 1, control);
    assert_eq!(
        pass.counted(),
        1,
        "leg D(i): only the control's sweep may be counted"
    );

    let next = gc.pass(&meta, &fleet, now).await;
    assert_eq!(
        (next.answer("leg D(i), next"), meta.value(&key)),
        (Reconciled::Satisfied, Some(refreshed)),
        "leg D(i): the next pass judges the refreshed value, young, and keeps it"
    );
}

/// **D(i), alone.** When the only mark a pass would sweep loses its precondition to a re-stamp, the
/// pass swept nothing and has not read the mark's new value: it answers `Partial`, never
/// `Satisfied`.
#[tokio::test]
async fn d1_a_pass_whose_only_sweep_lost_to_a_restamp_is_not_satisfied() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let mark = frag(0xF3, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    meta.faults()
        .racers
        .insert(key.clone(), Racer::Rewrite(legacy(now)));
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert_eq!(
        (
            pass.answer("leg D(i), alone"),
            meta.value(&key),
            pass.counted()
        ),
        (Reconciled::Partial, Some(legacy(now)), 0),
        "leg D(i): a pass whose only sweep lost to a re-stamp keeps the new mark, claims nothing, \
         and does not certify. got: {}",
        pass.log
    );
}

/// The ledger keys of `marks` on `dserver`, in ledger order.
fn keys_of(dserver: DServerId, marks: &[FragmentId]) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = marks.iter().map(|&f| orphan_key(dserver, f)).collect();
    keys.sort();
    keys
}

/// Split `marks` on `dserver` into those whose key is gone and those still in `meta`.
fn landed_and_not(
    meta: &Meta,
    dserver: DServerId,
    marks: &[FragmentId],
) -> (HashSet<FragmentId>, HashSet<FragmentId>) {
    marks
        .iter()
        .partition(|&&f| meta.value(&orphan_key(dserver, f)).is_none())
}

/// **D(ii), across a batch boundary.** `W + 5` fragment-less marks aged `D`, and the store fails
/// the commit that deletes the one sorting last. The pass fails with the fault — after the commits
/// before it landed. Every mark whose delete landed is audited exactly once and counted; none whose
/// delete did not land is; no commit carries more than `W` deletes. The next pass, with the fault
/// cleared, sweeps the rest and claims exactly those.
#[tokio::test]
async fn d2_a_fault_across_a_batch_boundary_claims_exactly_what_landed() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let marks: Vec<FragmentId> = (0..W + 5)
        .map(|i| frag(0x10_0000 + i as ChunkId, 0))
        .collect();
    for &f in &marks {
        meta.seed(orphan_key(1, f), legacy(T0));
    }
    let last = keys_of(1, &marks).pop().unwrap();
    meta.faults().fail_deleting = Some(last.clone());
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert!(
        matches!(pass.outcome, Err(ReconcileError::Store(_))),
        "leg D(ii): the failed commit must end the pass with its fault: {:?}",
        pass.outcome.as_ref().map(|_| ())
    );
    let (landed, not_landed) = landed_and_not(&meta, 1, &marks);
    assert!(
        !landed.is_empty() && !not_landed.is_empty(),
        "leg D(ii): the fault must strike partway through the sweep ({} landed, {} did not)",
        landed.len(),
        not_landed.len()
    );
    for &f in &landed {
        assert_eq!(
            pass.audit_of("sweep-mark", 1, f).len(),
            1,
            "leg D(ii): {f:?}'s delete landed before the fault, and must be audited exactly once"
        );
    }
    for &f in &not_landed {
        assert!(
            !pass.swept(1, f),
            "leg D(ii): {f:?}'s delete never landed, and its sweep was audited"
        );
        assert_eq!(meta.value(&orphan_key(1, f)), Some(legacy(T0)));
    }
    assert_eq!(
        (pass.audits("sweep-mark"), pass.counted()),
        (landed.len(), landed.len()),
        "leg D(ii): the audit trail and the count must say exactly how many deletes landed"
    );
    let events = meta.take_events();
    for event in &events {
        if let Event::Commit(commit) = event {
            assert!(
                commit.deletes.len() <= CW,
                "leg D(ii): a commit carried {} deletes, over the conditional batch of {CW} \
                 (two operations a mark: the transaction's operation budget, not its byte budget)",
                commit.deletes.len()
            );
        }
    }

    // The fault clears: the next pass sweeps the rest, and claims exactly those.
    meta.faults().fail_deleting = None;
    let next = gc.pass(&meta, &fleet, now).await;
    assert_eq!(next.answer("leg D(ii), next"), Reconciled::Changed);
    for &f in &not_landed {
        assert!(
            meta.value(&orphan_key(1, f)).is_none() && next.swept(1, f),
            "leg D(ii): the next pass must sweep {f:?}"
        );
    }
    assert_eq!(
        (next.audits("sweep-mark"), next.counted()),
        (not_landed.len(), not_landed.len()),
        "leg D(ii): the next pass claims exactly the rest"
    );
}

/// **D(ii), inside the retry.** Three marks in one batch; a writer re-stamps the middle one, so the
/// batch loses and each delete is tried alone; the first lands, the second loses to its new value,
/// and the store fails the third. Exactly the first is audited and counted, the second keeps its
/// new value, the third its old one, and the pass fails with the fault.
#[tokio::test]
async fn d2_a_fault_inside_the_retry_claims_exactly_what_landed() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let marks = [frag(0xF4, 0), frag(0xF5, 0), frag(0xF6, 0)];
    for &f in &marks {
        meta.seed(orphan_key(1, f), legacy(T0));
    }
    let keys = keys_of(1, &marks);
    let (first, middle, last) = (&keys[0], &keys[1], &keys[2]);
    meta.faults()
        .racers
        .insert(middle.clone(), Racer::Rewrite(legacy(now)));
    meta.faults().fail_deleting = Some(last.clone());
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert!(
        matches!(pass.outcome, Err(ReconcileError::Store(_))),
        "leg D(ii), retry: the failed commit must end the pass with its fault"
    );
    assert_eq!(
        (meta.value(first), meta.value(middle), meta.value(last)),
        (None, Some(legacy(now)), Some(legacy(T0))),
        "leg D(ii), retry: the first delete lands, the second loses to its re-stamp, the third \
         fails"
    );
    let (landed, _) = landed_and_not(&meta, 1, &marks);
    assert_eq!(landed.len(), 1, "fixture: exactly one delete landed");
    let landed = *landed.iter().next().unwrap();
    assert_eq!(
        (pass.audits("sweep-mark"), pass.counted()),
        (1, 1),
        "leg D(ii), retry: only the delete that landed is audited and counted. got: {}",
        pass.log
    );
    assert!(
        pass.swept(1, landed),
        "leg D(ii), retry: the delete that landed must be the one audited"
    );
}

/// **D(iii) — a lost precondition is judged on a fresh read, never on the `Conflict`.** A writer
/// rewrites the mark and then deletes it between the pass's read and its delete. The `Conflict` the
/// pass gets says only that a precondition lost; the key is in fact gone. So the pass reads the key
/// again after losing, and claims neither a sweep nor a protecting mark: no sweep audited or
/// counted, the mark named gone rather than changed, and the pass certifies — nothing is left for
/// it to have missed.
#[tokio::test]
async fn d3_a_mark_rewritten_then_deleted_under_the_pass_is_claimed_neither_swept_nor_kept() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let mark = frag(0xF7, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    meta.faults()
        .racers
        .insert(key.clone(), Racer::RewriteThenDelete(legacy(now)));
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    let answer = pass.answer("leg D(iii)");
    assert!(
        meta.faults().racers.is_empty(),
        "fixture: the writer must land between the pass's read and its delete — the pass never \
         tried to delete the mark"
    );
    assert_eq!(
        meta.value(&key),
        None,
        "fixture: the writer deleted the mark"
    );
    let events = meta.take_events();
    let lost = events.iter().rposition(|event| match event {
        Event::Commit(commit) => {
            commit.conditionally_deletes(&key) && commit.outcome == Some(CommitOutcome::Conflict)
        }
        Event::Get(_) => false,
    });
    let reread = events
        .iter()
        .rposition(|event| matches!(event, Event::Get(read) if *read == key));
    assert!(
        matches!((lost, reread), (Some(lost), Some(reread)) if reread > lost),
        "leg D(iii): after its delete lost, the pass must read the mark again before concluding \
         anything about it: {events:?}"
    );
    assert!(
        !pass.swept(1, mark) && pass.counted() == 0,
        "leg D(iii): the pass did not delete the mark, and claimed a sweep of it. got: {}",
        pass.log
    );
    assert_eq!(
        pass.mark_skips(1, mark),
        ["mark-gone"],
        "leg D(iii): a fresh read found no mark, so the pass must not claim one changed or kept \
         under it. got: {}",
        pass.log
    );
    assert_eq!(
        answer,
        Reconciled::Satisfied,
        "leg D(iii): nothing is left that the pass did not read — no mark protects the position — \
         so it certifies"
    );
}

/// **D(iii), unchanged.** The mark's delete loses twice — alone as well as in its batch — to a
/// concurrent commit holding the key, while its value stays exactly what the pass read. The fresh
/// read finds it there: the pass keeps it, claims nothing, and answers `Partial`; the next pass
/// sweeps it.
#[tokio::test]
async fn d3_a_delete_that_loses_to_an_unchanged_mark_leaves_it_for_the_next_pass() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let mark = frag(0xF8, 0);
    let key = orphan_key(1, mark);
    meta.seed(key.clone(), legacy(T0));
    meta.faults().racers.insert(key.clone(), Racer::Hold(2));
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert_eq!(
        (pass.answer("leg D(iii), unchanged"), meta.value(&key), pass.counted()),
        (Reconciled::Partial, Some(legacy(T0)), 0),
        "leg D(iii): a delete that lost to a held key keeps the mark, claims nothing, and does not \
         certify. got: {}",
        pass.log
    );
    assert_eq!(
        pass.mark_skips(1, mark),
        ["mark-unchanged"],
        "leg D(iii): the fresh read found the mark as the pass read it. got: {}",
        pass.log
    );

    let next = gc.pass(&meta, &fleet, now).await;
    assert!(
        next.answer("leg D(iii), next") == Reconciled::Changed
            && meta.value(&key).is_none()
            && next.swept(1, mark),
        "leg D(iii): with the key released, the next pass sweeps the mark. got: {}",
        next.log
    );
}

/// **D(iv).** The sweep's commit answers an unknown result the backend says is out of flight. The
/// pass settles it by a fresh read instead of giving up: (a) when the batch did land, every key is
/// gone, and each delete is claimed — audited once as `sweep-mark` marked settled by a re-read, and
/// counted — with no per-mark retry commit; (b) when it did not land, every mark is still there
/// and untouched, nothing is claimed, and the next pass sweeps them. Negation: drop the settle —
/// (a) ends the pass with the fault and claims nothing.
#[tokio::test]
async fn d4_an_out_of_flight_unknown_commit_is_settled_by_a_fresh_read() {
    install_global_default();
    for applied in [true, false] {
        let meta = Meta::default();
        let disks = disks(3);
        let fleet = fleet_of(&disks);
        let now = T0 + D;
        let marks: Vec<FragmentId> = (0..3).map(|i| frag(0x20_0000 + i, 0)).collect();
        for &f in &marks {
            meta.seed(orphan_key(1, f), legacy(T0));
        }
        meta.faults().unknown_deleting = Some(UnknownCommit {
            key: orphan_key(1, marks[0]),
            applied,
            may_still_commit: false,
        });
        let gc = Gc::elect().await;

        let pass = gc.pass(&meta, &fleet, now).await;
        let label = if applied { "D(iv)(a)" } else { "D(iv)(b)" };
        assert!(
            pass.outcome.is_ok(),
            "leg {label}: an out-of-flight unknown result is settled by a re-read, not a fault: {:?}",
            pass.outcome.as_ref().err().map(|e| e.to_string())
        );
        let commits = meta
            .take_events()
            .into_iter()
            .filter(|e| matches!(e, Event::Commit(c) if !c.deletes.is_empty()))
            .count();
        assert_eq!(
            commits, 1,
            "leg {label}: the unknown batch is settled by a read, never retried per mark"
        );
        if applied {
            for &f in &marks {
                assert!(
                    meta.value(&orphan_key(1, f)).is_none(),
                    "leg {label}: fixture"
                );
                let swept = pass.audit_of("sweep-mark", 1, f);
                assert_eq!(
                    swept.len(),
                    1,
                    "leg {label}: a delete that landed under an unknown result must be claimed \
                     exactly once. got: {}",
                    pass.log
                );
                assert!(
                    swept[0].contains(r#""settled":"unknown-commit-reread""#),
                    "leg {label}: the claim must say it rests on a re-read: {}",
                    swept[0]
                );
            }
            assert_eq!(
                pass.counted(),
                marks.len(),
                "leg {label}: each landed delete counted"
            );
            assert_eq!(pass.answer(label), Reconciled::Changed);
        } else {
            for &f in &marks {
                assert_eq!(
                    meta.value(&orphan_key(1, f)),
                    Some(legacy(T0)),
                    "leg {label}: a delete that did not land leaves the mark untouched"
                );
                assert!(
                    !pass.swept(1, f),
                    "leg {label}: claimed a delete that did not land"
                );
                assert_eq!(
                    pass.mark_skips(1, f),
                    ["mark-unchanged"],
                    "leg {label}: {}",
                    pass.log
                );
            }
            assert_eq!(
                pass.counted(),
                0,
                "leg {label}: nothing landed, nothing counted"
            );
            assert_ne!(
                pass.answer(label),
                Reconciled::Satisfied,
                "leg {label}: marks it meant to sweep are still there"
            );
            let next = gc.pass(&meta, &fleet, now).await;
            next.answer(label);
            for &f in &marks {
                assert!(
                    meta.value(&orphan_key(1, f)).is_none() && next.swept(1, f),
                    "leg {label}: the next pass must sweep {f:?}"
                );
            }
        }
    }
}

/// **D(v).** The sweep's commit answers an unknown result that **may still commit**: no read can
/// settle it. The pass names each mark `sweep-unsettled`, claims nothing, and ends on the fault;
/// whether the batch landed is left for the next pass to read. Negation: settle it anyway — a
/// batch that lands after the re-read is claimed as not swept and its marks vanish unaudited.
#[tokio::test]
async fn d5_an_unknown_commit_that_may_still_land_is_named_and_ends_the_pass() {
    install_global_default();
    let meta = Meta::default();
    let disks = disks(3);
    let fleet = fleet_of(&disks);
    let now = T0 + D;
    let marks: Vec<FragmentId> = (0..2).map(|i| frag(0x30_0000 + i, 0)).collect();
    for &f in &marks {
        meta.seed(orphan_key(1, f), legacy(T0));
    }
    meta.faults().unknown_deleting = Some(UnknownCommit {
        key: orphan_key(1, marks[0]),
        applied: false,
        may_still_commit: true,
    });
    let gc = Gc::elect().await;

    let pass = gc.pass(&meta, &fleet, now).await;
    assert!(
        matches!(pass.outcome, Err(ReconcileError::Store(_))),
        "leg D(v): an unknown result that may still commit ends the pass with its fault: {:?}",
        pass.outcome.as_ref().map(|_| ())
    );
    for &f in &marks {
        assert!(
            !pass.swept(1, f),
            "leg D(v): claimed a sweep it cannot know landed"
        );
        assert_eq!(
            pass.audit_of("sweep-unsettled", 1, f).len(),
            1,
            "leg D(v): each mark of the unsettled batch must be named once. got: {}",
            pass.log
        );
    }
    assert_eq!(pass.counted(), 0, "leg D(v): nothing is counted");
}

// ---- leg E: a differently spelled key never costs a mark ----

/// **E.** `parse_orphan_key` reads each field as a plain integer, so `orphan:1:0254:0` decodes to
/// the position `orphan:1:254:0` names — a spelling no writer produces. Two cases, and in both the
/// mark at its own key survives byte-identical, the alias is never deleted or rewritten — no commit
/// touches it — and the alias is named on the audit seam:
///
/// 1. the position is listed and the alias is old (past `D` and past grace), the mark young;
/// 2. the position is unlisted, the alias aged past `D`, the mark younger than `D`.
///
/// A sweep that took the alias for the mark of its position would delete the mark on the alias's
/// stamp, or on a listing flag kept per raw key.
#[tokio::test]
async fn e_a_differently_spelled_key_never_costs_a_mark() {
    install_global_default();
    let gc = Gc::elect().await;
    let now = T0 + D;
    for listed in [true, false] {
        let meta = Meta::default();
        let disks = disks(3);
        let fleet = fleet_of(&disks);
        let mark = frag(254, 0);
        let key = orphan_key(1, mark);
        let alias = b"orphan:1:0254:0".to_vec();
        assert_eq!(
            parse_orphan_key(&alias),
            parse_orphan_key(&key),
            "fixture: the alias must decode to the mark's own position"
        );
        assert!(alias.starts_with(ORPHAN_PREFIX) && alias != key);
        let (mark_value, alias_value) = if listed {
            disks[1].put(mark);
            (legacy(T0 + D), legacy(0))
        } else {
            (legacy(T0 + 1), legacy(T0 - 1))
        };
        meta.seed(key.clone(), mark_value.clone());
        meta.seed(alias.clone(), alias_value.clone());
        let control = seed_control(&meta, 2, 0xEE, now);

        let pass = gc.pass(&meta, &fleet, now).await;
        pass.answer("leg E");
        let case = if listed { "listed" } else { "unlisted" };
        assert_eq!(
            meta.value(&key),
            Some(mark_value),
            "leg E ({case}): the mark at its own key was deleted or rewritten on the strength of a \
             differently spelled key"
        );
        assert!(
            !pass.swept(1, mark),
            "leg E ({case}): a sweep of the mark was claimed"
        );
        assert_eq!(
            meta.value(&alias),
            Some(alias_value),
            "leg E ({case}): the differently spelled key was deleted or rewritten"
        );
        let touched: Vec<Commit> = meta
            .take_events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Commit(commit)
                    if commit.preconditions.contains(&alias) || commit.deletes.contains(&alias) =>
                {
                    Some(commit)
                }
                _ => None,
            })
            .collect();
        assert!(
            touched.is_empty(),
            "leg E ({case}): a commit named the differently spelled key: {touched:?}"
        );
        let named = format!(r#""key":"{}""#, String::from_utf8_lossy(&alias));
        assert!(
            pass.log.lines().any(|line| line.contains(AUDIT)
                && line.contains(r#""action":"malformed-orphan-key""#)
                && line.contains(&named)),
            "leg E ({case}): the differently spelled key must be named on the GC audit seam \
             ({named}). got: {}",
            pass.log
        );
        if listed {
            assert!(
                disks[1].holds(mark),
                "leg E (listed): the fragment was reclaimed"
            );
        } else {
            assert_eq!(
                pass.mark_skips(1, mark),
                ["within-late-write-deadline"],
                "leg E (unlisted): the mark must be judged on its own stamp. got: {}",
                pass.log
            );
        }
        assert_control_swept("leg E", &meta, &pass, 2, control);
    }
}
