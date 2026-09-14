//! Issue #661 (child 1 of #637): the `orphan:` ledger is **walked in pages, never read by one
//! `scan`** — by GC and by the post-restore pass alike (proposal 0016, `0016:1392-1408`).
//!
//! `scan` fails whole past its cap and returns no partial result (`MetadataStore`, clause 5), and
//! one maximum segmented-object retirement installs ~1.78 M marks against `SCAN_CAP` =
//! 1,048,576. A GC that read the ledger with one `scan` would fail every pass from the first large
//! delete on — the pass that should shrink the ledger being the one that cannot start — and the
//! post-restore command could never finish. On `main` both do exactly that: `gc.rs:177` (the
//! `scan` at `:526`) and `restore.rs:308`.
//!
//! Every leg runs the production entry points — `reconcile_step` with a `GcContext`, and
//! `reconcile_after_restore` — over in-memory doubles. The metadata double ([`LedgerMeta`])
//! enforces a **lowered** cap the way the production backends' `with_scan_cap` knob does
//! (`crates/metadata-redb/tests/scan.rs:9-19`): a `scan` whose answer would exceed [`CAP`] fails
//! with `ScanCapExceeded`, and a `scan_page` page carries at most [`CAP`] entries (unless a test
//! breaks the page contract on purpose, [`PageFault`]). Its `scan_page` is implemented directly
//! over its own ordered map with the shared page helpers — deliberately NOT
//! `wyrd_testkit::test_double_scan_page`, which pages over `scan` and so inherits the very cap the
//! walk exists to escape. The page cap sits below the window, so one pass needs several pages to
//! read its budget, as it would against a real backend.
//!
//! The double records what each pass received: every `orphan:` entry `scan_page` handed out
//! (counted across the pass's pages), every `scan` whose range reaches the `orphan:` prefix, and
//! every commit. Every pass builds a fresh `GcContext`, exactly as the deployed loop does
//! (`crates/server/src/custodian.rs:600-608`), so a walk that kept its place anywhere but the
//! store would restart each time. Each leg asserts on the pass's `Result` before anything else:
//! on `main` every leg's first pass fails on the ledger past the cap, and that is its red.
//!
//! The legs:
//!
//! * **A** — a GC pass survives a ledger past the cap and drains it.
//! * **B** — one pass reads at most [`B`] entries (exactly [`B`] while that many remain), drains
//!   `P` in exactly `⌈P / B⌉` passes, and commits its own deletes in batches of at most [`W`],
//!   `⌈n / W⌉` of them.
//! * **C** — a retention-safe head more than a window long does not starve the tail behind it,
//!   and the walk wraps back to the head.
//! * **D** — what a partial read may conclude: (i) an unread mark outranks an expired lease —
//!   ahead of the window, behind it, or exactly on the cursor the pass resumed from, the one key
//!   the window's range leaves out; (ii) a chunk's `pending:` entry outlives every fragment it
//!   still accounts for; (iii) a mark whose value is unreadable counts as a mark, is never
//!   deleted, and is named on the audit seam.
//! * **E** — only a fragment's own key, as `orphan_key` spells it, licenses a reclaim.
//! * **F** — restore survives the same ledger and never re-stamps a mark it did not read.
//! * **G** — a pass that stops short of the ledger's end and reclaims nothing answers `Partial`,
//!   never `Satisfied`; only a pass that read the ledger to its end may (PR #802 review).
//! * **H** — a persisted cursor the walk cannot resume from (longer than any key a writer
//!   spells) restarts the walk from the head and is rewritten, instead of failing every pass;
//!   the longest key a writer can spell is still honoured (PR #802 review).
//! * **The walk's guards** — a store whose `scan_page` repeats its cursor, or answers more than
//!   it was asked for, is refused with an error by both passes rather than walked.
//!
//! **The two constants are pinned by literal.** [`B`] and [`W`] are the production window and
//! cleanup batch (`gc::ORPHAN_WINDOW`, `gc::CLEANUP_BATCH`), written here as numbers rather than
//! named — this file compiles against `main`, where neither exists — so a change to either
//! production value fails this file instead of silently moving the bound it pins.
//!
//! **What leg B's commit bound proves, and what it does not.** It proves every commit of a pass's
//! own cleanup carries at most [`W`] writes: a constant, independent of the ledger and of the
//! pass. It does not prove a [`W`]-write commit finishes inside the backend's 5-second transaction
//! envelope — that depends on the deployment's round-trip time (proposal 0016's calibrated
//! `B_ops`, `0016:640-643`), which no in-memory test can measure.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, orphan_key, parse_orphan_key, PendingEntry, ORPHAN_PREFIX};
use wyrd_custodian::{
    mark_orphaned, reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy,
    FencedZone, GcContext, ReconcileError, Reconciled, RestoreReport,
};
use wyrd_traits::{
    page_cursor, page_limit, page_start, ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId,
    Health, MetadataStore, PageStart, Result, ScanCapExceeded, ScanPage, WriteBatch,
};

/// **B**, the production window (`gc::ORPHAN_WINDOW` = `SCAN_CAP / 16`): the most `orphan:`
/// entries one GC pass receives, across all of its pages.
const B: usize = 65_536;

/// **W**, the production cleanup batch (`gc::CLEANUP_BATCH`): the most writes one commit of a
/// pass's own ledger cleanup carries.
const W: usize = 1_000;

/// The double's lowered cap — on one `scan`'s answer and on one `scan_page` page, the single knob
/// the production backends apply to both. Far below every ledger seeded here, so a `scan` of the
/// ledger fails; below [`B`], and not a divisor of it, so a pass reads its window in several pages
/// and the last of them is cut to the budget.
const CAP: usize = 5_000;

/// The grace window every GC leg runs with.
const GRACE: u64 = 1_000;

/// An instant far from zero, so a stamp at it is inside the grace window and a stamp at zero is
/// long past it.
const NOW: u64 = 1_000_000;

// ---- the metadata double ----

/// What the double handed out and accepted during one pass.
#[derive(Default)]
struct Tap {
    /// `orphan:` entries handed out by `scan_page`, counted across every page of the pass.
    orphan_entries: usize,
    /// Each `orphan:` page, in order.
    orphan_pages: Vec<Page>,
    /// Every `scan` whose range reaches the `orphan:` prefix — there must be none.
    orphan_scans: Vec<Vec<u8>>,
    /// Every commit.
    commits: Vec<Commit>,
    /// The watched keys (see [`LedgerMeta::watch`]) some page handed out.
    watched_seen: HashSet<Vec<u8>>,
}

/// One `orphan:` page, as the double answered it.
struct Page {
    /// The cursor the page was asked for: it starts after this key, or at the head for `None`.
    after: Option<Vec<u8>>,
    first: Option<Vec<u8>>,
    last: Option<Vec<u8>>,
    terminal: bool,
}

/// One commit, as the double accepted it.
struct Commit {
    puts: Vec<Vec<u8>>,
    deletes: Vec<Vec<u8>>,
}

/// The key range one pass's ledger pages ran across.
#[derive(Debug)]
struct Span {
    first: Vec<u8>,
    last: Vec<u8>,
    /// The pages ended with the terminal one: the walk reached the end of the ledger.
    reached_end: bool,
}

impl Tap {
    /// Where the pass's ledger pages ran — `None` when no page carried an entry.
    fn span(&self) -> Option<Span> {
        let first = self
            .orphan_pages
            .iter()
            .find_map(|page| page.first.clone())?;
        let last = self
            .orphan_pages
            .iter()
            .rev()
            .find_map(|page| page.last.clone())?;
        Some(Span {
            first,
            last,
            reached_end: self.reached_end(),
        })
    }

    /// Whether the pass's pages ended with the terminal page — the walk reached the ledger's end.
    fn reached_end(&self) -> bool {
        self.orphan_pages.last().is_some_and(|page| page.terminal)
    }

    /// The cursor the pass's first ledger page was asked for — where the walk resumed from, or
    /// `None` when it started at the head (or read no page at all).
    fn resumed_from(&self) -> Option<&[u8]> {
        self.orphan_pages.first()?.after.as_deref()
    }
}

/// How the double's `scan_page` answers: honestly, or breaking one clause of its contract
/// (`crates/traits/src/lib.rs:1377-1408`) — the stores the walk must refuse rather than walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PageFault {
    /// Every clause kept.
    #[default]
    Honest,
    /// Clause 2 broken: the page starts AT its cursor rather than strictly after it, one key a
    /// page — so from the second page on it hands back the very key it was asked to pass.
    RepeatsCursor,
    /// The page bound broken: one entry more than the caller's `limit`.
    OverLong,
}

/// An in-memory metadata store over an ORDERED map, with a lowered cap (see the module docs).
#[derive(Default)]
struct LedgerMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    tap: Mutex<Tap>,
    watched: Mutex<HashSet<Vec<u8>>>,
    fault: PageFault,
}

/// Whether a `scan` of `prefix` reaches keys under `orphan:` — a prefix of it (`""`, `"orph"`),
/// or a narrowing of it (`"orphan:5:"`).
fn reaches_orphan(prefix: &[u8]) -> bool {
    ORPHAN_PREFIX.starts_with(prefix) || prefix.starts_with(ORPHAN_PREFIX)
}

impl LedgerMeta {
    fn seed(&self, key: Vec<u8>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key, value.into());
    }

    /// Seed `frag`'s mark on `dserver`, stamped `at` — the exact key and value
    /// [`mark_orphaned`] writes (pinned by [`assert_seeding_is_mark_orphaned`]).
    fn seed_mark(&self, dserver: DServerId, frag: FragmentId, at: u64) {
        self.seed(orphan_key(dserver, frag), at.to_string());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn orphan_len(&self) -> usize {
        self.kv
            .lock()
            .unwrap()
            .range(ORPHAN_PREFIX.to_vec()..)
            .take_while(|(key, _)| key.starts_with(ORPHAN_PREFIX))
            .count()
    }

    /// Every `orphan:` key, in order.
    fn orphan_keys(&self) -> Vec<Vec<u8>> {
        self.kv
            .lock()
            .unwrap()
            .range(ORPHAN_PREFIX.to_vec()..)
            .take_while(|(key, _)| key.starts_with(ORPHAN_PREFIX))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Record, per pass, whether a page handed out `key`.
    fn watch(&self, key: Vec<u8>) {
        self.watched.lock().unwrap().insert(key);
    }

    /// What the double saw since the last call, resetting it.
    fn take_tap(&self) -> Tap {
        std::mem::take(&mut *self.tap.lock().unwrap())
    }
}

#[async_trait]
impl MetadataStore for LedgerMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.value(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if reaches_orphan(prefix) {
            self.tap.lock().unwrap().orphan_scans.push(prefix.to_vec());
        }
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
        let honest = page_limit(limit, CAP, prefix)?;
        // A broken store breaks only the `orphan:` walk; every other listing stays honest.
        let fault = if prefix == ORPHAN_PREFIX {
            self.fault
        } else {
            PageFault::Honest
        };
        let lower = match page_start(prefix, after) {
            PageStart::Prefix => Bound::Included(prefix.to_vec()),
            PageStart::After(cursor) if fault == PageFault::RepeatsCursor => {
                Bound::Included(cursor.to_vec())
            }
            PageStart::After(cursor) => Bound::Excluded(cursor.to_vec()),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        // How many entries this page hands out: the resolved page bound, or a broken one.
        let bound = match fault {
            PageFault::Honest => honest,
            PageFault::RepeatsCursor => 1,
            PageFault::OverLong => limit.saturating_add(1),
        };
        let items: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(bound)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let next = page_cursor(&items, bound);
        if reaches_orphan(prefix) {
            let watched = self.watched.lock().unwrap();
            let mut tap = self.tap.lock().unwrap();
            tap.orphan_entries += items
                .iter()
                .filter(|(key, _)| key.starts_with(ORPHAN_PREFIX))
                .count();
            tap.orphan_pages.push(Page {
                after: after.map(<[u8]>::to_vec),
                first: items.first().map(|(key, _)| key.clone()),
                last: items.last().map(|(key, _)| key.clone()),
                terminal: next.is_none(),
            });
            for (key, _) in &items {
                if watched.contains(key) {
                    tap.watched_seen.insert(key.clone());
                }
            }
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        {
            let mut kv = self.kv.lock().unwrap();
            for pre in &batch.preconditions {
                if kv.get(&pre.key).cloned() != pre.expected {
                    return Ok(CommitOutcome::Conflict);
                }
            }
            for (key, value) in &batch.puts {
                kv.insert(key.clone(), value.clone());
            }
            for key in &batch.deletes {
                kv.remove(key);
            }
        }
        self.tap.lock().unwrap().commits.push(Commit {
            puts: batch.puts.into_iter().map(|(key, _)| key).collect(),
            deletes: batch.deletes,
        });
        Ok(CommitOutcome::Committed)
    }
}

// ---- the D-server double ----

/// One D server's fragment bytes — a deliberately dumb `ChunkStore`.
#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

impl MemDServer {
    fn put(&self, frag: FragmentId) {
        self.frags
            .lock()
            .unwrap()
            .insert(frag, Bytes::from_static(b"bytes"));
    }

    fn holds(&self, frag: FragmentId) -> bool {
        self.frags.lock().unwrap().contains_key(&frag)
    }

    fn fragment_count(&self) -> usize {
        self.frags.lock().unwrap().len()
    }
}

#[async_trait]
impl ChunkStore for MemDServer {
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

// ---- helpers ----

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// The fleet view a `GcContext` takes: `servers[i]` is D server `i`.
fn fleet_of(servers: &[MemDServer]) -> Vec<(DServerId, &dyn ChunkStore)> {
    servers
        .iter()
        .enumerate()
        .map(|(id, server)| (id as DServerId, server as &dyn ChunkStore))
        .collect()
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-gc-ledger-walk")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// An expired pending lease over `chunk`, written through the production writer.
async fn put_expired_lease(meta: &LedgerMeta, chunk: ChunkId) {
    let outcome = metadata::put_pending(
        meta,
        chunk,
        &PendingEntry {
            lease_expiry_millis: 1,
            owner: None,
            staged: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

/// The fixture's seeded marks are the production writer's: one [`mark_orphaned`] call and one
/// [`LedgerMeta::seed_mark`] leave byte-identical stores.
async fn assert_seeding_is_mark_orphaned() {
    let written = LedgerMeta::default();
    mark_orphaned(&written, 3, frag(77, 2), 4_242)
        .await
        .unwrap();
    let seeded = LedgerMeta::default();
    seeded.seed_mark(3, frag(77, 2), 4_242);
    assert_eq!(
        *written.kv.lock().unwrap(),
        *seeded.kv.lock().unwrap(),
        "fixture: a seeded mark must be exactly what `mark_orphaned` writes"
    );
}

/// The walk's driver: one custodian, and one GC pass at a time the way the deployed loop runs one.
struct Walk {
    zone: FencedZone,
    custodian: Custodian,
    policy: ExpiredPendingPolicy,
    passes: usize,
    /// What the last pass answered.
    outcome: Option<Reconciled>,
}

impl Walk {
    async fn new(coord: &MemCoordination, policy: ExpiredPendingPolicy) -> Self {
        let (zone, custodian) = elect(coord).await;
        Self {
            zone,
            custodian,
            policy,
            passes: 0,
            outcome: None,
        }
    }

    /// One GC pass at `now` with a FRESH `GcContext` built for it alone
    /// (`crates/server/src/custodian.rs:600-608`), through the fenced `reconcile_step`. Asserts
    /// on the pass's `Result` before anything else, then on the bounds every pass is held to, and
    /// returns what the double saw.
    async fn pass(
        &mut self,
        leg: &str,
        meta: &LedgerMeta,
        fleet: &[(DServerId, &dyn ChunkStore)],
        now: u64,
    ) -> Tap {
        self.passes += 1;
        meta.take_tap();
        let ctx = GcContext {
            meta,
            fleet,
            grace_window_millis: GRACE,
            expired_pending: self.policy,
        };
        let outcome: std::result::Result<Reconciled, ReconcileError> = reconcile_step(
            &self.zone,
            &self.custodian,
            Some(&ctx),
            None,
            None,
            None,
            now,
        )
        .await;
        match outcome {
            Err(err) => panic!(
                "{leg}: GC pass {} failed: {err}. On main this is the single `scan` of the \
                 `orphan:` ledger past the cap (`gc.rs:177`, the scan at `:526`) — a ledger this \
                 size fails every pass, and the pass that should shrink it can never start",
                self.passes
            ),
            Ok(outcome) => self.outcome = Some(outcome),
        }
        let tap = meta.take_tap();
        assert_bounded(leg, self.passes, &tap);
        tap
    }
}

/// The bounds every pass is held to, in every leg: the step never `scan`s the `orphan:` prefix
/// (`0016:1398-1399` — the ledger is "never read by a single scan"), a pass receives at most [`B`]
/// ledger entries across all of its pages, no commit carries more than [`W`] writes, and the
/// commits carrying its `n` key deletes number exactly `⌈n / W⌉` — so a batch flushed late, or
/// never, or one sized by the pass, fails here.
fn assert_bounded(leg: &str, pass: usize, tap: &Tap) {
    assert!(
        tap.orphan_scans.is_empty(),
        "{leg}, pass {pass}: the step ran `scan` over {:?}, which reaches the `orphan:` ledger — \
         a ledger past the cap fails that read whole",
        tap.orphan_scans
            .iter()
            .map(|prefix| String::from_utf8_lossy(prefix).into_owned())
            .collect::<Vec<_>>()
    );
    assert!(
        tap.orphan_entries <= B,
        "{leg}, pass {pass}: the pass received {} `orphan:` entries, over its window of {B}",
        tap.orphan_entries
    );
    for (i, commit) in tap.commits.iter().enumerate() {
        let writes = commit.puts.len() + commit.deletes.len();
        assert!(
            writes <= W,
            "{leg}, pass {pass}: commit {i} carried {writes} writes, over the cleanup batch of {W}"
        );
    }
    let deletes: usize = tap.commits.iter().map(|commit| commit.deletes.len()).sum();
    let carrying = tap
        .commits
        .iter()
        .filter(|commit| !commit.deletes.is_empty())
        .count();
    assert_eq!(
        carrying,
        deletes.div_ceil(W),
        "{leg}, pass {pass}: {deletes} key deletes rode {carrying} commits; batches of {W} make \
         that exactly {}",
        deletes.div_ceil(W)
    );
}

/// Where `key` sat relative to one pass's window: read by it, ahead of it, behind it — or the pass
/// read no entry at all.
#[derive(Debug, PartialEq, Eq)]
enum Relative {
    Read,
    Ahead,
    Behind,
    NothingRead,
}

fn relative(tap: &Tap, key: &[u8]) -> Relative {
    if tap.watched_seen.contains(key) {
        return Relative::Read;
    }
    match tap.span() {
        None => Relative::NothingRead,
        Some(span) if !span.reached_end && key > span.last.as_slice() => Relative::Ahead,
        Some(span) if key < span.first.as_slice() => Relative::Behind,
        // The pass's pages ran across the key without handing it out: a page skipped a key the
        // store holds, which no correct walk over this double can do.
        Some(span) => panic!("a pass's pages spanned {key:?} without handing it out: {span:?}"),
    }
}

// ---- leg A: a GC pass survives a ledger past the cap ----

/// A ledger larger than the lowered cap, every mark actionable — fragment present, unreferenced,
/// past its grace window. Every pass returns `Ok`, and passes run until the ledger is empty
/// reclaim every fragment and remove every consumed key. On `main` the first pass returns
/// `Err(ReconcileError::Store)` from the single `scan` (`gc.rs:177`): the red.
#[tokio::test]
async fn a_gc_pass_survives_an_orphan_ledger_past_the_scan_cap() {
    assert_seeding_is_mark_orphaned().await;
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..4).map(|_| MemDServer::default()).collect();
    let population = 3 * CAP + 1;
    for i in 0..population {
        let dserver = (i % servers.len()) as DServerId;
        let frag = frag(1_000_000 + i as ChunkId, 0);
        servers[dserver as usize].put(frag);
        meta.seed_mark(dserver, frag, 0);
    }
    assert!(
        meta.orphan_len() > CAP,
        "fixture: the ledger must be past the cap"
    );

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Defer).await;
    let fleet = fleet_of(&servers);
    while meta.orphan_len() > 0 {
        assert!(
            walk.passes < population.div_ceil(B) + 1,
            "leg A: the ledger is not drained after {} passes",
            walk.passes
        );
        walk.pass("leg A", &meta, &fleet, NOW).await;
    }
    for (id, server) in servers.iter().enumerate() {
        assert_eq!(
            server.fragment_count(),
            0,
            "leg A: every actionable fragment is reclaimed — D server {id} still holds some"
        );
    }
}

// ---- leg B: one pass reads and writes a bounded, pinned amount ----

/// A population `P > B`, every mark actionable. No pass receives more than [`B`] entries, and
/// while at least [`B`] remain a pass receives exactly [`B`] — so a budget that silently under-reads
/// fails. `P` drains in exactly `⌈P / B⌉` passes, not fewer. Each pass consumes every mark it
/// received, and commits those deletes in `⌈n / W⌉` commits of at most [`W`] (`assert_bounded`).
#[tokio::test]
async fn b_one_pass_reads_and_writes_a_bounded_pinned_amount() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..4).map(|_| MemDServer::default()).collect();
    let population = 2 * B + B / 2 + 123;
    for i in 0..population {
        let dserver = (i % servers.len()) as DServerId;
        let frag = frag(1_000_000 + i as ChunkId, 0);
        servers[dserver as usize].put(frag);
        meta.seed_mark(dserver, frag, 0);
    }

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Defer).await;
    let fleet = fleet_of(&servers);
    let drained_in = population.div_ceil(B);
    while meta.orphan_len() > 0 {
        assert!(
            walk.passes < drained_in,
            "leg B: {population} entries are not drained after {} passes; windows of {B} drain \
             them in exactly {drained_in}",
            walk.passes
        );
        let before = meta.orphan_len();
        let tap = walk.pass("leg B", &meta, &fleet, NOW).await;
        assert_eq!(
            tap.orphan_entries,
            before.min(B),
            "leg B, pass {}: {before} entries remained, so the pass must receive exactly {} of \
             them — a window that under-reads leaves the ledger to grow",
            walk.passes,
            before.min(B)
        );
        let deletes: usize = tap.commits.iter().map(|commit| commit.deletes.len()).sum();
        assert_eq!(
            deletes, tap.orphan_entries,
            "leg B, pass {}: every mark the pass received was actionable, so each is consumed",
            walk.passes
        );
    }
    assert_eq!(
        walk.passes, drained_in,
        "leg B: {population} entries drain in exactly ⌈P / B⌉ = {drained_in} passes"
    );
    for (id, server) in servers.iter().enumerate() {
        assert_eq!(
            server.fragment_count(),
            0,
            "leg B: D server {id} still holds fragments"
        );
    }
}

// ---- leg C: the tail does not starve, and the walk wraps ----

/// A retention-safe **head** — more than [`B`] marks still inside their grace window, first in key
/// order — and an actionable **tail** behind it, together past the cap. Every tail fragment is
/// reclaimed within `⌈(head + tail) / B⌉ + 1` passes and no head fragment or head mark is touched;
/// then, with the head aged past grace, every head fragment is reclaimed within as many further
/// passes. A walk that restarted at the first key every pass never reaches the tail; one that
/// stopped at the end never reaches the head again.
#[tokio::test]
async fn c_the_tail_does_not_starve_and_the_walk_wraps() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..3).map(|_| MemDServer::default()).collect();
    let head = B + B / 4 + 11;
    let tail = B / 2 + 7;
    // Head on D server 1, tail on D server 2: every `orphan:1:` key sorts before every
    // `orphan:2:` key, so the head is first in the ledger.
    let head_frags: Vec<FragmentId> = (0..head)
        .map(|i| frag(2_000_000 + i as ChunkId, 0))
        .collect();
    let tail_frags: Vec<FragmentId> = (0..tail)
        .map(|i| frag(3_000_000 + i as ChunkId, 0))
        .collect();
    for &f in &head_frags {
        servers[1].put(f);
        meta.seed_mark(1, f, NOW);
    }
    for &f in &tail_frags {
        servers[2].put(f);
        meta.seed_mark(2, f, 0);
    }
    let stamp = Bytes::from(NOW.to_string());
    let bound = (head + tail).div_ceil(B) + 1;

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Defer).await;
    let fleet = fleet_of(&servers);
    let mut passes = 0;
    while servers[2].fragment_count() > 0 {
        assert!(
            passes < bound,
            "leg C: {} tail fragments are still on disk after {passes} passes — the retention-safe \
             head starved the tail behind it (bound ⌈(head + tail) / B⌉ + 1 = {bound})",
            servers[2].fragment_count()
        );
        walk.pass("leg C", &meta, &fleet, NOW).await;
        passes += 1;
        assert_eq!(
            servers[1].fragment_count(),
            head,
            "leg C: a head fragment inside its grace window was reclaimed"
        );
        for &f in &head_frags {
            assert_eq!(
                meta.value(&orphan_key(1, f)).as_ref(),
                Some(&stamp),
                "leg C: the head mark for {f:?} was deleted or rewritten inside its grace window"
            );
        }
    }

    // Age the head past its grace window: the walk has to come back round to the start.
    let later = NOW + GRACE;
    let mut passes = 0;
    while servers[1].fragment_count() > 0 {
        assert!(
            passes < bound,
            "leg C: {} head fragments are still on disk {passes} passes after the head aged past \
             grace — the walk never returned to the start of the ledger (bound {bound})",
            servers[1].fragment_count()
        );
        walk.pass("leg C", &meta, &fleet, later).await;
        passes += 1;
    }
    assert_eq!(
        meta.orphan_len(),
        0,
        "leg C: every consumed mark is removed"
    );
}

// ---- leg G: a partial walk is not a certification ----

/// A ledger longer than one window whose first window holds nothing reclaimable (marks inside
/// their grace) while the window behind it holds expired marks. The first pass reclaims nothing
/// and must NOT answer `Satisfied` — a caller driving reconciliation to satisfaction would stop
/// with the tail's garbage unvisited. Only once the ledger has been read to its end does a pass
/// that reclaims nothing answer `Satisfied`. Negation: return `Satisfied` for an unchanged
/// partial window — the first assertion fails.
#[tokio::test]
async fn g_a_partial_walk_that_reclaims_nothing_is_not_satisfied() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..3).map(|_| MemDServer::default()).collect();
    let head = B + 10;
    let tail = 5;
    let head_frags: Vec<FragmentId> = (0..head)
        .map(|i| frag(2_000_000 + i as ChunkId, 0))
        .collect();
    let tail_frags: Vec<FragmentId> = (0..tail)
        .map(|i| frag(3_000_000 + i as ChunkId, 0))
        .collect();
    for &f in &head_frags {
        servers[1].put(f);
        meta.seed_mark(1, f, NOW); // inside grace at NOW: kept
    }
    for &f in &tail_frags {
        servers[2].put(f);
        meta.seed_mark(2, f, 0); // long expired: reclaimable
    }

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Defer).await;
    let fleet = fleet_of(&servers);

    let tap = walk.pass("leg G", &meta, &fleet, NOW).await;
    assert!(
        !tap.reached_end(),
        "fixture: the first window must stop short of the tail"
    );
    assert_eq!(
        servers[2].fragment_count(),
        tail,
        "fixture: the tail is beyond the window"
    );
    assert_eq!(
        walk.outcome,
        Some(Reconciled::Partial),
        "leg G: a pass that read one window of a longer ledger and reclaimed nothing certified \
         the store as converged with {tail} expired marks unvisited behind its window"
    );

    // Drive to the end: every pass before the ledger is empty is Changed or Partial, never
    // Satisfied, and the tail is reclaimed on the way.
    let later = NOW + GRACE;
    let bound = (head + tail).div_ceil(B) * 2 + 2;
    while meta.orphan_len() > 0 {
        assert!(
            walk.passes < bound,
            "leg G: ledger not drained after {} passes",
            walk.passes
        );
        walk.pass("leg G", &meta, &fleet, later).await;
        assert_ne!(
            walk.outcome,
            Some(Reconciled::Satisfied),
            "leg G: Satisfied answered while {} marks remained",
            meta.orphan_len()
        );
    }
    assert_eq!(
        servers[2].fragment_count(),
        0,
        "leg G: the tail was never reclaimed"
    );
    let tap = walk.pass("leg G", &meta, &fleet, later).await;
    assert!(
        tap.reached_end(),
        "leg G: an empty ledger is read to its end"
    );
    assert_eq!(
        walk.outcome,
        Some(Reconciled::Satisfied),
        "leg G: a pass that read the whole (empty) ledger and reclaimed nothing is Satisfied"
    );
}

// ---- leg H: a cursor the walk cannot resume from restarts it, not the failure ----

/// The cursor record is a metadata VALUE and may hold anything a restore or a fault left there,
/// while `scan_page` takes it as a KEY the backend bounds. A value under the prefix but longer
/// than any key a writer spells is not handed to the backend: the pass names it, restarts from
/// the head, and rewrites the record — so the next pass does not meet it again. The longest key a
/// writer CAN spell is still honoured as a cursor. Negation: drop the length bound — the
/// "rewritten" assertion fails (and against a real backend the pass would).
#[tokio::test]
async fn h_an_unusable_cursor_restarts_the_walk_from_the_head_and_is_rewritten() {
    const CURSOR: &[u8] = b"gc:orphan-cursor";
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..2).map(|_| MemDServer::default()).collect();
    let expired = frag(4_000_001, 0);
    servers[1].put(expired);
    meta.seed_mark(1, expired, 0);
    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Defer).await;
    let fleet = fleet_of(&servers);

    // The longest key any writer can spell: a legitimate resume point, honoured as one.
    let longest = orphan_key(
        u64::MAX,
        FragmentId {
            chunk: u128::MAX,
            index: u16::MAX,
        },
    );
    meta.seed(CURSOR.to_vec(), longest.clone());
    let tap = walk.pass("leg H", &meta, &fleet, NOW).await;
    assert_eq!(
        tap.resumed_from(),
        Some(longest.as_slice()),
        "leg H: the longest spellable key must still be a usable cursor"
    );
    // (Byte order puts `orphan:18446744073709551615:…` BEFORE `orphan:1:…`, so this pass read
    // the real mark too and reclaimed it; each case below re-seeds it.)

    // One byte past the longest spellable key: unusable. Oversized the same way, only more so.
    for (label, bad) in [
        ("one byte over", [longest.as_slice(), b"0"].concat()),
        (
            "oversized",
            [ORPHAN_PREFIX, vec![b'x'; 100_000].as_slice()].concat(),
        ),
    ] {
        meta.seed(CURSOR.to_vec(), bad.clone());
        servers[1].put(expired);
        meta.seed_mark(1, expired, 0);
        let tap = walk.pass("leg H", &meta, &fleet, NOW).await;
        assert_eq!(
            tap.resumed_from(),
            None,
            "leg H ({label}): an unusable cursor must restart the walk from the head"
        );
        assert_eq!(
            servers[1].fragment_count(),
            0,
            "leg H ({label}): the expired mark at the head was not reclaimed"
        );
        assert_ne!(
            meta.value(CURSOR).as_deref(),
            Some(bad.as_slice()),
            "leg H ({label}): the unusable cursor was left in place for the next pass to fail on"
        );
    }
}

// ---- leg D: what a pass may conclude from a partial read ----

/// The fragment legs D(i) and D(iii) keep: chunk 7, index 0, on D server 2 — its mark sorts after
/// every `orphan:1:` filler and before every `orphan:3:` one.
const KEPT: FragmentId = FragmentId { chunk: 7, index: 0 };

/// Fill the ledger with `before` marks on D server 1 and `after` marks on D server 3 — marks with
/// no fragment on disk, which no pass reclaims and none consumes, so the ledger stays several
/// windows long for as many passes as a leg runs.
fn seed_filler(meta: &LedgerMeta, before: usize, after: usize) {
    for i in 0..before {
        meta.seed_mark(1, frag(5_000_000 + i as ChunkId, 0), 0);
    }
    for i in 0..after {
        meta.seed_mark(3, frag(6_000_000 + i as ChunkId, 0), 0);
    }
}

/// **D(i) — an unread mark outranks an expired lease.** Under `ExpiredPendingPolicy::Reclaim`, a
/// fragment whose chunk carries an EXPIRED `pending:` lease and whose own mark is still inside its
/// grace window survives every pass until that mark's grace elapses. The ledger is four windows
/// long, so in most passes the mark sits outside the window being read — ahead of it in some and
/// behind it in others. A pass that reclaimed on the lease because it had not read the mark would
/// tear a reader the mark's grace window protects (round 1's patch did; three blocking findings).
#[tokio::test]
async fn d1_an_unread_mark_outranks_an_expired_lease() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..4).map(|_| MemDServer::default()).collect();
    seed_filler(&meta, B + B / 2, 2 * B);
    servers[2].put(KEPT);
    meta.seed_mark(2, KEPT, NOW);
    put_expired_lease(&meta, KEPT.chunk).await;
    let mark = orphan_key(2, KEPT);
    meta.watch(mark.clone());
    let lap = meta.orphan_len().div_ceil(B);
    assert!(lap >= 3, "fixture: the ledger must be several windows long");

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Reclaim).await;
    let fleet = fleet_of(&servers);
    let stamp = Bytes::from(NOW.to_string());
    let mut seen = Vec::new();
    // Two full laps and one more pass, all inside the mark's grace window.
    for _ in 0..2 * lap + 1 {
        let tap = walk.pass("leg D(i)", &meta, &fleet, NOW).await;
        assert!(
            servers[2].holds(KEPT),
            "leg D(i), pass {}: the fragment was reclaimed on its expired lease while its own mark \
             ({:?} relative to the pass's window) is inside its grace window",
            walk.passes,
            relative(&tap, &mark)
        );
        assert_eq!(
            meta.value(&mark).as_ref(),
            Some(&stamp),
            "leg D(i): the mark was deleted or rewritten inside its grace window"
        );
        assert!(
            meta.value(&metadata::pending_key(KEPT.chunk)).is_some(),
            "leg D(i): the lease's entry was retired while the fragment it names is on disk"
        );
        seen.push(relative(&tap, &mark));
    }
    assert!(
        seen.contains(&Relative::Ahead) && seen.contains(&Relative::Behind),
        "fixture: the mark must sit ahead of the window in some passes and behind it in others: \
         {seen:?}"
    );

    // The mark's own grace elapses: now, and only now, its fragment is reclaimed.
    let later = NOW + GRACE;
    let mut passes = 0;
    while servers[2].holds(KEPT) {
        assert!(
            passes <= lap,
            "leg D(i): the fragment outlived its mark's grace window by {passes} passes"
        );
        walk.pass("leg D(i)", &meta, &fleet, later).await;
        passes += 1;
    }
    assert!(
        meta.value(&mark).is_none(),
        "leg D(i): the consumed mark is removed with its fragment"
    );
    assert!(
        meta.value(&metadata::pending_key(KEPT.chunk)).is_none(),
        "leg D(i): with no fragment of the chunk left, the expired entry is retired"
    );
}

/// **D(i) on the window's lower bound — a mark that IS the cursor the pass resumed from.** A
/// window's key range is `(cursor, last key read]`, and the cursor is the last key the PREVIOUS
/// pass read: of every key in the ledger, the one at the cursor is exactly the one the range
/// leaves out. So the fragment's own mark — inside its grace window, over an expired lease, under
/// `ExpiredPendingPolicy::Reclaim` — is seeded as the ledger's `B`-th key: each lap's first pass
/// ends its window on it, and the next pass resumes from it without reading it. That pass has to
/// treat the mark as unknown, not absent. A coverage test that let the bound in (`>=` for `>`)
/// would find the fragment "covered and unmarked" and reclaim it on the lease, inside its mark's
/// grace window.
#[tokio::test]
async fn d1_a_mark_on_the_resume_cursor_outranks_an_expired_lease() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..4).map(|_| MemDServer::default()).collect();
    // `B - 1` filler keys before the mark and half a window after it: the mark is the last key of
    // the first window, and the ledger, past the cap, is two windows long.
    seed_filler(&meta, B - 1, B / 2);
    servers[2].put(KEPT);
    meta.seed_mark(2, KEPT, NOW);
    put_expired_lease(&meta, KEPT.chunk).await;
    let mark = orphan_key(2, KEPT);
    meta.watch(mark.clone());
    assert_eq!(
        meta.orphan_keys().iter().position(|key| *key == mark),
        Some(B - 1),
        "fixture: the mark must be the ledger's B-th key, the last one of the first window"
    );
    assert!(
        meta.orphan_len() > CAP,
        "fixture: the ledger must be past the cap"
    );

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Reclaim).await;
    let fleet = fleet_of(&servers);
    let stamp = Bytes::from(NOW.to_string());
    let (mut ended_on, mut resumed_from) = (0, 0);
    // Two laps of two passes each, all inside the mark's grace window.
    for _ in 0..4 {
        let tap = walk.pass("leg D(i), cursor", &meta, &fleet, NOW).await;
        assert!(
            servers[2].holds(KEPT),
            "leg D(i), cursor, pass {}: the fragment was reclaimed on its expired lease while its \
             own mark — the cursor this pass resumed from, which it did not read — is inside its \
             grace window",
            walk.passes
        );
        assert_eq!(
            meta.value(&mark).as_ref(),
            Some(&stamp),
            "leg D(i), cursor: the mark was deleted or rewritten inside its grace window"
        );
        assert!(
            meta.value(&metadata::pending_key(KEPT.chunk)).is_some(),
            "leg D(i), cursor: the lease's entry was retired while the fragment it names is on disk"
        );
        ended_on += usize::from(
            tap.span()
                .is_some_and(|span| span.last == mark && !span.reached_end),
        );
        resumed_from += usize::from(
            tap.resumed_from() == Some(mark.as_slice()) && !tap.watched_seen.contains(&mark),
        );
    }
    assert!(
        ended_on >= 2 && resumed_from >= 2,
        "fixture: in each lap one window must end on the mark and the next pass resume from it \
         without reading it (a window ended on it {ended_on} times, a pass resumed from it \
         {resumed_from} times)"
    );

    // Its grace elapses: the next pass to read the mark reclaims the fragment, consumes the mark
    // and retires the entry — within one lap.
    let later = NOW + GRACE;
    let mut passes = 0;
    while servers[2].holds(KEPT) {
        assert!(
            passes < 2,
            "leg D(i), cursor: the fragment outlived its mark's grace window by {passes} passes"
        );
        walk.pass("leg D(i), cursor", &meta, &fleet, later).await;
        passes += 1;
    }
    assert!(
        meta.value(&mark).is_none(),
        "leg D(i), cursor: the consumed mark is removed with its fragment"
    );
    assert!(
        meta.value(&metadata::pending_key(KEPT.chunk)).is_none(),
        "leg D(i), cursor: with no fragment of the chunk left, the expired entry is retired"
    );
}

/// **D(ii) — a chunk's `pending:` entry outlives every fragment it accounts for.** A chunk under an
/// expired lease has UNMARKED fragments on four D servers whose key positions fall in four
/// different windows, in a ledger past the cap of marks that do not touch it. Between any two
/// passes, every fragment of the chunk still on disk has its `pending:` entry or an `orphan:` mark;
/// the entry is deleted only once no fragment remains for it to account for; and every fragment
/// is reclaimed within `⌈P / B⌉ + 1` passes. A pass that retired the entry chunk-wide after
/// sweeping only its own window's fragments would leave the rest evidence-free, kept forever.
#[tokio::test]
async fn d2_an_expired_leases_entry_outlives_every_fragment_it_accounts_for() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..10).map(|_| MemDServer::default()).collect();
    let block = B * 4 / 5;
    // Filler on the odd D servers 1, 3, 5, 7, 9, a block each; the chunk's fragments on the even
    // ones between them, so each fragment's position lands a block further into the ledger.
    for (n, dserver) in [1, 3, 5, 7, 9].into_iter().enumerate() {
        for i in 0..block {
            meta.seed_mark(dserver, frag(7_000_000 + (n * block + i) as ChunkId, 0), 0);
        }
    }
    let chunk: ChunkId = 9;
    let leased: Vec<(DServerId, FragmentId)> = [2, 4, 6, 8]
        .into_iter()
        .enumerate()
        .map(|(index, dserver)| (dserver, frag(chunk, index as u16)))
        .collect();
    for &(dserver, f) in &leased {
        servers[dserver as usize].put(f);
    }
    put_expired_lease(&meta, chunk).await;
    let population = meta.orphan_len();
    assert!(population > CAP, "fixture: the ledger must be past the cap");
    let bound = population.div_ceil(B) + 1;

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Reclaim).await;
    let fleet = fleet_of(&servers);
    let mut spans = Vec::new();
    while leased.iter().any(|&(d, f)| servers[d as usize].holds(f)) {
        assert!(
            walk.passes < bound,
            "leg D(ii): fragments of the leased chunk are still on disk after {} passes (bound \
             ⌈P / B⌉ + 1 = {bound})",
            walk.passes
        );
        let tap = walk.pass("leg D(ii)", &meta, &fleet, NOW).await;
        spans.extend(tap.span());
        let entry = meta.value(&metadata::pending_key(chunk));
        for &(dserver, f) in &leased {
            if servers[dserver as usize].holds(f) {
                assert!(
                    entry.is_some() || meta.value(&orphan_key(dserver, f)).is_some(),
                    "leg D(ii), pass {}: {f:?} on D server {dserver} is on disk with neither its \
                     `pending:` entry nor an `orphan:` mark — evidence-free bytes GC keeps forever",
                    walk.passes
                );
            }
        }
    }
    assert!(
        meta.value(&metadata::pending_key(chunk)).is_none(),
        "leg D(ii): with every fragment reclaimed, nothing is left for the entry to account for"
    );
    // The fixture really spread the positions across windows: no single pass's pages spanned them.
    let window_holding = |key: &Vec<u8>| {
        spans
            .iter()
            .position(|span: &Span| *key > span.first && (span.reached_end || *key < span.last))
    };
    let distinct: HashSet<usize> = leased
        .iter()
        .filter_map(|&(dserver, f)| window_holding(&orphan_key(dserver, f)))
        .collect();
    assert!(
        distinct.len() >= 2,
        "fixture: the leased fragments' positions must fall in different windows: {spans:?}"
    );
}

/// Collects what a `tracing` subscriber writes, so leg D(iii) reads back the audit lines the pass
/// actually emitted (`crates/custodian/tests/gc.rs:895-918`).
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

/// **D(iii) — an unreadable mark counts as a mark.** Set up as D(i), but the fragment's own mark
/// holds a value that is not the decimal instant `mark_orphaned` writes. The fragment survives
/// every pass through two full laps of the walk, the mark's key and value are never deleted or
/// changed, and the mark is named on the GC audit seam, carrying its key. Treating the value as
/// no mark at all hands the fragment to the expired-lease arm and deletes the mark with it — the
/// last evidence a human has to repair.
///
/// The only leg in this binary that seeds an unreadable mark for GC to read, so no sibling fires
/// that audit callsite; the #214 global-default guard is installed before its first pass
/// (`crates/custodian/tests/gc.rs:1143-1146`).
#[tokio::test]
async fn d3_an_unreadable_mark_counts_as_a_mark() {
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..4).map(|_| MemDServer::default()).collect();
    seed_filler(&meta, B + B / 2, 2 * B);
    servers[2].put(KEPT);
    let mark = orphan_key(2, KEPT);
    let unreadable = Bytes::from_static(b"not an instant");
    meta.seed(mark.clone(), unreadable.clone());
    put_expired_lease(&meta, KEPT.chunk).await;
    meta.watch(mark.clone());
    let lap = meta.orphan_len().div_ceil(B);

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Reclaim).await;
    let fleet = fleet_of(&servers);
    let audit = Capture::default();
    let (mut read_it, mut laps_ended) = (0, 0);
    // Long after the lease expired and long after any grace window: nothing but the mark itself
    // stands between the fragment and a reclaim.
    let now = NOW + 10 * GRACE;
    for _ in 0..2 * lap + 1 {
        let tap = walk
            .pass("leg D(iii)", &meta, &fleet, now)
            .with_subscriber(tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(audit.clone()),
                ),
            ))
            .await;
        assert!(
            servers[2].holds(KEPT),
            "leg D(iii), pass {}: the fragment was reclaimed although its own mark exists — a \
             value the pass cannot read is still a mark",
            walk.passes
        );
        assert_eq!(
            meta.value(&mark).as_ref(),
            Some(&unreadable),
            "leg D(iii), pass {}: the unreadable mark was deleted or rewritten",
            walk.passes
        );
        read_it += usize::from(tap.watched_seen.contains(&mark));
        laps_ended += usize::from(tap.reached_end());
    }
    assert!(
        read_it >= 2 && laps_ended >= 2,
        "fixture: the run must cover two full laps, reading the mark in each (read in {read_it} \
         passes, {laps_ended} laps ended)"
    );

    let logged = String::from_utf8(audit.0.lock().unwrap().clone()).unwrap();
    let named = format!(r#""mark":"{}""#, String::from_utf8_lossy(&mark));
    assert!(
        logged
            .lines()
            .any(|line| line.contains(r#""action":"unreadable-orphan-mark""#)
                && line.contains(r#""target":"wyrd.custodian.gc.audit""#)
                && line.contains(&named)),
        "leg D(iii): the unreadable mark must be named on the GC audit seam, carrying its key \
         ({named}). got: {logged}"
    );
}

// ---- leg E: only a fragment's own key licenses a reclaim ----

/// `parse_orphan_key` reads each field as a plain integer (`crates/core/src/metadata.rs:78-85`),
/// so `orphan:5:01:0` decodes to the position `orphan:5:1:0` names — and no writer spells it
/// (every writer goes through `orphan_key`). The fragment's own mark is inside its grace window;
/// the other spelling carries an old stamp, more than [`B`] keys earlier in the ledger. The fragment
/// survives until its own mark's grace elapses, and the other spelling is never deleted,
/// rewritten or acted on. A pass that judged the fragment on the early window alone would reclaim
/// it on a key no writer wrote.
#[tokio::test]
async fn e_only_a_fragments_own_key_licenses_a_reclaim() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let own = frag(1, 0);
    let own_key = orphan_key(5, own);
    let other_key = b"orphan:5:01:0".to_vec();
    assert_eq!(
        parse_orphan_key(&other_key),
        Some((5, own)),
        "fixture: the other spelling decodes to the fragment's own position"
    );
    servers[5].put(own);
    meta.seed_mark(5, own, NOW);
    meta.seed(other_key.clone(), "0");
    // Chunk ids 100000.. spell `orphan:5:1<digits>:0`, which sorts after `orphan:5:01:0` and
    // before `orphan:5:1:0`: more than a window of keys between the two spellings.
    for i in 0..B + 500 {
        meta.seed_mark(5, frag(100_000 + i as ChunkId, 0), 0);
    }
    let keys = meta.orphan_keys();
    let between = keys
        .iter()
        .filter(|key| **key > other_key && **key < own_key)
        .count();
    assert!(
        keys.first() == Some(&other_key) && between > B,
        "fixture: the other spelling must open the ledger, more than a window before the mark"
    );
    let lap = keys.len().div_ceil(B);

    let coord = MemCoordination::new();
    let mut walk = Walk::new(&coord, ExpiredPendingPolicy::Reclaim).await;
    let fleet = fleet_of(&servers);
    let stamp = Bytes::from(NOW.to_string());
    let old = Bytes::from_static(b"0");
    let mut touched = Vec::new();
    for _ in 0..2 * lap + 1 {
        let tap = walk.pass("leg E", &meta, &fleet, NOW).await;
        assert!(
            servers[5].holds(own),
            "leg E, pass {}: the fragment was reclaimed inside its own mark's grace window — on a \
             key no writer spells",
            walk.passes
        );
        assert_eq!(
            meta.value(&own_key).as_ref(),
            Some(&stamp),
            "leg E: own mark changed"
        );
        touched.extend(tap.commits);
    }

    let later = NOW + GRACE;
    let mut passes = 0;
    while servers[5].holds(own) {
        assert!(
            passes <= lap,
            "leg E: the fragment outlived its own mark's grace window by {passes} passes"
        );
        let tap = walk.pass("leg E", &meta, &fleet, later).await;
        touched.extend(tap.commits);
        passes += 1;
    }
    assert!(
        meta.value(&own_key).is_none(),
        "leg E: the consumed own mark is removed"
    );
    assert_eq!(
        meta.value(&other_key).as_ref(),
        Some(&old),
        "leg E: the other spelling must be left exactly as it was"
    );
    assert!(
        touched
            .iter()
            .all(|commit| !commit.puts.contains(&other_key) && !commit.deletes.contains(&other_key)),
        "leg E: no commit may write or delete the other spelling"
    );
}

// ---- leg F: restore survives the same ledger, and never re-stamps a mark it did not read ----

/// `reconcile_after_restore` over a ledger past the cap returns `Ok` (on `main`, `restore.rs:308`
/// returns `Err`: the red). It writes a fresh stamp for any stranded fragment it did not find
/// marked, so a judgement drawn from part of the ledger would silently restart the grace clock of
/// every mark beyond it. Pre-marked stranded fragments sort after both the first [`B`] keys and
/// the first [`CAP`] keys, one of them the ledger's last key, plus one whose mark is unreadable:
/// each keeps its value bytes and is counted `already_marked`. A genuine stray with no mark IS
/// marked, so the leg cannot pass on a pass that did nothing.
#[tokio::test]
async fn f_restore_survives_the_ledger_and_never_restamps_a_mark_it_did_not_read() {
    let meta = LedgerMeta::default();
    let servers: Vec<MemDServer> = (0..10).map(|_| MemDServer::default()).collect();
    for i in 0..B + CAP + 1_000 {
        meta.seed_mark(1, frag(8_000_000 + i as ChunkId, 0), 0);
    }
    let pass_clock = NOW;
    let stamped_before = pass_clock - 100;
    // On D server 9, so every one of their keys sorts after every filler key.
    let premarked = [frag(12, 0), frag(13, 0), frag(19, 0)];
    let unreadable = frag(14, 0);
    let stray = frag(15, 0);
    for f in premarked {
        servers[9].put(f);
        meta.seed_mark(9, f, stamped_before);
    }
    servers[9].put(unreadable);
    let garbage = Bytes::from_static(b"not an instant");
    meta.seed(orphan_key(9, unreadable), garbage.clone());
    servers[9].put(stray);
    let keys = meta.orphan_keys();
    assert!(
        keys.last() == Some(&orphan_key(9, frag(19, 0)))
            && keys
                .iter()
                .position(|key| *key == orphan_key(9, premarked[0]))
                > Some(B.max(CAP)),
        "fixture: the pre-marked marks must sort past the first window and the first cap's worth, \
         one of them last"
    );

    let fleet = fleet_of(&servers);
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    meta.take_tap();
    let report: RestoreReport = match reconcile_after_restore(&ctx, pass_clock).await {
        Ok(report) => report,
        Err(err) => panic!(
            "leg F: the post-restore pass failed: {err}. On main this is its single `scan` of \
             the `orphan:` ledger past the cap (`restore.rs:308`)"
        ),
    };
    let tap = meta.take_tap();
    assert!(
        tap.orphan_scans.is_empty(),
        "leg F: the pass ran `scan` over the `orphan:` ledger"
    );
    let stamp = Bytes::from(stamped_before.to_string());
    for f in premarked {
        assert_eq!(
            meta.value(&orphan_key(9, f)).as_ref(),
            Some(&stamp),
            "leg F: the mark for {f:?} was re-stamped — its grace clock restarted"
        );
    }
    assert_eq!(
        meta.value(&orphan_key(9, unreadable)).as_ref(),
        Some(&garbage),
        "leg F: the unreadable mark was re-stamped — the only evidence a human had is gone"
    );
    assert_eq!(
        report.already_marked, 4,
        "leg F: every pre-marked fragment carried an `orphan:` record: {report:?}"
    );
    assert_eq!(
        (report.stranded_marked, meta.value(&orphan_key(9, stray))),
        (1, Some(Bytes::from(pass_clock.to_string()))),
        "leg F: the genuine stray is marked, at the pass's clock: {report:?}"
    );
}

// ---- the walk's guards: a page outside the `scan_page` contract is refused, never walked ----

/// `scan_page` promises a page of at most `limit` entries that starts strictly after its cursor
/// (clause 2 and "The page bound", `crates/traits/src/lib.rs:1377-1408`). The walk's bound and its
/// termination rest on those two promises, so a store that breaks either is refused with an
/// error — by GC's pass and by the post-restore pass alike — rather than walked:
///
/// * a store that **repeats its cursor** (an inclusive `after`, one key a page) hands back, from
///   its second page on, the very key it was asked to pass. Walked, GC's window would read that
///   one key its whole budget over and leave its cursor there for good, and restore's walk to the
///   end of the ledger would never end;
/// * a store that **overruns its limit** answers one entry more than it was asked for. Walked,
///   the window's budget would no longer bound what a pass reads.
///
/// On `main` both passes fail on the ledger past the cap instead — an error, but not this one.
#[tokio::test]
async fn guard_a_page_outside_the_scan_page_contract_is_refused_not_walked() {
    for (fault, refusal) in [
        (PageFault::RepeatsCursor, "does not advance the walk"),
        (
            PageFault::OverLong,
            "refused rather than read past the walk's budget",
        ),
    ] {
        let meta = LedgerMeta {
            fault,
            ..LedgerMeta::default()
        };
        let servers: Vec<MemDServer> = (0..3).map(|_| MemDServer::default()).collect();
        // Past the cap and past one window, so an over-long page can outrun a window's ask.
        for i in 0..B + 2 {
            meta.seed_mark(1, frag(9_000_000 + i as ChunkId, 0), 0);
        }
        // A stray the post-restore pass may mark, so that it reads the ledger at all.
        servers[2].put(frag(3, 0));
        let fleet = fleet_of(&servers);
        let ctx = GcContext {
            meta: &meta,
            fleet: &fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };

        let coord = MemCoordination::new();
        let (zone, custodian) = elect(&coord).await;
        let gc = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, NOW).await;
        let Err(err) = gc else {
            panic!(
                "{fault:?}: the GC pass walked a page outside the contract instead of refusing it"
            );
        };
        assert!(
            err.to_string().contains(refusal),
            "{fault:?}: the GC pass must refuse the page ({refusal:?}); it failed on something \
             else: {err}"
        );

        let Err(err) = reconcile_after_restore(&ctx, NOW).await else {
            panic!(
                "{fault:?}: the post-restore pass walked a page outside the contract instead of \
                 refusing it"
            );
        };
        assert!(
            err.to_string().contains(refusal),
            "{fault:?}: the post-restore pass must refuse the page ({refusal:?}); it failed on \
             something else: {err}"
        );
    }
}
