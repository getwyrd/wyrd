//! Issue #1084 (proposal 0016 X47, `0016:2587`; the pre-mark rule, `0016:1287-1312`; the write
//! gate, `0016:1341-1360`; the per-move nonce, `0016:1179-1189`) — **reconstruction's committed
//! repair pre-marks its destination, gates its write, and adopts the pre-mark in the commit that
//! repoints the chunk.**
//!
//! Before it, the committed repair wrote the rebuilt fragment, then committed the repoint with a
//! fresh legacy mark on the **vacated** position only: it never read or touched the mark at its
//! **destination**, wrote the vacated mark blind, and sent the write with no deadline. These legs
//! drive the repair through the production fenced control point, `reconcile_step`, with a
//! reconstruction context built as the deployed loop builds it (`crates/server/src/custodian.rs`,
//! `run_reconstruction_until`: the context's clock and the pass's `now_millis` one source, the
//! write window `W_WRITE_MILLIS`), over `RedbMetadataStore::in_memory()` behind a wrapper that can
//! hold or race a commit, and a placement-aware chunk-store double that enforces `put_fragment`'s
//! deadline on the test's own clock, as a D server does (`wyrd_traits::WriteDeadlineExpired`). One
//! `ManualClock` backs every context's clock and every pass's `now_millis`; GC passes run on it
//! too.
//!
//! **Topology.** Four D servers, 2–5, in four distinct failure domains. The object — published by
//! a production multipart Complete, so a second Complete can supersede it — holds one RS(2,1)
//! chunk on servers 3, 4 and 5 (fragment 2 on server 5), flat or segmented. Server 2 is free. A
//! repair excludes only the survivors' domains, so each pass whose destination a leg relies on is
//! given a topology with one free domain: servers 2, 3, 4 for a move to server 2, servers 3, 4, 5
//! for a move to — or a rebuild in place on — server 5. The placement is asserted after each such
//! pass before any mark is.
//!
//! Each leg runs on a flat and on a segmented object:
//!
//! * **(D1)** A stale legacy mark at the destination: once the repair commits, no mark exists at
//!   any position the new placement names — a fragment rebuilt in place included — and the
//!   vacated position carries its mark as it always has.
//! * **(D2)** The round trip: the fragment lost on 5 and rebuilt on 2, then lost on 2, within
//!   grace, and rebuilt on 5: no mark exists at server 5's position after the second commit.
//! * **(D3)** The consequence, after (D1) and after (D2): a production Complete supersedes the
//!   object; GC-only passes with the clock past the old mark's grace reclaim none of its
//!   fragments before the retirement drain marks them, and every one is gone after those marks'
//!   grace.
//! * **(D4)** A destination whose mark is `reclaiming`, or none of the three mark shapes, is
//!   never written: with no other free domain the repair is refused, names the position on the
//!   audit seam, and leaves the mark byte-identical. A **vacated** position whose mark is
//!   `reclaiming` or unreadable still holds those exact bytes after the adoption, and one whose
//!   mark changes after the adoption read it makes the adoption lose. A destination mark GC swaps
//!   to `reclaiming` between the assessment and the pre-mark makes the pre-mark lose, and the pass
//!   does not certify. A repair with no free domain and no mark anywhere is blocked for want of
//!   capacity, never named as refused, and the pass does not certify over it either.
//! * **(D5)** GC records its reclaim intent on the destination's mark while the move is in flight,
//!   and the adoption is released while the key still holds `reclaiming` — GC held right after
//!   its intent commit, or right after its fragment delete and before its key cleanup: the
//!   adoption fails, no placement names the reclaimed position, the pass does not certify, and a
//!   later pass repairs.
//! * **(D6)** A move that loses its commit (the root changes first): the fragment it wrote is
//!   reclaimed by GC after grace, not kept for ever. A root that moved does not show the chunk was
//!   repaired, so the pass does not certify, whether the move lost at its pre-mark or at its
//!   adoption. A move whose object is superseded, or whose `seg:` record alone is rewritten,
//!   before its pre-mark commits loses there, on the repoint's own pins: no write is sent and no
//!   pre-mark is written.
//! * **(D7)** The write is gated, not only the adoption. **(D7a)** Server 2 holds an unreferenced
//!   copy under an aged mark; the move is held right after its pre-mark lands while the clock
//!   passes `W_repoint` and the grace and GC reclaims the copy and the key: released, the move
//!   sends no write at all. **(D7b)** The same, with the write held in flight: the D server then
//!   refuses it. **(D7c)** The deadline passes while the pre-mark still stands: refused, nothing
//!   lands, nothing adopted; a millisecond earlier it lands and is adopted. **(D7d)** The gate's
//!   exact boundary: a move held after its pre-mark until exactly the stamp plus `W_repoint`
//!   sends no write; a millisecond earlier it does, and commits.
//! * **(D8)** Each move's identity is its own: two moves of one chunk pre-mark in the same
//!   millisecond; GC reclaims the first one's destination and its mark while the first is held
//!   before its adoption and the second before its pre-mark; the second then re-creates the key.
//!   The first move's adoption fails, and no placement names a missing fragment.
//! * **(D9)** A refusal never certifies: every refused leg above, and a write whose landing the D
//!   server cannot certify, answers a non-`Satisfied` verdict, keeps the obligation, and is named
//!   under its own reason on the audit seam, never `unplaced`.
//! * **(D10)** A fragment rebuilt in place is never left under a mark: **(a)** a pass that dies at
//!   its adoption, or whose adoption loses, leaves no mark on the position the placement still
//!   names, the next pass drains the obligation, and (D3) holds after it; **(b)** a supersede
//!   between the pre-mark batch and the write: a write that lands before its deadline is
//!   reclaimed only after the drain's mark's grace, and one held past the grace is refused.
//! * **(Swap)** A position a mark rules out rules out only itself: an RS(2,2) chunk missing two
//!   fragments, with the selector's first pick for one of them on a position whose mark no
//!   writer may replace, is repaired in one pass by the two free servers swapping fragments.
//!
//! The evacuation legs — rebalance's committed move, the same rule — are the test module of
//! `crates/custodian/src/rebalance.rs`: they build a `RebalanceContext`, whose clock and write
//! window fields this change adds, so no file here could hold them and compile on the base.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::Dispatch;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self as md, decode_orphan_mark, encode_orphan_mark, orphan_key, ChunkRef, EcScheme, InodeId,
    InodeRecord, OrphanMark,
};
use wyrd_core::multipart::complete::{
    complete_upload, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_part_record, decode_part_summary, decode_retire_obligation, part_key, psum_key, Budget,
    CompleteOutcome, CreateOutcome, Digest, PartNumber, PartSummary, UploadId,
};
use wyrd_core::placement::Topology;
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_custodian::gc::{W_REPOINT_MILLIS, W_WRITE_MILLIS};
use wyrd_custodian::{
    reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext, Reconciled,
    ReconstructionContext, Tombstone,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    Result as StoreResult, WriteBatch, WriteDeadlineExpired,
};

// ─────────────────────────────── fixtures ───────────────────────────────

/// Where the clock starts.
const T0: u64 = 10_000_000;
/// The orphan grace every GC pass here honours.
const GRACE: u64 = 60_000;
/// `W_repoint` (`crates/custodian/src/gc.rs:218`), stated here.
const W_REPOINT: u64 = 10_000;
const _: () = assert!(W_REPOINT == W_REPOINT_MILLIS);
// A clock moved past the grace is past `W_repoint` and the write window too.
const _: () = assert!(GRACE > W_REPOINT + W_WRITE_MILLIS);
// The gate closes inside the write window, so a move held exactly at the gate would still have
// its write taken by the D server (D7d).
const _: () = assert!(W_REPOINT < W_WRITE_MILLIS);
const BUCKET: &str = "photos";
const OBJECT: &str = "photos/moved";
const ROOT: InodeId = 0;
/// The inode the object is published under.
const INODE: InodeId = 40;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const COMMITTED_AT: u64 = 1_500;
const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
/// The swap leg's scheme.
const RS22: EcScheme = EcScheme::ReedSolomon { k: 2, m: 2 };
/// The chunk every leg moves.
const CHUNK: ChunkId = 0xC0FF_EE00;
const CHUNK_LEN: u64 = 64;
/// The fragment index on [`SOURCE`].
const INDEX: u16 = 2;
/// The four D servers, each in its own failure domain.
const SERVERS: [DServerId; 4] = [2, 3, 4, 5];
/// The server holding the moved fragment.
const SOURCE: DServerId = 5;
/// The one server in a free domain: the only legal destination for [`SOURCE`]'s fragment.
const FREE: DServerId = 2;
/// The fleet with [`SOURCE`] lost: the repair's one free domain is [`FREE`]'s.
const WITHOUT_SOURCE: [DServerId; 3] = [2, 3, 4];
/// The fleet with [`FREE`] down: the repair's one free domain is [`SOURCE`]'s own.
const WITHOUT_FREE: [DServerId; 3] = [3, 4, 5];
/// The swap leg's fleet: its RS(2,2) chunk's servers 5 and 6 gone, servers 2 and 7 free.
const SWAP_VIEW: [DServerId; 4] = [2, 3, 4, 7];
/// A stale legacy mark, stamped long before the clock starts.
const STALE: &[u8] = b"1000";
/// A value that is none of the three mark shapes.
const UNREADABLE: &[u8] = b"not a mark";
/// First chunk id of a segmented object's filler chunks.
const FILLER: ChunkId = 0x00F1_0000;
/// The chunk the superseding Complete publishes.
const REPLACEMENT: ChunkId = 0xBEEF_0000;
/// The audit target the repair names what it did on.
const AUDIT: &str = "wyrd.custodian.reconstruction.audit";
/// The gauge a pass emits its count of repairs it could not place on, as `tracing` spells it.
const BLOCKED_LEVEL: &str = "gauge.reconstruction_repair_blocked";
/// The fleet with [`SOURCE`] lost and [`FREE`] down: no free domain is left for a repair.
const SURVIVORS: [DServerId; 2] = [3, 4];

/// The committed object's shape.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// One part holding the moved chunk: a flat chunk map.
    Flat,
    /// Two parts of 100 chunks, past the flat map's 158: a segmented chunk map, the moved chunk
    /// first in segment 0. The filler chunks are single-copy chunks on server 3, which no leg
    /// moves.
    Segmented,
}

impl Shape {
    /// The object's parts, `moved` first.
    fn parts(self, moved: ChunkRef) -> Vec<(u32, Vec<ChunkRef>)> {
        match self {
            Shape::Flat => vec![(1, vec![moved])],
            Shape::Segmented => {
                let mut first = vec![moved];
                first.extend((0..99).map(filler));
                vec![(1, first), (2, (99..199).map(filler).collect())]
            }
        }
    }
}

fn moved_chunk() -> ChunkRef {
    ChunkRef {
        id: CHUNK,
        scheme: RS21,
        len: CHUNK_LEN,
        placement: vec![3, 4, SOURCE],
    }
}

fn filler(n: u64) -> ChunkRef {
    ChunkRef {
        id: FILLER + ChunkId::from(n),
        scheme: EcScheme::None,
        len: CHUNK_LEN,
        placement: vec![3],
    }
}

/// The moved fragment's identity.
fn moved() -> FragmentId {
    FragmentId {
        chunk: CHUNK,
        index: INDEX,
    }
}

/// Fragment `index` of the moved chunk, as the write path encodes it.
fn fragment(index: u16) -> Bytes {
    fragment_of(&moved_chunk(), index)
}

/// Fragment `index` of `chunk`, an erasure-coded chunk over the bytes `0..len`, as the write
/// path encodes it.
fn fragment_of(chunk: &ChunkRef, index: u16) -> Bytes {
    let EcScheme::ReedSolomon { k, m } = chunk.scheme else {
        panic!("an erasure-coded chunk");
    };
    let data: Vec<u8> = (0..chunk.len as u8).collect();
    let shards = erasure::encode(usize::from(k), usize::from(m), &data).unwrap();
    encode_ec_fragment(chunk.id, index, k, m, &shards[usize::from(index)])
}

fn legacy(at: u64) -> Bytes {
    encode_orphan_mark(&OrphanMark::legacy(at))
}

fn reclaiming() -> Bytes {
    encode_orphan_mark(&OrphanMark::legacy(1_000).into_reclaiming())
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

fn upload(n: u64) -> UploadId {
    UploadId::new(format!("{n:032x}")).unwrap()
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x3c; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

/// The puts planting part `n` of `id` over `chunks`: its `part:` record and `psum:` summary, each
/// in its codec's own spelling.
fn plant_part(id: &UploadId, n: u32, chunks: &[ChunkRef]) -> Vec<(Vec<u8>, Bytes)> {
    let digest = digest_of(n);
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    let record = format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{COMMITTED_AT},\"session_epoch\":0}}",
        String::from_utf8(md::encode(&chunks).to_vec()).unwrap()
    );
    let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
    assert_eq!(md::encode(&decoded), record.as_bytes());
    let summary = PartSummary {
        chunks: chunks.len() as u32,
        len,
        digest,
        committed_at_millis: COMMITTED_AT,
    };
    let psum = md::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    vec![
        (part_key(id, part(n)), Bytes::from(record)),
        (psum_key(id, part(n)), psum),
    ]
}

/// Create's runtime: one fixed identity, no jitter, no wait.
struct Fixed(Option<(UploadId, md::SegmentNonce)>);

impl CreateRuntime for Fixed {
    fn mint(&mut self) -> (UploadId, md::SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

/// A profile admitting sixteen sessions; Complete reads no other cap of it.
fn profile() -> Budget {
    let json = "{\"w_ref\":16,\"max_part_chunks\":1,\"max_parts_per_session\":1,\
                \"max_inflight_parts\":1,\"max_staged_chunks\":1}";
    md::decode(json.as_bytes()).expect("a legal profile")
}

// ─────────────────────────── one move's interleavings ───────────────────────────

/// A point a move is stopped at until the test releases it.
#[derive(Clone, Default)]
struct Hold {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl Hold {
    async fn wait(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }
}

/// Run `pass` until it reaches `hold`, then `meanwhile`, then release it; `true` if the hold was
/// reached at all. A pass that never reaches it simply finishes, so a move that skips the step
/// the hold sits on fails its leg instead of hanging it.
async fn held<T>(
    hold: &Hold,
    pass: impl Future<Output = T>,
    meanwhile: impl Future<Output = ()>,
) -> (T, bool) {
    let done = Notify::new();
    let pass = async {
        let out = pass.await;
        done.notify_one();
        out
    };
    let orchestrate = async {
        tokio::select! {
            () = hold.reached.notified() => {
                meanwhile.await;
                hold.release.notify_one();
                true
            }
            () = done.notified() => false,
        }
    };
    tokio::join!(pass, orchestrate)
}

/// A pass run alongside the leg's script, which can wait for it to finish.
struct Running {
    done: Notify,
}

impl Running {
    fn new() -> Self {
        Self {
            done: Notify::new(),
        }
    }

    /// `pass`, telling [`Self::finished`] when it ends.
    async fn run<T>(&self, pass: impl Future<Output = T>) -> T {
        let out = pass.await;
        self.done.notify_one();
        out
    }

    /// Wait for `hold`, failing the leg if the pass ends without reaching it.
    async fn reaches(&self, hold: &Hold, what: &str) {
        tokio::select! {
            () = hold.reached.notified() => {}
            () = self.done.notified() => panic!("{what}"),
        }
    }

    async fn finished(&self) {
        self.done.notified().await;
    }
}

/// What the metadata wrapper does with a commit a hook matches.
enum Act {
    /// Hold the commit before it is applied.
    Before(Hold),
    /// Hold the commit once it has been applied.
    After(Hold),
    /// Apply these puts first — another writer that got there first — then the commit.
    PlantFirst(Vec<(Vec<u8>, Bytes)>),
}

struct Hook {
    when: fn(&WriteBatch) -> bool,
    act: Act,
}

/// The commit that rewrites the object's placement record — the move's last, on the base and
/// with the fix alike.
fn repoints(batch: &WriteBatch) -> bool {
    batch
        .puts
        .iter()
        .any(|(key, _)| key.starts_with(b"inode:") || key.starts_with(b"seg:"))
}

/// Whether `batch` puts at the moved fragment's position on [`FREE`] a mark `matches` accepts.
fn marks_free(batch: &WriteBatch, matches: fn(&OrphanMark) -> bool) -> bool {
    let position = orphan_key(FREE, moved());
    batch.puts.iter().any(|(key, value)| {
        *key == position && decode_orphan_mark(value).is_ok_and(|mark| matches(&mark))
    })
}

/// A commit that pre-marks the free server's position and rewrites no placement record: a
/// destination pre-mark, a structured mark naming a move.
fn premarks_free(batch: &WriteBatch) -> bool {
    !repoints(batch)
        && marks_free(batch, |mark| {
            !mark.is_reclaiming() && mark.event().is_some_and(|e| e.starts_with("repoint:"))
        })
}

/// GC's reclaim-intent commit on the free server's position: its mark swapped to `reclaiming`.
fn reclaims_free(batch: &WriteBatch) -> bool {
    marks_free(batch, OrphanMark::is_reclaiming)
}

/// `RedbMetadataStore` behind a wrapper that acts once on the first commit each armed hook
/// matches, in the order they were armed.
struct Store {
    inner: RedbMetadataStore,
    hooks: Mutex<Vec<Hook>>,
}

impl Store {
    fn new() -> Self {
        Self {
            inner: RedbMetadataStore::in_memory().unwrap(),
            hooks: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self, when: fn(&WriteBatch) -> bool, act: Act) {
        self.hooks.lock().unwrap().push(Hook { when, act });
    }

    /// Fixture writes, straight to the inner store.
    async fn plant(&self, puts: Vec<(Vec<u8>, Bytes)>) {
        for chunk in puts.chunks(400) {
            let mut batch = WriteBatch::new();
            batch.puts.extend(chunk.iter().cloned());
            assert_eq!(
                self.inner.commit(batch).await.unwrap(),
                CommitOutcome::Committed
            );
        }
    }

    async fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.get(key).await.unwrap()
    }
}

#[async_trait]
impl MetadataStore for Store {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<wyrd_traits::ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        let act = {
            let mut hooks = self.hooks.lock().unwrap();
            let at = hooks.iter().position(|hook| (hook.when)(&batch));
            at.map(|at| hooks.remove(at).act)
        };
        match act {
            None => self.inner.commit(batch).await,
            Some(Act::Before(hold)) => {
                hold.wait().await;
                self.inner.commit(batch).await
            }
            Some(Act::After(hold)) => {
                let landed = self.inner.commit(batch).await;
                hold.wait().await;
                landed
            }
            Some(Act::PlantFirst(puts)) => {
                self.plant(puts).await;
                self.inner.commit(batch).await
            }
        }
    }
}

// ─────────────────────────────── the D servers ───────────────────────────────

#[derive(Default)]
struct FleetState {
    held: BTreeMap<DServerId, HashMap<FragmentId, Bytes>>,
    /// Every fragment write a D server was sent, accepted or refused.
    sent: Vec<(DServerId, FragmentId)>,
    /// One write to hold in flight, before the D server judges its deadline.
    hold: Option<((DServerId, FragmentId), Hold)>,
    /// One write to hold once its bytes are published, before the D server reads its clock again
    /// to certify the landing.
    hold_published: Option<((DServerId, FragmentId), Hold)>,
    /// One delete to hold once its bytes are gone.
    hold_deleted: Option<((DServerId, FragmentId), Hold)>,
}

/// In-process D servers addressed by id. Each judges a write's deadline on the test's clock, as a
/// D server judges it on its own: immediately before it stores the bytes
/// (`WriteDeadlineExpired::if_elapsed`), and once more right after, refusing to certify a landing
/// its clock then reads at or past the deadline (`WriteDeadlineExpired::if_publication_unverified`,
/// the bytes left where they landed).
#[derive(Clone)]
struct Fleet {
    state: Arc<Mutex<FleetState>>,
    clock: ManualClock,
}

impl Fleet {
    fn new(clock: ManualClock) -> Self {
        let mut state = FleetState::default();
        for id in SERVERS {
            state.held.insert(id, HashMap::new());
        }
        Self {
            state: Arc::new(Mutex::new(state)),
            clock,
        }
    }

    fn put(&self, dserver: DServerId, frag: FragmentId, bytes: Bytes) {
        let mut state = self.state.lock().unwrap();
        state.held.entry(dserver).or_default().insert(frag, bytes);
    }

    fn take(&self, dserver: DServerId, frag: FragmentId) -> Option<Bytes> {
        let mut state = self.state.lock().unwrap();
        state.held.get_mut(&dserver)?.remove(&frag)
    }

    fn holds(&self, dserver: DServerId, frag: FragmentId) -> Option<Bytes> {
        let state = self.state.lock().unwrap();
        state.held.get(&dserver)?.get(&frag).cloned()
    }

    fn listed(&self, dserver: DServerId) -> Vec<FragmentId> {
        let state = self.state.lock().unwrap();
        let held = state.held.get(&dserver);
        held.map(|held| held.keys().copied().collect())
            .unwrap_or_default()
    }

    /// The writes `dserver` was sent so far.
    fn sent_to(&self, dserver: DServerId) -> Vec<FragmentId> {
        let state = self.state.lock().unwrap();
        state
            .sent
            .iter()
            .filter(|(to, _)| *to == dserver)
            .map(|(_, frag)| *frag)
            .collect()
    }

    /// Hold the next write of `frag` to `dserver` in flight.
    fn hold_write(&self, dserver: DServerId, frag: FragmentId) -> Hold {
        let hold = Hold::default();
        self.state.lock().unwrap().hold = Some(((dserver, frag), hold.clone()));
        hold
    }

    /// Hold the next write of `frag` to `dserver` once its bytes are published.
    fn hold_published(&self, dserver: DServerId, frag: FragmentId) -> Hold {
        let hold = Hold::default();
        self.state.lock().unwrap().hold_published = Some(((dserver, frag), hold.clone()));
        hold
    }

    /// Hold the next delete of `frag` on `dserver` once its bytes are gone.
    fn hold_delete(&self, dserver: DServerId, frag: FragmentId) -> Hold {
        let hold = Hold::default();
        self.state.lock().unwrap().hold_deleted = Some(((dserver, frag), hold.clone()));
        hold
    }
}

/// Take `slot`'s hold if it is armed at `at`.
fn take_hold(
    slot: &mut Option<((DServerId, FragmentId), Hold)>,
    at: (DServerId, FragmentId),
) -> Option<Hold> {
    match slot.as_ref() {
        Some((held, _)) if *held == at => slot.take().map(|(_, hold)| hold),
        _ => None,
    }
}

/// One D server of a [`Fleet`].
struct Server {
    id: DServerId,
    fleet: Fleet,
}

#[async_trait]
impl ChunkStore for Server {
    async fn put_fragment(
        &self,
        id: FragmentId,
        bytes: Bytes,
        deadline_millis: Option<u64>,
    ) -> StoreResult<()> {
        let (hold, published) = {
            let mut state = self.fleet.state.lock().unwrap();
            state.sent.push((self.id, id));
            let at = (self.id, id);
            (
                take_hold(&mut state.hold, at),
                take_hold(&mut state.hold_published, at),
            )
        };
        if let Some(hold) = hold {
            hold.wait().await;
        }
        if let Some(deadline) = deadline_millis {
            let now = self.fleet.clock.now_millis();
            if let Some(refusal) = WriteDeadlineExpired::if_elapsed(id, deadline, now) {
                return Err(Box::new(refusal));
            }
        }
        self.fleet.put(self.id, id, bytes);
        if let Some(hold) = published {
            hold.wait().await;
        }
        if let Some(deadline) = deadline_millis {
            let now = self.fleet.clock.now_millis();
            if let Some(unverified) =
                WriteDeadlineExpired::if_publication_unverified(id, deadline, now)
            {
                return Err(Box::new(unverified));
            }
        }
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
        Ok(self.fleet.holds(self.id, id))
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        Ok(self.fleet.listed(self.id))
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        self.fleet.take(self.id, id);
        let hold = take_hold(
            &mut self.fleet.state.lock().unwrap().hold_deleted,
            (self.id, id),
        );
        if let Some(hold) = hold {
            hold.wait().await;
        }
        Ok(())
    }

    async fn health(&self) -> StoreResult<Health> {
        Ok(Health::Healthy)
    }
}

// ─────────────────────────────── the audit seam ───────────────────────────────

/// The repair's audit events, and each pass's repair-blocked level, captured by a subscriber
/// attached to each pass's future.
#[derive(Clone, Default)]
struct Audit(Arc<Mutex<Vec<HashMap<String, String>>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if metadata.target() == AUDIT || metadata.fields().field(BLOCKED_LEVEL).is_some() {
            let mut fields = FieldText(HashMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

struct FieldText(HashMap<String, String>);

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl Audit {
    fn dispatch(&self) -> Dispatch {
        Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    /// Whether an event of `action` names `subject` under `field`.
    fn names(&self, action: &str, field: &str, subject: &str) -> bool {
        self.0.lock().unwrap().iter().any(|event| {
            event.get("action").map(String::as_str) == Some(action)
                && event.get(field).map(String::as_str) == Some(subject)
        })
    }

    /// The repair-blocked level the latest pass emitted: the chunks it could not place.
    fn blocked_level(&self) -> Option<String> {
        let events = self.0.lock().unwrap();
        events
            .iter()
            .rev()
            .find_map(|event| event.get(BLOCKED_LEVEL).cloned())
    }

    /// A refusal is named under `reason`, and never as an abort for want of a place.
    fn names_refusal(&self, action: &str, reason: &str) {
        assert!(
            self.names(action, "reason", reason),
            "the audit seam does not name the refusal {action}/{reason}"
        );
        assert!(
            !self.names("aborted", "reason", "unplaced"),
            "a refusal was named as an abort for want of a place"
        );
    }
}

// ─────────────────────────────── the rig ───────────────────────────────

/// One store, one fleet, one clock — every pass's one time source — and the audit seam.
struct Rig {
    store: Store,
    fleet: Fleet,
    clock: ManualClock,
    audit: Audit,
    /// The moved chunk's scheme.
    scheme: EcScheme,
}

async fn elect() -> (FencedZone, Custodian) {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-move-destination-marks")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    (zone, custodian)
}

fn topology(view: &[DServerId]) -> Topology {
    let mut topology = Topology::default();
    for &id in view {
        topology.register(id, format!("d{id}"));
    }
    topology
}

impl Rig {
    /// The object of `shape` published by a production Complete, its moved chunk's fragments on
    /// servers 3, 4 and 5 and its filler chunks' on server 3, and the session's own records
    /// drained, so the committed map alone names the chunk.
    async fn new(shape: Shape) -> Self {
        Self::moving(shape, moved_chunk()).await
    }

    /// [`Self::new`] with `moved` as the chunk the legs move: an erasure-coded chunk with id
    /// [`CHUNK`].
    async fn moving(shape: Shape, moved: ChunkRef) -> Self {
        let clock = ManualClock::new(T0);
        let rig = Self {
            store: Store::new(),
            fleet: Fleet::new(clock.clone()),
            clock,
            audit: Audit::default(),
            scheme: moved.scheme,
        };
        rig.store
            .plant(vec![(md::bucket_key(BUCKET), Bytes::from_static(MARKER))])
            .await;
        let parts = shape.parts(moved);
        let id = rig.session(1).await;
        let mut puts = Vec::new();
        for (n, chunks) in &parts {
            puts.extend(plant_part(&id, *n, chunks));
            for chunk in chunks {
                for (index, dserver) in chunk.fragments() {
                    let frag = FragmentId {
                        chunk: chunk.id,
                        index,
                    };
                    let bytes = if chunk.id == CHUNK {
                        fragment_of(chunk, index)
                    } else {
                        Bytes::from(format!("{frag:?}"))
                    };
                    rig.fleet.put(dserver, frag, bytes);
                }
            }
        }
        rig.store.plant(puts).await;
        let named: Vec<u32> = parts.iter().map(|(n, _)| *n).collect();
        rig.complete(&id, &named, INODE).await;
        let flat = rig.root().await.chunk_map.as_flat().is_some();
        assert_eq!(flat, matches!(shape, Shape::Flat), "the object's shape");
        rig.settle().await;
        rig
    }

    /// A session made by Create, numbered `n`, targeting the object.
    async fn session(&self, n: u64) -> UploadId {
        let request = CreateRequest {
            bucket: BUCKET,
            parent: ROOT,
            object: OBJECT,
            content_type: None,
            created_at_millis: 1_000,
            clock_source: "wall",
        };
        let nonce = md::SegmentNonce::new(format!("{:032x}", 0x5e60_0000 + n)).unwrap();
        let created = create_session(
            &self.store.inner,
            &profile(),
            &request,
            &mut Fixed(Some((upload(n), nonce))),
        )
        .await
        .unwrap();
        let CreateOutcome::Created { upload_id } = created else {
            panic!("expected Created, got {created:?}");
        };
        upload_id
    }

    /// A production Complete of `id` naming `named`, publishing under `inode` when the name is
    /// free and superseding the object when it is not.
    async fn complete(&self, id: &UploadId, named: &[u32], inode: InodeId) {
        let list = NamedParts::new(named.iter().map(|&n| (part(n), digest_of(n))).collect());
        let request = CompleteRequest {
            bucket: BUCKET,
            parent: ROOT,
            object: OBJECT,
            parts: &list.unwrap(),
            fenced_at_millis: 2_000,
            clock_source: "wall",
        };
        let publish = PublishRequest {
            fresh_inode: inode,
            completed_at_millis: 3_000,
        };
        let outcome = complete_upload(&self.store.inner, id, &request, &publish).await;
        assert!(
            matches!(outcome, Ok(CompleteOutcome::Published(_))),
            "{outcome:?}"
        );
    }

    /// Drain passes until the `retire:` ledger is empty.
    async fn settle(&self) {
        for _ in 0..8 {
            if self.store.inner.scan(b"retire:").await.unwrap().is_empty() {
                return;
            }
            self.repair(&SERVERS).await;
        }
        panic!("the retire: ledger did not drain");
    }

    fn servers(&self, view: &[DServerId]) -> Vec<Server> {
        view.iter()
            .map(|&id| Server {
                id,
                fleet: self.fleet.clone(),
            })
            .collect()
    }

    /// One fenced reconstruction pass over the D servers of `view` — with the retirement leg
    /// after it, as every reconstruction pass runs — its context built as the deployed loop
    /// builds it: the context's clock and the pass's `now_millis` one source, the deployed write
    /// window.
    async fn repair(&self, view: &[DServerId]) -> Reconciled {
        let (zone, custodian) = elect().await;
        let servers = self.servers(view);
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
            .iter()
            .map(|s| (s.id, s as &dyn ChunkStore))
            .collect();
        let topology = topology(view);
        let ctx = ReconstructionContext {
            meta: &self.store,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
            clock: &self.clock,
            staged_write_window_millis: W_WRITE_MILLIS,
            operator_fleet_size: fleet.len(),
            tombstone: Tombstone::Retain,
        };
        let now = self.clock.now_millis();
        reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, None, now)
            .with_subscriber(self.audit.dispatch())
            .await
            .expect("the reconstruction pass never fails")
    }

    /// One fenced GC pass over the D servers of `view` at the clock's reading.
    async fn gc(&self, view: &[DServerId]) -> Reconciled {
        let (zone, custodian) = elect().await;
        let servers = self.servers(view);
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
            .iter()
            .map(|s| (s.id, s as &dyn ChunkStore))
            .collect();
        let ctx = GcContext {
            meta: &self.store,
            fleet: &fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let now = self.clock.now_millis();
        reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, None, now)
            .await
            .expect("the GC pass never fails")
    }

    /// GC passes until one changes nothing.
    async fn gc_until_quiet(&self, view: &[DServerId]) {
        for _ in 0..6 {
            if self.gc(view).await != Reconciled::Changed {
                return;
            }
        }
        panic!("GC never settled");
    }

    async fn root(&self) -> InodeRecord {
        let bytes = self.store.value(&md::inode_key(INODE)).await.unwrap();
        md::decode(&bytes).unwrap()
    }

    /// The live generation's chunks, through the one resolver every consumer reads it by.
    async fn chunks(&self) -> Vec<ChunkRef> {
        let root = self.root().await;
        let key = md::inode_key(INODE);
        let resolved = md::resolve_chunk_map(&self.store.inner, &key, &root)
            .await
            .unwrap()
            .expect("the object is live");
        resolved.chunks.into_owned()
    }

    /// The moved chunk's placement, as the live generation names it.
    async fn placement(&self) -> Vec<DServerId> {
        let chunks = self.chunks().await;
        let chunk = chunks.iter().find(|c| c.id == CHUNK).expect("the chunk");
        chunk.placement.clone()
    }

    /// Every position the live generation names.
    async fn positions(&self) -> Vec<(DServerId, FragmentId)> {
        let mut positions = Vec::new();
        for chunk in self.chunks().await {
            for (index, dserver) in chunk.fragments() {
                let frag = FragmentId {
                    chunk: chunk.id,
                    index,
                };
                positions.push((dserver, frag));
            }
        }
        positions
    }

    /// The mark at the moved fragment's position on `dserver`.
    async fn mark(&self, dserver: DServerId) -> Option<Bytes> {
        self.store.value(&orphan_key(dserver, moved())).await
    }

    async fn plant_mark(&self, dserver: DServerId, value: &[u8]) {
        self.plant_mark_at(dserver, moved(), value).await;
    }

    async fn plant_mark_at(&self, dserver: DServerId, frag: FragmentId, value: &[u8]) {
        let key = orphan_key(dserver, frag);
        self.store
            .plant(vec![(key, Bytes::copy_from_slice(value))])
            .await;
    }

    /// The stamp of a move's own pre-mark at the moved fragment's position on `dserver`: a
    /// structured, non-`reclaiming` mark whose event names a move of the chunk.
    async fn premark_stamp(&self, dserver: DServerId) -> u64 {
        let stored = self.mark(dserver).await.expect("the move's pre-mark");
        let mark = decode_orphan_mark(&stored).expect("one of the three shapes");
        let prefix = format!("repoint:{}:", wyrd_traits::chunk_hex(CHUNK));
        assert!(
            mark.event().is_some_and(|event| event.starts_with(&prefix)) && !mark.is_reclaiming(),
            "not a move's own pre-mark: {mark:?}"
        );
        mark.orphaned_at_millis()
    }

    /// `dserver` loses the moved fragment, and the repair is owed.
    async fn lose(&self, dserver: DServerId) {
        self.fleet.take(dserver, moved());
        repair::enqueue_repair(&self.store, CHUNK, "scrub")
            .await
            .unwrap();
    }

    async fn owed(&self) -> bool {
        repair::queued_repairs(&self.store)
            .await
            .unwrap()
            .contains(&CHUNK)
    }

    /// No position the live generation names carries a mark.
    async fn assert_no_mark_on_placement(&self) {
        for (dserver, frag) in self.positions().await {
            let mark = self.store.value(&orphan_key(dserver, frag)).await;
            assert!(
                mark.is_none(),
                "{frag:?} on {dserver} is named by the placement and marked {mark:?}"
            );
        }
    }

    /// Every position the live generation names holds its fragment, and the moved chunk's hold
    /// intact ones.
    async fn assert_complete(&self) {
        for (dserver, frag) in self.positions().await {
            let held = self.fleet.holds(dserver, frag);
            let held = held.unwrap_or_else(|| panic!("the placement names {frag:?} on {dserver}"));
            if frag.chunk == CHUNK {
                assert!(
                    repair::fragment_intact(&held, frag, self.scheme),
                    "{frag:?}"
                );
            }
        }
    }

    /// No fragment on `dserver` lacks both a reference and a mark.
    async fn assert_no_stray(&self, dserver: DServerId) {
        let named = self.positions().await;
        for frag in self.fleet.listed(dserver) {
            let marked = self.store.value(&orphan_key(dserver, frag)).await.is_some();
            assert!(
                marked || named.contains(&(dserver, frag)),
                "{frag:?} on {dserver} is neither referenced nor marked: GC keeps it for ever"
            );
        }
    }

    /// The move did not adopt: the placement still names the server that lost the fragment —
    /// the repair still owed, its obligation kept — and no other position it names is missing.
    async fn assert_not_adopted(&self) {
        assert_eq!(
            self.placement().await[usize::from(INDEX)],
            SOURCE,
            "the move adopted a destination it may not publish"
        );
        assert!(self.owed().await, "the obligation was dropped");
        for (dserver, frag) in self.positions().await {
            if frag != moved() {
                assert!(
                    self.fleet.holds(dserver, frag).is_some(),
                    "the placement names {frag:?} on {dserver}, which holds nothing"
                );
            }
        }
    }

    /// A later pass, server 2 the one free domain, moves the chunk there, whole and unmarked.
    async fn assert_repaired_later(&self) {
        assert_eq!(self.repair(&WITHOUT_SOURCE).await, Reconciled::Changed);
        assert_eq!(self.placement().await, vec![3, 4, FREE]);
        assert!(!self.owed().await, "the obligation was kept");
        self.assert_no_mark_on_placement().await;
        self.assert_complete().await;
    }

    /// A sibling's repoint in the segmented object's first `seg:` record, the one holding the
    /// moved chunk: the record's key, and its bytes with the first filler chunk moved from server
    /// 3 to server 4 — where this also moves its fragment. The root is not touched.
    async fn sibling_repoint(&self) -> (Vec<u8>, Bytes) {
        let root = self.root().await;
        let map = root.chunk_map.segmented().expect("a segmented object");
        let key = md::seg_key(map.group(), map.segments()[0].index).unwrap();
        let stored = self.store.value(&key).await.expect("the `seg:` record");
        let record: md::SegmentRecord = md::decode(&stored).unwrap();
        let offset = record.byte_offset();
        let mut chunks = record.into_chunks();
        assert_eq!((chunks[0].id, &chunks[1].placement), (CHUNK, &vec![3]));
        chunks[1].placement = vec![4];
        let sibling = FragmentId {
            chunk: chunks[1].id,
            index: 0,
        };
        let bytes = self.fleet.take(3, sibling).expect("the sibling's fragment");
        self.fleet.put(4, sibling, bytes);
        let rewritten = md::SegmentRecord::new(chunks, offset).unwrap();
        (key, md::encode(&rewritten))
    }

    /// A second session's Complete over the same name supersedes the object: the key of the
    /// `retire:bytes:` obligation it installs for the superseded generation, and that
    /// obligation's event.
    async fn supersede(&self) -> (Vec<u8>, String) {
        let id = self.session(2).await;
        let replacement = ChunkRef {
            id: REPLACEMENT,
            scheme: RS21,
            len: CHUNK_LEN,
            placement: vec![2, 3, 4],
        };
        for (index, dserver) in replacement.fragments() {
            let frag = FragmentId {
                chunk: REPLACEMENT,
                index,
            };
            self.fleet
                .put(dserver, frag, Bytes::from(format!("{frag:?}")));
        }
        self.store
            .plant(plant_part(&id, 1, std::slice::from_ref(&replacement)))
            .await;
        self.complete(&id, &[1], INODE + 1).await;
        let generations = self.store.inner.scan(b"retire:bytes:g:").await.unwrap();
        assert_eq!(
            generations.len(),
            1,
            "the superseded generation's obligation"
        );
        let (key, value) = generations.into_iter().next().unwrap();
        let (_, token, _) = decode_retire_obligation(&key, &value).unwrap();
        (key, token.to_string())
    }

    /// Drain passes until `obligation` is gone; every position in `positions` is then marked under
    /// `event`.
    async fn drain(&self, obligation: &[u8], event: &str, positions: &[(DServerId, FragmentId)]) {
        for _ in 0..8 {
            if self.store.value(obligation).await.is_none() {
                break;
            }
            self.repair(&SERVERS).await;
        }
        assert!(self.store.value(obligation).await.is_none(), "drained");
        for &(dserver, frag) in positions {
            let stored = self.store.value(&orphan_key(dserver, frag)).await;
            let stored = stored.unwrap_or_else(|| panic!("{frag:?} on {dserver} is unmarked"));
            let mark = decode_orphan_mark(&stored).unwrap();
            assert_eq!(mark.event(), Some(event), "{frag:?} on {dserver}");
        }
    }
}

/// One leg as two tests, on a flat and on a segmented object.
macro_rules! on_both_shapes {
    ($leg:ident => $flat:ident, $segmented:ident) => {
        #[tokio::test]
        async fn $flat() {
            $leg(Shape::Flat).await;
        }

        #[tokio::test]
        async fn $segmented() {
            $leg(Shape::Segmented).await;
        }
    };
}

// ─────────────────────────────── (D1) ───────────────────────────────

/// The (D1) move: server 2 carries a stale legacy mark at the moved fragment's position, server 5
/// loses the fragment, and the repair rebuilds it on server 2 — the move alone, so (D3) can run
/// after it. The rig, and the instant the move ran at.
async fn d1_moved(shape: Shape) -> (Rig, u64) {
    let rig = Rig::new(shape).await;
    rig.plant_mark(FREE, STALE).await;
    rig.lose(SOURCE).await;
    let at = rig.clock.now_millis();
    assert_eq!(rig.repair(&WITHOUT_SOURCE).await, Reconciled::Changed);
    assert_eq!(
        rig.placement().await,
        vec![3, 4, FREE],
        "repaired onto server 2"
    );
    (rig, at)
}

/// **(D1)** After the (D1) move no position the new placement names carries a mark, and the
/// vacated position on server 5 carries the legacy mark stamped at the move, as it always has.
async fn d1(shape: Shape) {
    let (rig, at) = d1_moved(shape).await;

    rig.assert_no_mark_on_placement().await;
    assert_eq!(
        rig.mark(SOURCE).await,
        Some(legacy(at)),
        "the vacated position carries its mark as it always has"
    );
    rig.assert_complete().await;
}

on_both_shapes!(d1 =>
    d1_a_stale_destination_mark_is_gone_once_a_flat_repair_commits,
    d1_a_stale_destination_mark_is_gone_once_a_segmented_repair_commits);

/// **(D1, rebuilt in place)** Server 2 is down, so the fragment server 5 lost is rebuilt on
/// server 5 itself, whose position carries a stale legacy mark: afterwards no mark exists there,
/// though the position never stopped being named.
async fn d1_in_place(shape: Shape) {
    let rig = Rig::new(shape).await;
    rig.plant_mark(SOURCE, STALE).await;
    rig.lose(SOURCE).await;

    assert_eq!(rig.repair(&WITHOUT_FREE).await, Reconciled::Changed);

    assert_eq!(
        rig.placement().await,
        vec![3, 4, SOURCE],
        "rebuilt in place"
    );
    rig.assert_no_mark_on_placement().await;
    rig.assert_complete().await;
}

on_both_shapes!(d1_in_place =>
    d1_a_flat_fragment_rebuilt_in_place_leaves_no_mark_on_its_position,
    d1_a_segmented_fragment_rebuilt_in_place_leaves_no_mark_on_its_position);

// ─────────────────────────────── (D2) ───────────────────────────────

/// The (D2) moves, the round trip: server 5 loses the fragment, which is rebuilt on server 2;
/// within grace server 2 loses it, and it is rebuilt on server 5 — the moves alone, so (D3) can
/// run after them. The rig, and the instants the two moves ran at.
async fn d2_moved(shape: Shape) -> (Rig, u64, u64) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let first = rig.clock.now_millis();
    assert_eq!(rig.repair(&WITHOUT_SOURCE).await, Reconciled::Changed);
    assert_eq!(rig.placement().await, vec![3, 4, FREE]);
    assert_eq!(
        rig.mark(SOURCE).await,
        Some(legacy(first)),
        "the vacated mark"
    );

    rig.clock.advance(1_000);
    rig.lose(FREE).await;
    let second = rig.clock.now_millis();
    assert_eq!(rig.repair(&WITHOUT_FREE).await, Reconciled::Changed);
    assert_eq!(
        rig.placement().await,
        vec![3, 4, SOURCE],
        "back on server 5"
    );
    (rig, first, second)
}

/// **(D2)** After the round trip no mark exists at server 5's position — the first move's mark
/// on it is gone — and server 2's vacated position carries one.
async fn d2(shape: Shape) {
    let (rig, _, second) = d2_moved(shape).await;

    assert_eq!(
        rig.mark(SOURCE).await,
        None,
        "the first move's mark outlived it on a position the placement names"
    );
    rig.assert_no_mark_on_placement().await;
    assert_eq!(
        rig.mark(FREE).await,
        Some(legacy(second)),
        "the vacated mark"
    );
    rig.assert_complete().await;
}

on_both_shapes!(d2 =>
    d2_a_flat_fragment_repaired_back_within_grace_carries_no_mark,
    d2_a_segmented_fragment_repaired_back_within_grace_carries_no_mark);

// ─────────────────────────────── (D3) ───────────────────────────────

/// A production Complete supersedes the object, then GC-only passes run with the clock past
/// `old`'s grace — `old` the stamp of the mark the base leaves on a position the moved placement
/// names: they reclaim none of the superseded generation's fragments, which no mark covers yet.
/// Drain passes then mark every one under the obligation's event, and past those marks' grace GC
/// reclaims them all.
async fn supersede_then_reclaim(rig: &Rig, old: u64) {
    let positions = rig.positions().await;
    let (obligation, event) = rig.supersede().await;
    let past = old + GRACE + 1;
    if rig.clock.now_millis() < past {
        rig.clock.set(past);
    }

    for _ in 0..2 {
        rig.gc(&SERVERS).await;
    }
    for &(dserver, frag) in &positions {
        assert!(
            rig.fleet.holds(dserver, frag).is_some(),
            "GC reclaimed {frag:?} on {dserver} before the drain marked it"
        );
    }

    rig.drain(&obligation, &event, &positions).await;

    rig.clock.advance(GRACE + 1);
    rig.gc_until_quiet(&SERVERS).await;
    for &(dserver, frag) in &positions {
        assert!(
            rig.fleet.holds(dserver, frag).is_none(),
            "{frag:?} on {dserver} survived its mark's grace"
        );
    }
}

/// **(D3, after D1)** The stale mark the base leaves at the destination is stamped 1000.
async fn d3_after_d1(shape: Shape) {
    let (rig, _) = d1_moved(shape).await;
    supersede_then_reclaim(&rig, 1_000).await;
}

on_both_shapes!(d3_after_d1 =>
    d3_a_superseded_flat_object_keeps_its_repaired_fragment_until_the_drain_marks_it,
    d3_a_superseded_segmented_object_keeps_its_repaired_fragment_until_the_drain_marks_it);

/// **(D3, after D2)** The mark the base leaves on server 5's position is the first move's.
async fn d3_after_d2(shape: Shape) {
    let (rig, first, _) = d2_moved(shape).await;
    supersede_then_reclaim(&rig, first).await;
}

on_both_shapes!(d3_after_d2 =>
    d3_a_superseded_flat_object_keeps_its_round_tripped_fragment_until_the_drain_marks_it,
    d3_a_superseded_segmented_object_keeps_its_round_tripped_fragment_until_the_drain_marks_it);

// ─────────────────────────────── (D4) ───────────────────────────────

/// **(D4, D9)** Server 2's position carries `mark`, a value no writer may replace; server 5 loses
/// the fragment, and server 2 is the only free domain. The repair sends server 2 no write, the
/// placement is unchanged, the mark is byte-identical, the obligation stays queued, the pass does
/// not certify, and the audit seam names the position under `action` and the refusal under its
/// own reason.
async fn refuses(shape: Shape, mark: &[u8], action: &str, field: &str) -> Rig {
    let rig = Rig::new(shape).await;
    rig.plant_mark(FREE, mark).await;
    rig.lose(SOURCE).await;

    let outcome = rig.repair(&WITHOUT_SOURCE).await;

    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "the repair wrote to a destination it may not write"
    );
    assert_eq!(
        rig.mark(FREE).await.as_deref(),
        Some(mark),
        "the mark moved"
    );
    rig.assert_not_adopted().await;
    assert_eq!(outcome, Reconciled::Blocked, "a refused repair certified");
    let position = name(&orphan_key(FREE, moved()));
    assert!(
        rig.audit.names(action, field, &position),
        "the audit seam does not name {position}"
    );
    rig.audit
        .names_refusal("repair-refused", "no-usable-destination");
    assert_eq!(
        rig.audit.blocked_level().as_deref(),
        Some("1"),
        "a refused repair is not counted on the repair-blocked level"
    );
    rig
}

/// **(D4, D9: no mark at all)** A repair with no free failure domain and no mark anywhere is
/// blocked for want of capacity, not refused: servers 5 and 2 are both gone, so no domain distinct
/// from the survivors' is left. It is counted on the repair-blocked level and never named
/// `repair-refused`, which is a mark's refusal alone — but the repair is still owed, so the pass
/// does not certify. Nothing is written, and the obligation stays queued.
async fn d4_no_free_domain(shape: Shape) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;

    let outcome = rig.repair(&SURVIVORS).await;

    assert_eq!(
        rig.audit.blocked_level().as_deref(),
        Some("1"),
        "the repair-blocked level"
    );
    assert!(
        !rig.audit
            .names("repair-refused", "reason", "no-usable-destination"),
        "a repair blocked for want of a free domain was named as refused by a mark"
    );
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a pass certified over a repair it owes and could not place"
    );
    assert!(rig.fleet.sent_to(FREE).is_empty(), "nothing is written");
    rig.assert_not_adopted().await;
}

on_both_shapes!(d4_no_free_domain =>
    d4_a_flat_repair_with_no_free_domain_is_blocked_not_refused,
    d4_a_segmented_repair_with_no_free_domain_is_blocked_not_refused);

/// **(D4, `reclaiming`)** …and once GC has finished the reclaim — its fragment-less sweep deletes
/// the mark, nothing beneath it — a later pass repairs the chunk onto server 2.
async fn d4_reclaiming(shape: Shape) {
    let rig = refuses(shape, &reclaiming(), "destination-passed-over", "subject").await;

    rig.gc_until_quiet(&WITHOUT_SOURCE).await;
    assert_eq!(rig.mark(FREE).await, None, "GC finished the reclaim");
    rig.assert_repaired_later().await;
}

on_both_shapes!(d4_reclaiming =>
    d4_a_reclaiming_destination_is_never_written_by_a_flat_repair,
    d4_a_reclaiming_destination_is_never_written_by_a_segmented_repair);

/// **(D4, unreadable)** A mark that is none of the three shapes is NEEDS-HUMAN: never overwritten.
async fn d4_unreadable(shape: Shape) {
    refuses(shape, UNREADABLE, "unreadable-destination-mark", "mark").await;
}

on_both_shapes!(d4_unreadable =>
    d4_an_unreadable_destination_mark_is_never_overwritten_by_a_flat_repair,
    d4_an_unreadable_destination_mark_is_never_overwritten_by_a_segmented_repair);

/// **(D4, the vacated position)** The mark on server 5's position — the one the repair vacates —
/// is written under a precondition on what the adoption read there, never blind: a `reclaiming`
/// or unreadable mark still holds those exact bytes after the adoption, and a stale legacy one is
/// re-stamped at the move.
async fn d4_vacated(shape: Shape) {
    let cases = [
        (reclaiming(), false),
        (Bytes::from_static(UNREADABLE), false),
        (Bytes::from_static(STALE), true),
    ];
    for (planted, restamped) in cases {
        let rig = Rig::new(shape).await;
        rig.plant_mark(SOURCE, &planted).await;
        rig.lose(SOURCE).await;
        let at = rig.clock.now_millis();

        assert_eq!(rig.repair(&WITHOUT_SOURCE).await, Reconciled::Changed);

        assert_eq!(rig.placement().await, vec![3, 4, FREE], "{planted:?}");
        let expected = if restamped {
            legacy(at)
        } else {
            planted.clone()
        };
        assert_eq!(
            rig.mark(SOURCE).await,
            Some(expected),
            "the vacated position's mark {planted:?} was written over"
        );
        rig.assert_no_mark_on_placement().await;
        rig.assert_complete().await;
    }
}

on_both_shapes!(d4_vacated =>
    d4_a_flat_repair_writes_the_vacated_mark_under_a_precondition,
    d4_a_segmented_repair_writes_the_vacated_mark_under_a_precondition);

/// **(D4, the vacated position, raced)** The vacated position's mark changes between the
/// adoption's read of it and the adoption's commit — the wrapper lands the other writer's put just
/// before that commit: an absent mark, or a stale legacy one, becomes `reclaiming`; an unreadable
/// one is replaced by a legacy mark. The adoption loses on what it read: the placement is
/// unchanged, the other writer's bytes are still there, the rebuilt fragment stays under its
/// pre-mark, the obligation stays queued and the pass does not certify. A later pass repairs, and
/// leaves a `reclaiming` mark as it finds it.
async fn d4_vacated_raced(shape: Shape) {
    let cases: [(Option<&[u8]>, Bytes); 3] = [
        (None, reclaiming()),
        (Some(STALE), reclaiming()),
        (Some(UNREADABLE), Bytes::from_static(STALE)),
    ];
    for (before, raced) in cases {
        let rig = Rig::new(shape).await;
        if let Some(before) = before {
            rig.plant_mark(SOURCE, before).await;
        }
        rig.lose(SOURCE).await;
        let racer = vec![(orphan_key(SOURCE, moved()), raced.clone())];
        rig.store.arm(repoints, Act::PlantFirst(racer));

        let outcome = rig.repair(&WITHOUT_SOURCE).await;

        assert_eq!(
            rig.mark(SOURCE).await,
            Some(raced.clone()),
            "{before:?}: the adoption wrote over a vacated mark that changed after its read"
        );
        rig.assert_not_adopted().await;
        assert_eq!(outcome, Reconciled::Blocked, "{before:?}: a lost adoption");
        rig.audit.names_refusal("aborted", "adoption-lost");
        assert!(
            rig.fleet.holds(FREE, moved()).is_some(),
            "{before:?}: the move wrote its fragment"
        );
        rig.premark_stamp(FREE).await;
        rig.assert_no_stray(FREE).await;

        rig.clock.advance(1_000);
        let later = rig.clock.now_millis();
        rig.assert_repaired_later().await;
        let kept = decode_orphan_mark(&raced).is_ok_and(|mark| mark.is_reclaiming());
        let expected = if kept { raced } else { legacy(later) };
        assert_eq!(rig.mark(SOURCE).await, Some(expected), "{before:?}");
    }
}

on_both_shapes!(d4_vacated_raced =>
    d4_a_flat_repairs_adoption_loses_to_a_vacated_mark_changed_after_its_read,
    d4_a_segmented_repairs_adoption_loses_to_a_vacated_mark_changed_after_its_read);

/// **(D4, D9: the destination, raced)** Server 2's position carries a stale legacy mark when the
/// repair is assessed, and GC swaps it to `reclaiming` before the pre-mark commits — the wrapper
/// lands the swap just before that commit. The pre-mark loses on the mark the choice read:
/// nothing is written, the `reclaiming` mark is byte-identical, the obligation stays queued, and
/// the pass does not certify, though no record of the object moved. Once GC has finished the
/// reclaim, a later pass repairs.
async fn d4_destination_raced(shape: Shape) {
    let rig = Rig::new(shape).await;
    rig.plant_mark(FREE, STALE).await;
    rig.lose(SOURCE).await;
    let swap = vec![(orphan_key(FREE, moved()), reclaiming())];
    rig.store.arm(premarks_free, Act::PlantFirst(swap));

    let outcome = rig.repair(&WITHOUT_SOURCE).await;

    assert_eq!(
        rig.mark(FREE).await,
        Some(reclaiming()),
        "the destination does not hold the `reclaiming` mark GC swapped in ahead of the pre-mark"
    );
    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "the repair wrote to a destination whose pre-mark lost"
    );
    rig.assert_not_adopted().await;
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a repair whose pre-mark lost to GC certified"
    );
    rig.audit.names_refusal("aborted", "pre-mark-lost");

    rig.gc_until_quiet(&WITHOUT_SOURCE).await;
    assert_eq!(rig.mark(FREE).await, None, "GC finished the reclaim");
    rig.assert_repaired_later().await;
}

on_both_shapes!(d4_destination_raced =>
    d4_a_flat_repairs_pre_mark_that_loses_to_gc_never_certifies,
    d4_a_segmented_repairs_pre_mark_that_loses_to_gc_never_certifies);

// ─────────────────────────────── (D5) ───────────────────────────────

/// Where GC is held while the move's adoption is released.
#[derive(Clone, Copy, Debug)]
enum GcHeld {
    /// Right after its reclaim-intent commit: the key holds `reclaiming`, the fragment is still
    /// there.
    AfterIntent,
    /// Right after it deleted the fragment, before its key cleanup: the key still holds
    /// `reclaiming`, the fragment is gone.
    AfterDelete,
}

/// **(D5)** Server 2's position carries a stale legacy mark; server 5 loses the fragment. The
/// move is held just before its adoption — after the destination write — while the clock passes
/// the grace and a GC pass records its reclaim intent on the destination's mark; GC is held at
/// `gc_held`, and the adoption is released while the key still holds `reclaiming`. The adoption
/// fails: no placement names the reclaimed position, the pass does not certify — no record of the
/// object moved, so the repair is still owed — and a later pass repairs the chunk.
async fn d5(shape: Shape, gc_held: GcHeld) {
    let rig = Rig::new(shape).await;
    rig.plant_mark(FREE, STALE).await;
    rig.lose(SOURCE).await;
    let adopt = Hold::default();
    rig.store.arm(repoints, Act::Before(adopt.clone()));
    let gc_hold = match gc_held {
        GcHeld::AfterIntent => {
            let hold = Hold::default();
            rig.store.arm(reclaims_free, Act::After(hold.clone()));
            hold
        }
        GcHeld::AfterDelete => rig.fleet.hold_delete(FREE, moved()),
    };

    let repair = Running::new();
    let script = async {
        repair
            .reaches(&adopt, "the move never reached its adoption")
            .await;
        assert!(
            rig.fleet.holds(FREE, moved()).is_some(),
            "the move wrote its fragment"
        );
        rig.clock.advance(GRACE + 1);
        let gc = Running::new();
        let meanwhile = async {
            gc.reaches(&gc_hold, "GC never reached its hold").await;
            let stored = rig.mark(FREE).await.expect("the destination's mark");
            assert!(
                decode_orphan_mark(&stored).is_ok_and(|mark| mark.is_reclaiming()),
                "{gc_held:?}: the key does not hold `reclaiming` as the adoption is released"
            );
            adopt.release.notify_one();
            repair.finished().await;
            gc_hold.release.notify_one();
        };
        tokio::join!(gc.run(rig.gc(&WITHOUT_SOURCE)), meanwhile);
    };
    let (outcome, ()) = tokio::join!(repair.run(rig.repair(&WITHOUT_SOURCE)), script);

    assert!(
        rig.fleet.holds(FREE, moved()).is_none(),
        "GC did not reclaim the destination in flight"
    );
    rig.assert_not_adopted().await;
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "{gc_held:?}: a repair whose adoption lost to GC certified"
    );
    rig.audit.names_refusal("aborted", "adoption-lost");
    rig.gc_until_quiet(&WITHOUT_SOURCE).await;
    rig.assert_repaired_later().await;
}

async fn d5_after_intent(shape: Shape) {
    d5(shape, GcHeld::AfterIntent).await;
}

async fn d5_after_delete(shape: Shape) {
    d5(shape, GcHeld::AfterDelete).await;
}

on_both_shapes!(d5_after_intent =>
    d5_a_flat_repairs_adoption_fails_while_gc_holds_its_intent,
    d5_a_segmented_repairs_adoption_fails_while_gc_holds_its_intent);

on_both_shapes!(d5_after_delete =>
    d5_a_flat_repairs_adoption_fails_after_gc_deleted_its_fragment,
    d5_a_segmented_repairs_adoption_fails_after_gc_deleted_its_fragment);

// ─────────────────────────────── (D6) ───────────────────────────────

/// Which of a repair's two commits a (D6) leg's racer beats.
#[derive(Clone, Copy, Debug)]
enum LostAt {
    /// The pre-mark commit: nothing has been written yet.
    Premark,
    /// The adoption: the rebuilt fragment has landed on server 2.
    Adoption,
}

/// **(D6)** Server 5 loses the fragment; another writer rewrites the root (its version bumped,
/// its chunks unchanged) just before the repair's pre-mark commit, or just before its adoption, so
/// that commit loses on the root pin. Lost at the pre-mark, the move wrote nothing and left no
/// mark; lost at the adoption, the fragment it wrote on server 2 is unreferenced, under its
/// pre-mark, and GC reclaims it once the grace has passed. Either way the root moved and this
/// chunk did not — its fragment is still missing and its obligation still queued — so the pass
/// does not certify, though the loss is named the lost CAS it has always been, and a later pass
/// repairs.
async fn d6(shape: Shape, lost_at: LostAt) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let mut root = rig.root().await;
    root.version += 1;
    let rewritten = vec![(md::inode_key(INODE), md::encode(&root))];
    let commit: fn(&WriteBatch) -> bool = match lost_at {
        LostAt::Premark => premarks_free,
        LostAt::Adoption => repoints,
    };
    rig.store.arm(commit, Act::PlantFirst(rewritten));

    let outcome = rig.repair(&WITHOUT_SOURCE).await;

    assert_eq!(
        rig.root().await.version,
        root.version,
        "{lost_at:?}: the racer never landed"
    );
    rig.assert_not_adopted().await;
    match lost_at {
        LostAt::Premark => {
            assert!(
                rig.fleet.sent_to(FREE).is_empty(),
                "a move whose pre-mark lost sent a write"
            );
            assert_eq!(
                rig.mark(FREE).await,
                None,
                "a pre-mark was committed over a root that had moved"
            );
        }
        LostAt::Adoption => {
            assert!(
                rig.fleet.holds(FREE, moved()).is_some(),
                "the move wrote its fragment"
            );
            rig.clock.advance(GRACE + 1);
            rig.gc_until_quiet(&WITHOUT_SOURCE).await;
            assert!(
                rig.fleet.holds(FREE, moved()).is_none(),
                "the lost move's fragment is kept for ever"
            );
        }
    }
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "{lost_at:?}: a pass certified over a repair that lost its race to a root rewrite, the \
         fragment still missing and the obligation still queued"
    );
    let chunk = wyrd_traits::chunk_hex(CHUNK);
    assert!(
        rig.audit.names("conflict", "chunk", &chunk)
            && !rig.audit.names("aborted", "chunk", &chunk),
        "{lost_at:?}: a CAS lost to a root that moved is not named a conflict"
    );
    rig.assert_repaired_later().await;
}

async fn d6_at_the_premark(shape: Shape) {
    d6(shape, LostAt::Premark).await;
}

async fn d6_at_the_adoption(shape: Shape) {
    d6(shape, LostAt::Adoption).await;
}

on_both_shapes!(d6_at_the_premark =>
    d6_a_flat_repair_that_loses_its_pre_mark_to_a_root_rewrite_never_certifies,
    d6_a_segmented_repair_that_loses_its_pre_mark_to_a_root_rewrite_never_certifies);

on_both_shapes!(d6_at_the_adoption =>
    d6_a_flat_repair_that_loses_its_commit_leaves_its_fragment_reclaimable,
    d6_a_segmented_repair_that_loses_its_commit_leaves_its_fragment_reclaimable);

/// **(D6, superseded before the pre-mark)** The pre-mark is pinned to the records the repoint is.
/// Server 5 loses the fragment, and the repair is held just before its pre-mark commit — after its
/// assessment chose server 2 — while a production Complete supersedes the object. Released, the
/// pre-mark loses on the repoint's own pins (the root generation, and the `seg:` record of a
/// segmented map): server 2 is sent no write and its position carries no pre-mark. A move whose
/// record has already moved writes nothing at all, and the loss is named a conflict. The pass
/// that lost did not read whether the repair is still owed, so it does not certify; the next
/// pass reads that no committed map names the chunk any more, and drains the obligation.
async fn d6_superseded_before_the_premark(shape: Shape) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let hold = Hold::default();
    rig.store.arm(premarks_free, Act::Before(hold.clone()));

    let (outcome, reached) = held(&hold, rig.repair(&WITHOUT_SOURCE), async {
        rig.supersede().await;
    })
    .await;

    assert!(reached, "the move never reached its pre-mark commit");
    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "a move whose object was superseded before its pre-mark sent a write"
    );
    assert_eq!(
        rig.mark(FREE).await,
        None,
        "a pre-mark was committed for a generation already superseded"
    );
    let chunk = wyrd_traits::chunk_hex(CHUNK);
    assert!(
        rig.audit.names("conflict", "chunk", &chunk)
            && !rig.audit.names("aborted", "chunk", &chunk),
        "a pre-mark lost to a root that moved is not named a conflict"
    );
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a pass certified over a repair that lost its race, its obligation still queued"
    );
    assert!(rig.owed().await, "the lost pass dropped the obligation");
    rig.repair(&SERVERS).await;
    assert!(
        !rig.owed().await,
        "the next pass kept the obligation of a chunk no committed map names"
    );
}

on_both_shapes!(d6_superseded_before_the_premark =>
    d6_a_flat_repair_superseded_before_its_pre_mark_writes_nothing,
    d6_a_segmented_repair_superseded_before_its_pre_mark_writes_nothing);

/// **(D6, the `seg:` pin)** The pre-mark is pinned to the `seg:` record the repoint read, not to
/// the root alone. A sibling chunk's repoint rewrites the segment record holding the moved chunk
/// — the root byte-identical — just before the repair's pre-mark commit. The pre-mark loses on
/// that record: server 2 is sent no write and its position carries no mark, the repair is still
/// owed and the pass does not certify. A later pass re-reads the record and repairs, the
/// sibling's edit kept.
#[tokio::test]
async fn d6_a_segmented_repairs_pre_mark_loses_to_a_seg_record_rewrite() {
    let rig = Rig::new(Shape::Segmented).await;
    rig.lose(SOURCE).await;
    let root = rig.store.value(&md::inode_key(INODE)).await;
    let (key, rewritten) = rig.sibling_repoint().await;
    let racer = vec![(key.clone(), rewritten.clone())];
    rig.store.arm(premarks_free, Act::PlantFirst(racer));

    let outcome = rig.repair(&WITHOUT_SOURCE).await;

    assert!(
        rig.store.value(&key).await == Some(rewritten),
        "the `seg:` record does not hold the sibling's repoint"
    );
    assert_eq!(
        rig.store.value(&md::inode_key(INODE)).await,
        root,
        "the root moved"
    );
    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "a move whose `seg:` record moved before its pre-mark sent a write"
    );
    assert_eq!(
        rig.mark(FREE).await,
        None,
        "a pre-mark was committed over a `seg:` record that had moved"
    );
    rig.assert_not_adopted().await;
    assert_eq!(outcome, Reconciled::Blocked, "a pre-mark lost certified");
    rig.audit.names_refusal("aborted", "pre-mark-lost");
    rig.assert_repaired_later().await;
    assert_eq!(
        rig.chunks().await[1].placement,
        vec![4],
        "the later repair dropped the sibling's edit"
    );
}

// ─────────────────────────────── (D7) ───────────────────────────────

/// Server 2 holds a copy of the fragment no record names, under the aged pre-mark of an earlier
/// move that lost its commit; server 5 loses the fragment.
async fn leftover(shape: Shape) -> Rig {
    let rig = Rig::new(shape).await;
    rig.fleet.put(FREE, moved(), fragment(INDEX));
    let earlier = T0 - 1_000;
    let event = format!("repoint:{}:earlier-move", wyrd_traits::chunk_hex(CHUNK));
    let mark = encode_orphan_mark(&OrphanMark::structured(earlier, event).unwrap());
    rig.plant_mark(FREE, &mark).await;
    rig.lose(SOURCE).await;
    rig
}

/// GC, with the clock past `W_repoint` and the pre-mark's grace: it reclaims server 2's old copy
/// and deletes the mark over it.
async fn reclaim_the_leftover(rig: &Rig) {
    rig.clock.advance(GRACE + 1);
    rig.gc_until_quiet(&WITHOUT_SOURCE).await;
    assert!(
        rig.fleet.holds(FREE, moved()).is_none(),
        "GC kept the old copy"
    );
    assert_eq!(rig.mark(FREE).await, None, "GC kept the mark");
}

/// What every (D7) leg's refused move leaves (D9): the pass did not certify, the abort is named
/// under `reason`, nothing on server 2 lacks both a reference and a mark, no placement names a
/// missing fragment, and a later pass moves the chunk.
async fn refused_then_repaired(rig: &Rig, outcome: Reconciled, reason: &str) {
    assert_eq!(outcome, Reconciled::Blocked, "a refused repair certified");
    rig.audit.names_refusal("aborted", reason);
    rig.assert_no_stray(FREE).await;
    rig.assert_not_adopted().await;
    rig.assert_repaired_later().await;
}

/// **(D7a)** The move is held right after its pre-mark commit lands; GC reclaims the old copy and
/// the pre-mark's key meanwhile. Released, the move sends server 2 no write at all.
async fn d7a(shape: Shape) {
    let rig = leftover(shape).await;
    let hold = Hold::default();
    rig.store.arm(premarks_free, Act::After(hold.clone()));

    let (outcome, reached) = held(
        &hold,
        rig.repair(&WITHOUT_SOURCE),
        reclaim_the_leftover(&rig),
    )
    .await;

    assert!(reached, "no pre-mark was committed before the write");
    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "a write was authorized on a pre-mark past W_repoint"
    );
    refused_then_repaired(&rig, outcome, "pre-mark-stale").await;
}

on_both_shapes!(d7a =>
    d7_a_a_flat_repairs_stale_pre_mark_authorizes_no_write,
    d7_a_a_segmented_repairs_stale_pre_mark_authorizes_no_write);

/// **(D7b)** The destination write is held in flight; GC reclaims the old copy and the pre-mark
/// meanwhile. Released, the D server refuses the write, its deadline passed, and nothing lands.
async fn d7b(shape: Shape) {
    let rig = leftover(shape).await;
    let hold = rig.fleet.hold_write(FREE, moved());

    let (outcome, reached) = held(
        &hold,
        rig.repair(&WITHOUT_SOURCE),
        reclaim_the_leftover(&rig),
    )
    .await;

    assert!(reached, "the move never sent its write");
    assert!(
        rig.fleet.holds(FREE, moved()).is_none(),
        "the D server stored a write past its pre-mark's evidence"
    );
    refused_then_repaired(&rig, outcome, "write-deadline-expired").await;
}

on_both_shapes!(d7b =>
    d7_b_a_flat_repairs_write_past_its_deadline_is_refused,
    d7_b_a_segmented_repairs_write_past_its_deadline_is_refused);

/// What a (D7c)/(D7d) twin that stayed inside its bound leaves: adopted.
async fn adopted_inside_grace(rig: &Rig, outcome: Reconciled) {
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "a move inside its bounds was refused"
    );
    assert_eq!(rig.placement().await, vec![3, 4, FREE]);
    assert!(!rig.owed().await, "the obligation was kept");
    rig.assert_no_mark_on_placement().await;
    rig.assert_complete().await;
}

/// Server 5 loses the fragment, and the repair runs with its destination write held — before
/// the D server judges the deadline, or once the bytes are published — while the clock moves to
/// the pre-mark's stamp plus `after`. No GC pass runs: the pre-mark stays inside its grace.
async fn hold_write_until(shape: Shape, published: bool, after: u64) -> (Rig, Reconciled) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let hold = if published {
        rig.fleet.hold_published(FREE, moved())
    } else {
        rig.fleet.hold_write(FREE, moved())
    };
    let (outcome, reached) = held(&hold, rig.repair(&WITHOUT_SOURCE), async {
        let stamp = rig.premark_stamp(FREE).await;
        rig.clock.set(stamp + after);
    })
    .await;
    assert!(reached, "the move never sent its write");
    (rig, outcome)
}

/// **(D7c)** The deadline passes while the pre-mark still stands — the clock at exactly the
/// pre-mark's stamp plus the write window, inside its grace — and the destination holds nothing:
/// the D server refuses the write, nothing is adopted, the obligation is kept, and the pre-mark
/// stands over the empty position. A move that ignored the refusal would publish a placement over
/// a position that holds nothing. A millisecond earlier the write lands and is adopted.
async fn d7c(shape: Shape) {
    let (rig, outcome) = hold_write_until(shape, false, W_WRITE_MILLIS).await;
    assert!(
        rig.fleet.holds(FREE, moved()).is_none(),
        "the D server stored a write at its deadline"
    );
    rig.premark_stamp(FREE).await;
    refused_then_repaired(&rig, outcome, "write-deadline-expired").await;

    let (rig, outcome) = hold_write_until(shape, false, W_WRITE_MILLIS - 1).await;
    adopted_inside_grace(&rig, outcome).await;
}

on_both_shapes!(d7c =>
    d7_c_a_flat_repairs_write_at_its_deadline_is_refused_and_never_adopted,
    d7_c_a_segmented_repairs_write_at_its_deadline_is_refused_and_never_adopted);

/// Server 5 loses the fragment, and the repair is held right after its pre-mark lands while the
/// clock moves to exactly the pre-mark's stamp — the clock's reading the batch was built at —
/// plus `after`. No GC pass runs.
async fn hold_premark_until(shape: Shape, after: u64) -> (Rig, Reconciled) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let hold = Hold::default();
    rig.store.arm(premarks_free, Act::After(hold.clone()));
    let (outcome, reached) = held(&hold, rig.repair(&WITHOUT_SOURCE), async {
        let stamp = rig.premark_stamp(FREE).await;
        rig.clock.set(stamp + after);
    })
    .await;
    assert!(reached, "no pre-mark was committed before the write");
    (rig, outcome)
}

/// **(D7d)** The gate's exact boundary: a pre-mark exactly `W_repoint` old — still inside the
/// write window, so the D server would take the write — authorizes no write, the move aborts and
/// the repair is still owed; one a millisecond younger authorizes the write, which lands and is
/// adopted.
async fn d7d(shape: Shape) {
    let (rig, outcome) = hold_premark_until(shape, W_REPOINT).await;
    assert!(
        rig.fleet.sent_to(FREE).is_empty(),
        "a write was authorized on a pre-mark W_repoint old"
    );
    refused_then_repaired(&rig, outcome, "pre-mark-stale").await;

    let (rig, outcome) = hold_premark_until(shape, W_REPOINT - 1).await;
    assert_eq!(rig.fleet.sent_to(FREE), vec![moved()], "the write");
    adopted_inside_grace(&rig, outcome).await;
}

on_both_shapes!(d7d =>
    d7_d_a_flat_repairs_write_gate_closes_at_exactly_w_repoint,
    d7_d_a_segmented_repairs_write_gate_closes_at_exactly_w_repoint);

// ─────────────────────────────── (D8) ───────────────────────────────

/// **(D8)** Each move's identity is its own. Two repairs of the chunk, B and A, pre-mark in the
/// same millisecond (one clock reading): B is held before its pre-mark commit; A pre-marks,
/// writes, and is held before its adoption; the clock passes the grace and GC reclaims A's
/// destination and its mark; B is released, and its pre-mark re-creates the now-empty key; A is
/// released. A's adoption fails — the key holds B's pre-mark, not its own — and no placement
/// names the fragment GC deleted.
async fn d8(shape: Shape) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let b_premark = Hold::default();
    rig.store.arm(premarks_free, Act::Before(b_premark.clone()));
    let a_adopt = Hold::default();

    let b = Running::new();
    let script = async {
        b.reaches(&b_premark, "B never reached its pre-mark commit")
            .await;
        rig.store.arm(repoints, Act::Before(a_adopt.clone()));
        let a = Running::new();
        let meanwhile = async {
            a.reaches(&a_adopt, "A never reached its adoption").await;
            let a_value = rig.mark(FREE).await.expect("A's pre-mark");
            rig.premark_stamp(FREE).await;
            assert!(rig.fleet.holds(FREE, moved()).is_some(), "A's write");

            reclaim_the_leftover(&rig).await;

            b_premark.release.notify_one();
            b.finished().await;
            let b_value = rig.mark(FREE).await.expect("B's pre-mark");
            assert_eq!(
                decode_orphan_mark(&b_value).unwrap().orphaned_at_millis(),
                decode_orphan_mark(&a_value).unwrap().orphaned_at_millis(),
                "the two pre-marks were stamped in one millisecond"
            );
            assert_ne!(b_value, a_value, "two moves' pre-marks are the same bytes");
            a_adopt.release.notify_one();
        };
        let (a_outcome, ()) = tokio::join!(a.run(rig.repair(&WITHOUT_SOURCE)), meanwhile);
        a_outcome
    };
    let (b_outcome, a_outcome) = tokio::join!(b.run(rig.repair(&WITHOUT_SOURCE)), script);

    assert_eq!(
        b_outcome,
        Reconciled::Blocked,
        "B's stale pre-mark certified"
    );
    assert_eq!(
        a_outcome,
        Reconciled::Blocked,
        "A's lost adoption certified"
    );
    assert!(rig.fleet.holds(FREE, moved()).is_none(), "GC's reclaim");
    rig.assert_not_adopted().await;
    rig.assert_no_stray(FREE).await;
    rig.assert_repaired_later().await;
}

on_both_shapes!(d8 =>
    d8_a_flat_repairs_adoption_is_never_satisfied_by_another_moves_pre_mark,
    d8_a_segmented_repairs_adoption_is_never_satisfied_by_another_moves_pre_mark);

// ─────────────────────────────── (D9) ───────────────────────────────

/// **(D9, landing unverified)** A write whose landing the D server cannot certify is never
/// adopted: held once its bytes are published while the clock reaches its deadline, the D server
/// answers that it cannot say whether it landed in time. Whatever landed is under the pre-mark,
/// and the pass does not certify.
async fn d9_unverified(shape: Shape) {
    let (rig, outcome) = hold_write_until(shape, true, W_WRITE_MILLIS).await;
    assert!(
        rig.fleet.holds(FREE, moved()).is_some(),
        "the uncertified write's bytes are where they landed"
    );
    rig.premark_stamp(FREE).await;
    refused_then_repaired(&rig, outcome, "write-unverified").await;
}

on_both_shapes!(d9_unverified =>
    d9_a_flat_repairs_uncertified_write_is_never_adopted_or_certified,
    d9_a_segmented_repairs_uncertified_write_is_never_adopted_or_certified);

// ─────────────────────────────── (D10) ───────────────────────────────

/// How a (D10a) leg's move stops after its write lands.
#[derive(Clone, Copy)]
enum Interrupt {
    /// The pass dies at its adoption: dropped while held there.
    Crash,
    /// Another writer rewrites the root first (its version bumped, its chunks unchanged), so the
    /// adoption loses — and the position carried a stale mark before the move.
    LostAdoption,
}

/// **(D10a)** Server 2 is down, so the fragment server 5 lost is rebuilt on server 5 itself, and
/// the move stops after its write lands and before its adoption. No mark is left on the position
/// the placement still names; the next pass finds the chunk whole and drains the obligation; and
/// then (D3) holds: a production Complete supersedes the object, and GC reclaims none of its
/// fragments before the drain marks them.
async fn in_place_interrupted(shape: Shape, interrupt: Interrupt) {
    let rig = Rig::new(shape).await;
    rig.lose(SOURCE).await;
    let at = rig.clock.now_millis();
    let lost = match interrupt {
        Interrupt::Crash => {
            let hold = Hold::default();
            rig.store.arm(repoints, Act::Before(hold.clone()));
            let pass = rig.repair(&WITHOUT_FREE);
            tokio::pin!(pass);
            tokio::select! {
                _ = &mut pass => panic!("the repair never reached its adoption"),
                () = hold.reached.notified() => {}
            }
            None
        }
        Interrupt::LostAdoption => {
            rig.plant_mark(SOURCE, STALE).await;
            let mut root = rig.root().await;
            root.version += 1;
            let rewritten = vec![(md::inode_key(INODE), md::encode(&root))];
            rig.store.arm(repoints, Act::PlantFirst(rewritten));
            let outcome = rig.repair(&WITHOUT_FREE).await;
            assert_eq!(rig.root().await.version, root.version, "the racer landed");
            Some(outcome)
        }
    };
    assert!(
        rig.fleet.holds(SOURCE, moved()).is_some(),
        "the in-place write landed"
    );
    assert_eq!(rig.placement().await, vec![3, 4, SOURCE]);
    rig.assert_no_mark_on_placement().await;
    if let Some(outcome) = lost {
        assert_eq!(
            outcome,
            Reconciled::Blocked,
            "a pass certified over an in-place repair whose adoption lost"
        );
    }

    rig.clock.advance(1_000);
    rig.repair(&WITHOUT_FREE).await;
    assert!(!rig.owed().await, "the whole chunk's obligation was kept");
    rig.assert_no_mark_on_placement().await;
    supersede_then_reclaim(&rig, at).await;
}

#[tokio::test]
async fn d10_a_a_flat_in_place_repair_that_crashes_after_its_write_leaves_no_mark() {
    in_place_interrupted(Shape::Flat, Interrupt::Crash).await;
}

#[tokio::test]
async fn d10_a_a_segmented_in_place_repair_that_crashes_after_its_write_leaves_no_mark() {
    in_place_interrupted(Shape::Segmented, Interrupt::Crash).await;
}

#[tokio::test]
async fn d10_a_a_flat_in_place_repair_that_loses_its_adoption_leaves_no_mark() {
    in_place_interrupted(Shape::Flat, Interrupt::LostAdoption).await;
}

#[tokio::test]
async fn d10_a_a_segmented_in_place_repair_that_loses_its_adoption_leaves_no_mark() {
    in_place_interrupted(Shape::Segmented, Interrupt::LostAdoption).await;
}

/// **(D10b)** Server 2 is down, so the fragment server 5 lost is rebuilt in place, and its write
/// is held in flight — after the pre-mark batch. Meanwhile a production Complete supersedes the
/// object, and drain passes mark every position of the superseded generation, server 5's
/// included. The write is then released:
///
/// * a millisecond before its deadline: it lands, the adoption loses to the supersede, and GC
///   reclaims the fragment only once the drain's mark is past its grace;
/// * once the drain's mark is past its grace and GC has swept it, nothing beneath it: the D
///   server refuses the write, and nothing is left on server 5 without a reference or a mark.
async fn d10b(shape: Shape) {
    for late in [false, true] {
        let rig = Rig::new(shape).await;
        rig.lose(SOURCE).await;
        let positions = rig.positions().await;
        let hold = rig.fleet.hold_write(SOURCE, moved());
        let mut marked_at = 0;
        let (outcome, reached) = held(&hold, rig.repair(&WITHOUT_FREE), async {
            let stamp = rig.clock.now_millis();
            rig.clock.advance(1_000);
            let (obligation, event) = rig.supersede().await;
            marked_at = rig.clock.now_millis();
            rig.drain(&obligation, &event, &positions).await;
            if late {
                rig.clock.set(marked_at + GRACE + 1);
                rig.gc_until_quiet(&SERVERS).await;
                assert_eq!(rig.mark(SOURCE).await, None, "GC swept the drain's mark");
            } else {
                rig.clock.set(stamp + W_WRITE_MILLIS - 1);
            }
        })
        .await;
        assert!(reached, "the in-place write was never sent");
        assert!(!rig.owed().await, "the superseded chunk's obligation");

        if late {
            assert!(
                rig.fleet.holds(SOURCE, moved()).is_none(),
                "the D server stored a write past its deadline"
            );
            assert_eq!(outcome, Reconciled::Blocked, "a refused write certified");
            assert_eq!(rig.mark(SOURCE).await, None);
            continue;
        }
        assert!(
            rig.fleet.holds(SOURCE, moved()).is_some(),
            "the in-place write landed before its deadline"
        );
        rig.clock.set(marked_at + GRACE - 1);
        rig.gc_until_quiet(&SERVERS).await;
        assert!(
            rig.fleet.holds(SOURCE, moved()).is_some(),
            "GC reclaimed the in-place write inside the drain's mark's grace"
        );
        rig.clock.set(marked_at + GRACE + 1);
        rig.gc_until_quiet(&SERVERS).await;
        assert!(
            rig.fleet.holds(SOURCE, moved()).is_none(),
            "the in-place write survived the drain's mark's grace"
        );
    }
}

on_both_shapes!(d10b =>
    d10_b_a_flat_in_place_write_after_a_supersede_is_reclaimed_after_the_drains_grace,
    d10_b_a_segmented_in_place_write_after_a_supersede_is_reclaimed_after_the_drains_grace);

// ─────────────────────────────── the swap ───────────────────────────────

/// The swap leg's RS(2,2) chunk, on servers 3, 4, 5 and 6.
fn wide_chunk() -> ChunkRef {
    ChunkRef {
        id: CHUNK,
        scheme: RS22,
        len: CHUNK_LEN,
        placement: vec![3, 4, 5, 6],
    }
}

fn index(index: u16) -> FragmentId {
    FragmentId {
        chunk: CHUNK,
        index,
    }
}

/// **(Swap)** A position a mark rules out rules out only itself, never its server. The RS(2,2)
/// chunk loses fragments 2 and 3 (servers 5 and 6 gone); servers 2 and 7 hold the two free
/// domains, and the selector's own pick gives fragment 2 to server 2, whose position for fragment
/// 2 carries a mark no writer may replace. One pass repairs both — fragment 2 onto server 7,
/// fragment 3 onto server 2 — never writes the ruled-out position, leaves its mark byte-identical
/// and names it on the audit seam. Ruling out server 2 whole would leave one server for two
/// fragments, and an unreadable mark would then block the chunk for good.
async fn swap(shape: Shape) {
    let bad = [
        (reclaiming(), "destination-passed-over", "subject"),
        (
            Bytes::from_static(UNREADABLE),
            "unreadable-destination-mark",
            "mark",
        ),
    ];
    for (mark, action, field) in bad {
        let rig = Rig::moving(shape, wide_chunk()).await;
        rig.plant_mark_at(FREE, index(2), &mark).await;
        repair::enqueue_repair(&rig.store, CHUNK, "scrub")
            .await
            .unwrap();
        let at = rig.clock.now_millis();

        assert_eq!(
            rig.repair(&SWAP_VIEW).await,
            Reconciled::Changed,
            "{action}: one pass repairs both lost fragments"
        );

        assert_eq!(
            rig.placement().await,
            vec![3, 4, 7, FREE],
            "{action}: the free servers swap fragments"
        );
        assert_eq!(
            rig.fleet.sent_to(FREE),
            vec![index(3)],
            "{action}: the ruled-out position was written"
        );
        assert_eq!(
            rig.store.value(&orphan_key(FREE, index(2))).await,
            Some(mark.clone()),
            "{action}: the ruled-out position's mark moved"
        );
        let position = name(&orphan_key(FREE, index(2)));
        assert!(
            rig.audit.names(action, field, &position),
            "{action}: the audit seam does not name {position}"
        );
        assert!(!rig.owed().await, "{action}: the obligation was kept");
        rig.assert_no_mark_on_placement().await;
        for (vacated, frag) in [(5, index(2)), (6, index(3))] {
            assert_eq!(
                rig.store.value(&orphan_key(vacated, frag)).await,
                Some(legacy(at)),
                "{action}: the vacated position on {vacated}"
            );
        }
        rig.assert_complete().await;
    }
}

on_both_shapes!(swap =>
    a_ruled_out_position_rules_out_only_itself_in_a_flat_repair,
    a_ruled_out_position_rules_out_only_itself_in_a_segmented_repair);

/// **(Mixed answers)** A move's writes are judged together. The RS(2,2) chunk loses fragments 2
/// and 3, and the repair sends both rebuilt fragments at once: fragment 2 to server 2, which
/// lands, and fragment 3 to server 7, held in flight until its deadline and refused. One refused
/// write refuses the whole move: nothing is adopted, the fragment that landed stays under its
/// pre-mark, the obligation is kept and the pass does not certify. A later pass repairs both.
async fn mixed(shape: Shape) {
    let rig = Rig::moving(shape, wide_chunk()).await;
    repair::enqueue_repair(&rig.store, CHUNK, "scrub")
        .await
        .unwrap();
    let hold = rig.fleet.hold_write(7, index(3));

    let (outcome, reached) = held(&hold, rig.repair(&SWAP_VIEW), async {
        let stamp = rig.premark_stamp(FREE).await;
        rig.clock.set(stamp + W_WRITE_MILLIS);
    })
    .await;

    assert!(reached, "the move never sent fragment 3's write");
    assert!(
        rig.fleet.holds(FREE, index(2)).is_some(),
        "fragment 2's write did not land"
    );
    assert!(
        rig.fleet.holds(7, index(3)).is_none(),
        "the D server stored a write at its deadline"
    );
    assert_eq!(
        rig.placement().await,
        wide_chunk().placement,
        "a move with one refused write adopted"
    );
    assert!(rig.owed().await, "the obligation was dropped");
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a move with one refused write certified"
    );
    rig.audit.names_refusal("aborted", "write-deadline-expired");
    rig.premark_stamp(FREE).await;
    rig.assert_no_stray(FREE).await;

    assert_eq!(rig.repair(&SWAP_VIEW).await, Reconciled::Changed);
    assert_eq!(rig.placement().await, vec![3, 4, FREE, 7]);
    assert!(!rig.owed().await, "the obligation was kept");
    rig.assert_no_mark_on_placement().await;
    rig.assert_complete().await;
}

on_both_shapes!(mixed =>
    one_refused_write_refuses_a_whole_flat_repair,
    one_refused_write_refuses_a_whole_segmented_repair);
