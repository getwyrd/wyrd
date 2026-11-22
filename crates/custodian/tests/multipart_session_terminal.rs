//! Issue #1013 — a multipart session that has left `Open` ends: the custodian walks its owned
//! `sidx:` residue (marks every position, then deletes the entry), retires an `Aborting` session's
//! untrusted records **by chunk id** on passes that see the whole fleet, and then deletes the
//! session in one batch that releases its `mpuctl` slot exactly once (proposal 0016's
//! *Owned-`sidx:` drain step* and *Terminal session delete* rows, `0016:672-674`; decision 5's
//! backstop, `0016:1654-1668`; the terminal-delete rule, `0016:582-602`; the tombstone rule,
//! `0016:574`).
//!
//! Driven **only** through `reconcile_step` with the contexts the deployed loop passes
//! (`crates/server/src/custodian.rs`, `run_reconstruction_until`): a reconstruction pass, every
//! other input `None`, its `now_millis` and its context's clock one source, its fleet the live
//! one, `operator_fleet_size` the configured count and the tombstone input `Retain`; GC a pass of
//! its own over the whole fleet. Every pass builds its leadership fence and context afresh. The
//! metadata is a real backend (`RedbMetadataStore::in_memory()`) behind a recording wrapper; the
//! D servers a placement-aware double whose listings are counted. Sessions are made by the
//! production Create, fenced by the production Abort, published by the production Complete.
//!
//! * **(D)** An `Aborting` session with committed parts, crashed in-flight residue and a slot it
//!   never released drains to: every residue position marked with the walk's `<upload-id>:<epoch>`
//!   event in a batch before the one deleting its entry, no `sidx:` or `slot:` record left, the
//!   `seggrp:` marker gone, the session gone in one committed batch that comes after the last
//!   entry's and the obligation's deletes, and `mpuctl.count` one lower — and no lower after more
//!   passes. Past the grace every fragment is gone. A second `Aborting` session whose nonce a
//!   `seg:` record names keeps its marker.
//! * **(D, `Completed`)** A `Completed` session with in-flight residue, drained with the deployed
//!   contexts: the residue is walked away, its obligations drain, and the session record (the
//!   tombstone) stays with its slot counted; an identical Complete retry answers
//!   `AlreadyCompleted`. A pass whose tombstone input says the window has passed deletes it once.
//! * **(D, the gate)** A `Completed` session past its window whose residue the walk holds (a mark
//!   of none of the three shapes over it) is not deleted while that entry stands; once the mark is
//!   repaired it is. An `Aborting` session whose obligation is held (the undecodable `part:`
//!   control below) is not deleted either.
//! * **(D, race)** A wrapper runs a second drainer to completion between the first's read of the
//!   session and its read of `mpuctl`: the first's terminal batch is refused, and the count goes
//!   down by exactly one.
//! * **(E1, E2, E3)** For an `Aborting` session, on a whole-fleet pass: a `part:` record whose
//!   RS(4,2) chunk is placed on five servers, an owned entry with that placement, and an owned value
//!   that will not decode (an unsupported scheme), its chunk taken from its key. For each, every
//!   fragment bearing the chunk id that the fleet's listings find is marked — for E2 every position
//!   the entry names as well — in a batch before the one deleting the record; past the grace those
//!   fragments are gone, a committed object's fragments on the same servers stay, and the terminal
//!   delete has fired. **Late landing:** a wrapper publishes a fragment right after the record's
//!   deletion, after the listing. For E2, at a position the entry names: it finds the walk's mark
//!   and is gone after the grace. For E3: it stays on disk across GC passes, none failing — the
//!   stated residual.
//! * **(E, controls)** An `Open` session's records of the same kinds, and every fragment of their
//!   chunks, stay; so do a `Completed` session's untrusted owned entries, past its tombstone
//!   window, each held. A server left out at startup and a server unreachable this pass each leave
//!   the records and fragments in place with a `partial-fleet` hold; once the fleet is whole they
//!   retire, and that server's fragments are gone after the grace too. A `part:` value that will
//!   not decode is held as `record-unreadable`, audited as needing a human, and the session stays.
//! * **(Past one step)** Work larger than one step's reads (`MARKS_PER_STEP`) still ends, over
//!   repeated passes: an owned entry naming 256 positions and a damaged one naming 300 resume from
//!   the walk's bookmark; a `part:` record of 79 chunks, 40 of them untrusted, resumes from its
//!   obligation's cursor. A chunk listed with more fragments than one step reads
//!   (`LISTED_PER_CHUNK`) is held as `listing-past-step`, and one listed with exactly that many
//!   retires. Twenty sessions — past one listing page (`SESSION_PAGE`) and past what one pass's
//!   steps (`SESSION_STEPS_PER_PASS`) can end — all end, no pass committing more than its steps.
//! * **(Held ahead)** A wide entry behind one the walk holds — an `Aborting` session's on a short
//!   fleet, a `Completed` session's past its window, or one held part-way, past one step's reads —
//!   retires while the held one stays: the bookmark stays on the entry that advances, and the entry
//!   it names is walked first.
//! * **(The bookmark lost)** With every position already marked and the bookmark gone, the walk
//!   re-reads, marks nothing, keeps its place at the literal key `custodian:owned-walk:<upload-id>`
//!   and ends the session; a bookmark naming another session's entry is audited and read as absent.
//! * **(Past held entries)** A trusted entry behind a whole page of held ones — sixteen, an
//!   `Aborting` session's on a short fleet or a `Completed` session's past its window — is marked
//!   and deleted while they stay: the walk keeps where its next page starts at the literal key
//!   `custodian:owned-scan:<upload-id>`, and is back at the head once nothing follows. So is one
//!   behind three entries held part-way whose re-reads alone outrun a step. An undecodable entry
//!   listed with `LISTED_PER_CHUNK` fragments, behind two held entries, retires: the bookmark
//!   names it at a count of zero and it is walked first. The bookmark of an entry held on another
//!   page survives the lap; one naming an entry that is gone does not. A position naming nothing
//!   of its range is audited and deleted, one left over an empty range is deleted, the terminal
//!   delete removes one written under it, and none is put once the session record has moved.
//! * **(The walk's own records and the pass)** A step that marks and deletes nothing is not one of
//!   the pass's steps, and ends its session's visit. Sixteen sessions whose held entries are
//!   re-read every pass — three held part-way each, or two held past one step's reads on two pages,
//!   whose one bookmark moves every pass — spend none: eight sessions listed after them, aborted
//!   before a pass, each have their residue marked and deleted in that one pass, with every step it
//!   has, round after round. Three entries held part-way leave the bookmark at rest: a pass over
//!   them commits the scan position alone. A bookmark is put only while the session record is the
//!   one read: a wrapper holds a drainer's bookmark batch back until a second drainer has ended
//!   the session, and the store refuses it — no bookmark is left behind a session that is gone.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::Dispatch;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self as md, decode_orphan_mark, orphan_key, seg_key, seggrp_key, ChunkRef, EcScheme, InodeId,
    OrphanMark, SegmentGroup, SegmentNonce, SegmentRecord,
};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::complete::{
    complete_upload, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    self as mp, decode_admission_record, decode_owned_entry, decode_part_record,
    decode_part_summary, decode_session_record, decode_slot_record, mpu_key, parse_sidx_key,
    part_key, psum_key, retire_key, sidx_key, AbortOutcome, Budget, CompleteOutcome, CreateOutcome,
    Digest, OwnedEntry, PartNumber, PartSummary, RetireMode, RetireToken, StagedPlacement,
    UploadId, MPUCTL_KEY,
};
use wyrd_core::placement::Topology;
use wyrd_custodian::gc::W_WRITE_MILLIS;
use wyrd_custodian::retirement::{LEDGER_CURSOR_KEY, LISTED_PER_CHUNK, MARKS_PER_STEP};
use wyrd_custodian::{
    reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext, ReconcileError,
    Reconciled, ReconstructionContext, Tombstone,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    Result as StoreResult, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

/// Where every rig's clock starts.
const T0: u64 = 10_000_000;
/// The orphan grace every GC pass here honours.
const GRACE: u64 = 60_000;
/// The late-write deadline GC's sweep of fragment-less marks waits out, stated here.
const LATE_WRITE: u64 = 41_000;
/// The audit target the retirement leg names a hold on.
const AUDIT_TARGET: &str = "wyrd.custodian.retirement.audit";
/// The target it reports a wait on.
const WAIT_TARGET: &str = "wyrd.custodian.retirement";
const BUCKET: &str = "photos";
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const COMMITTED_AT: u64 = 1_500;
/// Bytes per chunk of a planted part.
const CHUNK_LEN: u64 = 64;
/// Two data fragments and one parity: three positions a chunk.
const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
/// Four data fragments and two parity: six positions a chunk.
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };
/// A supported scheme with more positions a chunk (256) than one step reads.
const RS128: EcScheme = EcScheme::ReedSolomon { k: 128, m: 128 };

fn upload(n: u64) -> UploadId {
    UploadId::new(format!("{n:032x}")).unwrap()
}

fn nonce(n: u64) -> SegmentNonce {
    SegmentNonce::new(format!("{:032x}", 0x5e60_0000 + n)).unwrap()
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x3c; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A chunk of `scheme`, `CHUNK_LEN` bytes, at `placement`.
fn chunk(id: ChunkId, scheme: EcScheme, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme,
        len: CHUNK_LEN,
        placement,
    }
}

/// Every fragment position `chunks` names, in order.
fn positions(chunks: &[ChunkRef]) -> Vec<(DServerId, FragmentId)> {
    chunks
        .iter()
        .flat_map(|c| {
            c.fragments()
                .map(|(index, dserver)| (dserver, frag(c.id, index)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Fragment `i` of `chunk` on server `i`, for every `i` of `servers`.
fn diagonal(chunk: ChunkId, servers: &[u16]) -> Vec<(DServerId, FragmentId)> {
    servers
        .iter()
        .map(|&i| (DServerId::from(i), frag(chunk, i)))
        .collect()
}

/// The `part:` value of a part over `chunks` with `digest`, in the codec's own spelling — not
/// checked here, so a value the decoder refuses can be planted too.
fn raw_part_value(chunks: &[ChunkRef], digest: Digest) -> Bytes {
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    Bytes::from(format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{COMMITTED_AT},\"session_epoch\":0}}",
        String::from_utf8(md::encode(&chunks).to_vec()).unwrap()
    ))
}

/// The puts planting part `n` of `id` over `chunks`: its `part:` record — checked to decode —
/// and its `psum:` summary.
fn plant_part(id: &UploadId, n: u32, chunks: &[ChunkRef]) -> Vec<(Vec<u8>, Bytes)> {
    let digest = digest_of(n);
    let value = raw_part_value(chunks, digest);
    let decoded = decode_part_record(&value).expect("a canonical part");
    assert_eq!(md::encode(&decoded), value);
    let summary = PartSummary {
        chunks: chunks.len() as u32,
        len: chunks.iter().map(|c| c.len).sum(),
        digest,
        committed_at_millis: COMMITTED_AT,
    };
    let psum = md::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    vec![
        (part_key(id, part(n)), value),
        (psum_key(id, part(n)), psum),
    ]
}

/// The owned entry of `chunk`, staged by part `n` of `id` at `placement` under `scheme`: its key
/// and its value, checked to decode.
fn owned(
    id: &UploadId,
    n: u32,
    chunk: ChunkId,
    scheme: EcScheme,
    placement: Vec<DServerId>,
) -> (Vec<u8>, Bytes) {
    let staged = StagedPlacement::new(scheme, placement).unwrap();
    let entry = OwnedEntry::new(id.clone(), T0 + 10_000, staged).to_pending();
    let key = sidx_key(id, part(n), chunk);
    let value = md::encode(&entry);
    decode_owned_entry(&key, &value).expect("an owned entry");
    (key, value)
}

/// A record's key and value.
type Record = (Vec<u8>, Bytes);

/// The owned entry of `chunk`, staged by part `n` of `id` under RS(128,128) — 256 positions, more
/// than one walk step reads — round servers 0–5: its key and value, and every position it names.
fn wide(id: &UploadId, n: u32, chunk: ChunkId) -> (Record, Vec<(DServerId, FragmentId)>) {
    let placement: Vec<DServerId> = (0..256).map(|i| i % 6).collect();
    let at = (0..256_u16)
        .map(|i| (placement[usize::from(i)], frag(chunk, i)))
        .collect();
    (owned(id, n, chunk, RS128, placement), at)
}

/// Where the owned walk of `id` keeps its place, spelled out rather than built by the crate under
/// test: `custodian:owned-walk:<upload-id>`.
fn walk_bookmark(id: &UploadId) -> Vec<u8> {
    format!("custodian:owned-walk:{id}").into_bytes()
}

/// Where the owned walk of `id` keeps the start of its next page, spelled out rather than built by
/// the crate under test: `custodian:owned-scan:<upload-id>`. Its value is the key of the last entry
/// the walk passed.
fn walk_position(id: &UploadId) -> Vec<u8> {
    format!("custodian:owned-scan:{id}").into_bytes()
}

/// The bookmark's value for `marked` positions of the entry at `entry`: its key, then the count as
/// four big-endian bytes.
fn bookmark_value(entry: &[u8], marked: usize) -> Bytes {
    let mut value = entry.to_vec();
    value.extend_from_slice(&u32::try_from(marked).unwrap().to_be_bytes());
    Bytes::from(value)
}

/// The slot record a crashed attempt of part `n` left at index 0 of `id`, checked to decode.
fn slot(id: &UploadId, n: u32) -> (Vec<u8>, Bytes) {
    let value = format!(
        "{{\"part_number\":{n},\"attempt_id\":\"{:032x}\",\"reserved_at_millis\":{T0},\
         \"lease_expiry_millis\":{}}}",
        0xa77e_0000_u64 + u64::from(n),
        T0 + 10_000
    );
    decode_slot_record(value.as_bytes()).expect("a slot record");
    let key = format!("slot:{id}:000000").into_bytes();
    (key, Bytes::from(value))
}

// ─────────────────────────── the recording store ───────────────────────────

/// A second drainer, run to completion over the inner store the first time the wrapped store is
/// asked for `mpuctl`.
struct Rival {
    fleet: Fleet,
    clock: ManualClock,
    ran: Arc<AtomicBool>,
}

/// A fragment published right after the batch deleting `deleted` commits.
struct Late {
    deleted: Vec<u8>,
    fleet: Fleet,
    at: (DServerId, FragmentId),
    /// The fleet's listing count when it was published.
    listings_before: Arc<AtomicU64>,
}

/// A commit held back while a second drainer finishes: the first batch sent that puts `put` waits
/// while `rival` runs whole passes over the inner store until the session at `session` is gone,
/// and only then reaches the store.
struct Overtaken {
    put: Vec<u8>,
    session: Vec<u8>,
    rival: Rival,
}

/// `RedbMetadataStore` behind a wrapper that records every batch sent, with what the store
/// answered, and can run one [`Rival`], one [`Late`] and one [`Overtaken`].
struct Store {
    inner: RedbMetadataStore,
    sent: Mutex<Vec<(WriteBatch, Option<CommitOutcome>)>>,
    rival: Mutex<Option<Rival>>,
    late: Mutex<Option<Late>>,
    overtaken: Mutex<Option<Overtaken>>,
    /// A session Create run over the inner store — moving the `mpuctl` ledger — the first time the
    /// wrapped store is asked for `mpuctl`, right after that read: its number.
    creator: Mutex<Option<u64>>,
    /// Another writer's batch, committed to the inner store the first time the wrapped store is
    /// asked for this key, right after that read.
    meanwhile: Mutex<Option<(Vec<u8>, WriteBatch)>>,
}

impl Store {
    fn new() -> Self {
        Self {
            inner: RedbMetadataStore::in_memory().unwrap(),
            sent: Mutex::new(Vec::new()),
            rival: Mutex::new(None),
            late: Mutex::new(None),
            overtaken: Mutex::new(None),
            creator: Mutex::new(None),
            meanwhile: Mutex::new(None),
        }
    }

    /// Fixture writes, straight to the inner store.
    async fn plant(&self, puts: Vec<(Vec<u8>, Bytes)>) {
        let mut batch = WriteBatch::new();
        batch.puts.extend(puts);
        assert_eq!(
            self.inner.commit(batch).await.unwrap(),
            CommitOutcome::Committed
        );
    }

    /// A fixture delete, straight to the inner store.
    async fn remove(&self, key: &[u8]) {
        let batch = WriteBatch::new().delete(key.to_vec());
        assert_eq!(
            self.inner.commit(batch).await.unwrap(),
            CommitOutcome::Committed
        );
    }

    async fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.get(key).await.unwrap()
    }

    async fn under(&self, prefix: &[u8]) -> BTreeMap<Vec<u8>, Bytes> {
        let (page, next) = self.inner.scan_page(prefix, None, 10_000).await.unwrap();
        assert!(next.is_none(), "a fixture range fits one page");
        page.into_iter().collect()
    }

    /// Every batch sent since the last call, with what the store answered (`None`: an error).
    fn take_sent(&self) -> Vec<(WriteBatch, Option<CommitOutcome>)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

#[async_trait]
impl MetadataStore for Store {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let meanwhile = {
            let mut armed = self.meanwhile.lock().unwrap();
            if armed.as_ref().is_some_and(|(at, _)| at == key) {
                armed.take()
            } else {
                None
            }
        };
        if let Some((_, batch)) = meanwhile {
            // The reader gets the value as it was, and the other writer lands under it.
            let read = self.inner.get(key).await;
            assert_eq!(
                self.inner.commit(batch).await.unwrap(),
                CommitOutcome::Committed
            );
            return read;
        }
        if key == MPUCTL_KEY {
            let rival = self.rival.lock().unwrap().take();
            if let Some(rival) = rival {
                let view = View::deployed();
                let outcome = pass(&self.inner, &rival.fleet, &rival.clock, &view, None).await;
                assert!(outcome.is_ok(), "the rival's pass: {outcome:?}");
                rival.ran.store(true, Ordering::SeqCst);
            }
            let creator = self.creator.lock().unwrap().take();
            if let Some(n) = creator {
                // The reader gets the ledger as it was, and the Create moves it under it.
                let read = self.inner.get(key).await;
                create(&self.inner, n, &format!("{BUCKET}/created-{n}")).await;
                return read;
            }
        }
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
        let overtaken = {
            let mut armed = self.overtaken.lock().unwrap();
            let puts = |o: &Overtaken| batch.puts.iter().any(|(key, _)| *key == o.put);
            if armed.as_ref().is_some_and(puts) {
                armed.take()
            } else {
                None
            }
        };
        if let Some(Overtaken { session, rival, .. }) = overtaken {
            // The batch is on its way; the second drainer finishes before it lands.
            let view = View::deployed();
            for _ in 0..8 {
                if self.inner.get(&session).await?.is_none() {
                    break;
                }
                let outcome = pass(&self.inner, &rival.fleet, &rival.clock, &view, None).await;
                assert!(outcome.is_ok(), "the rival's pass: {outcome:?}");
            }
            assert_eq!(self.inner.get(&session).await?, None, "the rival ended it");
            rival.ran.store(true, Ordering::SeqCst);
        }
        let outcome = self.inner.commit(batch.clone()).await;
        let answered = outcome.as_ref().ok().copied();
        let deletes = batch.deletes.clone();
        self.sent.lock().unwrap().push((batch, answered));
        if answered == Some(CommitOutcome::Committed) {
            let mut late = self.late.lock().unwrap();
            if late.as_ref().is_some_and(|l| deletes.contains(&l.deleted)) {
                let late = late.take().unwrap();
                late.listings_before
                    .store(late.fleet.listings(), Ordering::SeqCst);
                let (dserver, at) = late.at;
                late.fleet.put(dserver, at, Bytes::from_static(b"late"));
            }
        }
        outcome
    }
}

// ─────────────────────────────── the D servers ───────────────────────────────

/// In-process D servers addressed by id, and how many listings were taken of them.
#[derive(Clone, Default)]
struct Fleet {
    servers: Arc<Mutex<BTreeMap<DServerId, HashMap<FragmentId, Bytes>>>>,
    listings: Arc<AtomicU64>,
}

impl Fleet {
    fn serving(ids: impl IntoIterator<Item = DServerId>) -> Self {
        let servers = ids.into_iter().map(|id| (id, HashMap::new())).collect();
        Self {
            servers: Arc::new(Mutex::new(servers)),
            listings: Arc::default(),
        }
    }

    fn put(&self, dserver: DServerId, frag: FragmentId, bytes: Bytes) {
        let mut servers = self.servers.lock().unwrap();
        servers
            .get_mut(&dserver)
            .expect("a server")
            .insert(frag, bytes);
    }

    fn holds(&self, dserver: DServerId, frag: FragmentId) -> bool {
        let servers = self.servers.lock().unwrap();
        servers
            .get(&dserver)
            .is_some_and(|held| held.contains_key(&frag))
    }

    fn store_at(&self, at: &[(DServerId, FragmentId)]) {
        for &(dserver, frag) in at {
            self.put(dserver, frag, Bytes::from(format!("{frag:?}")));
        }
    }

    fn ids(&self) -> Vec<DServerId> {
        self.servers.lock().unwrap().keys().copied().collect()
    }

    fn listings(&self) -> u64 {
        self.listings.load(Ordering::SeqCst)
    }
}

/// One D server of a [`Fleet`], as a pass's fleet lists it.
struct Server {
    id: DServerId,
    fleet: Fleet,
}

#[async_trait]
impl ChunkStore for Server {
    async fn put_fragment(&self, id: FragmentId, bytes: Bytes, _: Option<u64>) -> StoreResult<()> {
        self.fleet.put(self.id, id, bytes);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
        let servers = self.fleet.servers.lock().unwrap();
        Ok(servers
            .get(&self.id)
            .and_then(|held| held.get(&id).cloned()))
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        self.fleet.listings.fetch_add(1, Ordering::SeqCst);
        let servers = self.fleet.servers.lock().unwrap();
        Ok(servers
            .get(&self.id)
            .map(|held| held.keys().copied().collect())
            .unwrap_or_default())
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        let mut servers = self.fleet.servers.lock().unwrap();
        if let Some(held) = servers.get_mut(&self.id) {
            held.remove(&id);
        }
        Ok(())
    }

    async fn health(&self) -> StoreResult<Health> {
        Ok(Health::Healthy)
    }
}

// ─────────────────────────────── the audit seam ───────────────────────────────

/// The retirement leg's audit events and waits, captured by a subscriber attached to each pass's
/// future.
#[derive(Clone, Default)]
struct Audit {
    events: Arc<Mutex<Vec<HashMap<String, String>>>>,
    waits: Arc<Mutex<Vec<(String, String)>>>,
    /// Every `monotonic_counter.*` field any event carried, summed by name.
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldText(HashMap::new());
        event.record(&mut fields);
        let target = event.metadata().target();
        for (field, value) in &fields.0 {
            if let Some(counter) = field.strip_prefix("monotonic_counter.") {
                let by: u64 = value.parse().expect("a counter counts");
                *self
                    .counters
                    .lock()
                    .unwrap()
                    .entry(counter.to_owned())
                    .or_default() += by;
            }
        }
        if target == AUDIT_TARGET {
            self.events.lock().unwrap().push(fields.0);
        } else if target == WAIT_TARGET {
            if let (Some(key), Some(wait)) = (fields.0.get("key"), fields.0.get("wait")) {
                self.waits.lock().unwrap().push((key.clone(), wait.clone()));
            }
        }
    }
}

struct FieldText(HashMap<String, String>);

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl Audit {
    fn dispatch(&self) -> Dispatch {
        Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    /// The audit events of `action` naming `key`; empties the capture.
    fn take(&self, action: &str, key: &[u8]) -> Vec<HashMap<String, String>> {
        let key = name(key);
        let mut events = self.events.lock().unwrap();
        let named = events
            .iter()
            .filter(|e| e.get("action").map(String::as_str) == Some(action) && e["key"] == key)
            .cloned()
            .collect();
        events.clear();
        named
    }

    /// The holds naming the obligation at `key`; empties the capture.
    fn obligation_holds(&self, key: &[u8]) -> Vec<HashMap<String, String>> {
        self.take("retire-obligation-held", key)
    }

    /// The holds naming the session at `key`; empties the capture.
    fn session_holds(&self, key: &[u8]) -> Vec<HashMap<String, String>> {
        self.take("retire-session-held", key)
    }

    fn is_empty(&self) -> bool {
        self.events.lock().unwrap().is_empty()
    }

    /// The sum of the counter `name` over every event so far.
    fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or_default()
    }

    /// Whether a wait of `reason` named the record at `key`.
    fn waited(&self, key: &[u8], reason: &str) -> bool {
        let key = name(key);
        self.waits
            .lock()
            .unwrap()
            .iter()
            .any(|(k, w)| *k == key && w == reason)
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
        self.waits.lock().unwrap().clear();
    }
}

// ─────────────────────────────── the passes ───────────────────────────────

/// What one reconstruction pass sees: which configured servers are missing from its fleet, which
/// of those it dropped as unreachable, and the tombstone input.
struct View {
    missing: Vec<DServerId>,
    unreachable: Vec<DServerId>,
    tombstone: Tombstone,
}

impl View {
    /// What the deployed loop passes with every configured server reachable: the whole fleet,
    /// nothing unreachable, the tombstone retained (#625 not landed).
    fn deployed() -> Self {
        Self {
            missing: Vec::new(),
            unreachable: Vec::new(),
            tombstone: Tombstone::Retain,
        }
    }

    /// The deployed view, with the tombstone window passed.
    fn window_passed() -> Self {
        Self {
            tombstone: Tombstone::Elapsed,
            ..Self::deployed()
        }
    }

    /// A pass whose fleet lacks `missing`: left out at startup (never connected, so not
    /// unreachable either) or, when `dropped`, unreachable this pass.
    fn without(missing: DServerId, dropped: bool) -> Self {
        Self {
            missing: vec![missing],
            unreachable: if dropped { vec![missing] } else { Vec::new() },
            tombstone: Tombstone::Retain,
        }
    }
}

/// One fenced reconstruction pass as the deployed loop runs it, over `meta`: a fresh leadership
/// term and context, its fleet the configured servers `view` presents, `operator_fleet_size` the
/// configured count, `now_millis` and the context's clock read from the one clock.
async fn pass(
    meta: &dyn MetadataStore,
    fleet: &Fleet,
    clock: &ManualClock,
    view: &View,
    audit: Option<&Audit>,
) -> Result<Reconciled, ReconcileError> {
    let (zone, custodian) = elect().await;
    let configured = fleet.ids();
    let servers: Vec<Server> = configured
        .iter()
        .copied()
        .filter(|id| !view.missing.contains(id))
        .map(|id| Server {
            id,
            fleet: fleet.clone(),
        })
        .collect();
    let live: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .map(|s| (s.id, s as &dyn ChunkStore))
        .collect();
    let topology = Topology::default();
    let ctx = ReconstructionContext {
        meta,
        fleet: &live,
        topology: &topology,
        unreachable: &view.unreachable,
        clock,
        staged_write_window_millis: W_WRITE_MILLIS,
        operator_fleet_size: configured.len(),
        tombstone: view.tombstone,
    };
    let now = clock.now_millis();
    let step = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, now);
    match audit {
        Some(audit) => step.with_subscriber(audit.dispatch()).await,
        None => step.await,
    }
}

async fn elect() -> (FencedZone, Custodian) {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-session-terminal")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    (zone, custodian)
}

// ─────────────────────────────── the rig ───────────────────────────────

/// One store, one fleet, one clock — the deployed loop's one time source — and the audit seam.
struct Rig {
    store: Store,
    fleet: Fleet,
    clock: ManualClock,
    audit: Audit,
}

/// Create's runtime: one fixed identity, no jitter, no wait.
struct Fixed(Option<(UploadId, SegmentNonce)>);

impl CreateRuntime for Fixed {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

/// The production Create over `meta` of a session numbered `n`, targeting `object`.
async fn create(meta: &RedbMetadataStore, n: u64, object: &str) -> UploadId {
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object,
        content_type: None,
        created_at_millis: 1_000,
        clock_source: "wall",
    };
    let created = create_session(
        meta,
        &profile(),
        &request,
        &mut Fixed(Some((upload(n), nonce(n)))),
    )
    .await
    .unwrap();
    let CreateOutcome::Created { upload_id } = created else {
        panic!("expected Created, got {created:?}");
    };
    upload_id
}

/// A profile admitting sixty-four sessions (`w_ref / U_ref` = 128 / 2); Complete reads no other
/// cap of it.
fn profile() -> Budget {
    let json = "{\"w_ref\":128,\"max_part_chunks\":1,\"max_parts_per_session\":1,\
                \"max_inflight_parts\":1,\"max_staged_chunks\":1}";
    md::decode(json.as_bytes()).expect("a legal profile")
}

impl Rig {
    /// A rig over D servers `0..servers`, the bucket in place.
    async fn new(servers: u64) -> Self {
        let rig = Self {
            store: Store::new(),
            fleet: Fleet::serving(0..servers),
            clock: ManualClock::new(T0),
            audit: Audit::default(),
        };
        let bucket = vec![(md::bucket_key(BUCKET), Bytes::from_static(MARKER))];
        rig.store.plant(bucket).await;
        rig
    }

    /// One reconstruction pass with the contexts the deployed loop passes.
    async fn drain(&self) -> Result<Reconciled, ReconcileError> {
        self.drain_seeing(&View::deployed()).await
    }

    async fn drain_seeing(&self, view: &View) -> Result<Reconciled, ReconcileError> {
        pass(
            &self.store,
            &self.fleet,
            &self.clock,
            view,
            Some(&self.audit),
        )
        .await
    }

    /// Deployed passes until the session `id` is gone, at most `max` of them; none fails or holds.
    async fn drain_until_ended(&self, id: &UploadId, max: usize) {
        let mut passes = 0;
        while self.store.value(&mpu_key(id)).await.is_some() {
            assert!(passes < max, "{id} ends within {max} passes");
            let outcome = self.drain().await.expect("the pass never fails");
            assert_ne!(outcome, Reconciled::Blocked, "pass {passes}");
            passes += 1;
        }
        assert!(self.audit.is_empty(), "nothing held");
    }

    /// One fenced GC pass over the whole fleet, as the deployed loop runs it.
    async fn gc(&self) -> Reconciled {
        let (zone, custodian) = elect().await;
        let servers: Vec<Server> = self
            .fleet
            .ids()
            .into_iter()
            .map(|id| Server {
                id,
                fleet: self.fleet.clone(),
            })
            .collect();
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
        reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, now)
            .await
            .expect("the GC pass never fails")
    }

    /// Move past the grace of every mark written so far, and run GC passes until one reclaims
    /// nothing more.
    async fn reclaim_past_grace(&self) {
        self.clock.advance(GRACE + 1);
        for _ in 0..4 {
            if self.gc().await != Reconciled::Changed {
                return;
            }
        }
    }

    /// A session made by Create, numbered `n`, targeting `object`.
    async fn session(&self, n: u64, object: &str) -> UploadId {
        create(&self.store.inner, n, object).await
    }

    /// The client Abort of `id`; it must fence.
    async fn abort(&self, id: &UploadId) {
        let aborted = abort_session(&self.store.inner, id).await;
        assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    }

    /// Plant `parts` of `id` (each a part number and its chunks), their fragments on the fleet.
    async fn plant_parts(&self, id: &UploadId, parts: &[(u32, Vec<ChunkRef>)]) {
        let mut puts = Vec::new();
        for (n, chunks) in parts {
            puts.extend(plant_part(id, *n, chunks));
            self.fleet.store_at(&positions(chunks));
        }
        self.store.plant(puts).await;
    }

    /// The production Complete of `id` at `object`, naming `named`, publishing under `inode`.
    async fn complete(
        &self,
        id: &UploadId,
        object: &str,
        named: &[u32],
        inode: InodeId,
    ) -> CompleteOutcome {
        let list = NamedParts::new(named.iter().map(|&n| (part(n), digest_of(n))).collect());
        let request = CompleteRequest {
            bucket: BUCKET,
            parent: ROOT,
            object,
            parts: &list.unwrap(),
            fenced_at_millis: 2_000,
            clock_source: "wall",
        };
        let publish = PublishRequest {
            fresh_inode: inode,
            completed_at_millis: 3_000,
        };
        complete_upload(&self.store.inner, id, &request, &publish)
            .await
            .expect("the Complete answers")
    }

    /// A committed object published by session `n` at `inode`: one RS(2,1) chunk on servers 0–2
    /// with its fragments. Its positions.
    async fn committed_object(&self, n: u64, inode: InodeId) -> Vec<(DServerId, FragmentId)> {
        let object = format!("{BUCKET}/committed-{n}");
        let id = self.session(n, &object).await;
        let chunks = vec![chunk(ChunkId::from(n) * 1_000, RS21, vec![0, 1, 2])];
        self.plant_parts(&id, &[(1, chunks.clone())]).await;
        let outcome = self.complete(&id, &object, &[1], inode).await;
        assert!(
            matches!(outcome, CompleteOutcome::Published(_)),
            "{outcome:?}"
        );
        positions(&chunks)
    }

    /// The `mpuctl` count.
    async fn sessions_counted(&self) -> u64 {
        let ledger = self.store.value(MPUCTL_KEY).await.expect("the ledger");
        decode_admission_record(&ledger).unwrap().count()
    }

    /// The epoch of `id`'s session record.
    async fn epoch_of(&self, id: &UploadId) -> u64 {
        let record = self.store.value(&mpu_key(id)).await.expect("the session");
        decode_session_record(&record).unwrap().epoch()
    }

    /// The decoded `orphan:` mark at `position`, if any.
    async fn mark_at(&self, position: (DServerId, FragmentId)) -> Option<OrphanMark> {
        let stored = self
            .store
            .value(&orphan_key(position.0, position.1))
            .await?;
        Some(decode_orphan_mark(&stored).expect("a mark of one of the three shapes"))
    }

    /// Every position of `positions` carries a structured mark of `event`.
    async fn assert_marked(&self, positions: &[(DServerId, FragmentId)], event: &str) {
        for &position in positions {
            let mark = self.mark_at(position).await;
            let mark = mark.unwrap_or_else(|| panic!("{position:?} is unmarked"));
            assert_eq!(mark.event(), Some(event), "{position:?}");
        }
    }

    /// No position bearing `chunk` carries a mark.
    async fn assert_unmarked(&self, chunk: ChunkId) {
        for key in self.store.under(b"orphan:").await.keys() {
            let (_, frag) = md::parse_orphan_key(key).expect("a mark key");
            assert_ne!(frag.chunk, chunk, "{} marks the held chunk", name(key));
        }
    }

    fn assert_gone(&self, positions: &[(DServerId, FragmentId)]) {
        for &(dserver, frag) in positions {
            assert!(
                !self.fleet.holds(dserver, frag),
                "{frag:?} on {dserver} survived"
            );
        }
    }

    fn assert_kept(&self, positions: &[(DServerId, FragmentId)]) {
        for &(dserver, frag) in positions {
            assert!(
                self.fleet.holds(dserver, frag),
                "{frag:?} on {dserver} was reclaimed"
            );
        }
    }
}

/// The bytes-mode key of the suffix-free session token of `id` at `epoch`, and its event.
fn teardown(id: &UploadId, epoch: u64) -> (Vec<u8>, String) {
    let token = RetireToken::Session {
        upload_id: id.clone(),
        epoch,
        part: None,
    };
    (retire_key(RetireMode::Bytes, &token), token.to_string())
}

/// The index in `sent` of the first **committed** batch that puts `key`, and of the first that
/// deletes it.
fn put_at(sent: &[(WriteBatch, Option<CommitOutcome>)], key: &[u8]) -> Option<usize> {
    sent.iter().position(|(batch, outcome)| {
        *outcome == Some(CommitOutcome::Committed) && batch.puts.iter().any(|(k, _)| k == key)
    })
}

fn deleted_at(sent: &[(WriteBatch, Option<CommitOutcome>)], key: &[u8]) -> Option<usize> {
    sent.iter().position(|(batch, outcome)| {
        *outcome == Some(CommitOutcome::Committed) && batch.deletes.iter().any(|k| k == key)
    })
}

/// How many committed batches of `sent` delete `key`.
fn committed_deletes(sent: &[(WriteBatch, Option<CommitOutcome>)], key: &[u8]) -> usize {
    sent.iter()
        .filter(|(batch, outcome)| {
            *outcome == Some(CommitOutcome::Committed) && batch.deletes.iter().any(|k| k == key)
        })
        .count()
}

/// Every position of `positions` was marked by a committed batch strictly before the committed
/// batch deleting `record`.
fn assert_marked_before_deleted(
    sent: &[(WriteBatch, Option<CommitOutcome>)],
    positions: &[(DServerId, FragmentId)],
    record: &[u8],
) {
    let deleted = deleted_at(sent, record).unwrap_or_else(|| panic!("{} deleted", name(record)));
    for &(dserver, frag) in positions {
        let key = orphan_key(dserver, frag);
        let marked = put_at(sent, &key).unwrap_or_else(|| panic!("{} marked", name(&key)));
        assert!(
            marked < deleted,
            "{} marked in batch {marked}, {} deleted in batch {deleted}",
            name(&key),
            name(record)
        );
    }
}

// ─────────────────────────────── (D) ───────────────────────────────

/// **(D)** An `Aborting` session — two committed parts, the residue of a crashed third part (two
/// owned entries, one of whose fragments all landed and one of which landed only its first), and
/// the slot that attempt never released — drains through deployed passes to its end: every residue
/// position, landed or not, marked from its entry's placement with the walk's event in a batch
/// before the one deleting that entry; no `sidx:` or `slot:` record left; the `seggrp:` marker
/// gone; the session deleted by one committed batch, after the last entry's delete and the
/// obligation's; `mpuctl.count` one lower, and no lower after more passes. Past the grace every
/// fragment of the session is gone.
#[tokio::test]
async fn d_an_aborted_session_walks_its_residue_and_ends_once() {
    let rig = Rig::new(3).await;
    let id = rig.session(1, &format!("{BUCKET}/d")).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = vec![
        (1, vec![chunk(1_100, RS21, vec![0, 1, 2])]),
        (2, vec![chunk(1_200, RS21, vec![0, 1, 2])]),
    ];
    rig.plant_parts(&id, &parts).await;
    let whole = owned(&id, 3, 1_300, RS21, vec![0, 1, 2]);
    let torn = owned(&id, 3, 1_301, RS21, vec![0, 1, 2]);
    let whole_at = diagonal(1_300, &[0, 1, 2]);
    let torn_at = diagonal(1_301, &[0, 1, 2]);
    rig.fleet.store_at(&whole_at);
    rig.fleet.store_at(&torn_at[..1]);
    rig.store
        .plant(vec![whole.clone(), torn.clone(), slot(&id, 3)])
        .await;
    rig.abort(&id).await;
    let epoch = rig.epoch_of(&id).await;
    let event = format!("{id}:{epoch}");
    let (obligation, _) = teardown(&id, 0);
    assert!(rig.store.value(&obligation).await.is_some());
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    rig.drain_until_ended(&id, 6).await;

    let sent = rig.store.take_sent();
    for ((key, _), at) in [(&whole, &whole_at), (&torn, &torn_at)] {
        rig.assert_marked(at, &event).await;
        assert_marked_before_deleted(&sent, at, key);
    }
    let session = mpu_key(&id);
    assert_eq!(committed_deletes(&sent, &session), 1, "one terminal delete");
    let ended = deleted_at(&sent, &session).unwrap();
    for key in [&whole.0, &torn.0, &obligation] {
        assert!(deleted_at(&sent, key).unwrap() < ended, "{}", name(key));
    }
    assert!(rig.store.under(&mp::sidx_range(&id)).await.is_empty());
    assert!(rig.store.under(&mp::slot_range(&id)).await.is_empty());
    assert!(rig.store.under(&mp::part_range(&id)).await.is_empty());
    assert_eq!(rig.store.value(&seggrp_key(&nonce(1))).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);

    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    assert_eq!(rig.sessions_counted().await, counted - 1, "released once");
    assert!(
        rig.audit.waited(&session, "obligations-first"),
        "the empty range waited on the obligation"
    );
    let audit = &rig.audit;
    assert_eq!(audit.counter("retirement_owned_marks_written"), 6);
    assert_eq!(audit.counter("retirement_owned_entries_deleted"), 2);
    assert_eq!(audit.counter("retirement_sessions_deleted"), 1);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&whole_at);
    rig.assert_gone(&torn_at[..1]);
    for (_, chunks) in &parts {
        rig.assert_gone(&positions(chunks));
    }
}

/// **(D, the marker)** An `Aborting` session whose segment-group nonce a `seg:` record names — a
/// group adopted elsewhere — ends with its marker kept, and the record with it; the count still
/// goes down by one.
#[tokio::test]
async fn d_an_adopted_groups_marker_outlives_the_session() {
    let rig = Rig::new(3).await;
    let id = rig.session(2, &format!("{BUCKET}/d-adopted")).await;
    let group = SegmentGroup::from_nonce(nonce(2), 1);
    let record = SegmentRecord::new(vec![chunk(2_000, RS21, vec![0, 1, 2])], 0).unwrap();
    let seg = seg_key(&group, 0).unwrap();
    rig.store
        .plant(vec![(seg.clone(), md::encode(&record))])
        .await;
    rig.abort(&id).await;
    let counted = rig.sessions_counted().await;

    rig.drain_until_ended(&id, 4).await;

    assert!(
        rig.store.value(&seggrp_key(&nonce(2))).await.is_some(),
        "kept"
    );
    assert!(rig.store.value(&seg).await.is_some());
    assert_eq!(rig.sessions_counted().await, counted - 1);
}

/// **(D, `Completed`)** A session with two parts and the residue of a crashed third (one owned
/// entry, its fragments landed) is completed naming both parts. Deployed passes walk the residue
/// away and drain the Complete's obligation, and the session record — the tombstone — stays with
/// its slot counted; an identical Complete retry answers `AlreadyCompleted`. A pass whose tombstone
/// input says the window has passed deletes it, once: the count goes down by one, the marker of the
/// unadopted group goes, and more such passes change nothing. Past the grace the residue's
/// fragments are gone and the published ones stay.
#[tokio::test]
async fn d_a_completed_session_keeps_its_tombstone_until_the_window_passes() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/d-completed");
    let id = rig.session(3, &object).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = vec![
        (1, vec![chunk(3_100, RS21, vec![0, 1, 2])]),
        (2, vec![chunk(3_200, RS21, vec![0, 1, 2])]),
    ];
    rig.plant_parts(&id, &parts).await;
    let residue = owned(&id, 3, 3_300, RS21, vec![0, 1, 2]);
    let residue_at = diagonal(3_300, &[0, 1, 2]);
    rig.fleet.store_at(&residue_at);
    rig.store.plant(vec![residue.clone()]).await;
    let first = rig.complete(&id, &object, &[1, 2], 42).await;
    let CompleteOutcome::Published(publication) = first else {
        panic!("{first:?}");
    };
    let epoch = rig.epoch_of(&id).await;
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    for round in 0..4 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_ne!(outcome, Reconciled::Blocked, "round {round}");
    }

    let sent = rig.store.take_sent();
    rig.assert_marked(&residue_at, &format!("{id}:{epoch}"))
        .await;
    assert_marked_before_deleted(&sent, &residue_at, &residue.0);
    assert!(rig.store.under(&mp::sidx_range(&id)).await.is_empty());
    assert!(rig
        .store
        .under(&mp::retire_session_range(RetireMode::Records, &id))
        .await
        .is_empty());
    assert!(rig
        .store
        .under(&mp::retire_session_range(RetireMode::Bytes, &id))
        .await
        .is_empty());
    assert!(
        rig.store.value(&mpu_key(&id)).await.is_some(),
        "the tombstone stays"
    );
    assert_eq!(committed_deletes(&sent, &mpu_key(&id)), 0);
    assert_eq!(
        rig.sessions_counted().await,
        counted,
        "its slot still counted"
    );
    let retry = rig.complete(&id, &object, &[1, 2], 43).await;
    assert_eq!(retry, CompleteOutcome::AlreadyCompleted(publication));

    let outcome = rig.drain_seeing(&View::window_passed()).await.unwrap();
    assert_eq!(outcome, Reconciled::Changed);
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);
    assert_eq!(rig.store.value(&seggrp_key(&nonce(3))).await, None);
    for _ in 0..2 {
        rig.drain_seeing(&View::window_passed()).await.unwrap();
    }
    assert_eq!(rig.sessions_counted().await, counted - 1, "released once");
    assert_eq!(committed_deletes(&rig.store.take_sent(), &mpu_key(&id)), 1);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&residue_at);
    for (_, chunks) in &parts {
        rig.assert_kept(&positions(chunks));
    }
}

/// **(D, the gate)** A `Completed` session past its tombstone window, every obligation of it
/// drained, owns one residue entry whose first position carries a mark of none of the three
/// shapes: the walk holds it, so its `sidx:` range is never observed empty and the session is not
/// deleted, pass after pass, its slot still counted. Once the mark is repaired the walk empties the
/// range and the session is deleted, once.
#[tokio::test]
async fn d_the_terminal_delete_waits_for_an_empty_owned_range() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/d-gate");
    let id = rig.session(4, &object).await;
    rig.plant_parts(&id, &[(1, vec![chunk(4_100, RS21, vec![0, 1, 2])])])
        .await;
    let residue = owned(&id, 2, 4_200, RS21, vec![0, 1, 2]);
    let damaged = orphan_key(0, frag(4_200, 0));
    rig.store
        .plant(vec![
            residue.clone(),
            (damaged.clone(), Bytes::from_static(b"not-a-mark")),
        ])
        .await;
    let published = rig.complete(&id, &object, &[1], 44).await;
    assert!(matches!(published, CompleteOutcome::Published(_)));
    let counted = rig.sessions_counted().await;

    for round in 0..3 {
        let outcome = rig.drain_seeing(&View::window_passed()).await.unwrap();
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.session_holds(&mpu_key(&id));
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "unreadable-mark");
        assert_eq!(holds[0]["record"], name(&residue.0));
        assert!(
            rig.store.value(&mpu_key(&id)).await.is_some(),
            "round {round}"
        );
        assert_eq!(rig.store.value(&residue.0).await, Some(residue.1.clone()));
        assert_eq!(rig.sessions_counted().await, counted, "round {round}");
    }
    assert!(
        rig.store
            .under(&mp::retire_session_range(RetireMode::Records, &id))
            .await
            .is_empty(),
        "no obligation is what held it"
    );
    assert_eq!(
        rig.store.value(&walk_bookmark(&id)).await,
        None,
        "held at its first position: nothing to keep"
    );

    rig.store.remove(&damaged).await;
    rig.store.take_sent();
    for _ in 0..3 {
        rig.drain_seeing(&View::window_passed()).await.unwrap();
    }
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);
    assert_eq!(committed_deletes(&rig.store.take_sent(), &mpu_key(&id)), 1);
}

/// An `Aborting` session with nothing left but its record — beside an `Open` one, when `beside`,
/// so the ledger counts two. A wrapper runs a second drainer's whole pass between the first
/// drainer's read of the session and its read of `mpuctl`: the second deletes the session and
/// lowers the count. The count goes down by exactly one and stays there, and the first holds
/// nothing. Its sent batches deleting the session record are returned.
async fn race(n: u64, beside: bool) -> Vec<(WriteBatch, Option<CommitOutcome>)> {
    let rig = Rig::new(3).await;
    if beside {
        rig.session(n + 100, &format!("{BUCKET}/d-race-open")).await;
    }
    let id = rig.session(n, &format!("{BUCKET}/d-race")).await;
    rig.abort(&id).await;
    let (obligation, _) = teardown(&id, 0);
    rig.drain().await.unwrap();
    assert_eq!(rig.store.value(&obligation).await, None, "drained first");
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
    let counted = rig.sessions_counted().await;
    let ran = Arc::new(AtomicBool::new(false));
    *rig.store.rival.lock().unwrap() = Some(Rival {
        fleet: rig.fleet.clone(),
        clock: rig.clock.clone(),
        ran: Arc::clone(&ran),
    });
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("the pass never fails");

    assert!(ran.load(Ordering::SeqCst), "the rival ran");
    assert_ne!(
        outcome,
        Reconciled::Blocked,
        "the first drainer holds nothing"
    );
    assert!(rig.audit.is_empty(), "nothing audited");
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1, "released once");
    let sent: Vec<_> = rig
        .store
        .take_sent()
        .into_iter()
        .filter(|(batch, _)| batch.deletes.contains(&mpu_key(&id)))
        .collect();
    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    assert_eq!(rig.sessions_counted().await, counted - 1);
    sent
}

/// **(D, race)** With another session counted, the first drainer reads the lowered ledger and
/// sends its terminal batch — preconditioned on the session record's exact bytes — and the store
/// refuses it: the count goes down by one, not two.
#[tokio::test]
async fn d_two_drainers_racing_the_terminal_delete_release_the_slot_once() {
    let sent = race(5, true).await;
    assert_eq!(sent.len(), 1, "the first drainer sent one terminal batch");
    assert_eq!(
        sent[0].1,
        Some(CommitOutcome::Conflict),
        "and it was refused"
    );
}

/// **(D, race, the last session)** With no other session counted, the rival leaves the ledger at
/// zero. The first drainer reads that, finds the session gone, and stops: no batch, and no hold
/// claiming the ledger needs a human.
#[tokio::test]
async fn d_the_drainer_that_loses_the_last_sessions_race_holds_nothing() {
    let sent = race(6, false).await;
    assert!(sent.is_empty(), "{sent:?}");
}

/// **(D, race, the ledger moved)** A Create admits another session between the drainer's read of
/// `mpuctl` and its terminal batch: the batch, conditioned on the ledger's exact bytes, is refused,
/// and the drainer judges the session again in the same pass — it is still there — reads the
/// moved ledger and deletes it. The count ends where it began: one admitted, one released.
#[tokio::test]
async fn d_a_terminal_delete_whose_ledger_moved_is_judged_again_in_the_same_pass() {
    let rig = Rig::new(3).await;
    let id = rig.session(7, &format!("{BUCKET}/d-ledger-moved")).await;
    rig.abort(&id).await;
    rig.drain().await.unwrap();
    assert_eq!(rig.store.value(&teardown(&id, 0).0).await, None);
    let counted = rig.sessions_counted().await;
    *rig.store.creator.lock().unwrap() = Some(107);
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("the pass never fails");

    assert_eq!(outcome, Reconciled::Changed);
    assert!(
        rig.store.creator.lock().unwrap().is_none(),
        "the Create ran"
    );
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None, "in this pass");
    assert_eq!(rig.sessions_counted().await, counted);
    let terminal: Vec<_> = rig
        .store
        .take_sent()
        .into_iter()
        .filter(|(batch, _)| batch.deletes.contains(&mpu_key(&id)))
        .map(|(_, outcome)| outcome)
        .collect();
    assert_eq!(
        terminal,
        [
            Some(CommitOutcome::Conflict),
            Some(CommitOutcome::Committed)
        ]
    );
    assert!(rig.store.value(&mpu_key(&upload(107))).await.is_some());
}

// ─────────────────────────────── (E) ───────────────────────────────

/// Which untrusted record an `Aborting` session holds.
#[derive(Clone, Copy, Debug)]
enum Untrusted {
    /// E1: a `part:` record whose RS(4,2) chunk is placed on servers 0–4.
    Part,
    /// E2: an owned entry with that placement.
    Entry,
    /// E3: an owned value that will not decode — an unsupported scheme.
    Undecodable,
}

/// The chunk every untrusted record of session `n` names.
fn untrusted_chunk(n: u64) -> ChunkId {
    ChunkId::from(n) * 1_000 + 7
}

/// Plant, for session `id` numbered `n`, the record `kind` names; its key.
async fn plant_untrusted(rig: &Rig, id: &UploadId, n: u64, kind: Untrusted) -> Vec<u8> {
    let bad = untrusted_chunk(n);
    match kind {
        Untrusted::Part => {
            let puts = plant_part(id, 1, &[chunk(bad, RS42, vec![0, 1, 2, 3, 4])]);
            let key = puts[0].0.clone();
            rig.store.plant(puts).await;
            key
        }
        Untrusted::Entry => {
            let (key, value) = owned(id, 1, bad, RS42, vec![0, 1, 2, 3, 4]);
            rig.store.plant(vec![(key.clone(), value)]).await;
            key
        }
        Untrusted::Undecodable => {
            let (key, value) = owned(id, 1, bad, RS42, vec![0, 1, 2, 3, 4, 5]);
            let spelled = name(&value).replace("\"k\":4,\"m\":2", "\"k\":0,\"m\":0");
            assert_ne!(spelled, name(&value), "the scheme was rewritten");
            assert!(decode_owned_entry(&key, spelled.as_bytes()).is_err());
            let (owner, _, chunk) = parse_sidx_key(&key).unwrap();
            assert_eq!((owner, chunk), (id.clone(), bad), "its key names the chunk");
            rig.store
                .plant(vec![(key.clone(), Bytes::from(spelled))])
                .await;
            key
        }
    }
}

/// The event an untrusted record of session `id` is retired under: the Abort's obligation's for
/// a `part:` record, the owned walk's `<upload-id>:<epoch>` for an entry.
async fn retired_under(rig: &Rig, id: &UploadId, kind: Untrusted) -> String {
    match kind {
        Untrusted::Part => teardown(id, 0).1,
        Untrusted::Entry | Untrusted::Undecodable => format!("{id}:{}", rig.epoch_of(id).await),
    }
}

/// **(E)** For an `Aborting` session holding the record `kind` names, with a committed object's
/// fragments on the same servers: the untrusted chunk's fragments sit at `landed` — a position per
/// server — and one more lands, at `late`, right after the record's deletion (after the listing).
/// Deployed passes, whole fleet: every landed fragment, and for an entry that decodes every
/// position it names, is marked in a batch before the one deleting the record; the session ends.
/// Past the grace every landed fragment is gone, and the committed object's stay. What becomes of
/// the late fragment, when there is one, is the caller's to check on the rig returned.
async fn retire_by_chunk_id(
    kind: Untrusted,
    n: u64,
    landed: &[u16],
    late: Option<(DServerId, FragmentId)>,
) -> Rig {
    let rig = Rig::new(6).await;
    let committed = rig.committed_object(90 + n, 77).await;
    let id = rig.session(n, &format!("{BUCKET}/e-{n}")).await;
    let key = plant_untrusted(&rig, &id, n, kind).await;
    let bad = untrusted_chunk(n);
    let landed_at = diagonal(bad, landed);
    rig.fleet.store_at(&landed_at);
    rig.abort(&id).await;
    let event = retired_under(&rig, &id, kind).await;
    let counted = rig.sessions_counted().await;
    let listings_before = Arc::new(AtomicU64::new(0));
    if let Some(at) = late {
        assert!(!rig.fleet.holds(at.0, at.1), "nothing there to list yet");
        *rig.store.late.lock().unwrap() = Some(Late {
            deleted: key.clone(),
            fleet: rig.fleet.clone(),
            at,
            listings_before: Arc::clone(&listings_before),
        });
    }
    rig.store.take_sent();

    rig.drain_until_ended(&id, 8).await;

    if let Some(at) = late {
        assert!(
            rig.fleet.holds(at.0, at.1),
            "the late fragment landed after the record's deletion"
        );
        assert!(
            listings_before.load(Ordering::SeqCst) > 0,
            "and after the listing"
        );
    }
    let sent = rig.store.take_sent();
    let mut evidenced = landed_at.clone();
    if matches!(kind, Untrusted::Entry) {
        evidenced.extend(diagonal(bad, &[0, 1, 2, 3, 4]));
    }
    rig.assert_marked(&evidenced, &event).await;
    assert_marked_before_deleted(&sent, &evidenced, &key);
    assert_eq!(rig.store.value(&key).await, None, "the record is gone");
    assert_eq!(
        rig.sessions_counted().await,
        counted - 1,
        "the terminal delete fired"
    );

    rig.reclaim_past_grace().await;
    rig.assert_gone(&landed_at);
    rig.assert_kept(&committed);
    rig
}

/// **(E1)** A `part:` record whose RS(4,2) chunk is placed on five servers, its six fragments on
/// servers 0–5 — the one on server 5 at a position the record does not name — is retired by chunk
/// id: all six are marked, then the record goes, and past the grace all six are gone.
#[tokio::test]
async fn e1_an_aborted_sessions_untrusted_part_is_retired_by_chunk_id() {
    // A part's chunk has no write in flight (its commit follows every write's `Ok`): no late leg.
    retire_by_chunk_id(Untrusted::Part, 21, &[0, 1, 2, 3, 4, 5], None).await;
}

/// **(E2)** An owned entry with that placement, its chunk's fragments landed on servers 0, 1, 2
/// and 5: the four landed and the five positions it names (3 and 4 with nothing under them yet)
/// are marked, then the entry goes. **Late landing:** a fragment published at position 3 right
/// after the entry's deletion finds the walk's mark, and is gone after the grace with the rest.
#[tokio::test]
async fn e2_an_aborted_sessions_wrong_length_entry_is_retired_by_chunk_id() {
    let late = (3, frag(untrusted_chunk(22), 3));
    let rig = retire_by_chunk_id(Untrusted::Entry, 22, &[0, 1, 2, 5], Some(late)).await;
    rig.assert_gone(&[late]);
}

/// **(E3)** An owned value that will not decode (`RS(0,0)`, which the staged placement refuses),
/// its chunk taken from its key, fragments landed on servers 0–3: all four marked, then the entry
/// goes, and past the grace they are gone. **Late landing:** a fragment published at position 4
/// right after the deletion has no mark and no record — the stated residual — so it stays on disk
/// across GC passes past the late-write deadline and the grace, none of which fails.
#[tokio::test]
async fn e3_an_aborted_sessions_undecodable_entry_is_retired_by_its_keys_chunk_id() {
    let late = (4, frag(untrusted_chunk(23), 4));
    let rig = retire_by_chunk_id(Untrusted::Undecodable, 23, &[0, 1, 2, 3], Some(late)).await;
    for _ in 0..3 {
        rig.clock.advance(LATE_WRITE + GRACE);
        rig.gc().await;
    }
    rig.assert_kept(&[late]);
    assert!(
        rig.mark_at(late).await.is_none(),
        "no mark: kept, never reclaimed"
    );
}

// ─────────────────────────────── (E, controls) ───────────────────────────────

/// **(E, `Open`)** An `Open` session holding all three kinds of record — a wrong-length `part:`
/// record, a wrong-length owned entry and an undecodable one, each over its own chunk with
/// fragments on servers 0–5 — keeps every record and every fragment across deployed passes and GC
/// past the grace. Nothing marks any of them.
#[tokio::test]
async fn e_an_open_sessions_untrusted_records_are_kept_whole() {
    let rig = Rig::new(6).await;
    let mut kept = Vec::new();
    let mut records = Vec::new();
    for (n, kind) in [
        (31, Untrusted::Part),
        (32, Untrusted::Entry),
        (33, Untrusted::Undecodable),
    ] {
        let id = rig.session(n, &format!("{BUCKET}/open-{n}")).await;
        let key = plant_untrusted(&rig, &id, n, kind).await;
        records.push((key.clone(), rig.store.value(&key).await.unwrap()));
        let at = diagonal(untrusted_chunk(n), &[0, 1, 2, 3, 4, 5]);
        rig.fleet.store_at(&at);
        kept.extend(at);
    }

    for _ in 0..3 {
        rig.drain().await.expect("no pass fails");
    }
    rig.reclaim_past_grace().await;

    for (key, value) in &records {
        assert_eq!(
            rig.store.value(key).await.as_ref(),
            Some(value),
            "{}",
            name(key)
        );
    }
    rig.assert_kept(&kept);
    for n in [31, 32, 33] {
        rig.assert_unmarked(untrusted_chunk(n)).await;
    }
}

/// An `Aborting` session holding `kinds`, each over its own chunk with fragments on servers 0–5,
/// drained while server 5 is missing — left out at startup, or, when `dropped`, unreachable this
/// pass. Every pass holds each record as `partial-fleet`: the records stay, nothing of their
/// chunks is marked, the session stays, and GC past the grace keeps every fragment. Once the fleet
/// is whole the records retire and the session ends; past the grace every fragment is gone, server
/// 5's included.
async fn short_fleet(kinds: &[(u64, Untrusted)], dropped: bool) {
    let rig = Rig::new(6).await;
    let n = kinds[0].0;
    let id = rig.session(n, &format!("{BUCKET}/short-{n}")).await;
    let mut records = Vec::new();
    let mut fragments = Vec::new();
    for &(m, kind) in kinds {
        let key = plant_untrusted(&rig, &id, m, kind).await;
        records.push((key.clone(), rig.store.value(&key).await.unwrap()));
        let at = diagonal(untrusted_chunk(m), &[0, 1, 2, 3, 4, 5]);
        rig.fleet.store_at(&at);
        fragments.extend(at);
    }
    // Beside held owned entries, a trusted residue entry the walk drains on the short fleet too:
    // a step that holds and commits, which still holds each record once a pass.
    let owned_kinds = !matches!(kinds[0].1, Untrusted::Part);
    let sound_at = diagonal(ChunkId::from(n) * 1_000 + 9, &[0, 1, 2]);
    if owned_kinds {
        let residue = owned(&id, 9, sound_at[0].1.chunk, RS21, vec![0, 1, 2]);
        rig.store.plant(vec![residue]).await;
        rig.fleet.store_at(&sound_at);
    }
    rig.abort(&id).await;
    let (obligation, _) = teardown(&id, 0);
    let counted = rig.sessions_counted().await;
    let view = View::without(5, dropped);

    for round in 0..3 {
        let outcome = rig.drain_seeing(&view).await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = if matches!(kinds[0].1, Untrusted::Part) {
            rig.audit.obligation_holds(&obligation)
        } else {
            rig.audit.session_holds(&mpu_key(&id))
        };
        assert_eq!(holds.len(), records.len(), "round {round}: {holds:?}");
        for (hold, (key, _)) in holds.iter().zip(&records) {
            assert_eq!(hold["hold"], "partial-fleet", "round {round}");
            assert_eq!(hold["record"], name(key), "round {round}");
        }
        for (key, value) in &records {
            assert_eq!(
                rig.store.value(key).await.as_ref(),
                Some(value),
                "{}",
                name(key)
            );
        }
        for &(m, _) in kinds {
            rig.assert_unmarked(untrusted_chunk(m)).await;
        }
        assert!(rig.store.value(&mpu_key(&id)).await.is_some());
        assert_eq!(rig.sessions_counted().await, counted);
    }
    rig.reclaim_past_grace().await;
    rig.assert_kept(&fragments);
    rig.audit.clear();

    rig.drain_until_ended(&id, 8).await;
    for (key, _) in &records {
        assert_eq!(rig.store.value(key).await, None, "{}", name(key));
    }
    assert_eq!(rig.sessions_counted().await, counted - 1);
    rig.reclaim_past_grace().await;
    rig.assert_gone(&fragments);
    if owned_kinds {
        rig.assert_gone(&sound_at);
    }
}

/// **(E, fleet, a `part:` record)** Server 5 left out at startup.
#[tokio::test]
async fn e_a_part_record_waits_for_a_server_left_out_at_startup() {
    short_fleet(&[(41, Untrusted::Part)], false).await;
}

/// **(E, fleet, a `part:` record)** Server 5 unreachable this pass.
#[tokio::test]
async fn e_a_part_record_waits_for_a_server_unreachable_this_pass() {
    short_fleet(&[(42, Untrusted::Part)], true).await;
}

/// **(E, fleet, owned entries)** A wrong-length entry and an undecodable one; server 5 left out at
/// startup.
#[tokio::test]
async fn e_owned_entries_wait_for_a_server_left_out_at_startup() {
    short_fleet(
        &[(43, Untrusted::Entry), (44, Untrusted::Undecodable)],
        false,
    )
    .await;
}

/// **(E, fleet, owned entries)** The same two; server 5 unreachable this pass.
#[tokio::test]
async fn e_owned_entries_wait_for_a_server_unreachable_this_pass() {
    short_fleet(
        &[(45, Untrusted::Entry), (46, Untrusted::Undecodable)],
        true,
    )
    .await;
}

/// **(E, undecodable `part:`)** An `Aborting` session whose one part value will not decode — its
/// chunk's scheme `RS(0,0)`, which `decode_part_record` refuses. Its key names no chunk, so nothing
/// can find its bytes: every pass holds the obligation as `record-unreadable`, the audit event says
/// it needs a human, the record and the obligation stay byte-identical, nothing of the chunk is
/// marked, and the session's terminal delete does not fire — its record stays, its slot counted.
#[tokio::test]
async fn e_an_undecodable_part_record_needs_a_human_and_the_session_stays() {
    let rig = Rig::new(3).await;
    let id = rig.session(51, &format!("{BUCKET}/e-undecodable")).await;
    let zero = EcScheme::ReedSolomon { k: 0, m: 0 };
    let value = raw_part_value(&[chunk(51_000, zero, vec![0])], digest_of(1));
    assert!(
        decode_part_record(&value).is_err(),
        "the decoder refuses it"
    );
    let key = part_key(&id, part(1));
    rig.store.plant(vec![(key.clone(), value.clone())]).await;
    rig.fleet.store_at(&[(0, frag(51_000, 0))]);
    rig.abort(&id).await;
    let (obligation, _) = teardown(&id, 0);
    let owed = rig.store.value(&obligation).await.unwrap();
    let counted = rig.sessions_counted().await;

    for round in 0..3 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.obligation_holds(&obligation);
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "record-unreadable");
        assert_eq!(holds[0]["record"], name(&key));
        assert!(
            holds[0]["detail"].contains("needs a human"),
            "{:?}",
            holds[0]
        );
        assert_eq!(rig.store.value(&key).await, Some(value.clone()));
        assert_eq!(rig.store.value(&obligation).await, Some(owed.clone()));
        assert!(
            rig.store.value(&mpu_key(&id)).await.is_some(),
            "round {round}"
        );
        assert_eq!(rig.sessions_counted().await, counted, "round {round}");
        rig.assert_unmarked(51_000).await;
    }
}

/// **(E, `Completed`)** A `Completed` session past its tombstone window owns a wrong-length entry
/// and an undecodable one, each over its own chunk with fragments on servers 0–5. Retirement by
/// chunk id is an `Aborting` session's only: every pass on the whole fleet holds both entries —
/// `untrusted-placement` and `record-unreadable` — nothing of either chunk is marked, both records
/// stay byte-identical, the session stays with its slot counted, and GC past the grace keeps every
/// fragment.
#[tokio::test]
async fn e_a_completed_sessions_untrusted_entries_are_held_past_its_window() {
    let rig = Rig::new(6).await;
    let object = format!("{BUCKET}/e-completed");
    let id = rig.session(52, &object).await;
    rig.plant_parts(&id, &[(2, vec![chunk(52_200, RS21, vec![0, 1, 2])])])
        .await;
    let mut records = Vec::new();
    let mut kept = Vec::new();
    for (m, kind) in [(53, Untrusted::Entry), (54, Untrusted::Undecodable)] {
        let key = plant_untrusted(&rig, &id, m, kind).await;
        records.push((key.clone(), rig.store.value(&key).await.unwrap()));
        let at = diagonal(untrusted_chunk(m), &[0, 1, 2, 3, 4, 5]);
        rig.fleet.store_at(&at);
        kept.extend(at);
    }
    let published = rig.complete(&id, &object, &[2], 45).await;
    assert!(matches!(published, CompleteOutcome::Published(_)));
    let counted = rig.sessions_counted().await;

    for round in 0..3 {
        let outcome = rig.drain_seeing(&View::window_passed()).await.unwrap();
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.session_holds(&mpu_key(&id));
        assert_eq!(holds.len(), 2, "round {round}: {holds:?}");
        for (hold, ((key, _), reason)) in holds.iter().zip(
            records
                .iter()
                .zip(["untrusted-placement", "record-unreadable"]),
        ) {
            assert_eq!(hold["hold"], reason, "round {round}");
            assert_eq!(hold["record"], name(key), "round {round}");
        }
        for (key, value) in &records {
            assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
        }
        for m in [53, 54] {
            rig.assert_unmarked(untrusted_chunk(m)).await;
        }
        assert!(rig.store.value(&mpu_key(&id)).await.is_some());
        assert_eq!(rig.sessions_counted().await, counted, "round {round}");
    }
    rig.reclaim_past_grace().await;
    rig.assert_kept(&kept);
}

// ─────────────────────────────── past one step ───────────────────────────────

/// Every committed batch of `sent` that puts a mark of `chunk`.
fn marking_batches(sent: &[(WriteBatch, Option<CommitOutcome>)], chunk: ChunkId) -> usize {
    sent.iter()
        .filter(|(batch, outcome)| {
            *outcome == Some(CommitOutcome::Committed)
                && batch.puts.iter().any(|(key, _)| {
                    md::parse_orphan_key(key).is_some_and(|(_, frag)| frag.chunk == chunk)
                })
        })
        .count()
}

/// How many marks each committed batch of `sent` puts, in order, for the batches that put one.
fn marks_per_batch(sent: &[(WriteBatch, Option<CommitOutcome>)]) -> Vec<(usize, &WriteBatch)> {
    sent.iter()
        .filter(|(_, outcome)| *outcome == Some(CommitOutcome::Committed))
        .map(|(batch, _)| {
            let marks = batch
                .puts
                .iter()
                .filter(|(key, _)| key.starts_with(b"orphan:"))
                .count();
            (marks, batch)
        })
        .filter(|(marks, _)| *marks > 0)
        .collect()
}

/// Whether `batch` puts a mark of `chunk`.
fn marks_chunk(batch: &WriteBatch, chunk: ChunkId) -> bool {
    batch
        .puts
        .iter()
        .any(|(key, _)| md::parse_orphan_key(key).is_some_and(|(_, frag)| frag.chunk == chunk))
}

/// Whether `batch` is the ledger walk's — a step under its obligation's precondition, or the
/// write of its resume point — rather than the session leg's.
fn ledger_walks(batch: &WriteBatch) -> bool {
    batch
        .preconditions
        .iter()
        .any(|p| p.key.starts_with(b"retire:"))
        || batch.puts.iter().any(|(key, _)| key == LEDGER_CURSOR_KEY)
}

/// **(D, past one step)** An `Aborting` session owns two entries that each name more positions
/// than one walk step reads: a trusted RS(128,128) entry, its 256 positions on servers 0–5 with
/// every fragment landed, and a wrong-length RS(4,2) entry naming 300 positions, its six fragments
/// landed on the diagonal and a copy of fragment 0 on server 1, which it does not name. Deployed
/// passes end the session — in two, the walk taking as many steps as it needs within a pass: it
/// resumes from its bookmark — written along the way, gone at the end — so every position each
/// entry names, and the listed copy, is marked in a batch before the one deleting that entry, over
/// more than one step each. The first step marks exactly `MARKS_PER_STEP` positions and none marks
/// more. The count goes down by one, and past the grace every landed fragment is gone.
#[tokio::test]
async fn d_owned_entries_past_one_steps_reads_resume_from_the_walks_bookmark() {
    let rig = Rig::new(6).await;
    let id = rig.session(61, &format!("{BUCKET}/d-past-one-step")).await;
    let wide = 61_100;
    let wide_placement: Vec<DServerId> = (0..256).map(|i| i % 6).collect();
    let wide_entry = owned(&id, 2, wide, RS128, wide_placement.clone());
    let wide_at: Vec<_> = (0..256_u16)
        .map(|i| (wide_placement[usize::from(i)], frag(wide, i)))
        .collect();
    let damaged = 61_200;
    let damaged_entry = owned(&id, 3, damaged, RS42, (0..300).map(|i| i % 6).collect());
    let named_at: Vec<_> = (0..300_u16)
        .map(|i| (u64::from(i % 6), frag(damaged, i)))
        .collect();
    let landed_at = diagonal(damaged, &[0, 1, 2, 3, 4, 5]);
    let copy = (1, frag(damaged, 0));
    rig.fleet.store_at(&wide_at);
    rig.fleet.store_at(&landed_at);
    rig.fleet.store_at(&[copy]);
    rig.store
        .plant(vec![wide_entry.clone(), damaged_entry.clone()])
        .await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    rig.drain_until_ended(&id, 2).await;

    let sent = rig.store.take_sent();
    let steps = marks_per_batch(&sent);
    assert_eq!(
        steps[0].0, MARKS_PER_STEP,
        "the first step reads its whole budget"
    );
    assert!(marks_chunk(steps[0].1, wide));
    assert!(steps.iter().all(|(marks, _)| *marks <= MARKS_PER_STEP));
    rig.assert_marked(&wide_at, &event).await;
    assert_marked_before_deleted(&sent, &wide_at, &wide_entry.0);
    let mut evidenced = named_at.clone();
    evidenced.push(copy);
    rig.assert_marked(&evidenced, &event).await;
    assert_marked_before_deleted(&sent, &evidenced, &damaged_entry.0);
    for chunk in [wide, damaged] {
        assert!(
            marking_batches(&sent, chunk) >= 2,
            "chunk {chunk}: past one step"
        );
    }
    let bookmark = walk_bookmark(&id);
    assert!(
        put_at(&sent, &bookmark).is_some(),
        "the walk kept its place"
    );
    assert_eq!(rig.store.value(&bookmark).await, None, "and dropped it");
    assert_eq!(rig.sessions_counted().await, counted - 1);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&wide_at);
    rig.assert_gone(&landed_at);
    rig.assert_gone(&[copy]);
}

/// **(E1, past one step)** An `Aborting` session's part 2 holds 79 RS(4,2) chunks: 39 placed on
/// servers 0–5, and, between them, 40 placed on servers 0–4 — the staged rule refuses those —
/// each of which has its six fragments on servers 0–5. That is 234 trusted positions and 240
/// fragments found by chunk id, far past one step's reads. Its part 1, all trusted, names exactly
/// `MARKS_PER_STEP` positions, so the first step spends every read on it and meets part 2 with
/// none left. Deployed passes resume from the obligation's cursor, inside the trusted positions
/// and then a chunk at a time, and end the session: every position and every listed fragment is
/// marked in a batch before the one deleting its part, over several steps, none marking more than
/// `MARKS_PER_STEP` and the first of part 2's one fewer (its session record is read from the same
/// budget); past the grace they are all gone, and a committed object's fragments on the same
/// servers stay.
#[tokio::test]
async fn e1_a_part_past_one_steps_reads_resumes_from_its_cursor() {
    let rig = Rig::new(6).await;
    let committed = rig.committed_object(162, 78).await;
    let id = rig.session(62, &format!("{BUCKET}/e1-past-one-step")).await;
    let mut chunks = Vec::new();
    let mut sound = Vec::new();
    let mut found = Vec::new();
    for j in 0..40_u128 {
        if j < 39 {
            let good = chunk(62_000 + j, RS42, vec![0, 1, 2, 3, 4, 5]);
            sound.extend(positions(std::slice::from_ref(&good)));
            chunks.push(good);
        }
        let bad = 62_500 + j;
        chunks.push(chunk(bad, RS42, vec![0, 1, 2, 3, 4]));
        found.extend(diagonal(bad, &[0, 1, 2, 3, 4, 5]));
    }
    let mut first = Vec::new();
    for j in 0..(MARKS_PER_STEP / 6) as u128 {
        first.push(chunk(61_000 + j, RS42, vec![0, 1, 2, 3, 4, 5]));
    }
    for j in 0..(MARKS_PER_STEP % 6) as u64 {
        first.push(chunk(61_900 + u128::from(j), EcScheme::None, vec![j]));
    }
    let first_at = positions(&first);
    assert_eq!(first_at.len(), MARKS_PER_STEP);
    let mut puts = plant_part(&id, 1, &first);
    let first_record = puts[0].0.clone();
    puts.extend(plant_part(&id, 2, &chunks));
    let record = puts[2].0.clone();
    rig.store.plant(puts).await;
    rig.fleet.store_at(&first_at);
    rig.fleet.store_at(&sound);
    rig.fleet.store_at(&found);
    rig.abort(&id).await;
    let (_, event) = teardown(&id, 0);
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    rig.drain_until_ended(&id, 12).await;

    let sent = rig.store.take_sent();
    rig.assert_marked(&first_at, &event).await;
    assert_marked_before_deleted(&sent, &first_at, &first_record);
    let mut evidenced = sound.clone();
    evidenced.extend(found.iter().copied());
    rig.assert_marked(&evidenced, &event).await;
    assert_marked_before_deleted(&sent, &evidenced, &record);
    let steps = marks_per_batch(&sent);
    assert!(steps.iter().all(|(_, batch)| ledger_walks(batch)));
    assert!(steps.iter().all(|(marks, _)| *marks <= MARKS_PER_STEP));
    assert_eq!(steps[0].0, MARKS_PER_STEP, "part 1 takes the first step");
    assert_eq!(steps[1].0, MARKS_PER_STEP - 1, "part 2's first step");
    assert!(steps.len() >= 4, "{} marking steps", steps.len());
    assert_eq!(rig.store.value(&first_record).await, None);
    assert_eq!(rig.store.value(&record).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&first_at);
    rig.assert_gone(&evidenced);
    rig.assert_kept(&committed);
}

/// **(E, the listing past one step)** Two `Aborting` sessions, each owning one record over an
/// RS(4,2) chunk the fleet's listings show `LISTED_PER_CHUNK + 1` fragments of (a fragment at
/// every index up to that, round the six servers): a wrong-length `part:` record, and an
/// undecodable owned entry. Neither can be read whole in one step, so every pass holds each as
/// `listing-past-step` — the obligation for the part, the session for the entry — with the record
/// kept, nothing of the chunk marked, and the session kept with its slot. A third session's
/// wrong-length part, its chunk listed with exactly `LISTED_PER_CHUNK` fragments, retires, and
/// its session ends.
#[tokio::test]
async fn e_a_chunk_listed_past_one_steps_reads_is_held() {
    let rig = Rig::new(6).await;
    let spread = |chunk: ChunkId, count: usize| -> Vec<(DServerId, FragmentId)> {
        (0..count)
            .map(|i| ((i % 6) as DServerId, frag(chunk, i as u16)))
            .collect()
    };
    let mut held = Vec::new();
    for (n, kind) in [(63, Untrusted::Part), (64, Untrusted::Undecodable)] {
        let id = rig.session(n, &format!("{BUCKET}/e-listing-{n}")).await;
        let key = plant_untrusted(&rig, &id, n, kind).await;
        let value = rig.store.value(&key).await.unwrap();
        rig.fleet
            .store_at(&spread(untrusted_chunk(n), LISTED_PER_CHUNK + 1));
        rig.abort(&id).await;
        held.push((n, kind, id, key, value));
    }
    let fits = rig.session(65, &format!("{BUCKET}/e-listing-65")).await;
    let fits_key = plant_untrusted(&rig, &fits, 65, Untrusted::Part).await;
    let fits_at = spread(untrusted_chunk(65), LISTED_PER_CHUNK);
    rig.fleet.store_at(&fits_at);
    rig.abort(&fits).await;
    let counted = rig.sessions_counted().await;

    for round in 0..4 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let events = rig.audit.events.lock().unwrap().clone();
        rig.audit.clear();
        for (n, kind, id, key, value) in &held {
            let holder = match kind {
                Untrusted::Part => teardown(id, 0).0,
                _ => mpu_key(id),
            };
            let named: Vec<_> = events
                .iter()
                .filter(|e| e["key"] == name(&holder))
                .collect();
            assert_eq!(named.len(), 1, "round {round}, session {n}: {named:?}");
            assert_eq!(named[0]["hold"], "listing-past-step");
            assert_eq!(named[0]["record"], name(key));
            assert!(named[0]["detail"].contains("needs a human"));
            assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
            rig.assert_unmarked(untrusted_chunk(*n)).await;
            assert!(rig.store.value(&mpu_key(id)).await.is_some());
        }
    }
    assert_eq!(
        rig.store.value(&fits_key).await,
        None,
        "the part that fits retired"
    );
    assert_eq!(rig.store.value(&mpu_key(&fits)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);
    rig.assert_marked(&fits_at, &teardown(&fits, 0).1).await;
}

/// **(D, many sessions)** Sixteen `Open` sessions fill the first page of the session listing —
/// the leg passes them by, and they never leave it — and twenty `Aborting` sessions listed after
/// them each own one residue entry with its fragments landed: every one of those past the first
/// page, and more commits than one pass's steps. No pass commits more than
/// `SESSION_STEPS_PER_PASS` session-leg batches, and the first is cut short at that; repeated
/// deployed passes end every `Aborting` session: each residue marked before its entry's delete,
/// every range empty, the count down by twenty. The `Open` sessions stay.
#[tokio::test]
async fn d_sessions_past_one_page_and_one_pass_of_steps_all_end() {
    let rig = Rig::new(3).await;
    let mut open = Vec::new();
    for n in 0..16_u64 {
        open.push(rig.session(200 + n, &format!("{BUCKET}/d-open-{n}")).await);
    }
    let mut sessions = Vec::new();
    for n in 100..120_u64 {
        let id = rig.session(200 + n, &format!("{BUCKET}/d-many-{n}")).await;
        let residue_chunk = ChunkId::from(200 + n) * 1_000;
        let residue = owned(&id, 1, residue_chunk, RS21, vec![0, 1, 2]);
        let at = diagonal(residue_chunk, &[0, 1, 2]);
        rig.fleet.store_at(&at);
        rig.store.plant(vec![residue.clone()]).await;
        rig.abort(&id).await;
        sessions.push((id, residue, at));
    }
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    let mut passes = 0;
    let mut sent = Vec::new();
    loop {
        let mut left = 0;
        for (id, _, _) in &sessions {
            if rig.store.value(&mpu_key(id)).await.is_some() {
                left += 1;
            }
        }
        if left == 0 {
            break;
        }
        assert!(passes < 20, "{left} sessions left after {passes} passes");
        let outcome = rig.drain().await.expect("no pass fails");
        assert_ne!(outcome, Reconciled::Blocked, "pass {passes}");
        let this_pass = rig.store.take_sent();
        let session_leg = this_pass
            .iter()
            .filter(|(batch, outcome)| {
                *outcome == Some(CommitOutcome::Committed) && !ledger_walks(batch)
            })
            .count();
        assert!(session_leg <= 16, "pass {passes}: {session_leg} commits");
        if passes == 0 {
            assert!(
                left == 20 && session_leg == 16,
                "the first pass is cut short"
            );
        }
        sent.extend(this_pass);
        passes += 1;
    }
    assert!(rig.audit.is_empty(), "nothing held");
    for (id, (key, _), at) in &sessions {
        assert_marked_before_deleted(&sent, at, key);
        assert!(rig.store.under(&mp::sidx_range(id)).await.is_empty());
        assert_eq!(committed_deletes(&sent, &mpu_key(id)), 1);
    }
    assert_eq!(rig.sessions_counted().await, counted - 20);
    for id in &open {
        assert!(rig.store.value(&mpu_key(id)).await.is_some(), "{id} stays");
    }
}

// ──────────────────────── a held entry ahead of a wide one ────────────────────────

/// What holds the owned entry ahead of a wide one ([`held_ahead_of_a_wide_entry`]).
#[derive(Clone, Copy, Debug)]
enum Ahead {
    /// An `Aborting` session's wrong-length entry, on passes whose fleet lacks server 5 (left out
    /// at startup): `partial-fleet`, before anything of it is read.
    ShortFleet,
    /// A `Completed` session's wrong-length entry, past its tombstone window:
    /// `untrusted-placement`, before anything of it is read.
    Completed,
    /// An `Aborting` session's RS(128,128) entry whose position 240 — past what one step reads from
    /// its first position — carries a mark of none of the three shapes: `unreadable-mark`, part-way
    /// through it.
    MarkPastOneStep,
}

/// A session numbered `n` owns, at part 1, the entry `ahead` holds, and at part 2 a wide entry —
/// RS(128,128), 256 positions, every fragment landed. Repeated deployed passes, each holding the
/// entry ahead once: the wide entry resumes from the bookmark the walk keeps on it — the held
/// entry never takes it — and retires within three passes, every position marked with the walk's
/// event in a batch before the one deleting it; two more passes mark nothing. The held entry stays
/// byte-identical, the session stays with its slot counted, and the bookmark is gone — unless the
/// held entry has a count of its own: for `MarkPastOneStep` its 240 positions before the hold are
/// marked, none after it, and the bookmark keeps them. Past the grace the wide entry's fragments
/// are gone and the held entry's stay. The rig, the session and the held entry's key.
async fn held_ahead_of_a_wide_entry(n: u64, ahead: Ahead) -> (Rig, UploadId, Vec<u8>) {
    let rig = Rig::new(6).await;
    let object = format!("{BUCKET}/held-ahead-{n}");
    let id = rig.session(n, &object).await;
    let base = ChunkId::from(n) * 1_000;
    let (held_key, held_at, reason, view) = match ahead {
        Ahead::ShortFleet | Ahead::Completed => {
            let key = plant_untrusted(&rig, &id, n, Untrusted::Entry).await;
            let at = diagonal(untrusted_chunk(n), &[0, 1, 2, 3, 4, 5]);
            match ahead {
                Ahead::ShortFleet => (key, at, "partial-fleet", View::without(5, false)),
                _ => (key, at, "untrusted-placement", View::window_passed()),
            }
        }
        Ahead::MarkPastOneStep => {
            let (entry, at) = wide(&id, 1, base + 1);
            let damaged = orphan_key(at[240].0, at[240].1);
            rig.store
                .plant(vec![
                    entry.clone(),
                    (damaged, Bytes::from_static(b"not-a-mark")),
                ])
                .await;
            (entry.0, at, "unreadable-mark", View::deployed())
        }
    };
    rig.fleet.store_at(&held_at);
    let held_value = rig.store.value(&held_key).await.unwrap();
    let (wide_entry, wide_at) = wide(&id, 2, base + 2);
    assert!(
        held_key < wide_entry.0,
        "the held entry is ahead in the range"
    );
    rig.fleet.store_at(&wide_at);
    rig.store.plant(vec![wide_entry.clone()]).await;
    if let Ahead::Completed = ahead {
        rig.plant_parts(&id, &[(3, vec![chunk(base + 3, RS21, vec![0, 1, 2])])])
            .await;
        let published = rig.complete(&id, &object, &[3], n).await;
        assert!(
            matches!(published, CompleteOutcome::Published(_)),
            "{published:?}"
        );
    } else {
        rig.abort(&id).await;
    }
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    let mut passes = 0;
    while rig.store.value(&wide_entry.0).await.is_some() {
        assert!(
            passes < 3,
            "{ahead:?}: the wide entry retires within 3 passes"
        );
        let outcome = rig.drain_seeing(&view).await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "{ahead:?}, pass {passes}");
        let holds = rig.audit.session_holds(&mpu_key(&id));
        assert_eq!(holds.len(), 1, "{ahead:?}, pass {passes}: {holds:?}");
        assert_eq!(holds[0]["hold"], reason, "{ahead:?}, pass {passes}");
        assert_eq!(holds[0]["record"], name(&held_key), "{ahead:?}");
        passes += 1;
    }
    let sent = rig.store.take_sent();
    rig.assert_marked(&wide_at, &event).await;
    assert_marked_before_deleted(&sent, &wide_at, &wide_entry.0);

    for _ in 0..2 {
        rig.drain_seeing(&view).await.expect("no pass fails");
    }
    assert!(
        marks_per_batch(&rig.store.take_sent()).is_empty(),
        "{ahead:?}: nothing more is marked"
    );
    rig.audit.clear();
    assert_eq!(rig.store.value(&held_key).await, Some(held_value));
    assert!(
        rig.store.value(&mpu_key(&id)).await.is_some(),
        "{ahead:?}: the session stays"
    );
    assert_eq!(rig.sessions_counted().await, counted);
    let bookmark = rig.store.value(&walk_bookmark(&id)).await;
    if let Ahead::MarkPastOneStep = ahead {
        rig.assert_marked(&held_at[..240], &event).await;
        for &position in &held_at[241..] {
            assert!(rig.mark_at(position).await.is_none(), "{position:?}");
        }
        assert_eq!(
            bookmark,
            Some(bookmark_value(&held_key, 240)),
            "the held entry keeps its place"
        );
    } else {
        rig.assert_unmarked(untrusted_chunk(n)).await;
        assert_eq!(bookmark, None, "{ahead:?}: nothing counted, nothing kept");
    }

    rig.reclaim_past_grace().await;
    rig.assert_gone(&wide_at);
    rig.assert_kept(&held_at);
    (rig, id, held_key)
}

/// **(D, held ahead, a short fleet)** An `Aborting` session's wrong-length entry, held while
/// server 5 is left out at startup, ahead of a wide entry: the wide entry retires while it is held.
/// Once the fleet is whole the held entry retires by chunk id too, and the session ends.
#[tokio::test]
async fn d_a_wide_entry_behind_one_held_for_a_short_fleet_retires() {
    let (rig, id, held) = held_ahead_of_a_wide_entry(71, Ahead::ShortFleet).await;
    rig.drain_until_ended(&id, 4).await;
    assert_eq!(rig.store.value(&held).await, None);
    rig.reclaim_past_grace().await;
    rig.assert_gone(&diagonal(untrusted_chunk(71), &[0, 1, 2, 3, 4, 5]));
}

/// **(D, held ahead, `Completed`)** A `Completed` session past its tombstone window, its
/// wrong-length entry held ahead of a wide entry: the wide entry retires; the held one stays, and
/// so does the session.
#[tokio::test]
async fn d_a_completed_sessions_wide_entry_behind_a_held_one_retires() {
    held_ahead_of_a_wide_entry(72, Ahead::Completed).await;
}

/// **(D, held part-way)** An `Aborting` session's wide entry held at its position 240, ahead of
/// another wide entry: the walk takes the bookmarked entry first, so the second retires — it is
/// not re-read from its first position each step behind the first's 240 marks — and the first
/// stays, its place kept at its hold.
#[tokio::test]
async fn d_a_wide_entry_behind_one_held_past_one_step_retires() {
    held_ahead_of_a_wide_entry(73, Ahead::MarkPastOneStep).await;
}

// ─────────────────────────────── the bookmark lost ───────────────────────────────

/// **(D, a lost bookmark)** An `Aborting` session owns one wide entry every one of whose 256
/// positions already carries the walk's own mark, and no bookmark — as a step leaves them whose
/// bookmark is then lost. The walk reads from the first position again, marks nothing, and keeps
/// its place after one step's reads — the stored key `custodian:owned-walk:<upload-id>`, its value
/// the entry's key and `MARKS_PER_STEP` — in a step that commits only that, under a precondition on
/// the session record as read, and ends the session's visit for the pass. The next pass goes on
/// from there: the entry is deleted and the session ends, the bookmark gone with the entry, no mark
/// written.
#[tokio::test]
async fn d_a_lost_bookmark_costs_reads_not_progress() {
    let rig = Rig::new(6).await;
    let id = rig.session(74, &format!("{BUCKET}/d-lost-bookmark")).await;
    let (entry, at) = wide(&id, 1, 74_100);
    rig.fleet.store_at(&at);
    rig.store.plant(vec![entry.clone()]).await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let mark = md::encode_orphan_mark(&OrphanMark::structured(T0, event.clone()).unwrap());
    let marks = at
        .iter()
        .map(|&(dserver, frag)| (orphan_key(dserver, frag), mark.clone()))
        .collect();
    rig.store.plant(marks).await;
    let counted = rig.sessions_counted().await;
    let session = rig.store.value(&mpu_key(&id)).await.unwrap();
    let bookmark = walk_bookmark(&id);
    rig.store.take_sent();

    // The first pass: one step, which marks and deletes nothing. It keeps its place — put only
    // while the session record is the one read — and the visit ends there.
    let outcome = rig.drain().await.expect("no pass fails");
    assert_ne!(outcome, Reconciled::Blocked);
    let mut sent = rig.store.take_sent();
    let kept = session_leg(&sent);
    assert_eq!(kept.len(), 1, "one batch: {kept:?}");
    assert_eq!(
        kept[0].puts,
        [(bookmark.clone(), bookmark_value(&entry.0, MARKS_PER_STEP))]
    );
    assert!(kept[0].deletes.is_empty());
    assert!(
        kept[0]
            .preconditions
            .iter()
            .any(|p| p.key == mpu_key(&id) && p.expected.as_ref() == Some(&session)),
        "put only while the session record is the one read: {:?}",
        kept[0].preconditions
    );
    assert_eq!(rig.store.value(&entry.0).await, Some(entry.1.clone()));

    rig.drain_until_ended(&id, 2).await;

    sent.extend(rig.store.take_sent());
    assert!(marks_per_batch(&sent).is_empty(), "nothing is marked again");
    let kept: Vec<&Bytes> = sent
        .iter()
        .filter(|(_, outcome)| *outcome == Some(CommitOutcome::Committed))
        .flat_map(|(batch, _)| {
            batch
                .puts
                .iter()
                .filter(|(key, _)| *key == bookmark)
                .map(|(_, value)| value)
        })
        .collect();
    assert_eq!(
        kept,
        [&bookmark_value(&entry.0, MARKS_PER_STEP)],
        "one step's reads, kept"
    );
    assert!(put_at(&sent, &bookmark).unwrap() < deleted_at(&sent, &entry.0).unwrap());
    assert_eq!(rig.store.value(&bookmark).await, None);
    rig.assert_marked(&at, &event).await;
    assert_eq!(rig.sessions_counted().await, counted - 1);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&at);
}

/// **(D, a bookmark naming no entry of its session)** An `Aborting` session owns one wide entry,
/// nothing of it marked, and its bookmark names another session's entry at `MARKS_PER_STEP`
/// positions. That is no evidence: the first pass names it on the audit seam
/// (`unusable-owned-walk-bookmark`), the walk reads the entry from its first position, every one of
/// its 256 positions is marked before its delete, and the session ends.
#[tokio::test]
async fn d_a_bookmark_naming_no_entry_of_its_session_is_no_evidence() {
    let rig = Rig::new(6).await;
    let id = rig
        .session(75, &format!("{BUCKET}/d-unusable-bookmark"))
        .await;
    let (entry, at) = wide(&id, 1, 75_100);
    rig.fleet.store_at(&at);
    let elsewhere = sidx_key(&upload(76), part(1), 76_100);
    let planted = (
        walk_bookmark(&id),
        bookmark_value(&elsewhere, MARKS_PER_STEP),
    );
    rig.store.plant(vec![entry.clone(), planted]).await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    rig.store.take_sent();

    rig.drain().await.expect("no pass fails");
    let named = rig
        .audit
        .take("unusable-owned-walk-bookmark", &mpu_key(&id));
    assert_eq!(named.len(), 1, "{named:?}");
    rig.drain_until_ended(&id, 3).await;

    let sent = rig.store.take_sent();
    rig.assert_marked(&at, &event).await;
    assert_marked_before_deleted(&sent, &at, &entry.0);
    assert_eq!(rig.store.value(&walk_bookmark(&id)).await, None);
}

/// **(D, a held entry keeps its place)** An `Aborting` session owns, at part 1, a residue entry
/// whose first position carries a mark of none of the three shapes, and at part 2 a wrong-length
/// RS(4,2) entry naming 300 positions, its fragments landed on the diagonal and a copy of fragment
/// 0 on server 1, which it does not name. A whole-fleet pass holds the first and walks the second
/// as far as one step reads, keeping its place in the bookmark. A pass that lacks server 5 holds
/// both — the second as `partial-fleet`, before anything of it is read — and commits nothing: the
/// bookmark keeps the second's count. Once the mark is repaired and the fleet is whole the walk
/// resumes there, and the session ends: every position the second names, and the listed copy,
/// marked before its delete.
#[tokio::test]
async fn d_a_held_entry_keeps_its_place_in_the_walk() {
    let rig = Rig::new(6).await;
    let id = rig
        .session(77, &format!("{BUCKET}/d-held-keeps-place"))
        .await;
    let first = owned(&id, 1, 77_100, RS21, vec![0, 1, 2]);
    let first_at = diagonal(77_100, &[0, 1, 2]);
    let damaged = orphan_key(first_at[0].0, first_at[0].1);
    let second = owned(&id, 2, 77_200, RS42, (0..300).map(|i| i % 6).collect());
    let named_at: Vec<_> = (0..300_u16)
        .map(|i| (u64::from(i % 6), frag(77_200, i)))
        .collect();
    let copy = (1, frag(77_200, 0));
    rig.fleet.store_at(&first_at);
    rig.fleet.store_at(&diagonal(77_200, &[0, 1, 2, 3, 4, 5]));
    rig.fleet.store_at(&[copy]);
    rig.store
        .plant(vec![
            first.clone(),
            second.clone(),
            (damaged.clone(), Bytes::from_static(b"not-a-mark")),
        ])
        .await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let bookmark = walk_bookmark(&id);
    // One read goes to the first entry's damaged mark; the rest mark the second.
    let place = bookmark_value(&second.0, MARKS_PER_STEP - 1);

    let outcome = rig.drain().await.expect("no pass fails");
    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(rig.store.value(&bookmark).await, Some(place.clone()));
    rig.audit.clear();
    let mut sent = rig.store.take_sent();

    let outcome = rig.drain_seeing(&View::without(5, false)).await;
    assert_eq!(outcome.expect("no pass fails"), Reconciled::Blocked);
    let holds = rig.audit.session_holds(&mpu_key(&id));
    let reasons: Vec<(&str, String)> = holds
        .iter()
        .map(|hold| (hold["hold"].as_str(), hold["record"].clone()))
        .collect();
    assert_eq!(
        reasons,
        [
            ("partial-fleet", name(&second.0)),
            ("unreadable-mark", name(&first.0))
        ],
        "the bookmarked entry is met first"
    );
    let short = rig.store.take_sent();
    assert!(
        short.iter().all(
            |(batch, _)| !batch.puts.iter().any(|(key, _)| *key == bookmark)
                && !batch.deletes.contains(&bookmark)
        ),
        "the short pass leaves the bookmark alone"
    );
    assert_eq!(rig.store.value(&bookmark).await, Some(place));
    sent.extend(short);

    rig.store.remove(&damaged).await;
    rig.audit.clear();
    rig.drain_until_ended(&id, 3).await;
    let resumed = rig.store.take_sent();
    assert_eq!(
        put_at(&resumed, &orphan_key(0, frag(77_200, 0))),
        None,
        "the second resumes past its counted positions"
    );
    sent.extend(resumed);
    let mut evidenced = named_at;
    evidenced.push(copy);
    rig.assert_marked(&evidenced, &event).await;
    assert_marked_before_deleted(&sent, &evidenced, &second.0);
    rig.assert_marked(&first_at, &event).await;
    assert_eq!(rig.store.value(&bookmark).await, None);
}

// ─────────────────────────────── past held entries ───────────────────────────────

/// The session-leg batches of `sent` the store committed: every one that is not the ledger walk's.
fn session_leg(sent: &[(WriteBatch, Option<CommitOutcome>)]) -> Vec<&WriteBatch> {
    sent.iter()
        .filter(|(batch, outcome)| {
            *outcome == Some(CommitOutcome::Committed) && !ledger_walks(batch)
        })
        .map(|(batch, _)| batch)
        .collect()
}

/// The owned entry of `chunk`, staged by part `n` of `id`, that a `Completed` session's walk
/// holds on any fleet and an `Aborting` session's on a short one: a wrong-length RS(4,2) placement
/// on servers 0–4, or — when `undecodable` — a value under an unsupported scheme.
fn untrusted_entry(id: &UploadId, n: u32, chunk: ChunkId, undecodable: bool) -> Record {
    if !undecodable {
        return owned(id, n, chunk, RS42, vec![0, 1, 2, 3, 4]);
    }
    let (key, value) = owned(id, n, chunk, RS42, vec![0, 1, 2, 3, 4, 5]);
    let spelled = name(&value).replace("\"k\":4,\"m\":2", "\"k\":0,\"m\":0");
    assert!(decode_owned_entry(&key, spelled.as_bytes()).is_err());
    (key, Bytes::from(spelled))
}

/// A session numbered `n` whose owned range holds, at parts 1–16, sixteen untrusted entries — a
/// whole page of the walk — each with its chunk's six fragments on servers 0–5, and behind them,
/// at part 17, a trusted RS(2,1) entry with its fragments landed. When `completed` the session is
/// `Completed` (every other entry undecodable) and the view is past its tombstone window, so the
/// sixteen are held as `untrusted-placement` and `record-unreadable`; otherwise it is `Aborting`
/// and the view lacks server 5, left out at startup, so they are held as `partial-fleet`.
struct HeldPage {
    rig: Rig,
    id: UploadId,
    view: View,
    held: Vec<Record>,
    held_at: Vec<(DServerId, FragmentId)>,
    reasons: Vec<&'static str>,
    sound: Record,
    sound_at: Vec<(DServerId, FragmentId)>,
}

impl HeldPage {
    async fn new(n: u64, completed: bool) -> Self {
        let rig = Rig::new(6).await;
        let object = format!("{BUCKET}/held-page-{n}");
        let id = rig.session(n, &object).await;
        let base = ChunkId::from(n) * 1_000;
        let mut held = Vec::new();
        let mut held_at = Vec::new();
        let mut reasons = Vec::new();
        for p in 1..=16_u32 {
            let undecodable = completed && p % 2 == 0;
            let chunk = base + ChunkId::from(p);
            held.push(untrusted_entry(&id, p, chunk, undecodable));
            held_at.extend(diagonal(chunk, &[0, 1, 2, 3, 4, 5]));
            reasons.push(match (completed, undecodable) {
                (false, _) => "partial-fleet",
                (true, false) => "untrusted-placement",
                (true, true) => "record-unreadable",
            });
        }
        let sound = owned(&id, 17, base + 17, RS21, vec![0, 1, 2]);
        let sound_at = diagonal(base + 17, &[0, 1, 2]);
        rig.fleet.store_at(&held_at);
        rig.fleet.store_at(&sound_at);
        let mut puts = held.clone();
        puts.push(sound.clone());
        rig.store.plant(puts).await;
        let view = if completed {
            rig.plant_parts(&id, &[(20, vec![chunk(base + 20, RS21, vec![0, 1, 2])])])
                .await;
            let published = rig.complete(&id, &object, &[20], n).await;
            assert!(
                matches!(published, CompleteOutcome::Published(_)),
                "{published:?}"
            );
            View::window_passed()
        } else {
            rig.abort(&id).await;
            View::without(5, false)
        };
        Self {
            rig,
            id,
            view,
            held,
            held_at,
            reasons,
            sound,
            sound_at,
        }
    }

    /// One pass under the view: it is `Blocked`, and holds each of the sixteen once, in range
    /// order, for its reason.
    async fn pass_holding_the_page(&self, round: usize) {
        let outcome = self.rig.drain_seeing(&self.view).await;
        assert_eq!(
            outcome.expect("no pass fails"),
            Reconciled::Blocked,
            "pass {round}"
        );
        let holds = self.rig.audit.session_holds(&mpu_key(&self.id));
        let named: Vec<(&str, String)> = holds
            .iter()
            .map(|hold| (hold["hold"].as_str(), hold["record"].clone()))
            .collect();
        let expected: Vec<(&str, String)> = self
            .reasons
            .iter()
            .zip(&self.held)
            .map(|(reason, (key, _))| (*reason, name(key)))
            .collect();
        assert_eq!(named, expected, "pass {round}");
    }

    /// Every held entry is byte-identical and nothing of its chunk is marked.
    async fn assert_the_page_stands(&self) {
        for (key, value) in &self.held {
            assert_eq!(
                self.rig.store.value(key).await.as_ref(),
                Some(value),
                "{}",
                name(key)
            );
            let (_, _, chunk) = parse_sidx_key(key).unwrap();
            self.rig.assert_unmarked(chunk).await;
        }
    }
}

/// A session whose owned range starts with a whole page of held entries, a trusted entry behind
/// them ([`HeldPage`]). The first pass holds the sixteen, marks nothing, and commits one batch:
/// the walk's scan position, at the literal key `custodian:owned-scan:<upload-id>`, naming the
/// sixteenth entry — put only while the session record is the one read. The next pass reads past
/// them: the trusted entry is marked with the walk's event, deleted in a later batch, and the
/// walk, finding nothing after the position, is back at the head — the position deleted, the
/// sixteen held again. More passes commit nothing: sixteen entries are exactly one page, and one
/// row read past it says nothing follows. The sixteen stay byte-identical, nothing of their chunks
/// marked, and the session stays with its slot counted; past the grace the trusted entry's
/// fragments are gone and theirs stay.
async fn a_sound_entry_behind_a_page_of_held_ones_retires(n: u64, completed: bool) -> HeldPage {
    let page = HeldPage::new(n, completed).await;
    let (rig, id) = (&page.rig, &page.id);
    let event = format!("{id}:{}", rig.epoch_of(id).await);
    let counted = rig.sessions_counted().await;
    let position = walk_position(id);
    let session = rig.store.value(&mpu_key(id)).await.unwrap();
    rig.store.take_sent();

    page.pass_holding_the_page(0).await;
    let first = rig.store.take_sent();
    let moved = session_leg(&first);
    assert_eq!(moved.len(), 1, "one batch: {moved:?}");
    let sixteenth = Bytes::from(page.held[15].0.clone());
    assert_eq!(moved[0].puts, [(position.clone(), sixteenth.clone())]);
    assert!(moved[0].deletes.is_empty());
    assert!(
        moved[0]
            .preconditions
            .iter()
            .any(|p| p.key == mpu_key(id) && p.expected.as_ref() == Some(&session)),
        "put only while the session record is the one read: {:?}",
        moved[0].preconditions
    );
    assert_eq!(rig.store.value(&position).await, Some(sixteenth));
    assert!(rig.mark_at(page.sound_at[0]).await.is_none());

    let mut sent = Vec::new();
    let mut passes = 1;
    while rig.store.value(&page.sound.0).await.is_some() {
        assert!(passes < 3, "the trusted entry retires within 3 passes");
        page.pass_holding_the_page(passes).await;
        sent.extend(rig.store.take_sent());
        passes += 1;
    }
    rig.assert_marked(&page.sound_at, &event).await;
    assert_marked_before_deleted(&sent, &page.sound_at, &page.sound.0);
    assert_eq!(
        rig.store.value(&position).await,
        None,
        "nothing follows the sixteen: the walk is back at the head"
    );
    assert_eq!(committed_deletes(&sent, &position), 1);
    assert_eq!(rig.store.value(&walk_bookmark(id)).await, None);

    for round in 0..3 {
        page.pass_holding_the_page(passes + round).await;
        let quiet = rig.store.take_sent();
        assert!(
            session_leg(&quiet).is_empty(),
            "exactly one page of held entries moves nothing: {:?}",
            session_leg(&quiet)
        );
    }
    assert_eq!(rig.store.value(&position).await, None);
    page.assert_the_page_stands().await;
    assert!(rig.store.value(&mpu_key(id)).await.is_some());
    assert_eq!(rig.sessions_counted().await, counted);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&page.sound_at);
    rig.assert_kept(&page.held_at);
    rig.audit.clear();
    page
}

/// **(D, a page of held entries, a short fleet)** An `Aborting` session's sixteen wrong-length
/// entries, held while server 5 is left out at startup, ahead of a trusted one: the trusted entry
/// retires while they are held. Once the fleet is whole the sixteen retire by chunk id, the
/// session ends, and past the grace every fragment is gone, server 5's included.
#[tokio::test]
async fn d_a_trusted_entry_behind_a_page_held_for_a_short_fleet_retires() {
    let page = a_sound_entry_behind_a_page_of_held_ones_retires(81, false).await;
    let (rig, id) = (&page.rig, &page.id);
    let counted = rig.sessions_counted().await;
    rig.drain_until_ended(id, 6).await;
    assert!(rig.store.under(&mp::sidx_range(id)).await.is_empty());
    assert_eq!(rig.store.value(&walk_position(id)).await, None);
    assert_eq!(rig.store.value(&walk_bookmark(id)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);
    rig.reclaim_past_grace().await;
    rig.assert_gone(&page.held_at);
}

/// **(D, a page of held entries, `Completed`)** A `Completed` session past its tombstone window,
/// sixteen untrusted entries — wrong-length and undecodable by turns — ahead of a trusted one: the
/// trusted entry retires; the sixteen stay, and so does the session.
#[tokio::test]
async fn d_a_completed_sessions_trusted_entry_behind_a_page_of_held_ones_retires() {
    a_sound_entry_behind_a_page_of_held_ones_retires(82, true).await;
}

/// **(D, a position and a session that moved)** The first pass over a page of held entries reads
/// the session, and another writer deletes the session record before the walk's position is put.
/// The put is conditioned on the record as read, so the store refuses it: no position is left
/// behind a session that is gone.
#[tokio::test]
async fn d_no_scan_position_is_put_once_the_session_record_has_moved() {
    let page = HeldPage::new(83, false).await;
    let (rig, id) = (&page.rig, &page.id);
    let position = walk_position(id);
    *rig.store.meanwhile.lock().unwrap() =
        Some((position.clone(), WriteBatch::new().delete(mpu_key(id))));
    rig.store.take_sent();

    rig.drain_seeing(&page.view).await.expect("no pass fails");

    assert!(rig.store.meanwhile.lock().unwrap().is_none(), "it ran");
    let sent = rig.store.take_sent();
    let puts: Vec<_> = sent
        .iter()
        .filter(|(batch, _)| batch.puts.iter().any(|(key, _)| *key == position))
        .map(|(_, outcome)| *outcome)
        .collect();
    assert_eq!(puts, [Some(CommitOutcome::Conflict)], "sent, and refused");
    assert_eq!(rig.store.value(&position).await, None);
}

/// **(D, the listed fragments past what is left of a step)** An `Aborting` session owns, at parts
/// 1 and 2, two residue entries whose first position carries a mark of none of the three shapes —
/// each held after one read — and at part 3 an undecodable entry whose chunk the fleet's listings
/// show exactly `LISTED_PER_CHUNK` fragments of: one more than the reads the two leave it. Its
/// listed fragments are never counted, so it names no position to keep a count of; the bookmark
/// names it all the same, at a count of zero, so the next step walks it first with every read.
/// Deployed passes retire it within four: every listed fragment marked with the walk's event in a
/// batch before the one deleting it, no batch marking more than `MARKS_PER_STEP`. The two held
/// entries stay, and the session with them; past the grace its fragments are gone and theirs stay.
#[tokio::test]
async fn d_an_undecodable_entry_listed_to_the_brim_behind_two_held_ones_retires() {
    let rig = Rig::new(6).await;
    let id = rig.session(84, &format!("{BUCKET}/d-listed-brim")).await;
    let mut held = Vec::new();
    let mut held_at = Vec::new();
    for p in 1..=2_u32 {
        let chunk = 84_000 + ChunkId::from(p);
        let entry = owned(&id, p, chunk, RS21, vec![0, 1, 2]);
        let at = diagonal(chunk, &[0, 1, 2]);
        let damaged = (
            orphan_key(at[0].0, at[0].1),
            Bytes::from_static(b"not-a-mark"),
        );
        rig.store.plant(vec![entry.clone(), damaged]).await;
        held.push(entry);
        held_at.extend(at);
    }
    let listed = 84_003;
    let undecodable = untrusted_entry(&id, 3, listed, true);
    let listed_at: Vec<(DServerId, FragmentId)> = (0..LISTED_PER_CHUNK)
        .map(|i| ((i % 6) as DServerId, frag(listed, i as u16)))
        .collect();
    rig.store.plant(vec![undecodable.clone()]).await;
    rig.fleet.store_at(&held_at);
    rig.fleet.store_at(&listed_at);
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    let mut passes = 0;
    while rig.store.value(&undecodable.0).await.is_some() {
        assert!(passes < 4, "the undecodable entry retires within 4 passes");
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "pass {passes}");
        let holds = rig.audit.session_holds(&mpu_key(&id));
        assert!(!holds.is_empty(), "pass {passes}");
        for hold in &holds {
            assert_eq!(hold["hold"], "unreadable-mark", "pass {passes}");
            assert!(
                held.iter().any(|(key, _)| hold["record"] == name(key)),
                "pass {passes}: {hold:?}"
            );
        }
        if passes == 0 {
            assert_eq!(
                rig.store.value(&walk_bookmark(&id)).await,
                Some(bookmark_value(&undecodable.0, 0)),
                "named for the next step, nothing counted"
            );
        }
        passes += 1;
    }
    let sent = rig.store.take_sent();
    rig.assert_marked(&listed_at, &event).await;
    assert_marked_before_deleted(&sent, &listed_at, &undecodable.0);
    let steps = marks_per_batch(&sent);
    assert_eq!(steps[0].0, MARKS_PER_STEP - 2, "what the two left it");
    assert!(steps.iter().all(|(marks, _)| *marks <= MARKS_PER_STEP));
    // The step that deleted it ran out of reads on the second held entry and named that one for
    // the next step, which holds it at its first position: nothing to keep, and then nothing moves.
    rig.drain().await.expect("no pass fails");
    rig.store.take_sent();
    rig.drain().await.expect("no pass fails");
    assert!(session_leg(&rig.store.take_sent()).is_empty());
    rig.audit.clear();
    assert_eq!(rig.store.value(&walk_bookmark(&id)).await, None);
    assert_eq!(rig.store.value(&walk_position(&id)).await, None);
    for (key, value) in &held {
        assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
    }
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
    assert_eq!(rig.sessions_counted().await, counted);

    rig.reclaim_past_grace().await;
    rig.assert_gone(&listed_at);
    rig.assert_kept(&held_at);
}

/// **(D, held part-way, three of them)** An `Aborting` session owns three wide entries, each with
/// a mark of none of the three shapes at its position 150, and behind them a trusted RS(2,1)
/// entry. Re-reading two of the three up to their holds takes more reads than one step has, so
/// while its page starts with them the trusted entry is never reached. The walk passes the held
/// entries at the head of its page — its scan position names the second, the third being the one
/// its reads ran out on — and the trusted entry, now in reach, is marked and deleted within eight
/// passes. **Then nothing is left that the walk can work on, and it is quiet:** over eight more
/// passes the bookmark stays, byte for byte, on the last entry the walk held, at its hold — an
/// entry merely re-read as far as the step's reads went never takes it — and each pass commits one
/// batch of the session's, the scan position alone; no mark is written. The three stay
/// byte-identical, and the session with them; past the grace the trusted entry's fragments are
/// gone and theirs stay.
#[tokio::test]
async fn d_a_trusted_entry_behind_three_held_part_way_retires() {
    let rig = Rig::new(6).await;
    let id = rig.session(85, &format!("{BUCKET}/d-three-held")).await;
    let mut held = Vec::new();
    let mut held_at = Vec::new();
    for p in 1..=3_u32 {
        let (entry, at) = wide(&id, p, 85_000 + ChunkId::from(p));
        let damaged = (
            orphan_key(at[150].0, at[150].1),
            Bytes::from_static(b"not-a-mark"),
        );
        rig.store.plant(vec![entry.clone(), damaged]).await;
        held.push(entry);
        held_at.extend(at);
    }
    let sound = owned(&id, 4, 85_004, RS21, vec![0, 1, 2]);
    let sound_at = diagonal(85_004, &[0, 1, 2]);
    rig.store.plant(vec![sound.clone()]).await;
    rig.fleet.store_at(&held_at);
    rig.fleet.store_at(&sound_at);
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    let mut passes = 0;
    while rig.store.value(&sound.0).await.is_some() {
        assert!(passes < 8, "the trusted entry retires within 8 passes");
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "pass {passes}");
        passes += 1;
    }
    let sent = rig.store.take_sent();
    rig.assert_marked(&sound_at, &event).await;
    assert_marked_before_deleted(&sent, &sound_at, &sound.0);
    let position = walk_position(&id);
    let passed: Vec<&Bytes> = session_leg(&sent)
        .into_iter()
        .flat_map(|batch| &batch.puts)
        .filter(|(key, _)| *key == position)
        .map(|(_, value)| value)
        .collect();
    assert_eq!(
        passed,
        [&Bytes::from(held[1].0.clone())],
        "past the two it held, as far as the one it did not"
    );
    // The steady state: three held entries and nothing the walk can work on. The bookmark is at
    // rest on the last entry the walk held, at its hold, and a pass commits nothing but the scan
    // position — one write, as the walk laps the range.
    let bookmark = walk_bookmark(&id);
    let place = Some(bookmark_value(&held[2].0, 150));
    assert_eq!(rig.store.value(&bookmark).await, place);
    let marks = rig.store.under(b"orphan:").await;
    rig.audit.clear();
    for round in 0..8 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "steady pass {round}");
        let holds = rig.audit.session_holds(&mpu_key(&id));
        assert!(!holds.is_empty(), "steady pass {round}");
        assert!(
            holds.iter().all(|hold| hold["hold"] == "unreadable-mark"),
            "steady pass {round}: {holds:?}"
        );
        let quiet = rig.store.take_sent();
        let moved: Vec<_> = quiet
            .iter()
            .filter(|(batch, _)| !ledger_walks(batch))
            .collect();
        assert_eq!(moved.len(), 1, "steady pass {round}: {moved:?}");
        let (batch, outcome) = moved[0];
        assert_eq!(*outcome, Some(CommitOutcome::Committed));
        let touched: Vec<&Vec<u8>> = batch
            .puts
            .iter()
            .map(|(key, _)| key)
            .chain(&batch.deletes)
            .collect();
        assert_eq!(touched, [&position], "steady pass {round}");
        assert_eq!(
            rig.store.value(&bookmark).await,
            place,
            "steady pass {round}: the bookmark is at rest"
        );
    }
    assert_eq!(rig.store.under(b"orphan:").await, marks, "nothing marked");
    for (key, value) in &held {
        assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
    }
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
    assert_eq!(rig.sessions_counted().await, counted);

    rig.audit.clear();
    rig.reclaim_past_grace().await;
    rig.assert_gone(&sound_at);
    rig.assert_kept(&held_at);
}

/// An `Aborting` session owns seventeen entries — a page of the walk and one more: sixteen
/// residue entries whose second position carries a mark of none of the three shapes, and one wide
/// entry with such a mark at its position 240, the `at`-th of the seventeen in range order. Every
/// one is held part-way, so each has a count the bookmark could keep, and the walk laps the range.
/// Once the walk has read the wide entry up to its hold the bookmark names it at 240, and it still
/// does after every one of six more passes — whichever page the pass read, and whatever is held
/// there: an entry on another page keeps its place. Nothing more of the wide entry is marked
/// meanwhile. Once its mark is repaired the wide entry resumes there and retires within four
/// passes, every position marked before its delete; the sixteen stay, and the session with them.
async fn the_bookmark_of_a_held_entry_survives_the_lap(n: u64, at: usize) {
    let rig = Rig::new(6).await;
    let id = rig.session(n, &format!("{BUCKET}/d-lap-{n}")).await;
    let base = ChunkId::from(n) * 1_000;
    let mut small = Vec::new();
    let mut wide_entry = None;
    for (slot, p) in (1..=17_u32).enumerate() {
        let chunk = base + ChunkId::from(p);
        let (entry, positions, damaged) = if slot == at {
            let (entry, positions) = wide(&id, p, chunk);
            let damaged = positions[240];
            wide_entry = Some((entry.clone(), positions.clone(), damaged));
            (entry, positions, damaged)
        } else {
            let entry = owned(&id, p, chunk, RS21, vec![0, 1, 2]);
            let positions = diagonal(chunk, &[0, 1, 2]);
            small.push(entry.clone());
            (entry, positions.clone(), positions[1])
        };
        let mark = (
            orphan_key(damaged.0, damaged.1),
            Bytes::from_static(b"not-a-mark"),
        );
        rig.store.plant(vec![entry, mark]).await;
        rig.fleet.store_at(&positions);
    }
    let (wide_entry, wide_at, damaged) = wide_entry.expect("the wide entry");
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let bookmark = walk_bookmark(&id);
    let place = bookmark_value(&wide_entry.0, 240);
    rig.store.take_sent();

    let mut passes = 0;
    while rig.store.value(&bookmark).await.as_ref() != Some(&place) {
        assert!(passes < 4, "the walk reaches the wide entry's hold");
        rig.drain().await.expect("no pass fails");
        passes += 1;
    }
    let mut sent = rig.store.take_sent();
    for round in 0..6 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "lap pass {round}");
        assert_eq!(
            rig.store.value(&bookmark).await.as_ref(),
            Some(&place),
            "lap pass {round}"
        );
    }
    let laps = rig.store.take_sent();
    assert!(
        put_at(&laps, &walk_position(&id)).is_some()
            && deleted_at(&laps, &walk_position(&id)).is_some(),
        "the walk lapped the range"
    );
    assert_eq!(
        marking_batches(&laps, wide_at[0].1.chunk),
        0,
        "and marked nothing more of the wide entry"
    );
    sent.extend(laps);
    rig.assert_marked(&wide_at[..240], &event).await;
    for &position in &wide_at[241..] {
        assert!(rig.mark_at(position).await.is_none(), "{position:?}");
    }

    rig.store.remove(&orphan_key(damaged.0, damaged.1)).await;
    let mut passes = 0;
    while rig.store.value(&wide_entry.0).await.is_some() {
        assert!(passes < 4, "the wide entry retires within 4 passes");
        rig.drain().await.expect("no pass fails");
        passes += 1;
    }
    let resumed = rig.store.take_sent();
    assert_eq!(
        marks_per_batch(&resumed)
            .iter()
            .map(|(marks, _)| *marks)
            .sum::<usize>(),
        16,
        "it resumes at its hold: only the positions from there are marked"
    );
    sent.extend(resumed);
    rig.assert_marked(&wide_at, &event).await;
    assert_marked_before_deleted(&sent, &wide_at, &wide_entry.0);
    for (key, value) in &small {
        assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
    }
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
}

/// **(D, the bookmark and the lap, an entry before the position)** The wide entry is the first of
/// the seventeen: while the walk reads the page after the sixteenth, the bookmark names an entry
/// before its scan position.
#[tokio::test]
async fn d_the_bookmark_of_an_entry_before_the_scan_position_survives_the_lap() {
    the_bookmark_of_a_held_entry_survives_the_lap(86, 0).await;
}

/// **(D, the bookmark and the lap, the entry at the position)** The wide entry is the sixteenth:
/// the scan position names the very entry the bookmark does.
#[tokio::test]
async fn d_the_bookmark_of_the_entry_at_the_scan_position_survives_the_lap() {
    the_bookmark_of_a_held_entry_survives_the_lap(87, 15).await;
}

/// **(D, the bookmark and the lap, an entry after the page)** The wide entry is the seventeenth:
/// while the walk reads the first page, the bookmark names an entry after the page's last.
#[tokio::test]
async fn d_the_bookmark_of_an_entry_after_the_page_survives_the_lap() {
    the_bookmark_of_a_held_entry_survives_the_lap(88, 16).await;
}

/// **(D, a bookmark naming an entry that is gone)** An `Aborting` session owns one residue entry,
/// held at its first position, and its bookmark names an entry of the same session that is not
/// there — past the last one, where the range has ended. The page shows it is gone, so the first
/// pass drops the bookmark, in a batch that carries nothing else; the held entry stays.
#[tokio::test]
async fn d_a_bookmark_naming_an_entry_that_is_gone_is_dropped() {
    let rig = Rig::new(3).await;
    let id = rig.session(89, &format!("{BUCKET}/d-gone-bookmark")).await;
    let entry = owned(&id, 1, 89_001, RS21, vec![0, 1, 2]);
    let at = diagonal(89_001, &[0, 1, 2]);
    let bookmark = walk_bookmark(&id);
    let gone = sidx_key(&id, part(9), 89_009);
    rig.fleet.store_at(&at);
    rig.store
        .plant(vec![
            entry.clone(),
            (
                orphan_key(at[0].0, at[0].1),
                Bytes::from_static(b"not-a-mark"),
            ),
            (bookmark.clone(), bookmark_value(&gone, 2)),
        ])
        .await;
    rig.abort(&id).await;
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("no pass fails");

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(rig.store.value(&bookmark).await, None);
    let sent = rig.store.take_sent();
    let dropped = session_leg(&sent);
    assert_eq!(dropped.len(), 1, "{dropped:?}");
    assert_eq!(dropped[0].deletes, [bookmark]);
    assert!(dropped[0].puts.is_empty());
    assert_eq!(rig.store.value(&entry.0).await, Some(entry.1));
    assert_eq!(rig.store.value(&walk_position(&id)).await, None);
}

/// **(D, a position naming nothing of its range)** An `Aborting` session owns one residue entry,
/// and its scan position holds another session's key. That is no position: the first pass names it
/// on the audit seam (`unusable-owned-walk-position`), deletes it before anything else, and reads
/// from the head — the entry is marked, then deleted, and the session ends.
#[tokio::test]
async fn d_a_scan_position_naming_nothing_of_its_range_is_read_as_the_head() {
    let rig = Rig::new(3).await;
    let id = rig
        .session(90, &format!("{BUCKET}/d-unusable-position"))
        .await;
    let entry = owned(&id, 1, 90_001, RS21, vec![0, 1, 2]);
    let at = diagonal(90_001, &[0, 1, 2]);
    let position = walk_position(&id);
    let elsewhere = sidx_key(&upload(91), part(1), 91_001);
    rig.fleet.store_at(&at);
    rig.store
        .plant(vec![
            entry.clone(),
            (position.clone(), Bytes::from(elsewhere)),
        ])
        .await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    rig.store.take_sent();

    rig.drain().await.expect("no pass fails");
    let named = rig
        .audit
        .take("unusable-owned-walk-position", &mpu_key(&id));
    assert_eq!(named.len(), 1, "{named:?}");
    rig.drain_until_ended(&id, 3).await;

    let sent = rig.store.take_sent();
    assert_eq!(rig.store.value(&position).await, None);
    assert!(
        deleted_at(&sent, &position).unwrap()
            < put_at(&sent, &orphan_key(at[0].0, at[0].1)).unwrap(),
        "deleted before the walk marks anything"
    );
    rig.assert_marked(&at, &event).await;
    assert_marked_before_deleted(&sent, &at, &entry.0);
}

/// **(D, a position over an empty range)** A `Completed` session inside its tombstone window owns
/// no residue, and a scan position is left under it, naming an entry that is gone. Nothing is left
/// after it and nothing at the head: a deployed pass deletes the position, holds nothing, and the
/// session record — the tombstone — stays with its slot counted.
#[tokio::test]
async fn d_a_scan_position_left_over_an_empty_range_is_deleted() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/d-position-empty");
    let id = rig.session(92, &object).await;
    rig.plant_parts(&id, &[(1, vec![chunk(92_001, RS21, vec![0, 1, 2])])])
        .await;
    let published = rig.complete(&id, &object, &[1], 92).await;
    assert!(matches!(published, CompleteOutcome::Published(_)));
    let position = walk_position(&id);
    let gone = sidx_key(&id, part(2), 92_002);
    rig.store
        .plant(vec![(position.clone(), Bytes::from(gone))])
        .await;
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("no pass fails");

    assert_ne!(outcome, Reconciled::Blocked);
    assert!(rig.audit.is_empty(), "nothing held, nothing named");
    assert_eq!(rig.store.value(&position).await, None);
    assert_eq!(committed_deletes(&rig.store.take_sent(), &position), 1);
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
    assert_eq!(rig.sessions_counted().await, counted);
}

/// **(D, the terminal delete and the walk's own records)** An `Aborting` session with nothing
/// left but its record. Between the walk's reading of its empty range and the terminal batch,
/// another drainer leaves a scan position and a bookmark under it. The terminal delete removes
/// both with the session: nothing of the walk outlives it.
#[tokio::test]
async fn d_the_terminal_delete_removes_the_walks_own_records() {
    let rig = Rig::new(3).await;
    let id = rig.session(93, &format!("{BUCKET}/d-terminal-own")).await;
    rig.abort(&id).await;
    rig.drain().await.unwrap();
    assert_eq!(rig.store.value(&teardown(&id, 0).0).await, None);
    assert!(rig.store.value(&mpu_key(&id)).await.is_some());
    let gone = sidx_key(&id, part(1), 93_001);
    let left = WriteBatch::new()
        .put(walk_position(&id), Bytes::from(gone.clone()))
        .put(walk_bookmark(&id), bookmark_value(&gone, 3));
    *rig.store.meanwhile.lock().unwrap() = Some((MPUCTL_KEY.to_vec(), left));
    let counted = rig.sessions_counted().await;

    rig.drain().await.expect("no pass fails");

    assert!(rig.store.meanwhile.lock().unwrap().is_none(), "it ran");
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None);
    assert_eq!(rig.sessions_counted().await, counted - 1);
    assert_eq!(rig.store.value(&walk_position(&id)).await, None);
    assert_eq!(rig.store.value(&walk_bookmark(&id)).await, None);
}

/// **(D, an entry the walk can work on, at the head)** An `Aborting` session owns two wide
/// entries, every position of both already carrying the walk's own mark but for a mark of none of
/// the three shapes at the second's position 240, and the bookmark names the second at its hold.
/// The first pass holds the second after one read and re-reads the first as far as the step's
/// reads go: nothing is marked or deleted and something is held, but the entry at the head of the
/// page is not — so the page stays, no scan position is written, and the bookmark moves to the
/// first entry, which the next pass finishes and deletes. The second stays, held.
#[tokio::test]
async fn d_a_page_whose_first_entry_is_not_held_stays() {
    let rig = Rig::new(6).await;
    let id = rig.session(94, &format!("{BUCKET}/d-head-not-held")).await;
    let (first, first_at) = wide(&id, 1, 94_001);
    let (second, second_at) = wide(&id, 2, 94_002);
    rig.fleet.store_at(&first_at);
    rig.fleet.store_at(&second_at);
    rig.store.plant(vec![first.clone(), second.clone()]).await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let own = md::encode_orphan_mark(&OrphanMark::structured(T0, event.clone()).unwrap());
    let mut marks: Vec<Record> = first_at
        .iter()
        .chain(&second_at)
        .map(|&(dserver, frag)| (orphan_key(dserver, frag), own.clone()))
        .collect();
    marks.push((
        orphan_key(second_at[240].0, second_at[240].1),
        Bytes::from_static(b"not-a-mark"),
    ));
    let bookmark = walk_bookmark(&id);
    marks.push((bookmark.clone(), bookmark_value(&second.0, 240)));
    rig.store.plant(marks).await;
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("no pass fails");

    assert_eq!(outcome, Reconciled::Blocked);
    let holds = rig.audit.session_holds(&mpu_key(&id));
    assert_eq!(holds.len(), 1, "{holds:?}");
    assert_eq!(holds[0]["record"], name(&second.0));
    assert_eq!(
        rig.store.value(&bookmark).await,
        Some(bookmark_value(&first.0, MARKS_PER_STEP - 1)),
        "one read went to the second's hold; the first keeps the rest"
    );
    let sent = rig.store.take_sent();
    let position = walk_position(&id);
    assert!(
        sent.iter().all(
            |(batch, _)| !batch.puts.iter().any(|(key, _)| *key == position)
                && !batch.deletes.contains(&position)
        ),
        "the page stays: no scan position is written"
    );
    assert!(marks_per_batch(&sent).is_empty());

    rig.drain().await.expect("no pass fails");
    assert_eq!(rig.store.value(&first.0).await, None, "the first retires");
    assert_eq!(rig.store.value(&second.0).await, Some(second.1));
    assert_eq!(rig.store.value(&position).await, None);
    assert_eq!(
        rig.store.value(&bookmark).await,
        Some(bookmark_value(&second.0, 240)),
        "and the second has its place back"
    );
}

// ─────────────────────────── the walk's own records and the pass ───────────────────────────

/// **(D, a bookmark and a session another drainer ended)** An `Aborting` session owns one wide
/// entry, every position already carrying the walk's own mark, and no bookmark: the first
/// drainer's step marks nothing and sends a batch that only keeps its place. A wrapper holds that
/// batch back while a second drainer runs whole passes to the session's end — it walks the entry
/// away, drains the obligation and commits the terminal delete, which removes its own bookmark —
/// and only then lets the first's batch reach the store. The bookmark key is absent again, so a
/// batch conditioned on that alone would put it back behind a session that is gone. It is
/// conditioned on the session record as read as well, and the store refuses it: no bookmark and
/// no scan position is left, the count went down once, and the first drainer holds nothing.
#[tokio::test]
async fn d_no_bookmark_is_put_once_another_drainer_has_ended_the_session() {
    let rig = Rig::new(6).await;
    let id = rig.session(95, &format!("{BUCKET}/d-overtaken")).await;
    let (entry, at) = wide(&id, 1, 95_100);
    rig.fleet.store_at(&at);
    rig.store.plant(vec![entry.clone()]).await;
    rig.abort(&id).await;
    let event = format!("{id}:{}", rig.epoch_of(&id).await);
    let mark = md::encode_orphan_mark(&OrphanMark::structured(T0, event).unwrap());
    let marks = at
        .iter()
        .map(|&(dserver, frag)| (orphan_key(dserver, frag), mark.clone()))
        .collect();
    rig.store.plant(marks).await;
    let counted = rig.sessions_counted().await;
    let session = rig.store.value(&mpu_key(&id)).await.unwrap();
    let bookmark = walk_bookmark(&id);
    let ran = Arc::new(AtomicBool::new(false));
    *rig.store.overtaken.lock().unwrap() = Some(Overtaken {
        put: bookmark.clone(),
        session: mpu_key(&id),
        rival: Rival {
            fleet: rig.fleet.clone(),
            clock: rig.clock.clone(),
            ran: Arc::clone(&ran),
        },
    });
    rig.store.take_sent();

    let outcome = rig.drain().await.expect("the pass never fails");

    assert!(
        ran.load(Ordering::SeqCst),
        "the rival ended the session first"
    );
    assert_ne!(
        outcome,
        Reconciled::Blocked,
        "the first drainer holds nothing"
    );
    assert!(rig.audit.is_empty(), "nothing audited");
    let sent = rig.store.take_sent();
    let puts: Vec<_> = sent
        .iter()
        .filter(|(batch, _)| batch.puts.iter().any(|(key, _)| *key == bookmark))
        .collect();
    assert_eq!(puts.len(), 1, "the first drainer sent its place once");
    assert_eq!(
        puts[0].1,
        Some(CommitOutcome::Conflict),
        "and it was refused"
    );
    assert!(
        puts[0]
            .0
            .preconditions
            .iter()
            .any(|p| p.key == mpu_key(&id) && p.expected.as_ref() == Some(&session)),
        "conditioned on the session record as read: {:?}",
        puts[0].0.preconditions
    );
    assert_eq!(rig.store.value(&mpu_key(&id)).await, None);
    assert_eq!(rig.store.value(&entry.0).await, None);
    for _ in 0..2 {
        rig.drain().await.expect("the pass never fails");
    }
    assert!(
        rig.store.under(b"custodian:owned-walk:").await.is_empty(),
        "no bookmark is left behind the session"
    );
    assert!(rig.store.under(b"custodian:owned-scan:").await.is_empty());
    assert_eq!(rig.sessions_counted().await, counted - 1, "released once");
}

/// What holds each of sixteen sessions listed ahead of the ones with work to do
/// ([`held_sessions_spend_no_step`]).
#[derive(Clone, Copy, Debug)]
enum Churn {
    /// Three wide entries, each with a mark of none of the three shapes at its position 150. One
    /// step cannot re-read all three up to their holds, so the walk laps them, a page a pass.
    PartWay,
    /// A wide entry with such a mark at its position 240, fifteen wrong-length entries held while
    /// server 5 is left out at startup, and a second wide entry with such a mark at its position
    /// 250: two entries, on two pages of the walk, each held deeper than one step reads. The one
    /// bookmark can count only one of them, so the walk re-reads the other from its first position
    /// every lap — a step that holds nothing — and then finds its hold: the bookmark moves every
    /// pass.
    PastOneStep,
}

/// Whether `batch` writes nothing but the owned walk's own records: bookmarks and scan positions.
fn walks_own_records(batch: &WriteBatch) -> bool {
    let own = |key: &Vec<u8>| {
        key.starts_with(b"custodian:owned-walk:") || key.starts_with(b"custodian:owned-scan:")
    };
    batch
        .puts
        .iter()
        .map(|(key, _)| key)
        .chain(&batch.deletes)
        .all(own)
}

/// Sixteen `Aborting` sessions numbered from `base` — as many as one pass has steps — are held as
/// `churn` says, and listed after them, the seventeenth onward, are four rounds of eight sessions,
/// each with one residue entry and `Open` until its round. Passes first settle the sixteen: the
/// walk marks what it can of them, until four passes in a row — a lap of the longest walk here —
/// commit nothing of the session leg's but the walk's own records. Then, round by round, eight
/// sessions are aborted and **one** pass runs. It marks and deletes all eight residue entries —
/// two steps each, every step the pass has — so not one of the sixteen spent a step, on whichever
/// page its walk stood: the pass's sixteen batches that mark or delete are the eight sessions'
/// own. Each of the sixteen commits at most three batches that pass, its bookmark and scan
/// position alone. For `PartWay` it never writes its bookmark, which is at rest. For `PastOneStep`
/// it writes it every pass, and over the four rounds both in a pass that names the session in no
/// hold — the step ran out of reads before it reached one — and in a pass that does: neither kind
/// of step is charged. Every one of the sixteen is named to the operator within the four rounds.
/// More passes then end all thirty-two sessions, the count down by exactly that; the sixteen stay,
/// every entry byte-identical.
async fn held_sessions_spend_no_step(base: u64, churn: Churn) {
    let rig = Rig::new(6).await;
    let view = match churn {
        Churn::PartWay => View::deployed(),
        Churn::PastOneStep => View::without(5, false),
    };
    let damage =
        |at: (DServerId, FragmentId)| (orphan_key(at.0, at.1), Bytes::from_static(b"not-a-mark"));
    let mut sixteen = Vec::new();
    for n in base..base + 16 {
        let id = rig.session(n, &format!("{BUCKET}/d-churn-{n}")).await;
        let first = ChunkId::from(n) * 1_000;
        let mut entries = Vec::new();
        let mut damaged = Vec::new();
        match churn {
            Churn::PartWay => {
                for p in 1..=3_u32 {
                    let (entry, at) = wide(&id, p, first + ChunkId::from(p));
                    damaged.push(damage(at[150]));
                    entries.push(entry);
                }
            }
            Churn::PastOneStep => {
                for p in 1..=17_u32 {
                    let chunk = first + ChunkId::from(p);
                    if p == 1 || p == 17 {
                        let (entry, at) = wide(&id, p, chunk);
                        damaged.push(damage(at[if p == 1 { 240 } else { 250 }]));
                        entries.push(entry);
                    } else {
                        entries.push(untrusted_entry(&id, p, chunk, false));
                    }
                }
            }
        }
        rig.store.plant(entries.clone()).await;
        rig.store.plant(damaged).await;
        rig.abort(&id).await;
        sixteen.push((id, entries));
    }
    // A later round's sessions are listed ahead of an earlier round's, so the terminal deletes
    // the earlier ones still wait for never come before the round's own work.
    let mut rounds = Vec::new();
    for r in 0..4_u64 {
        let mut round = Vec::new();
        for j in 0..8_u64 {
            let n = base + 100 + (3 - r) * 10 + j;
            let id = rig.session(n, &format!("{BUCKET}/d-work-{n}")).await;
            let chunk = ChunkId::from(n) * 1_000;
            let residue = owned(&id, 1, chunk, RS21, vec![0, 1, 2]);
            let at = diagonal(chunk, &[0, 1, 2]);
            rig.fleet.store_at(&at);
            rig.store.plant(vec![residue.clone()]).await;
            round.push((id, residue, at));
        }
        rounds.push(round);
    }
    let counted = rig.sessions_counted().await;
    rig.store.take_sent();

    // `PartWay` holds something every pass. A `PastOneStep` pass in which every step ran out of
    // reads before it reached a hold names none, and answers for the bookmarks it moved.
    let holds_or_moves = |outcome: Reconciled| match churn {
        Churn::PartWay => outcome == Reconciled::Blocked,
        Churn::PastOneStep => matches!(outcome, Reconciled::Blocked | Reconciled::Changed),
    };
    // Settled: a whole lap of the longest walk here — four passes — marks and deletes nothing.
    let (mut settling, mut quiet) = (0, 0);
    while quiet < 4 {
        assert!(settling < 40, "{churn:?}: the sixteen settle");
        let outcome = rig.drain_seeing(&view).await.expect("no pass fails");
        assert!(holds_or_moves(outcome), "{churn:?}, settling {settling}");
        settling += 1;
        let sent = rig.store.take_sent();
        if session_leg(&sent).into_iter().all(walks_own_records) {
            quiet += 1;
        } else {
            quiet = 0;
        }
    }
    rig.audit.clear();

    // For each of the sixteen: whether a round named it in a hold, and whether its bookmark was put
    // in a round that did not, and in one that did.
    let mut seen = vec![(false, false, false); sixteen.len()];
    for (r, round) in rounds.iter().enumerate() {
        for (id, _, _) in round {
            rig.abort(id).await;
        }
        let outcome = rig.drain_seeing(&view).await.expect("no pass fails");
        assert!(holds_or_moves(outcome), "{churn:?}, round {r}");
        let sent = rig.store.take_sent();
        let events = rig.audit.events.lock().unwrap().clone();
        rig.audit.clear();
        for (id, (key, _), at) in round {
            assert_eq!(
                rig.store.value(key).await,
                None,
                "{churn:?}, round {r}: {id}'s residue is walked away in this one pass"
            );
            let event = format!("{id}:{}", rig.epoch_of(id).await);
            rig.assert_marked(at, &event).await;
            assert_marked_before_deleted(&sent, at, key);
        }
        let batches = session_leg(&sent);
        let work = batches
            .iter()
            .filter(|batch| !walks_own_records(batch))
            .count();
        assert_eq!(work, 16, "{churn:?}, round {r}: every step the pass has");
        for ((id, _), seen) in sixteen.iter().zip(&mut seen) {
            let (bookmark, position) = (walk_bookmark(id), walk_position(id));
            let touches = |batch: &&&WriteBatch, own: &Vec<u8>| {
                batch.puts.iter().any(|(key, _)| key == own) || batch.deletes.contains(own)
            };
            let theirs: Vec<_> = batches
                .iter()
                .filter(|batch| touches(batch, &bookmark) || touches(batch, &position))
                .collect();
            assert!(
                theirs.len() <= 3 && theirs.iter().all(|batch| walks_own_records(batch)),
                "{churn:?}, round {r}: {id} writes its own two records at most: {theirs:?}"
            );
            let key = name(&mpu_key(id));
            let named = events.iter().any(|e| {
                e.get("action").map(String::as_str) == Some("retire-session-held")
                    && e["key"] == key
            });
            let put = theirs
                .iter()
                .any(|batch| batch.puts.iter().any(|(key, _)| *key == bookmark));
            match churn {
                Churn::PartWay => assert!(
                    !theirs.iter().any(|batch| touches(batch, &bookmark)),
                    "{churn:?}, round {r}: {id}'s bookmark is at rest: {theirs:?}"
                ),
                Churn::PastOneStep => assert!(
                    put,
                    "{churn:?}, round {r}: {id}'s bookmark moves every pass: {theirs:?}"
                ),
            }
            seen.0 |= named;
            seen.1 |= put && !named;
            seen.2 |= put && named;
        }
    }
    // The four rounds are a lap of the longest walk here: every one of the sixteen was named to
    // the operator in it, though none spent a step.
    for ((id, _), (named, moved_unheld, moved_held)) in sixteen.iter().zip(seen) {
        assert!(named, "{churn:?}: {id} is named within the lap");
        if let Churn::PastOneStep = churn {
            assert!(
                moved_unheld && moved_held,
                "{churn:?}: {id}'s bookmark moved in a step that held nothing \
                 ({moved_unheld}) and in one that held ({moved_held})"
            );
        }
    }

    let mut passes = 0;
    loop {
        let mut left = 0;
        for (id, _, _) in rounds.iter().flatten() {
            if rig.store.value(&mpu_key(id)).await.is_some() {
                left += 1;
            }
        }
        if left == 0 {
            break;
        }
        assert!(
            passes < 16,
            "{churn:?}: {left} sessions left after {passes} passes"
        );
        rig.drain_seeing(&view).await.expect("no pass fails");
        passes += 1;
    }
    assert_eq!(rig.sessions_counted().await, counted - 32);
    for (id, entries) in &sixteen {
        assert!(rig.store.value(&mpu_key(id)).await.is_some(), "{id} stays");
        for (key, value) in entries {
            assert_eq!(rig.store.value(key).await.as_ref(), Some(value));
        }
    }
}

/// **(D, held sessions and the pass's steps, held part-way)** Sixteen sessions, each with three
/// wide entries held part-way: their re-reads move nothing, so they spend no step, and the
/// sessions listed after them are drained at once.
#[tokio::test]
async fn d_sixteen_sessions_held_part_way_spend_no_step() {
    held_sessions_spend_no_step(300, Churn::PartWay).await;
}

/// **(D, held sessions and the pass's steps, held past one step)** Sixteen sessions whose bookmark
/// moves every pass — in a step that holds nothing, then in one that holds — spend no step either:
/// a batch that only moves the bookmark is not one of the pass's steps.
#[tokio::test]
async fn d_sixteen_sessions_held_past_one_step_spend_no_step() {
    held_sessions_spend_no_step(500, Churn::PastOneStep).await;
}
