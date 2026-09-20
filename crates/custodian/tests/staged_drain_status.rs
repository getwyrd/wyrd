//! Issue #808 (664.1) — **the drain-status surface counts staged multipart bytes**
//! (proposal 0016 decision 2: the drain row
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md:826-827`, the rebalance row
//! `:881`, and the two failure-table rows `:883`).
//!
//! A multipart upload's bytes are durable long before they are published: a committed part's
//! fragments are named by its `part:` record, and a still-streaming part's by the owned `sidx:`
//! entries staged for it. On `main` `reconciliation_status` computed "satisfied" from the
//! **committed** placement records alone (`crates/custodian/src/desired_state.rs:191-197`), so a
//! D server holding nothing but a live upload's staged fragments answered `Satisfied` — the F6
//! trace: the operator is told the box is safe to wipe, wipes it, the part then commits and a
//! Complete publishes a chunk map naming bytes that are gone.
//!
//! Every leg drives the production entry points — `reconciliation_status` and, for (D), the real
//! `reconcile_step` fenced control point with a `RebalanceContext` — over in-memory doubles. No
//! client can create a session before the S3 verbs (#508), so every staged record is seeded, as
//! the raw JSON the base decoders accept (the shapes of
//! `crates/core/tests/multipart_session_records.rs:81-145`), and each value is round-tripped
//! through `decode_session_record` / `decode_part_record` / `decode_owned_entry` before a pass
//! reads it.
//!
//! The legs:
//! - **A** an **in-flight** owned (`sidx:`) fragment holds the drain: `Pending`, not `Satisfied`.
//!   RED on base.
//! - **B** a **committed part** (`part:`) fragment holds it too, as its own case — an
//!   implementation counting only one of the two staged classes passes one of A and B and fails
//!   the other (`0016:883`). RED on base.
//! - **C** a draining server holding **none** of the staged fragments still drains: `Satisfied`.
//!   A guard, green on base — it is what fails a `*server != dserver` mutant that every other leg
//!   survives. Its liveness is a second test over the same fixture: every server that *does*
//!   carry one of those staged fragments answers `Pending`, so the guard can never pass by the
//!   query simply not seeing the records. That half is RED on base.
//! - **D** rebalance and drain agree (`0016:881`): a pass over a draining server holding **only**
//!   staged fragments writes no fragment anywhere and rewrites no `part:` record, while
//!   `reconciliation_status` answers `Pending`. The `Pending` half is RED on base. The same
//!   fixture then gets a committed fragment on the draining server and the pass **does** move it,
//!   so the "nothing was written" half is never the answer of a pass that could not write at all.
//! - **E** a staged record the query cannot read blocks every drain, and one it can read but not
//!   trust blocks them exactly as a committed map with an untrustworthy placement does. Both RED
//!   on base.
//!
//! Nothing here names a symbol the fix adds: every type, function and variant it uses is on
//! `main` already, so the whole file compiles against the base and the red is an assertion
//! failure, never a compile error.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::FragmentHeader;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::multipart::{
    decode_owned_entry, decode_part_record, decode_session_record, mpu_key, part_key, sidx_key,
    OwnedEntry, PartNumber, StagedPlacement, UploadId,
};
use wyrd_core::placement::Topology;
use wyrd_custodian::{
    reconcile_step, reconciliation_status, set_lifecycle, Custodian, DServerLifecycle, FencedZone,
    RebalanceContext, Reconciled, ReconciliationStatus,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    ScanPage, WriteBatch,
};

/// The instant every pass here runs at.
const NOW: u64 = 10_000;
/// An owned entry's lease: far past every pass's clock, so no leg turns on expiry.
const LEASE: u64 = NOW * 1_000;
/// The bucket and object every seeded session targets.
const PARENT: InodeId = 42;
const OBJECT: &str = "staged/object";
/// Every seeded session's epoch.
const EPOCH: u64 = 3;
/// An erasure-coded scheme: three fragments, so a one-server placement under it is the
/// wrong-length (untrusted) shape leg (E)(ii) needs.
const RS_2_1: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };

// ---- the in-memory doubles ----------------------------------------------------------------

/// A trivial in-memory metadata store with version-conditional commit, ordered so a snapshot of
/// it compares directly against a later one.
#[derive(Default)]
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
}

impl Meta {
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    /// Every key/value under `prefix`, read around the store's own `scan`.
    fn records_under(&self, prefix: &[u8]) -> BTreeMap<Vec<u8>, Bytes> {
        self.kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Every record of an upload's own three namespaces, so a leg can assert a pass rewrote
    /// none of them.
    fn upload_records(&self) -> BTreeMap<Vec<u8>, Bytes> {
        [b"mpu:".as_slice(), b"part:".as_slice(), b"sidx:".as_slice()]
            .into_iter()
            .flat_map(|prefix| self.records_under(prefix))
            .collect()
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    // The required paginated read (#634): a test double needs *a* body, not a backend's — the
    // dev-only testkit helper pages over this store's own `scan`.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (key, value) in batch.puts {
            kv.insert(key, value);
        }
        for key in batch.deletes {
            kv.remove(&key);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// One D server's fragment bytes.
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

/// The four-D-server fleet every leg runs over.
fn disks() -> [Disk; 4] {
    [
        Disk::default(),
        Disk::default(),
        Disk::default(),
        Disk::default(),
    ]
}

/// A four-domain topology A..D over servers 0..3 — so an evacuation off any one server always
/// has three distinct domains left to land in.
fn four_domains() -> Topology {
    let mut topology = Topology::default();
    topology
        .register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(3, "D");
    topology
}

/// Every fragment on the fleet with its bytes, in a stable order — so "this pass wrote no
/// fragment anywhere" is one comparison of two readings, not a per-server spot check.
fn inventory(disks: &[Disk; 4]) -> Vec<(DServerId, ChunkId, u16, Bytes)> {
    let mut all = Vec::new();
    for (server, disk) in disks.iter().enumerate() {
        for (frag, bytes) in disk.frags.lock().unwrap().iter() {
            all.push((server as DServerId, frag.chunk, frag.index, bytes.clone()));
        }
    }
    all.sort_by_key(|entry| (entry.0, entry.1, entry.2));
    all
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-staged-drain").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

// ---- hand-authored records ------------------------------------------------------------------

fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn part_no(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number in range")
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Put a real v1 single-copy fragment of `frag`'s chunk on `dserver` — bytes an evacuation's
/// full-identity verify accepts (`wyrd_core::repair::fragment_intact`), so leg (D)'s control
/// move is a real one.
fn place(disks: &[Disk; 4], dserver: DServerId, frag: FragmentId) {
    let payload = b"staged";
    let bytes = wyrd_chunk_format::encode(
        &FragmentHeader::new_v1(frag.chunk, payload.len() as u64),
        payload,
    );
    disks[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .insert(frag, Bytes::from(bytes));
}

/// An `Open` session record, spelled as the base decoder's own encoding and round-tripped
/// through it.
fn session_open() -> Bytes {
    let bytes = format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":{EPOCH},\"attempts\":1,\
         \"state\":{{\"kind\":\"Open\"}}}}"
    )
    .into_bytes();
    let record = decode_session_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded session must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded session must be the decoder's own spelling"
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
/// one decodes too.
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

/// An owned staging entry of `owner`, planned as `placement` under `scheme`, round-tripped
/// through `decode_owned_entry` under `key`. A placement's length is not a decode-time rule here
/// either.
fn owned(owner: &UploadId, key: &[u8], scheme: EcScheme, placement: &[DServerId]) -> Bytes {
    let staged = StagedPlacement::new(scheme, placement.to_vec()).expect("a supported scheme");
    let value = metadata::encode(&OwnedEntry::new(owner.clone(), LEASE, staged).to_pending());
    decode_owned_entry(key, &value)
        .unwrap_or_else(|fault| panic!("the seeded owned entry must decode: {fault}"));
    value
}

/// A committed object at `inode`, seeded straight under its `inode:` key — the shape every
/// consumer of the committed reference set scans for. Written directly rather than through
/// `metadata::create` so a leg may seed a MALFORMED placement, which the publication check
/// rejects (`crates/custodian/tests/rebalance.rs:1449-1455` does the same).
fn commit_object(meta: &Meta, inode: InodeId, chunks: &[ChunkRef]) {
    let record = InodeRecord {
        size: chunks.iter().map(|chunk| chunk.len).sum(),
        chunk_map: chunks.to_vec().into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.seed(metadata::inode_key(inode), metadata::encode(&record));
}

/// The name the drain answer gives a record: the key as the store spells it. Every key this file
/// seeds is printable ASCII, which `crate::gc::object_name`'s escaping leaves unchanged.
fn record_name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).expect("a printable-ASCII key")
}

// ---- audit capture --------------------------------------------------------------------------

/// A `MakeWriter` collecting what the subscriber emits, so leg (E)(i) asserts on the audit line
/// the query actually produced rather than assuming one exists (the in-tree pattern of
/// `crates/custodian/tests/segmented_map_consumers.rs:296-344`).
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

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

/// Install a permissive global `tracing` default **once**, so the audit callsites never latch
/// `Interest::never` under the parallel test harness: `tracing` caches each callsite's interest
/// in process-global state the first time it is hit, so a sibling test in this binary that hits
/// the callsite with no subscriber installed could otherwise disable it for the whole process
/// and leave the capture below empty (issue #214). Called at the top of EVERY test here, so
/// whichever the harness schedules first is the one that installs the default.
fn enable_audit_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

fn capturing_dispatch(capture: Capture) -> tracing::Dispatch {
    tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().json().with_writer(capture)),
    )
}

// ---- (A) an in-flight owned fragment holds the drain -----------------------------------------

/// **(A)** A server holding **only** an owned `sidx:` fragment — a part still streaming, with no
/// `part:` record for it yet — is not drained: `reconciliation_status` answers `Pending`.
///
/// The sharpest form of the F6 trace (`0016:827`): nothing committed names this chunk at all, so
/// a drain computed from committed placements alone sees an empty reference set and certifies the
/// server. The operator wipes the disk, the part then commits, and a Complete publishes a chunk
/// map naming bytes that are gone.
///
/// RED on base: `Satisfied`.
#[tokio::test]
async fn an_in_flight_owned_fragment_holds_the_drain() {
    enable_audit_callsites();
    const CHUNK: ChunkId = 0x0A01;
    const HOLDER: DServerId = 1;

    let meta = Meta::default();
    let disks = disks();
    let id = upload("a1");
    meta.seed(mpu_key(&id), session_open());
    let key = sidx_key(&id, part_no(1), CHUNK);
    meta.seed(key.clone(), owned(&id, &key, EcScheme::None, &[HOLDER]));
    place(&disks, HOLDER, frag(CHUNK, 0));

    // The committed namespace is EMPTY: the only thing that can hold this drain is the staged
    // record, so the answer below is about that record and nothing else.
    assert!(
        meta.records_under(b"inode:").is_empty(),
        "the fixture must commit no object, or a committed reference could hold the drain instead"
    );

    set_lifecycle(&meta, HOLDER, DServerLifecycle::Draining)
        .await
        .unwrap();
    let status = reconciliation_status(&meta, HOLDER).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "a drain over a server holding a live upload's in-flight (`sidx:`) fragment must answer \
         `Pending` — `Satisfied` tells the operator to wipe bytes the part is about to commit and \
         a Complete is about to publish a map over (0016:827); got {status:?}"
    );
}

// ---- (B) a committed part's fragment holds the drain -----------------------------------------

/// **(B)** A server holding **only** a committed part's (`part:`) fragment is not drained
/// either — its own case, because the two staged classes reach the set through two different
/// readers: an owned entry's *planned* placement and a part record's *recorded* one. An
/// implementation that counts one of them passes exactly one of (A) and (B) (`0016:883`).
///
/// RED on base: `Satisfied`.
#[tokio::test]
async fn a_committed_parts_fragment_holds_the_drain() {
    enable_audit_callsites();
    const CHUNK: ChunkId = 0x0B01;
    const HOLDER: DServerId = 2;

    let meta = Meta::default();
    let disks = disks();
    let id = upload("b1");
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[chunk_ref(CHUNK, EcScheme::None, &[HOLDER])]),
    );
    place(&disks, HOLDER, frag(CHUNK, 0));

    assert!(
        meta.records_under(b"inode:").is_empty(),
        "the fixture must commit no object, or a committed reference could hold the drain instead"
    );

    set_lifecycle(&meta, HOLDER, DServerLifecycle::Draining)
        .await
        .unwrap();
    let status = reconciliation_status(&meta, HOLDER).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "a drain over a server holding a committed part's staged fragment must answer `Pending` \
         — the part is durable and unpublished, and a Complete will publish a map naming it \
         (0016:826); got {status:?}"
    );
}

// ---- (C) a server holding none of them still drains ------------------------------------------

/// Leg (C)'s store, shared by the guard and its liveness twin below so both plainly run over the
/// SAME one: one `Open` session whose committed part sits on server 0 and whose two in-flight
/// owned entries sit on servers 1 and 2, with every server in the fleet marked draining. Server 3
/// carries no staged fragment and no committed reference at all.
const C_BASE: ChunkId = 0x0C00;
const C_EMPTY: DServerId = 3;

async fn seed_three_holders(meta: &Meta, disks: &[Disk; 4]) {
    let id = upload("c1");
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[chunk_ref(C_BASE + 1, EcScheme::None, &[0])]),
    );
    place(disks, 0, frag(C_BASE + 1, 0));
    for (number, holder, chunk) in [(2, 1, C_BASE + 2), (3, 2, C_BASE + 3)] {
        let key = sidx_key(&id, part_no(number), chunk);
        meta.seed(key.clone(), owned(&id, &key, EcScheme::None, &[holder]));
        place(disks, holder, frag(chunk, 0));
    }
    for server in [0, 1, 2, C_EMPTY] {
        set_lifecycle(meta, server, DServerLifecycle::Draining)
            .await
            .unwrap();
    }
}

/// **(C)** Staged fragments on servers 0, 1 and 2; server 3 holds none of them and no committed
/// reference. Its drain is `Satisfied` — counting the staged class must not turn "somebody else
/// holds staged bytes" into "every drain waits".
///
/// A guard: green on base, and green under the fix. It is the leg that kills the
/// `*server != dserver` mutant that iteration 1 of #664 left alive — every other leg here passes
/// under that mutant, and this one would answer `Pending` for a server no record names. Its
/// liveness (that the query does see this store's staged records at all) is the next test, over
/// the same fixture.
#[tokio::test]
async fn a_server_holding_none_of_the_staged_bytes_still_drains() {
    enable_audit_callsites();
    let meta = Meta::default();
    let disks = disks();
    seed_three_holders(&meta, &disks).await;

    let status = reconciliation_status(&meta, C_EMPTY).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Satisfied,
        "a draining server that holds none of the store's staged fragments — and no committed \
         reference — must still drain; a drain that waits on somebody else's staged bytes never \
         finishes; got {status:?}"
    );
}

/// The guard above, read from the other side: in that same store every server that DOES carry a
/// staged fragment answers `Pending`. So the `Satisfied` it asserts is the query telling servers
/// apart, never the query failing to see the staged records at all.
///
/// RED on base: all three answer `Satisfied`.
#[tokio::test]
async fn every_server_that_does_carry_staged_bytes_holds_its_own_drain() {
    enable_audit_callsites();
    let meta = Meta::default();
    let disks = disks();
    seed_three_holders(&meta, &disks).await;

    for holder in [0, 1, 2] {
        let status = reconciliation_status(&meta, holder).await.unwrap();
        assert_eq!(
            status,
            ReconciliationStatus::Pending,
            "server {holder} carries a staged fragment of this store's one upload, so its drain \
             must answer `Pending`; got {status:?}"
        );
    }
}

// ---- (D) rebalance and drain agree -----------------------------------------------------------

/// **(D)** For a draining server holding **only** staged fragments, one real rebalance pass
/// (through `reconcile_step`) writes no fragment anywhere and rewrites no `part:` / `sidx:` /
/// `mpu:` record, **and** `reconciliation_status` answers `Pending` (`0016:881`).
///
/// The two halves are one contract: the staged class is *disjoint* from the committed reference
/// set rather than merged into it, so rebalance — which scans `inode:` — plans nothing for a
/// staged chunk, while the operator's per-server query counts it as held. Merging the two sets
/// would make these two answers contradict each other, which is exactly the failure 0016's table
/// names.
///
/// The pass answers `Reconciled::Satisfied` here, and that is honest: it is the loop's answer
/// about *its own* question — "is there committed content left to evacuate" — not the operator's
/// drain verdict, which is the `Pending` asserted below (`crates/custodian/src/rebalance.rs:194`).
///
/// The `Pending` half is RED on base. The control at the end puts a *committed* fragment on the
/// same draining server and shows the pass moves it — so "nothing was written" is never the
/// answer of a pass that could not have written anything.
#[tokio::test]
async fn rebalance_leaves_staged_bytes_alone_while_the_drain_stays_pending() {
    enable_audit_callsites();
    const PART_CHUNK: ChunkId = 0x0D01;
    const OWNED_CHUNK: ChunkId = 0x0D02;
    const ELSEWHERE: ChunkId = 0x0D03;
    const COMMITTED: ChunkId = 0x0D04;
    const DRAIN: DServerId = 0;

    let meta = Meta::default();
    let disks = disks();
    let topology = four_domains();

    // The draining server holds ONLY staged bytes: a committed part's fragment and an in-flight
    // owned entry's.
    let id = upload("d1");
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[chunk_ref(PART_CHUNK, EcScheme::None, &[DRAIN])]),
    );
    let owned_key = sidx_key(&id, part_no(2), OWNED_CHUNK);
    meta.seed(
        owned_key.clone(),
        owned(&id, &owned_key, EcScheme::None, &[DRAIN]),
    );
    place(&disks, DRAIN, frag(PART_CHUNK, 0));
    place(&disks, DRAIN, frag(OWNED_CHUNK, 0));

    // ...and a committed object well away from it, so the pass has a real committed namespace to
    // walk rather than an empty one.
    commit_object(&meta, 1, &[chunk_ref(ELSEWHERE, EcScheme::None, &[1])]);
    place(&disks, 1, frag(ELSEWHERE, 0));

    set_lifecycle(&meta, DRAIN, DServerLifecycle::Draining)
        .await
        .unwrap();

    let fleet: [(DServerId, &dyn ChunkStore); 4] = [
        (0, &disks[0]),
        (1, &disks[1]),
        (2, &disks[2]),
        (3, &disks[3]),
    ];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &fleet,
        topology: &topology,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;

    let before_fragments = inventory(&disks);
    let before_records = meta.upload_records();
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), NOW)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the rebalance pass has no committed content to evacuate off the draining server, so it \
         reports its own pass converged — the operator's drain verdict is the per-server query \
         asserted below, not this; got {outcome:?}"
    );
    assert_eq!(
        inventory(&disks),
        before_fragments,
        "a rebalance pass over a draining server holding only staged fragments must write no \
         fragment anywhere and delete none (0016:881)"
    );
    assert_eq!(
        meta.upload_records(),
        before_records,
        "a rebalance pass must rewrite no `part:` / `sidx:` / `mpu:` record — a staged chunk's \
         placement is repointed only under the session fence, never by this loop (0016:875, :881)"
    );

    let status = reconciliation_status(&meta, DRAIN).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "while staged fragments sit on the draining server the operator's drain query must answer \
         `Pending`, even though the rebalance loop has nothing to move: the bytes leave when the \
         upload publishes, aborts or is reaped, not when a pass evacuates them (0016:881); got \
         {status:?}"
    );

    // ---- the control: the same pass DOES move committed content off the same server ----
    commit_object(&meta, 2, &[chunk_ref(COMMITTED, EcScheme::None, &[DRAIN])]);
    place(&disks, DRAIN, frag(COMMITTED, 0));
    let staged_records = meta.upload_records();

    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), NOW)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the control must actually evacuate, or the assertions above are a pass that could not \
         write at all; got {outcome:?}"
    );
    assert!(
        disks
            .iter()
            .enumerate()
            .any(|(server, disk)| server as DServerId != DRAIN
                && disk.frags.lock().unwrap().contains_key(&frag(COMMITTED, 0))),
        "the evacuated committed fragment must have been copied onto a server that is not \
         draining"
    );
    let repointed: InodeRecord =
        metadata::decode(&meta.get(&metadata::inode_key(2)).await.unwrap().unwrap()).unwrap();
    assert!(
        !repointed.chunk_map.as_flat().unwrap()[0]
            .placement
            .contains(&DRAIN),
        "the committed placement record must no longer name the draining server — the copy on \
         the draining server is now an orphan GC reclaims after its grace window, which is why \
         this half is asserted on the RECORD, not on the source bytes"
    );

    // ...and the staged bytes and records beside it are untouched by that same pass.
    for chunk in [PART_CHUNK, OWNED_CHUNK] {
        assert!(
            disks[DRAIN as usize]
                .get_fragment(frag(chunk, 0))
                .await
                .unwrap()
                .is_some(),
            "the staged fragment of chunk {chunk:#x} must stay on the draining server — a pass \
             that evacuates committed content must still leave staged bytes alone"
        );
    }
    assert_eq!(
        meta.upload_records(),
        staged_records,
        "the evacuating pass must still rewrite no upload record"
    );
    let status = reconciliation_status(&meta, DRAIN).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "no committed placement names the draining server any more, but the staged records still \
         do: the drain is still not finished; got {status:?}"
    );
}

// ---- (E) an unreadable or untrusted staged record never yields `Satisfied` -------------------

/// **(E)(i)** A staged record the query cannot read at all blocks **every** drain, and the answer
/// names the record to repair.
///
/// The containment `StagedSet::protects` already applies on the reclamation side, read here on
/// the certification side: an unreadable upload record hides *which* chunks its session owns, so
/// no server can be shown not to hold one of them. Blocked cluster-wide, deliberately not scoped
/// to servers the damaged record might name.
///
/// RED on base: `Satisfied` for both servers.
#[tokio::test]
async fn a_staged_record_the_query_cannot_read_blocks_every_drain() {
    enable_audit_callsites();
    const READABLE: ChunkId = 0x0E01;
    const HOLDER: DServerId = 1;

    let meta = Meta::default();
    let disks = disks();
    let id = upload("e1");
    meta.seed(mpu_key(&id), session_open());
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[chunk_ref(READABLE, EcScheme::None, &[HOLDER])]),
    );
    place(&disks, HOLDER, frag(READABLE, 0));
    // A second part record whose value will not decode: the key is one a writer spells, so the
    // damage is not attributable to any one chunk — the whole class is incomplete.
    let damaged = part_key(&id, part_no(2));
    meta.seed(
        damaged.clone(),
        Bytes::from_static(b"{\"chunks\":\"not a chunk list\"}"),
    );

    // Servers 2 and 3 hold nothing at all — on the base both drain.
    for server in [2, 3] {
        set_lifecycle(&meta, server, DServerLifecycle::Draining)
            .await
            .unwrap();
    }

    let expected = ReconciliationStatus::PendingUnresolvable {
        objects: vec![record_name(&damaged)],
    };
    for server in [2, 3] {
        let audit = Capture::default();
        let status = reconciliation_status(&meta, server)
            .with_subscriber(capturing_dispatch(audit.clone()))
            .await
            .unwrap();
        assert_eq!(
            status, expected,
            "a staged record the query cannot read must block server {server}'s drain too — \
             which chunks that upload owns is exactly what could not be read — and the answer \
             must NAME the record to repair, or the stall has no way out; got {status:?}"
        );
        let logged = audit.contents();
        assert!(
            logged.contains(r#""target":"wyrd.custodian.drain.audit""#)
                && logged.contains(r#""action":"unresolvable-staged-record""#)
                && logged.contains(&format!(r#""record":"{}""#, record_name(&damaged))),
            "the blocker must also be attributed on the durability seam, so a collector that \
             watches the plane rather than polling the status still sees which record to repair. \
             got: {logged}"
        );
    }
}

/// **(E)(ii)** A staged record the query can read but cannot **trust** — a placement that is not
/// one D server per fragment — blocks every drain the way a committed map with an untrustworthy
/// placement does: the same `PendingMalformed` answer, naming the chunk.
///
/// Where its fragments actually sit is what is unknown, so the block is cluster-wide and the
/// chunk is named. The second half seeds a malformed *committed* placement beside it and asserts
/// the two classes merge into one sorted list of blockers — one answer an operator works through,
/// not two shapes depending on which class happened to be damaged.
///
/// RED on base: `Satisfied` (first half) and only the committed chunk (second half).
#[tokio::test]
async fn a_staged_record_the_query_cannot_trust_blocks_every_drain() {
    enable_audit_callsites();
    const HELD: ChunkId = 0x0E22;
    const MALFORMED_COMMITTED: ChunkId = 0x0E11;

    let meta = Meta::default();
    let id = upload("e2");
    meta.seed(mpu_key(&id), session_open());
    // An owned entry for an RS(2,1) chunk — three fragments — whose planned placement names one
    // D server. Its length is not a decode-time rule, so the record decodes and the class holds
    // the chunk whole instead.
    let key = sidx_key(&id, part_no(1), HELD);
    meta.seed(key.clone(), owned(&id, &key, RS_2_1, &[1]));

    for server in [2, 3] {
        set_lifecycle(&meta, server, DServerLifecycle::Draining)
            .await
            .unwrap();
    }

    for server in [2, 3] {
        let status = reconciliation_status(&meta, server).await.unwrap();
        assert_eq!(
            status,
            ReconciliationStatus::PendingMalformed { chunks: vec![HELD] },
            "a staged record whose placement cannot be trusted must block server {server}'s drain \
             — it cannot be trusted to *not* name that server — and the answer must attribute the \
             stall to the chunk; got {status:?}"
        );
    }

    // The committed twin of the same rule, in the same store: one answer, both blockers, sorted.
    commit_object(&meta, 1, &[chunk_ref(MALFORMED_COMMITTED, RS_2_1, &[1])]);
    let status = reconciliation_status(&meta, 2).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::PendingMalformed {
            chunks: vec![MALFORMED_COMMITTED, HELD],
        },
        "a drain blocked by both an untrustworthy committed placement and an untrustworthy staged \
         one must name both, in one sorted list — an operator repairs a list of records, not one \
         class at a time; got {status:?}"
    );
}
