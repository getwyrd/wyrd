//! **Issue #710 — a placement write that would not survive is refused, and a move that did
//! not persist neither certifies nor counts.**
//!
//! Two write-path defects on the **flat** repair/evacuation path, one rule:
//!
//! 1. Every placement mutation is `require(key, encode(prior))` + `put(key, encode(next))`,
//!    and nothing on the write path weighed `encode(next)` against the value ceiling every
//!    backend inherits ([`MAX_VALUE_BYTES`]). A repair that moves a fragment onto a
//!    twenty-digit [`DServerId`] re-encodes the record longer than it was — so a repair could
//!    commit a record the tightest backend would refuse, after which **every** later repair of
//!    that object fails: an object whose placement can never be maintained again.
//! 2. A move that did **not** persist still certified: the pass answered
//!    [`Reconciled::Satisfied`] over a drain that had not moved a byte, which tells an operator
//!    the box is safe to remove.
//!
//! Every leg drives the real fenced control point [`reconcile_step`] and observes the
//! **store**. The ceiling helper and the outcome variants the fix introduces are never named
//! here, so reverting the production change leaves this file compiling and RED (the base's own
//! behaviour) rather than failing to build.
//!
//! Legs:
//! 1. [`a_repair_that_would_cross_the_value_ceiling_is_refused_and_not_persisted`]
//! 2. [`an_evacuation_that_would_cross_the_value_ceiling_does_not_certify_the_drain`], with
//!    [`an_evacuation_landing_exactly_on_the_value_ceiling_still_commits`] pinning the
//!    ceiling's admissible side (the refusal is `>`, never `>=`) and
//!    [`a_move_that_cannot_reach_its_fragment_is_aborted_not_refused`] pinning that a compound
//!    failure is named by its *recoverable* cause
//! 3. [`a_ceiling_refused_repair_is_subtracted_from_the_reported_successes`], which carries the
//!    same precedence pin for the repair path
//!
//! Everything from the imports to `// ---- legs ----` is harness with no decision content: the
//! in-memory [`MetadataStore`] double every `custodian` integration test rolls for itself
//! (there is no shared one), one fleet builder, one record builder and the pass runners. The D
//! servers are the **real** `chunkstore-fs` backend precisely so this file needs no second
//! double, and every fixture is shared by more than one leg for the same reason.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tempfile::TempDir;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, MAX_VALUE_BYTES,
};
use wyrd_core::placement::Topology;
use wyrd_core::{erasure, repair, write};
use wyrd_custodian::{
    reconcile_step, reconciliation_status, set_lifecycle, Custodian, DServerLifecycle,
    DurabilityTelemetry, ExporterConfig, FencedZone, RebalanceContext, Reconciled,
    ReconciliationStatus, ReconstructionContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

/// The largest [`DServerId`] there is — the growth vector `crates/core/src/metadata.rs`'s
/// ceiling note names: one moved placement entry re-encoded from a one-digit id to a
/// twenty-digit one is ~19 bytes the record did not carry before.
const HUGE: DServerId = u64::MAX;
/// A second twenty-digit id, registered in the topology but deliberately **absent from the
/// fleet view** — the transient failure the compound leg pairs with an oversized record.
const GHOST: DServerId = u64::MAX - 1;
/// Every fixture's chunk is RS(2,1): two survivors are enough to rebuild, and one lost
/// fragment is one placement entry to move.
const SCHEME: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
const K: usize = 2;
const M: usize = 1;
/// The chunk payload. Deliberately tiny — what these fixtures size is the **record**, not the
/// data it points at.
const DATA: &[u8] = b"the payload every placement-ceiling fixture stores";
/// A record length comfortably clear of the ceiling, for the roots whose own size is
/// irrelevant to the outcome their plan takes.
const CLEAR_OF_CEILING: usize = 400;
/// The object every single-root fixture seeds and reads back.
const INODE: InodeId = 1;

// ---- harness: an in-memory metadata store; the D servers are the REAL fs backend ----

#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let kv = self.kv.lock().unwrap();
        Ok(kv
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        if batch
            .preconditions
            .iter()
            .any(|pre| kv.get(&pre.key).cloned() != pre.expected)
        {
            return Ok(CommitOutcome::Conflict);
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

/// The D servers a pass sees, each the real on-disk [`FsChunkStore`] rooted in its own temp
/// dir — which verifies a fragment's identity and checksum on both `put` and `get`, so a
/// fixture that seeded bytes the loops would not accept fails loudly here instead of quietly
/// proving nothing.
struct Fleet {
    ids: Vec<DServerId>,
    stores: Vec<FsChunkStore>,
    _dirs: Vec<TempDir>,
}

impl Fleet {
    fn new(ids: &[DServerId]) -> Self {
        let dirs: Vec<TempDir> = ids.iter().map(|_| tempfile::tempdir().unwrap()).collect();
        let open = |dir: &TempDir| FsChunkStore::open(dir.path()).unwrap();
        let stores = dirs.iter().map(open).collect();
        let (ids, _dirs) = (ids.to_vec(), dirs);
        Self { ids, stores, _dirs }
    }

    /// The fleet view a context takes: each id paired with its D server's store.
    fn view(&self) -> Vec<(DServerId, &dyn ChunkStore)> {
        let stores = self.stores.iter().map(|store| store as &dyn ChunkStore);
        self.ids.iter().copied().zip(stores).collect()
    }

    fn store(&self, id: DServerId) -> &FsChunkStore {
        let at = self.ids.iter().position(|held| *held == id);
        &self.stores[at.expect("the fixture asked for a D server it never built")]
    }
}

/// A topology of `(server, failure domain, utilization)` — utilization orders the free domains
/// the selector offers (cheapest first, ties by label), which is how one pass can hand three
/// plans three different targets.
fn topo(servers: &[(DServerId, &str, u64)]) -> Topology {
    let mut topology = Topology::default();
    for (id, domain, used) in servers {
        topology.register(*id, *domain).set_utilization(*id, *used);
    }
    topology
}

/// A committed **flat** root for one RS(2,1) `chunk` on `placement`, its ADR-0047
/// `content_type` — object metadata a placement-maintenance commit preserves verbatim
/// (`..prior.clone()`), so it sizes the record without touching anything the loops read —
/// padded so the whole record encodes to EXACTLY `len` bytes.
fn root(chunk: ChunkId, placement: Vec<DServerId>, version: u64, len: usize) -> InodeRecord {
    let chunk_ref = ChunkRef {
        id: chunk,
        scheme: SCHEME,
        len: DATA.len() as u64,
        placement,
    };
    let mut record = InodeRecord {
        size: DATA.len() as u64,
        chunk_map: ChunkMap::Flat(vec![chunk_ref]),
        state: InodeState::Committed,
        version,
        content_type: Some(String::new()),
        ..Default::default()
    };
    record.content_type = Some("x".repeat(len - metadata::encode(&record).len()));
    record
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Commit `record` at `inode` and store its chunk's real RS(2,1) fragments on the servers its
/// placement names — all but the one at `missing`, this pass's loss. Real shards through the
/// production encoder, so the loops' identity + checksum verify passes on every survivor.
async fn seed(
    meta: &MemMeta,
    fleet: &Fleet,
    inode: InodeId,
    record: &InodeRecord,
    missing: Option<usize>,
) {
    let key = metadata::inode_key(inode);
    let batch = WriteBatch::new().put(key, metadata::encode(record));
    meta.commit(batch).await.unwrap();
    let chunk = &record.chunk_map.as_flat().unwrap()[0];
    let shards = erasure::encode(K, M, DATA).unwrap();
    for (index, dserver) in chunk.placement.iter().enumerate() {
        if Some(index) == missing {
            continue;
        }
        let id = frag(chunk.id, index as u16);
        let shard = &shards[index];
        let bytes = write::encode_ec_fragment(id.chunk, id.index, K as u8, M as u8, shard);
        fleet.store(*dserver).put_fragment(id, bytes).await.unwrap();
    }
}

async fn enqueue(meta: &MemMeta, chunk: ChunkId) {
    repair::enqueue_repair(meta, chunk, "health").await.unwrap();
}

/// The committed record as the store holds it now.
async fn stored(meta: &MemMeta) -> Bytes {
    let key = metadata::inode_key(INODE);
    meta.get(&key).await.unwrap().unwrap()
}

/// Whether D server `id` holds `fragment` — the check that a refusal wrote **nothing**, not
/// merely no record.
async fn holds(fleet: &Fleet, id: DServerId, fragment: FragmentId) -> bool {
    let store = fleet.store(id);
    store.get_fragment(fragment).await.unwrap().is_some()
}

/// One fenced pass through the real control point: what it answered, and the durability
/// surface a deployment would have scraped from it.
async fn pass(
    repair: Option<&ReconstructionContext<'_>>,
    drain: Option<&RebalanceContext<'_>>,
) -> (Reconciled, String) {
    // A permissive global default, so the durability metric callsites never latch
    // `Interest::never` under the parallel harness (the loops' own tests do the same).
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-ceiling").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    let outcome = reconcile_step(&zone, &custodian, None, None, repair, drain, 500)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    telemetry.flush().unwrap();
    let exposed = telemetry.gather_prometheus().expect("Prometheus");
    (outcome, exposed)
}

/// One fenced **reconstruction** pass over the whole repair queue.
async fn repair_pass(meta: &MemMeta, fleet: &Fleet, topology: &Topology) -> (Reconciled, String) {
    let view = fleet.view();
    let ctx = ReconstructionContext {
        meta,
        fleet: &view,
        topology,
        unreachable: &[],
    };
    pass(Some(&ctx), None).await
}

/// A durability counter's value on that surface (the exporter suffixes a monotonic counter
/// `_total` and hangs the scope labels off the name; a `# HELP`/`# TYPE` line never starts
/// with the name itself).
fn counter(exposed: &str, name: &str) -> u64 {
    exposed
        .lines()
        .filter(|line| line.starts_with(name))
        .filter_map(|line| line.rsplit_once(' ')?.1.parse::<f64>().ok())
        .map(|value| value as u64)
        .sum()
}

// ---- legs ----
// ---- leg 1: a repoint that would cross the value ceiling is refused, not persisted ----

/// A committed flat root sits exactly ON the ceiling (a legal record). Its chunk has lost the
/// fragment on server 1, and the only free distinct domain left holds the twenty-digit
/// [`HUGE`] id — so the repointed record would grow ~19 bytes PAST the ceiling.
///
/// RED on the base: `repair_chunk`'s CAS weighs nothing, so the oversized record COMMITS —
/// `get(inode_key)` then returns bytes longer than the tightest backend would ever have
/// stored, the obligation is drained, and every later repair of that object would fail on the
/// `put`.
#[tokio::test]
async fn a_repair_that_would_cross_the_value_ceiling_is_refused_and_not_persisted() {
    const CHUNK: ChunkId = 0x00C0_FFEE;
    let meta = MemMeta::default();
    // Survivors on servers 0 ("a") and 2 ("c"); the lost fragment's server 1 is off the fleet,
    // so the one free distinct domain is "h" — held by the HUGE id.
    let fleet = Fleet::new(&[0, 2, HUGE]);
    let topology = topo(&[(0, "a", 0), (2, "c", 0), (HUGE, "h", 0)]);
    let seeded = root(CHUNK, vec![0, 1, 2], 1, MAX_VALUE_BYTES);
    seed(&meta, &fleet, INODE, &seeded, Some(1)).await;
    enqueue(&meta, CHUNK).await;

    let (outcome, exposed) = repair_pass(&meta, &fleet, &topology).await;

    // The binding assertion: an in-memory store has no ceiling of its own, so what proves the
    // refusal is the STORED LENGTH — bytes the tightest backend would have refused.
    let after = stored(&meta).await;
    let bytes = after.len();
    let untouched = metadata::encode(&seeded);
    assert!(
        bytes <= MAX_VALUE_BYTES,
        "{bytes} bytes stored, past the ceiling"
    );
    assert_eq!(after, untouched, "the record is not byte-identical");
    // Nothing at all was written: not the record, and not the rebuilt fragment — which GC
    // would otherwise have to hold forever with no grace evidence for it.
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(queued.contains(&CHUNK), "obligation dropped: {queued:?}");
    let stranded = holds(&fleet, HUGE, frag(CHUNK, 1)).await;
    assert!(!stranded, "a refused repair stranded a rebuilt fragment");
    // A pass that refused a repair has a hole in its redundancy picture, so it certifies
    // nothing — an operator reading `Satisfied` is told redundancy is restored.
    assert_eq!(outcome, Reconciled::Blocked, "the pass certified a refusal");
    assert!(
        exposed.contains("reconstruction_ceiling_refused"),
        "the refusal is not named on the durability audit seam:\n{exposed}"
    );
}

// ---- leg 2: an evacuation that did not persist does not certify the drain ----

/// One drain pass over a committed flat root whose fragment on the draining server can only
/// move to the [`HUGE`]-id server, seeded so the record the move WOULD commit encodes to
/// exactly `moved_len` bytes; `missing` withholds one fragment from its D server.
///
/// Returns what the pass answered, the record as stored afterwards, whether the target D
/// server holds the fragment, the drain's own status, and the durability surface.
async fn drain_pass(
    moved_len: usize,
    missing: Option<usize>,
) -> (Reconciled, Bytes, bool, ReconciliationStatus, String) {
    const CHUNK: ChunkId = 0x0000_BEEF;
    const DRAIN: DServerId = 1;
    let meta = MemMeta::default();
    let fleet = Fleet::new(&[0, DRAIN, 2, HUGE]);
    // The fragments that stay hold "a" and "c", and the draining server's own "b" is out of
    // the pool — so the one free distinct domain is "h", held by the HUGE id.
    let servers = [(0, "a", 0), (DRAIN, "b", 0), (2, "c", 0), (HUGE, "h", 0)];
    let topology = topo(&servers);

    // Pad the record the evacuation WOULD write (the fragment repointed onto the HUGE id,
    // version 2) to exactly `moved_len`, then seed that same record with the placement it has
    // BEFORE the move — so the loop's own re-encode lands exactly on `moved_len`, whatever the
    // id widths cost, rather than on a delta this fixture hard-codes.
    // Only `before`'s chunk_map is taken: the padding that matters is `moved`'s, and the
    // seeded record carries it, so the pass's own repoint re-encodes to exactly `moved_len`.
    let moved = root(CHUNK, vec![0, HUGE, 2], 2, moved_len);
    let before = root(CHUNK, vec![0, DRAIN, 2], 1, moved_len).chunk_map;
    let seeded = InodeRecord {
        chunk_map: before,
        version: 1,
        ..moved
    };
    seed(&meta, &fleet, INODE, &seeded, missing).await;
    set_lifecycle(&meta, DRAIN, DServerLifecycle::Draining)
        .await
        .unwrap();

    let view = fleet.view();
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &view,
        topology: &topology,
    };
    let (outcome, exposed) = pass(None, Some(&ctx)).await;
    let on_target = holds(&fleet, HUGE, frag(CHUNK, 1)).await;
    let status = reconciliation_status(&meta, DRAIN).await.unwrap();
    (outcome, stored(&meta).await, on_target, status, exposed)
}

/// RED on the base twice over: the oversized repoint COMMITS (so the stored record crosses the
/// ceiling and the fragment is copied to its new home), and even where a move does not persist
/// the pass answers `Satisfied` — certifying a drain that moved nothing.
#[tokio::test]
async fn an_evacuation_that_would_cross_the_value_ceiling_does_not_certify_the_drain() {
    let (outcome, after, on_target, status, exposed) = drain_pass(MAX_VALUE_BYTES + 1, None).await;
    let record: InodeRecord = metadata::decode(&after).unwrap();
    let bytes = after.len();

    // A drain that did not move a byte is not a converging drain: an operator reading
    // `Satisfied` here is being told the box is safe to pull.
    assert_eq!(outcome, Reconciled::Blocked, "a stalled drain certified");
    assert_eq!(status, ReconciliationStatus::Pending, "drain reported done");
    assert!(
        bytes <= MAX_VALUE_BYTES,
        "{bytes} bytes stored, past the ceiling"
    );
    assert_eq!(record.version, 1, "the refused move persisted something");
    assert_eq!(
        record.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "the record was repointed off the draining server anyway"
    );
    // Nothing at all was written — not even the fragment copy, which GC would otherwise have
    // to hold forever with no grace evidence for it.
    assert!(!on_target, "a refused move left a stranded fragment copy");
    assert!(
        exposed.contains("rebalance_ceiling_refused"),
        "the refusal is not named on the durability audit seam:\n{exposed}"
    );
}

/// **The ceiling's admissible side.** A repoint landing EXACTLY on [`MAX_VALUE_BYTES`] is a
/// value every backend stores, so it must still be written: the refusal is `>`, never `>=`.
/// The same fixture, one byte under — the move persists and the drain converges.
#[tokio::test]
async fn an_evacuation_landing_exactly_on_the_value_ceiling_still_commits() {
    let (outcome, after, on_target, status, _) = drain_pass(MAX_VALUE_BYTES, None).await;
    let record: InodeRecord = metadata::decode(&after).unwrap();

    assert_eq!(outcome, Reconciled::Changed, "a legal repoint was withheld");
    assert_eq!(after.len(), MAX_VALUE_BYTES, "not the record under test");
    assert_eq!(
        record.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, HUGE, 2],
        "the fragment was not repointed off the draining server"
    );
    assert!(on_target, "the evacuated fragment is not at its new home");
    assert_eq!(
        status,
        ReconciliationStatus::Satisfied,
        "drain not converged"
    );
}

/// **Precedence over a compound failure.** The same oversized move, but the fragment is no
/// longer on the draining server at all: the move could not have proceeded whatever the record
/// weighed. It must be named by that *recoverable* cause — an abort, re-assessed next pass —
/// and NOT by the permanent "this record must shrink" refusal, which pages a human to repair
/// an object whose real blocker is a lost fragment the reconstruction loop owns.
///
/// RED on the base: an abort is silent there, so the pass certifies the drain `Satisfied`
/// while the draining server is still referenced.
#[tokio::test]
async fn a_move_that_cannot_reach_its_fragment_is_aborted_not_refused() {
    let (outcome, after, on_target, status, exposed) =
        drain_pass(MAX_VALUE_BYTES + 1, Some(1)).await;
    let record: InodeRecord = metadata::decode(&after).unwrap();

    assert_ne!(outcome, Reconciled::Satisfied, "a stalled drain certified");
    assert_eq!(status, ReconciliationStatus::Pending, "drain reported done");
    assert_eq!(record.version, 1, "an aborted move persisted something");
    assert!(!on_target, "an aborted move left a stranded fragment copy");
    assert!(
        !exposed.contains("rebalance_ceiling_refused"),
        "a transient abort was reported as a permanent ceiling refusal:\n{exposed}"
    );
}

// ---- leg 3: a refused move is subtracted, never counted as a success ----

/// One pass, three chunks, three outcomes: one repaired, one refused for crossing the ceiling,
/// and one whose only free domain is a ghost the fleet does not hold — aborted, though ITS
/// record would have crossed the ceiling too (the same precedence rule as
/// [`a_move_that_cannot_reach_its_fragment_is_aborted_not_refused`], on the repair path).
///
/// The documented durability identity — `reconstruction_repaired − conflict − aborted −
/// ceiling_refused` — must equal the obligations the pass actually drained with a commit, so a
/// refused repair can never inflate the reported successes.
#[tokio::test]
async fn a_ceiling_refused_repair_is_subtracted_from_the_reported_successes() {
    const REPAIRED: ChunkId = 0xC0;
    const REFUSED: ChunkId = 0xEF;
    const ABORTED: ChunkId = 0xAB;

    let meta = MemMeta::default();
    let fleet = Fleet::new(&[0, 1, 2, HUGE]);
    // Three plans select three different targets out of ONE topology, ordered by utilization.
    // "g" is registered but its (also twenty-digit) server is NOT in the fleet view.
    let topology = topo(&[
        (0, "a", 3),
        (1, "b", 4),
        (2, "c", 0),
        (GHOST, "g", 2),
        (HUGE, "h", 1),
    ]);

    // Survivors in "a","b" → the cheapest free domain is "c" (server 2, in the fleet): commits.
    let repaired = root(REPAIRED, vec![0, 1, 2], 1, CLEAR_OF_CEILING);
    seed(&meta, &fleet, 1, &repaired, Some(2)).await;
    // Survivors in "a","c" → the cheapest free domain is "h" (the HUGE id), and this root is
    // already AT the ceiling, so that twenty-digit id is what crosses it: refused.
    let refused = root(REFUSED, vec![0, 1, 2], 1, MAX_VALUE_BYTES);
    seed(&meta, &fleet, 2, &refused, Some(1)).await;
    // Survivors in "c","h" → the cheapest free domain is "g", whose twenty-digit server the
    // fleet does not hold. Its root is AT the ceiling too, so the repoint would ALSO have
    // crossed: the transient cause must win.
    let aborted = root(ABORTED, vec![2, HUGE, 1], 1, MAX_VALUE_BYTES);
    seed(&meta, &fleet, 3, &aborted, Some(2)).await;
    for chunk in [REPAIRED, REFUSED, ABORTED] {
        enqueue(&meta, chunk).await;
    }

    let (outcome, exposed) = repair_pass(&meta, &fleet, &topology).await;
    assert_eq!(outcome, Reconciled::Blocked, "the pass certified a refusal");

    // The committed count, observed independently of the metric: an obligation leaves the
    // queue only in the same atomic batch that repointed its record.
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(
        queued.len() == 2 && queued.contains(&REFUSED) && queued.contains(&ABORTED),
        "exactly the refused and the aborted obligations stay queued: {queued:?}"
    );
    let committed = 3 - queued.len() as u64;
    // No fragment was displaced by any of the three: the one repair that landed rebuilt onto
    // the domain it had lost, and the other two wrote nothing. So an orphan grace record here
    // would be a GC delete queued against a fragment the committed record still references.
    let orphans = meta.scan(b"orphan:").await.unwrap();
    assert!(orphans.is_empty(), "a referenced fragment was orphaned");

    let refusals = counter(&exposed, "reconstruction_ceiling_refused");
    let aborts = counter(&exposed, "reconstruction_aborted");
    let successes = counter(&exposed, "reconstruction_repaired")
        .saturating_sub(counter(&exposed, "reconstruction_conflict"))
        .saturating_sub(aborts)
        .saturating_sub(refusals);
    assert_eq!(
        successes, committed,
        "reported successes (repaired − conflict − aborted − ceiling_refused) must equal the \
         commits\n{exposed}"
    );
    // Precedence, counted: exactly ONE chunk is the permanent record defect. The compound one
    // is the transient abort it also was.
    assert_eq!(
        (refusals, aborts),
        (1, 1),
        "an unplaceable rebuild counted as a permanent ceiling refusal\n{exposed}"
    );
}
