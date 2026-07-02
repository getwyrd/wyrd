//! Issue #350 (ADR-0040 decision 6, steps 1–2): the **backfill custodian pass**
//! drains the pre-M3 / mixed-era population of committed chunk maps whose
//! `placement` vector is empty, plus the drain-to-zero observability signal on the
//! durability-plane seam.
//!
//! **Repro / RED baseline (feature-absence):** on `origin/main` (+ the #348 fold) no
//! `wyrd_custodian::backfill` module exists at all — this test file fails to
//! **compile** pre-patch, which is the demonstrable red the brief's verification
//! posture calls for (a born-at-tier NET-NEW test, no prior failing assertion to
//! flip). Post-patch every assertion below is green.
//!
//! The BINDING legs of the issue #350 success criterion, proven in-process over the
//! `MetadataStore` seam alone (this pass touches no D-server fleet, ADR-0010):
//!
//! (a) **Identity backfill, version-conditional**: a committed chunk with an EMPTY
//!     `placement` is rewritten to the explicit full-length identity vector
//!     (`placement.len() == fragment_count()`, `placement[i] == i`) via the SAME
//!     prior-record CAS the custodians already use — so a racing writer/custodian
//!     wins the CAS and the fill is retried on a later pass rather than clobbering.
//! (b) **Malformed vectors are never rewritten** (ADR-0040 decision 3, #348): a
//!     non-empty, wrong-length `placement` is left EXACTLY as committed.
//! (c) **Drain-to-zero observability**: the count of empty-placement committed
//!     records remaining is emitted on the durability-plane seam every pass and
//!     reads ZERO once backfill has covered the store.
//!
//! Idempotence (an already-explicit full-length vector is left untouched) is also
//! covered — it is the third leg of ADR-0040 decision 4's classification alongside
//! (a)/(b).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_custodian::backfill::{reconcile, BackfillContext};
use wyrd_custodian::Reconciled;
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, MetadataStore, Result, WriteBatch};

// ---- in-memory metadata store (backend-agnostic; the pass is proven over the seam) ----

/// A trivial in-memory metadata store (with version-conditional commit) — the same
/// minimal shape every other custodian-loop test suite uses (`rebalance.rs`,
/// `gc.rs`, `gc_telemetry.rs`).
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
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// A [`MetadataStore`] that injects a **single** concurrent inode mutation the first
/// time an inode-conditional commit is attempted after [`RacingMeta::arm`] — modelling
/// a writer/custodian that supersedes the record between backfill's read (the `scan`
/// in [`reconcile`]) and its commit. The injected write bumps the inode version
/// (placement left UNCHANGED) so backfill's `require(prior)` precondition fails: it
/// loses the CAS rather than clobbering the racing write. Mirrors
/// `crates/custodian/tests/rebalance.rs`'s `RacingMeta`.
struct RacingMeta {
    inner: MemMeta,
    armed: Mutex<bool>,
    raced: Mutex<bool>,
}

impl RacingMeta {
    fn new() -> Self {
        Self {
            inner: MemMeta::default(),
            armed: Mutex::new(false),
            raced: Mutex::new(false),
        }
    }

    fn arm(&self) {
        *self.armed.lock().unwrap() = true;
    }
}

#[async_trait]
impl MetadataStore for RacingMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let inject = {
            let armed = *self.armed.lock().unwrap();
            let mut raced = self.raced.lock().unwrap();
            let targets_inode = batch
                .preconditions
                .iter()
                .any(|p| p.key.starts_with(b"inode:"));
            if armed && !*raced && targets_inode {
                *raced = true;
                true
            } else {
                false
            }
        };
        if inject {
            let key = batch
                .preconditions
                .iter()
                .find(|p| p.key.starts_with(b"inode:"))
                .unwrap()
                .key
                .clone();
            if let Some(bytes) = self.inner.get(&key).await? {
                let mut record: InodeRecord = metadata::decode(&bytes).unwrap();
                record.version += 1; // racing writer bumps version, placement UNCHANGED
                let outcome = self
                    .inner
                    .commit(WriteBatch::new().put(key, metadata::encode(&record)))
                    .await?;
                assert_eq!(outcome, CommitOutcome::Committed, "racing writer commits");
            }
        }
        self.inner.commit(batch).await
    }
}

// ---- helpers ----

/// A ReedSolomon `{k, m}` chunk with the given (possibly empty / malformed / full)
/// `placement`.
fn rs_chunk(id: ChunkId, k: u8, m: u8, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::ReedSolomon { k, m },
        len: 5,
        placement,
    }
}

/// Commit `chunk_map` onto a freshly-seeded inode `id` via the real four-phase-write
/// commit point (`metadata::commit_chunk_map`, `core/src/metadata.rs:299-317`) — the
/// brief's repro instruction: an inode whose `ChunkRef` carries the given (possibly
/// empty) `placement`, simulating a pre-M3 record decoded through `#[serde(default)]`.
/// Returns the freshly-committed [`InodeRecord`] (state `Committed`, version 2).
async fn seed_committed(
    meta: &impl MetadataStore,
    id: InodeId,
    chunk_map: Vec<ChunkRef>,
    size: u64,
) -> InodeRecord {
    let prior = InodeRecord {
        size: 0,
        chunk_map: vec![],
        state: InodeState::Committed,
        version: 1,
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), metadata::encode(&prior)))
        .await
        .unwrap();
    let outcome = metadata::commit_chunk_map(meta, id, &prior, chunk_map, size)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    read_inode(meta, id).await
}

async fn read_inode(meta: &impl MetadataStore, id: InodeId) -> InodeRecord {
    let bytes = meta
        .get(&metadata::inode_key(id))
        .await
        .unwrap()
        .expect("inode present");
    metadata::decode(&bytes).unwrap()
}

// ---- (a) identity backfill, version-conditional -------------------------------------

/// **BINDING (a):** an empty-placement committed chunk is backfilled to the explicit
/// full-length identity vector, committed under the same prior-record CAS the
/// custodians use. Pre-patch this doesn't compile (no `backfill` module exists) —
/// the demonstrable red for this NET-NEW suite.
#[tokio::test]
async fn backfills_identity_placement_for_an_empty_placement_committed_chunk() {
    let meta = MemMeta::default();
    // ReedSolomon{k:2,m:1} -> fragment_count() == 3.
    let chunk = rs_chunk(0xC0, 2, 1, vec![]);
    let before = seed_committed(&meta, 1, vec![chunk], 5).await;
    assert!(
        before.chunk_map[0].placement.is_empty(),
        "pre-M3 shape: the committed record carries an EMPTY placement"
    );
    assert_eq!(before.version, 2);

    let ctx = BackfillContext { meta: &meta };
    let outcome = reconcile(&ctx).await.unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "BINDING (#350a): the empty-placement committed chunk IS backfilled"
    );

    let after = read_inode(&meta, 1).await;
    assert_eq!(
        after.version, 3,
        "exactly one version-conditional commit bumped the version"
    );
    assert_eq!(
        after.chunk_map[0].placement,
        vec![0, 1, 2],
        "full-length identity placement: placement.len() == fragment_count() and \
         placement[i] == i for all i"
    );
}

// ---- (a) CAS-conflict handling: a racing writer wins, backfill retries later --------

/// **BINDING (a), CAS-conflict leg:** a record mutated between backfill's read and its
/// commit is NOT clobbered — the racing writer wins the CAS, backfill's fill is
/// retried on a later pass, and only THEN converges.
#[tokio::test]
async fn a_racing_writer_wins_the_cas_and_backfill_retries_on_a_later_pass() {
    let racing = RacingMeta::new();
    let chunk = rs_chunk(0xC1, 2, 1, vec![]);
    let before = seed_committed(&racing, 1, vec![chunk], 5).await;
    assert_eq!(before.version, 2);

    // Arm the race: the next inode-conditional commit (backfill's identity-fill
    // repoint) will find the inode mutated underneath it.
    racing.arm();

    let ctx = BackfillContext { meta: &racing };
    let outcome = reconcile(&ctx).await.unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the only candidate backfill lost its CAS race — nothing converged this pass"
    );

    // SAFETY: the record reflects the RACING WRITER (version bumped, placement still
    // EMPTY), never a clobber by backfill's identity fill.
    let after_race = read_inode(&racing, 1).await;
    assert_eq!(
        after_race.version, 3,
        "the racing writer's commit landed (version 2 -> 3)"
    );
    assert!(
        after_race.chunk_map[0].placement.is_empty(),
        "placement is still EMPTY — the lost CAS prevented the clobber, backfill did \
         not (and could not) write over the racing writer's record"
    );

    // Retried on a later pass: no more race armed, backfill now converges uncontested.
    let outcome2 = reconcile(&ctx).await.unwrap();
    assert_eq!(
        outcome2,
        Reconciled::Changed,
        "retried on a later pass: the record backfills once uncontested"
    );
    let after = read_inode(&racing, 1).await;
    assert_eq!(after.version, 4);
    assert_eq!(after.chunk_map[0].placement, vec![0, 1, 2]);
}

// ---- (b) malformed placement is never rewritten --------------------------------------

/// **BINDING (b) / ADR-0040 decision 3, #348's posture:** a malformed (non-empty,
/// wrong-length) committed placement is left EXACTLY as committed — never rewritten.
#[tokio::test]
async fn malformed_placement_is_never_rewritten() {
    let meta = MemMeta::default();
    // fragment_count() == 3 but a length-1 vector: malformed (truncation/corruption).
    let chunk = rs_chunk(0xC2, 2, 1, vec![7]);
    let before = seed_committed(&meta, 1, vec![chunk], 5).await;

    let ctx = BackfillContext { meta: &meta };
    let outcome = reconcile(&ctx).await.unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "a malformed vector is never backfilled"
    );

    let after = read_inode(&meta, 1).await;
    assert_eq!(
        after.version, before.version,
        "no version-conditional commit landed for the malformed chunk"
    );
    assert_eq!(
        after.chunk_map[0].placement,
        vec![7],
        "malformed placement left EXACTLY as committed — never rewritten (#348 posture)"
    );
}

// ---- idempotence: an already-explicit full-length vector is left untouched ----------

/// The third leg of ADR-0040 decision 4's classification alongside (a)/(b): a
/// committed chunk whose placement is ALREADY explicit and full-length is idempotent
/// — backfill leaves it untouched (no spurious commit / version bump).
#[tokio::test]
async fn already_explicit_full_length_placement_is_left_untouched() {
    let meta = MemMeta::default();
    let chunk = rs_chunk(0xC3, 2, 1, vec![5, 6, 7]);
    let before = seed_committed(&meta, 1, vec![chunk], 5).await;

    let ctx = BackfillContext { meta: &meta };
    let outcome = reconcile(&ctx).await.unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "an already-explicit full-length vector is idempotent: nothing to backfill"
    );

    let after = read_inode(&meta, 1).await;
    assert_eq!(
        after.version, before.version,
        "no spurious commit / version bump"
    );
    assert_eq!(after.chunk_map[0].placement, vec![5, 6, 7]);
}

// The drain-to-zero observability leg (BINDING (c)) lives in its own test binary,
// `backfill_telemetry.rs` — a `tracing` metric callsite caches interest in
// process-global state, so a no-op-subscriber sibling test in the same process can
// race and disable it (issue #214). Mirrors the `gc.rs` / `gc_telemetry.rs` split.
