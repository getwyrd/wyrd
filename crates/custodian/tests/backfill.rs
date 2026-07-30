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

#![forbid(unsafe_code)]

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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not).
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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not).
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
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
        chunk_map: vec![].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
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
        before.chunk_map.as_flat().unwrap()[0].placement.is_empty(),
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
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "full-length identity placement: placement.len() == fragment_count() and \
         placement[i] == i for all i"
    );
    assert_eq!(
        after.size, before.size,
        "backfill materializes a placement; it never restates the object's extent — a \
         rewritten size would make every reader's framing disagree with its bytes"
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
        after_race.chunk_map.as_flat().unwrap()[0]
            .placement
            .is_empty(),
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
    assert_eq!(
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2]
    );
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
        after.chunk_map.as_flat().unwrap()[0].placement,
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
    assert_eq!(
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![5, 6, 7]
    );
}

// ---- (d) ADR-0047: a backfill PRESERVES the object metadata (repair, not republish) ----

/// A backfill identity-fill is placement maintenance on the SAME content, so it must
/// PRESERVE the object metadata (ADR-0047): the ETag / content-type / Last-Modified trio
/// survives the commit unchanged. This guards `backfill.rs`'s preservation `..record.clone()`
/// against silently regressing to `..Default::default()` (which still compiles but drops
/// the trio to `None`, so GET would serve `application/octet-stream` and no ETag for a
/// repaired object). Every OTHER backfill test seeds all-`None` metadata (`..Default::default()`),
/// so this invariant is otherwise VACUOUSLY true.
#[tokio::test]
async fn backfill_preserves_object_metadata_while_filling_placement() {
    let meta = MemMeta::default();
    let etag = "9a8b7c6d5e4f3021".to_string();
    let content_type = "application/json".to_string();
    let modified = 1_700_000_000_456_u64;
    // A published object whose one chunk carries the pre-M3 EMPTY placement (so backfill
    // fires) AND the full ADR-0047 metadata trio.
    let chunk = rs_chunk(0xD0, 2, 1, vec![]); // ReedSolomon{k:2,m:1} -> fragment_count() == 3
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![chunk].into(),
        state: InodeState::Committed,
        version: 2,
        etag: Some(etag.clone()),
        content_type: Some(content_type.clone()),
        modified: Some(modified),
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&record)))
        .await
        .unwrap();

    let ctx = BackfillContext { meta: &meta };
    assert_eq!(
        reconcile(&ctx).await.unwrap(),
        Reconciled::Changed,
        "the empty-placement chunk is backfilled (the commit under test fires)"
    );

    let after = read_inode(&meta, 1).await;
    assert_eq!(
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "the empty placement was filled to the full-length identity vector"
    );
    assert_eq!(
        after.etag,
        Some(etag),
        "backfill PRESERVES the ETag (ADR-0047): a repair does not republish the content"
    );
    assert_eq!(
        after.content_type,
        Some(content_type),
        "backfill PRESERVES the stored content type"
    );
    assert_eq!(
        after.modified,
        Some(modified),
        "backfill must NOT move Last-Modified"
    );
}

// ---- the resolve that RESTARTED: every decision follows the live generation ---------

/// A store whose `inode:` **scan** answers from an older cut than its `get`s do — the
/// interleaving where a publication lands between a maintenance pass's scan and its
/// resolve. `MetadataStore::scan` promises one consistent cut, not the latest one, so a
/// later `get` legitimately sees a newer record (`crates/traits/src/lib.rs:770-775`).
struct StaleScan<'a> {
    inner: &'a MemMeta,
    stale: Vec<(Vec<u8>, Bytes)>,
}

#[async_trait]
impl MetadataStore for StaleScan<'_> {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if prefix == b"inode:" {
            return Ok(self.stale.clone());
        }
        self.inner.scan(prefix).await
    }

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

/// **A pass decides on the generation it RESOLVED, never on the snapshot it scanned.**
///
/// The shared resolver is total against a generation retired mid-pass: it re-reads the
/// root and resolves the one that replaced it (proposal 0016 decision 7(h)). The
/// replacement can have a different **shape** — here a segmented generation is superseded
/// by a flat one — and a pass that then consulted its own stale snapshot would take the
/// segmented skip: it would report a perfectly fillable map as **unfillable**, failing the
/// whole pass, and the CAS it declined would have been against bytes no longer in the
/// store. The record that carries the chunks is the record that gets classified, skipped
/// and compare-and-swapped.
#[tokio::test]
async fn backfill_follows_the_live_generation_when_the_resolve_restarts() {
    let meta = MemMeta::default();
    // The LIVE generation: flat, committed, and carrying the empty placement this pass
    // exists to fill.
    let live = InodeRecord {
        size: 5,
        chunk_map: vec![rs_chunk(0xF0, 2, 1, vec![])].into(),
        state: InodeState::Committed,
        version: 3,
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&live)))
        .await
        .unwrap();

    // The stale cut the scan hands back: the SEGMENTED generation that was superseded,
    // whose `seg:` records the retirement has already drained.
    let group = metadata::SegmentGroup::new("0123456789abcdef0123456789abcdef", 7).unwrap();
    let stale = InodeRecord {
        size: 5,
        chunk_map: metadata::ChunkMap::Segmented(
            metadata::SegmentedMap::new(
                group,
                vec![metadata::SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: 5,
                }],
            )
            .unwrap(),
        ),
        state: InodeState::Committed,
        version: 2,
        ..Default::default()
    };
    let store = StaleScan {
        inner: &meta,
        stale: vec![(metadata::inode_key(1), metadata::encode(&stale))],
    };

    let ctx = BackfillContext { meta: &store };
    assert_eq!(
        reconcile(&ctx).await.unwrap(),
        Reconciled::Changed,
        "the live generation is flat and fillable — a pass that judged the stale \
         segmented snapshot would fail the whole pass as unfillable instead",
    );
    let after = read_inode(&meta, 1).await;
    assert_eq!(
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "…and the fill landed on the generation the resolve returned",
    );
    assert_eq!(
        after.version,
        live.version + 1,
        "one version-conditional commit, against the LIVE prior",
    );
}

// ---- containment: one unreadable record does not stop the drain ---------------------

/// **A record this pass cannot read is contained, attributed, and does not stop the
/// store's drain.**
///
/// `reconcile` walks every committed record. A segmented root whose generation is
/// incomplete cannot be resolved by anyone, and propagating that fault out of the walk
/// would end the pass at the damaged record: every healthy record after it stays
/// un-drained, and the population gauge an operator watches the drain by is never emitted.
/// So the fault is contained per object and raised once, after the sweep — the typed
/// assertion leg `segmented_map_consumers.rs` cannot make, since that file may not name
/// the types this slice adds.
#[tokio::test]
async fn an_unreadable_record_does_not_stop_the_drain_and_is_reported_afterwards() {
    let meta = MemMeta::default();
    // A committed segmented root whose one segment record was never written: readable as a
    // record, unresolvable as a map.
    let group = metadata::SegmentGroup::new("fedcba9876543210fedcba9876543210", 11).unwrap();
    let damaged = InodeRecord {
        size: 5,
        chunk_map: metadata::ChunkMap::Segmented(
            metadata::SegmentedMap::new(
                group,
                vec![metadata::SegmentRef {
                    index: 0,
                    byte_offset: 0,
                    byte_len: 5,
                }],
            )
            .unwrap(),
        ),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&damaged)))
        .await
        .unwrap();
    // …and a healthy pre-M3 record that this pass exists to fill, seeded AFTER it so a
    // walk that stopped at the damaged one would leave it empty.
    seed_committed(&meta, 2, vec![rs_chunk(0xC0, 2, 1, vec![])], 5).await;

    let ctx = BackfillContext { meta: &meta };
    let err = reconcile(&ctx)
        .await
        .expect_err("a record the pass could not read may not be reported as swept");

    // The healthy record was drained anyway — the containment's whole point.
    assert_eq!(
        read_inode(&meta, 2).await.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "one unreadable record stopped the drain of a healthy record in the same store",
    );
    // …and the damaged one was left exactly as it was: nothing is rewritten on the way
    // past it.
    assert_eq!(
        meta.get(&metadata::inode_key(1)).await.unwrap().unwrap(),
        metadata::encode(&damaged),
    );
    // The report is typed and attributes the object, so an operator has a record to
    // repair rather than a count to interpret.
    let reported = err
        .downcast_ref::<wyrd_custodian::backfill::UnresolvableChunkMaps>()
        .expect("the population is reported as its own type, not as a decode error");
    assert_eq!(reported.records, 1);
    assert_eq!(reported.first, "inode:1");
}

/// **An UNCOMMITTED record this pass could not read is not a member of its population,
/// and may not fail the pass.**
///
/// `reconcile` fills committed records; everything else is skipped by the `state` filter.
/// But a root whose bytes do not decode at all fails *before* that filter, and treating it
/// as a blocker there raises [`UnresolvableChunkMaps`] over an object with nothing to
/// fill — permanently, on every future pass over that store, so a healthy record
/// committed after it never reads as drained either. So the state is taken from the bytes
/// that are still readable (`metadata::inode_state_hint`, via the shared
/// `resolve::classify_root`) BEFORE the fault is recorded.
///
/// The control in the same test is the *committed* spelling of the same bytes, which MUST
/// still fail the pass: a fix that bought precision by dropping the containment would pass
/// this test's first half and fail here. Mirrors
/// `gc.rs:an_unreadable_uncommitted_record_does_not_freeze_the_fleets_reclamation`.
#[tokio::test]
async fn an_unreadable_uncommitted_record_does_not_fail_the_pass() {
    /// A segmented root whose table spans 16 bytes while `size` says 99: structurally
    /// invalid, so `metadata::decode` refuses it — the record class this pass meets before
    /// any `state` filter can run.
    fn damaged_root(state: &str) -> Bytes {
        Bytes::from(format!(
            r#"{{"size":99,"chunk_map":{{"group":{{"nonce":"0123456789abcdef0123456789abcdef","epoch":7}},"segment_count":1,"segments":[{{"index":0,"byte_offset":0,"byte_len":16}}]}},"state":"{state}","version":1}}"#
        ))
    }

    for (state, fails, why) in [
        (
            "Pending",
            false,
            "an uncommitted record has nothing to fill, so the pass over every other \
             record is complete and reports as such",
        ),
        (
            "Committed",
            true,
            "a COMMITTED record whose map cannot be read has an UNKNOWN empty-placement \
             count, so the pass may not report a swept store",
        ),
    ] {
        let meta = MemMeta::default();
        let key = metadata::inode_key(1);
        meta.commit(WriteBatch::new().put(key.clone(), damaged_root(state)))
            .await
            .unwrap();
        // The fixture carries the fault it is about: the record really is in the store and
        // really does not decode.
        assert!(
            metadata::decode::<InodeRecord>(&meta.get(&key).await.unwrap().unwrap()).is_err(),
            "fixture: the seeded {state} record must genuinely fail to decode",
        );
        // …and a healthy pre-M3 record that this pass exists to fill, seeded AFTER it.
        seed_committed(&meta, 2, vec![rs_chunk(0xC0, 2, 1, vec![])], 5).await;

        let ctx = BackfillContext { meta: &meta };
        let outcome = reconcile(&ctx).await;

        // The positive observable, either way: the healthy record is filled. Containment
        // is about what the pass REPORTS, never about what it stops doing.
        assert_eq!(
            read_inode(&meta, 2).await.chunk_map.as_flat().unwrap()[0].placement,
            vec![0, 1, 2],
            "{state}: the healthy record is drained whatever the damaged one's state",
        );
        if fails {
            let err = outcome.expect_err(why);
            let reported = err
                .downcast_ref::<wyrd_custodian::backfill::UnresolvableChunkMaps>()
                .unwrap_or_else(|| panic!("the population is typed and names the record: {err}"));
            assert_eq!((reported.records, reported.first.as_str()), (1, "inode:1"));
        } else {
            assert_eq!(
                outcome.unwrap_or_else(|e| panic!("{why}: {e}")),
                Reconciled::Changed,
                "{why}",
            );
        }
        // The damaged record itself is never rewritten on the way past it.
        assert_eq!(
            meta.get(&key).await.unwrap().unwrap(),
            damaged_root(state),
            "{state}: the record the pass could not read is left byte-identical",
        );
    }
}

// The drain-to-zero observability leg (BINDING (c)) lives in its own test binary,
// `backfill_telemetry.rs` — a `tracing` metric callsite caches interest in
// process-global state, so a no-op-subscriber sibling test in the same process can
// race and disable it (issue #214). Mirrors the `gc.rs` / `gc_telemetry.rs` split.
