//! Issue #350 leg (c) (ADR-0040 decision 6, steps 1–2; proposal 0005 durability
//! plane; ADR-0011/0012): **drain-to-zero observability** — the count of
//! empty-placement committed records remaining is emitted on the
//! [`DurabilityTelemetry`] seam every backfill pass and read back in-process via
//! `gather_prometheus`, reading ZERO once backfill has covered the store.
//!
//! This leg lives in its **own** test binary, deliberately apart from the other
//! backfill criteria in `backfill.rs` — mirroring the `gc.rs` / `gc_telemetry.rs`
//! split. The backfill metric is a `tracing::info!(gauge.backfill_placement_remaining
//! = …)` callsite (`backfill.rs` `reconcile`) bridged to OpenTelemetry, and `tracing`
//! caches per-callsite *interest* in **process-global** state. The other backfill
//! tests exercise that same callsite under a no-op subscriber (they install none);
//! run in the same process they race this test on callsite registration and can cache
//! the callsite as disabled, silently dropping the gauge here (issue #214). A separate
//! test binary is a separate process, so its callsite cache is its own.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_custodian::backfill::{reconcile, BackfillContext};
use wyrd_custodian::{
    DurabilityTelemetry, ExporterConfig, Reconciled, SegmentedPlacementUnfillable,
};
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, MetadataStore, Result, WriteBatch};

// ---- in-memory metadata store (backend-agnostic; the pass is proven over the seam) ----

/// A trivial in-memory metadata store (with version-conditional commit) — the same
/// minimal shape the sibling `backfill.rs` and the other custodian-loop suites use.
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

/// A store whose **`scan` is a snapshot** and whose `get`/`commit` are live — the shape
/// every maintenance pass actually runs against.
///
/// A pass scans `inode:` once and then works through the result; over a large store the
/// last record it reaches was read minutes ago, and anything published in between is
/// invisible to that snapshot. This double makes that window explicit and reproducible:
/// `scan` answers from the rows captured by [`StaleScan::freeze`], `get` answers from the
/// store as it is now.
struct StaleScan {
    inner: MemMeta,
    frozen: Mutex<HashMap<Vec<u8>, Bytes>>,
}

impl StaleScan {
    fn new(inner: MemMeta) -> Self {
        Self {
            inner,
            frozen: Mutex::new(HashMap::new()),
        }
    }

    /// Capture the store as the pass's scan will see it.
    fn freeze(&self) {
        *self.frozen.lock().unwrap() = self.inner.kv.lock().unwrap().clone();
    }
}

#[async_trait]
impl MetadataStore for StaleScan {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .frozen
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
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
        self.inner.commit(batch).await
    }
}

// ---- helpers ----

/// A ReedSolomon `{k, m}` chunk with the given (possibly empty) `placement`.
fn rs_chunk(id: ChunkId, k: u8, m: u8, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::ReedSolomon { k, m },
        len: 5,
        placement,
    }
}

/// An `EcScheme::None` (single-fragment) chunk with the given placement.
fn ec_none_chunk(id: ChunkId, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::None,
        len: 5,
        placement,
    }
}

/// Commit `chunk_map` onto a freshly-seeded inode `id` via the real four-phase-write
/// commit point — a committed record whose `ChunkRef` carries the given (possibly
/// empty) `placement`, simulating a pre-M3 record decoded through `#[serde(default)]`.
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

/// The value of a `gauge` metric read back off the Prometheus surface (the last
/// non-comment sample matching `name`, ignoring any label set).
fn gauge_value(exposed: &str, name: &str) -> Option<f64> {
    exposed
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let key = fields.next()?;
            let value = fields.next()?;
            let metric = key.split('{').next().unwrap_or(key);
            (metric == name)
                .then(|| value.parse::<f64>().ok())
                .flatten()
        })
        .next_back()
}

// ---- (c) drain-to-zero observability on the durability-plane seam -------------------

/// The gauge counts the population **backfill cannot fill** as well as the one it can:
/// a segmented map is resolved and counted, never silently excluded. Otherwise a class of
/// committed record with an unfillable empty placement would read as a drained store.
///
/// And the pass **fails** on it. The gauge alone is a count, and a count an operator has
/// to notice is exactly the "silent skip" the *Absent or unsupported entries* rule
/// forbids: this entry is one no pass in the system drains, so the pass says so out loud
/// rather than returning `Satisfied` over it. Both halves are asserted together, because
/// the failure must not cost the observability — the flat records are still filled and
/// the gauge is still emitted before it is raised.
#[tokio::test]
async fn the_remaining_gauge_counts_a_segmented_records_empty_placement_too() {
    let meta = MemMeta::default();
    // Two flat records the pass DOES fill…
    seed_committed(&meta, 1, vec![rs_chunk(0xE0, 2, 1, vec![])], 5).await;
    seed_committed(&meta, 2, vec![rs_chunk(0xE1, 2, 1, vec![])], 5).await;
    // …and one SEGMENTED record it declines to rewrite, carrying an empty placement.
    let group = metadata::SegmentGroup::new("0123456789abcdef0123456789abcdef", 7).unwrap();
    let segment = metadata::SegmentRecord::new(vec![rs_chunk(0xE2, 2, 1, vec![])], 0).unwrap();
    let table = metadata::SegmentedMap::new(
        group.clone(),
        vec![metadata::SegmentRef {
            index: 0,
            byte_offset: 0,
            byte_len: segment.byte_len(),
        }],
    )
    .unwrap();
    let root = InodeRecord {
        size: segment.byte_len(),
        chunk_map: metadata::ChunkMap::Segmented(table),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.commit(
        WriteBatch::new()
            .put(metadata::seg_key(&group, 0), metadata::encode(&segment))
            .put(metadata::inode_key(3), metadata::encode(&root)),
    )
    .await
    .unwrap();

    let ctx = BackfillContext { meta: &meta };
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    let err = reconcile(&ctx)
        .with_subscriber(subscriber)
        .await
        .expect_err("an entry no pass can drain fails the pass; it is never skipped");
    let unfillable = err
        .downcast_ref::<SegmentedPlacementUnfillable>()
        .unwrap_or_else(|| panic!("a typed, explicit failure naming the record: {err}"));
    assert_eq!(
        (unfillable.inode, unfillable.unfilled, unfillable.records),
        (3, 1, 1),
        "the failure names the record, its unfillable chunks, and how many such records",
    );

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert_eq!(
        gauge_value(&exposed, "backfill_placement_remaining"),
        Some(1.0),
        "the two flat chunks were filled; the segmented one remains and MUST still be \
         counted, or an unfillable population reads as a drained store; got:\n{exposed}"
    );
    // …the fillable records were still filled — the failure is raised at the END, so one
    // undrainable record does not stop the rest of the store from draining…
    for id in [1, 2] {
        assert!(
            !read_inode(&meta, id).await.chunk_map.as_flat().unwrap()[0]
                .placement
                .is_empty(),
            "inode {id} was still backfilled",
        );
    }
    // …and the segmented record itself is untouched by the pass.
    assert_eq!(
        meta.get(&metadata::inode_key(3)).await.unwrap().unwrap(),
        metadata::encode(&root),
    );
}

/// **BINDING (c):** the empty-placement-remaining population is emitted on the
/// durability-plane seam and reads ZERO once backfill has covered the store.
#[tokio::test]
async fn emitted_remaining_count_reaches_zero_once_backfill_covers_the_store() {
    let meta = MemMeta::default();
    // Three committed records, each with one empty-placement chunk — the pre-M3
    // population this pass must drain.
    seed_committed(&meta, 1, vec![rs_chunk(0xD0, 2, 1, vec![])], 5).await;
    seed_committed(&meta, 2, vec![rs_chunk(0xD1, 4, 2, vec![])], 5).await;
    seed_committed(&meta, 3, vec![ec_none_chunk(0xD2, vec![])], 5).await;

    // Baseline: the raw store really carries three empty-placement committed chunks
    // before any pass — the population the drain-to-zero signal must depart from.
    let mut remaining_before = 0usize;
    for id in 1..=3u64 {
        let record = read_inode(&meta, id).await;
        remaining_before += record
            .chunk_map
            .as_flat()
            .unwrap()
            .iter()
            .filter(|c| c.placement.is_empty())
            .count();
    }
    assert_eq!(
        remaining_before, 3,
        "baseline: three committed chunks still carry an empty placement pre-pass"
    );

    let ctx = BackfillContext { meta: &meta };
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    let outcome = reconcile(&ctx).with_subscriber(subscriber).await.unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "all three empty-placement chunks backfill uncontested in one pass"
    );

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert_eq!(
        gauge_value(&exposed, "backfill_placement_remaining"),
        Some(0.0),
        "the empty-placement population reads ZERO once backfill has covered the \
         store; got:\n{exposed}"
    );

    // And the store itself confirms it: every committed chunk now carries an
    // explicit full-length identity placement.
    for id in 1..=3u64 {
        let record = read_inode(&meta, id).await;
        assert!(
            record
                .chunk_map
                .as_flat()
                .unwrap()
                .iter()
                .all(|c| !c.placement.is_empty()),
            "inode {id}: every committed chunk carries an explicit placement post-pass"
        );
    }
}

/// The value of a `monotonic_counter` metric read back off the Prometheus surface (the
/// last non-comment sample matching `name`, ignoring any label set). Counters are exposed
/// with a `_total` suffix.
fn counter_value(exposed: &str, name: &str) -> Option<f64> {
    gauge_value(exposed, &format!("{name}_total"))
}

/// **The unreadable-records LEVEL qualifies the remaining population, so it counts only
/// records that are actually missing from it.**
///
/// `backfill_unreadable_records` exists to say "this much remains, over this store minus
/// these unreadable records". The remaining population is committed records only, so an
/// unreadable **uncommitted** record excludes nothing from it: counting one leaves a
/// permanently non-zero level telling an operator the drain figure is incomplete when it
/// is exact — and it can never return to zero, because the record it counts has nothing
/// to repair into the population.
///
/// The record is still **named** on the seam
/// (`custodian_unreadable_uncommitted_record`): not counted is not the same as not seen,
/// and a silent skip is the *Absent or unsupported entries* failure (`AGENTS.md:175-177`).
/// The committed spelling of the same bytes is the control — it must still raise the
/// level, or the qualifier means nothing.
#[tokio::test]
async fn the_unreadable_level_counts_committed_records_only() {
    /// A segmented root whose table spans 16 bytes while `size` says 99: structurally
    /// invalid, so `metadata::decode` refuses it before any `state` filter runs.
    fn damaged_root(state: &str) -> Bytes {
        Bytes::from(format!(
            r#"{{"size":99,"chunk_map":{{"group":{{"nonce":"0123456789abcdef0123456789abcdef","epoch":7}},"segment_count":1,"segments":[{{"index":0,"byte_offset":0,"byte_len":16}}]}},"state":"{state}","version":1}}"#
        ))
    }

    for (state, unreadable, why) in [
        (
            "Pending",
            0.0,
            "an uncommitted record is outside the remaining population either way, so it \
             qualifies nothing and the level stays at zero",
        ),
        (
            "Committed",
            1.0,
            "a COMMITTED record the pass could not read IS missing from the population, \
             so the level says the figure beside it is partial",
        ),
    ] {
        let meta = MemMeta::default();
        let key = metadata::inode_key(1);
        meta.commit(WriteBatch::new().put(key.clone(), damaged_root(state)))
            .await
            .unwrap();
        assert!(
            metadata::decode::<InodeRecord>(&meta.get(&key).await.unwrap().unwrap()).is_err(),
            "fixture: the seeded {state} record must genuinely fail to decode",
        );
        // One healthy record with an empty placement, so the population figure the level
        // qualifies is a real one that drains to zero in this same pass.
        seed_committed(&meta, 2, vec![rs_chunk(0xB0, 2, 1, vec![])], 5).await;

        let ctx = BackfillContext { meta: &meta };
        let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
        let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
        // The pass's own outcome is asserted in `backfill.rs`; here it is only driven so
        // the gauges are emitted — a `?`-propagated error would skip them entirely.
        let _ = reconcile(&ctx).with_subscriber(subscriber).await;

        telemetry.flush().unwrap();
        let exposed = telemetry
            .gather_prometheus()
            .expect("Prometheus surface configured");
        assert_eq!(
            gauge_value(&exposed, "backfill_unreadable_records"),
            Some(unreadable),
            "{state}: {why}; got:\n{exposed}",
        );
        assert_eq!(
            gauge_value(&exposed, "backfill_placement_remaining"),
            Some(0.0),
            "{state}: the healthy record drained, so the population it qualifies is zero; \
             got:\n{exposed}",
        );
        // Seen either way: the two levels differ in what they COUNT, never in whether the
        // operator learns the record is damaged.
        assert_eq!(
            counter_value(
                &exposed,
                if state == "Committed" {
                    "custodian_unresolvable_chunk_map"
                } else {
                    "custodian_unreadable_uncommitted_record"
                }
            ),
            Some(1.0),
            "{state}: the damaged record is named on the seam whatever its state; \
             got:\n{exposed}",
        );
    }
}

/// **A pass acts on the LIVE root, not on the generation its own scan decoded — and says
/// so on the seam when the two differ.**
///
/// The shared maintenance resolver reads the root for exactly this case, and the case that
/// needs it is the **flat** one: a segmented snapshot is caught anyway (resolving it
/// re-reads the root to settle decision 7(h)), while a flat snapshot *is* the map, so a
/// superseded object's old chunk list comes back looking perfectly current. Here that means
/// a pass classifying, skipping and compare-and-swapping against a generation the object no
/// longer has: it sees the snapshot's fully-placed chunk, reports the store drained, and
/// leaves the live generation's empty placement unfilled — and, in GC's and restore's
/// spelling of the same walk, protects the retired generation's fragments while the live
/// one's are in nobody's reference set.
///
/// The emission is asserted with it: an operator reading a report about a superseded
/// generation has to be able to see that the pass re-derived. It is emitted **only** when
/// the snapshot really was stale, so the second half of the fixture is a pass whose
/// snapshot is current and which therefore emits nothing.
#[tokio::test]
async fn a_pass_resolves_the_live_root_and_reports_that_its_snapshot_was_stale() {
    let meta = StaleScan::new(MemMeta::default());
    // What the pass will scan: a flat record with nothing to fill.
    let stale_root = InodeRecord {
        size: 5,
        chunk_map: vec![rs_chunk(0xF0, 2, 1, vec![0, 1, 2])].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&stale_root)))
        .await
        .unwrap();
    meta.freeze();

    // …and what the store actually holds by the time the pass resolves it: another flat
    // generation, this one with an empty placement waiting to be filled. Flat to flat, so
    // nothing but the root re-read can tell the difference.
    let live_root = InodeRecord {
        size: 5,
        chunk_map: vec![rs_chunk(0xF1, 2, 1, vec![])].into(),
        state: InodeState::Committed,
        version: 2,
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&live_root)))
        .await
        .unwrap();

    let ctx = BackfillContext { meta: &meta };
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    assert_eq!(
        reconcile(&ctx).with_subscriber(subscriber).await.unwrap(),
        Reconciled::Changed,
        "the live generation has a placement to fill; a pass that answered from its own \
         snapshot would report the store drained over it",
    );
    let filled = read_inode(&meta, 1).await;
    assert_eq!(
        filled.chunk_map.as_flat().unwrap()[0].id,
        0xF1,
        "the pass acted on the LIVE generation, not on the one it scanned",
    );
    assert!(
        !filled.chunk_map.as_flat().unwrap()[0].placement.is_empty(),
        "…and filled it",
    );

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        counter_value(&exposed, "custodian_retired_generation_restarted")
            .is_some_and(|restarts| restarts >= 1.0),
        "a pass that re-derived against a root its snapshot did not have MUST say so on \
         the seam; got:\n{exposed}"
    );

    // A pass whose snapshot IS current says nothing: the counter is the exception's
    // signal, not a per-object one.
    meta.freeze();
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    assert_eq!(
        reconcile(&ctx).with_subscriber(subscriber).await.unwrap(),
        Reconciled::Satisfied,
    );
    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert_eq!(
        counter_value(&exposed, "custodian_retired_generation_restarted"),
        None,
        "a current snapshot is not a restart; got:\n{exposed}"
    );
}

/// **The unreadable-records level counts the OTHER spelling of unreadable too: a record
/// whose root decodes and whose MAP cannot be resolved.**
///
/// The qualifier's meaning is "this much remains, over this store minus these records", so
/// what it must count is records missing from the population — and a committed segmented root
/// whose `seg:` records are gone is missing from it just as surely as one whose bytes do not
/// decode. It reaches the walk through a different arm (the decode succeeded; the *resolve*
/// failed), so a level wired to the decode arm alone reads zero over a store whose drain
/// figure is incomplete, which is the count-based reassurance the rubric's *Absent or
/// unsupported entries* rule forbids (`AGENTS.md:175-177`).
///
/// The healthy record beside it is what makes the figure the level qualifies a real one: it
/// drains to zero in this same pass.
#[tokio::test]
async fn the_unreadable_level_counts_a_record_whose_map_cannot_be_resolved() {
    /// A **valid** segmented root — it decodes, so every `state` filter admits it — whose
    /// `seg:` records were never written, so only resolving its map fails.
    const UNRESOLVABLE_ROOT: &str = r#"{"size":16,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":7},"segment_count":1,"segments":[{"index":0,"byte_offset":0,"byte_len":16}]},"state":"Committed","version":1}"#;

    let meta = MemMeta::default();
    let key = metadata::inode_key(1);
    meta.commit(WriteBatch::new().put(
        key.clone(),
        Bytes::from_static(UNRESOLVABLE_ROOT.as_bytes()),
    ))
    .await
    .unwrap();
    let stored = meta.get(&key).await.unwrap().unwrap();
    let record: InodeRecord = metadata::decode(&stored).expect("fixture: the root must DECODE");
    assert_eq!(record.state, InodeState::Committed);
    assert!(
        metadata::resolve_chunk_map(&meta, &key, &record)
            .await
            .is_err(),
        "fixture: only the RESOLVE of this root's map may fail",
    );
    // One healthy record with an empty placement, so the population figure the level
    // qualifies is a real one that drains to zero in this same pass.
    seed_committed(&meta, 2, vec![rs_chunk(0xB0, 2, 1, vec![])], 5).await;

    let ctx = BackfillContext { meta: &meta };
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    // The pass's own outcome is asserted in `backfill.rs`; here it is only driven so the
    // gauges are emitted.
    let _ = reconcile(&ctx).with_subscriber(subscriber).await;

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert_eq!(
        gauge_value(&exposed, "backfill_unreadable_records"),
        Some(1.0),
        "a committed record whose MAP could not be resolved is missing from the remaining \
         population, so the level beside that population must say so; got:\n{exposed}",
    );
    assert_eq!(
        gauge_value(&exposed, "backfill_placement_remaining"),
        Some(0.0),
        "…and the healthy record still drained, so the figure the level qualifies is zero; \
         got:\n{exposed}",
    );
}
