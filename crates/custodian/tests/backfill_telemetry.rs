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

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_custodian::backfill::{reconcile, BackfillContext};
use wyrd_custodian::{DurabilityTelemetry, ExporterConfig, Reconciled};
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
        chunk_map: vec![],
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
            record.chunk_map.iter().all(|c| !c.placement.is_empty()),
            "inode {id}: every committed chunk carries an explicit placement post-pass"
        );
    }
}
