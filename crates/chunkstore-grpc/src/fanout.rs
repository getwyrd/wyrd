//! [`FanoutChunkStore`]: a [`ChunkStore`] that spreads a chunk's fragments across
//! several backing stores (Milestone 2, proposal 0004).
//!
//! This is the **minimal placement primitive** of M2: a fragment at index `i`
//! lands on store `i % n`, so a chunk's `n` fragments prefer `n` distinct D
//! servers and a single D-server loss costs at most one fragment. It is
//! best-effort, **not** an enforced placement guarantee — with fewer stores than
//! fragments some share a store, and failure-domain-aware placement is L2 /
//! custodian work (M3+). Placement is **stateless** and deterministic: the same
//! index always routes to the same store.
//!
//! As of M3.1 (proposal 0005, "The placement record") the **chunk map** records, per
//! fragment, the stable D server holding it, and the read path resolves each fragment
//! from that record ([`wyrd_traits::PlacementChunkStore`]) — so the recorded-placement
//! question M2 deferred is now settled in the affirmative. The fan-out is no longer the
//! location *authority*: it stays a single-D-server-per-store [`PlacementChunkStore`]
//! whose `index % n` is the identity placement the write records, so an **un-moved**
//! fragment routes exactly as before. Honouring a *moved* id (a relocatable, custodian-
//! aware fan-out) is a later M3 slice; this slice records and resolves placement.
//!
//! Generic over `C: ChunkStore`, so it composes whatever the binary injects —
//! `GrpcChunkStore` clients to networked D servers in production, in-process
//! stores under test — without depending on a concrete (ADR-0010). The parallel
//! fan-out itself lives in `core::write_fragments`, which fires the per-fragment
//! puts concurrently; this store only routes each to its placed backend.

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{ChunkStore, FragmentId, Health, PlacementChunkStore, Result};

/// A [`ChunkStore`] over an ordered set of backing stores, routing each fragment
/// by `index % n`.
pub struct FanoutChunkStore<C> {
    stores: Vec<C>,
}

impl<C: ChunkStore> FanoutChunkStore<C> {
    /// Compose a fan-out over `stores`, in placement order (store `i` holds the
    /// fragments at indices `i`, `i + n`, …).
    ///
    /// # Panics
    /// Panics if `stores` is empty — a fan-out with no backend could not place any
    /// fragment, which is a wiring bug, not a runtime condition.
    pub fn new(stores: Vec<C>) -> Self {
        assert!(
            !stores.is_empty(),
            "FanoutChunkStore needs at least one backing store"
        );
        Self { stores }
    }

    /// The number of backing stores fragments are spread across.
    pub fn width(&self) -> usize {
        self.stores.len()
    }

    /// The backing store a fragment at `index` is placed on.
    fn route(&self, index: u16) -> &C {
        &self.stores[index as usize % self.stores.len()]
    }
}

#[async_trait]
impl<C: ChunkStore> ChunkStore for FanoutChunkStore<C> {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.route(id.index).put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        self.route(id.index).get_fragment(id).await
    }

    /// The union of every backing store's fragments — the fan-out holds what its
    /// backends hold. Order is unspecified (the trait makes no promise), and the
    /// per-store sets are disjoint by construction (`route` places each index on
    /// exactly one backend), so no de-duplication is needed.
    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        let mut ids = Vec::new();
        for store in &self.stores {
            ids.extend(store.list_fragments().await?);
        }
        Ok(ids)
    }

    /// Delete from the one backend the fragment is placed on, matching how `put`
    /// and `get` route by `index % n`.
    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.route(id.index).delete_fragment(id).await
    }

    /// Aggregate liveness: `Healthy` only if every backing store is, `Unhealthy`
    /// only if all are unreachable or unhealthy, else `Degraded` — a single dead D
    /// server degrades the fan-out without failing it (a read can still reconstruct
    /// from the survivors).
    async fn health(&self) -> Result<Health> {
        let mut healthy = 0usize;
        let mut unhealthy = 0usize;
        for store in &self.stores {
            match store.health().await {
                Ok(Health::Healthy) => healthy += 1,
                Ok(Health::Degraded) => {}
                Ok(Health::Unhealthy) | Err(_) => unhealthy += 1,
            }
        }
        Ok(if healthy == self.stores.len() {
            Health::Healthy
        } else if unhealthy == self.stores.len() {
            Health::Unhealthy
        } else {
            Health::Degraded
        })
    }
}

/// The fan-out is a single-D-server-per-store [`PlacementChunkStore`]: its `index % n`
/// routing **is** the identity placement the write commit records, so an un-moved
/// fragment resolves through [`ChunkStore::get_fragment`] exactly as in M2 (the trait's
/// defaults). The chunk map — not the fan-out — is now the location authority; a
/// custodian-aware fan-out that honours a *moved* id is a later M3 slice (0005, M3.1).
impl<C: ChunkStore> PlacementChunkStore for FanoutChunkStore<C> {}
