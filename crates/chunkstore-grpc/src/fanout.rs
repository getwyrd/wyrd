//! [`FanoutChunkStore`]: a [`ChunkStore`] that spreads a chunk's fragments across
//! several backing stores (Milestone 2, proposal 0004).
//!
//! This is the **minimal placement primitive** of M2: a fragment at index `i`
//! lands on store `i % n`, so a chunk's `n` fragments prefer `n` distinct D
//! servers and a single D-server loss costs at most one fragment. It is
//! best-effort, **not** an enforced placement guarantee — with fewer stores than
//! fragments some share a store, and failure-domain-aware placement is L2 /
//! custodian work (M3+). Placement is **stateless** and deterministic: the same
//! index always routes to the same store, so the read resolves a fragment back to
//! where the write put it without a placement record (the recorded-placement
//! question is settled at M3).
//!
//! Generic over `C: ChunkStore`, so it composes whatever the binary injects —
//! `GrpcChunkStore` clients to networked D servers in production, in-process
//! stores under test — without depending on a concrete (ADR-0010). The parallel
//! fan-out itself lives in `core::write_fragments`, which fires the per-fragment
//! puts concurrently; this store only routes each to its placed backend.

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{ChunkStore, FragmentId, Health, Result};

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
