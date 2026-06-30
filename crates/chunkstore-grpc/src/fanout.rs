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
//! location *authority*, but it **is relocatable** (issue #356): `get_fragment_at` /
//! `put_fragment_at` route by the resolved `dserver` rather than by `id.index % n`, so
//! an **un-moved** fragment (`dserver == index`, the identity placement the write
//! records) routes exactly as before, and a fragment a custodian has **moved**
//! resolves to the store its placement names instead of its stale `index % n` home.
//!
//! Generic over `C: ChunkStore`, so it composes whatever the binary injects —
//! `GrpcChunkStore` clients to networked D servers in production, in-process
//! stores under test — without depending on a concrete (ADR-0010). The parallel
//! fan-out itself lives in `core::write_fragments`, which fires the per-fragment
//! puts concurrently; this store only routes each to its placed backend.

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{ChunkStore, DServerId, FragmentId, Health, PlacementChunkStore, Result};

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

    /// The backing store the D server `dserver` lives on — same `% n` mapping as
    /// [`Self::route`], but keyed by the **placement-resolved** D-server id a
    /// `PlacementChunkStore` caller already resolved, not by the fragment's own
    /// index. The default identity placement (`DServerId == index`, set at a fresh
    /// commit by [`wyrd_traits::PlacementChunkStore::placement`]) makes this compute
    /// exactly what `route` would, so an **un-moved** fragment is unaffected; once a
    /// custodian repoints `placement[i]` to a *different* `dserver`, this is what
    /// lets `get_fragment_at` / `put_fragment_at` follow the move instead of
    /// re-deriving (and getting wrong) the stale `index % n` home.
    fn route_dserver(&self, dserver: DServerId) -> &C {
        &self.stores[dserver as usize % self.stores.len()]
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

/// The fan-out is a **relocatable** [`PlacementChunkStore`] (issue #356): rather than
/// take the trait's defaults — which ignore `dserver` and delegate to
/// [`ChunkStore::get_fragment`] / [`ChunkStore::put_fragment`], i.e. `index % n`
/// (`wyrd_traits::PlacementChunkStore` defaults) — it routes by the **resolved**
/// `dserver` ([`FanoutChunkStore::route_dserver`]). An un-moved fragment's identity
/// placement (`dserver == index`) makes that compute exactly the `index % n` the
/// defaults would have, so M0–M2 behaviour for an un-moved fragment is unchanged; a
/// fragment a custodian has **moved** (`dserver != index`) now resolves to the store
/// the placement record names, instead of being silently dropped back to its stale
/// `index % n` home at this trait boundary (the gap `:15-19`'s old note flagged and
/// `crates/traits/src/lib.rs:289-290` names as the later "genuinely relocatable
/// fleet" override).
#[async_trait]
impl<C: ChunkStore> PlacementChunkStore for FanoutChunkStore<C> {
    async fn get_fragment_at(&self, dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        self.route_dserver(dserver).get_fragment(id).await
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
    ) -> Result<()> {
        self.route_dserver(dserver).put_fragment(id, fragment).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// An in-memory `ChunkStore` with a fixed reported health, cheap to clone so a
    /// test can keep a handle after the store is moved into the fan-out and inspect
    /// which backend a put landed on.
    #[derive(Clone)]
    struct MemStore {
        frags: Arc<Mutex<HashMap<FragmentId, Bytes>>>,
        health: Health,
    }

    impl MemStore {
        fn with_health(health: Health) -> Self {
            Self {
                frags: Arc::new(Mutex::new(HashMap::new())),
                health,
            }
        }
        fn healthy() -> Self {
            Self::with_health(Health::Healthy)
        }
        fn has(&self, id: FragmentId) -> bool {
            self.frags.lock().unwrap().contains_key(&id)
        }
    }

    #[async_trait]
    impl ChunkStore for MemStore {
        async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
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
            Ok(self.health)
        }
    }

    fn fid(index: u16) -> FragmentId {
        FragmentId {
            chunk: 0xC0FFEE,
            index,
        }
    }

    /// `:54` `width -> 0` / `1` — the reported width is the backing-store count.
    #[tokio::test]
    async fn width_reports_the_backing_store_count() {
        let fanout = FanoutChunkStore::new(vec![
            MemStore::healthy(),
            MemStore::healthy(),
            MemStore::healthy(),
        ]);
        assert_eq!(fanout.width(), 3, "width is the number of backing stores");
    }

    /// `:59` `% -> +` / `% -> /` — routing places index `i` on store `i % n`.
    /// `+` overruns the slice (panic); `/` collapses low indices onto store 0. Pin
    /// that index 1 lands on store[1] (not store[0]) and index 2 wraps to store[0].
    #[tokio::test]
    async fn route_places_each_index_on_index_mod_n() {
        let a = MemStore::healthy();
        let b = MemStore::healthy();
        let fanout = FanoutChunkStore::new(vec![a.clone(), b.clone()]);

        fanout
            .put_fragment(fid(1), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(b.has(fid(1)), "index 1 routes to store[1] (1 % 2)");
        assert!(!a.has(fid(1)), "index 1 is not on store[0]");

        fanout
            .put_fragment(fid(2), Bytes::from_static(b"y"))
            .await
            .unwrap();
        assert!(a.has(fid(2)), "index 2 wraps to store[0] (2 % 2)");
    }

    /// `:78` `list_fragments -> Ok(vec![])` — the listing is the union of the
    /// backends' fragments, not empty.
    #[tokio::test]
    async fn list_fragments_unions_every_backend() {
        let fanout = FanoutChunkStore::new(vec![MemStore::healthy(), MemStore::healthy()]);
        fanout
            .put_fragment(fid(0), Bytes::from_static(b"x"))
            .await
            .unwrap();
        fanout
            .put_fragment(fid(1), Bytes::from_static(b"y"))
            .await
            .unwrap();

        let mut listed = fanout.list_fragments().await.unwrap();
        listed.sort_by_key(|f| f.index);
        assert_eq!(
            listed,
            vec![fid(0), fid(1)],
            "the fan-out lists what its backends hold"
        );
    }

    /// `:105` `== -> !=` and `:100` `+= -> -=` — `Healthy` requires every backend
    /// healthy (`healthy == n`). `-=` underflows the `usize` counter (panic); `!=`
    /// flips an all-healthy fan-out to `Degraded`.
    #[tokio::test]
    async fn health_is_healthy_only_when_all_backends_are() {
        let fanout = FanoutChunkStore::new(vec![MemStore::healthy(), MemStore::healthy()]);
        assert_eq!(fanout.health().await.unwrap(), Health::Healthy);
    }

    /// `:107` `== -> !=` and `:102` `+= -> -=` — `Unhealthy` requires every backend
    /// unhealthy (`unhealthy == n`).
    #[tokio::test]
    async fn health_is_unhealthy_only_when_all_backends_are() {
        let fanout = FanoutChunkStore::new(vec![
            MemStore::with_health(Health::Unhealthy),
            MemStore::with_health(Health::Unhealthy),
        ]);
        assert_eq!(fanout.health().await.unwrap(), Health::Unhealthy);
    }

    /// A single dead backend degrades the fan-out without failing it.
    #[tokio::test]
    async fn health_degrades_on_a_single_dead_backend() {
        let fanout = FanoutChunkStore::new(vec![
            MemStore::healthy(),
            MemStore::with_health(Health::Unhealthy),
        ]);
        assert_eq!(fanout.health().await.unwrap(), Health::Degraded);
    }

    // ---- issue #356: a moved placement is honoured at the `_at` trait boundary ----
    //
    // Mirrors `crates/core/tests/placement_record.rs::moved_fragment_resolved_from_record_
    // after_reopen`, but exercises `FanoutChunkStore` directly (no metadata store / reopen)
    // since the defect is at the `PlacementChunkStore` trait boundary the fan-out
    // implements, not in the placement record itself.
    //
    // RED pre-fix: the empty `impl PlacementChunkStore for FanoutChunkStore<C> {}` (the
    // default `get_fragment_at` / `put_fragment_at`, `crates/traits/src/lib.rs:306-319`)
    // ignores `dserver` and routes by `id.index % n`, so each assertion below sees the
    // OLD `index % n` store instead of the one the (genuinely different) `dserver`
    // names. GREEN post-fix: `get_fragment_at` / `put_fragment_at` route by
    // `route_dserver(dserver)`.

    /// `EcScheme::None` shape: a single fragment at index 0. Its identity home is
    /// `stores[0 % n]`; a custodian move repoints it to a *different* D server without
    /// touching its index. Pre-fix this is a **total miss** (`Ok(None)`) — there is no
    /// redundancy to read around for a single-fragment chunk, exactly the unreadable-
    /// object failure mode the brief names.
    #[tokio::test]
    async fn get_fragment_at_resolves_an_ec_none_fragment_moved_off_its_index_home() {
        let a = MemStore::healthy();
        let b = MemStore::healthy();
        let fanout = FanoutChunkStore::new(vec![a.clone(), b.clone()]);
        let id = fid(0); // identity home: stores[0 % 2] == a
        let moved_dserver: DServerId = 1; // != 0 % 2 -> a genuine move, to b

        // The fragment already lives where a custodian's evacuation placed it: on the
        // backend the moved placement names, written directly so this test isolates
        // the READ side (`get_fragment_at`) from any write-side routing.
        b.put_fragment(id, Bytes::from_static(b"moved-fragment"))
            .await
            .unwrap();

        let got = fanout.get_fragment_at(moved_dserver, id).await.unwrap();
        assert_eq!(
            got,
            Some(Bytes::from_static(b"moved-fragment")),
            "get_fragment_at(1, ..) must return store[1]'s content (the D server the \
             placement names), not stores[index % n] (store[0], a miss)"
        );
    }

    /// Reed-Solomon shape: a multi-fragment chunk (mirrors `rs(4,2)`'s 6 fragments)
    /// rotated by one store so **every** fragment's placed `dserver` differs from
    /// `index % n` (the same all-moved property
    /// `placement_record.rs::moved_fragment_resolved_from_record_after_reopen` pins).
    /// Pre-fix, a read "around" the wrong-located fragment via `index % n` would mask
    /// the gap chunk-by-chunk rather than failing outright — this pins that every
    /// single moved fragment is resolved from its named `dserver`, not silently
    /// substituted from the stale `index % n` slot.
    #[tokio::test]
    async fn get_fragment_at_resolves_every_fragment_in_a_rotated_reed_solomon_placement() {
        const WIDTH: usize = 3; // stores
        const N: u16 = 6; // rs(4,2): k + m fragments
        const SHIFT: u16 = 1; // rotate by one store so every fragment moves

        let stores: Vec<MemStore> = (0..WIDTH).map(|_| MemStore::healthy()).collect();
        let fanout = FanoutChunkStore::new(stores.clone());

        for index in 0..N {
            let id = fid(index);
            let dserver = u64::from((index + SHIFT) % WIDTH as u16);
            assert_ne!(
                dserver,
                u64::from(index % WIDTH as u16),
                "fragment {index} must be off its index % n home for this to be a genuine move"
            );
            // Placed directly on the named backend, as a custodian relocation would
            // have left it — independent of `put_fragment_at`, so this isolates the
            // read side.
            stores[dserver as usize]
                .put_fragment(id, Bytes::from(format!("frag-{index}")))
                .await
                .unwrap();
        }

        for index in 0..N {
            let id = fid(index);
            let dserver = u64::from((index + SHIFT) % WIDTH as u16);
            let got = fanout.get_fragment_at(dserver, id).await.unwrap();
            assert_eq!(
                got,
                Some(Bytes::from(format!("frag-{index}"))),
                "fragment {index} must resolve from the placed server ({dserver}), not \
                 from index % n ({})",
                index % WIDTH as u16
            );
        }
    }

    /// The write side of the same invariant: `put_fragment_at` must place on the named
    /// `dserver`, not fall back to `stores[index % n]`, so a custodian-directed write
    /// (e.g. backfilling a moved fragment) lands where it is told, not where the
    /// fragment's own index would have put it.
    #[tokio::test]
    async fn put_fragment_at_places_on_the_named_dserver_not_index_mod_n() {
        let a = MemStore::healthy();
        let b = MemStore::healthy();
        let fanout = FanoutChunkStore::new(vec![a.clone(), b.clone()]);
        let id = fid(0); // index % 2 == 0 -> identity home would be store[0]
        let moved_dserver: DServerId = 1;

        fanout
            .put_fragment_at(moved_dserver, id, Bytes::from_static(b"x"))
            .await
            .unwrap();

        assert!(
            b.has(id),
            "put_fragment_at(1, ..) must place on store[1], the named dserver"
        );
        assert!(
            !a.has(id),
            "must NOT fall back to stores[index % n] (store[0])"
        );
    }
}
