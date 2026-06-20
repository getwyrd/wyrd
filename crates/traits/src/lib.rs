//! Pluggability-seam trait definitions for Wyrd.
//!
//! These traits are the keystone of the architecture's dependency rule
//! (ADR-0010): implementations and consumers depend on this crate, never on
//! each other's concretes, and only the `server` binary wires concretes
//! together. That is what makes "swap redb for TiKV" or "in-memory for etcd" a
//! composition change rather than a refactor.
//!
//! This crate contains **definitions only — no implementations**. The
//! signatures are intentionally coarse at Milestone 0 and will firm up as the
//! commit protocol and the deterministic-simulation harness (ADR-0009) pin the
//! semantics. Every trait is `async` and object-safe (via [`async_trait`]) so a
//! single deterministic simulator can drive real and faked backends through the
//! same surface.

#![forbid(unsafe_code)]

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// A 128-bit chunk identifier (ADR-0019). Wide enough to be minted without
/// central coordination, which suits the direct-write data path.
pub type ChunkId = u128;

/// Addresses one fragment of a chunk: the chunk id plus the fragment's
/// `ec_fragment_index` (ADR-0019). A chunk under `replication(1)`/`none` has a
/// single fragment at index 0; an erasure-coded chunk has `k + m` fragments at
/// indices `0..k+m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FragmentId {
    /// The chunk this fragment belongs to.
    pub chunk: ChunkId,
    /// The fragment's 0-based index within the chunk's stripe.
    pub index: u16,
}

/// A monotonic fencing token handed out with a lock or leadership grant, so a
/// stale holder's writes can be rejected after it has lost the lock.
pub type FencingToken = u64;

/// The boxed error type used across the trait surface at Milestone 0. Concrete
/// backends surface their own error detail through it; richer typed errors are
/// a later refinement once the failure modes are pinned by an implementation.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A convenience result alias for the trait surface.
pub type Result<T> = std::result::Result<T, BoxError>;

/// A coarse health signal a backend reports about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Serving normally.
    Healthy,
    /// Reachable but degraded (e.g. a disk nearing capacity).
    Degraded,
    /// Not serving.
    Unhealthy,
}

/// Stores and retrieves erasure-coded chunk fragments, addressed by
/// [`FragmentId`] — chunk id plus fragment index.
///
/// Deliberately dumb (building-block view, L4): no placement logic and no
/// metadata. A fragment is the on-disk bytes specified by `chunk-format`
/// (ADR-0019); this trait moves those bytes and verifies their integrity, but
/// does not interpret them beyond the format's own checksums. Fragment-addressed
/// from M1 so erasure-coded chunks (many fragments per chunk) and M0's
/// `replication(1)` (a single fragment at index 0) share one contract — the
/// addressing M2's networked D servers inherit.
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Persist a fragment's bytes under `id`. Implementations verify the
    /// fragment's self-describing checksums before acknowledging.
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()>;

    /// Fetch a fragment's bytes, or `Ok(None)` if this store holds no fragment
    /// for `id`. Implementations verify integrity before returning bytes.
    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>>;

    /// Report this store's current health.
    async fn health(&self) -> Result<Health>;
}

/// The authoritative metadata store: inodes, dirents, chunk maps, the
/// pending-chunk GC ledger, and version counters.
///
/// Deliberately a **narrow key/value primitive** (ADR-0008): get, prefix scan,
/// and a single atomic [`commit`](MetadataStore::commit) of a [`WriteBatch`]
/// guarded by multi-key preconditions. Filesystem semantics — inode/dirent
/// records, version compare-and-set, the pending-chunk ledger — are expressed
/// *through* this primitive by the metadata model in `core`, never baked into
/// the trait, which keeps the layer honest about the KV features it depends on
/// and makes a backend swap (redb → TiKV) a composition change (ADR-0010).
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Read the raw value stored under `key`, if any.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Return every `(key, value)` whose key begins with `prefix`, e.g. every
    /// dirent under a parent. Order is unspecified.
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>>;

    /// Apply `batch` as a single atomic mutation — the commit point. Either
    /// every precondition holds and every put/delete lands, or nothing changes.
    /// Returns [`CommitOutcome::Conflict`] (not `Err`) when a precondition fails,
    /// so a stale writer is rejected distinguishably from a backend fault.
    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome>;
}

/// The result of a [`commit`](MetadataStore::commit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// All preconditions held; the batch was applied.
    Committed,
    /// A precondition did not hold; nothing was written (e.g. a stale-version
    /// writer, or a name that already exists).
    Conflict,
}

/// A precondition the store checks atomically before applying a [`WriteBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Precondition {
    /// The key whose current value is constrained.
    pub key: Vec<u8>,
    /// The required current value: `Some(bytes)` to require an exact match,
    /// `None` to require the key be absent.
    pub expected: Option<Bytes>,
}

/// A set of preconditions plus puts and deletes, applied atomically by
/// [`commit`](MetadataStore::commit). Build it with the helpers below.
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    /// Conditions that must all hold for the batch to apply.
    pub preconditions: Vec<Precondition>,
    /// Keys to set to the given values.
    pub puts: Vec<(Vec<u8>, Bytes)>,
    /// Keys to remove.
    pub deletes: Vec<Vec<u8>>,
}

impl WriteBatch {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Require `key` to currently equal `value`.
    pub fn require(mut self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) -> Self {
        self.preconditions.push(Precondition {
            key: key.into(),
            expected: Some(value.into()),
        });
        self
    }

    /// Require `key` to currently be absent.
    pub fn require_absent(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.preconditions.push(Precondition {
            key: key.into(),
            expected: None,
        });
        self
    }

    /// Set `key` to `value`.
    pub fn put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) -> Self {
        self.puts.push((key.into(), value.into()));
        self
    }

    /// Remove `key`.
    pub fn delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.deletes.push(key.into());
        self
    }
}

/// Bootstrap and coordination (L5): service discovery, leader election, locks
/// with fencing tokens, and zone-wide config.
///
/// Losing coordination loses no data (established connections keep working from
/// cached state); what is lost is the ability to *react* until it returns.
///
/// Some semantics are provisional until a second backend (etcd, ADR-0006) pins
/// them against a networked implementation: **blocking** lock acquisition (this
/// surface offers non-blocking try-acquire) and a **push** config watch (this
/// surface offers a pollable [`config_revision`](Coordination::config_revision))
/// are later refinements.
#[async_trait]
pub trait Coordination: Send + Sync {
    /// Register this member under `key` with a lease that expires after `ttl`
    /// unless [`renew`](Coordination::renew)ed, so a crashed member's
    /// registration lapses (leased service discovery).
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease>;

    /// Extend `lease` by its original `ttl` from now. Errors if the lease is
    /// unknown or already expired.
    async fn renew(&self, lease: Lease) -> Result<()>;

    /// Withdraw the registration backing `lease` immediately.
    async fn revoke(&self, lease: Lease) -> Result<()>;

    /// Discover the current (unexpired) members registered under `key`.
    async fn discover(&self, key: &str) -> Result<Vec<Bytes>>;

    /// Campaign to become the single active leader for `key`. Resolves when
    /// leadership is granted, carrying a fencing token for the term.
    async fn elect_leader(&self, key: &str) -> Result<Leadership>;

    /// Try to acquire the distributed lock on `key`. Returns `Some` with a fenced
    /// grant if the lock was free, or `None` if it is already held — genuine
    /// mutual exclusion without blocking. (A blocking acquire is a later
    /// refinement; see the trait note.)
    async fn lock(&self, key: &str) -> Result<Option<LockGuard>>;

    /// Release a lock previously acquired via [`lock`](Coordination::lock).
    /// Releasing goes through the trait (not `Drop`) because a real backend's
    /// release is an async operation. Idempotent.
    async fn unlock(&self, guard: LockGuard) -> Result<()>;

    /// Set the zone-wide config value for `key`, bumping
    /// [`config_revision`](Coordination::config_revision).
    async fn set_config(&self, key: &str, value: Bytes) -> Result<()>;

    /// Read the current zone-wide config value for `key`.
    async fn get_config(&self, key: &str) -> Result<Option<Bytes>>;

    /// The monotonic config revision, bumped on every [`set_config`]. A watcher
    /// polls it to detect changes and re-reads the keys it cares about — the
    /// dep-free stand-in for a push watch until etcd backs a real stream.
    ///
    /// [`set_config`]: Coordination::set_config
    async fn config_revision(&self) -> Result<u64>;
}

/// A renewable lease backing a registration; letting it expire (or
/// [`revoke`](Coordination::revoke)ing it) withdraws the registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// Opaque lease identifier assigned by the coordination backend.
    pub id: u64,
}

/// A granted leadership term, fenced by a monotonic token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leadership {
    /// The fencing token for this term; rises on every new leadership grant.
    pub token: FencingToken,
}

/// A held distributed lock, fenced by a monotonic token so a stale holder's
/// writes can be rejected after it has lost the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockGuard {
    /// The fencing token for this lock acquisition.
    pub token: FencingToken,
}
