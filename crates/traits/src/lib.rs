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

use std::fmt;
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

/// A **stable D-server identifier** (proposal 0005, "The placement record"). A D
/// server is referenced by this stable id — assigned at registration and resolved
/// to a *current* endpoint by discovery — **not** by its endpoint URL, which
/// rebinds under restart/NAT and would rot a placement record keyed on it. The
/// committed chunk map records one `DServerId` per fragment index (the placement
/// vector), so a fragment that a custodian has *moved* is still found.
///
/// A `u64` is the encoding for M3.1 (the wire/registration source firms up with the
/// failure-domain selector, #141); it is deliberately opaque — consumers compare it,
/// they do not interpret its bits.
pub type DServerId = u64;

/// The boxed error type used across the trait surface at Milestone 0. Concrete
/// backends surface their own error detail through it; richer typed errors are
/// a later refinement once the failure modes are pinned by an implementation.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A convenience result alias for the trait surface.
pub type Result<T> = std::result::Result<T, BoxError>;

/// A fragment failed its **integrity** check: its self-describing checksum did not
/// verify, or its header named a different chunk/index than the [`FragmentId`] it is
/// filed under (bit rot / a tampered or misplaced fragment, `chunk-format` ADR-0019).
///
/// This is a **corruption** fault, categorically distinct from a **transient** fault
/// (unreachable / timed out / busy) AND from a **block-layer read fault**
/// ([`BlockReadFault`] — `EIO` / dead sector): the bytes are bad (checksum failed),
/// so *retrying the same fetch cannot help*. A consumer that walks fragments — the
/// custodian's scrub loop, the read path — must turn it into a **durable repair
/// obligation** (enqueue the chunk for reconstruction, emit a corruption finding) and
/// carry on past it, never retry it; the **three** fault categories are handled
/// differently (corruption-repair-and-continue, block-read-around-no-corruption-emit,
/// and transient-retry), so they must stay mutually distinguishable along the whole
/// path from the store to the consumer's decision point.
///
/// It lives in the seam crate so **every** backend produces the *same* type and
/// every consumer classifies it the *same* way ([`is_integrity_fault`]) without
/// depending on a concrete store (ADR-0010). A networked backend that surfaces the
/// fault over gRPC (a `DATA_LOSS` status, distinct from both `FAILED_PRECONDITION`
/// for block-read faults and the transient codes) reconstructs *this* type on the
/// client side, so the distinction survives the wire seam too.
#[derive(Debug)]
pub struct IntegrityFault {
    /// The fragment whose stored (or offered) bytes failed integrity.
    pub id: FragmentId,
    /// Backend detail for the durability audit trail — the concrete
    /// checksum/decode or id-mismatch reason.
    pub detail: String,
}

impl fmt::Display for IntegrityFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment integrity failure (chunk {:032x} index {}): {}",
            self.id.chunk, self.id.index, self.detail
        )
    }
}

impl std::error::Error for IntegrityFault {}

/// Whether `err` is an [`IntegrityFault`] (a corruption / integrity failure) anywhere
/// in its source chain — the seam-level classifier that lets a consumer branch
/// **repair-and-continue** (corruption) vs. **propagate/retry** (transient) without
/// knowing the backend's concrete error type. Walks [`source`](std::error::Error::source)
/// so a backend may wrap the fault in its own error and still be classified.
pub fn is_integrity_fault(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(err);
    while let Some(e) = next {
        if e.is::<IntegrityFault>() {
            return true;
        }
        next = e.source();
    }
    false
}

/// POSIX `EIO` (errno 5) — the OS errno a block-layer read fault raises (a dead
/// sector, a `dm-error` target). This is the **single** definition of the closure
/// "permanent block-layer fault" (errno-5 only; a wider class is deferred per
/// #251 §6 item 2) so every site — the gRPC server, the gRPC client, and
/// [`is_block_read_fault`] — agrees without re-deriving the predicate.
const BLOCK_READ_FAULT_ERRNO: i32 = 5;

/// A fragment could not be read because the **block device reported a read error**
/// (POSIX `EIO`, errno 5 — a dead sector, a `dm-error` target, or equivalent
/// block-layer I/O failure). This is a *permanent* durability fault — the device
/// physically cannot return the bytes — but is categorically **distinct** from
/// [`IntegrityFault`]:
///
/// * like [`IntegrityFault`], *retrying the same fetch cannot help* — read around
///   it and rebuild from the ≥k survivors;
/// * unlike [`IntegrityFault`], the stored content has **not** been shown to be
///   corrupt — the fault is at the block layer, not in the bytes. A consumer
///   **must not** record it as a corruption finding or schedule a checksum-repair.
///
/// It lives in the seam crate so a networked backend (the gRPC D server, which
/// maps it to `FAILED_PRECONDITION` rather than `DATA_LOSS`) can reconstruct *this*
/// type on the client side, preserving the block-read-fault ≠ corruption distinction
/// across the wire seam (ADR-0010).
///
/// Its [`source`](std::error::Error::source) exposes a synthetic `EIO`
/// [`std::io::Error`] so the source-chain walker `is_block_read_fault` in
/// `reconstruction.rs` classifies remote and local dead sectors identically without
/// a consumer-side code change — this type is transparent to the existing chain-
/// walking classifier.
#[derive(Debug)]
pub struct BlockReadFault {
    /// The fragment that could not be read.
    pub id: FragmentId,
    /// Backend detail for the durability audit trail.
    pub detail: String,
    // Synthetic EIO exposed via `source()` so the existing source-chain walker in
    // `reconstruction.rs` (`is_block_read_fault`) finds it — remote and local dead
    // sectors are classified identically without touching the consumer.
    io_source: std::io::Error,
}

impl BlockReadFault {
    /// Construct a block-read-fault for `id` with the given `detail` string.
    pub fn new(id: FragmentId, detail: impl Into<String>) -> Self {
        Self {
            id,
            detail: detail.into(),
            io_source: std::io::Error::from_raw_os_error(BLOCK_READ_FAULT_ERRNO),
        }
    }
}

impl fmt::Display for BlockReadFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "block-layer read fault (chunk {:032x} index {}): {}",
            self.id.chunk, self.id.index, self.detail
        )
    }
}

impl std::error::Error for BlockReadFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Expose the synthetic EIO so source-chain walkers (e.g. the private
        // `is_block_read_fault` in `reconstruction.rs`) classify this seam type
        // identically to a raw `io::Error(EIO)` raised by the fs backend.
        Some(&self.io_source)
    }
}

/// Whether `err` is a block-layer read fault anywhere in its source chain —
/// checks for [`BlockReadFault`] (the seam type a remote gRPC backend
/// reconstructs on the client) **or** a [`std::io::Error`] with
/// `raw_os_error() == Some(5)` (a local `EIO` / dead sector raised by the fs
/// backend directly).
///
/// This is the **single decision point** for the closure of "permanent block-layer
/// fault" (EIO / errno-5 only; the wider class is deferred per #251 §6 item 2) —
/// the gRPC server calls this to decide what to map to `FAILED_PRECONDITION`
/// rather than re-deriving the check inline.
///
/// Walks [`source`](std::error::Error::source) so a backend may wrap the fault
/// in its own type and still be classified.
pub fn is_block_read_fault(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(err);
    while let Some(e) = next {
        if e.is::<BlockReadFault>() {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.raw_os_error() == Some(BLOCK_READ_FAULT_ERRNO) {
                return true;
            }
        }
        next = e.source();
    }
    false
}

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

    /// Enumerate every fragment this store currently holds. Order is
    /// unspecified. The maintenance plane's **GC** loop (M3, proposal 0005) walks
    /// this to diff a D server's actual contents against the committed chunk map
    /// and reclaim orphans (`crates/custodian/src/gc.rs`). The **scrub** loop
    /// (M3, proposal 0005; missing-fragment detection issue #330) instead drives
    /// off the committed reference set directly, fetching each placed fragment by
    /// id via [`ChunkStore::get_fragment`] — a listing alone can only surface a
    /// fragment's presence, never prove a specific one is genuinely absent,
    /// since an absent fragment by definition never appears in it. Added
    /// additively for M3; it neither moves bytes nor interprets them beyond
    /// their addressing.
    async fn list_fragments(&self) -> Result<Vec<FragmentId>>;

    /// Remove the bytes stored for `id`. **Idempotent**: deleting a fragment the
    /// store does not hold succeeds with `Ok(())`, so a retried or duplicated GC
    /// reclaim is not an error. The maintenance plane's **GC** loop (M3, proposal
    /// 0005) reclaims orphaned bytes through this; the store stays deliberately
    /// dumb (building-block view, §8.5) — it removes the bytes it is told to,
    /// making no reference-safety judgement (that is the caller's invariant).
    async fn delete_fragment(&self, id: FragmentId) -> Result<()>;

    /// Report this store's current health.
    async fn health(&self) -> Result<Health>;
}

/// **Placement-aware** addressing over a fleet of D servers (proposal 0005, M3.1).
///
/// M2 routed a fragment **statelessly** — `index % n` — so the read found it only
/// because nothing had moved it. M3 records, per fragment index, the [`DServerId`]
/// holding that fragment (the chunk map's placement vector) and resolves the read
/// **from that record**, so a *moved* fragment is still found. This trait is the
/// seam the read/write path uses to address a specific D server by its stable id;
/// it is layered **beside** [`ChunkStore`] (its supertrait), which stays the dumb
/// fragment-bytes primitive — its only M3 growth is the bytes-level
/// enumerate/delete affordances ([`ChunkStore::list_fragments`] /
/// [`ChunkStore::delete_fragment`], a sibling slice), not any placement logic.
///
/// Every backing store provides the methods through their defaults: a bare
/// `ChunkStore` is a **single location authority** that already routes by
/// `FragmentId` (M0's one store, M2's `index % n` fan-out), so the recorded id is
/// advisory and the at-server calls delegate straight through — M0–M2 behaviour is
/// preserved exactly. A genuinely **relocatable** fleet (a custodian-aware store,
/// later M3 slices) overrides them to honour a moved id.
#[async_trait]
pub trait PlacementChunkStore: ChunkStore {
    /// The stable D-server ids a fresh chunk's `0..n` fragments are placed on, in
    /// fragment-index order — recorded into the chunk map at the write commit. The
    /// default is the identity placement (`index` → D-server `index`): a single
    /// store / `index % n` fan-out is its own location authority, so the record just
    /// mirrors the fragment order.
    fn placement(&self, n: u16) -> Vec<DServerId> {
        (0..u64::from(n)).collect()
    }

    /// Fetch fragment `id` from the D server `dserver` the placement record names.
    /// The default ignores `dserver` and delegates to
    /// [`ChunkStore::get_fragment`] — a single-authority store already routes by
    /// `FragmentId`.
    async fn get_fragment_at(&self, _dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        self.get_fragment(id).await
    }

    /// Place fragment `id` on the D server `dserver`. The default ignores `dserver`
    /// and delegates to [`ChunkStore::put_fragment`].
    async fn put_fragment_at(
        &self,
        _dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
    ) -> Result<()> {
        self.put_fragment(id, fragment).await
    }
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
