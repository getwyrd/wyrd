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

/// The **canonical textual rendering of a [`ChunkId`]** — zero-padded lowercase hex.
///
/// This is not cosmetic. `{:032x}` is already the form the on-disk fragment directory is
/// named after (`chunkstore-fs`), the form [`IntegrityFault`] and [`BlockReadFault`] print,
/// and the form the read path's error messages carry. A log line that renders the same id
/// as decimal is a **broken join key**: the operator holding `…c0ffee` from an error, or
/// from an `ls` of the data directory, cannot grep for it. One definition here so every
/// emitter agrees (#527).
pub fn chunk_hex(id: ChunkId) -> String {
    format!("{id:032x}")
}

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

/// A [`MetadataStore::commit`] whose outcome the backend **could not determine**:
/// the batch may or may not have been applied.
///
/// The contract's hardest error class (see [`MetadataStore`], "Errors and the
/// caller's obligations"). It is `Err`, never [`CommitOutcome::Conflict`] —
/// `Conflict` asserts *nothing was written*, which is exactly what is not known —
/// and a backend must **never silently retry** it, because a [`WriteBatch`] is not
/// guaranteed idempotent. The caller's only remedy is to **re-read** and establish
/// what happened.
///
/// It lives in the seam crate, like [`IntegrityFault`] and [`ScanCapExceeded`], so
/// one `downcast_ref::<CommitUnknownResult>()` classifies the class on **any**
/// backend. It was previously an FDB-only type (`metadata-fdb`'s
/// `classify::CommitUnknownResult`), with the DST harness carrying a third
/// hand-rolled copy — so a consumer could only recognise an undetermined commit if
/// it happened to know it was talking to FoundationDB (#515).
///
/// Every distributed backend has the class; only its spelling differs.
/// FoundationDB reports it natively (`1021 commit_unknown_result`, `1031
/// transaction_timed_out`). TiKV does **not**: `tikv_client::Error::Undetermined`
/// exists but is set only when the client cannot *connect* to the primary — which
/// is a definite non-commit — and is **not** set when the commit RPC times out,
/// which is the case that genuinely is undetermined (Percolator commits once the
/// primary key's commit record lands, whether or not the client learns it). So the
/// TiKV driver derives the class itself, conservatively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitUnknownResult {
    /// The backend that could not determine the outcome (`"foundationdb"`, `"tikv"`).
    pub backend: &'static str,
    /// The backend-native error code, where it has one — FoundationDB's `1021` /
    /// `1031`. `None` for a backend (TiKV) whose client reports no code for the
    /// class.
    pub code: Option<i32>,
    /// Backend detail for the audit trail.
    pub detail: String,
    /// Whether the batch may still be applied **after** this error was returned.
    ///
    /// `false` means the transaction is already out of flight, so a single re-read
    /// establishes the outcome once and for all (FoundationDB's 1021). `true` means
    /// a re-read that observes nothing does **not** prove nothing will land — the
    /// commit may still be in flight (FoundationDB's 1031; every TiKV case, since
    /// the client may have given up on a commit RPC that TiKV goes on to apply).
    pub may_still_commit: bool,
}

impl fmt::Display for CommitUnknownResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadata commit returned an unknown result ({} — {}): the batch may or may \
             not have been applied. It is not retried — a WriteBatch is not guaranteed \
             idempotent — and it is not a Conflict; the caller must re-read to establish \
             what happened.",
            self.backend, self.detail,
        )?;
        if self.may_still_commit {
            write!(
                f,
                " The batch may still be applied AFTER this error, so a re-read that \
                 observes nothing does not prove it will never land.",
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for CommitUnknownResult {}

/// Interim ceiling on the **total** materialized results of a single
/// [`MetadataStore::scan`]. On breach a backend fails loud (`Err`, via
/// [`ScanCapExceeded`]) and returns **no** partial `Vec` — the
/// completeness-or-fail-loud clause of the store contract (#262, ADR-0011): a
/// silently truncated `inode:` scan shrinks GC's never-reclaim safety set, which
/// is data loss, so this is a **correctness constraint, not a tuning knob**.
///
/// 2^20 dirents is far past any legitimate single directory yet bounds a
/// gateway's heap against a pathological prefix. It lives here, in the seam
/// crate, because **backends of the same trait must not disagree about how large
/// a listing may be** — it was previously a per-crate constant duplicated
/// verbatim in `metadata-tikv` and `metadata-fdb`, each asserting in a comment
/// that the other's value had to match (#516).
pub const SCAN_CAP: usize = 1 << 20;

/// A [`MetadataStore::scan`] exceeded [`SCAN_CAP`] (or a store's lower configured
/// cap): the call fails loud instead of truncating (#262, ADR-0011), and returns
/// **no** partial result set.
///
/// Like [`IntegrityFault`] and [`BlockReadFault`], this lives in the seam crate so
/// **every** backend raises the *same* type and every consumer classifies it the
/// *same* way — `err.downcast_ref::<ScanCapExceeded>()` distinguishes "too big,
/// failed loud" from a genuine backend fault without the caller knowing which
/// store it holds. It was previously defined *separately* in `metadata-tikv` and
/// `metadata-fdb` with identical fields and `Display`, so the same downcast
/// silently depended on which backend was wired in (#516).
///
/// The operator-visible ADR-0011 audit signal is surfaced by the caller
/// (GC/custodian), which already owns the telemetry path; a descriptive typed
/// error keeps that signal caller-side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanCapExceeded {
    /// The cap that was breached.
    pub cap: usize,
    /// The logical prefix whose scan overflowed (lossy-rendered for operators).
    pub prefix: Vec<u8>,
}

impl fmt::Display for ScanCapExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadata scan exceeded the interim per-listing cap of {} keys for \
             prefix {:?}: failing loud rather than returning a truncated result set \
             (a silently truncated scan is data loss — #262, ADR-0011)",
            self.cap,
            String::from_utf8_lossy(&self.prefix),
        )
    }
}

impl std::error::Error for ScanCapExceeded {}

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
/// and makes a backend swap (redb → TiKV → FoundationDB) a composition change
/// (ADR-0010).
///
/// # The contract
///
/// Written down **after** the FoundationDB port (#438) from what it taught, per
/// ADR-0002's implementation-first posture for component interfaces (#437); the
/// clauses of the shared `wyrd-metadata-conformance` suite (`run_all`) are the
/// *executable* record, and this prose says what they mean. Stated
/// backend-neutrally: the three shipped backends reach these guarantees by three
/// different mechanisms — redb serializes write transactions, TiKV takes
/// pessimistic locking reads, FoundationDB uses an optimistic read-conflict set —
/// and a fourth backend may use a fourth, but it must land here.
///
/// **1. Keys and values are opaque bytes.** A backend stores them
/// byte-identically and never interprets them, so a full-value
/// [`Precondition`] is an exact compare-and-set.
///
/// **2. `commit` is the only mutation point, and it is atomic.** Every
/// precondition is evaluated against *committed* state, atomically with the
/// batch's own writes — not against a snapshot read taken earlier. So a caller
/// may safely `get` a key, decide, and then guard its batch with a
/// [`require`](WriteBatch::require) on what it read: correctness rests on the
/// **in-commit re-check**, never on the freshness of that earlier read. This is
/// what makes the read-then-commit `rename` in `core::metadata` safe, and it is
/// pinned by `contract_rename_race_yields_conflict`.
///
/// **3. `Conflict` means a precondition lost — and only a *conditional* batch
/// can conflict.** See [`CommitOutcome`], whose docs carry the full partition;
/// the blind-batch half is pinned by `contract_blind_batch_is_never_conflict`.
///
/// **4. Reads observe the most recent committed state, and a `scan` is one
/// consistent cut.** No stale, cached, bounded-staleness or follower reads
/// (ADR-0015 clause 3, decided in #261): a `get` never serves a value older than
/// the latest committed one for that key (`contract_read_after_commit`), and a
/// single `scan` observes one instant — a concurrent rename under the scanned
/// prefix appears at exactly one of its two positions, never both and never
/// neither, however many pages the backend internally reads
/// (`contract_scan_is_consistent_cut`). A backend that pages a `scan` therefore
/// holds ONE read version across all of its pages; it may not stitch pages from
/// different versions, which would tear the cut.
///
/// **5. A `scan` is complete or it fails loudly.** It returns the whole matching
/// set at that one version, or `Err` — never a silently truncated `Vec` (#262,
/// ADR-0011). Silent truncation is a data-loss bug, not a performance
/// characteristic: a short `inode:` listing would shrink GC's never-reclaim
/// safety set. The distributed backends enforce this with a shared result cap
/// (`SCAN_CAP`, deliberately identical across them) above which they `Err`
/// rather than truncate.
///
/// # Errors and the caller's obligations
///
/// The error channel is [`BoxError`], so backends distinguish failures by the
/// **concrete type** the caller downcasts to, not by a trait-level enum.
///
/// **An `Err` from `commit` does not mean "nothing was written."** For a
/// distributed backend some commit failures are *unknown-result*: the transaction
/// may or may not have been applied (FoundationDB's `commit_unknown_result`
/// (1021) and `transaction_timed_out` (1031) are the concrete instances; any
/// networked backend has the class). Two rules follow, and they bind every
/// backend:
///
/// - **An unknown-result commit is never reported as [`CommitOutcome::Conflict`]**
///   — `Conflict` asserts nothing was written, which is exactly what is not known.
///   It surfaces as `Err`, distinguishable by **one** downcast on every backend:
///   [`CommitUnknownResult`], whose `may_still_commit` says whether a re-read can
///   settle the outcome at all.
/// - **A backend never silently retries an unknown-result commit**, because a
///   [`WriteBatch`] is **not guaranteed idempotent** (see its docs) — a blind
///   re-apply could double-apply it. A backend may retry only errors its
///   substrate reports as *definitively not committed*.
///
/// So a caller that must know the outcome of a batch it cannot replay has one
/// remedy: **re-read** and establish what happened. A caller may also retry
/// a [`CommitOutcome::Conflict`] — that is what `Conflict` is *for* — but the retry belongs to
/// the caller, who owns the decision the precondition encodes; a backend must not
/// retry a conditional batch internally, since re-reading the precondition at a
/// newer version would quietly turn the caller's compare-and-set into a
/// last-writer-wins overwrite.
///
/// # Operational envelope
///
/// The trait sets no key/value/batch size limits of its own; a backend's native
/// limits are **inherited and surface as `Err`** (FoundationDB's are the tightest
/// in play and are therefore the de-facto ceiling: 10 KB key, 100 KB value, 10 MB
/// and 5 s per transaction). The metadata model in `core` writes small records
/// and stays far inside them. Two envelope properties *are* contractual, because
/// they are correctness rather than tuning: the `scan` cap of clause 5, and that
/// **every operation terminates** — a backend must bound its own waiting rather
/// than block a caller forever on an unreachable cluster.
///
/// Termination is the backend's own responsibility, and a *networked* backend
/// cannot assume its client library provides it: FoundationDB's client retries an
/// unreachable cluster indefinitely, and tikv-client bounds each RPC attempt but
/// neither connection establishment nor the timestamp stream every operation opens
/// with — so both drivers impose their own deadline (#517). An **embedded** backend
/// (redb) satisfies the clause with nothing to add: it has no network to wait on.
/// Note the interaction with the unknown-result rule above: a `commit` abandoned at
/// a deadline is **undetermined**, not a definite failure — the store stopped
/// waiting, which is not the same as the cluster stopping.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Read the raw value stored under `key`, if any — the latest committed
    /// value, never a stale or cached one (contract clause 4).
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Return every `(key, value)` whose key begins with `prefix`, e.g. every
    /// dirent under a parent, as one consistent cut (contract clause 4). Order is
    /// unspecified. The result is complete or this returns `Err`; it is never
    /// silently truncated (clause 5).
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>>;

    /// Apply `batch` as a single atomic mutation — the commit point. Either
    /// every precondition holds and every put/delete lands, or nothing changes.
    ///
    /// Returns [`CommitOutcome::Conflict`] (not `Err`) when a precondition fails,
    /// so a stale writer is rejected distinguishably from a backend fault. An
    /// `Err` may be an *unknown-result* commit rather than a definite non-commit —
    /// see the trait's "Errors and the caller's obligations".
    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome>;
}

/// The result of a [`commit`](MetadataStore::commit).
///
/// The partition is three-way, and the third clause is the one the FoundationDB
/// port made load-bearing (#437):
///
/// 1. All preconditions held and the batch was applied → `Committed`.
/// 2. A **conditional** batch (one carrying at least one [`Precondition`]) lost —
///    either the precondition was already false, or it held at the batch's read
///    point and a concurrent writer invalidated it before the commit landed →
///    `Conflict`. Both are "a stale writer was rejected"; a backend must not
///    distinguish them, because a caller cannot act on the difference.
/// 3. A **blind** batch (one carrying NO preconditions) is **never** `Conflict`.
///    It has asserted nothing about prior state, so there is nothing for it to
///    lose; if it cannot be applied, that is `Err`. This is not a nicety: blind
///    writers throughout the codebase (`core::repair::enqueue_repair`, the
///    custodian's desired-state writes) `?` the call and ignore the
///    [`CommitOutcome`], so a `Conflict` returned to them would read as success
///    while the write silently vanished. An optimistic backend that must give up
///    on a blind batch therefore exhausts its retries into `Err`, and a
///    pessimistic one reports the lost race as `Err` — never as `Conflict`.
///
/// Pinned by `contract_require_absent_gates`, `contract_require_value_gates`,
/// `contract_rename_race_yields_conflict` (clause 2) and
/// `contract_blind_batch_is_never_conflict` (clause 3) in the shared suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// All preconditions held; the batch was applied.
    Committed,
    /// A conditional batch's precondition did not hold — because it was already
    /// false, or because a concurrent writer invalidated it before the commit
    /// landed. Nothing was written (e.g. a stale-version writer, or a name that
    /// already exists). A batch with no preconditions never yields this.
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
///
/// **A batch is not guaranteed idempotent.** Nothing here stops a caller from
/// building one whose re-application is not a no-op (a counter bump guarded by a
/// [`require`](WriteBatch::require) is the everyday case: replayed against the
/// value it just wrote, it no longer means what it meant). That is why a backend
/// may not blindly re-apply a batch whose commit returned an *unknown result* —
/// see [`MetadataStore`]'s "Errors and the caller's obligations". A caller that
/// wants replay safety must build that safety into the batch itself, with a
/// precondition that makes the second application a `Conflict`.
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
