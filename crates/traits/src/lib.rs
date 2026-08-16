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

/// One page of a [`MetadataStore::scan_page`] walk: the page's `(key, value)`
/// pairs in raw byte-lexicographic order, plus the cursor to resume from —
/// `Some(last key returned)` while more may remain, `None` only when the prefix
/// is exhausted at that instant.
///
/// Exactly `(Vec<(Vec<u8>, Bytes)>, Option<Vec<u8>>)`, the shape proposal 0016
/// fixes (`docs/design/proposals/draft/0016-multipart-commit-protocol.md:2648`);
/// it is named here only because every backend and every test double returns it.
pub type ScanPage = (Vec<(Vec<u8>, Bytes)>, Option<Vec<u8>>);

/// A [`MetadataStore::scan_page`]'s page bound resolved to **zero** keys — either
/// the caller asked for `limit == 0`, or the store's own effective cap is `0`. The
/// call is **rejected**, never answered with an empty page (#634).
///
/// An empty page is not a harmless answer to "give me nothing" — both of its
/// spellings are worse than an error. With `next: Some(_)` it is a *successful,
/// non-terminal* response that made no progress, which is exactly the shape that
/// makes a drain loop forever; with `next: None` it falsely reports the prefix
/// exhausted, which is the silent skip the paginated walk exists to prevent. And
/// the third spelling — answering a page anyway, ignoring a bound of zero — is an
/// **unbounded** page, the heap growth [`SCAN_CAP`] exists to stop.
///
/// A zero *cap* raises the same type as a zero *limit* deliberately: the two are
/// one defect class (no page can carry a key), a caller cannot act on the
/// difference, and one type keeps the classification single. The fields say which
/// side produced it, for the operator reading the message.
///
/// Like [`ScanCapExceeded`] this lives in the seam crate so **every** backend
/// raises the *same* type and a caller classifies it identically whichever store
/// it holds — `err.downcast_ref::<ZeroPageLimit>()` separates "no page was
/// possible" from a genuine backend fault (#516's rule, applied to the new
/// primitive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZeroPageLimit {
    /// The logical prefix the walk was over (lossy-rendered for operators).
    pub prefix: Vec<u8>,
    /// The page size the caller asked for.
    pub limit: usize,
    /// The store's own effective cap at that moment.
    pub cap: usize,
}

impl fmt::Display for ZeroPageLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadata scan_page under prefix {:?} resolved to a page bound of 0 keys \
             (limit {}, store cap {}): rejected rather than answered, because every \
             answer would be wrong — an empty page with a cursor makes a drain loop \
             forever with no progress, an empty page without one falsely reports the \
             prefix exhausted, and a page returned in spite of the bound is \
             unbounded (#634)",
            String::from_utf8_lossy(&self.prefix),
            self.limit,
            self.cap,
        )
    }
}

impl std::error::Error for ZeroPageLimit {}

/// The effective page size of one [`MetadataStore::scan_page`] call: the caller's
/// `limit` clamped to the store's own effective `cap`, or [`ZeroPageLimit`] when
/// that clamp resolves to **zero**.
///
/// All three rules resolved in **one** place so no backend can disagree about them
/// (#634, and the same single-definition argument [`SCAN_CAP`] itself carries,
/// #516):
///
/// * A `limit` **above** the store's cap is **clamped**, never an `Err`. The cap
///   refuses to be *raised* (a truncated listing is data loss, #262), but a
///   caller asking for a bigger page has asserted nothing and must not be failed
///   for it — it gets a smaller page and a cursor, which is a complete answer.
/// * A `limit` of **0** is an error, for the reasons on [`ZeroPageLimit`].
/// * A `cap` of **0** is the *same* error, and this is the load-bearing half: the
///   cap knobs clamp only from above (`cap.min(SCAN_CAP)`), so `with_scan_cap(0)`
///   is an accepted configuration whose `min` would otherwise hand every backend
///   an effective limit of zero — which a page loop that stops at `len >= limit`
///   answers with an **unbounded** page, exactly inverting the bound. The one
///   place that resolves the limit is the one place that must refuse it.
///
/// The returned limit is therefore always `>= 1`, and
/// `items.len() <= min(limit, cap)` holds by construction on every backend that
/// resolves its page bound here.
///
/// # Errors
///
/// [`ZeroPageLimit`] when `min(limit, cap) == 0`.
pub fn page_limit(limit: usize, cap: usize, prefix: &[u8]) -> Result<usize> {
    let resolved = limit.min(cap);
    if resolved == 0 {
        return Err(BoxError::from(ZeroPageLimit {
            prefix: prefix.to_vec(),
            limit,
            cap,
        }));
    }
    Ok(resolved)
}

/// Where one [`MetadataStore::scan_page`] page starts, resolved from the caller's
/// `after` by [`page_start`].
///
/// **Three arms, not an `Option`,** because the two degenerate cursors fail in
/// *opposite* directions and one of them cannot be expressed as a lower bound at
/// all — so a two-way answer left the second to each backend to notice, and two
/// of them did not (#634 iteration-2 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageStart<'a> {
    /// Start **inclusively at `prefix` itself**: there is no cursor, or the cursor
    /// sorts *below* the prefix.
    ///
    /// A backend that fed a below-the-prefix cursor straight into its range read
    /// would open the range on some *earlier* namespace, hit a key that does not
    /// carry `prefix`, and stop: an **empty terminal page** for a prefix that is
    /// not exhausted — a false "nothing left", which is precisely the silent skip
    /// clause 3 forbids. The page is the intersection of "after the cursor" and
    /// "under the prefix"; here the prefix is the binding half.
    Prefix,
    /// Start **strictly after** this cursor, which lies inside the prefix's range
    /// (`cursor` carries `prefix`, the `cursor == prefix` case included).
    After(&'a [u8]),
    /// The cursor is at or beyond the prefix's **exclusive upper bound**, so no
    /// key under the prefix can follow it: the page is **empty and terminal**
    /// (`items = []`, `next = None`) and the backend must not build a range for
    /// it at all.
    ///
    /// Why this is an arm of its own rather than "just another lower bound": the
    /// distributed backends read a **bounded** range `[cursor, upper_bound(prefix))`,
    /// which such a cursor **inverts** (`begin > end`) — and the substrates do
    /// *different* things with an inverted range. tikv-client resolves it against
    /// its transaction buffer with `BTreeMap::range`
    /// (`tikv-client-0.4.0/src/transaction/buffer.rs:129`), which **panics** on a
    /// start past its end, client-side, before any RPC; FoundationDB's key-selector
    /// form tolerates it and reads back nothing. A contract clause that every
    /// backend must answer identically cannot be left to that difference — least of
    /// all when one side of it is a panic in a metadata read, which no caller can
    /// handle. The seam decides it once, above every substrate.
    ///
    /// It is reachable from ordinary callers: a drain resuming from a cursor
    /// persisted under a different (earlier) namespace, a shared cursor column, or
    /// a walk whose prefix narrowed between laps.
    PastPrefix,
}

/// Resolve where one [`MetadataStore::scan_page`] page starts — the seam's answer
/// to contract clause 2, decided once here rather than hand-rolled per backend
/// (five implementations is five chances to get it wrong, and the shared
/// conformance clause asserts every arm through the trait).
///
/// The three arms are [`PageStart`]'s, and the enum is deliberate: a `match` on it
/// is **exhaustive**, so a backend cannot silently omit the terminal arm the way it
/// could ignore a `bool` — it does not compile.
///
/// The decision is made in **logical** key space and without computing the
/// prefix's upper bound, because a backend that keys physically would otherwise
/// have to redo it in its own space. That is sound: for any cursor `c`,
///
/// ```text
/// c >= upper_bound(prefix)   ⟺   c > prefix && !c.starts_with(prefix)
/// ```
///
/// — since a `c` above the prefix that does not carry it must differ from the
/// prefix at some byte it exceeds, which is at or past the incremented byte the
/// upper bound is built from. (When no upper bound exists — an empty or all-`0xff`
/// prefix, whose range runs to the end of the keyspace — no cursor satisfies the
/// right-hand side either, so both sides stay false together.)
#[must_use]
pub fn page_start<'a>(prefix: &[u8], after: Option<&'a [u8]>) -> PageStart<'a> {
    // No cursor at all: the page starts at the prefix.
    let Some(cursor) = after else {
        return PageStart::Prefix;
    };
    // The arms run in KEYSPACE order — below the prefix, inside it, above it — which
    // is also the order [`PageStart`] declares them in, and it is what keeps each
    // comparison's boundary an observable decision rather than a spelling: a cursor
    // *equal* to the prefix is decided here, by the `<` (it is not below), and then
    // by `starts_with` (it is inside, and means "everything strictly after `p:`").
    if cursor < prefix {
        // Below the prefix — including a cursor SHORTER than it (`p` for a `p:`
        // walk). The page is the intersection of "after the cursor" and "under the
        // prefix", so the prefix is the binding half and the page starts there.
        PageStart::Prefix
    } else if cursor.starts_with(prefix) {
        // Inside the prefix's range — an ordinary exclusive cursor. A cursor that
        // carries the prefix is never below it, so this arm can never be stolen by
        // the one above.
        PageStart::After(cursor)
    } else {
        // Above the prefix and not under it: at or beyond the range's exclusive end.
        PageStart::PastPrefix
    }
}

/// Whether a page of `got` pairs has reached its bound of `limit` — **the one
/// rule a backend's fill loop stops on and the one [`page_cursor`] emits a cursor
/// from**, deliberately the same function so the two can never disagree.
///
/// That coupling is the whole point, and it is a defect class rather than a
/// tidiness preference (#634, iteration-5 adversarial review). [`page_cursor`]
/// infers *why* a page stopped from its length: short means "the prefix is
/// exhausted at this instant", i.e. terminal. So a fill loop that stops for any
/// **other** reason — an off-by-one in its own comparison, a substrate chunk
/// boundary, a byte budget — hands back a short page that `page_cursor` then
/// labels `next: None`, and the caller stops walking a prefix that is not
/// exhausted. That is the silent skip the paginated primitive exists to prevent,
/// and it was demonstrated on a live cluster: with `>=` flipped to `<` inside
/// FoundationDB's chunk loop, a 600-key range answered 138 pairs with
/// `next: None` while every conformance clause stayed green (their populations fit
/// one chunk).
///
/// One `#[must_use]` function, called by both sides, makes the disagreement
/// unrepresentable: a loop that stops early either agrees the page is full (and
/// gets a cursor) or is not using this rule at all — which the shared conformance
/// clauses then catch.
///
/// `>=`, not `==`: a page that somehow over-filled its bound is still full, and
/// must report a resumable cursor rather than silently claim exhaustion.
#[must_use]
pub fn page_is_full(got: usize, limit: usize) -> bool {
    got >= limit
}

/// The `next` cursor of one [`MetadataStore::scan_page`] page: `Some(last key
/// returned)` on a page that filled its `limit`, `None` on a short one.
///
/// Contract clause 3 in one place (`0016:2657-2658`). A **short** page has
/// exhausted the prefix at that instant, so it is terminal; a **full** one may
/// have more behind it and hands back the last key it returned, which the next lap
/// passes as `after`. An empty page is short by definition and therefore never
/// carries a cursor — a successful, non-terminal answer that made no progress is
/// what makes a drain loop forever.
///
/// "Full" is [`page_is_full`], the same predicate every backend's fill loop stops
/// on — see there for why sharing it is load-bearing.
#[must_use]
pub fn page_cursor(items: &[(Vec<u8>, Bytes)], limit: usize) -> Option<Vec<u8>> {
    if page_is_full(items.len(), limit) {
        items.last().map(|(key, _)| key.clone())
    } else {
        None
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

/// A **transient** fault: the call failed for a reason that **may not hold a moment
/// later** — the peer was unreachable, a deadline expired, the backend was busy or
/// shedding load. Unlike [`IntegrityFault`], [`BlockReadFault`] and [`ScanCapExceeded`]
/// — for all of which *retrying the same call cannot help* — retrying this one may
/// simply succeed.
///
/// It lives in the seam crate for the reason [`IntegrityFault`] does (ADR-0010): **every**
/// backend raises the *same* type for the class, so one [`classify`] call answers "is this
/// worth retrying?" without the caller knowing which store it holds. Before it, everything
/// that was not one of the four *specific* typed faults crossed the seam as an opaque
/// `BoxError` string, and "the network dropped" was indistinguishable from "the config is
/// wrong" (proposal 0010 §Motivation, "Errors are opaque").
///
/// It **wraps rather than replaces** the backend's own error. The producing backend's
/// concrete error stays reachable through [`source`](std::error::Error::source) — the gRPC
/// client keeps its `TransportError`, so a caller that wants the wire `Status` still finds
/// it by walking the chain — while the *class* is now carried by a **type** instead of
/// being re-derived from a string at each consumer. That is exactly [`BlockReadFault`]'s
/// trick (a seam type in the chain that a chain-walking classifier finds), applied to the
/// one class the seam could not previously name.
#[derive(Debug)]
pub struct TransientFault {
    /// Why this fault is transient — the producing backend's own account, for the
    /// operator-facing audit trail.
    pub detail: String,
    source: Option<BoxError>,
}

impl TransientFault {
    /// A transient fault with no underlying error to carry.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            source: None,
        }
    }

    /// A transient fault wrapping the backend's own `source`, which stays reachable via
    /// [`source`](std::error::Error::source) — so naming the *class* never costs the
    /// detail the backend already had.
    pub fn with_source(detail: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self {
            detail: detail.into(),
            source: Some(source.into()),
        }
    }
}

impl fmt::Display for TransientFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transient fault (a retry may succeed): {}", self.detail)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for TransientFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// The **failure class** of an error crossing the trait seam: not *what* went wrong (the
/// specific typed faults above already say that) but **how a caller must read it** —
/// independent of which backend produced it.
///
/// This is the seam's half of "why did this request fail" (proposal 0010 §"Scope boundary"
/// item 6). The class is a *value*, not a set of boolean predicates, deliberately: it has
/// a **stable, bounded label form** ([`as_str`](Self::as_str)) over a **closed** set
/// ([`ALL`](Self::ALL)), so a consumer can key an error counter by it and pre-register
/// every series (issue #575) rather than discovering a label the first time something
/// breaks.
///
/// **The partition is not binary, and that is load-bearing.** A third outcome exists
/// because [`CommitUnknownResult`] is genuinely neither: retrying is *forbidden* (a
/// [`WriteBatch`] is not guaranteed idempotent) yet the write may still land, so calling
/// it "terminal" would tell a caller "nothing happened" when something may have. It gets
/// [`Indeterminate`](Self::Indeterminate) and is never collapsed into the binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// **Retrying may succeed**: unreachable, timed out, or busy — a [`TransientFault`].
    Transient,
    /// **Retrying cannot help**: a permanent store error, invalid config, a
    /// [`BlockReadFault`] (the device physically cannot return the bytes), a
    /// [`ScanCapExceeded`] — *and* every error the seam cannot otherwise classify, which
    /// is the fail-safe default (see [`classify`]).
    Terminal,
    /// **Stored data is corrupt** — an [`IntegrityFault`]. A **terminal** class
    /// ([`is_terminal`](Self::is_terminal) is true for it: retrying the same fetch cannot
    /// help), kept *distinct* because its consumer obligation is distinct: corruption is a
    /// durable **repair obligation** (reconstruct the chunk, emit a corruption finding),
    /// which no other terminal fault carries.
    Integrity,
    /// **The outcome is unknown** — a [`CommitUnknownResult`]: the batch may or may not
    /// have been applied. Neither transient (it must not be retried) nor terminal (it may
    /// have succeeded); the caller's only remedy is to re-read.
    Indeterminate,
}

impl ErrorClass {
    /// Every class, in a stable order — the **bounded label space** a consumer enumerates
    /// up front (issue #575's error-by-class counter pre-registers one series per class; a
    /// counter that only learns a label when the fault first fires reports nothing at all
    /// until something breaks).
    pub const ALL: [ErrorClass; 4] = [
        ErrorClass::Transient,
        ErrorClass::Terminal,
        ErrorClass::Integrity,
        ErrorClass::Indeterminate,
    ];

    /// The class's **stable** label — lowercase, single-word, and part of the contract:
    /// it keys metric series and appears in operator-facing logs, so renaming one breaks
    /// every dashboard and alert built on it. New classes may be added; these spellings do
    /// not change.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Transient => "transient",
            ErrorClass::Terminal => "terminal",
            ErrorClass::Integrity => "integrity",
            ErrorClass::Indeterminate => "indeterminate",
        }
    }

    /// Whether retrying the same call could plausibly succeed. This is the **only**
    /// predicate a retry policy may act on: [`Indeterminate`](Self::Indeterminate) is not
    /// terminal, but it must not be retried either, so `!is_terminal()` is *not* a licence
    /// to retry.
    #[must_use]
    pub fn is_transient(self) -> bool {
        matches!(self, ErrorClass::Transient)
    }

    /// Whether retrying cannot help — true for [`Terminal`](Self::Terminal) **and**
    /// [`Integrity`](Self::Integrity), which is a terminal class that stays distinct.
    /// False for [`Indeterminate`](Self::Indeterminate), whose outcome is not known to be
    /// a failure at all.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, ErrorClass::Terminal | ErrorClass::Integrity)
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The [`ErrorClass`] of `err` — the seam-level classifier that turns any error crossing
/// the seam into a class a caller can branch on, generalizing [`is_integrity_fault`] from
/// one fault to the whole surface.
///
/// Like the predicates it generalizes it walks [`source`](std::error::Error::source), so a
/// backend may wrap a seam fault in its own error and still be classified; the
/// **outermost** seam type it recognises wins, since that is the producer's most specific
/// statement about the failure.
///
/// The mapping (proposal 0010 §Design, fixed there so no backend guesses):
///
/// | error                    | class                          |
/// |--------------------------|--------------------------------|
/// | [`TransientFault`]       | [`Transient`](ErrorClass::Transient)         |
/// | [`IntegrityFault`]       | [`Integrity`](ErrorClass::Integrity) (terminal, distinct) |
/// | [`CommitUnknownResult`]  | [`Indeterminate`](ErrorClass::Indeterminate) |
/// | [`BlockReadFault`] / a raw `EIO` ([`is_block_read_fault`]) | [`Terminal`](ErrorClass::Terminal) |
/// | [`ScanCapExceeded`]      | [`Terminal`](ErrorClass::Terminal)           |
/// | [`ZeroPageLimit`]        | [`Terminal`](ErrorClass::Terminal)           |
/// | [`WriteDeadlineExpired`] | [`WriteDeadlineExpired::error_class`] — [`Terminal`](ErrorClass::Terminal) for a refusal, [`Indeterminate`](ErrorClass::Indeterminate) when the store could not establish the publication's timing |
/// | anything else            | [`Terminal`](ErrorClass::Terminal)           |
///
/// The [`BlockReadFault`] / [`ScanCapExceeded`] / [`ZeroPageLimit`] / "anything else" rows
/// are one arm — the **fail-safe default** — rather than explicit checks that would return
/// what the default already returns. That default is the whole safety argument: retry logic
/// must act only on a *known-transient* signal, because defaulting the unknown to transient
/// turns every unrecognised fault into a retry storm against a backend that will never
/// answer differently. [`WriteDeadlineExpired`] is the one row that cannot ride the default:
/// its [`Unknown`](WriteEffect::Unknown) case is genuinely *not* terminal — the bytes may
/// have landed — so collapsing it into the default would tell a caller "nothing happened"
/// when something may have, the exact mistake [`ErrorClass`]'s third outcome exists to
/// prevent. Each row is pinned by a unit test below, so the mapping is binding even where
/// it is not spelled out in code.
pub fn classify(err: &(dyn std::error::Error + 'static)) -> ErrorClass {
    let mut next = Some(err);
    while let Some(e) = next {
        if e.is::<IntegrityFault>() {
            return ErrorClass::Integrity;
        }
        if e.is::<CommitUnknownResult>() {
            return ErrorClass::Indeterminate;
        }
        if e.is::<TransientFault>() {
            return ErrorClass::Transient;
        }
        if let Some(deadline) = e.downcast_ref::<WriteDeadlineExpired>() {
            return deadline.error_class();
        }
        next = e.source();
    }
    ErrorClass::Terminal
}

/// A [`ChunkStore::put_fragment`] the store would not acknowledge because its authorization
/// deadline had elapsed — refused before publication, or published after the deadline had
/// passed. Proposal 0016 decision 5's server-enforced `W_write` bound (issue #638,
/// `docs/design/proposals/draft/0016-multipart-commit-protocol.md:1551-1576`).
///
/// A caller-side `await` timeout alone bounds only how long the *writer waits*, not when
/// an already-accepted write *takes effect*: a write parked between acceptance and
/// application could otherwise land arbitrarily late — after the reaper's `orphan:` grace
/// elapsed and the evidence protecting it was reclaimed (0016 outcome (a), the leak the
/// strict margin `G_orphan > W_write + δ_clock` (`0016:1478`) exists to prevent). So the
/// acceptor enforces the deadline itself, at the last instant before it publishes, and
/// **refuses rather than queues** (`0016:1560`) — this is what it raises instead.
///
/// It carries **what is known about the bytes** ([`effect`](Self::effect)), because a store
/// judges the deadline at two instants and only one of them can promise a clean refusal:
/// [`NotApplied`](WriteEffect::NotApplied) before it publishes (the ordinary case — a
/// definite, unconditional "the write did not take effect, and nothing of it is on the
/// store", which holds across a crash and which a caller-side timeout can never give), and
/// [`Unknown`](WriteEffect::Unknown) when the store could not *certify* that the publishing
/// step completed before the deadline. The second is what makes `Ok(())` mean something: a
/// store MUST NOT acknowledge a publication it did not verify landed in time, so a
/// successful `put_fragment` is a **checked** end-to-end claim rather than an assumption
/// that the publish syscall was quick (`0016:1557-1564` — the bound is on when the write
/// *takes effect*, not on when it was accepted).
///
/// This is an **expected, non-fault** outcome — the write was too late, the backend is
/// healthy — so, exactly like [`IntegrityFault`] / [`BlockReadFault`] / [`ScanCapExceeded`],
/// it lives in the seam crate: **every** backend raises the *same* type, and a caller tells
/// "refused, too late" apart from "the disk is broken" the same way on any of them
/// ([`is_write_deadline_expired`]). Its [`classify`] row is [`error_class`](Self::error_class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteDeadlineExpired {
    /// The fragment write that was refused.
    pub id: FragmentId,
    /// What is known about the write's bytes — the difference between "certainly nothing
    /// landed" and "the store cannot say". A caller MUST branch on this rather than assume
    /// the clean case: see [`WriteEffect`].
    pub effect: WriteEffect,
    /// Detail for the durability audit trail: the deadline the write carried and what the
    /// **acceptor's** clock read when it refused — the two evaluation sites `δ_clock`
    /// bounds the skew between. Rendered by [`WriteDeadlineExpired::if_elapsed`] /
    /// [`WriteDeadlineExpired::if_publication_unverified`] at the store that enforced it,
    /// and carried verbatim across the wire when the gRPC client reconstructs the class (the
    /// same shape [`IntegrityFault`] uses, for the same reason: the reconstruction must not
    /// fabricate readings it never took).
    pub detail: String,
}

/// What a [`WriteDeadlineExpired`] establishes about the write's bytes (issue #638).
///
/// The distinction is not cosmetic: one value promises the store is untouched, the other
/// promises nothing at all. Collapsing them would make the clean promise a lie in exactly
/// the case where it matters — and, in the other direction, an implementation that reported
/// the *uncertain* case as a definite lateness would be asserting something its evidence
/// does not support (a clock read that follows a syscall dates the read, not the syscall).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEffect {
    /// **Nothing landed.** The store judged the write too late *before* it published
    /// anything and restored its pre-write state (no fragment, no scratch), so the fragment
    /// is not observable through [`ChunkStore::get_fragment`] and never was. This holds
    /// across a crash — there is no interval in which the bytes were readable — because the
    /// implementation contract requires the judgment to precede publication. If a store
    /// cannot restore that state it must report the cleanup failure as a **backend fault**
    /// instead of this verdict, so `NotApplied` is never returned over residue.
    NotApplied,
    /// **The store cannot say whether the write took effect in its window.** It began
    /// publishing while the write was still live, but the publishing step (a `rename`, an
    /// object-store `PUT`, …) is a step whose duration the store neither bounds nor cancels,
    /// and the first clock reading it could take once that step returned was already at or
    /// past the deadline. So the fragment may be on the store, in window or late, and the
    /// caller may not count the write either way — its only remedy is to re-read, exactly
    /// like [`CommitUnknownResult`].
    ///
    /// **It is deliberately not called "published late".** The reading that produced it
    /// dates the *observation*, not the publication: a thread descheduled after a perfectly
    /// timely `rename` takes the same reading as one whose `rename` genuinely straddled the
    /// deadline, and the store has no way to tell them apart. Naming the outcome after the
    /// worse of the two would assert a fact the evidence does not carry, and would make an
    /// in-window write look like a leak to every consumer that reads the label.
    ///
    /// The store deliberately does **not** unlink whatever landed: retraction is not atomic
    /// with the publication (a crash in between leaves the bytes anyway) and it deletes by
    /// *path*, so it can destroy a concurrent same-id writer's already-acknowledged
    /// fragment — trading an uncertain write for certain data loss. Reporting is the sound
    /// half; bytes that did land late are garbage their position's evidence covers
    /// (`0016:1547-1550`, position coverage), while an erased live fragment is unrecoverable.
    Unknown,
}

impl WriteEffect {
    /// Whether the store's bytes **may** be present — true only for
    /// [`Unknown`](WriteEffect::Unknown). [`NotApplied`](WriteEffect::NotApplied) is the
    /// definite negative, so this is the predicate a caller uses to decide whether it must
    /// re-read before concluding anything about durable state.
    #[must_use]
    pub fn may_have_landed(self) -> bool {
        matches!(self, WriteEffect::Unknown)
    }
}

impl WriteDeadlineExpired {
    /// The **one** place the seam decides whether a write is too late: `Some(refusal)` iff
    /// the acceptor's clock has reached `deadline_millis`, `None` while the write is still
    /// live. Every backend calls this rather than re-deriving the comparison, so
    /// `chunkstore-fs`, the gRPC D server and any future store cannot disagree about what
    /// the deadline *means* (the reason a caller may not get a weaker guarantee by holding
    /// a local store, issue #638 leg E). *Where* a backend calls it is the backend's own
    /// obligation and is fixed by [`ChunkStore::put_fragment`]'s contract: immediately
    /// before the step that publishes.
    ///
    /// The comparison is `now >= deadline` — **inclusive**, i.e. the deadline instant
    /// itself is already too late. That is the fail-closed direction, and it matches GC's
    /// own inclusive grace test (`crates/custodian/src/gc.rs:174`), so the arithmetic
    /// `G_orphan > W_write + δ_clock` (`0016:1478`) composes without a boundary tick that
    /// belongs to neither side.
    ///
    /// It takes `now_millis` rather than reading a clock: the seam crate owns no clock, and
    /// the read belongs to the lifecycle that applies the write (the store's own clock —
    /// AGENTS.md § Review rubric, "one clock per correctness lifecycle").
    pub fn if_elapsed(id: FragmentId, deadline_millis: u64, now_millis: u64) -> Option<Self> {
        (now_millis >= deadline_millis).then(|| Self {
            id,
            effect: WriteEffect::NotApplied,
            detail: format!(
                "authorization deadline {deadline_millis} ms (epoch) had elapsed when the \
                 store reached the point of publishing the write; the store's clock read \
                 {now_millis} ms"
            ),
        })
    }

    /// The **post-publication** half of the same comparison: `Some(unknown)` iff the store's
    /// clock had already reached `deadline_millis` at the first reading it could take once
    /// its publishing step *returned*.
    ///
    /// This is what keeps `Ok(())` from being an assumption. The pre-publication verdict
    /// ([`if_elapsed`](Self::if_elapsed)) can only establish that publication had not yet
    /// *begun* too late; the publishing step is a syscall (or an RPC) whose own latency no
    /// caller controls, and on a slow or hung device it can straddle the deadline. Calling
    /// this immediately after that step — the same clock, one more read — is how a store
    /// learns it **cannot certify** the landing, and the contract then forbids acknowledging
    /// the write ([`ChunkStore::put_fragment`]). Without it, `Ok(())` would mean "publication
    /// started in time", which is precisely the "bounds acceptance, not effect" gap 0016
    /// rejects for caller-side timeouts (`0016:1557-1564`).
    ///
    /// What it does **not** establish is that the publication *was* late — a clock read
    /// timestamps the read, not the syscall that preceded it, so a timely `rename` followed
    /// by a descheduled thread is indistinguishable here from one that genuinely overran.
    /// Hence [`WriteEffect::Unknown`] and [`ErrorClass::Indeterminate`]: the store reports
    /// what it knows (nothing, about the timing) rather than the worse of the two readings.
    /// The fail-closed direction is `Ok(())` only on a *verified* in-window landing, so the
    /// uncertainty is charged to the write, never to the caller's safety argument.
    pub fn if_publication_unverified(
        id: FragmentId,
        deadline_millis: u64,
        now_millis: u64,
    ) -> Option<Self> {
        (now_millis >= deadline_millis).then(|| Self {
            id,
            effect: WriteEffect::Unknown,
            detail: format!(
                "the store could not verify that this write was published before its \
                 authorization deadline {deadline_millis} ms (epoch): the first clock reading \
                 taken after its publishing step returned was {now_millis} ms. The bytes may \
                 or may not be on the store, and may or may not have landed in window"
            ),
        })
    }

    /// This outcome's [`ErrorClass`] — the row [`classify`] returns for it.
    ///
    /// [`NotApplied`](WriteEffect::NotApplied) is [`Terminal`](ErrorClass::Terminal):
    /// re-sending the *same* expired authorization can never succeed, so the caller must
    /// re-authorize rather than retry. [`Unknown`](WriteEffect::Unknown) is
    /// [`Indeterminate`](ErrorClass::Indeterminate) for the reason [`CommitUnknownResult`]
    /// is — the call failed *and* durable state may have changed, so calling it terminal
    /// would tell a caller "nothing happened" when something may have
    /// ([`ErrorClass`]'s "the partition is not binary, and that is load-bearing").
    #[must_use]
    pub fn error_class(&self) -> ErrorClass {
        match self.effect {
            WriteEffect::NotApplied => ErrorClass::Terminal,
            WriteEffect::Unknown => ErrorClass::Indeterminate,
        }
    }
}

impl fmt::Display for WriteDeadlineExpired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The verdict and the remedy are read off the **same** effect, because the two
        // outcomes need opposite next moves and a single suffix can only be right for one of
        // them: `NotApplied` is definite ("nothing landed"), so the caller re-authorizes and
        // writes again; `Unknown` says durable state may have changed, so the caller must
        // **re-read** first — re-authorizing over it would either duplicate a write that did
        // land or, worse, record it as never written. This is `CommitUnknownResult`'s own
        // register ("the caller must re-read to establish what happened") and the reason
        // `WriteEffect::may_have_landed` exists.
        let (verdict, remedy) = match self.effect {
            WriteEffect::NotApplied => (
                "refused and NOT applied",
                "re-authorize and write again, do not retry this authorization",
            ),
            WriteEffect::Unknown => (
                "NOT acknowledged: its publication could not be verified",
                "re-read to establish what landed before counting this write either way",
            ),
        };
        write!(
            f,
            "fragment write {verdict} (chunk {:032x} index {}): {} \
             (proposal 0016 decision 5's W_write — {remedy})",
            self.id.chunk, self.id.index, self.detail,
        )
    }
}

impl std::error::Error for WriteDeadlineExpired {}

/// Whether `err` is a [`WriteDeadlineExpired`] refusal anywhere in its source chain — the
/// seam-level classifier a caller uses to tell an **expected** "refused, too late" apart
/// from a genuine backend fault, mirroring [`is_integrity_fault`] /
/// [`is_block_read_fault`]. Walks [`source`](std::error::Error::source) so a backend (or
/// the gRPC client) may wrap the refusal in its own type and still be classified.
pub fn is_write_deadline_expired(err: &(dyn std::error::Error + 'static)) -> bool {
    write_deadline_outcome(err).is_some()
}

/// The [`WriteDeadlineExpired`] in `err`'s source chain, if any — the accessor a consumer
/// that must branch on [`WriteEffect`] uses, rather than re-implementing the chain walk (the
/// gRPC D server does, to give the two effects distinct wire codes). Same walk as
/// [`is_write_deadline_expired`], which is defined in terms of it so the two can never
/// disagree about what counts as a deadline outcome.
#[must_use]
pub fn write_deadline_outcome<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a WriteDeadlineExpired> {
    let mut next = Some(err);
    while let Some(e) = next {
        if let Some(found) = e.downcast_ref::<WriteDeadlineExpired>() {
            return Some(found);
        }
        next = e.source();
    }
    None
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
    ///
    /// `deadline_millis` is the write's **authorization deadline** in epoch
    /// milliseconds — proposal 0016 decision 5's `W_write` (issue #638,
    /// `docs/design/proposals/draft/0016-multipart-commit-protocol.md:1551-1576`).
    /// A caller-side timeout bounds only how long the writer *waits*, so the
    /// deadline travels with the write and **the implementation itself must
    /// enforce it**:
    ///
    /// * It **refuses** a write whose deadline has elapsed, returning
    ///   [`WriteDeadlineExpired`] ([`WriteDeadlineExpired::if_elapsed`] is the shared
    ///   comparison, so every backend means the same thing by "too late").
    /// * The judgment happens at the implementation's **publication point**: the last
    ///   instant before the single step that makes the fragment visible, with every
    ///   segment that can consume unbounded time — an accept queue, a thread-pool
    ///   queue, directory creation, the data write — already behind it. That is what
    ///   makes the deadline a bound on when the write *takes effect* and not merely on
    ///   when it was accepted; a write that sat anywhere upstream is refused rather than
    ///   queued (`0016:1560`). An implementation may *also* refuse an already-expired
    ///   write on entry — that costs no I/O — but that check is an optimisation, never
    ///   the bound.
    /// * The judgment MUST **precede** publication, not follow it. This is the
    ///   crash-safety clause, and it is why the ordering is normative rather than an
    ///   implementation detail: an implementation that publishes first and then retracts
    ///   has a window in which the bytes are readable and, if the process dies inside
    ///   it, durably present — so [`WriteEffect::NotApplied`] would be a lie exactly
    ///   when it matters; its retraction is also not atomic with the publication, so it
    ///   can destroy a *concurrent* same-id writer's acknowledged fragment. Judging
    ///   first has neither hazard.
    /// * A [`NotApplied`](WriteEffect::NotApplied) refusal MUST leave the store
    ///   **exactly as if the write had never arrived**: the fragment MUST NOT be
    ///   observable through [`ChunkStore::get_fragment`], and no scratch and no
    ///   part-written object may be left behind. Refusing the caller while *keeping* the
    ///   bytes is precisely the leak (0016 outcome (a)) the deadline exists to prevent.
    ///   An implementation that **cannot** restore that state (the unlink was denied, the
    ///   object-store delete failed) MUST report that failure as a **backend fault** — a
    ///   fault a caller distinguishes from a refusal by [`is_write_deadline_expired`] —
    ///   and MUST NOT return the definite [`NotApplied`](WriteEffect::NotApplied) verdict
    ///   over residue it did not remove. Reporting a clean refusal it did not achieve is
    ///   a silent skip (AGENTS.md § Review rubric, *Absent or unsupported entries*), and
    ///   it is what lets the residue accumulate unnoticed. Container state an
    ///   implementation *shares* with concurrent writes (a chunk directory, a bucket
    ///   prefix) is **not** part of the write's bytes and MUST NOT be removed by the
    ///   refusal: a container left empty is invisible through this trait — nothing is
    ///   observable through [`ChunkStore::get_fragment`] or
    ///   [`ChunkStore::list_fragments`] because of it — whereas removing it can strip it
    ///   from under a live writer that is creating the same container, failing a *live*
    ///   write on behalf of an expired one. That hazard is not bounded by retrying the
    ///   creation: N retries lose to N+1 racing refusals. An implementation that collects
    ///   empty containers does it where no write of that store is in flight (`FsChunkStore`
    ///   does it at `open`) and by an operation atomic in the container's emptiness, so
    ///   collection can never take a container somebody is putting bytes into.
    /// * Publication itself is **verified, not assumed**. The publishing step takes
    ///   non-zero time — a `rename(2)`, an object `PUT` — and neither its duration nor
    ///   its cancellation is under the store's control, so on a hung device it can
    ///   straddle the deadline. The implementation therefore re-reads its clock
    ///   immediately **after** that step
    ///   ([`WriteDeadlineExpired::if_publication_unverified`]) and, unless that reading
    ///   is still inside the window, returns [`Unknown`](WriteEffect::Unknown) rather
    ///   than `Ok(())`. So `Ok(())` means *the fragment was published strictly before its
    ///   deadline*, checked at both ends of the publishing step — not "publication
    ///   started in time", which is the very "bounds acceptance, not effect" gap 0016
    ///   rejects for caller-side timeouts. An implementation MUST NOT report that case as
    ///   a definite late landing: the reading dates the observation, not the syscall (see
    ///   [`WriteEffect::Unknown`]). The bytes, if any, stay where they are — retracting
    ///   them is the worse trade, for the reasons recorded on that variant.
    ///
    /// `None` means the caller supplied no deadline: the write is unbounded —
    /// **exactly the pre-#638 behaviour** — which is what keeps every existing
    /// writer (the ordinary write path, backfill, reconstruction, rebalance)
    /// working unchanged.
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()>;

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
    /// and delegates to [`ChunkStore::put_fragment`], forwarding `deadline_millis`
    /// **unchanged** — routing a write must never strip its authorization deadline
    /// (issue #638), since the store it routes to is the site that enforces it.
    async fn put_fragment_at(
        &self,
        _dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.put_fragment(id, fragment, deadline_millis).await
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
/// rather than truncate. A namespace that outgrows that cap is enumerated by
/// [`scan_page`](MetadataStore::scan_page) instead — pagination is the way past
/// the bound, never a truncated `scan` (#634).
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

    /// Read **one bounded page** of the keys beginning with `prefix`: at most
    /// `limit` pairs, starting strictly after `after`, plus the cursor to resume
    /// from.
    ///
    /// This is how a namespace larger than [`SCAN_CAP`] is enumerated **at all**.
    /// `scan` is complete-or-fail-loud (clause 5), so a population past the cap
    /// cannot be read by it at any size, and two multipart-era namespaces cross
    /// it: the deliberately unbounded `retire:` drain, and GC's `orphan:` ledger,
    /// where one maximum segmented-object retirement installs ~1.78 M marks
    /// against a cap of 1,048,576 (proposal 0016, "What the implementing slices
    /// change"; #634).
    ///
    /// **The signature is not the contract.** `scan` leaves order *unspecified*,
    /// and a `scan_page` that inherited that freedom could return a continuation
    /// that silently **skips** a key — a skipped `retire:` obligation retains its
    /// bytes and its records forever. Four clauses therefore bind every backend,
    /// and the shared `wyrd-metadata-conformance` suite asserts each of them on
    /// every implementation (`0016:2653-2666`):
    ///
    /// 1. **Order.** Results are ordered by **raw byte-lexicographic key**,
    ///    identically on every backend — not by decoded string, not by insertion.
    /// 2. **Exclusive cursor.** `after` is exclusive: the page starts strictly
    ///    after that key. An inclusive cursor re-yields the boundary key forever.
    ///    The page is the intersection of "after the cursor" and "under the
    ///    prefix", so both degenerate cursors have an answer and it is a *page*,
    ///    never an error ([`page_start`]): an `after` *below* `prefix` starts the
    ///    page at `prefix` itself rather than claiming the prefix is done, and an
    ///    `after` at or beyond the prefix's exclusive upper bound is an **empty
    ///    terminal page** rather than an inverted range read.
    /// 3. **Termination.** `next` is `Some(last key returned)` while more may
    ///    remain, and `None` **only** when the prefix is exhausted at that
    ///    instant. An empty page is therefore always terminal.
    /// 4. **No-skip for stable keys.** Under concurrent mutation, a key present
    ///    **throughout** the walk and not lexicographically before the cursor is
    ///    returned **exactly once**. Everything else is unconstrained: keys
    ///    inserted before the cursor after it passed, or deleted mid-walk, may be
    ///    missed or duplicated, and **no snapshot isolation is required of any
    ///    backend**. The asymmetry (duplicates permitted, skips forbidden) is
    ///    what keeps this implementable on redb, FoundationDB and TiKV alike, and
    ///    it is exactly what the idempotent retirement drain needs: a duplicate is
    ///    a no-op, a skip is unbounded retention.
    ///
    /// # The page bound
    ///
    /// `items.len() <= min(limit, the store's effective cap)` always — no page
    /// may exceed [`SCAN_CAP`], which is what keeps a page inside the heap bound
    /// `scan` protects (`0016:2647-2650`). A `limit` above the cap is **clamped**,
    /// never an `Err`; a page bound that resolves to `0` — from `limit == 0` or
    /// from a store cap of `0` — is rejected with [`ZeroPageLimit`] rather than
    /// answered with a page that makes no progress (or, worse, an unbounded one).
    /// [`page_limit`] resolves all of that in one place, and every backend calls
    /// it; [`page_cursor`] and [`page_start`] do the same for clauses 3 and 2, so
    /// the five implementations share one decision each instead of five
    /// hand-rolled copies. [`page_is_full`] is the fourth, and the one a backend
    /// is most tempted to re-spell inline: the rule its fill loop stops on **is**
    /// the rule `next` is derived from, so a loop that stops on its own comparison
    /// can hand back a short page that then falsely reports the prefix exhausted.
    ///
    /// # Errors
    ///
    /// [`ZeroPageLimit`] when the page bound resolves to zero, and whatever the
    /// backend raises for a fault of its own. A page **never** fails with
    /// [`ScanCapExceeded`]: escaping that failure is the method's whole purpose.
    ///
    /// # Why there is no default body
    ///
    /// A default implementation over [`scan`](MetadataStore::scan) would inherit
    /// the very cap this method exists to escape, and **nothing could detect the
    /// inheritance**: a store's cap knob is a per-backend inherent method, not
    /// part of this trait, so the conformance suite cannot lower a backend's cap
    /// through the seam and reach the cap-escape clause generically. An
    /// undetectable wrong default in the one primitive whose whole purpose is
    /// escaping the cap is not a trade worth making, so this method is
    /// **required**. Test doubles that only need *a* body delegate to
    /// `wyrd_testkit::test_double_scan_page` — which pages over `scan` and
    /// therefore inherits the cap, and which lives in ADR-0009's test-seam crate.
    /// Each of the three production metadata backends takes `wyrd-testkit` as a
    /// **dev-dependency only**, so a `MetadataStore` body there naming that helper
    /// does not compile; see the helper's own docs for the exact reach of that
    /// backstop, which is narrower than "no production crate can name it".
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage>;

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

#[cfg(test)]
mod error_class_tests {
    use super::*;

    fn frag() -> FragmentId {
        FragmentId { chunk: 7, index: 0 }
    }

    /// A backend error that wraps another — the shape every `classify` claim about
    /// source-chain walking rests on.
    #[derive(Debug)]
    struct Wrapper(BoxError);

    impl fmt::Display for Wrapper {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a backend wrapped: {}", self.0)
        }
    }

    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    /// The row of the mapping table that only a *type* can carry: a transient fault
    /// classifies `Transient` — the class the seam previously had no way to express, so
    /// every unreachable/timed-out/busy failure arrived as an opaque string.
    #[test]
    fn a_transient_fault_classifies_transient() {
        let err = TransientFault::new("the D server did not answer");
        assert_eq!(classify(&err), ErrorClass::Transient);
        assert!(classify(&err).is_transient());
        assert!(!classify(&err).is_terminal());
    }

    /// `IntegrityFault` stays a **distinct** class AND is terminal — both halves, because
    /// collapsing it into `Terminal` would lose the repair obligation, and calling it
    /// non-terminal would invite a retry of bytes that can never verify.
    #[test]
    fn an_integrity_fault_classifies_integrity_and_is_terminal() {
        let err = IntegrityFault {
            id: frag(),
            detail: "checksum mismatch".into(),
        };
        assert_eq!(classify(&err), ErrorClass::Integrity);
        assert!(classify(&err).is_terminal());
        assert!(!classify(&err).is_transient());
    }

    /// `CommitUnknownResult` is carried as its own outcome and **never** collapsed into
    /// the binary partition: it is neither transient (it must not be retried) nor terminal
    /// (the batch may still have landed).
    #[test]
    fn an_unknown_commit_result_is_indeterminate_and_neither_half_of_the_binary() {
        let err = CommitUnknownResult {
            backend: "foundationdb",
            code: Some(1021),
            detail: "commit_unknown_result".into(),
            may_still_commit: false,
        };
        assert_eq!(classify(&err), ErrorClass::Indeterminate);
        assert!(!classify(&err).is_transient(), "it must never be retried");
        assert!(
            !classify(&err).is_terminal(),
            "it must never be reported as a definite failure — the write may have landed"
        );
    }

    /// The `Terminal` rows of the mapping table that reach the answer through the fail-safe
    /// default rather than an explicit arm. They are pinned here precisely *because* they
    /// are not spelled out in `classify`'s body.
    #[test]
    fn the_permanent_faults_classify_terminal() {
        let block = BlockReadFault::new(frag(), "dead sector");
        assert_eq!(classify(&block), ErrorClass::Terminal);

        let raw_eio = std::io::Error::from_raw_os_error(5);
        assert_eq!(classify(&raw_eio), ErrorClass::Terminal);

        let cap = ScanCapExceeded {
            cap: SCAN_CAP,
            prefix: b"inode:".to_vec(),
        };
        assert_eq!(classify(&cap), ErrorClass::Terminal);

        // The paginated read's own refusal (#634). A page bound of zero is a caller's
        // argument or a store's configuration, neither of which the next attempt
        // changes — so the class that must never attach to it is `Transient`: a drain
        // that read this as "try again" would spin on a call that can only ever be
        // refused, and one that read it as indeterminate would wait for an outcome
        // that does not exist. Pinned here because `classify` reaches it through the
        // fail-safe default, where a later explicit arm could silently move it.
        let zero = ZeroPageLimit {
            prefix: b"retire:".to_vec(),
            limit: 0,
            cap: SCAN_CAP,
        };
        assert_eq!(classify(&zero), ErrorClass::Terminal);
        assert!(!classify(&zero).is_transient());
    }

    /// The safety property: an error the seam does not recognise defaults to **terminal**,
    /// never transient. A default-transient would turn every unknown fault into a retry
    /// storm against a backend that will never answer differently.
    #[test]
    fn an_unclassifiable_error_defaults_to_terminal_not_transient() {
        let err = std::io::Error::other("something nobody has typed yet");
        assert_eq!(classify(&err), ErrorClass::Terminal);
        assert!(!classify(&err).is_transient());
    }

    /// A backend may wrap a seam fault in its own error and still be classified — the
    /// property that lets a producer add context without destroying the class.
    #[test]
    fn classification_walks_the_source_chain() {
        let wrapped = Wrapper(Box::new(TransientFault::new("PD unreachable")));
        assert_eq!(classify(&wrapped), ErrorClass::Transient);

        let wrapped = Wrapper(Box::new(IntegrityFault {
            id: frag(),
            detail: "bit rot".into(),
        }));
        assert_eq!(classify(&wrapped), ErrorClass::Integrity);
    }

    /// Wrapping must not cost the backend its own detail: the wrapped error stays
    /// reachable, so a caller that wants the concrete fault still finds it.
    #[test]
    fn a_transient_fault_keeps_its_source_reachable() {
        let err = TransientFault::with_source(
            "the request deadline expired",
            std::io::Error::from(std::io::ErrorKind::TimedOut),
        );
        let source = std::error::Error::source(&err).expect("the wrapped error is the source");
        assert!(
            source.downcast_ref::<std::io::Error>().is_some(),
            "the producing backend's own error must survive classification"
        );
        assert!(
            err.to_string().contains("the request deadline expired"),
            "the class and the detail both reach an operator: {err}"
        );
    }

    /// The label form issue #575's error counter keys on: stable, bounded, and total over
    /// `ALL` — every class has a distinct label, and `ALL` really does enumerate them.
    #[test]
    fn the_label_space_is_stable_bounded_and_distinct() {
        assert_eq!(ErrorClass::Transient.as_str(), "transient");
        assert_eq!(ErrorClass::Terminal.as_str(), "terminal");
        assert_eq!(ErrorClass::Integrity.as_str(), "integrity");
        assert_eq!(ErrorClass::Indeterminate.as_str(), "indeterminate");

        let mut labels: Vec<&str> = ErrorClass::ALL.iter().map(|c| c.as_str()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(
            labels.len(),
            ErrorClass::ALL.len(),
            "each class needs its own label, or a counter keyed by it merges two classes"
        );
        // `Display` and `as_str` must not drift apart — both are operator-facing.
        for class in ErrorClass::ALL {
            assert_eq!(class.to_string(), class.as_str());
        }
    }

    /// Exactly one of the two dispositions holds for every class, and `Indeterminate`
    /// holds neither — the property that stops `!is_terminal()` being read as "safe to
    /// retry".
    #[test]
    fn the_dispositions_partition_the_class_space_correctly() {
        for class in ErrorClass::ALL {
            assert!(
                !(class.is_transient() && class.is_terminal()),
                "{class} cannot be both"
            );
        }
        assert!(ErrorClass::ALL.iter().any(|c| c.is_transient()));
        assert!(ErrorClass::Integrity.is_terminal(), "integrity is terminal");
        assert!(
            !ErrorClass::Indeterminate.is_transient() && !ErrorClass::Indeterminate.is_terminal(),
            "an undetermined commit is neither — that is why the partition is not binary"
        );
    }

    /// `:if_elapsed` — the **one** comparison every backend shares (issue #638), pinned at
    /// its boundary. A write is live strictly *before* its deadline and too late from the
    /// deadline instant on (the fail-closed direction, matching GC's inclusive grace test at
    /// `crates/custodian/src/gc.rs:174` so `G_orphan > W_write + δ_clock` has no tick that
    /// belongs to neither side). Mutating `>=` to `>` — the tempting "one more tick" — is
    /// caught here.
    #[test]
    fn a_write_is_live_before_its_deadline_and_refused_from_the_deadline_instant_on() {
        assert!(
            WriteDeadlineExpired::if_elapsed(frag(), 1_000, 999).is_none(),
            "one millisecond before its deadline the write is still live"
        );
        assert!(
            WriteDeadlineExpired::if_elapsed(frag(), 1_000, 1_000).is_some(),
            "the deadline instant itself is already too late (fail-closed)"
        );
        let refusal = WriteDeadlineExpired::if_elapsed(frag(), 1_000, 5_000)
            .expect("well past the deadline is refused");
        assert_eq!(refusal.id, frag());
        // Both evaluation sites' readings survive into the audit trail: the authorizer's
        // deadline and the acceptor's own clock (what `δ_clock` bounds the skew between).
        let rendered = refusal.to_string();
        assert!(
            rendered.contains("1000") && rendered.contains("5000"),
            "the refusal must record the deadline AND the acceptor's clock reading: {rendered}"
        );
        assert!(
            rendered.contains("NOT applied"),
            "an operator must be able to read off that the write did not happen: {rendered}"
        );
    }

    /// A deadline refusal is classifiable across the seam (issue #638 leg F): recognised by
    /// type — through a wrapper, so a backend or the gRPC client may add context — and
    /// **never** confused with a genuine backend fault. It classifies `Terminal` (re-sending
    /// the same expired authorization can never succeed) but, unlike a fault, it says the
    /// write definitively did not happen.
    #[test]
    fn a_deadline_refusal_is_typed_distinguishable_and_terminal() {
        let refusal = WriteDeadlineExpired::if_elapsed(frag(), 1_000, 2_000).expect("expired");
        assert!(is_write_deadline_expired(&refusal));
        let wrapped = Wrapper(Box::new(refusal.clone()));
        assert!(
            is_write_deadline_expired(&wrapped),
            "the classifier must walk the source chain, like is_integrity_fault"
        );
        assert_eq!(classify(&refusal), ErrorClass::Terminal);

        // The other direction: genuine backend faults must never read as a deadline
        // refusal, or a caller would silently treat a broken disk as "just too late".
        for fault in [
            Box::new(IntegrityFault {
                id: frag(),
                detail: "checksum mismatch".into(),
            }) as BoxError,
            Box::new(BlockReadFault::new(frag(), "dead sector")),
            Box::new(std::io::Error::from_raw_os_error(5)),
            Box::new(TransientFault::new("the D server did not answer")),
        ] {
            assert!(
                !is_write_deadline_expired(fault.as_ref()),
                "a genuine backend fault is not a deadline refusal: {fault}"
            );
        }
        assert!(
            !is_integrity_fault(&refusal) && !is_block_read_fault(&refusal),
            "and a deadline refusal is not a fault: {refusal}"
        );
    }

    /// `:if_publication_unverified` — the **post-publication** half of the bound (issue
    /// #638). It shares `if_elapsed`'s boundary exactly (a publication whose following
    /// reading is one millisecond before the deadline is verified in window; one taken *at*
    /// the deadline is not), and it renders a different verdict, because the two say
    /// different things about the bytes.
    #[test]
    fn a_publication_the_store_could_not_time_is_reported_as_unknown_not_as_late() {
        assert!(
            WriteDeadlineExpired::if_publication_unverified(frag(), 1_000, 999).is_none(),
            "publication verified inside the window: the write is acknowledged"
        );
        let unknown = WriteDeadlineExpired::if_publication_unverified(frag(), 1_000, 1_000)
            .expect("the deadline instant itself is already too late (fail-closed)");
        assert_eq!(unknown.effect, WriteEffect::Unknown);
        assert!(
            unknown.effect.may_have_landed(),
            "the caller must be able to see that durable bytes may exist"
        );
        let rendered = unknown.to_string();
        assert!(
            rendered.contains("could not verify") && rendered.contains("NOT acknowledged"),
            "the operator-facing rendering must not read as a clean refusal: {rendered}"
        );
        // The negative half, and the reason this variant is not called `PublishedLate`: a
        // reading taken after the syscall dates the reading, so the rendering must not
        // assert a late landing it cannot evidence.
        assert!(
            !rendered.contains("landed late"),
            "an unverified publication must not be reported as a definite late landing — a \
             timely rename followed by a descheduled thread reads identically: {rendered}"
        );

        // **The remedy is read off the effect, not stamped on every outcome.** `Unknown`
        // means durable state may have changed, so the caller's next move is a *re-read*;
        // telling it to re-authorize would send it to write again over bytes that may
        // already be there, or to record as unwritten a fragment that landed. The clean
        // refusal's rendering says the opposite thing, and the pair is asserted together so
        // neither can be collapsed back into one suffix.
        assert!(
            rendered.contains("re-read"),
            "an indeterminate outcome must send the caller to re-read: {rendered}"
        );
        assert!(
            !rendered.contains("re-authorize"),
            "and must NOT send it to re-authorize, which is the definite case's remedy: \
             {rendered}"
        );
        let refused = WriteDeadlineExpired::if_elapsed(frag(), 1_000, 1_000)
            .expect("expired")
            .to_string();
        assert!(
            refused.contains("re-authorize") && !refused.contains("re-read"),
            "a definite refusal's remedy is the other one — nothing landed, so there is \
             nothing to re-read: {refused}"
        );
    }

    /// The two effects are the same *class* of outcome (both `is_write_deadline_expired`,
    /// neither a backend fault) but **not** the same [`ErrorClass`]: a clean refusal is
    /// terminal, an unverified publication is indeterminate, because durable state may have
    /// changed. A caller that treated `Unknown` as terminal would record "nothing happened"
    /// over bytes that may be on the store — the mistake the third class exists to prevent.
    #[test]
    fn the_two_deadline_effects_classify_differently() {
        let refused = WriteDeadlineExpired::if_elapsed(frag(), 1_000, 2_000).expect("expired");
        let unknown = WriteDeadlineExpired::if_publication_unverified(frag(), 1_000, 2_000)
            .expect("unverified publication");

        assert_eq!(refused.effect, WriteEffect::NotApplied);
        assert!(!refused.effect.may_have_landed());
        assert_eq!(classify(&refused), ErrorClass::Terminal);

        assert_eq!(classify(&unknown), ErrorClass::Indeterminate);
        assert!(
            !classify(&unknown).is_terminal() && !classify(&unknown).is_transient(),
            "an unverified publication is neither: it must not be retried blind, and it is \
             not known to have left the store untouched"
        );
        assert_eq!(
            classify(&Wrapper(Box::new(unknown.clone()))),
            ErrorClass::Indeterminate,
            "the class must survive a backend wrapping it, like every other seam type"
        );
        assert!(
            is_write_deadline_expired(&unknown) && !is_integrity_fault(&unknown),
            "it is still the deadline class, not a fault: {unknown}"
        );
    }
}

/// The four shared `scan_page` page-bound decisions (#634), unit-tested here
/// because they are the *seam's* answers, not any backend's: five implementations
/// call them, so a slip here is a slip in all five at once. These are the
/// load-light production units the paginated read is built from — the same
/// discipline `metadata-tikv`'s `paging` module records for its cursor mechanics.
#[cfg(test)]
mod page_bound_tests {
    use super::*;

    fn page(keys: &[&[u8]]) -> Vec<(Vec<u8>, Bytes)> {
        keys.iter()
            .map(|k| (k.to_vec(), Bytes::from_static(b"v")))
            .collect()
    }

    fn zero_err(limit: usize, cap: usize) -> ZeroPageLimit {
        let err = page_limit(limit, cap, b"p:").expect_err("a zero page bound must be rejected");
        err.downcast_ref::<ZeroPageLimit>()
            .cloned()
            .unwrap_or_else(|| panic!("a zero page bound must raise the seam type, got: {err}"))
    }

    #[test]
    fn a_limit_under_the_cap_is_the_limit() {
        assert_eq!(page_limit(4, SCAN_CAP, b"p:").unwrap(), 4);
    }

    #[test]
    fn a_limit_above_the_cap_is_clamped_never_refused() {
        // The cap refuses to be raised (#262), but a caller asking for a bigger page
        // has asserted nothing and must not be failed for it.
        assert_eq!(page_limit(usize::MAX, 8, b"p:").unwrap(), 8);
        assert_eq!(page_limit(9, 8, b"p:").unwrap(), 8);
        // Exactly at the cap is the cap, not a breach.
        assert_eq!(page_limit(8, 8, b"p:").unwrap(), 8);
    }

    #[test]
    fn a_zero_limit_is_rejected_with_the_seam_error() {
        let err = zero_err(0, SCAN_CAP);
        assert_eq!(err.prefix, b"p:".to_vec());
        assert_eq!((err.limit, err.cap), (0, SCAN_CAP));
    }

    #[test]
    fn a_zero_cap_is_rejected_too_never_answered_with_an_unbounded_page() {
        // The regression this test exists for: `with_scan_cap(0)` is an accepted
        // configuration on every backend (the knobs clamp only from above), so a
        // resolver that merely `min`-ed would hand back 0 — and a page loop that
        // stops at `len >= limit` then never stops, returning the WHOLE prefix. The
        // bound must refuse, not invert.
        for limit in [1usize, 5, usize::MAX] {
            let err = zero_err(limit, 0);
            assert_eq!((err.limit, err.cap), (limit, 0));
        }
    }

    #[test]
    fn the_resolved_limit_is_never_zero() {
        // The property every backend's page loop relies on: past this call, `limit >= 1`.
        for (limit, cap) in [(1usize, 1usize), (3, 9), (usize::MAX, 1), (7, 7)] {
            assert!(page_limit(limit, cap, b"p:").unwrap() >= 1);
        }
    }

    #[test]
    fn a_cursor_inside_the_prefix_is_an_exclusive_lower_bound() {
        assert_eq!(
            page_start(b"p:", Some(b"p:5")),
            PageStart::After(&b"p:5"[..])
        );
        // The prefix itself is a legal cursor — "everything strictly after `p:`".
        assert_eq!(page_start(b"p:", Some(b"p:")), PageStart::After(&b"p:"[..]));
        // An empty prefix is the whole keyspace, so every cursor is inside it.
        assert_eq!(page_start(b"", Some(b"q")), PageStart::After(&b"q"[..]));
    }

    #[test]
    fn a_cursor_below_the_prefix_starts_the_page_at_the_prefix() {
        // The guard that stops a false "prefix exhausted": fed straight into a range
        // read, `after = b"a"` would open the range on an earlier namespace, hit a key
        // that does not carry the prefix, and stop — an empty terminal page for a
        // prefix that is not exhausted (the silent skip clause 3 forbids).
        assert_eq!(page_start(b"p:", Some(b"a")), PageStart::Prefix);
        // `b"p"` is *shorter* than the prefix and therefore sorts below it — the
        // boundary a length-blind comparison gets wrong.
        assert_eq!(page_start(b"p:", Some(b"p")), PageStart::Prefix);
        assert_eq!(page_start(b"p:", Some(b"")), PageStart::Prefix);
        // No cursor at all is the same arm: start at the prefix.
        assert_eq!(page_start(b"p:", None), PageStart::Prefix);
    }

    #[test]
    fn a_cursor_past_the_prefix_range_is_a_terminal_page_not_a_lower_bound() {
        // The iteration-2 defect: treated as a lower bound, each of these inverts
        // the bounded range `[cursor, upper_bound(prefix))` the distributed backends
        // read, whose substrates then disagree (tikv-client panics in its buffer's
        // `BTreeMap::range`; FDB's key selectors tolerate it) — where the contract
        // requires one answer, an empty terminal page.
        for cursor in [&b"p;"[..], &b"q"[..], &b"q:9"[..], &b"\xff"[..]] {
            assert_eq!(
                page_start(b"p:", Some(cursor)),
                PageStart::PastPrefix,
                "cursor {cursor:?} is at or past the exclusive end of the `p:` range"
            );
        }
    }

    #[test]
    fn past_the_prefix_is_exactly_at_or_beyond_the_prefixs_upper_bound() {
        // The equivalence the doc claims, checked rather than asserted in prose —
        // `page_start` decides it WITHOUT computing the upper bound, so the two must
        // be shown to agree on the boundary cases a physical-keyspace backend would
        // otherwise re-derive.
        fn upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
            let mut end = prefix.to_vec();
            while let Some(last) = end.last_mut() {
                if *last < 0xff {
                    *last += 1;
                    return Some(end);
                }
                end.pop();
            }
            None
        }

        let prefixes: [&[u8]; 6] = [b"p:", b"p", b"", b"\xff", b"p\xff", b"orphan:"];
        let cursors: [&[u8]; 12] = [
            b"",
            b"a",
            b"p",
            b"p:",
            b"p:0",
            b"p:\xff\xff",
            b"p;",
            b"p\xff",
            b"p\xff\x00",
            b"q",
            b"\xfe",
            b"\xff\xff",
        ];
        for prefix in prefixes {
            for cursor in cursors {
                let past = upper_bound(prefix).is_some_and(|end| cursor >= end.as_slice());
                assert_eq!(
                    page_start(prefix, Some(cursor)) == PageStart::PastPrefix,
                    past,
                    "prefix {prefix:?} / cursor {cursor:?}: `PastPrefix` must mean \
                     exactly `cursor >= upper_bound(prefix)`"
                );
            }
        }
    }

    #[test]
    fn a_full_page_carries_its_last_key_as_the_cursor() {
        assert_eq!(
            page_cursor(&page(&[b"p:1", b"p:2"]), 2),
            Some(b"p:2".to_vec())
        );
    }

    #[test]
    fn a_short_page_is_terminal_and_an_empty_one_never_carries_a_cursor() {
        // Short => the prefix is exhausted at this instant.
        assert_eq!(page_cursor(&page(&[b"p:1"]), 2), None);
        // Empty => terminal too. A non-terminal answer with no progress is what makes
        // a drain loop forever.
        assert_eq!(page_cursor(&[], 2), None);
        // Even for a limit of 1, where "empty" and "short" coincide.
        assert_eq!(page_cursor(&[], 1), None);
    }

    #[test]
    fn a_page_is_full_at_its_bound_not_one_pair_before_or_after() {
        // The boundary itself: `limit - 1` is short, `limit` is full, and anything
        // past it is still full (an over-filled page must not claim exhaustion).
        assert!(!page_is_full(1, 2));
        assert!(page_is_full(2, 2));
        assert!(page_is_full(3, 2));
        // A limit of 1 is the tightest bound a resolved page bound can have
        // (`page_limit` never returns 0), and an empty page is never full under it.
        assert!(!page_is_full(0, 1));
        assert!(page_is_full(1, 1));
    }

    #[test]
    fn the_rule_a_fill_loop_stops_on_is_the_rule_the_cursor_is_emitted_from() {
        // The coupling `page_is_full` exists to make unrepresentable (#634,
        // iteration-5 adversarial review): a backend whose loop stops on some OTHER
        // comparison returns a short page, which `page_cursor` labels terminal — the
        // caller stops walking a prefix that is not exhausted, and every key behind
        // the cursor is skipped forever. Asserted as an equivalence over the boundary
        // neighbourhood, so the two can never be changed apart.
        for limit in [1usize, 2, 3, 7] {
            for got in 0..=limit + 2 {
                let keys: Vec<Vec<u8>> =
                    (0..got).map(|i| format!("p:{i:04}").into_bytes()).collect();
                let items = page(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>());
                assert_eq!(
                    page_cursor(&items, limit).is_some(),
                    page_is_full(items.len(), limit),
                    "a page of {got} pair(s) at limit {limit}: a cursor is emitted \
                     exactly when the fill loop's own rule says the page is full"
                );
            }
        }
    }
}
