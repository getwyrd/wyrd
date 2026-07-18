//! The L4 metadata model, layered on the narrow [`MetadataStore`] primitive.
//!
//! The store is a conditional key/value commit; this module gives it
//! filesystem meaning (architecture §5): hierarchical **inode + dirent** keys so
//! that `create` writes an inode and its dirent atomically and `rename` is a
//! single dirent mutation, a per-inode **version** for compare-and-set at the
//! commit point, and the **pending-chunk ledger**. It is backend-agnostic —
//! generic over `&impl MetadataStore` — so the same model runs over redb today
//! and TiKV later (ADR-0008, ADR-0010).
//!
//! Records are encoded as JSON for M0 (debuggable; a compact codec is a later
//! optimization). The four-phase write protocol that drives these operations
//! lands with the client write path (M0.5).

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use wyrd_traits::{
    ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

/// An inode identifier.
pub type InodeId = u64;

/// The reserved global version-fence counter (ADR-0015). Initialized but not yet
/// enforced as a read fence in M0; per-inode versions carry the commit CAS.
pub const VERSION_KEY: &[u8] = b"meta:version";

/// Key for an inode record: `inode:<id>`.
pub fn inode_key(id: InodeId) -> Vec<u8> {
    format!("inode:{id}").into_bytes()
}

/// Key for a directory entry: `dirent:<parent_id>/<name>`.
pub fn dirent_key(parent: InodeId, name: &str) -> Vec<u8> {
    format!("dirent:{parent}/{name}").into_bytes()
}

/// Key for a pending-chunk ledger entry: `pending:<chunk_id>`.
pub fn pending_key(chunk: ChunkId) -> Vec<u8> {
    format!("pending:{chunk}").into_bytes()
}

/// Key prefix for the **orphan ledger** — the reader-safe grace record an orphaning
/// operation (a delete, or a completed reconstruction / rebalance) writes when it
/// strands a fragment, so the custodian **GC** loop reclaims the bytes only once the
/// grace window has elapsed (proposal 0005, "The four custodian loops" / GC,
/// `0005:288-295`; the reader-safe window `0005:291-294`). The value is the
/// logical-millis instant the fragment became orphaned.
pub const ORPHAN_PREFIX: &[u8] = b"orphan:";

/// Key for an orphan-ledger grace record: `orphan:<dserver>:<chunk>:<index>`.
///
/// Defined here beside [`pending_key`] because the orphan ledger is a **metadata-store
/// key protocol shared by both sides of a delete**: the delete path ([`unlink`], and the
/// gateway's `delete_object`) **writes** it, and the custodian GC
/// (`crates/custodian/src/gc.rs`) **reads** it. A single source of truth so a delete's
/// grace record and GC's scan can never key-format-drift — the crash-leak backstop is only
/// real if the record a delete writes is the exact key GC reclaims (issue #364).
pub fn orphan_key(dserver: DServerId, frag: FragmentId) -> Vec<u8> {
    format!("orphan:{dserver}:{}:{}", frag.chunk, frag.index).into_bytes()
}

/// Parse an [`orphan_key`] back into its `(dserver, fragment)`, or `None` if `key` is
/// not a well-formed orphan-ledger key. The inverse GC uses to read the ledger.
pub fn parse_orphan_key(key: &[u8]) -> Option<(DServerId, FragmentId)> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix("orphan:")?;
    let mut parts = rest.splitn(3, ':');
    let dserver = parts.next()?.parse().ok()?;
    let chunk = parts.next()?.parse().ok()?;
    let index = parts.next()?.parse().ok()?;
    Some((dserver, FragmentId { chunk, index }))
}

/// Whether an inode's content is fully committed or still being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InodeState {
    /// Content not yet committed (chunks may be in the pending ledger).
    Pending,
    /// The chunk map is committed and readable.
    Committed,
}

/// The durability scheme a chunk is stored under (ADR-0008 mixed-era data: the
/// scheme is recorded per chunk, so chunks written under different schemes read
/// correctly through one path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EcScheme {
    /// A single fragment per chunk at index 0 (the M0 `replication(1)`/`none`
    /// behaviour).
    None,
    /// Reed-Solomon erasure coding: `k` data + `m` parity fragments per chunk
    /// (`k`/`m` are `u8` to match the v1 header's `ec_k`/`ec_m`).
    ReedSolomon {
        /// Data-fragment count.
        k: u8,
        /// Parity-fragment count.
        m: u8,
    },
}

/// One chunk in an inode's chunk map: its id, durability scheme, **logical length**
/// (the reader truncates to this after reconstruction, stripping shard padding), and
/// the **placement record** — the stable D-server holding each fragment.
///
/// `placement[i]` is the [`DServerId`] of the D server holding the fragment at index
/// `i` (proposal 0005, "The placement record", M3.1): recorded at the write commit
/// point and consumed by the read path **in place of** M2's stateless `index % n`, so
/// a fragment a custodian has *moved* is still resolved. It is **additive** metadata
/// on a never-yet-deployed schema (`#[serde(default)]`), so an inode written before
/// the field decodes with an empty vector and the read falls back to the identity
/// placement (M0–M2 read through the same path).
///
/// (Carrying a `Vec` makes `ChunkRef` no longer `Copy`; the chunk map is cloned
/// where ownership is needed.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    /// The chunk's id (shared by all its fragments).
    pub id: ChunkId,
    /// How the chunk is fragmented.
    pub scheme: EcScheme,
    /// The chunk's logical (pre-coding) length in bytes.
    pub len: u64,
    /// The stable D-server id holding each fragment, by fragment index (length `n`).
    /// Empty on a pre-M3 record; the read path then resolves by fragment index.
    #[serde(default)]
    pub placement: Vec<DServerId>,
}

impl ChunkRef {
    /// The total number of fragments this chunk has, derived from its EC scheme:
    /// `EcScheme::None` → 1; `EcScheme::ReedSolomon { k, m }` → `k + m`. This is
    /// the authoritative fragment count shared by the read path, GC, scrub, and
    /// reconstruction — the single source of truth for "how many fragments does this
    /// chunk have?"
    pub fn fragment_count(&self) -> u16 {
        match self.scheme {
            EcScheme::None => 1,
            EcScheme::ReedSolomon { k, m } => u16::from(k) + u16::from(m),
        }
    }

    /// The D server holding fragment `index` of this chunk, applying the
    /// **identity-placement fallback** for pre-M3 / mixed-era records whose
    /// `placement` vector is empty or shorter than `n` (decoded via
    /// `#[serde(default)]`): if `placement[index]` is absent, the fragment resolves
    /// to D-server `index`. This is the **single authoritative placement-resolution
    /// definition** for the read path (`read.rs:fragment_dserver`), GC
    /// (`gc.rs:referenced_fragments`), scrub, reconstruction
    /// (`reconstruction.rs:assess`), and rebalance (`rebalance.rs:plan_evacuations`),
    /// so placement semantics cannot drift across callers.
    pub fn placed_dserver(&self, index: u16) -> DServerId {
        self.placement
            .get(index as usize)
            .copied()
            .unwrap_or(u64::from(index))
    }

    /// Every fragment of this chunk, resolved to its holding D server: the full
    /// `0..fragment_count()` index space, each index resolved through
    /// [`Self::placed_dserver`] (ADR-0040 decision 1, the normative expansion rule).
    /// This is *the* "walk every fragment to its holding D-server" call (ADR-0040
    /// decision 2) — the single definition every read-expansion consumer draws from
    /// instead of open-coding `(0..fragment_count()).map(|i| placed_dserver(i))`
    /// itself: GC's `referenced_fragments` (`gc.rs`), reconstruction's `assess`
    /// (`reconstruction.rs`), and rebalance's `plan_evacuations` (`rebalance.rs`).
    ///
    /// Deliberately **liberal**, like `placed_dserver`: it applies the identity
    /// fallback unconditionally and does not validate `placement`'s length, so it is
    /// infallible and safe for the read path. A malformed (non-empty, wrong-length)
    /// vector is a maintenance-loop concern (ADR-0040 decisions 3–4) — classifying and
    /// rejecting one *before* expansion is a separate, fallible companion
    /// (`checked_fragments()` / `placement_is_valid()`, #348), not a property of this
    /// helper.
    pub fn fragments(&self) -> impl Iterator<Item = (u16, DServerId)> + '_ {
        (0..self.fragment_count()).map(move |i| (i, self.placed_dserver(i)))
    }

    /// Whether the committed `placement` vector is **well-formed** — the single
    /// classifier the maintenance loops share (ADR-0040 decision 3, the "liberal read,
    /// strict maintenance" boundary). A committed `placement` is valid **iff** it is
    /// **empty** (pre-M3 / mixed-era → identity fallback) **or** its length equals
    /// [`Self::fragment_count`] (an explicit full-length record). Any other non-empty
    /// length is **malformed**: no writer emits it (the write path always commits a
    /// full-length vector; `#[serde(default)]` only ever yields empty), so in practice
    /// it can only mean truncation or corruption.
    ///
    /// This is the strict counterpart to the deliberately liberal [`Self::fragments`]
    /// expansion (#348): the read path stays liberal via `fragments()`, while a
    /// maintenance loop consults this gate (or [`Self::checked_fragments`]) *before*
    /// expanding, so a malformed vector is never silently identity-filled.
    pub fn placement_is_valid(&self) -> bool {
        self.placement.is_empty() || self.placement.len() == self.fragment_count() as usize
    }

    /// The **strict** companion to [`Self::fragments`]: the same full-index-space
    /// expansion, but only **after** classifying the committed `placement` (ADR-0040
    /// decision 4). A valid vector (empty or full-length) expands exactly as
    /// `fragments()` does; a **malformed** one (non-empty, `len != fragment_count()`) is
    /// rejected with [`MalformedPlacement`] *before* any expansion, so no identity entry
    /// is ever fabricated for its missing tail.
    ///
    /// Every maintenance loop resolves committed placement through this gate — GC/scrub
    /// treat a malformed chunk as fully referenced and audit it; reconstruction/rebalance
    /// skip it and flag NEEDS-HUMAN — while the read path keeps using the infallible
    /// `fragments()` (availability first).
    pub fn checked_fragments(
        &self,
    ) -> std::result::Result<impl Iterator<Item = (u16, DServerId)> + '_, MalformedPlacement> {
        if self.placement_is_valid() {
            Ok(self.fragments())
        } else {
            Err(MalformedPlacement {
                expected: self.fragment_count(),
                actual: self.placement.len(),
            })
        }
    }
}

/// A committed `placement` vector classified as **malformed** by
/// [`ChunkRef::checked_fragments`] (ADR-0040 decision 3): non-empty but of a length
/// other than the chunk's [`ChunkRef::fragment_count`]. It carries the mismatch so a
/// maintenance loop can surface it as an operator signal (audit event / NEEDS-HUMAN).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedPlacement {
    /// The fragment count the chunk's [`EcScheme`] requires (`fragment_count()`).
    pub expected: u16,
    /// The actual length of the committed `placement` vector.
    pub actual: usize,
}

/// Object metadata surfaced on the wire beyond byte size (ADR-0047): the content
/// `etag`, the client's declared `content_type`, and the content-publication time
/// (`modified`). Set together at **content publication** (create / overwrite) and
/// **preserved** across reconstruction/backfill commits, so a repair never moves
/// `Last-Modified` or drops the content type. Every field is optional so a record
/// written before this model — or by a path that has no value to record — degrades on
/// the wire to the pre-metadata behaviour (no ETag, `application/octet-stream`) rather
/// than to an error. `x-amz-meta-*` user metadata is deliberately not modelled here; the
/// flat shape leaves room to add it later.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The content digest as an opaque change-token: the lowercase-hex SHA-256 of the
    /// object bytes (ADR-0047; **not** MD5). Rendered quoted on the wire as S3's `ETag`.
    pub etag: Option<String>,
    /// The `Content-Type` the writing client declared, round-tripped verbatim.
    pub content_type: Option<String>,
    /// Content-publication time in epoch milliseconds; rendered RFC-7231 IMF-fixdate
    /// as `Last-Modified` on the wire.
    pub modified: Option<u64>,
}

/// An inode: attributes, the ordered chunk map, state, and version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    /// Logical content length in bytes.
    pub size: u64,
    /// The ordered chunks making up the content.
    pub chunk_map: Vec<ChunkRef>,
    /// Commit state.
    pub state: InodeState,
    /// Monotonic per-inode version; the commit point bumps it under CAS.
    pub version: u64,
    /// The content digest (opaque change-token), quoted as S3's `ETag` on the wire.
    /// `Option` + `#[serde(default)]` for stored-record compatibility (ADR-0047): a
    /// record written before this field decodes with `None`. Set only at content
    /// publication; preserved across reconstruction/backfill.
    ///
    /// `skip_serializing_if` is **load-bearing**, not cosmetic: every CAS commit in
    /// this module (`require(key, encode(prior))`) compares the RE-ENCODED prior
    /// record byte-for-byte against the bytes still in the store. A legacy record
    /// decodes these fields to `None`; serializing that as `"etag":null` could never
    /// equal the stored legacy JSON, so every overwrite and every
    /// backfill/reconstruction/rebalance of a pre-ADR-0047 object would return
    /// `Conflict` forever. Skipping `None` makes decode→encode the identity on
    /// legacy bytes, so the CAS sees exactly what the store holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// The client's declared `Content-Type`, round-tripped verbatim. `Option` +
    /// `#[serde(default)]` for stored-record compatibility; falls back to
    /// `application/octet-stream` on the wire when absent. `skip_serializing_if`:
    /// see `etag` — required for the CAS round trip on legacy records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Content-publication time (epoch millis), rendered `Last-Modified` on the wire.
    /// `Option` + `#[serde(default)]` for stored-record compatibility.
    /// `skip_serializing_if`: see `etag` — required for the CAS round trip on legacy
    /// records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified: Option<u64>,
}

impl InodeRecord {
    /// A freshly-created, empty inode at version 1, awaiting content.
    pub fn new_empty() -> Self {
        Self {
            size: 0,
            chunk_map: Vec::new(),
            state: InodeState::Pending,
            version: 1,
            etag: None,
            content_type: None,
            modified: None,
        }
    }

    /// The object metadata carried on this record (ADR-0047), collected into an
    /// [`ObjectMeta`] for the wire layer.
    pub fn object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            etag: self.etag.clone(),
            content_type: self.content_type.clone(),
            modified: self.modified,
        }
    }
}

impl Default for InodeRecord {
    /// The empty inode ([`InodeRecord::new_empty`]) — so struct-update construction
    /// (`InodeRecord { size, chunk_map, state, version, ..Default::default() }`) fills
    /// the optional metadata fields with `None` at the many call sites that do not set
    /// object metadata.
    fn default() -> Self {
        Self::new_empty()
    }
}

/// A directory entry: the inode a name binds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentRecord {
    /// The inode this name resolves to.
    pub inode: InodeId,
}

/// A pending-chunk ledger entry: a lease on a provisionally-written chunk id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    /// When the lease expires (logical milliseconds); a custodian sweep may
    /// reclaim the chunk after this.
    pub lease_expiry_millis: u64,
}

/// Encode a record to its stored bytes. Serialization of these plain structs is
/// infallible.
pub fn encode<T: Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).expect("metadata record serialization is infallible"))
}

/// Decode a record from stored bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Atomically create an inode and the dirent that names it. Fails with
/// [`CommitOutcome::Conflict`] if the name (or the inode id) already exists, so a
/// just-created file is never duplicated or clobbered.
pub async fn create(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    id: InodeId,
    record: &InodeRecord,
) -> Result<CommitOutcome> {
    let batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), encode(record))
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
    store.commit(batch).await
}

/// Like [`create`], but the inode + dirent are published only if every chunk in
/// `pending_chunks` still holds a **live, unexpired** `pending:<id>` lease at `now_millis`,
/// enforced **atomically** with the create (issue #490). This is phase 3 of a **streaming**
/// write: an early chunk's fragments are protected from the custodian GC only by their pending
/// lease until the commit publishes the inode, so a commit that outran the lease (a stall past
/// the TTL after the last chunk, or between `stream_write_data` returning and the caller
/// driving this commit) must fail closed rather than publish an object over bytes the GC may
/// reclaim.
///
/// The per-chunk `require(pending_key, read-back-value)` preconditions ride in the **same**
/// [`WriteBatch`] as the create ([`live_lease_guards`]), so a sweep that reclaims a lease
/// between the read-back and the commit yields [`CommitOutcome::Conflict`], never a publish;
/// an already-absent or already-lapsed lease refuses up front with the same `Conflict`.
/// [`create`] is this with no leases to guard.
pub async fn create_leased(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    id: InodeId,
    record: &InodeRecord,
    pending_chunks: &[ChunkId],
    now_millis: u64,
) -> Result<CommitOutcome> {
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let mut batch = WriteBatch::new()
        .require_absent(inode_key(id))
        .require_absent(dirent_key(parent, name))
        .put(inode_key(id), encode(record))
        .put(
            dirent_key(parent, name),
            encode(&DirentRecord { inode: id }),
        );
    for (key, value) in guards {
        batch = batch.require(key, value);
    }
    store.commit(batch).await
}

/// Rename: move a name binding in a single dirent mutation. The inode is
/// untouched. Fails with [`CommitOutcome::Conflict`] if the source moved
/// concurrently or the target name is taken; returns `Conflict` if the source
/// does not exist.
pub async fn rename(
    store: &impl MetadataStore,
    old_parent: InodeId,
    old_name: &str,
    new_parent: InodeId,
    new_name: &str,
) -> Result<CommitOutcome> {
    let old_key = dirent_key(old_parent, old_name);
    let Some(current) = store.get(&old_key).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let batch = WriteBatch::new()
        .require(old_key.clone(), current.clone()) // source unchanged since read
        .require_absent(dirent_key(new_parent, new_name)) // target free
        .delete(old_key)
        .put(dirent_key(new_parent, new_name), current);
    store.commit(batch).await
}

/// The result of an [`unlink`] attempt on a bound name: the commit `outcome` and, when
/// the dirent resolved to one, the `inode` record that was removed — so the caller can
/// reclaim exactly that object's chunk fragments on a winning commit (issue #364).
#[derive(Debug, Clone)]
pub struct Unlinked {
    /// Whether the removal committed or lost a compare-and-set to a racing writer.
    pub outcome: CommitOutcome,
    /// The inode the removed dirent pointed at (`None` only for a dangling dirent).
    pub inode: Option<InodeRecord>,
}

/// Atomically remove a name binding and the inode it resolves to — the metadata
/// half of an S3 DELETE (issue #364). Returns `Ok(None)` if the name is already
/// unbound (an idempotent no-op the caller reports as success), else an [`Unlinked`]
/// carrying the commit outcome and the removed inode.
///
/// Compare-and-set on **both** the dirent and the inode so a delete racing an
/// overwrite (which replaces the inode) or a concurrent delete loses with
/// [`CommitOutcome::Conflict`] rather than removing a record a racing writer just
/// changed — the caller retries or treats an already-absent key as success so the
/// *observable* DELETE is idempotent (S3's 204).
///
/// This removes the **metadata** records **and**, in the *same atomic commit*, writes an
/// **orphan grace record** ([`orphan_key`], value `orphaned_at_millis`) for every fragment
/// the removed object placed — keyed by the **D-server the chunk map actually placed it on**
/// ([`ChunkRef::fragments`]), the placement-aware address GC reclaims from. The fragment bytes
/// are **not** reclaimed eagerly on the delete path: they are left under the orphan ledger for
/// the custodian **GC** (`crates/custodian/src/gc.rs`) to reclaim once the reader-safe grace
/// window elapses (proposal 0005, `0005:288-295`), so a concurrent reader still streaming the
/// prior object from those fragments is never torn mid-read (a GET during a DELETE completes
/// intact). Because the records are durable the instant the object becomes unreferenced, a
/// crash never strands the bytes forever either. This is a *real* backstop, not the
/// pending-ledger sweep: the **pending sweep**
/// ([`sweep_pending`] / [`sweep_expired_leases`]) scans `pending:` lease keys only, and a
/// committed object's fragments carry no pending entry, so without the orphan record GC would
/// see an unreferenced-but-undeadlined fragment and conservatively keep it forever
/// (`gc.rs:reconcile`) — the crash-leak this record closes (issue #364).
///
/// `orphaned_at_millis` is the caller's logical clock; GC honours the grace window relative
/// to it. On a lost CAS ([`CommitOutcome::Conflict`]) the whole batch rolls back, so no
/// orphan record is written for a delete that did not remove the object.
pub async fn unlink(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    orphaned_at_millis: u64,
) -> Result<Option<Unlinked>> {
    let dirent_key = dirent_key(parent, name);
    let Some(dirent_bytes) = store.get(&dirent_key).await? else {
        return Ok(None);
    };
    let dirent: DirentRecord = decode(&dirent_bytes)?;
    let inode_key = inode_key(dirent.inode);
    let inode_bytes = store.get(&inode_key).await?;
    let inode = inode_bytes
        .as_ref()
        .map(|bytes| decode::<InodeRecord>(bytes))
        .transpose()?;

    let mut batch = WriteBatch::new()
        .require(dirent_key.clone(), dirent_bytes)
        .delete(dirent_key)
        .delete(inode_key.clone());
    batch = match inode_bytes {
        Some(bytes) => batch.require(inode_key, bytes),
        None => batch.require_absent(inode_key),
    };
    // Grace-record every fragment the removed object placed, in the SAME atomic commit
    // that unbinds it (placement-aware: keyed by the D-server the chunk map placed the
    // fragment on, not `index`), so GC can reclaim it after a crash before the eager
    // reclaim runs.
    if let Some(inode) = &inode {
        for chunk in &inode.chunk_map {
            for (index, dserver) in chunk.fragments() {
                let frag = FragmentId {
                    chunk: chunk.id,
                    index,
                };
                batch = batch.put(
                    orphan_key(dserver, frag),
                    orphaned_at_millis.to_string().into_bytes(),
                );
            }
        }
    }
    let outcome = store.commit(batch).await?;
    Ok(Some(Unlinked { outcome, inode }))
}

/// Commit a chunk map and size onto an inode at the commit point, bumping its
/// version **conditional on the prior record** (full-value compare-and-set). A
/// writer holding a stale `prior` loses with [`CommitOutcome::Conflict`];
/// exactly one concurrent writer wins.
pub async fn commit_chunk_map(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
) -> Result<CommitOutcome> {
    let next = InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version: prior.version + 1,
        // Reconstruction/backfill re-commits the SAME content, so it PRESERVES the
        // publication metadata (ADR-0047): a repair must not move `Last-Modified` or
        // drop the content type. Only the superseding commits below set new metadata.
        ..prior.clone()
    };
    let key = inode_key(id);
    let batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    store.commit(batch).await
}

/// Commit a new chunk map onto an inode (an object-content **overwrite**), CAS-conditional
/// on `prior`, **and** orphan every fragment the *prior* chunk map placed — in the *same
/// atomic batch*. This is the overwrite counterpart of the orphan grace records [`unlink`]
/// writes for a DELETE (issue #364, PUT-overwrite reclaim): the superseded fragments become
/// unreferenced the instant the new map wins, so a crash *after* the CAS never strands the
/// prior object's bytes — the custodian **GC** (`crates/custodian/src/gc.rs`) reclaims each
/// recorded orphan once the reader-safe grace window elapses (proposal 0005, `0005:288-295`).
///
/// Reclaim is left to GC (not done eagerly) precisely so a concurrent reader still holding the
/// prior chunk map reads its fragments intact within the grace window — the same reader-safe
/// discipline that keeps a GET during a DELETE from being truncated. The prior fragments are
/// orphaned by their **placed** D-server ([`ChunkRef::fragments`]), the address GC reclaims
/// from. [`commit_chunk_map`] (used by reconstruction/backfill, which *keep* the fragments and
/// only re-place them) is deliberately left non-orphaning — only a content overwrite
/// supersedes the bytes.
///
/// A `Conflict` (a stale writer lost the CAS) rolls the whole batch back, so no orphan record
/// is ever written for an overwrite that did not win.
pub async fn commit_chunk_map_superseding(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
    orphaned_at_millis: u64,
    meta: &ObjectMeta,
) -> Result<CommitOutcome> {
    let next = InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version: prior.version + 1,
        // A content **overwrite** is a fresh publication (ADR-0047), so it stamps the new
        // object metadata (digest / content type / publication time) rather than carrying
        // the prior version's forward.
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone(),
        modified: meta.modified,
    };
    let key = inode_key(id);
    let mut batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    for chunk in &prior.chunk_map {
        for (index, dserver) in chunk.fragments() {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            batch = batch.put(
                orphan_key(dserver, frag),
                orphaned_at_millis.to_string().into_bytes(),
            );
        }
    }
    store.commit(batch).await
}

/// Like [`commit_chunk_map_superseding`], but the overwrite CAS lands only if every chunk in
/// `pending_chunks` still holds a **live, unexpired** `pending:<id>` lease at `now_millis`,
/// enforced **atomically** with the inode CAS and the prior fragments' orphaning (issue #490).
/// This is phase 3 of a **streaming overwrite**: the new version's chunks are protected from
/// the custodian GC only by their pending leases until this commit publishes them, so a commit
/// that outran a lease (a stall past the TTL after the last chunk, or between
/// `stream_write_data` returning and the caller driving this commit) must fail closed rather
/// than publish an object over bytes the GC may reclaim.
///
/// The per-chunk `require(pending_key, read-back-value)` preconditions ride in the **same**
/// [`WriteBatch`] as the CAS and every `orphan:` record ([`live_lease_guards`]), so a sweep
/// that reclaims a lease between the read-back and the commit yields [`CommitOutcome::Conflict`]
/// — never a publish, and never a stranded orphan record — and an already-absent or
/// already-lapsed lease refuses up front with the same `Conflict`.
/// [`commit_chunk_map_superseding`] is this with no leases to guard.
#[allow(clippy::too_many_arguments)]
pub async fn commit_chunk_map_superseding_leased(
    store: &impl MetadataStore,
    id: InodeId,
    prior: &InodeRecord,
    chunk_map: Vec<ChunkRef>,
    size: u64,
    orphaned_at_millis: u64,
    pending_chunks: &[ChunkId],
    now_millis: u64,
    meta: &ObjectMeta,
) -> Result<CommitOutcome> {
    let Some(guards) = live_lease_guards(store, pending_chunks, now_millis).await? else {
        return Ok(CommitOutcome::Conflict);
    };
    let next = InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version: prior.version + 1,
        // A content **overwrite** is a fresh publication (ADR-0047): stamp the new object
        // metadata rather than carrying the prior version's forward.
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone(),
        modified: meta.modified,
    };
    let key = inode_key(id);
    let mut batch = WriteBatch::new()
        .require(key.clone(), encode(prior))
        .put(key, encode(&next));
    for chunk in &prior.chunk_map {
        for (index, dserver) in chunk.fragments() {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            batch = batch.put(
                orphan_key(dserver, frag),
                orphaned_at_millis.to_string().into_bytes(),
            );
        }
    }
    for (pk, pv) in guards {
        batch = batch.require(pk, pv);
    }
    store.commit(batch).await
}

/// Write a pending-chunk ledger entry (the Intent phase of the write protocol).
pub async fn put_pending(
    store: &impl MetadataStore,
    chunk: ChunkId,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    store
        .commit(WriteBatch::new().put(pending_key(chunk), encode(entry)))
        .await
}

/// Clear pending-chunk ledger entries (the Release phase / a custodian sweep).
pub async fn sweep_pending(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
) -> Result<CommitOutcome> {
    let mut batch = WriteBatch::new();
    for &chunk in chunks {
        batch = batch.delete(pending_key(chunk));
    }
    store.commit(batch).await
}

/// **Renew** the pending-ledger lease on every chunk in `chunks` to `entry` in one atomic,
/// **conditional** batch. The streaming write path calls this as an upload progresses so an
/// already-written but not-yet-committed chunk's lease never lapses before the final commit:
/// until the commit publishes the inode, an in-flight chunk's fragments are protected from
/// the custodian **GC** only by its unexpired pending lease (they are in no committed chunk
/// map, so GC's reference set does not cover them). A single start-of-upload deadline let a
/// slow upload run past it and the GC would reclaim the early chunks as expired garbage
/// before the commit — publishing an object with missing fragments (issue #364 durability
/// finding 2, `write::stream_write_data`).
///
/// Renewal may only **extend** a lease that still exists and has not lapsed — it must never
/// re-create authority the sweep already revoked (issue #490). A *blind* overwrite of each
/// `pending:<id>` entry resurrected a chunk whose lease had already lapsed and been swept
/// mid-upload, and the upload then committed an inode pointing at bytes the GC was free to
/// reclaim. So each entry is read back and the renewal **refuses** — returning
/// [`CommitOutcome::Conflict`], nothing written — when a chunk's entry is either:
///  * **absent** — a sweep reclaimed it ([`sweep_expired_leases`]), or
///  * present but its recorded `lease_expiry_millis` is **`<= now_millis`** — lapsed but not
///    yet reaped (renewing it would resurrect revoked authority, `write.rs:417-418`). The
///    `<=` boundary is the sweep's own reap condition (`write.rs:572`): both lease consumers
///    agree a lease is dead at `expiry <= now`, so a renewal at exactly the deadline (`now ==
///    expiry`) is renewing a lease the reaper is already entitled to take.
///
/// The check and the write are ONE batch: for every chunk it pairs
/// `require(pending_key, current-value)` with `put(pending_key, entry)`, so a sweep that
/// deletes an entry **between** the read-back and the commit turns the precondition false and
/// the whole batch is `Conflict` — a read-verify-then-blind-put in two commits could not
/// close that interleave. An empty slice is a no-op.
pub async fn renew_pending(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
    now_millis: u64,
    entry: &PendingEntry,
) -> Result<CommitOutcome> {
    if chunks.is_empty() {
        return Ok(CommitOutcome::Committed);
    }
    let mut batch = WriteBatch::new();
    for &chunk in chunks {
        let key = pending_key(chunk);
        let current = match store.get(&key).await? {
            // Swept out from under the upload — refuse rather than resurrect.
            None => return Ok(CommitOutcome::Conflict),
            Some(bytes) => bytes,
        };
        let existing: PendingEntry = decode(&current)?;
        if existing.lease_expiry_millis <= now_millis {
            // Lapsed but not yet reaped — renewing it would revive revoked authority.
            return Ok(CommitOutcome::Conflict);
        }
        batch = batch.require(key.clone(), current).put(key, encode(entry));
    }
    store.commit(batch).await
}

/// Read back the `pending:<id>` ledger entry of every chunk in `chunks` and, when all are
/// still **live**, return the compare-and-set preconditions that pin each key to the exact
/// bytes just read. This is the lease-conditional guard the phase-3 committers thread into the
/// **same** [`WriteBatch`] as the inode create/CAS (issue #490): a racing custodian sweep that
/// deletes an entry **between** this read-back and the commit turns its precondition false, so
/// the whole batch is [`CommitOutcome::Conflict`] — the object is never published over
/// fragments the GC is free to reclaim.
///
/// Returns `Ok(None)` — the commit must **refuse, fail-closed** — as soon as any chunk's entry
/// is either **absent** (already reaped by [`sweep_expired_leases`]) or present but **lapsed**
/// (`lease_expiry_millis <= now_millis`, the sweep's own reap boundary, `write.rs:572`): a
/// lapsed lease is dead authority and GC reclaims its bytes keyed on expiry even while the
/// entry is still present (`crates/custodian/src/gc.rs:142-144`). An empty slice yields
/// `Ok(Some(vec![]))` — no leases to guard, so [`create`] / [`commit_chunk_map_superseding`]
/// (their unconditional counterparts) delegate through here unchanged.
async fn live_lease_guards(
    store: &impl MetadataStore,
    chunks: &[ChunkId],
    now_millis: u64,
) -> Result<Option<Vec<(Vec<u8>, Bytes)>>> {
    let mut guards = Vec::with_capacity(chunks.len());
    for &chunk in chunks {
        let key = pending_key(chunk);
        let Some(current) = store.get(&key).await? else {
            return Ok(None);
        };
        let entry: PendingEntry = decode(&current)?;
        if entry.lease_expiry_millis <= now_millis {
            return Ok(None);
        }
        guards.push((key, current));
    }
    Ok(Some(guards))
}

/// Parse the inode id out of an `inode:<id>` key (the inverse of [`inode_key`]).
fn parse_inode_key(key: &[u8]) -> Option<InodeId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// Parse the chunk id out of a `pending:<id>` key (the inverse of [`pending_key`]).
fn parse_pending_chunk_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}

/// The high-water marks of the **in-process id allocators** over the persisted metadata:
/// the largest inode id any record uses, and the largest **in-process-scheme** chunk id
/// (those below `2^64`) any committed chunk map, pending-ledger entry, **or orphan-ledger
/// grace record** references.
///
/// A single-process gateway (`wyrd_server::Gateway`) allocates inode and chunk ids from
/// in-memory counters; on a restart over a **non-empty** store those counters must resume
/// *above* everything already on disk. Otherwise a new-key PUT reuses a committed inode id
/// — a bogus "concurrent writer won" conflict, because [`create`] is `require_absent` on the
/// inode key — and an overwrite mints a chunk id that already backs a committed object,
/// clobbering its fragments on the shared chunk store (issue #364 durability finding 1).
/// This scan supplies those marks so `Gateway::recover` can bump the counters. An empty
/// store yields `(0, 0)` and allocation starts at 1, unchanged.
///
/// The `orphan:` scan closes a third re-mint hazard: after `PUT → DELETE → restart` the
/// deleted object's inode key is gone ([`unlink`] removes it) and its chunk was already
/// committed (so no `pending:` entry survives), yet its fragments are still on disk under a
/// live [`orphan_key`] grace record until the custodian GC's reader-safe window elapses
/// (`crates/custodian/src/gc.rs:134-141`). Were that chunk id not counted here, `recover`
/// would re-mint it for the next object — and GC's reference gate keys on `(dserver, chunk,
/// index)` (`ReferenceSet::protects`, `gc.rs:200`), so the stale orphan record then either
/// leaks the old bytes permanently (the id now looks referenced) or reclaims a fragment the
/// re-minting object has just written but not yet committed (data loss). Projecting the
/// orphan record's chunk id into `max_chunk` makes re-mint step past every id whose orphan
/// record / on-disk fragments are still live (issue #364 durability finding, iter-8 review).
///
/// Chunk ids are projected to the `< 2^64` in-process space on purpose: the cluster client
/// mode derives chunk ids as `(inode << 64) | seq` (`server::cli::chunk_id_minter`) and
/// resumes *its* allocator from the durable `meta:next_inode` counter, so those disjoint,
/// above-`2^64` ids are not the in-process counter's to recover from (and never collide with
/// it — the in-process counter only ever mints ids below `2^64`).
pub async fn high_water_marks(store: &impl MetadataStore) -> Result<(InodeId, ChunkId)> {
    const IN_PROCESS_CHUNK_CEILING: ChunkId = 1 << 64;
    let mut max_inode: InodeId = 0;
    let mut max_chunk: ChunkId = 0;
    for (key, value) in store.scan(b"inode:").await? {
        if let Some(id) = parse_inode_key(&key) {
            max_inode = max_inode.max(id);
        }
        let record: InodeRecord = decode(&value)?;
        for chunk in &record.chunk_map {
            if chunk.id < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(chunk.id);
            }
        }
    }
    for (key, _value) in store.scan(b"pending:").await? {
        if let Some(chunk) = parse_pending_chunk_key(&key) {
            if chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(chunk);
            }
        }
    }
    // Orphan grace records (`orphan:<dserver>:<chunk>:<index>`): a deleted object's
    // fragments still live on disk under this ledger until GC's grace window elapses, so
    // their chunk id is not yet free to re-mint even though no `inode:`/`pending:` key
    // references it any more (see the doc comment above; issue #364).
    for (key, _value) in store.scan(ORPHAN_PREFIX).await? {
        if let Some((_dserver, frag)) = parse_orphan_key(&key) {
            if frag.chunk < IN_PROCESS_CHUNK_CEILING {
                max_chunk = max_chunk.max(frag.chunk);
            }
        }
    }
    Ok((max_inode, max_chunk))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs_chunk(placement: Vec<DServerId>) -> ChunkRef {
        // ReedSolomon { k: 4, m: 2 } → fragment_count() == 6.
        ChunkRef {
            id: 0xC0,
            scheme: EcScheme::ReedSolomon { k: 4, m: 2 },
            len: 5,
            placement,
        }
    }

    #[test]
    fn empty_placement_is_valid_pre_m3_identity() {
        // A pre-M3 / mixed-era record decodes with an empty vector (`#[serde(default)]`):
        // valid, resolved by the identity fallback (ADR-0040 decision 3).
        let chunk = rs_chunk(vec![]);
        assert!(chunk.placement_is_valid());
        assert!(chunk.checked_fragments().is_ok());
    }

    #[test]
    fn full_length_placement_is_valid() {
        // len == fragment_count() (6): an explicit full-length record is valid.
        let chunk = rs_chunk(vec![10, 11, 12, 13, 14, 15]);
        assert!(chunk.placement_is_valid());
        let resolved: Vec<_> = chunk.checked_fragments().unwrap().collect();
        assert_eq!(
            resolved,
            vec![(0, 10), (1, 11), (2, 12), (3, 13), (4, 14), (5, 15)]
        );
    }

    #[test]
    fn non_empty_wrong_length_placement_is_malformed() {
        // fragment_count() == 6 but a length-2 vector: malformed (truncation/corruption),
        // rejected BEFORE expansion — never identity-filled (ADR-0040 decisions 3–4).
        let chunk = rs_chunk(vec![10, 11]);
        assert!(!chunk.placement_is_valid());
        assert_eq!(
            chunk.checked_fragments().err(),
            Some(MalformedPlacement {
                expected: 6,
                actual: 2,
            })
        );
    }

    #[test]
    fn read_path_fragments_stays_liberal_for_malformed_placement() {
        // The read path is UNCHANGED (ADR-0040 decision 4, availability first): the
        // liberal `fragments()` still resolves the same malformed-placement chunk via the
        // per-index identity fallback — indices 0..2 from the vector, 2..6 identity-filled.
        let chunk = rs_chunk(vec![10, 11]);
        let resolved: Vec<_> = chunk.fragments().collect();
        assert_eq!(
            resolved,
            vec![(0, 10), (1, 11), (2, 2), (3, 3), (4, 4), (5, 5)]
        );
    }
}
