//! The Wyrd S3 gateway (L1, access layer): composes the concrete backends and
//! maps minimal S3 PUT/GET onto the client write and read paths.
//!
//! This is the one place that knows concretes (ADR-0010) — `server`'s `main` and
//! the tests pick the redb metadata store, the filesystem chunk store, and the
//! in-memory coordination, and a networked profile swaps them here without
//! touching any caller. PUT/GET/DELETE are exposed both as in-process methods and,
//! from the M4 first-deployment gate, over an S3-compatible HTTP wire surface with
//! mandatory SigV4 auth and **streaming** request/response bodies (the [`s3`]
//! module, issue #364, m4-first-deployment-blueprint). Full S3 semantics beyond
//! object PUT/GET/DELETE (multipart / ListObjectsV2 / ACLs beyond signing) remain
//! deferred.

#![forbid(unsafe_code)]

pub mod cli;
pub mod consistency_observable;
pub mod consistency_workload;
pub mod custodian;
pub mod dserver;
pub mod logging;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;
use wyrd_core::metadata::{self, EcScheme, InodeId};
use wyrd_core::{read, write};
use wyrd_traits::{
    ChunkId, CommitOutcome, Coordination, MetadataStore, PlacementChunkStore, Result,
};

// The client-facing gateway seam this crate composes concretes behind (ADR-0010). The S3
// wire surface (`wyrd-gateway-s3`) is generic over `ObjectGateway`; `Gateway` implements it.
pub use wyrd_gateway_core::GatewayError;
use wyrd_gateway_core::{ContentHash, ObjectGateway, ObjectRead};

/// The root inode every object key is bound under — a flat namespace at M0.
const ROOT: InodeId = 0;
/// Default durability: Reed-Solomon RS(6,3) — k=6 data, m=3 parity (proposal 0003).
pub const DEFAULT_DURABILITY: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };
/// Default chunk size (1 MiB). Tests override it to exercise multi-chunk objects.
const DEFAULT_CHUNK_SIZE: usize = 1 << 20;
/// Default pending-ledger lease lifetime.
const DEFAULT_LEASE_TTL_MILLIS: u64 = 30_000;
/// Coordination group under which gateway nodes register for discovery.
const NODES_GROUP: &str = "nodes";
/// Lease lifetime for a node registration. Generous because the single-process
/// gateway does not run a renewal loop at M0; a networked node would renew.
const NODE_LEASE_TTL: Duration = Duration::from_secs(3600);

/// An S3 gateway over a metadata store, a chunk store, and a coordination
/// backend. Generic over the three seams so the concretes are chosen by the
/// composing binary (ADR-0010).
pub struct Gateway<M, C, Co> {
    meta: M,
    chunks: C,
    coord: Co,
    chunk_size: usize,
    durability: EcScheme,
    lease_ttl_millis: u64,
    // Id allocation is cluster-safe: several gateway processes may run active-active over one
    // shared fleet (M4, "one shared front door, N gateways", #465), so no id may be minted
    // from per-process state that two processes seeded identically. Inodes come from the
    // SHARED store's `meta:next_inode` CAS allocator (`cli::alloc_inode`, the CLI cluster
    // path's scheme), so two gateways never mint the same inode. Chunk ids are
    // **coordination-free** (ADR-0019): a per-gateway random `chunk_epoch` (the high 64 bits)
    // makes two processes draw disjoint id ranges without any shared counter, and a per-gateway
    // monotonic `next_chunk_seq` (the low 64 bits) never repeats within a process — so an
    // overwrite of an existing inode mints a fresh chunk id rather than re-minting (and
    // clobbering) the prior version's. This is restart-safe by construction — a new process
    // resumes inodes from the persisted counter and draws a fresh chunk epoch, never replaying
    // a committed id and corrupting an object (issue #364 finding 1, preserved and generalised).
    chunk_epoch: u64,
    next_chunk_seq: AtomicU64,
}

impl<M, C, Co> Gateway<M, C, Co>
where
    M: MetadataStore,
    // `PlacementChunkStore` (its supertrait is `ChunkStore`) so the read path resolves
    // each fragment from the committed placement record, not `index % n` (0005, M3.1).
    C: PlacementChunkStore,
    Co: Coordination,
{
    /// Compose a gateway over the given backends.
    pub fn new(meta: M, chunks: C, coord: Co) -> Self {
        Self {
            meta,
            chunks,
            coord,
            chunk_size: DEFAULT_CHUNK_SIZE,
            durability: DEFAULT_DURABILITY,
            lease_ttl_millis: DEFAULT_LEASE_TTL_MILLIS,
            chunk_epoch: random_chunk_epoch(),
            next_chunk_seq: AtomicU64::new(0),
        }
    }

    /// Seed the **shared, persisted** inode allocator from persisted state so this gateway
    /// resumes allocating *above* every inode already on disk — a restart over a non-empty
    /// store, or an in-place upgrade from a store an older single-process gateway wrote with
    /// no `meta:next_inode` counter, never re-mints a committed inode id and spuriously
    /// rejects a new-key PUT (issue #364 durability finding 1, preserved).
    ///
    /// Chunk ids need **no** recovery: they are coordination-free (a per-gateway random
    /// [`chunk_epoch`](Self::chunk_epoch), ADR-0019), so a fresh process draws a disjoint id
    /// range and can never replay a committed chunk id. A fresh store leaves the allocator at
    /// its default (next id 1), exactly as [`new`](Gateway::new) leaves it.
    ///
    /// The composition root calls this after `new` and before serving
    /// (`server::cli::serve_s3`, `cli.rs:1415`). Idempotent and monotone: it only ever
    /// *raises* the persisted counter, so a concurrent allocation already past the mark is
    /// never rewound.
    pub async fn recover(&self) -> Result<()> {
        let (max_inode, _max_chunk) = metadata::high_water_marks(&self.meta).await?;
        crate::cli::seed_next_inode_floor(&self.meta, max_inode.saturating_add(1)).await
    }

    /// Set the chunk size (mainly so tests can force multi-chunk objects).
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(1);
        self
    }

    /// Set the durability scheme objects are stored under.
    pub fn with_durability(mut self, durability: EcScheme) -> Self {
        self.durability = durability;
        self
    }

    /// Register this node in the coordination seam so peers can discover it. At
    /// M0 this mainly proves all three backends are genuinely composed.
    pub async fn announce(&self, node: &str) -> Result<()> {
        self.coord
            .register(
                NODES_GROUP,
                Bytes::copy_from_slice(node.as_bytes()),
                NODE_LEASE_TTL,
            )
            .await?;
        Ok(())
    }

    /// The nodes currently registered with the coordination backend.
    pub async fn nodes(&self) -> Result<Vec<Bytes>> {
        self.coord.discover(NODES_GROUP).await
    }

    /// S3 PUT: store `data` under `key`, creating the object or overwriting an
    /// existing one. Runs the four-phase write protocol; the commit is atomic and
    /// conditional, so a concurrent writer would lose with an error rather than
    /// corrupt the object.
    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let plan = write::plan_write(data, self.chunk_size, self.durability, || {
            self.mint_chunk_id()
        })?;
        let lease_expiry = now_millis() + self.lease_ttl_millis;
        write::intent(&self.meta, &plan, lease_expiry).await?;
        write::write_fragments(&self.chunks, &plan).await?;
        self.commit_written(key, &plan).await
    }

    /// Phase 3–4 (commit + release) shared by the buffered and streaming PUT paths:
    /// resolve the key, CAS the new chunk map onto the prior inode (overwrite) or
    /// create it, and release the ledger on a winning commit. A concurrent writer
    /// loses with [`GatewayError::Conflict`] rather than corrupting the object.
    ///
    /// On an **overwrite** the prior object's fragments are superseded; they are orphaned in
    /// the *same atomic commit* that swaps the chunk map ([`write::commit_overwrite`] →
    /// [`metadata::commit_chunk_map_superseding`]), so the custodian GC reclaims them after the
    /// reader-safe grace window — an overwrite neither leaks the old bytes (issue #364) nor
    /// tears a concurrent reader still streaming the prior version.
    async fn commit_written(&self, key: &str, plan: &write::WritePlan) -> Result<()> {
        let outcome = match read::resolve(&self.meta, ROOT, key).await? {
            Some(inode_id) => {
                let prior = read::read_inode(&self.meta, inode_id)
                    .await?
                    .ok_or(GatewayError::DanglingDirent)?;
                write::commit_overwrite(&self.meta, inode_id, &prior, plan, now_millis()).await?
            }
            None => {
                // Allocate the inode from the SHARED store's `meta:next_inode` CAS allocator,
                // exactly as the CLI cluster path does (`cli::cluster_store_put`,
                // `cli.rs:1158`; allocator body `cli.rs:1027`) — never a per-process counter,
                // so two active-active gateways seeded from the same baseline mint DISTINCT
                // inodes and the create CAS resolves any dirent race cleanly (issue #477).
                let inode_id = crate::cli::alloc_inode(&self.meta).await?;
                write::commit_create(&self.meta, ROOT, key, inode_id, plan).await?
            }
        };
        match outcome {
            CommitOutcome::Committed => {
                write::release(&self.meta, plan).await?;
                Ok(())
            }
            CommitOutcome::Conflict => Err(GatewayError::Conflict.into()),
        }
    }

    /// S3 GET: read the object stored under `key`, or `None` if there is none.
    /// Fragment checksums are verified on the way out (never returns bad data).
    pub async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
        read::read_path(&self.meta, &self.chunks, ROOT, key).await
    }

    /// Mint a **coordination-free** chunk id (ADR-0019). The per-gateway random
    /// [`chunk_epoch`](Self::chunk_epoch) forms the high 64 bits, so two concurrently-active
    /// gateways over the same fleet draw disjoint id ranges without any shared counter — neither
    /// can write a fragment under a chunk id the other has committed or has in flight. The
    /// monotonic [`next_chunk_seq`](Self::next_chunk_seq) forms the low 64 bits, so an id never
    /// repeats within a process: an **overwrite** (which reuses the inode but stores a new
    /// version's fragments) mints fresh ids rather than re-minting the prior version's. The
    /// epoch's top bit is set, so every id is ≥ 2^127 — clear of the `< 2^64` in-process space
    /// [`metadata::high_water_marks`] scans and of the cluster path's `(inode << 64) | seq` ids.
    fn mint_chunk_id(&self) -> ChunkId {
        let seq = self.next_chunk_seq.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.chunk_epoch) << 64) | u128::from(seq)
    }
}

/// Draw a per-gateway random 64-bit chunk-id epoch (top bit set) from OS entropy.
///
/// This is the coordination-free half of the chunk-id scheme (ADR-0019): two gateway
/// **processes** seeded from the same persisted state still draw **independent** epochs, so
/// their chunk-id ranges are disjoint and neither can overwrite a chunk the other committed —
/// the multi-writer invariant (issue #477), without a shared allocator. Setting the top bit
/// keeps every minted id ≥ 2^127, clear of the `< 2^64` in-process space
/// [`metadata::high_water_marks`] recovers and of the cluster path's `(inode << 64) | seq` ids.
///
/// The entropy comes from [`std::collections::hash_map::RandomState`], which the standard
/// library seeds from the OS RNG (the same source that keys `HashMap` against collision
/// attacks) — so no extra crate is pulled into the gateway binary, and each construction draws
/// a fresh key, so two gateways built in one process draw different epochs too.
fn random_chunk_epoch() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let raw = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    raw | (1u64 << 63)
}

/// `Gateway` **is** an [`ObjectGateway`] — the shared gateway seam (`wyrd-gateway-core`)
/// the S3 wire surface (`wyrd-gateway-s3`) and any future front-door drive objects through
/// (issue #364, T5-a). This impl maps the neutral seam onto the concrete write/read/delete
/// paths; the wire layer never names `M`/`C`/`Co`. The `Send + Sync + 'static` bounds are
/// what a networked front-door needs (it spawns the streaming-GET reader task).
impl<M, C, Co> ObjectGateway for Gateway<M, C, Co>
where
    M: MetadataStore + Send + Sync + 'static,
    C: PlacementChunkStore + Send + Sync + 'static,
    Co: Coordination + Send + Sync + 'static,
{
    /// PUT, **streaming**: store the object whose bytes arrive over `source` without ever
    /// holding the whole object in memory — the "stream, don't buffer" invariant that closes
    /// the `0015:789` OOM cliff on the wire surface (issue #364). Each `chunk_size` piece is
    /// leased + written as it arrives ([`write::stream_write_data`]); only the
    /// leased-but-uncommitted chunk **map** is retained until the final commit.
    ///
    /// `expected` is the content-integrity check the wire layer already authenticated the
    /// body against (verified *before* this call runs, so a body is only ever read for an
    /// authenticated request). The data phase is leased garbage until the commit, so a body
    /// whose running SHA-256 does not match [`ContentHash::Expected`] is rejected **before**
    /// it is published — the write never commits and the sweep reclaims it.
    async fn put_object_streaming<S>(
        &self,
        key: &str,
        source: S,
        expected: ContentHash,
    ) -> Result<()>
    where
        S: futures_util::Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        let mut hashing = HashingSource::new(source);
        let plan = write::stream_write_data(
            &self.meta,
            &self.chunks,
            &mut hashing,
            self.chunk_size,
            self.durability,
            // A clock closure, not a fixed instant: `stream_write_data` reads it per chunk so
            // each chunk is leased from its own write time and the in-flight leases are renewed
            // as a slow upload progresses (issue #364 finding 2).
            now_millis,
            self.lease_ttl_millis,
            || self.mint_chunk_id(),
        )
        .await?;

        if let ContentHash::Expected(claimed) = expected {
            let actual = hex(&hashing.finalize());
            if !constant_time_eq(claimed.as_bytes(), actual.as_bytes()) {
                // Authenticated, but the delivered bytes are not the expected bytes:
                // abort before the commit — the leased fragments are never published.
                return Err(GatewayError::PayloadMismatch.into());
            }
        }
        self.commit_written(key, &plan).await
    }

    /// GET, **streaming**: resolve the object and return a body stream that reads it one
    /// chunk at a time (bounded-channel backpressure), so the whole object is never resident
    /// in the gateway heap (issue #364, `0015:789`). `None` if `key` has no committed object.
    /// Each chunk's fragment checksums are verified on the way out, so a streamed read still
    /// never yields corrupt bytes. The body is boxed to the neutral
    /// [`ObjectRead`](wyrd_gateway_core::ObjectRead) stream so the seam names no runtime
    /// detail, and the committed object size rides alongside it for response framing.
    async fn get_object_streaming(self: Arc<Self>, key: &str) -> Result<Option<ObjectRead>> {
        let Some(inode) = read::committed_inode(&self.meta, ROOT, key).await? else {
            return Ok(None);
        };
        // Carry the committed object size out with the stream so the wire layer frames the
        // response (`Content-Length`): if a chunk read faults mid-stream (e.g. a fragment
        // reclaimed by a racing DELETE), the body ends early and the client detects the short
        // read as a truncation instead of a complete object (issue #364 carry-forward).
        let size = inode.size;
        // A small bound (a handful of chunks) so peak resident bytes stay O(chunk_size),
        // not O(object): the reader task blocks on the channel until the socket drains.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);
        let this = Arc::clone(&self);
        let chunk_map = inode.chunk_map;
        // `in_current_span` carries the caller's span — the S3 request span holding
        // `request_id` (#529) — across the spawn. Without it the reader task starts with no
        // span, and every fault the read path raises here would be logged unattributed: the
        // task is *detached*, so its errors already reach no caller (#464 — "the gateway
        // logged nothing for the failed read"), and an unattributed log line could not be
        // joined back to the request that provoked it either.
        tokio::spawn(
            async move {
                for chunk in &chunk_map {
                    let piece = read::read_chunk_verified(&this.meta, &this.chunks, chunk).await;
                    let is_err = piece.is_err();
                    if tx.send(piece).await.is_err() || is_err {
                        break;
                    }
                }
            }
            .in_current_span(),
        );
        Ok(Some(ObjectRead {
            size,
            stream: Box::pin(ReceiverStream::new(rx)),
        }))
    }

    /// DELETE: remove the object stored under `key`. Returns `true` if an object was removed
    /// and `false` if `key` was already absent — DELETE is **idempotent** (S3's 204), so
    /// deleting a missing key is a success, not an error.
    ///
    /// The dirent and its inode are removed under a compare-and-set on both records
    /// ([`metadata::unlink`]). If a concurrent writer wins that CAS, we re-resolve:
    /// a racing DELETE that already removed the key is success (the *observable*
    /// outcome is idempotent — two concurrent DELETE operations on the same key both succeed),
    /// while a racing overwrite is retried against the new record.
    ///
    /// The removed object's chunk fragments are **not** reclaimed eagerly on the delete
    /// path. Instead [`metadata::unlink`] writes an orphan grace record for each fragment in
    /// the *same atomic batch* that unbinds the object — keyed by the **D server the chunk
    /// map placed it on** — and the custodian GC (`crates/custodian/src/gc.rs`) reclaims the
    /// bytes only once the **reader-safe grace window** has elapsed (proposal 0005,
    /// `0005:288-295`). This is deliberate: an eager reclaim would delete fragments out from
    /// under a **concurrent streaming GET** that resolved the chunk map before the DELETE,
    /// truncating its body (issue #364 carry-forward, GET-during-DELETE). Deferring to the
    /// grace window keeps such a reader intact, and — because the orphan record is durable
    /// the instant the object becomes unreferenced — a crash never strands the bytes either.
    async fn delete_object(&self, key: &str) -> Result<bool> {
        // Bound the retry so a pathological overwrite storm cannot spin forever; a
        // genuine concurrent-delete race resolves on the first re-check.
        const MAX_DELETE_RETRIES: usize = 8;
        for _ in 0..MAX_DELETE_RETRIES {
            match metadata::unlink(&self.meta, ROOT, key, now_millis()).await? {
                None => return Ok(false),
                Some(metadata::Unlinked {
                    outcome: CommitOutcome::Committed,
                    ..
                }) => {
                    // We won the removal. The object's fragments are now orphaned under the
                    // grace ledger `unlink` wrote; GC reclaims them after the reader-safe
                    // window (never eagerly here — a concurrent GET may still be streaming
                    // them). The removal is the observable success.
                    return Ok(true);
                }
                Some(metadata::Unlinked {
                    outcome: CommitOutcome::Conflict,
                    ..
                }) => {
                    if read::resolve(&self.meta, ROOT, key).await?.is_none() {
                        // A racing DELETE removed it first: idempotent success.
                        return Ok(false);
                    }
                    // A racing overwrite replaced the inode: retry against the new one.
                }
            }
        }
        Err(GatewayError::Conflict.into())
    }
}

/// A body stream that computes the running SHA-256 of the bytes flowing through it,
/// so the streamed payload's hash can be checked against the wire layer's authenticated
/// content hash **after** the body has streamed to the store (never buffering it). Wraps
/// an inner byte stream and forwards its items unchanged. `Unpin` because both the inner
/// stream and [`sha2::Sha256`] are `Unpin`, so no `unsafe`/pin-project is needed under the
/// crate's `#![forbid(unsafe_code)]`.
struct HashingSource<S> {
    inner: S,
    hasher: Sha256,
}

impl<S> HashingSource<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

/// Lower-case hex encoding of `bytes` — used to render the streamed body's digest for the
/// constant-time comparison against the wire layer's [`ContentHash::Expected`] claim.
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    s
}

/// Length-then-constant-time byte comparison, so the payload-hash check leaks neither a
/// length nor an early-mismatch timing signal.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl<S> futures_util::Stream for HashingSource<S>
where
    S: futures_util::Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut this.inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                this.hasher.update(bytes.as_ref());
                std::task::Poll::Ready(Some(Ok(bytes)))
            }
            other => other,
        }
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch. The access
/// layer may read the real clock; the DST-tested core takes time as a parameter.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `:32` `<<` — the gateway's default chunk size is 1 MiB; `>>` collapses it to 0.
    #[test]
    fn default_chunk_size_is_one_mib() {
        assert_eq!(DEFAULT_CHUNK_SIZE, 1 << 20);
    }
}
