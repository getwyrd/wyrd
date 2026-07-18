//! The client write path: the four-phase commit protocol (architecture §5/§6.1)
//! over the [`MetadataStore`] and [`ChunkStore`] seams.
//!
//! 1. **Intent** — register the chunk ids in the pending ledger with a lease.
//! 2. **Data** — write each fragment to the chunk store (it verifies checksums).
//! 3. **Commit** — one atomic metadata mutation writes the chunk map, sets state
//!    `COMMITTED`, and bumps the version conditional on the prior. *This is the
//!    atomicity*: the file does not exist until it and fully exists after it;
//!    concurrent writers conflict here and exactly one wins.
//! 4. **Release** — delete the ledger entries.
//!
//! A crash before step 3 leaves leased garbage; a crash between 3 and 4 leaves
//! ledger entries. Both are reclaimed by [`sweep_expired_leases`], the
//! test-invoked stand-in for the custodian GC. The steps are exposed
//! individually so a test can stop after any phase to model a crash.
//!
//! Time enters as `now_millis` and chunk ids via a caller-supplied generator, so
//! this module is backend- and runtime-agnostic and a run is reproducible from a
//! seed (ADR-0009).

use bytes::Bytes;
use wyrd_chunk_format::{encode, EcSchemeType, FragmentHeader};
use wyrd_traits::{
    ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, PlacementChunkStore, Result,
    WriteBatch,
};

use crate::erasure;
use crate::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, ObjectMeta, PendingEntry,
};
use crate::placement::{select_distinct_domains, SelectorError, Topology};

/// One planned chunk: its id, durability scheme, logical length, the encoded
/// fragments to store — each `(ec_fragment_index, fragment bytes)` — and the
/// **placement** the fragments land on.
#[derive(Debug, Clone)]
pub struct PlannedChunk {
    /// The chunk id minted for this chunk (shared by all its fragments).
    pub id: ChunkId,
    /// How the chunk is fragmented.
    pub scheme: EcScheme,
    /// The chunk's logical (pre-coding) length.
    pub len: u64,
    /// The fragments to write, by `ec_fragment_index`.
    pub fragments: Vec<(u16, Bytes)>,
    /// The stable D-server id each fragment is placed on, in fragment-index order
    /// (length `n`). Defaults to the **identity** placement (`index` → D-server
    /// `index`) — M0–M2's `index % n` route — and is overwritten by
    /// [`WritePlan::place`] with the failure-domain selector's distinct-domain
    /// choice (Option B, proposal 0005, `0005:510-513`). The write fan-out places
    /// each fragment at `placement[index]` and the commit records this vector.
    pub placement: Vec<DServerId>,
}

/// The pure result of chunking and encoding an object — no store access yet.
#[derive(Debug, Clone, Default)]
pub struct WritePlan {
    /// The chunks in object order.
    pub chunks: Vec<PlannedChunk>,
    /// Total object length in bytes.
    pub size: u64,
    /// The object metadata to commit **atomically with the chunk map** at content
    /// publication (ADR-0047): the content digest / declared content type / publication
    /// time. Chunking cannot know the ETag (the streaming digest is only final once the
    /// body has streamed), so the composition root fills this after the data phase and
    /// before the commit; a plan left with the default (all `None`) commits no metadata,
    /// preserving the pre-metadata wire behaviour for callers that do not set it.
    pub object_meta: ObjectMeta,
}

impl WritePlan {
    /// The chunk ids in object order — the keys of the pending ledger.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.id).collect()
    }

    /// The chunk map for the inode record — id, scheme, logical length, and the
    /// **placement record** per chunk. Placement is each chunk's `placement` vector:
    /// the identity vector (`index` → D-server `index`) for the M0–M2 `index % n`
    /// route, or — once [`place`](WritePlan::place) has run the failure-domain
    /// selector — the **distinct-domain** choice (Option B, proposal 0005,
    /// `0005:510-513`, `0005:235-245`). The read path resolves each fragment **from
    /// the record** instead of recomputing the route (`0005:489-490`), so a custodian
    /// that later *moves* a fragment rewrites this entry and the read follows it.
    pub fn chunk_refs(&self) -> Vec<ChunkRef> {
        self.chunks
            .iter()
            .map(|c| ChunkRef {
                id: c.id,
                scheme: c.scheme,
                len: c.len,
                placement: c.placement.clone(),
            })
            .collect()
    }

    /// Assign each chunk's placement with the **failure-domain-aware selector** so
    /// the committed record spreads a chunk's `n` fragments across `n` **distinct**
    /// failure domains, **retiring identity `index % n`** as the write's placement
    /// choice (Option B, proposal 0005 §"Failure-domain-aware placement",
    /// `0005:235-245`, `0005:510-513`). Errors with [`SelectorError`] when `topology`
    /// offers fewer than `n` distinct domains — the selector refuses rather than
    /// collide domains (durability is gate-zero, `0005:303`).
    ///
    /// This is the point at which the selector is **shared by the write fan-out**
    /// (`0005:241-242`): the placement it records is the placement
    /// [`write_fragments`] fans the bytes out to.
    pub fn place(&mut self, topology: &Topology) -> std::result::Result<(), SelectorError> {
        for chunk in &mut self.chunks {
            let n = chunk.fragments.len() as u16;
            chunk.placement = select_distinct_domains(topology, n)?;
        }
        Ok(())
    }
}

/// Encode a single **rebuilt** Reed-Solomon shard into a self-describing v1 fragment,
/// stamping the chunk id and EC header fields (`ec_k`/`ec_m`/`ec_fragment_index`)
/// exactly as the write fan-out does ([`encode_chunk`]) so a reader decodes and
/// verifies it identically. This is the re-place step's format primitive, exposed for
/// the **reconstruction** custodian (`0005:276`) — which thus rebuilds a fragment
/// without depending on the on-disk format directly (ADR-0010, `0005:421-422`);
/// `core` owns the format writer.
pub fn encode_ec_fragment(chunk: ChunkId, index: u16, k: u8, m: u8, shard: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(chunk, shard.len() as u64);
    header.ec_scheme_type = EcSchemeType::ReedSolomon;
    header.ec_k = k;
    header.ec_m = m;
    header.ec_fragment_index = index;
    Bytes::from(encode(&header, shard))
}

/// Encode one chunk's `piece` into its fragments under `scheme`.
fn encode_chunk(
    scheme: EcScheme,
    id: ChunkId,
    piece: &[u8],
) -> std::result::Result<Vec<(u16, Bytes)>, erasure::ErasureError> {
    match scheme {
        EcScheme::None => {
            let header = FragmentHeader::new_v1(id, piece.len() as u64);
            Ok(vec![(0, Bytes::from(encode(&header, piece)))])
        }
        EcScheme::ReedSolomon { k, m } => {
            let shards = erasure::encode(k as usize, m as usize, piece)?;
            Ok(shards
                .into_iter()
                .enumerate()
                .map(|(i, shard)| {
                    let mut header = FragmentHeader::new_v1(id, shard.len() as u64);
                    header.ec_scheme_type = EcSchemeType::ReedSolomon;
                    header.ec_k = k;
                    header.ec_m = m;
                    header.ec_fragment_index = i as u16;
                    (i as u16, Bytes::from(encode(&header, &shard)))
                })
                .collect())
        }
    }
}

/// Chunk `data` into `chunk_size`-byte pieces, mint a chunk id per piece via
/// `next_id`, and erasure-code each into its fragments under `scheme`. Pure and
/// deterministic given `next_id`. An empty object yields an empty plan.
pub fn plan_write(
    data: &[u8],
    chunk_size: usize,
    scheme: EcScheme,
    mut next_id: impl FnMut() -> ChunkId,
) -> std::result::Result<WritePlan, erasure::ErasureError> {
    let chunk_size = chunk_size.max(1);
    let mut chunks = Vec::new();
    for piece in data.chunks(chunk_size) {
        let id = next_id();
        let fragments = encode_chunk(scheme, id, piece)?;
        // Default to the identity placement (`index` → D-server `index`), the M0–M2
        // `index % n` route; `WritePlan::place` overwrites it with the selector's
        // distinct-domain choice when a topology is supplied (Option B).
        let placement = (0..fragments.len() as u64).collect();
        chunks.push(PlannedChunk {
            id,
            scheme,
            len: piece.len() as u64,
            fragments,
            placement,
        });
    }
    Ok(WritePlan {
        chunks,
        size: data.len() as u64,
        object_meta: ObjectMeta::default(),
    })
}

/// Phase 1 — Intent: register every chunk id in the pending ledger with a lease
/// expiring at `lease_expiry_millis`.
pub async fn intent(
    meta: &impl MetadataStore,
    plan: &WritePlan,
    lease_expiry_millis: u64,
) -> Result<()> {
    for chunk in &plan.chunks {
        metadata::put_pending(
            meta,
            chunk.id,
            &PendingEntry {
                lease_expiry_millis,
            },
        )
        .await?;
    }
    Ok(())
}

/// Phase 2 — Data: write every fragment to the chunk store, which verifies its
/// checksums.
///
/// The puts are fired as a **concurrent fan-out** and joined on every one: with a
/// fan-out store (M2.4) a chunk's `n` fragments stream to distinct D servers in
/// parallel rather than one after another, so the data phase costs one round trip,
/// not `n`. The join is single-task cooperative concurrency (it polls the futures,
/// it does not spawn), so it stays deterministic under simulation (ADR-0009).
///
/// **Fail-closed:** if any put fails or times out the whole phase returns that
/// error, so the four-phase protocol aborts *before* the commit — there is never a
/// half-committed chunk. The fragments that did land are harmless **leased
/// garbage** the pending-ledger sweep reclaims. Degraded-write tolerance
/// (committing with fewer than `n` and letting custodians backfill) is deferred to
/// M3; M2 commits only after **all `n`** ack.
pub async fn write_fragments(chunks: &impl PlacementChunkStore, plan: &WritePlan) -> Result<()> {
    let puts = plan.chunks.iter().flat_map(|chunk| {
        chunk.fragments.iter().map(move |(index, fragment)| {
            let id = FragmentId {
                chunk: chunk.id,
                index: *index,
            };
            // Place the fragment at the D server its placement record names — the
            // selector's distinct-domain choice once `place` has run, or the identity
            // route otherwise. The default `put_fragment_at` of a single-authority
            // store delegates to `put_fragment` (index-routed), so M0–M2 behaviour is
            // preserved; a placement-aware fleet honours the chosen id.
            let dserver = chunk
                .placement
                .get(*index as usize)
                .copied()
                .unwrap_or_else(|| DServerId::from(*index));
            chunks.put_fragment_at(dserver, id, fragment.clone())
        })
    });
    futures_util::future::try_join_all(puts).await?;
    Ok(())
}

/// Phase 3 — Commit (new file): atomically create the inode (state `COMMITTED`,
/// the chunk map, version 1) and its dirent. `Conflict` if the name exists.
///
/// The create is **lease-conditional** (issue #490): it publishes only if every chunk in the
/// plan still holds a live, unexpired `pending:<id>` lease at `now_millis` — enforced in the
/// same atomic batch as the create, so a commit that outran its leases (a stall past the TTL,
/// or a lease a racing sweep reclaimed) fails closed with `Conflict` rather than publishing an
/// object over fragments the custodian GC is free to reclaim. `now_millis` is the commit's own
/// logical instant (the caller's clock at the commit point). See [`metadata::create_leased`].
pub async fn commit_create(
    meta: &impl MetadataStore,
    parent: InodeId,
    name: &str,
    inode_id: InodeId,
    plan: &WritePlan,
    now_millis: u64,
) -> Result<CommitOutcome> {
    let record = InodeRecord {
        size: plan.size,
        chunk_map: plan.chunk_refs(),
        state: InodeState::Committed,
        version: 1,
        // A create is a content publication (ADR-0047): stamp the object metadata the
        // composition root recorded on the plan (empty by default, so callers that do not
        // set it commit no metadata and keep the pre-metadata wire behaviour).
        etag: plan.object_meta.etag.clone(),
        content_type: plan.object_meta.content_type.clone(),
        modified: plan.object_meta.modified,
    };
    metadata::create_leased(
        meta,
        parent,
        name,
        inode_id,
        &record,
        &plan.chunk_ids(),
        now_millis,
    )
    .await
}

/// Phase 3 — Commit (overwrite): CAS the inode's chunk map + size onto `prior`,
/// bumping the version. `Conflict` rejects a stale writer; exactly one wins.
///
/// The prior object's fragments are **superseded** by this overwrite, so they are orphaned
/// in the *same atomic commit* (`orphaned_at` stamps the grace record) — the custodian GC
/// reclaims them after the reader-safe grace window, so an overwrite neither leaks the old
/// bytes (issue #364, PUT-overwrite reclaim) nor tears a concurrent reader of the prior
/// version. See [`metadata::commit_chunk_map_superseding_leased`].
///
/// The overwrite is **lease-conditional** (issue #490): it publishes the new chunk map only if
/// every chunk in the plan still holds a live, unexpired `pending:<id>` lease — enforced in the
/// same atomic batch as the CAS, so a commit that outran its leases (a stall past the TTL, or a
/// lease a racing sweep reclaimed) fails closed with `Conflict` rather than publishing over
/// fragments the custodian GC is free to reclaim. `orphaned_at_millis` **is** the commit's own
/// logical instant, so it doubles as the `now` the lease check is evaluated against (obligation
/// (e), issue #490); its one production caller passes `now_millis()` (`server/src/lib.rs:184`).
pub async fn commit_overwrite(
    meta: &impl MetadataStore,
    inode_id: InodeId,
    prior: &InodeRecord,
    plan: &WritePlan,
    orphaned_at_millis: u64,
) -> Result<CommitOutcome> {
    metadata::commit_chunk_map_superseding_leased(
        meta,
        inode_id,
        prior,
        plan.chunk_refs(),
        plan.size,
        orphaned_at_millis,
        &plan.chunk_ids(),
        orphaned_at_millis,
        // An overwrite is a fresh content publication (ADR-0047): stamp the plan's object
        // metadata onto the new version.
        &plan.object_meta,
    )
    .await
}

/// Phase 4 — Release: delete the pending-ledger entries for a committed write.
pub async fn release(meta: &impl MetadataStore, plan: &WritePlan) -> Result<()> {
    metadata::sweep_pending(meta, &plan.chunk_ids()).await?;
    Ok(())
}

/// Write a brand-new object end to end (the four phases in order). `now_fn` is the
/// caller's clock (a closure, exactly as [`stream_write_data`] takes one): it is read
/// once to stamp the lease (expiring `lease_ttl_millis` later) and read AGAIN at the
/// commit point, so the lease-conditional create (issue #490) is evaluated against the
/// commit's own instant. A single fixed instant would make the guard a tautology —
/// `lease_expiry = t + ttl` compared against the same `t` can never read as lapsed, no
/// matter how long the data phase or the caller stalled, and the helper would publish
/// over bytes the custodian GC is already free to reclaim. The ledger is released only
/// on a winning commit; a losing commit leaves leased garbage for the sweep.
#[allow(clippy::too_many_arguments)]
pub async fn write_new_object(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    parent: InodeId,
    name: &str,
    inode_id: InodeId,
    data: &[u8],
    chunk_size: usize,
    scheme: EcScheme,
    mut now_fn: impl FnMut() -> u64,
    lease_ttl_millis: u64,
    next_id: impl FnMut() -> ChunkId,
) -> Result<CommitOutcome> {
    let plan = plan_write(data, chunk_size, scheme, next_id)?;
    intent(meta, &plan, now_fn() + lease_ttl_millis).await?;
    write_fragments(chunks, &plan).await?;
    let outcome = commit_create(meta, parent, name, inode_id, &plan, now_fn()).await?;
    if outcome == CommitOutcome::Committed {
        release(meta, &plan).await?;
    }
    Ok(outcome)
}

/// Write a brand-new object **placing its fragments across distinct failure
/// domains** (Option B, proposal 0005 §"Failure-domain-aware placement"). Identical
/// to [`write_new_object`] except the plan is run through the failure-domain
/// selector against `topology` before the data phase, so the committed placement
/// record reflects the **distinct-domain** choice (NOT the identity `index % n`
/// vector) and the fan-out streams each fragment to its selected D server
/// (`0005:510-513`). Errors with the selector's [`SelectorError`] (surfaced through
/// the boxed `Result`) when the topology offers fewer than `n` distinct domains —
/// the write fails closed rather than collide domains.
///
/// `now_fn` is read twice — lease stamp, then commit instant — for the same
/// lease-liveness reason as [`write_new_object`].
#[allow(clippy::too_many_arguments)]
pub async fn write_new_object_placed(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    parent: InodeId,
    name: &str,
    inode_id: InodeId,
    data: &[u8],
    chunk_size: usize,
    scheme: EcScheme,
    topology: &Topology,
    mut now_fn: impl FnMut() -> u64,
    lease_ttl_millis: u64,
    next_id: impl FnMut() -> ChunkId,
) -> Result<CommitOutcome> {
    let mut plan = plan_write(data, chunk_size, scheme, next_id)?;
    plan.place(topology)?;
    intent(meta, &plan, now_fn() + lease_ttl_millis).await?;
    write_fragments(chunks, &plan).await?;
    let outcome = commit_create(meta, parent, name, inode_id, &plan, now_fn()).await?;
    if outcome == CommitOutcome::Committed {
        release(meta, &plan).await?;
    }
    Ok(outcome)
}

/// Intent + Data for **one** chunk, streaming: lease the chunk id then fan its
/// fragments out (the same per-chunk `intent`→`write_fragments` the whole-object path
/// runs, `write.rs`). Returns the chunk's metadata with its fragment **bytes dropped**
/// — they are already written, so only the id/scheme/len/placement is retained for the
/// commit, keeping the streaming write's resident footprint at one chunk (issue #364,
/// `0015:789`).
async fn intent_and_write_chunk(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    scheme: EcScheme,
    id: ChunkId,
    piece: &[u8],
    lease_expiry_millis: u64,
) -> Result<PlannedChunk> {
    let fragments = encode_chunk(scheme, id, piece)?;
    // Identity placement (`index` → D-server `index`), the M0–M2 `index % n` route —
    // the streaming path does not run the failure-domain selector (a later refinement).
    let placement: Vec<DServerId> = (0..fragments.len() as u64).collect();
    metadata::put_pending(
        meta,
        id,
        &PendingEntry {
            lease_expiry_millis,
        },
    )
    .await?;
    let puts = fragments.iter().map(|(index, fragment)| {
        let frag_id = FragmentId {
            chunk: id,
            index: *index,
        };
        let dserver = placement
            .get(*index as usize)
            .copied()
            .unwrap_or_else(|| DServerId::from(*index));
        chunks.put_fragment_at(dserver, frag_id, fragment.clone())
    });
    futures_util::future::try_join_all(puts).await?;
    Ok(PlannedChunk {
        id,
        scheme,
        len: piece.len() as u64,
        // Bytes already written and dropped — the commit needs only `chunk_refs()`.
        fragments: Vec::new(),
        placement,
    })
}

/// Lease + write one streaming chunk, first **renewing the in-flight leases** written so
/// far if the clock has advanced past `renew_at`.
///
/// Because a streaming PUT commits only after its *last* chunk arrives, an early chunk's
/// fragments are protected from the custodian **GC** solely by its pending lease — they are
/// in no committed chunk map yet, so GC's committed reference set does not cover them
/// (`custodian::gc::reconcile`). A single start-of-upload deadline stamped on every chunk
/// (the prior behaviour) let a slow authenticated upload run past it, so the GC would reclaim
/// the early chunks as *expired* garbage before the commit and publish an object with missing
/// fragments (issue #364 durability finding 2). Renewing keeps every in-flight lease a fresh
/// TTL ahead of the most recent write, and the new chunk is stamped from its own write time.
/// A stall longer than the TTL *between* two chunks still lapses — a genuinely dead upload the
/// sweep should reap.
#[allow(clippy::too_many_arguments)]
async fn lease_write_chunk(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    scheme: EcScheme,
    id: ChunkId,
    piece: &[u8],
    now_millis: u64,
    lease_ttl_millis: u64,
    leased: &mut Vec<ChunkId>,
    renew_at: &mut u64,
) -> Result<PlannedChunk> {
    if !leased.is_empty() && now_millis >= *renew_at {
        // Renewal is CONDITIONAL: it refuses (Conflict) if an in-flight lease was swept or has
        // already lapsed. A lapsed lease is dead — resurrecting it would let this upload commit
        // an inode over fragments the GC is free to reclaim (issue #490). Abort the upload
        // BEFORE the next chunk is written toward commit, rather than publish missing fragments.
        let outcome = metadata::renew_pending(
            meta,
            leased,
            now_millis,
            &PendingEntry {
                lease_expiry_millis: now_millis + lease_ttl_millis,
            },
        )
        .await?;
        if outcome != CommitOutcome::Committed {
            return Err(WriteError::LeaseLapsed {
                chunks: leased.clone(),
            }
            .into());
        }
        *renew_at = now_millis + lease_ttl_millis / 2;
    }
    let planned = intent_and_write_chunk(
        meta,
        chunks,
        scheme,
        id,
        piece,
        now_millis + lease_ttl_millis,
    )
    .await?;
    if leased.is_empty() {
        // First chunk of the upload: arm the renewal clock half a TTL out.
        *renew_at = now_millis + lease_ttl_millis / 2;
    }
    leased.push(id);
    Ok(planned)
}

/// Phases 1–2 (Intent + Data) driven from a **byte stream** rather than a full
/// `&[u8]`: re-chunk the incoming buffers into `chunk_size` pieces and lease + write
/// each chunk **as it arrives**, so the object is never held whole in memory — the
/// "stream, don't buffer" invariant that closes the `0015:789` OOM cliff for the S3
/// wire surface (issue #364). Peak resident bytes are one `chunk_size` piece plus its
/// fragments, independent of object size.
///
/// The returned [`WritePlan`] carries the committed chunk **map** (no fragment bytes)
/// and total size, so the caller drives phase 3 (commit) with the existing
/// [`commit_create`] / [`commit_overwrite`] and phase 4 ([`release`]) exactly as the
/// buffered path does — the data written here is **leased garbage** until that commit,
/// so a caller that aborts (e.g. a payload-hash mismatch) never publishes it; the
/// sweep reclaims it. `next_id` mints the chunk ids and `source` yields the body
/// buffers in order (any sizes); an empty stream yields an empty plan.
///
/// `now_fn` is read **per chunk** (not once), so each chunk is leased from its own write
/// time and the in-flight leases are **renewed** as the upload progresses ([`lease_write_chunk`]).
/// A slow upload therefore never lets the custodian GC reclaim an already-written chunk before
/// the commit — the durability hole a single start-of-upload deadline left open (issue #364
/// durability finding 2). It takes a clock closure rather than a fixed instant so a DST run
/// still drives a deterministic logical clock (ADR-0009).
#[allow(clippy::too_many_arguments)]
pub async fn stream_write_data<S>(
    meta: &impl MetadataStore,
    chunks: &impl PlacementChunkStore,
    mut source: S,
    chunk_size: usize,
    scheme: EcScheme,
    mut now_fn: impl FnMut() -> u64,
    lease_ttl_millis: u64,
    mut next_id: impl FnMut() -> ChunkId,
) -> Result<WritePlan>
where
    S: futures_util::Stream<Item = Result<Bytes>> + Unpin,
{
    use futures_util::StreamExt;

    let chunk_size = chunk_size.max(1);
    let mut planned: Vec<PlannedChunk> = Vec::new();
    // The chunk ids leased-but-not-yet-committed by this upload, and the next logical
    // instant at which to renew their leases (half a TTL ahead of the most recent write).
    let mut leased: Vec<ChunkId> = Vec::new();
    let mut renew_at: u64 = 0;
    let mut size: u64 = 0;
    let mut buf: Vec<u8> = Vec::with_capacity(chunk_size);

    while let Some(item) = source.next().await {
        let bytes = item?;
        let mut rest = &bytes[..];
        while !rest.is_empty() {
            let take = (chunk_size - buf.len()).min(rest.len());
            buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if buf.len() == chunk_size {
                let id = next_id();
                planned.push(
                    lease_write_chunk(
                        meta,
                        chunks,
                        scheme,
                        id,
                        &buf,
                        now_fn(),
                        lease_ttl_millis,
                        &mut leased,
                        &mut renew_at,
                    )
                    .await?,
                );
                size += buf.len() as u64;
                buf.clear();
            }
        }
    }
    if !buf.is_empty() {
        let id = next_id();
        planned.push(
            lease_write_chunk(
                meta,
                chunks,
                scheme,
                id,
                &buf,
                now_fn(),
                lease_ttl_millis,
                &mut leased,
                &mut renew_at,
            )
            .await?,
        );
        size += buf.len() as u64;
    }

    Ok(WritePlan {
        chunks: planned,
        size,
        object_meta: ObjectMeta::default(),
    })
}

/// Reclaim expired pending-ledger entries — the custodian sweep stand-in
/// (architecture §6.2). Deletes every `pending:` entry whose lease has expired
/// as of `now_millis` in one atomic commit, and returns the reclaimed chunk ids.
/// Orphaned *fragments* are collectable garbage; reclaiming them needs a
/// chunk-store delete (a later milestone).
pub async fn sweep_expired_leases(
    meta: &impl MetadataStore,
    now_millis: u64,
) -> Result<Vec<ChunkId>> {
    let pending = meta.scan(b"pending:").await?;
    let mut batch = WriteBatch::new();
    let mut reclaimed = Vec::new();
    for (key, value) in pending {
        let entry: PendingEntry = metadata::decode(&value)?;
        if entry.lease_expiry_millis <= now_millis {
            if let Some(id) = parse_pending_key(&key) {
                reclaimed.push(id);
            }
            batch = batch.delete(key);
        }
    }
    if !reclaimed.is_empty() {
        meta.commit(batch).await?;
    }
    Ok(reclaimed)
}

/// Parse the chunk id out of a `pending:<id>` key.
fn parse_pending_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}

/// Errors specific to the write path; surfaced through the trait's boxed error.
#[derive(Debug)]
pub enum WriteError {
    /// A streaming upload's in-flight pending lease could not be **renewed** because it had
    /// already lapsed — swept out from under the upload, or expired past its recorded deadline
    /// (issue #490). A lapsed lease is dead authority; renewing it would resurrect protection
    /// the custodian sweep already revoked and let the upload commit an inode over fragments the
    /// GC is free to reclaim. The upload is aborted before its next chunk is written toward
    /// commit, so nothing is published.
    LeaseLapsed {
        /// The in-flight chunk ids whose leases the renewal could not extend.
        chunks: Vec<ChunkId>,
    },
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::LeaseLapsed { chunks } => {
                write!(
                    f,
                    "streaming upload aborted: an in-flight pending lease lapsed and could not \
                     be renewed ({} in-flight chunk(s)); refusing to resurrect a swept lease and \
                     publish missing fragments",
                    chunks.len()
                )
            }
        }
    }
}

impl std::error::Error for WriteError {}
