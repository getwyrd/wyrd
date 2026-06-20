//! The Wyrd S3 gateway (L1, access layer): composes the concrete backends and
//! maps minimal S3 PUT/GET onto the client write and read paths.
//!
//! This is the one place that knows concretes (ADR-0010) — `server`'s `main` and
//! the tests pick the redb metadata store, the filesystem chunk store, and the
//! in-memory coordination, and a networked profile swaps them here without
//! touching any caller. PUT/GET are exposed as in-process methods at M0; the
//! HTTP/S3 wire surface is a later milestone. Full S3 semantics
//! (buckets/ACLs/multipart) are deferred — this is the walking-skeleton slice.

#![forbid(unsafe_code)]

pub mod cli;
pub mod dserver;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use wyrd_core::metadata::{EcScheme, InodeId};
use wyrd_core::{read, write};
use wyrd_traits::{ChunkId, ChunkStore, CommitOutcome, Coordination, MetadataStore, Result};

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
    // Id allocation is an in-process counter at M0 (one process, not deployable);
    // random/uncoordinated chunk ids (ADR-0019) and durable inode allocation are
    // later-milestone refinements.
    next_inode: AtomicU64,
    next_chunk: AtomicU64,
}

impl<M, C, Co> Gateway<M, C, Co>
where
    M: MetadataStore,
    C: ChunkStore,
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
            next_inode: AtomicU64::new(1), // 0 is ROOT
            next_chunk: AtomicU64::new(1),
        }
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

        let outcome = match read::resolve(&self.meta, ROOT, key).await? {
            Some(inode_id) => {
                let prior = read::read_inode(&self.meta, inode_id)
                    .await?
                    .ok_or(GatewayError::DanglingDirent)?;
                write::commit_overwrite(&self.meta, inode_id, &prior, &plan).await?
            }
            None => {
                let inode_id = self.next_inode.fetch_add(1, Ordering::Relaxed);
                write::commit_create(&self.meta, ROOT, key, inode_id, &plan).await?
            }
        };

        match outcome {
            CommitOutcome::Committed => {
                write::release(&self.meta, &plan).await?;
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

    fn mint_chunk_id(&self) -> ChunkId {
        ChunkId::from(self.next_chunk.fetch_add(1, Ordering::Relaxed))
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

/// Errors raised by the gateway, surfaced through the boxed `Result`.
#[derive(Debug, PartialEq, Eq)]
pub enum GatewayError {
    /// A concurrent writer won the commit; this PUT was rejected rather than
    /// allowed to corrupt the object.
    Conflict,
    /// A directory entry pointed at an inode the metadata store does not hold.
    DanglingDirent,
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatewayError::Conflict => write!(f, "put rejected: a concurrent writer won the commit"),
            GatewayError::DanglingDirent => write!(f, "dangling directory entry: inode missing"),
        }
    }
}

impl std::error::Error for GatewayError {}
