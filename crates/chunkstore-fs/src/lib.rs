//! Filesystem-backed [`ChunkStore`]: the embedded "D server" for dev and the
//! NAS profile. Stores each fragment's bytes in one file under its chunk's
//! directory, keyed by [`FragmentId`] (chunk id + fragment index), and verifies
//! the fragment's self-describing checksums (`chunk-format`, ADR-0019) on the way
//! in and out.
//!
//! Deliberately dumb (architecture §5, ADR-0010): it moves bytes and checks
//! their integrity, with **no placement or metadata logic**. A networked /
//! object-store backend is a later, trait-compatible swap wired by `server`.

#![forbid(unsafe_code)]

use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{decode, FragmentError};
use wyrd_testkit::{Clock, SystemClock};
use wyrd_traits::{
    ChunkId, ChunkStore, FragmentId, Health, IntegrityFault, PlacementChunkStore, Result,
    WriteDeadlineExpired,
};

/// A [`ChunkStore`] that keeps each fragment as a file under a root directory.
///
/// Generic over the [`Clock`] that decides fragment-write deadline expiry (issue #638):
/// the real [`SystemClock`] by default, a test-controlled clock through
/// [`FsChunkStore::open_with_clock`] — the seam AGENTS.md § Review rubric prescribes for
/// code that needs controllable time (ADR-0024), rather than a bare `SystemTime::now()`
/// (#619). This store, as the write's **acceptor**, is one of the two evaluation sites
/// `δ_clock` bounds the skew between in `G_orphan > W_write + δ_clock` (`0016:1478`); the
/// other is the authorizer that chose the deadline.
pub struct FsChunkStore<C: Clock = SystemClock> {
    root: PathBuf,
    /// Monotonic sequence that makes each write's scratch file name unique
    /// *within this store*, so two concurrent writes of the same [`FragmentId`]
    /// never share a scratch path and race on it (issue #203). Per-store, not
    /// process-global: this store (one `Arc<FsChunkStore>` shared across the
    /// gateway/custodian writers, `from_arc`) is the concurrency boundary every
    /// racing same-id write passes through, and one D server owns its root
    /// (ADR-0034, Model A — one D server per disk), so a per-store counter gives
    /// every concurrent writer a private scratch path with no shared *global*
    /// mutable state (which would couple otherwise-independent simulation nodes).
    scratch_seq: AtomicU64,
    /// The clock the fragment-write deadline is compared against, held behind an
    /// [`Arc`] so the write can carry it into the blocking closure that renders the
    /// verdict immediately before the publishing rename (issue #638), without
    /// demanding `Clone` of a caller's clock.
    clock: Arc<C>,
}

impl FsChunkStore<SystemClock> {
    /// Open a store rooted at `root`, creating the directory if it does not
    /// exist. Write deadlines are judged against the real wall clock.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_clock(root, SystemClock)
    }
}

impl<C: Clock> FsChunkStore<C> {
    /// Like [`FsChunkStore::open`], but judges fragment-write deadlines against
    /// `clock` — the injection point for a manual/simulated clock, so deadline
    /// expiry is exercised deterministically instead of by real waiting (ADR-0024,
    /// issue #638).
    pub fn open_with_clock(root: impl Into<PathBuf>, clock: C) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let store = Self {
            root,
            scratch_seq: AtomicU64::new(0),
            clock: Arc::new(clock),
        };
        // Clear write scratch orphaned by a crash before this store was opened, and
        // the empty chunk directories a failed, refused or reclaimed write leaves.
        // Unique per-write scratch names (issue #203) no longer self-clean the
        // way a single fixed `<index>.tmp` did (the next write of the same id
        // overwrote it), so without reaping a hard crash would let them
        // accumulate as litter. Open is the safe place: one D server owns this
        // root (ADR-0034, Model A) and no write on this just-constructed store is
        // in flight yet, so reaping cannot race a live put's scratch — nor, since
        // issue #638, a live put's chunk directory.
        store.reap_write_residue();
        Ok(store)
    }

    /// `root/<32-hex chunk>/<05-index>.frag` — a directory per chunk, one file
    /// per fragment index.
    fn fragment_path(&self, id: FragmentId) -> PathBuf {
        fragment_path(&self.root, id)
    }

    /// Sibling scratch path for the write-then-rename, made **unique per call**
    /// (chunk dir + fragment index + a per-store sequence) so two concurrent
    /// writes of the same [`FragmentId`] never share a scratch file and race on
    /// it (issue #203). The atomic rename onto `<index>.frag` is the sole
    /// publish/serialization point; the `.tmp` suffix keeps the scratch invisible
    /// to `list_fragments` (which parses only `.frag`) and matchable by
    /// [`Self::reap_write_residue`].
    fn temp_path(&self, id: FragmentId) -> PathBuf {
        let seq = self.scratch_seq.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(format!("{:032x}", id.chunk))
            .join(scratch_file_name(id.index, seq))
    }

    /// Remove what an unfinished write leaves under the root: stale scratch
    /// (`*.tmp`) from a process that crashed mid-write, before the atomic rename
    /// published the fragment, and the chunk directories left **empty** by a
    /// failed write, by a refused one (issue #638) or by the reclaim of a chunk's
    /// last fragment.
    ///
    /// Best-effort and only over recognised `<32-hex>` chunk dirs: an entry that
    /// cannot be read or removed is left in place (it is harmless — neither scratch
    /// nor an empty directory is visible to `list_fragments`, which parses only
    /// `.frag`). The directory removal is `remove_dir`, never `remove_dir_all`, so
    /// the kernel's atomic emptiness test — not this code's belief about what the
    /// directory holds — decides: a directory holding any fragment survives. It costs
    /// one `rmdir` per chunk directory on a walk that already opens each of them, so
    /// the marginal startup cost is one syscall per chunk.
    ///
    /// **Why here and nowhere else.** Collecting empty directories is the one part
    /// of write cleanup that touches state *shared* between concurrent writes, so it
    /// runs at the single point where this store has no write in flight by
    /// construction: `open`, where one D server owns the root (ADR-0034, Model A).
    /// The alternative — collecting them on the refusal path, where the write that
    /// emptied the directory notices — races every live writer creating the same
    /// chunk, and no number of create retries closes that race (issue #638; see
    /// `restore_pre_write_state`).
    fn reap_write_residue(&self) {
        let Ok(chunk_dirs) = fs::read_dir(&self.root) else {
            return;
        };
        for chunk_entry in chunk_dirs.flatten() {
            // Only descend real `<32-hex>` chunk directories.
            if chunk_entry
                .file_name()
                .to_str()
                .and_then(parse_chunk_dir_name)
                .is_none()
            {
                continue;
            }
            let Ok(entries) = fs::read_dir(chunk_entry.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                if is_temp_scratch_name(&entry.file_name()) {
                    let _ = fs::remove_file(entry.path());
                }
            }
            // Now that this chunk's scratch is gone, the directory goes too **iff**
            // it holds nothing else — `remove_dir` fails `DirectoryNotEmpty` over a
            // single surviving `.frag`, so a chunk that still has fragments keeps
            // its directory.
            let _ = fs::remove_dir(chunk_entry.path());
        }
    }

    /// Verify the fragment decodes and that its header records the expected
    /// chunk id *and* fragment index.
    fn verify(id: FragmentId, bytes: &[u8]) -> std::result::Result<(), FsChunkStoreError> {
        let decoded = decode(bytes).map_err(FsChunkStoreError::NotAFragment)?;
        let found = FragmentId {
            chunk: decoded.header.chunk_id,
            index: decoded.header.ec_fragment_index,
        };
        if found != id {
            return Err(FsChunkStoreError::IdMismatch {
                expected: id,
                found,
            });
        }
        Ok(())
    }
}

/// Undo what a **refused** write put on disk, so no fragment of it is on the store and no
/// residue of it is left behind (issue #638, the `WriteEffect::NotApplied` postcondition).
///
/// It removes **exactly one thing: this write's own scratch file** — the private
/// `<index>.<seq>.tmp` whose name no other write can name ([`FsChunkStore::temp_path`],
/// issue #203). Its errors are **returned, never swallowed**: the caller reports a rollback
/// failure as a backend fault instead of the definite "nothing landed" verdict, because that
/// verdict over leftover bytes is a silent skip (AGENTS.md § Review rubric, *Absent or
/// unsupported entries*). Only `NotFound` is not a failure — the state we wanted is the state
/// we have.
///
/// **What it deliberately does not touch is the chunk directory**, and that is the whole
/// safety property of this function: a refusal mutates **no path any other write can be
/// using**, so no number of concurrent refusals — one, three, a thousand, staggered any way
/// the scheduler likes — can interfere with a live write. A rollback that also removed the
/// (shared) chunk directory could strip it from under a concurrent live writer between its
/// `create_dir_all` and its data write, failing a *live* write on behalf of an *expired* one;
/// bounding that with N create retries only moves the failure to N+1 racing refusals, which
/// is a margin, not a fix. Not writing the shared path removes the race instead of racing it.
///
/// The directory an expired write may leave behind is empty, inert and pre-existing in kind:
/// the store already leaves one behind whenever a data write or a rename fails
/// (`put_fragment`) and whenever [`ChunkStore::delete_fragment`] reclaims a chunk's last
/// fragment. It is not a fragment — `get_fragment` reads `None` through it and
/// `list_fragments` cannot see it (it parses `.frag`) — so it is not part of the postcondition
/// this function establishes. Empty chunk directories are collected by
/// [`FsChunkStore::reap_write_residue`] at `open`, the one point where no write of this store
/// is in flight and the collection therefore cannot race anybody.
fn restore_pre_write_state(scratch: &Path) -> std::io::Result<()> {
    match fs::remove_file(scratch) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Run `f`'s blocking filesystem work **off the async reactor**.
///
/// The [`ChunkStore`] methods are `async` but their bodies are synchronous
/// `std::fs` syscalls. On the d-server's multi-threaded tokio runtime
/// (`crates/server/src/cli.rs`, `new_multi_thread().enable_all()`) those
/// syscalls would otherwise execute on a **reactor worker thread**, pinning it
/// for the whole syscall and starving every other task on that runtime —
/// including the lease-renew heartbeat, whose missed tick past the lease TTL
/// drops the server out of discovery (issue #204). Moving the work to tokio's
/// blocking pool keeps the reactor — its accept loop, its timers, and every
/// other in-flight task — schedulable independent of how many storage syscalls
/// are in flight.
///
/// Runtime-agnostic by design (ADR-0009): the offload engages only when a tokio
/// runtime is actually hosting the call. Driven off a tokio runtime (e.g. a
/// `pollster::block_on` test) there is no reactor worker thread to starve, so the
/// work runs inline — preserving the store's executor-independence rather than
/// hard-wiring it to tokio.
#[cfg(not(madsim))]
async fn offload<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::spawn_blocking(f)
            .await
            .expect("storage blocking task panicked"),
        Err(_) => f(),
    }
}

/// Under madsim (ADR-0009) the simulator is single-threaded and deterministic and
/// owns its own clock; offloading to a real OS thread would break seed
/// reproducibility, so the blocking work runs inline on the simulated thread —
/// exactly the pre-#204 behaviour, which is already non-starving there because the
/// simulator has no separate reactor to block.
#[cfg(madsim)]
async fn offload<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    f()
}

#[async_trait]
impl<C: Clock + Send + Sync + 'static> ChunkStore for FsChunkStore<C> {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        // Verify integrity and that the fragment belongs under this id before
        // acknowledging the write. A verify failure here is a **malformed fragment
        // the caller offered** — surfaced as a seam-level `IntegrityFault` so the
        // networked seam can classify it as a client (invalid-argument) fault, not
        // a server-internal one that invites futile retries.
        Self::verify(id, fragment.as_ref()).map_err(|e| IntegrityFault {
            id,
            detail: e.to_string(),
        })?;

        // **Fast-path refusal** (issue #638): a write already past its deadline on
        // arrival costs no I/O and leaves not even a chunk directory behind. It is an
        // optimisation, *not* the bound — everything after it (the blocking pool's
        // queue, the chunk-directory creation, the scratch write) can consume arbitrary
        // time. The bound is the pre-publication verdict inside the closure below.
        if let Some(deadline_millis) = deadline_millis {
            if let Some(refusal) =
                WriteDeadlineExpired::if_elapsed(id, deadline_millis, self.clock.now_millis())
            {
                return Err(Box::new(refusal));
            }
        }

        // Compute the paths on the reactor (no I/O — a per-call atomic bump and
        // path joins), then perform every blocking syscall off the reactor.
        let final_path = self.fragment_path(id);
        let temp = self.temp_path(id);
        // The store's clock travels into the blocking closure so the deadline is
        // judged *there*, immediately before the publish point (issue #638) — the same
        // clock this lifecycle read above, never a second time source (AGENTS.md
        // § Review rubric, "one clock per correctness lifecycle").
        let clock = Arc::clone(&self.clock);
        offload(move || -> Result<()> {
            // Write to a per-call private scratch file, then atomically rename it
            // onto the final path: the rename is the only publish point, so a
            // concurrent same-id write can neither observe nor clobber our partial
            // bytes and last-writer-wins is a no-op (same id ⇒ identical bytes). On
            // a failed write/rename we remove our *own* scratch (its name is
            // unique, so this never touches a concurrent write's file); a hard
            // crash before the rename leaves it for `reap_write_residue` to clear at
            // the next open.
            //
            // The chunk directory exists after the first fragment of the chunk, so
            // attempt the scratch write straight away and create the directory only
            // on the genuine first-fragment `NotFound` — sparing the steady-state
            // write the per-call `create_dir_all` stat (issue #204).
            //
            // One recovery is enough, and issue #638's refusals do not change that: a
            // refused write's rollback removes only its own private scratch and never the
            // shared chunk directory (`restore_pre_write_state`), so no concurrent refusal
            // can take this directory away between the `create_dir_all` and the write below.
            // A `NotFound` that survives the create is therefore a real I/O fault and
            // surfaces as one, rather than being retried against.
            match fs::write(&temp, &fragment) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    if let Some(chunk_dir) = final_path.parent() {
                        fs::create_dir_all(chunk_dir)?;
                    }
                    if let Err(e) = fs::write(&temp, &fragment) {
                        let _ = fs::remove_file(&temp);
                        return Err(e.into());
                    }
                }
                Err(e) => {
                    let _ = fs::remove_file(&temp);
                    return Err(e.into());
                }
            }
            // **THE VERDICT** (issue #638, proposal 0016 decision 5). Every segment of
            // this write that can consume unbounded time is now *behind* us — the D
            // server's accept queue, the blocking pool's queue, the chunk-directory
            // creation, and the fragment's data write. Ahead of us is `fs::rename`
            // alone: the single atomic step that publishes, on a file that is already
            // written, within one directory. So this reading is the latest instant at
            // which the store can still *refuse*, and it is the bound: a write that sat
            // anywhere upstream is **refused rather than queued** (`0016:1560`), and its
            // scratch goes with it, so nothing was ever published.
            //
            // The refusal is deliberately **before** the publication and not after it, and
            // that ordering is what makes `WriteEffect::NotApplied` *true*:
            //
            // * A refusal here has published nothing, so there is no reader-visible
            //   interval and no crash residue. Kill the process at any point in this
            //   closure and the store holds either the pre-write state or (before the
            //   rename) a scratch file `reap_write_residue` clears at the next open —
            //   never a fragment whose write was refused.
            // * Deciding to *retract* after the rename could not achieve that. The store
            //   would have to unlink the published file — not atomic with the rename, so a
            //   crash in between leaves exactly the bytes the refusal claims do not exist,
            //   and an unlink by path can destroy a concurrent same-id writer's
            //   already-acknowledged fragment. (The post-rename check below therefore
            //   *reports* rather than retracts: a different verdict, not a late refusal.)
            //
            // What this verdict does *not* establish is that the `rename` below returns
            // before the deadline — a syscall's own latency is not the caller's to control,
            // and on a hung device (the NAS profile) it can straddle the deadline. That is
            // checked separately, after the rename, and is why `Ok(())` from this store
            // means "published strictly before the deadline" rather than "publication
            // started in time".
            if let Some(deadline_millis) = deadline_millis {
                if let Some(refusal) =
                    WriteDeadlineExpired::if_elapsed(id, deadline_millis, clock.now_millis())
                {
                    // A refusal must take its own bytes back off the disk, and it must say
                    // so honestly: if the rollback fails, the caller gets the **backend
                    // fault** rather than the definite "nothing landed" verdict, because
                    // `WriteEffect::NotApplied` over leftover scratch is a silent skip
                    // (AGENTS.md § Review rubric, *Absent or unsupported entries*) and the
                    // residue then accumulates unseen. It takes back *only* its own private
                    // scratch: a refusal never touches the shared chunk directory, so it can
                    // never knock over a concurrent live writer creating it.
                    restore_pre_write_state(&temp)
                        .map_err(|source| FsChunkStoreError::RefusalNotRolledBack { id, source })?;
                    return Err(Box::new(refusal));
                }
            }

            if let Err(e) = fs::rename(&temp, &final_path) {
                let _ = fs::remove_file(&temp);
                return Err(e.into());
            }

            // **THE PUBLICATION IS VERIFIED, NOT ASSUMED** (issue #638). `rename(2)` is the
            // one step of this write whose duration the store cannot bound: it admits no
            // predicate, it cannot be cancelled, and on a hung device it can return long
            // after the verdict above was taken. One more read of the *same* clock is what
            // keeps `Ok(())` from asserting only that publication *began* in time — exactly
            // the "bounds acceptance, not effect" gap 0016 rejects for caller-side timeouts
            // (`0016:1557-1564`), one layer down. So `Ok(())` here means the store *saw* a
            // reading inside the window with the fragment already published.
            //
            // The failing side is reported as `WriteEffect::Unknown`, not as a late landing,
            // and the distinction is the point: this reading dates the *read*, not the
            // syscall — a `rename` that returned comfortably in time followed by a
            // descheduled thread produces exactly the same evidence as one that overran. The
            // store therefore says what it knows (it could not verify the timing) instead of
            // asserting the worse reading, and the caller's remedy is a re-read.
            //
            // The bytes stay where they landed. Unlinking them would be worse in both
            // directions: it is not atomic with the rename, so a crash in between leaves the
            // very bytes a retraction claims to have removed; and it deletes by *path*, so a
            // concurrent same-id writer that published in between loses an already
            // acknowledged fragment. Bytes that did land late are garbage their position's
            // evidence covers (`0016:1547-1550`); an erased live fragment is unrecoverable.
            if let Some(deadline_millis) = deadline_millis {
                if let Some(unverified) = WriteDeadlineExpired::if_publication_unverified(
                    id,
                    deadline_millis,
                    clock.now_millis(),
                ) {
                    return Err(Box::new(unverified));
                }
            }
            Ok(())
        })
        .await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        let path = self.fragment_path(id);
        let bytes = match offload(move || -> Result<Option<Vec<u8>>> {
            match fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                // A missing chunk directory or a missing file both read as not-found.
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?
        {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        // Detect bit-rot / tampering before returning data. A verify failure on the
        // read path is **stored-data corruption** — surfaced as a seam-level
        // `IntegrityFault` so a consumer (scrub, the read path) records a repair
        // obligation and carries on, rather than retrying bytes that cannot heal.
        // The corrupt bytes are never returned as a valid fragment.
        Self::verify(id, &bytes).map_err(|e| IntegrityFault {
            id,
            detail: e.to_string(),
        })?;
        Ok(Some(Bytes::from(bytes)))
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        // The on-disk layout is `root/<32-hex chunk>/<05-index>.frag`, so a walk
        // of two directory levels recovers exactly the placed fragment ids — the
        // inverse of `fragment_path`. Names that don't match (e.g. a `.tmp` from
        // an interrupted put, or any foreign entry) are skipped, so a crash mid
        // write never surfaces as a phantom fragment.
        // The O(N) walk is the worst worker-thread-starvation source (issue
        // #204), so the whole two-level directory walk runs off the reactor.
        let root = self.root.clone();
        offload(move || -> Result<Vec<FragmentId>> {
            let mut ids = Vec::new();
            let chunk_dirs = match fs::read_dir(&root) {
                Ok(dirs) => dirs,
                // A never-written store has no root contents yet — an empty walk.
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(ids),
                Err(e) => return Err(e.into()),
            };
            for chunk_entry in chunk_dirs {
                let chunk_entry = chunk_entry?;
                if !chunk_entry.file_type()?.is_dir() {
                    continue;
                }
                let Some(chunk) = chunk_entry
                    .file_name()
                    .to_str()
                    .and_then(parse_chunk_dir_name)
                else {
                    continue;
                };
                for frag_entry in fs::read_dir(chunk_entry.path())? {
                    let frag_entry = frag_entry?;
                    if let Some(index) = frag_entry
                        .file_name()
                        .to_str()
                        .and_then(parse_fragment_file_name)
                    {
                        ids.push(FragmentId { chunk, index });
                    }
                }
            }
            Ok(ids)
        })
        .await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        let path = self.fragment_path(id);
        offload(move || -> Result<()> {
            // Idempotent: a missing file is a successful no-op, so a retried GC
            // reclaim never errors.
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    async fn health(&self) -> Result<Health> {
        let root = self.root.clone();
        Ok(offload(move || match fs::metadata(&root) {
            Ok(meta) if meta.is_dir() => Health::Healthy,
            _ => Health::Unhealthy,
        })
        .await)
    }
}

/// A single on-disk store is its own location authority: it holds every fragment
/// addressed by `FragmentId`, so it is a single-D-server [`PlacementChunkStore`] and
/// uses the trait's identity defaults (the placement record is advisory here — the
/// store routes by `FragmentId`). Proposal 0005, M3.1.
impl<C: Clock + Send + Sync + 'static> PlacementChunkStore for FsChunkStore<C> {}

/// Errors specific to the filesystem chunk store; surfaced through the trait's
/// boxed error.
#[derive(Debug)]
pub enum FsChunkStoreError {
    /// The bytes on disk (or offered) are not a valid fragment.
    NotAFragment(FragmentError),
    /// The fragment's header records a different chunk id or fragment index than
    /// the one it is filed under — a misplaced or tampered fragment.
    IdMismatch {
        /// The id the store was asked for.
        expected: FragmentId,
        /// The id recorded in the fragment header.
        found: FragmentId,
    },
    /// A write was refused as past its authorization deadline (issue #638) but the store
    /// could **not** put itself back the way the write found it — the scratch file, or a
    /// chunk directory the write created, is still there.
    ///
    /// This is deliberately **not** a `wyrd_traits::WriteDeadlineExpired`: that class
    /// promises "nothing of this write is on the store", which is exactly what did not
    /// happen here. Returning it anyway would report a clean refusal over residue — a
    /// silent skip that lets the litter accumulate unnoticed — so the caller gets a backend
    /// fault instead, and `wyrd_traits::is_write_deadline_expired` says `false` for it.
    RefusalNotRolledBack {
        /// The fragment write that was refused.
        id: FragmentId,
        /// Why the rollback failed.
        source: std::io::Error,
    },
}

impl fmt::Display for FsChunkStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FsChunkStoreError::NotAFragment(e) => write!(f, "not a valid fragment: {e}"),
            FsChunkStoreError::IdMismatch { expected, found } => write!(
                f,
                "fragment id mismatch: filed under chunk {:032x} index {} but header says \
                 chunk {:032x} index {}",
                expected.chunk, expected.index, found.chunk, found.index
            ),
            FsChunkStoreError::RefusalNotRolledBack { id, source } => write!(
                f,
                "fragment write for chunk {:032x} index {} was past its authorization \
                 deadline, but the store could not remove what it had already written: \
                 {source}. The write was NOT published, and scratch may remain on disk — \
                 this is a backend fault, not the clean deadline refusal",
                id.chunk, id.index
            ),
        }
    }
}

impl std::error::Error for FsChunkStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FsChunkStoreError::NotAFragment(e) => Some(e),
            FsChunkStoreError::IdMismatch { .. } => None,
            FsChunkStoreError::RefusalNotRolledBack { source, .. } => Some(source),
        }
    }
}

/// The path a fragment for `id` would occupy under `root`. Exposed so tests (and
/// a future scrubber) can locate a fragment on disk.
pub fn fragment_path(root: &Path, id: FragmentId) -> PathBuf {
    root.join(format!("{:032x}", id.chunk))
        .join(format!("{:05}.frag", id.index))
}

/// Recover a chunk id from a chunk directory name, inverting the `{:032x}` in
/// [`fragment_path`]. `None` for any name that is not exactly 32 lowercase-hex
/// digits, so a foreign directory is skipped by the walk rather than misread.
fn parse_chunk_dir_name(name: &str) -> Option<ChunkId> {
    if name.len() != 32 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    ChunkId::from_str_radix(name, 16).ok()
}

/// Recover a fragment index from a fragment file name, inverting the
/// `{:05}.frag` in [`fragment_path`]. `None` for anything not ending `.frag`
/// with a `u16` stem — notably the `.tmp` of an interrupted put.
fn parse_fragment_file_name(name: &str) -> Option<u16> {
    name.strip_suffix(".frag")?.parse().ok()
}

/// Name of a write's private scratch file: the `.tmp` sibling of the
/// `<index>.frag` it will be renamed onto. `seq` (a per-store sequence) makes it
/// **unique per write**, so two concurrent writes of one [`FragmentId`] never
/// share a scratch path (issue #203). The `.tmp` suffix keeps it out of
/// `list_fragments` (which parses only `.frag`) and reapable by
/// [`is_temp_scratch_name`].
fn scratch_file_name(index: u16, seq: u64) -> String {
    format!("{index:05}.{seq}.tmp")
}

/// Whether a directory-entry name is a write's private scratch file — the
/// `.tmp` sibling of an `<index>.frag` publish. Matched by suffix so both the
/// per-write `<index>.<seq>.tmp` scheme and any legacy `<index>.tmp` are reaped,
/// and never a real `.frag`.
fn is_temp_scratch_name(name: &std::ffi::OsStr) -> bool {
    name.to_str().is_some_and(|n| n.ends_with(".tmp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    /// `:212` `|| -> &&` — a chunk directory name is a chunk only when it is BOTH
    /// exactly 32 chars AND all hex. With `&&`, a name that fails only the length
    /// test (a short all-hex name) is no longer rejected up front, and
    /// `from_str_radix` happily parses it — so a 3-char hex directory would be
    /// misread as a chunk. Pin the short-hex name to `None`.
    #[test]
    fn parse_chunk_dir_name_requires_full_width_and_hex() {
        assert!(
            parse_chunk_dir_name(&"a".repeat(32)).is_some(),
            "exactly 32 hex digits is a valid chunk dir"
        );
        assert_eq!(
            parse_chunk_dir_name("abc"),
            None,
            "a short all-hex name is not a chunk dir"
        );
        assert_eq!(
            parse_chunk_dir_name(&"z".repeat(32)),
            None,
            "32 non-hex chars are not a chunk dir"
        );
    }

    /// `:109` `== -> !=` — `list_fragments` treats a MISSING root as an empty walk
    /// (a never-written or removed store lists nothing), and only `NotFound`.
    /// Flipping `==` to `!=` turns an absent root into a propagated error.
    #[test]
    fn list_fragments_on_an_absent_root_is_empty_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("store");
        let store = FsChunkStore::open(&root).unwrap();
        std::fs::remove_dir_all(&root).unwrap();

        let listed = block_on(store.list_fragments()).unwrap();
        assert!(
            listed.is_empty(),
            "a store whose root is absent lists nothing rather than erroring"
        );
    }

    /// Per-write scratch privacy is **structural**, not timing-dependent (issue
    /// #203): distinct sequence values yield distinct scratch names, so no two
    /// writes through one store ever name the same scratch path — independent of
    /// any interleaving. The scratch name is also never mistaken for a published
    /// fragment (`list_fragments` skips it) yet is recognised as reapable.
    #[test]
    fn scratch_names_are_unique_per_seq_and_invisible_to_listing() {
        let a = scratch_file_name(7, 0);
        let b = scratch_file_name(7, 1);
        assert_ne!(a, b, "a different sequence is a different scratch path");
        assert_eq!(
            parse_fragment_file_name(&a),
            None,
            "scratch is never listed as a fragment"
        );
        assert!(
            is_temp_scratch_name(std::ffi::OsStr::new(&a)),
            "scratch is recognised for reaping"
        );
        // The published name it will be renamed onto is a real fragment.
        assert_eq!(parse_fragment_file_name("00007.frag"), Some(7));
    }

    /// `:194` `source -> None` — the error source must expose the wrapped
    /// `FragmentError` so the error chain stays walkable.
    #[test]
    fn not_a_fragment_error_exposes_its_source() {
        let err = FsChunkStoreError::NotAFragment(FragmentError::BadMagic);
        assert!(
            std::error::Error::source(&err).is_some(),
            "NotAFragment carries its FragmentError as the error source"
        );
    }
}
