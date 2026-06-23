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

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{decode, FragmentError};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health, PlacementChunkStore, Result};

/// A [`ChunkStore`] that keeps each fragment as a file under a root directory.
pub struct FsChunkStore {
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
}

impl FsChunkStore {
    /// Open a store rooted at `root`, creating the directory if it does not
    /// exist.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let store = Self {
            root,
            scratch_seq: AtomicU64::new(0),
        };
        // Clear write scratch orphaned by a crash before this store was opened.
        // Unique per-write scratch names (issue #203) no longer self-clean the
        // way a single fixed `<index>.tmp` did (the next write of the same id
        // overwrote it), so without reaping a hard crash would let them
        // accumulate as litter. Open is the safe place: one D server owns this
        // root (ADR-0034, Model A) and no write on this just-constructed store is
        // in flight yet, so reaping cannot race a live put's scratch.
        store.reap_stale_temps();
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
    /// [`Self::reap_stale_temps`].
    fn temp_path(&self, id: FragmentId) -> PathBuf {
        let seq = self.scratch_seq.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(format!("{:032x}", id.chunk))
            .join(scratch_file_name(id.index, seq))
    }

    /// Remove stale write scratch (`*.tmp`) left under chunk directories by a
    /// process that crashed mid-write, before the atomic rename published the
    /// fragment. Best-effort and only over recognised `<32-hex>` chunk dirs: an
    /// entry that cannot be read or removed is left in place (it is harmless —
    /// scratch is invisible to `list_fragments`, which parses only `.frag`).
    /// Called from `open`, where one D server owns the root (ADR-0034) and no
    /// write on this store is in flight, so it can never delete a concurrent
    /// put's in-flight scratch.
    fn reap_stale_temps(&self) {
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

#[async_trait]
impl ChunkStore for FsChunkStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        // Verify integrity and that the fragment belongs under this id before
        // acknowledging the write.
        Self::verify(id, fragment.as_ref())?;

        let final_path = self.fragment_path(id);
        // The chunk's directory may not exist yet (first fragment of the chunk).
        if let Some(chunk_dir) = final_path.parent() {
            fs::create_dir_all(chunk_dir)?;
        }
        // Write to a per-call private scratch file, then atomically rename it
        // onto the final path: the rename is the only publish point, so a
        // concurrent same-id write can neither observe nor clobber our partial
        // bytes and last-writer-wins is a no-op (same id ⇒ identical bytes). On
        // a failed write/rename we remove our *own* scratch (its name is unique,
        // so this never touches a concurrent write's file); a hard crash before
        // the rename leaves it for `reap_stale_temps` to clear at the next open.
        let temp = self.temp_path(id);
        if let Err(e) = fs::write(&temp, &fragment) {
            let _ = fs::remove_file(&temp);
            return Err(e.into());
        }
        if let Err(e) = fs::rename(&temp, &final_path) {
            let _ = fs::remove_file(&temp);
            return Err(e.into());
        }
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        let bytes = match fs::read(self.fragment_path(id)) {
            Ok(bytes) => bytes,
            // A missing chunk directory or a missing file both read as not-found.
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Detect bit-rot / tampering before returning data.
        Self::verify(id, &bytes)?;
        Ok(Some(Bytes::from(bytes)))
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        // The on-disk layout is `root/<32-hex chunk>/<05-index>.frag`, so a walk
        // of two directory levels recovers exactly the placed fragment ids — the
        // inverse of `fragment_path`. Names that don't match (e.g. a `.tmp` from
        // an interrupted put, or any foreign entry) are skipped, so a crash mid
        // write never surfaces as a phantom fragment.
        let mut ids = Vec::new();
        let chunk_dirs = match fs::read_dir(&self.root) {
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
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        // Idempotent: a missing file is a successful no-op, so a retried GC
        // reclaim never errors.
        match fs::remove_file(self.fragment_path(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn health(&self) -> Result<Health> {
        Ok(match fs::metadata(&self.root) {
            Ok(meta) if meta.is_dir() => Health::Healthy,
            _ => Health::Unhealthy,
        })
    }
}

/// A single on-disk store is its own location authority: it holds every fragment
/// addressed by `FragmentId`, so it is a single-D-server [`PlacementChunkStore`] and
/// uses the trait's identity defaults (the placement record is advisory here — the
/// store routes by `FragmentId`). Proposal 0005, M3.1.
impl PlacementChunkStore for FsChunkStore {}

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
        }
    }
}

impl std::error::Error for FsChunkStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FsChunkStoreError::NotAFragment(e) => Some(e),
            FsChunkStoreError::IdMismatch { .. } => None,
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
