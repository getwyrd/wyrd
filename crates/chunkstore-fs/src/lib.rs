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

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{decode, FragmentError};
use wyrd_traits::{ChunkStore, FragmentId, Health, Result};

/// A [`ChunkStore`] that keeps each fragment as a file under a root directory.
pub struct FsChunkStore {
    root: PathBuf,
}

impl FsChunkStore {
    /// Open a store rooted at `root`, creating the directory if it does not
    /// exist.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// `root/<32-hex chunk>/<05-index>.frag` — a directory per chunk, one file
    /// per fragment index.
    fn fragment_path(&self, id: FragmentId) -> PathBuf {
        fragment_path(&self.root, id)
    }

    /// Sibling temp path used for the write-then-rename so a crash never leaves a
    /// half-written fragment visible.
    fn temp_path(&self, id: FragmentId) -> PathBuf {
        self.root
            .join(format!("{:032x}", id.chunk))
            .join(format!("{:05}.tmp", id.index))
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
        let temp = self.temp_path(id);
        fs::write(&temp, &fragment)?;
        fs::rename(&temp, final_path)?;
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

    async fn health(&self) -> Result<Health> {
        Ok(match fs::metadata(&self.root) {
            Ok(meta) if meta.is_dir() => Health::Healthy,
            _ => Health::Unhealthy,
        })
    }
}

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
