//! Filesystem-backed [`ChunkStore`]: the embedded "D server" for dev and the
//! NAS profile. Stores each fragment's bytes in one file named by chunk id and
//! verifies the fragment's self-describing checksums (`chunk-format`, ADR-0019)
//! on the way in and out.
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
use wyrd_traits::{ChunkId, ChunkStore, Health, Result};

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

    /// `root/<32-hex chunk_id>.frag` — one flat file per fragment.
    fn fragment_path(&self, id: ChunkId) -> PathBuf {
        self.root.join(format!("{id:032x}.frag"))
    }

    /// Sibling temp path used for the write-then-rename so a crash never leaves a
    /// half-written fragment visible.
    fn temp_path(&self, id: ChunkId) -> PathBuf {
        self.root.join(format!("{id:032x}.tmp"))
    }

    /// Verify the fragment decodes and that its header records the expected id.
    fn verify(id: ChunkId, bytes: &[u8]) -> std::result::Result<(), FsChunkStoreError> {
        let decoded = decode(bytes).map_err(FsChunkStoreError::NotAFragment)?;
        if decoded.header.chunk_id != id {
            return Err(FsChunkStoreError::IdMismatch {
                expected: id,
                found: decoded.header.chunk_id,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl ChunkStore for FsChunkStore {
    async fn put_fragment(&self, id: ChunkId, fragment: Bytes) -> Result<()> {
        // Verify integrity and that the fragment belongs under this id before
        // acknowledging the write.
        Self::verify(id, fragment.as_ref())?;

        let temp = self.temp_path(id);
        fs::write(&temp, &fragment)?;
        fs::rename(&temp, self.fragment_path(id))?;
        Ok(())
    }

    async fn get_fragment(&self, id: ChunkId) -> Result<Option<Bytes>> {
        let bytes = match fs::read(self.fragment_path(id)) {
            Ok(bytes) => bytes,
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
    /// The fragment's header records a different chunk id than the one it is
    /// filed under — a misplaced or tampered fragment.
    IdMismatch {
        /// The id the store was asked for.
        expected: ChunkId,
        /// The id recorded in the fragment header.
        found: ChunkId,
    },
}

impl fmt::Display for FsChunkStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FsChunkStoreError::NotAFragment(e) => write!(f, "not a valid fragment: {e}"),
            FsChunkStoreError::IdMismatch { expected, found } => write!(
                f,
                "fragment id mismatch: filed under {expected:032x} but header says {found:032x}"
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
pub fn fragment_path(root: &Path, id: ChunkId) -> PathBuf {
    root.join(format!("{id:032x}.frag"))
}
