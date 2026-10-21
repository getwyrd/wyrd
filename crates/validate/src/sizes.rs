//! Size classes, computed from the deployment's tuning (proposal 0017 §4).
//!
//! The sizes a run writes straddle S3's boundaries and Wyrd's own. Wyrd's depend on two
//! values a blackbox tool cannot import (proposal 0017 §9): the chunk size `C` and the root
//! value budget, from which the chunk-map segmentation threshold `T` follows. So they are
//! inputs — `--chunk-size`, `--max-root-value-bytes`, `--segment-threshold-bytes` — and the
//! table is computed from them at run time, never copied from today's constants. A table copied
//! from them goes stale the day one moves, and the run then reports coverage of a boundary it
//! never crossed.
//!
//! Each input is optional, with a default that mirrors today's server value; the resolved
//! configuration echo records each value with where it came from ([`Source`]), so a run's size
//! coverage reads against the tuning it actually used. A tuning no deployment can have is
//! refused by flag name, never clamped to one that can ([`TuningError`]): a chunk size outside
//! the range the server's `s3` role accepts ([`MIN_CHUNK_SIZE`] to [`MAX_CHUNK_SIZE`]), a root
//! budget of zero or above the value ceiling every backend inherits
//! ([`MAX_ROOT_VALUE_BYTES_CAP`]), a threshold below one chunk or not a whole number of chunks,
//! a budget too small to derive a threshold from (when none is given), and a size the table
//! needs that does not fit in 64 bits.
//!
//! The defaults and the range mirror, they do not import: a dev-only test
//! (`crates/validate/tests/payload_and_sizes.rs`) compares the root budget and the
//! chunk-record width with the `wyrd-core` constants, the chunk size with the single-PUT limit
//! a default server gateway refuses with, and the chunk-size range with the bounds the server's
//! `s3` role names when it refuses a chunk size; it goes red the day either side moves alone.
//!
//! Which verb drives a class, and what each class is expected to answer, is the capability
//! matrix's business (#743), not this module's.

use std::fmt;

/// Today's chunk size, mirroring the server's `DEFAULT_CHUNK_SIZE = 1 << 20`
/// (`crates/server/src/lib.rs`). That constant is private, so the dev-only test reads it off a
/// default gateway instead: the single-PUT limit such a gateway refuses with is
/// `⌊root budget / CHUNK_REF_BYTES⌋` (158) chunks of its chunk size.
pub const DEFAULT_CHUNK_SIZE: u64 = 1 << 20;

/// The smallest chunk size the server's `s3` role accepts, mirroring its `MIN_CHUNK_SIZE`
/// (`crates/server/src/lib.rs`): 1 MiB, inclusive. Below it a gateway would refuse objects the
/// default gateway stores, so no deployment runs one. Private there too, like the default; the
/// dev-only test reads both bounds off the refusal the role gives a chunk size outside them.
pub const MIN_CHUNK_SIZE: u64 = 1 << 20;

/// The largest chunk size the server's `s3` role accepts, mirroring its `MAX_CHUNK_SIZE`
/// (`crates/server/src/lib.rs`): 16 MiB, inclusive, set by the D-server transport's message
/// limit.
pub const MAX_CHUNK_SIZE: u64 = 16 << 20;

/// Today's root value budget, mirroring `wyrd_core::metadata::MAX_ROOT_VALUE_BYTES`.
pub const DEFAULT_MAX_ROOT_VALUE_BYTES: u64 = 50_000;

/// The largest root value budget a deployment can have, mirroring the same
/// `wyrd_core::metadata::MAX_ROOT_VALUE_BYTES`: half the 100,000-byte value limit every backend
/// inherits from FoundationDB, the tightest in play (`crates/core/src/metadata.rs`). A bigger
/// budget needs that limit raised, so no deployment runs one; the dev-only test fails the day
/// the constant moves without this cap. Making the budget deployment configuration on the
/// server side is #739's business, and the cap moves with it.
pub const MAX_ROOT_VALUE_BYTES_CAP: u64 = 50_000;

/// The widest encoding of one chunk record in a chunk map, mirroring
/// `wyrd_core::multipart::MAX_CHUNKREF_BYTES`. Not a flag: it is an internal detail of the
/// server's encoding, which is why the segmentation threshold has its own flag.
pub const CHUNK_REF_BYTES: u64 = 315;

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

// Tied at compile time, the way the server ties its own (`crates/server/src/lib.rs`): the
// default inside the range, and both sub-chunk sizes below every chunk size this module
// accepts — which is what makes the `SubChunk` label true in every table it produces. Lowering
// the floor past 64 KiB fails the build here, so whoever does it decides what to call those
// sizes then.
const _: () = assert!(MIN_CHUNK_SIZE <= DEFAULT_CHUNK_SIZE && DEFAULT_CHUNK_SIZE <= MAX_CHUNK_SIZE);
const _: () = assert!(64 * KIB < MIN_CHUNK_SIZE);
// The default budget holds a chunk record and sits inside the cap, so the tuning with no flag
// given is one `resolve` accepts.
const _: () = assert!(
    CHUNK_REF_BYTES <= DEFAULT_MAX_ROOT_VALUE_BYTES
        && DEFAULT_MAX_ROOT_VALUE_BYTES <= MAX_ROOT_VALUE_BYTES_CAP
);

/// Where a tuning value came from, as the resolved-configuration echo prints it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The operator passed the flag.
    Given,
    /// The flag was absent; the value mirrors today's server value.
    Default,
    /// The flag was absent; the value was computed from the other tuning values.
    Derived,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Given => "given",
            Self::Default => "default",
            Self::Derived => "derived",
        })
    }
}

/// One resolved tuning value and its [`Source`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting {
    pub value: u64,
    pub source: Source,
}

/// The tuning flags as given on the command line: `None` for an absent flag. The argument
/// parser refuses a zero or a non-integer before a value gets here; a zero in a `TuningFlags`
/// built by hand is refused by [`Tuning::resolve`] all the same, as a value no deployment has.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TuningFlags {
    pub chunk_size: Option<u64>,
    pub max_root_value_bytes: Option<u64>,
    pub segment_threshold_bytes: Option<u64>,
}

/// The deployment's tuning, resolved: every value present, each with its [`Source`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuning {
    /// `C`: the chunk size, in bytes; [`MIN_CHUNK_SIZE`] to [`MAX_CHUNK_SIZE`].
    pub chunk_size: Setting,
    /// The root value budget, in bytes; 1 to [`MAX_ROOT_VALUE_BYTES_CAP`].
    pub max_root_value_bytes: Setting,
    /// `T`: the largest object size whose chunk map is still flat, in bytes — a whole number of
    /// chunks, at least one. `T + 1` is the first size that is not (it segments, or a single
    /// PUT of it is refused).
    pub segment_threshold_bytes: Setting,
}

/// Why a tuning was refused: no deployment can have it, or it cannot produce a valid size
/// table. Never clamped to one that can. Every variant names the flag or flags it is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuningError {
    /// A chunk size outside [`MIN_CHUNK_SIZE`] to [`MAX_CHUNK_SIZE`], the range the server's
    /// `s3` role accepts.
    ChunkSizeOutOfRange { chunk_size: u64 },
    /// A root budget of zero, or above [`MAX_ROOT_VALUE_BYTES_CAP`], the value ceiling every
    /// backend inherits. Refused whether or not a threshold is given: no deployment has it.
    RootBudgetOutOfRange { max_root_value_bytes: u64 },
    /// The root budget holds no chunk record at all, so no segmentation threshold can be
    /// derived from it.
    NoChunkRefFits { max_root_value_bytes: u64 },
    /// A segmentation threshold below one chunk. The largest flat object holds at least one
    /// whole chunk, so no deployment has this threshold.
    ThresholdBelowChunk { threshold: u64, chunk_size: u64 },
    /// A segmentation threshold that is not a whole number of chunks. A flat chunk map names
    /// whole chunks, so the largest flat object is a whole number of them; a threshold that is
    /// not puts `T` and `T + 1` in maps of the same size, straddling nothing.
    ThresholdNotWholeChunks { threshold: u64, chunk_size: u64 },
    /// A size the table needs does not fit in 64 bits: `class` is the size (e.g. `T + 1`),
    /// `from` the flag or flags its value is computed from, so the refusal names them.
    Overflow {
        class: &'static str,
        from: &'static str,
    },
}

/// What the chunk-size classes are computed from.
const FROM_CHUNK_SIZE: &str = "`--chunk-size`";
/// What a given segmentation threshold, and the classes around it, are computed from.
const FROM_GIVEN_THRESHOLD: &str = "`--segment-threshold-bytes`";
/// What a derived segmentation threshold, and the classes around it, are computed from.
const FROM_DERIVED_THRESHOLD: &str =
    "`--max-root-value-bytes` and `--chunk-size` (no `--segment-threshold-bytes` was given)";

impl fmt::Display for TuningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChunkSizeOutOfRange { chunk_size } => write!(
                f,
                "`--chunk-size` {chunk_size} is outside the range the server's `s3` role \
                 accepts: {MIN_CHUNK_SIZE} to {MAX_CHUNK_SIZE} bytes ({} to {} MiB), inclusive",
                MIN_CHUNK_SIZE >> 20,
                MAX_CHUNK_SIZE >> 20
            ),
            Self::RootBudgetOutOfRange {
                max_root_value_bytes,
            } => write!(
                f,
                "`--max-root-value-bytes` {max_root_value_bytes} is outside the range a deployment \
                 can have: 1 to {MAX_ROOT_VALUE_BYTES_CAP} bytes, inclusive (half the 100,000-byte \
                 value limit every backend inherits)"
            ),
            Self::NoChunkRefFits {
                max_root_value_bytes,
            } => write!(
                f,
                "`--max-root-value-bytes` {max_root_value_bytes} holds no {CHUNK_REF_BYTES}-byte \
                 chunk record, so no segmentation threshold can be derived from it; pass \
                 `--segment-threshold-bytes` or a larger budget"
            ),
            Self::ThresholdBelowChunk {
                threshold,
                chunk_size,
            } => write!(
                f,
                "`--segment-threshold-bytes` {threshold} is below `--chunk-size` {chunk_size}: \
                 the largest flat object holds at least one whole chunk"
            ),
            Self::ThresholdNotWholeChunks {
                threshold,
                chunk_size,
            } => write!(
                f,
                "`--segment-threshold-bytes` {threshold} is not a whole number of `--chunk-size` \
                 {chunk_size} chunks: a flat chunk map names whole chunks, so the largest flat \
                 object is a whole number of them"
            ),
            Self::Overflow { class, from } => write!(
                f,
                "the {class} size class, computed from {from}, does not fit in 64 bits"
            ),
        }
    }
}

impl std::error::Error for TuningError {}

/// The segmentation threshold the server's rule gives for `chunk_size` and
/// `max_root_value_bytes`: a flat chunk map holds `⌊max_root_value_bytes / CHUNK_REF_BYTES⌋`
/// chunks, and a larger one segments, so `T = ⌊max_root_value_bytes / CHUNK_REF_BYTES⌋ ×
/// chunk_size` (158 MiB at the defaults).
pub fn derived_segment_threshold(
    chunk_size: u64,
    max_root_value_bytes: u64,
) -> Result<u64, TuningError> {
    let chunks = max_root_value_bytes / CHUNK_REF_BYTES;
    if chunks == 0 {
        return Err(TuningError::NoChunkRefFits {
            max_root_value_bytes,
        });
    }
    chunks.checked_mul(chunk_size).ok_or(TuningError::Overflow {
        class: "T (⌊root budget / 315⌋ × C)",
        from: FROM_DERIVED_THRESHOLD,
    })
}

/// Refuse a chunk size outside the range the server's `s3` role accepts.
fn check_chunk_size(chunk_size: u64) -> Result<(), TuningError> {
    if (MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
        Ok(())
    } else {
        Err(TuningError::ChunkSizeOutOfRange { chunk_size })
    }
}

/// Refuse a root budget no deployment can have: zero, or above the value ceiling.
fn check_root_budget(max_root_value_bytes: u64) -> Result<(), TuningError> {
    if (1..=MAX_ROOT_VALUE_BYTES_CAP).contains(&max_root_value_bytes) {
        Ok(())
    } else {
        Err(TuningError::RootBudgetOutOfRange {
            max_root_value_bytes,
        })
    }
}

impl Tuning {
    /// Resolve the flags: a given value is used as given, an absent chunk size or root budget
    /// takes its default, and an absent segmentation threshold is derived from the other two.
    /// Refused if no deployment can have the result or it cannot produce a valid size table.
    pub fn resolve(flags: &TuningFlags) -> Result<Self, TuningError> {
        let setting = |given: Option<u64>, default: u64| match given {
            Some(value) => Setting {
                value,
                source: Source::Given,
            },
            None => Setting {
                value: default,
                source: Source::Default,
            },
        };
        let chunk_size = setting(flags.chunk_size, DEFAULT_CHUNK_SIZE);
        // Before the threshold is derived from it, so a chunk size no deployment has is
        // refused as that, not as a threshold that does not fit.
        check_chunk_size(chunk_size.value)?;
        let max_root_value_bytes =
            setting(flags.max_root_value_bytes, DEFAULT_MAX_ROOT_VALUE_BYTES);
        // Likewise before the threshold is derived, and whether or not one is given: a budget
        // no deployment has is refused as that, not as one that derives nothing.
        check_root_budget(max_root_value_bytes.value)?;
        let segment_threshold_bytes = match flags.segment_threshold_bytes {
            Some(value) => Setting {
                value,
                source: Source::Given,
            },
            None => Setting {
                value: derived_segment_threshold(chunk_size.value, max_root_value_bytes.value)?,
                source: Source::Derived,
            },
        };
        let tuning = Self {
            chunk_size,
            max_root_value_bytes,
            segment_threshold_bytes,
        };
        tuning.size_classes()?;
        Ok(tuning)
    }

    /// `(flag, setting)` for each tuning flag, in the order the echo prints them.
    pub fn pairs(&self) -> [(&'static str, Setting); 3] {
        [
            ("chunk-size", self.chunk_size),
            ("max-root-value-bytes", self.max_root_value_bytes),
            ("segment-threshold-bytes", self.segment_threshold_bytes),
        ]
    }

    /// Refuse a tuning no deployment can have (see [`TuningError`]). Run before any size is
    /// computed, so a `Tuning` built by hand is held to the same rule as one
    /// [`Tuning::resolve`] produced.
    fn check(&self) -> Result<(), TuningError> {
        let chunk_size = self.chunk_size.value;
        check_chunk_size(chunk_size)?;
        check_root_budget(self.max_root_value_bytes.value)?;
        let threshold = self.segment_threshold_bytes.value;
        if threshold < chunk_size {
            return Err(TuningError::ThresholdBelowChunk {
                threshold,
                chunk_size,
            });
        }
        if !threshold.is_multiple_of(chunk_size) {
            return Err(TuningError::ThresholdNotWholeChunks {
                threshold,
                chunk_size,
            });
        }
        Ok(())
    }

    /// The size classes for this tuning: ascending, each size once, each carrying every
    /// boundary it straddles (two boundaries that land on one size share a class). Refused for
    /// a tuning no deployment can have ([`Tuning::check`]), so 4 KiB and 64 KiB are below `C` in
    /// every table and the chunk classes fit; checked arithmetic throughout all the same, and a
    /// threshold class that does not fit in 64 bits is refused.
    pub fn size_classes(&self) -> Result<Vec<SizeClass>, TuningError> {
        self.check()?;
        let c = self.chunk_size.value;
        let t = self.segment_threshold_bytes.value;
        let chunk = |class| TuningError::Overflow {
            class,
            from: FROM_CHUNK_SIZE,
        };
        let t_from = match self.segment_threshold_bytes.source {
            Source::Given => FROM_GIVEN_THRESHOLD,
            Source::Default | Source::Derived => FROM_DERIVED_THRESHOLD,
        };
        let threshold = |class| TuningError::Overflow {
            class,
            from: t_from,
        };
        let mut points: Vec<(u64, Boundary)> = vec![
            (0, Boundary::Empty),
            (1, Boundary::MinimumNonEmpty),
            (4 * KIB, Boundary::SubChunk),
            (64 * KIB, Boundary::SubChunk),
            (
                c.checked_sub(1).ok_or(chunk("C - 1"))?,
                Boundary::BelowChunk,
            ),
            (c, Boundary::AtChunk),
            (
                c.checked_add(1).ok_or(chunk("C + 1"))?,
                Boundary::AboveChunk,
            ),
            (5 * MIB, Boundary::S3MinimumPart),
            (8 * MIB, Boundary::SdkMultipartThreshold),
            (
                c.checked_mul(64).ok_or(chunk("64 x C"))?,
                Boundary::MultiChunk,
            ),
            (
                t.checked_sub(1).ok_or(threshold("T - 1"))?,
                Boundary::BelowSegmentation,
            ),
            (t, Boundary::AtSegmentation),
            (
                t.checked_add(1).ok_or(threshold("T + 1"))?,
                Boundary::AboveSegmentation,
            ),
            (5 * GIB, Boundary::S3MaxSinglePut),
            (5 * GIB + 1, Boundary::PastS3MaxSinglePut),
            (10 * GIB + 1, Boundary::BeyondTenGib),
        ];
        points.sort_unstable();
        let mut classes: Vec<SizeClass> = Vec::with_capacity(points.len());
        for (size, boundary) in points {
            match classes.last_mut() {
                Some(class) if class.size == size => {
                    if !class.boundaries.contains(&boundary) {
                        class.boundaries.push(boundary);
                    }
                }
                _ => classes.push(SizeClass {
                    size,
                    boundaries: vec![boundary],
                }),
            }
        }
        Ok(classes)
    }
}

/// The boundary a size class straddles (proposal 0017 §4's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Boundary {
    /// 0 B.
    Empty,
    /// 1 B.
    MinimumNonEmpty,
    /// 4 KiB and 64 KiB: inside one chunk, at every chunk size the tuning accepts (both are
    /// below [`MIN_CHUNK_SIZE`], tied at compile time).
    SubChunk,
    /// `C − 1`.
    BelowChunk,
    /// `C`.
    AtChunk,
    /// `C + 1`.
    AboveChunk,
    /// 5 MiB.
    S3MinimumPart,
    /// 8 MiB.
    SdkMultipartThreshold,
    /// `64 × C`.
    MultiChunk,
    /// `T − 1`.
    BelowSegmentation,
    /// `T`.
    AtSegmentation,
    /// `T + 1`.
    AboveSegmentation,
    /// 5 GiB.
    S3MaxSinglePut,
    /// 5 GiB + 1.
    PastS3MaxSinglePut,
    /// 10 GiB + 1, standing for the > 10 GiB launch requirement.
    BeyondTenGib,
}

impl Boundary {
    /// What the boundary is, in words.
    pub fn label(self) -> &'static str {
        match self {
            Self::Empty => "empty object",
            Self::MinimumNonEmpty => "minimum non-empty",
            Self::SubChunk => "sub-chunk",
            Self::BelowChunk => "one byte below the chunk size (C - 1)",
            Self::AtChunk => "the chunk size (C)",
            Self::AboveChunk => "one byte past the chunk size (C + 1)",
            Self::S3MinimumPart => "S3 minimum part size (5 MiB)",
            Self::SdkMultipartThreshold => {
                "stock SDK multipart threshold; bulk-delete body cap (8 MiB)"
            }
            Self::MultiChunk => "multi-chunk, many fragments (64 x C)",
            Self::BelowSegmentation => {
                "one byte below the chunk-map segmentation threshold (T - 1)"
            }
            Self::AtSegmentation => "the chunk-map segmentation threshold (T)",
            Self::AboveSegmentation => "one byte past the chunk-map segmentation threshold (T + 1)",
            Self::S3MaxSinglePut => "S3's largest accepted single PUT (5 GiB)",
            Self::PastS3MaxSinglePut => "first size past S3's single-PUT maximum (5 GiB + 1)",
            Self::BeyondTenGib => {
                "beyond 5 GiB: an object over 10 GiB, the launch requirement (10 GiB + 1)"
            }
        }
    }
}

impl fmt::Display for Boundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// One size a run writes, with every boundary it straddles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeClass {
    pub size: u64,
    pub boundaries: Vec<Boundary>,
}
