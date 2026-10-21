//! Derived payloads, size classes and the deployment tuning flags (proposal 0017 §4; #744).
//!
//! Binds two properties a months-long run depends on:
//!
//! - **Payloads are derived, never stored**: `bytes = f(seed, key, size, generation)`, the same
//!   in every process (pinned known-answer digests), with every input mattering — the
//!   generation above all, or a stale read after a same-size overwrite looks correct — and any
//!   byte range computable on its own, in bounded memory, wherever it sits in the object.
//! - **Size classes come from the tuning**, never from a hardcoded table: they move with
//!   `--chunk-size`, `--max-root-value-bytes` and `--segment-threshold-bytes`, whose defaults
//!   are echoed as defaults and checked here against what they mirror: the `wyrd-core`
//!   constants, the server's default chunk size as a default gateway reports it, and the
//!   chunk-size range as the server's `s3` role names it when it refuses a chunk size. A tuning
//!   no deployment can have is refused by flag name, never clamped.
//!
//! Every test's whole body runs under a wall-clock bound ([`bounded`]), so no call into the
//! library is ever made outside one; (C′)'s direct-access checks run under a tighter bound of
//! their own ([`within`] [`DIRECT`]). Every stream is drained through [`drain`], which stops at
//! the first byte past the expected length. `cargo test` has no per-test timeout, so without
//! them a generator that never finishes (a stream that does not advance, a fill that does not
//! end, a range or a range's constructor that regenerates the prefix of a 1 TiB object) would
//! hang the run instead of failing a test.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::process::Command;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::cli::{serve_s3_role, CoordinationBackend, MetadataBackend};
use wyrd_server::{Gateway, GatewayError};
use wyrd_validate::sizes::derived_segment_threshold;
use wyrd_validate::{
    run, Boundary, OutOfRange, Payload, Pieces, Setting, SizeClass, Source, Tuning, TuningError,
    TuningFlags, CHUNK_REF_BYTES, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_ROOT_VALUE_BYTES,
    EXIT_INCONCLUSIVE, EXIT_USAGE, MAX_CHUNK_SIZE, MAX_ROOT_VALUE_BYTES_CAP, MIN_CHUNK_SIZE,
    PIECE_BYTES, TUNING_FLAGS,
};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
const TIB: u64 = 1 << 40;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Run `work` on its own thread and fail if it has not finished within `limit` of wall time.
/// A panic inside `work` is raised again here, with its own message. Work that runs past the
/// limit is left running on its thread while this test fails; the test process ends it when
/// the run ends.
fn within<T: Send + 'static>(
    limit: Duration,
    what: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (done, finished) = mpsc::channel();
    let started = Instant::now();
    let worker = std::thread::spawn(move || {
        let value = work();
        // Nobody is listening if the limit has already passed; the value is dropped then.
        let _ = done.send(());
        value
    });
    match finished.recv_timeout(limit) {
        // Finished, or panicked (which drops `done` unsent): either way the thread is ending,
        // so the join returns promptly and a panic is raised again with its own message.
        Ok(()) | Err(RecvTimeoutError::Disconnected) => worker
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
        Err(RecvTimeoutError::Timeout) => panic!(
            "{what} did not finish within {limit:?} (gave up after {:?})",
            started.elapsed()
        ),
    }
}

/// The bound on every test body in this file. Honest work here takes milliseconds; this is far
/// above that and still below any harness timeout, so code that never finishes fails the test
/// that drove it.
const GUARD: Duration = Duration::from_secs(10);

/// A whole test body, run [`within`] [`GUARD`]. Every test in this file is one call of this.
fn bounded(test: &str, body: impl FnOnce() + Send + 'static) {
    within(GUARD, test, body);
}

/// Drain `pieces`, which must stream exactly `len` bytes, and check the [`Pieces`] contract on
/// the way: no piece is empty, every piece but the last is exactly [`PIECE_BYTES`] and the last
/// at most that, the total never passes `len` (so a stream that never ends fails at once,
/// holding no more than `len` bytes), and `size_hint` reports exactly the pieces still to
/// come, before every step and after the end.
fn drain(mut pieces: Pieces, len: u64) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    let mut piece_lens: Vec<usize> = Vec::new();
    let mut hints = vec![pieces.size_hint()];
    while let Some(piece) = pieces.next() {
        assert!(
            !piece.is_empty(),
            "an empty piece after {} bytes",
            bytes.len()
        );
        assert!(piece.len() <= PIECE_BYTES, "a {}-byte piece", piece.len());
        piece_lens.push(piece.len());
        bytes.extend_from_slice(&piece);
        assert!(
            bytes.len() as u64 <= len,
            "the stream passed its {len} bytes"
        );
        hints.push(pieces.size_hint());
    }
    assert_eq!(bytes.len() as u64, len, "the stream ended early");
    if let Some((_, full)) = piece_lens.split_last() {
        assert!(
            full.iter().all(|piece_len| *piece_len == PIECE_BYTES),
            "a short piece before the last: {piece_lens:?}"
        );
    }
    // Before item k of n, exactly n - k remain; after the end, none.
    let remaining: Vec<(usize, Option<usize>)> = (0..=piece_lens.len())
        .rev()
        .map(|left| (left, Some(left)))
        .collect();
    assert_eq!(hints, remaining, "size_hint against the pieces yielded");
    // An ended stream stays ended.
    assert!(pieces.next().is_none(), "a piece after the end");
    assert_eq!(pieces.size_hint(), (0, Some(0)));
    bytes
}

/// The whole payload, materialized through [`drain`]. Only for the small sizes these tests use.
fn whole(payload: &Payload) -> Vec<u8> {
    drain(payload.pieces(), payload.size())
}

/// `[start, start + len)` produced directly.
fn direct(payload: &Payload, start: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0; len];
    payload
        .fill(start, &mut buf)
        .unwrap_or_else(|e| panic!("fill: {e}"));
    buf
}

// ---- (A) Payload determinism: known answers --------------------------------------------------

/// `(seed, key, size, generation, SHA-256 of the payload)`. The digests were produced by a
/// separate process, and checked against an independent implementation of the construction
/// written from `src/payload.rs`'s module docs alone. Any input the generator takes from
/// process state breaks them; so does any change to the construction, which would silently
/// invalidate every checkpoint ever written.
const KNOWN_ANSWERS: [(&str, &str, u64, u64, &str); 6] = [
    (
        "424242",
        "kat/empty",
        0,
        0,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    ),
    (
        "424242",
        "kat/one",
        1,
        0,
        "1f184f101c67d585ba00d24cfde36863f747fbab9e84af7760ec771e9b36ca51",
    ),
    (
        "424242",
        "kat/block",
        32,
        3,
        "2e2ec4a04bbd9bedd9d0557cfbd8ae03a9a855ce49a407f494b1c145f138b0da",
    ),
    (
        "424242",
        "kat/4k",
        4096,
        1,
        "a2e4f16a4135b8f9d108370cbff07e956d716f0d2f4cc5a51ea7d1114f1bf8a3",
    ),
    (
        "seed-\u{fc}",
        "dir/obj with space",
        100_003,
        7,
        "863f70c69e72cb2ce14d4abe7100704f4fd06ccb5b2118e5908a1067c4572446",
    ),
    (
        "424242",
        "kat/multi-piece",
        3 * 64 * KIB + 5,
        2,
        "802a4f5a805613e4c78c7b325ddf5e554d30e06a376c5bfb0059cd8c919398aa",
    ),
];

#[test]
fn a_payload_digest_matches_its_known_answer() {
    bounded("a_payload_digest_matches_its_known_answer", || {
        for (seed, key, size, generation, expected) in KNOWN_ANSWERS {
            let payload = Payload::new(seed, key, size, generation);
            let bytes = whole(&payload);
            assert_eq!(
                hex(&Sha256::digest(&bytes)),
                expected,
                "({seed:?}, {key:?}, {size}, {generation})"
            );
            // The streaming digest the oracle and checkpoints key on is the digest of those
            // bytes.
            assert_eq!(
                hex(&payload.sha256()),
                expected,
                "streaming digest of ({seed:?}, {key:?}, {size}, {generation})"
            );
        }
    });
}

#[test]
fn a_payload_reports_the_size_and_generation_it_was_derived_for() {
    bounded(
        "a_payload_reports_the_size_and_generation_it_was_derived_for",
        || {
            let payload = Payload::new("424242", "a/inputs", 4096, 5);
            assert_eq!(payload.size(), 4096);
            assert_eq!(payload.generation(), 5);
            // Its debug form shows the two numbers and nothing of the keyed hash state.
            assert_eq!(
                format!("{payload:?}"),
                "Payload { size: 4096, generation: 5, .. }"
            );
        },
    );
}

// ---- (B) Every input matters, at sizes where it can -----------------------------------------

#[test]
fn changing_any_one_input_changes_the_bytes() {
    bounded("changing_any_one_input_changes_the_bytes", || {
        for size in [4 * KIB, 100_003] {
            let base = whole(&Payload::new("424242", "b/key", size, 5));
            let len = base.len();
            let variants = [
                ("seed", whole(&Payload::new("424243", "b/key", size, 5))),
                ("key", whole(&Payload::new("424242", "b/key2", size, 5))),
                // The same-size overwrite: without the generation in the derivation it re-sends
                // identical bytes, and a read of the prior generation would look correct.
                (
                    "generation",
                    whole(&Payload::new("424242", "b/key", size, 6)),
                ),
            ];
            for (input, bytes) in variants {
                assert_eq!(bytes.len(), len, "{input} at {size}");
                assert_ne!(
                    bytes, base,
                    "changing the {input} left {size} bytes unchanged"
                );
            }
            // The size is an input too, not just a length: the longer payload is not the
            // shorter one extended.
            let longer = whole(&Payload::new("424242", "b/key", size + 1, 5));
            assert_ne!(
                &longer[..len],
                &base[..],
                "a {size}-byte payload is a prefix of the {}-byte one",
                size + 1
            );
        }
    });
}

#[test]
fn at_zero_bytes_no_input_can_matter() {
    bounded("at_zero_bytes_no_input_can_matter", || {
        // The documented limit (module docs, for the oracle in #746): at 0 B every input gives
        // the same empty payload, so the bytes alone cannot tell generations apart there.
        assert!(whole(&Payload::new("424242", "z", 0, 1)).is_empty());
        assert!(whole(&Payload::new("other", "y", 0, 2)).is_empty());
    });
}

// ---- (C) Exact length and random access ------------------------------------------------------

const ACCESS_SIZES: [u64; 9] = [0, 1, 31, 32, 33, 4096, 65_537, 100_003, 3 * 64 * KIB + 5];

#[test]
fn the_generator_yields_exactly_size_bytes() {
    bounded("the_generator_yields_exactly_size_bytes", || {
        for size in ACCESS_SIZES {
            let payload = Payload::new("424242", "c/len", size, 1);
            assert_eq!(payload.size(), size);
            // `whole` drains the stream, which fails on a byte past `size` or short of it.
            assert_eq!(whole(&payload).len() as u64, size);
        }
    });
}

#[test]
fn split_pieces_concatenate_to_the_whole() {
    bounded("split_pieces_concatenate_to_the_whole", || {
        for size in ACCESS_SIZES {
            let payload = Payload::new("424242", "c/split", size, 1);
            let bytes = whole(&payload);
            let mut splits = vec![0, 1, 31, 32, 33, size / 2, size.saturating_sub(1), size];
            splits.retain(|split| *split <= size);
            for split in splits {
                let head = direct(&payload, 0, split as usize);
                let tail = direct(&payload, split, (size - split) as usize);
                assert_eq!(
                    [head, tail].concat(),
                    bytes,
                    "size {size}, split at {split}"
                );
            }
        }
    });
}

#[test]
fn any_range_produced_directly_equals_the_same_range_cut_from_the_whole() {
    bounded(
        "any_range_produced_directly_equals_the_same_range_cut_from_the_whole",
        || {
            for size in ACCESS_SIZES {
                let payload = Payload::new("424242", "c/range", size, 9);
                let bytes = whole(&payload);
                let n = bytes.len();
                let mut ranges = vec![(0, n), (0, 0), (n, n)];
                for (a, b) in [
                    (1, 2),
                    (5, 37),
                    (31, 33),
                    (32, 64),
                    (33, 4097),
                    (4095, 70_000),
                    (65_535, 65_537),
                    (n / 3, 2 * n / 3),
                    (n.saturating_sub(7), n),
                ] {
                    if a <= b && b <= n {
                        ranges.push((a, b));
                    }
                }
                for (a, b) in ranges {
                    let expected = &bytes[a..b];
                    assert_eq!(
                        direct(&payload, a as u64, b - a),
                        expected,
                        "fill [{a}, {b}) of {size}"
                    );
                    let pieces = payload
                        .range(a as u64, b as u64)
                        .unwrap_or_else(|e| panic!("range [{a}, {b}) of {size}: {e}"));
                    assert_eq!(
                        drain(pieces, (b - a) as u64),
                        expected,
                        "range [{a}, {b}) of {size}"
                    );
                }
            }
        },
    );
}

#[test]
fn a_range_outside_the_payload_is_refused_not_truncated() {
    bounded(
        "a_range_outside_the_payload_is_refused_not_truncated",
        || {
            let payload = Payload::new("424242", "c/bounds", 100, 1);
            let mut two = [0u8; 2];
            let past_the_end = payload.fill(99, &mut two).expect_err("[99, 101) accepted");
            assert_eq!(
                past_the_end,
                OutOfRange {
                    start: 99,
                    end: Some(101),
                    size: 100
                }
            );
            assert_eq!(
                past_the_end.to_string(),
                "byte range [99, 101) is not inside a 100-byte payload"
            );
            assert_eq!(two, [0, 0], "a refused fill wrote into the buffer");
            let overflowing = payload
                .fill(u64::MAX, &mut two)
                .expect_err("an overflowing range accepted");
            assert_eq!(
                overflowing,
                OutOfRange {
                    start: u64::MAX,
                    end: None,
                    size: 100
                }
            );
            assert_eq!(
            overflowing.to_string(),
            "byte range starting at 18446744073709551615 overflows a 64-bit offset (payload is \
             100 bytes)"
        );
            assert_eq!(
                payload.range(10, 9).expect_err("[10, 9) accepted"),
                OutOfRange {
                    start: 10,
                    end: Some(9),
                    size: 100
                }
            );
            assert_eq!(
                payload.range(0, 101).expect_err("[0, 101) accepted"),
                OutOfRange {
                    start: 0,
                    end: Some(101),
                    size: 100
                }
            );
            let empty = payload.range(100, 100).expect("the empty range at the end");
            assert!(drain(empty, 0).is_empty());
        },
    );
}

// ---- (C′) Bounded state, direct access -------------------------------------------------------

#[test]
fn no_piece_exceeds_the_documented_bound_whatever_the_size() {
    bounded(
        "no_piece_exceeds_the_documented_bound_whatever_the_size",
        || {
            assert_eq!(PIECE_BYTES, 64 * 1024);
            // `drain` checks every piece against the bound, and `size_hint` at every step.
            for size in [
                64 * KIB - 1,
                64 * KIB,
                64 * KIB + 1,
                3 * 64 * KIB + 5,
                MIB + 1,
            ] {
                let payload = Payload::new("424242", "c'/pieces", size, 1);
                assert_eq!(drain(payload.pieces(), size).len() as u64, size);
            }
            // At sizes no test can stream whole, the count of bounded pieces is still exact.
            // Building the streams hashes nothing; a constructor that generated the prefix
            // first would fail here at the bound, not hang.
            for (size, pieces) in [(10 * GIB + 1, 10 * (GIB / (64 * KIB)) + 1), (TIB, 1 << 24)] {
                let pieces = usize::try_from(pieces).expect("a 64-bit usize");
                let payload = Payload::new("424242", "c'/huge", size, 1);
                assert_eq!(
                    payload.pieces().size_hint(),
                    (pieces, Some(pieces)),
                    "{size}"
                );
                assert_eq!(
                    payload
                        .range(size - PIECE_BYTES as u64 - 3, size)
                        .expect("tail range")
                        .size_hint(),
                    (2, Some(2)),
                    "{size}"
                );
            }
        },
    );
}

/// `(key, size, hex of the last 64 bytes)` at seed `424242`, generation 1; pinned like
/// [`KNOWN_ANSWERS`].
const TAIL_ANSWERS: [(&str, u64, &str); 2] = [
    (
        "kat/10g+1",
        10 * GIB + 1,
        "74a9d7960da87ac5b739ef864f2771446697a363db8c3348f45e35d9167f22ce\
         61b6f3d686f758670ff50fab3c77f0d8f71a7774f80aa2090932b471eeacccdd",
    ),
    (
        "kat/1t",
        TIB,
        "487bb9027e10c54f627d3a3b172cb5bc73982e1eaeacdd60d5b380fbca443326\
         b8ff6ef76280f8396a4ac496b2378e7643b2949def230b7fba9dbd7d13a1e4f8",
    ),
];

/// (C′)'s bound: honest work is a few hash blocks; regenerating the prefix is minutes (10 GiB)
/// to hours (1 TiB).
const DIRECT: Duration = Duration::from_secs(5);

#[test]
fn the_last_bytes_of_a_huge_payload_are_produced_directly() {
    bounded(
        "the_last_bytes_of_a_huge_payload_are_produced_directly",
        || {
            for (key, size, expected) in TAIL_ANSWERS {
                let payload = Payload::new("424242", key, size, 1);
                let start = size - 64;
                let tail = within(
                    DIRECT,
                    &format!("the tail of {key}, which must be computed directly,"),
                    {
                        let payload = payload.clone();
                        move || direct(&payload, start, 64)
                    },
                );
                assert_eq!(hex(&tail), expected, "{key}");
                // And it is the concatenation of its two halves, each produced directly (split
                // off a block boundary, so each half straddles one).
                let halves = within(
                    DIRECT,
                    &format!("the halves of {key}'s tail, each computed directly,"),
                    {
                        let payload = payload.clone();
                        move || {
                            [
                                direct(&payload, start, 27),
                                direct(&payload, start + 27, 37),
                            ]
                            .concat()
                        }
                    },
                );
                assert_eq!(halves, tail, "{key}");
                // The streaming form of the same range starts directly too.
                let streamed = within(
                    DIRECT,
                    &format!("the streamed tail of {key}, which must start directly,"),
                    {
                        let payload = payload.clone();
                        move || drain(payload.range(start, size).expect("tail range"), 64)
                    },
                );
                assert_eq!(streamed, tail, "{key}");
                // The first piece, and the last full piece and the 3 bytes after it, are
                // bounded; `drain` checks each piece's length.
                let (first, last) = within(
                    DIRECT,
                    &format!("the first and last pieces of {key}, each computed directly,"),
                    move || {
                        let first = payload.pieces().next().expect("a first piece");
                        let tail_len = PIECE_BYTES as u64 + 3;
                        let last = drain(
                            payload.range(size - tail_len, size).expect("tail range"),
                            tail_len,
                        );
                        (first, last)
                    },
                );
                assert_eq!(first.len(), PIECE_BYTES, "{key}");
                assert_eq!(&last[last.len() - 64..], &tail[..], "{key}");
            }
        },
    );
}

// ---- (D) Size classes come from the tuning ---------------------------------------------------

fn tuning(flags: TuningFlags) -> Tuning {
    Tuning::resolve(&flags).unwrap_or_else(|e| panic!("{flags:?} refused: {e}"))
}

fn classes(flags: TuningFlags) -> Vec<SizeClass> {
    tuning(flags).size_classes().expect("size classes")
}

/// The one size labelled `boundary`.
fn size_at(classes: &[SizeClass], boundary: Boundary) -> u64 {
    let found: Vec<u64> = classes
        .iter()
        .filter(|class| class.boundaries.contains(&boundary))
        .map(|class| class.size)
        .collect();
    assert_eq!(found.len(), 1, "{boundary:?} labels {found:?}");
    found[0]
}

fn assert_ascending_without_duplicates(classes: &[SizeClass]) {
    for pair in classes.windows(2) {
        assert!(pair[0].size < pair[1].size, "{classes:?}");
    }
}

/// The boundaries whose size does not depend on the tuning, with that size. `SubChunk`, which
/// labels two sizes, is checked on its own.
const FIXED: [(Boundary, u64); 7] = [
    (Boundary::Empty, 0),
    (Boundary::MinimumNonEmpty, 1),
    (Boundary::S3MinimumPart, 5 * MIB),
    (Boundary::SdkMultipartThreshold, 8 * MIB),
    (Boundary::S3MaxSinglePut, 5 * GIB),
    (Boundary::PastS3MaxSinglePut, 5 * GIB + 1),
    (Boundary::BeyondTenGib, 10 * GIB + 1),
];

fn assert_fixed_classes_unmoved(classes: &[SizeClass]) {
    for (boundary, size) in FIXED {
        assert_eq!(size_at(classes, boundary), size, "{boundary:?} moved");
    }
    let sub_chunk: Vec<u64> = classes
        .iter()
        .filter(|class| class.boundaries.contains(&Boundary::SubChunk))
        .map(|class| class.size)
        .collect();
    assert_eq!(sub_chunk, [4 * KIB, 64 * KIB]);
}

#[test]
fn the_default_tuning_gives_the_proposal_table() {
    bounded("the_default_tuning_gives_the_proposal_table", || {
        let c = MIB;
        let t = 158 * MIB;
        let expected = vec![
            (0, vec![Boundary::Empty]),
            (1, vec![Boundary::MinimumNonEmpty]),
            (4 * KIB, vec![Boundary::SubChunk]),
            (64 * KIB, vec![Boundary::SubChunk]),
            (c - 1, vec![Boundary::BelowChunk]),
            (c, vec![Boundary::AtChunk]),
            (c + 1, vec![Boundary::AboveChunk]),
            (5 * MIB, vec![Boundary::S3MinimumPart]),
            (8 * MIB, vec![Boundary::SdkMultipartThreshold]),
            (64 * c, vec![Boundary::MultiChunk]),
            (t - 1, vec![Boundary::BelowSegmentation]),
            (t, vec![Boundary::AtSegmentation]),
            (t + 1, vec![Boundary::AboveSegmentation]),
            (5 * GIB, vec![Boundary::S3MaxSinglePut]),
            (5 * GIB + 1, vec![Boundary::PastS3MaxSinglePut]),
            (10 * GIB + 1, vec![Boundary::BeyondTenGib]),
        ];
        let actual: Vec<(u64, Vec<Boundary>)> = classes(TuningFlags::default())
            .into_iter()
            .map(|class| (class.size, class.boundaries))
            .collect();
        assert_eq!(actual, expected);
    });
}

/// Every boundary with the words it is labelled with: proposal 0017 §4's "boundary it
/// straddles" column, with the size it stands for.
const LABELS: [(Boundary, &str); 15] = [
    (Boundary::Empty, "empty object"),
    (Boundary::MinimumNonEmpty, "minimum non-empty"),
    (Boundary::SubChunk, "sub-chunk"),
    (
        Boundary::BelowChunk,
        "one byte below the chunk size (C - 1)",
    ),
    (Boundary::AtChunk, "the chunk size (C)"),
    (Boundary::AboveChunk, "one byte past the chunk size (C + 1)"),
    (Boundary::S3MinimumPart, "S3 minimum part size (5 MiB)"),
    (
        Boundary::SdkMultipartThreshold,
        "stock SDK multipart threshold; bulk-delete body cap (8 MiB)",
    ),
    (Boundary::MultiChunk, "multi-chunk, many fragments (64 x C)"),
    (
        Boundary::BelowSegmentation,
        "one byte below the chunk-map segmentation threshold (T - 1)",
    ),
    (
        Boundary::AtSegmentation,
        "the chunk-map segmentation threshold (T)",
    ),
    (
        Boundary::AboveSegmentation,
        "one byte past the chunk-map segmentation threshold (T + 1)",
    ),
    (
        Boundary::S3MaxSinglePut,
        "S3's largest accepted single PUT (5 GiB)",
    ),
    (
        Boundary::PastS3MaxSinglePut,
        "first size past S3's single-PUT maximum (5 GiB + 1)",
    ),
    (
        Boundary::BeyondTenGib,
        "beyond 5 GiB: an object over 10 GiB, the launch requirement (10 GiB + 1)",
    ),
];

#[test]
fn each_class_is_labelled_with_the_boundary_it_straddles() {
    bounded(
        "each_class_is_labelled_with_the_boundary_it_straddles",
        || {
            for (boundary, words) in LABELS {
                assert_eq!(boundary.label(), words, "{boundary:?}");
                assert_eq!(boundary.to_string(), words, "{boundary:?} displayed");
            }
            // Every boundary in the default table is one of them.
            for class in classes(TuningFlags::default()) {
                for boundary in class.boundaries {
                    assert!(
                        LABELS.iter().any(|(labelled, _)| *labelled == boundary),
                        "{boundary:?} has no pinned label"
                    );
                }
            }
        },
    );
}

#[test]
fn a_different_chunk_size_moves_the_chunk_classes_and_nothing_fixed() {
    bounded(
        "a_different_chunk_size_moves_the_chunk_classes_and_nothing_fixed",
        || {
            let c = 4 * MIB;
            let classes = classes(TuningFlags {
                chunk_size: Some(c),
                ..TuningFlags::default()
            });
            assert_ascending_without_duplicates(&classes);
            assert_eq!(size_at(&classes, Boundary::BelowChunk), c - 1);
            assert_eq!(size_at(&classes, Boundary::AtChunk), c);
            assert_eq!(size_at(&classes, Boundary::AboveChunk), c + 1);
            assert_eq!(size_at(&classes, Boundary::MultiChunk), 64 * c);
            // The derived threshold follows the chunk size: 158 chunks of 4 MiB.
            assert_eq!(size_at(&classes, Boundary::AtSegmentation), 158 * c);
            assert_eq!(size_at(&classes, Boundary::BelowSegmentation), 158 * c - 1);
            assert_eq!(size_at(&classes, Boundary::AboveSegmentation), 158 * c + 1);
            assert_fixed_classes_unmoved(&classes);
        },
    );
}

#[test]
fn only_the_root_budget_changed_moves_only_the_derived_threshold() {
    bounded(
        "only_the_root_budget_changed_moves_only_the_derived_threshold",
        || {
            // 31,500 is below today's 50,000: ⌊31,500 / 315⌋ = 100 chunks of 1 MiB.
            let classes = classes(TuningFlags {
                max_root_value_bytes: Some(31_500),
                ..TuningFlags::default()
            });
            assert_ascending_without_duplicates(&classes);
            assert_eq!(size_at(&classes, Boundary::AtSegmentation), 100 * MIB);
            assert_eq!(
                size_at(&classes, Boundary::BelowSegmentation),
                100 * MIB - 1
            );
            assert_eq!(
                size_at(&classes, Boundary::AboveSegmentation),
                100 * MIB + 1
            );
            assert_eq!(size_at(&classes, Boundary::BelowChunk), MIB - 1);
            assert_eq!(size_at(&classes, Boundary::AtChunk), MIB);
            assert_eq!(size_at(&classes, Boundary::AboveChunk), MIB + 1);
            assert_eq!(size_at(&classes, Boundary::MultiChunk), 64 * MIB);
            assert_fixed_classes_unmoved(&classes);
        },
    );
}

#[test]
fn a_given_threshold_is_used_exactly_whatever_the_root_budget() {
    bounded(
        "a_given_threshold_is_used_exactly_whatever_the_root_budget",
        || {
            // 200 chunks of the default 1 MiB: a whole number of chunks, as the server's rule
            // makes every threshold. With it given, a budget too small to derive from (200, or
            // 1) is accepted, and so is the cap itself.
            let t = 200 * MIB;
            for budget in [
                None,
                Some(31_500),
                Some(200),
                Some(1),
                Some(MAX_ROOT_VALUE_BYTES_CAP),
            ] {
                let resolved = tuning(TuningFlags {
                    max_root_value_bytes: budget,
                    segment_threshold_bytes: Some(t),
                    ..TuningFlags::default()
                });
                assert_eq!(
                    resolved.segment_threshold_bytes.value, t,
                    "budget {budget:?}"
                );
                assert_eq!(resolved.segment_threshold_bytes.source, Source::Given);
                let classes = resolved.size_classes().expect("size classes");
                assert_eq!(size_at(&classes, Boundary::AtSegmentation), t);
                assert_eq!(size_at(&classes, Boundary::BelowSegmentation), t - 1);
                assert_eq!(size_at(&classes, Boundary::AboveSegmentation), t + 1);
            }
        },
    );
}

/// The boundaries of the one class at `size`.
fn boundaries_at(classes: &[SizeClass], size: u64) -> Vec<Boundary> {
    let found: Vec<&SizeClass> = classes.iter().filter(|class| class.size == size).collect();
    assert_eq!(found.len(), 1, "{size}: {classes:?}");
    found[0].boundaries.clone()
}

#[test]
fn two_classes_that_coincide_appear_once_carrying_both_labels() {
    bounded(
        "two_classes_that_coincide_appear_once_carrying_both_labels",
        || {
            // An 8 MiB chunk lands `C` on the stock SDK multipart threshold.
            let classes = classes(TuningFlags {
                chunk_size: Some(8 * MIB),
                ..TuningFlags::default()
            });
            assert_ascending_without_duplicates(&classes);
            assert_eq!(
                boundaries_at(&classes, 8 * MIB),
                [Boundary::AtChunk, Boundary::SdkMultipartThreshold]
            );
            assert_eq!(boundaries_at(&classes, 8 * MIB + 1), [Boundary::AboveChunk]);
            // A given threshold of 5 GiB lands `T` on S3's largest single PUT and `T + 1` on
            // the first size past it.
            let classes = self::classes(TuningFlags {
                segment_threshold_bytes: Some(5 * GIB),
                ..TuningFlags::default()
            });
            assert_ascending_without_duplicates(&classes);
            assert_eq!(
                boundaries_at(&classes, 5 * GIB),
                [Boundary::AtSegmentation, Boundary::S3MaxSinglePut]
            );
            assert_eq!(
                boundaries_at(&classes, 5 * GIB + 1),
                [Boundary::AboveSegmentation, Boundary::PastS3MaxSinglePut]
            );
            assert_eq!(
                boundaries_at(&classes, 5 * GIB - 1),
                [Boundary::BelowSegmentation]
            );
        },
    );
}

/// A chunk size inside the accepted range that divides `u64::MAX` (17 × 65,537; `u64::MAX` =
/// 3 × 5 × 17 × 257 × 65,537 × 4,294,967,297). A given threshold of exactly `u64::MAX` is then
/// a whole number of its chunks, so `T + 1` is the one size class that cannot fit.
const ODD_CHUNK: u64 = 17 * 65_537;

/// The typed refusals; their messages are pinned at the command line, below.
#[test]
fn a_tuning_no_deployment_can_have_is_refused_not_clamped() {
    bounded(
        "a_tuning_no_deployment_can_have_is_refused_not_clamped",
        || {
            let flags = |chunk_size, max_root_value_bytes, segment_threshold_bytes| TuningFlags {
                chunk_size,
                max_root_value_bytes,
                segment_threshold_bytes,
            };
            let out_of_range = |chunk_size| TuningError::ChunkSizeOutOfRange { chunk_size };
            let budget = |max_root_value_bytes| TuningError::RootBudgetOutOfRange {
                max_root_value_bytes,
            };
            let below = |threshold| TuningError::ThresholdBelowChunk {
                threshold,
                chunk_size: MIB,
            };
            let not_whole = |threshold| TuningError::ThresholdNotWholeChunks {
                threshold,
                chunk_size: MIB,
            };
            let overflow = |class, from| TuningError::Overflow { class, from };
            let derived = "`--max-root-value-bytes` and `--chunk-size` \
                       (no `--segment-threshold-bytes` was given)";
            for (flags, expected) in [
                // A chunk size outside the server's range, refused as that before anything is
                // derived from it (`u64::MAX` would also overflow `C + 1` and the threshold). A zero
                // built by hand (the argument parser refuses it first) is one.
                (flags(Some(4 * KIB), None, None), out_of_range(4 * KIB)),
                (
                    flags(Some(u64::MAX), None, Some(GIB)),
                    out_of_range(u64::MAX),
                ),
                (flags(Some(0), None, None), out_of_range(0)),
                // A root budget of zero or above the cap, whether or not a threshold is given (a
                // zero built by hand: the argument parser refuses it first); `u64::MAX` is one,
                // refused as that before any threshold is derived from it.
                (flags(None, Some(0), None), budget(0)),
                (flags(None, Some(0), Some(GIB)), budget(0)),
                (
                    flags(None, Some(MAX_ROOT_VALUE_BYTES_CAP + 1), None),
                    budget(MAX_ROOT_VALUE_BYTES_CAP + 1),
                ),
                (
                    flags(None, Some(MAX_ROOT_VALUE_BYTES_CAP + 1), Some(GIB)),
                    budget(MAX_ROOT_VALUE_BYTES_CAP + 1),
                ),
                (flags(None, Some(u64::MAX), None), budget(u64::MAX)),
                // A budget smaller than one chunk record derives no threshold.
                (
                    flags(None, Some(314), None),
                    TuningError::NoChunkRefFits {
                        max_root_value_bytes: 314,
                    },
                ),
                // A given threshold below one chunk (a zero built by hand is one), or not a whole
                // number of chunks.
                (flags(None, None, Some(158)), below(158)),
                (flags(None, None, Some(0)), below(0)),
                (flags(None, None, Some(MIB + 1)), not_whole(MIB + 1)),
                (flags(None, None, Some(u64::MAX)), not_whole(u64::MAX)),
                // The one size that cannot fit in 64 bits at a tuning `resolve` otherwise
                // accepts, naming the flag it is computed from.
                (
                    flags(Some(ODD_CHUNK), None, Some(u64::MAX)),
                    overflow("T + 1", "`--segment-threshold-bytes`"),
                ),
            ] {
                assert_eq!(Tuning::resolve(&flags), Err(expected), "{flags:?}");
            }
            // A `Tuning` built by hand, past `resolve`, is held to the same rule by `size_classes`:
            // so 4 KiB and 64 KiB are below `C` in every table, and "sub-chunk" is true wherever it
            // appears; and a zero budget is refused beside a threshold it did not derive.
            let setting = |value, source| Setting { value, source };
            let by_hand = Tuning {
                chunk_size: setting(4 * KIB, Source::Given),
                max_root_value_bytes: setting(DEFAULT_MAX_ROOT_VALUE_BYTES, Source::Default),
                segment_threshold_bytes: setting(158 * 4 * KIB, Source::Derived),
            };
            assert_eq!(by_hand.size_classes(), Err(out_of_range(4 * KIB)));
            let by_hand = Tuning {
                chunk_size: setting(MIB, Source::Default),
                max_root_value_bytes: setting(0, Source::Given),
                segment_threshold_bytes: setting(GIB, Source::Given),
            };
            assert_eq!(by_hand.size_classes(), Err(budget(0)));
            // With the budget capped, no derived threshold overflows through `resolve` (at most
            // 158 × 16 MiB); `derived_segment_threshold` and a `Tuning` built by hand still name
            // the flags a size that does not fit was computed from.
            assert_eq!(
                derived_segment_threshold(MIB, u64::MAX),
                Err(overflow("T (⌊root budget / 315⌋ × C)", derived))
            );
            let by_hand = Tuning {
                chunk_size: setting(ODD_CHUNK, Source::Given),
                max_root_value_bytes: setting(DEFAULT_MAX_ROOT_VALUE_BYTES, Source::Default),
                segment_threshold_bytes: setting(u64::MAX, Source::Derived),
            };
            assert_eq!(by_hand.size_classes(), Err(overflow("T + 1", derived)));
        },
    );
}

// ---- (E) The tuning flags ---------------------------------------------------------------------

/// The ten required flags, as `cli_surface.rs` gives them.
const GIVEN: [(&str, &str); 10] = [
    ("endpoint", "http://127.0.0.1:9000"),
    ("region", "eu-central-1"),
    ("bucket", "validate-bucket"),
    ("scenario", "smoke"),
    ("duration", "2m"),
    ("workers", "7"),
    ("seed", "424242"),
    ("out", "/var/lib/wyrd-validate/out"),
    ("run-id", "run-0017"),
    ("driver-placement", "external"),
];

fn argv(pairs: &[(&str, &str)]) -> Vec<String> {
    pairs
        .iter()
        .flat_map(|(flag, value)| [format!("--{flag}"), (*value).to_string()])
        .collect()
}

fn wyrd_pair() -> HashMap<&'static str, OsString> {
    HashMap::from([
        ("WYRD_S3_ACCESS_KEY", OsString::from("WYRDKEYID0001")),
        ("WYRD_S3_SECRET_KEY", OsString::from("wyrd-secret")),
    ])
}

/// The library entry point over the Wyrd credential pair: (status, stdout, stderr).
fn lib_run(args: &[String]) -> (u8, String, String) {
    let vars = wyrd_pair();
    let lookup = move |name: &str| vars.get(name).cloned();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let status = run(args, &lookup, &mut out, &mut err);
    (
        status,
        String::from_utf8(out).expect("utf-8"),
        String::from_utf8(err).expect("utf-8"),
    )
}

fn lines(stdout: &str) -> Vec<&str> {
    stdout.lines().collect()
}

#[test]
fn absent_tuning_flags_are_echoed_as_defaults_and_derived() {
    bounded(
        "absent_tuning_flags_are_echoed_as_defaults_and_derived",
        || {
            let (status, stdout, stderr) = lib_run(&argv(&GIVEN));
            assert_eq!(status, EXIT_INCONCLUSIVE, "{stderr}");
            let lines = lines(&stdout);
            for expected in [
                "  --chunk-size = 1048576 (default)",
                "  --max-root-value-bytes = 50000 (default)",
                "  --segment-threshold-bytes = 165675008 (derived)",
            ] {
                assert!(lines.contains(&expected), "missing `{expected}`:\n{stdout}");
            }
            // The ten required flags are still echoed each with its value.
            for (flag, value) in GIVEN {
                let line = format!("  --{flag} = {value}");
                assert!(lines.contains(&line.as_str()), "{stdout}");
            }
        },
    );
}

/// One accepted invocation: the tuning flags given, and the echo lines they must produce.
type Echo<'a> = (&'a [(&'a str, &'a str)], &'a [&'a str]);

#[test]
fn given_tuning_flags_are_used_and_echoed_as_given() {
    bounded("given_tuning_flags_are_used_and_echoed_as_given", || {
        let cases: [Echo<'_>; 6] = [
            // One given; the threshold is derived from it, 158 × 4 MiB.
            (
                &[("chunk-size", "4194304")],
                &[
                    "  --chunk-size = 4194304 (given)",
                    "  --max-root-value-bytes = 50000 (default)",
                    "  --segment-threshold-bytes = 662700032 (derived)",
                ],
            ),
            // All three given; the threshold is 125 chunks of 2 MiB.
            (
                &[
                    ("segment-threshold-bytes", "262144000"),
                    ("max-root-value-bytes", "31500"),
                    ("chunk-size", "2097152"),
                ],
                &[
                    "  --chunk-size = 2097152 (given)",
                    "  --max-root-value-bytes = 31500 (given)",
                    "  --segment-threshold-bytes = 262144000 (given)",
                ],
            ),
            // Both ends of the server's chunk-size range, inclusive, and a threshold of exactly
            // one chunk.
            (
                &[("chunk-size", "1048576")],
                &["  --chunk-size = 1048576 (given)"],
            ),
            (
                &[("chunk-size", "16777216")],
                &[
                    "  --chunk-size = 16777216 (given)",
                    "  --segment-threshold-bytes = 2650800128 (derived)",
                ],
            ),
            (
                &[("segment-threshold-bytes", "1048576")],
                &["  --segment-threshold-bytes = 1048576 (given)"],
            ),
            (
                &[
                    ("chunk-size", "16777216"),
                    ("segment-threshold-bytes", "16777216"),
                ],
                &["  --segment-threshold-bytes = 16777216 (given)"],
            ),
        ];
        for (tuning, echoed) in cases {
            // The tuning flags in among the required ones, not only after them.
            let mut args = argv(&GIVEN[..4]);
            args.extend(argv(tuning));
            args.extend(argv(&GIVEN[4..]));
            let (status, stdout, stderr) = lib_run(&args);
            assert_eq!(status, EXIT_INCONCLUSIVE, "{tuning:?}: {stderr}");
            let lines = lines(&stdout);
            for expected in echoed {
                assert!(
                    lines.contains(expected),
                    "{tuning:?}: missing `{expected}`:\n{stdout}"
                );
            }
        }
    });
}

/// `args` refused with a usage error naming `--flag`, with `phrase` on stderr and nothing echoed.
fn assert_refused(args: &[String], flag: &str, phrase: &str) {
    let (status, stdout, stderr) = lib_run(args);
    assert_eq!(status, EXIT_USAGE, "{args:?} accepted:\n{stdout}");
    assert!(stdout.is_empty(), "a refused run echoed:\n{stdout}");
    assert!(
        stderr.contains(&format!("`--{flag}`")),
        "{flag} not named: {stderr}"
    );
    assert!(stderr.contains(phrase), "`{phrase}` not in: {stderr}");
}

#[test]
fn each_bad_tuning_value_is_refused_by_name() {
    bounded("each_bad_tuning_value_is_refused_by_name", || {
        for flag in TUNING_FLAGS {
            let with = |value: &str| {
                let mut args = argv(&GIVEN);
                args.extend([format!("--{flag}"), value.to_string()]);
                args
            };
            assert_refused(&with("0"), flag, "must be a positive integer, not 0");
            assert_refused(&with("000"), flag, "must be a positive integer, not 0");
            for not_an_integer in ["1MiB", "+5", "-5", " 5", "5 ", "1.5", "0x10", "1_000"] {
                assert_refused(
                    &with(not_an_integer),
                    flag,
                    &format!("needs a positive integer byte count, not `{not_an_integer}`"),
                );
            }
            assert_refused(
                &with("18446744073709551616"),
                flag,
                "value `18446744073709551616` does not fit in 64 bits",
            );
            // `u64::MAX` parses, and is then refused as a tuning no deployment has, naming the
            // flag all the same: a chunk size outside the server's range, a budget above the
            // cap, a threshold that is not a whole number of chunks.
            let no_deployment_has = match flag {
                "chunk-size" => "is outside the range the server's `s3` role accepts",
                "max-root-value-bytes" => "is outside the range a deployment can have",
                _ => "is not a whole number of `--chunk-size` 1048576 chunks",
            };
            assert_refused(&with("18446744073709551615"), flag, no_deployment_has);
            assert_refused(&with(""), flag, "was given an empty value");
            assert_refused(&with("--typo"), flag, "`--typo`");

            // Repeated, even with the same value.
            let mut args = with("4194304");
            args.extend([format!("--{flag}"), "4194304".to_string()]);
            assert_refused(&args, flag, "given more than once");

            // The last token, with no value after it.
            let mut args = argv(&GIVEN);
            args.push(format!("--{flag}"));
            assert_refused(&args, flag, "needs a value");
        }
    });
}

/// One command-line refusal: the tuning flags given, every flag the refusal must name, and its
/// message (whole for the first case of each kind, so each kind's wording is pinned here).
type Refusal<'a> = (&'a [(&'a str, &'a str)], &'a [&'a str], &'a str);

/// At the command line, each tuning no deployment can have is refused naming its flag or flags.
#[test]
fn a_tuning_no_deployment_can_have_is_refused_at_the_command_line_by_flag_name() {
    bounded(
        "a_tuning_no_deployment_can_have_is_refused_at_the_command_line_by_flag_name",
        || {
            let odd_chunk = ODD_CHUNK.to_string();
            let cases: [Refusal<'_>; 11] = [
                (
                    &[("max-root-value-bytes", "314")],
                    &["max-root-value-bytes"],
                    "`--max-root-value-bytes` 314 holds no 315-byte chunk record, so no \
                     segmentation threshold can be derived from it; pass \
                     `--segment-threshold-bytes` or a larger budget",
                ),
                // A root budget above the cap, with and without a threshold given (the argument
                // parser refuses a zero first).
                (
                    &[("max-root-value-bytes", "50001")],
                    &["max-root-value-bytes"],
                    "`--max-root-value-bytes` 50001 is outside the range a deployment can have: \
                     1 to 50000 bytes, inclusive (half the 100,000-byte value limit every \
                     backend inherits)",
                ),
                (
                    &[
                        ("max-root-value-bytes", "50001"),
                        ("segment-threshold-bytes", "1048576"),
                    ],
                    &["max-root-value-bytes"],
                    "50001 is outside the range",
                ),
                // A threshold below one chunk, a chunk size below the floor, and a chunk size
                // past the ceiling beside a threshold below it (the chunk size is refused first).
                (
                    &[("segment-threshold-bytes", "158")],
                    &["segment-threshold-bytes", "chunk-size"],
                    "`--segment-threshold-bytes` 158 is below `--chunk-size` 1048576: the \
                     largest flat object holds at least one whole chunk",
                ),
                (
                    &[("chunk-size", "4096")],
                    &["chunk-size"],
                    "`--chunk-size` 4096 is outside the range the server's `s3` role accepts: \
                     1048576 to 16777216 bytes (1 to 16 MiB), inclusive",
                ),
                (
                    &[
                        ("chunk-size", "4294967296"),
                        ("segment-threshold-bytes", "1024"),
                    ],
                    &["chunk-size"],
                    "4294967296 is outside the range",
                ),
                (
                    &[("chunk-size", "1048575")],
                    &["chunk-size"],
                    "1048575 is outside the range",
                ),
                (
                    &[("chunk-size", "16777217")],
                    &["chunk-size"],
                    "16777217 is outside the range",
                ),
                (
                    &[("segment-threshold-bytes", "1048577")],
                    &["segment-threshold-bytes", "chunk-size"],
                    "`--segment-threshold-bytes` 1048577 is not a whole number of \
                     `--chunk-size` 1048576 chunks: a flat chunk map names whole chunks, so the \
                     largest flat object is a whole number of them",
                ),
                // `u64::MAX` as a budget is above the cap (before it could derive a threshold
                // that does not fit); as a threshold at a chunk size that divides it, `T + 1` is
                // the one size that does not fit in 64 bits, naming the flag it is computed from.
                (
                    &[("max-root-value-bytes", "18446744073709551615")],
                    &["max-root-value-bytes"],
                    "18446744073709551615 is outside the range a deployment can have",
                ),
                (
                    &[
                        ("chunk-size", odd_chunk.as_str()),
                        ("segment-threshold-bytes", "18446744073709551615"),
                    ],
                    &["segment-threshold-bytes"],
                    "the T + 1 size class, computed from `--segment-threshold-bytes`, does not \
                     fit in 64 bits",
                ),
            ];
            for (tuning, named, message) in cases {
                let mut args = argv(&GIVEN);
                args.extend(argv(tuning));
                for flag in named {
                    assert_refused(&args, flag, message);
                }
            }
        },
    );
}

#[test]
fn the_ten_required_flags_stay_required_with_the_tuning_flags_given() {
    bounded(
        "the_ten_required_flags_stay_required_with_the_tuning_flags_given",
        || {
            let mut args = argv(&GIVEN[1..]);
            args.extend(argv(&[("chunk-size", "4194304")]));
            assert_refused(&args, "endpoint", "missing required flag(s): `--endpoint`");
        },
    );
}

#[test]
fn the_real_binary_echoes_the_tuning_with_its_sources() {
    bounded("the_real_binary_echoes_the_tuning_with_its_sources", || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_wyrd-validate"));
        let mut args = argv(&GIVEN);
        args.extend(argv(&[("max-root-value-bytes", "31500")]));
        command.args(&args).env_clear().envs(wyrd_pair());
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        let output = command.output().expect("spawn wyrd-validate");
        let stdout = String::from_utf8(output.stdout).expect("utf-8");
        assert_eq!(
            output.status.code(),
            Some(i32::from(EXIT_INCONCLUSIVE)),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let lines = lines(&stdout);
        for expected in [
            "  --chunk-size = 1048576 (default)",
            "  --max-root-value-bytes = 31500 (given)",
            "  --segment-threshold-bytes = 104857600 (derived)",
        ] {
            assert!(lines.contains(&expected), "missing `{expected}`:\n{stdout}");
        }
    });
}

// ---- (F) The defaults have not drifted from the values they mirror ---------------------------

#[test]
fn the_mirrored_defaults_match_the_core_constants() {
    bounded("the_mirrored_defaults_match_the_core_constants", || {
        // `wyrd-core` is a dev-dependency only: the shipped binary links no `wyrd-*` crate.
        assert_eq!(
            DEFAULT_MAX_ROOT_VALUE_BYTES,
            wyrd_core::metadata::MAX_ROOT_VALUE_BYTES as u64,
            "the root budget default drifted from MAX_ROOT_VALUE_BYTES"
        );
        assert_eq!(
            MAX_ROOT_VALUE_BYTES_CAP,
            wyrd_core::metadata::MAX_ROOT_VALUE_BYTES as u64,
            "the root budget cap drifted from MAX_ROOT_VALUE_BYTES"
        );
        assert_eq!(
            CHUNK_REF_BYTES,
            wyrd_core::multipart::MAX_CHUNKREF_BYTES as u64,
            "the chunk-record width drifted from MAX_CHUNKREF_BYTES"
        );
        assert_eq!(
            DEFAULT_MAX_ROOT_VALUE_BYTES / CHUNK_REF_BYTES,
            u64::from(wyrd_core::multipart::MAX_MAP_CHUNKS),
            "the server's flat-map capacity is no longer ⌊root budget / chunk-ref width⌋"
        );
        let derived = derived_segment_threshold(DEFAULT_CHUNK_SIZE, DEFAULT_MAX_ROOT_VALUE_BYTES)
            .expect("default threshold");
        assert_eq!(
            derived,
            wyrd_core::multipart::max_single_put_bytes(DEFAULT_CHUNK_SIZE),
            "the derived default threshold drifted from max_single_put_bytes at the default chunk"
        );
        assert_eq!(
            tuning(TuningFlags::default()).segment_threshold_bytes.value,
            derived
        );
    });
}

/// What a gateway composed with the server's defaults (`Gateway::new`, no `with_chunk_size`)
/// answers a single PUT of `declared` bytes: `Some(limit)` when it refuses the PUT as too large,
/// `None` when it stores it. The refusal's `limit` is `max_single_put_bytes` at the server's
/// own default chunk size (`Gateway::admit_single_put`, `crates/server/src/lib.rs`), and it
/// comes before any byte of the body is read, so the zeroed body is never touched.
fn default_gateway_single_put(declared: u64) -> Option<u64> {
    let body = vec![0u8; usize::try_from(declared).expect("a probe size this host can address")];
    let dir = tempfile::tempdir().expect("temp dir");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let gateway = Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        );
        match gateway.put_object("default-chunk-size-probe", &body).await {
            Ok(()) => None,
            Err(err) => match err.downcast_ref::<GatewayError>() {
                Some(&GatewayError::EntityTooLarge {
                    declared: refused,
                    limit,
                }) if refused == declared => Some(limit),
                _ => panic!("the default gateway failed a {declared}-byte PUT otherwise: {err}"),
            },
        }
    })
}

/// The server's `DEFAULT_CHUNK_SIZE` is private, so it is read off a default gateway: a PUT one
/// byte past the tool's default `T` must be refused, with `T` as the limit. A server default
/// below the tool's is refused with a smaller limit. A server default above it admits the PUT
/// and really writes its `T + 1` bytes, so that failure takes seconds, not milliseconds (or ends
/// at the bound, which also fails this test).
#[test]
fn the_default_chunk_size_matches_the_servers() {
    bounded("the_default_chunk_size_matches_the_servers", || {
        let threshold = derived_segment_threshold(DEFAULT_CHUNK_SIZE, DEFAULT_MAX_ROOT_VALUE_BYTES)
            .expect("default threshold");
        let probe = threshold + 1;
        match default_gateway_single_put(probe) {
            Some(limit) => assert_eq!(
                limit,
                threshold,
                "a default server gateway's single-PUT limit is {limit} B, {} chunks of {} B, \
                 but the tool's default chunk size is {DEFAULT_CHUNK_SIZE} B",
                wyrd_core::multipart::MAX_MAP_CHUNKS,
                limit / u64::from(wyrd_core::multipart::MAX_MAP_CHUNKS),
            ),
            None => panic!(
                "a default server gateway stored a {probe}-byte single PUT, so its default chunk \
                 size is above the tool's {DEFAULT_CHUNK_SIZE} B"
            ),
        }
    });
}

/// The bounds the server's `s3` role names when it refuses a chunk size outside them, as
/// `(floor, ceiling)`, read off `serve_s3_role`, which checks the chunk size before it opens or
/// serves anything (`check_s3_chunk_size`, `crates/server/src/cli.rs`). The server's
/// `MIN_CHUNK_SIZE` and `MAX_CHUNK_SIZE` are private, like its default chunk size; the refusal
/// is the public path that carries them ("it must be at least N bytes" / "at most N bytes").
fn server_chunk_size_bounds() -> (u64, u64) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let mut bounds = Vec::new();
        for (chunk_size, side, after) in [
            (1, "too small", "at least "),
            (usize::MAX, "too large", "at most "),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback listener");
            let refused = serve_s3_role(
                MetadataBackend::Redb,
                CoordinationBackend::Mem,
                "",
                None,
                Some(chunk_size),
                Vec::new(),
                String::new(),
                listener,
                None,
            )
            .await
            .expect_err("the s3 role accepted a chunk size of 1 byte or usize::MAX")
            .to_string();
            assert!(
                refused.starts_with("s3: --chunk-size ") && refused.contains(side),
                "{refused}"
            );
            let bound = refused
                .split(after)
                .nth(1)
                .and_then(|rest| rest.split(' ').next())
                .and_then(|digits| digits.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("no bound after `{after}` in: {refused}"));
            bounds.push(bound);
        }
        (bounds[0], bounds[1])
    })
}

/// The chunk-size range the tool refuses outside of mirrors the server's `s3` role's: 1 MiB to
/// 16 MiB, inclusive. Both bounds are read off the refusals the role gives a 1-byte and a
/// `usize::MAX` chunk size, which name the floor and the ceiling.
#[test]
fn the_chunk_size_range_matches_the_servers() {
    bounded("the_chunk_size_range_matches_the_servers", || {
        assert_eq!(
            (MIN_CHUNK_SIZE, MAX_CHUNK_SIZE),
            server_chunk_size_bounds(),
            "the tool's chunk-size range (left) drifted from the server's (right)"
        );
    });
}
