//! Issue #648, slice 1/6 of the #635 re-slicing (0016 decision 7(a)): the segmented
//! chunk-map **record shape and its codec** — `ChunkMap::{Flat, Segmented}`, the
//! decode-time structural invariants, and the `seg:`/`seggrp:` key helpers. This
//! slice lands no resolver and no producer (#649 onward); it binds entirely through
//! **base-visible** API (`wyrd_core::metadata::{encode, decode, InodeRecord}`) over
//! **raw stored bytes**.
//!
//! Every segmented case here is a hand-authored JSON byte string, decoded through
//! `decode::<InodeRecord>`. This file therefore imports nothing this patch adds
//! (no `ChunkMap`, `SegmentedMap`, `SegmentRef`, `SegmentGroup`, `seg_key`,
//! `ChunkMapError`, …) — only symbols already visible on `origin/main`. On a revert
//! of `crates/core/src/metadata.rs` (the RED leg), `chunk_map` is back to a bare
//! `Vec<ChunkRef>`, so this file still COMPILES (it names nothing new) and instead
//! fails as an **assertion**: a segmented root's raw bytes no longer decode
//! (criterion 2's positive case and criterion 3 both fail as assertions), which is
//! the falsifiability this test binds — never a compile error.
//!
//! The one place a number from the patched module is needed — criterion 3's
//! `MAX_ROOT_SEGMENTS`, `MAX_VALUE_BYTES` and the `MAX_ROOT_VALUE_BYTES` budget the
//! table is sized against (`0016:1467`) — reads it out of the module's **source text**
//! ([`production_constant`]) instead of copying it, so the ceiling this test measures is
//! the ceiling that ships, and a later slice moving any of those constants is judged
//! here rather than in production.
//!
//! Criterion 3 is then asserted against an upper bound over **every** root at that
//! segment count ([`encoded_upper_bound`]) rather than against one hand-built table:
//! "the worst case" is a maximisation nobody should have to win by hand — equal spans
//! are not it ([`front_loaded_table`] beats them by 387 bytes at 641 segments) — and a
//! capacity test that measures only its author's guess can pass while the shape that
//! ships is over budget.

#![forbid(unsafe_code)]

use wyrd_core::metadata::{decode, encode, inode_key, InodeRecord, InodeState};
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

/// A well-formed 32-lowercase-hex segment-group nonce (`SEG_NONCE_HEX_LEN` on the
/// patched branch).
const NONCE: &str = "0123456789abcdef0123456789abcdef";

// ===========================================================================
// Criterion 1 — legacy round-trips byte-identically, and CAS still commits.
// ===========================================================================

/// A hand-authored legacy `inode:` value in **exactly** the shape `origin/main`
/// emits today: `chunk_map` a bare JSON array (mixed `EcScheme` wire shapes — a
/// unit-variant `"None"` and a struct-variant `{"ReedSolomon":{...}}` — to exercise
/// both), and `etag`/`content_type`/`modified` **absent** (they decode to `None` and
/// carry `skip_serializing_if`, so a legacy writer never emitted them,
/// `crates/core/src/metadata.rs:1015-1028` pre-patch numbering). Field order matches
/// `InodeRecord`'s declaration order (`size, chunk_map, state, version`), which is
/// also `serde_json`'s struct-field emission order.
///
/// This exact byte string was captured from `wyrd_core::metadata::encode` of the
/// equivalent record on the **patched** branch (`ChunkMap::Flat` serialises as a
/// bare array, unchanged from the pre-patch `Vec<ChunkRef>` derive) — so it is, by
/// construction, what `origin/main` already writes.
const LEGACY_BYTES: &[u8] = br#"{"size":11,"chunk_map":[{"id":7,"scheme":{"ReedSolomon":{"k":6,"m":3}},"len":11,"placement":[1,2,3,4,5,6,7,8,9]},{"id":8,"scheme":"None","len":3,"placement":[]}],"state":"Committed","version":3}"#;

#[test]
fn legacy_flat_record_round_trips_byte_identically() {
    let prior: InodeRecord =
        decode(LEGACY_BYTES).expect("a pre-existing legacy record must still decode");
    let re_encoded = encode(&prior);
    assert_eq!(
        re_encoded.as_ref(),
        LEGACY_BYTES,
        "decode -> encode must be the identity on a byte sequence origin/main already wrote, \
         or every CAS over a pre-existing record turns into a permanent Conflict"
    );
}

#[test]
fn legacy_flat_record_cas_still_commits_against_the_original_bytes() {
    pollster::block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store =
            wyrd_metadata_redb::RedbMetadataStore::open(dir.path().join("meta.redb")).unwrap();
        let key = inode_key(1);

        // Seed the store with the ORIGINAL legacy bytes, unconditionally — standing in
        // for a record `origin/main` already wrote before this patch existed.
        store
            .commit(WriteBatch::new().put(key.clone(), LEGACY_BYTES))
            .await
            .unwrap();

        let prior: InodeRecord = decode(LEGACY_BYTES).unwrap();

        // The precondition every CAS commit in `metadata.rs` uses:
        // `require(key, encode(prior))`. If decode -> encode were not the identity on
        // this record, this precondition would never match the bytes the store holds
        // and this commit would return `Conflict` instead.
        let outcome = store
            .commit(
                WriteBatch::new()
                    .require(key.clone(), encode(&prior))
                    .put(key, encode(&prior)),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            CommitOutcome::Committed,
            "require(key, encode(prior)) must commit against a store holding the original bytes"
        );
    });
}

// ===========================================================================
// Criterion 2 — a well-formed segmented root decodes; each decode invariant has
// its raw-byte negative case.
// ===========================================================================

/// A well-formed segmented root: two segments contiguously tiling `size` (0..5,
/// 5..12) under one segment group.
const SEGMENTED_ROOT_OK: &[u8] = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;

#[test]
fn well_formed_segmented_root_decodes() {
    let record: std::result::Result<InodeRecord, _> = decode(SEGMENTED_ROOT_OK);
    assert!(
        record.is_ok(),
        "a well-formed segmented root (contiguous tiling spanning exactly `size`) must decode; got {:?}",
        record.err()
    );
}

/// The **same rule criterion 1 binds for a legacy record, now for a segmented one**:
/// decode -> encode is the identity on the stored bytes.
///
/// It is not a corollary of the legacy case. Every metadata CAS in the system is
/// `require(key, encode(prior))` compared byte-for-byte with the stored value, and a
/// segmented root is re-encoded by exactly those paths (a placement repair —
/// reconstruction, rebalance, backfill — supersedes the record it read). If the
/// segmented half of the codec re-ordered a field, dropped `segment_count`, or spelled
/// the group differently on the way out, every such commit against an already-published
/// segmented object would return `Conflict` **forever**: a permanently unrepairable
/// object, which is the C-1 failure mode this shape exists to avoid. So the field order
/// and spelling of the segmented wire form are pinned here, on raw bytes.
#[test]
fn segmented_root_round_trips_byte_identically() {
    let prior: InodeRecord =
        decode(SEGMENTED_ROOT_OK).expect("a well-formed segmented root decodes");
    assert_eq!(
        encode(&prior).as_ref(),
        SEGMENTED_ROOT_OK,
        "decode -> encode must be the identity on a stored segmented root, or every CAS \
         over one turns into a permanent Conflict"
    );
}

#[test]
fn segmented_root_cas_commits_against_the_stored_bytes() {
    pollster::block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store =
            wyrd_metadata_redb::RedbMetadataStore::open(dir.path().join("meta.redb")).unwrap();
        let key = inode_key(2);

        // A store already holding a segmented root — what #653's publisher will leave
        // behind, and what every later maintenance pass reads.
        store
            .commit(WriteBatch::new().put(key.clone(), SEGMENTED_ROOT_OK))
            .await
            .unwrap();

        let prior: InodeRecord = decode(SEGMENTED_ROOT_OK).expect("the stored root decodes");
        let outcome = store
            .commit(
                WriteBatch::new()
                    .require(key.clone(), encode(&prior))
                    .put(key, encode(&prior)),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            CommitOutcome::Committed,
            "require(key, encode(prior)) must commit against a store holding a segmented \
             root's own bytes"
        );
    });
}

/// An **empty** segmented map — `segment_count: 0`, no segments — must be `Err`.
///
/// This is the one malformed root every *other* invariant waves through: the declared
/// count agrees with the (empty) list, there is no index to duplicate or gap and no
/// span to overlap, and the table spans 0 bytes, which a `size` of 0 then confirms. So
/// these bytes decode into a value **only** because the emptiness guard refuses them
/// (`crates/core/src/metadata.rs:676-678`); without it a consumer would hold a
/// *segmented* map that names nothing, i.e. the answer "this object owns no chunks"
/// arrived at structurally — the C-1 failure mode this shape exists to avoid, because it
/// is indistinguishable from a genuinely empty object and is how a live object's
/// fragments become unreferenced. An object with no chunks is the FLAT shape
/// (`"chunk_map":[]`), never this one.
#[test]
fn an_empty_segmented_map_is_err() {
    // `size` is 0 — the value a table spanning nothing agrees with — so this case
    // cannot be caught by the size-vs-span rule for the wrong reason.
    let bytes = br#"{"size":0,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":0,"segments":[]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "a segmented map naming no segments must be Err — an empty chunk map is the flat \
         shape, and a segmented root that names nothing is a map no consumer can resolve"
    );
    // The control: the same root with ONE segment decodes. It pins the refusal above to
    // the empty segment table rather than to anything else about these bytes.
    let one_segment = br#"{"size":5,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":1,"segments":[{"index":0,"byte_offset":0,"byte_len":5}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(one_segment);
    assert!(
        record.is_ok(),
        "the otherwise identical single-segment root must decode, or the case above is \
         rejected for some reason other than its empty segment table; got {:?}",
        record.err()
    );
}

#[test]
fn segment_count_mismatching_segments_len_is_err() {
    // `segment_count` claims 3 segments; only 2 are present.
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":3,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "segment_count disagreeing with segments.len() must be Err"
    );
}

#[test]
fn duplicate_segment_index_is_err() {
    // Both segments claim index 0.
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":0,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(record.is_err(), "a duplicate segment index must be Err");
}

#[test]
fn segment_index_gap_is_err() {
    // Indices 0, 2 — index 1 is missing.
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":2,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(record.is_err(), "a gap in segment indices must be Err");
}

#[test]
fn overlapping_segment_spans_is_err() {
    // Segment 0 covers [0, 5); segment 1 claims to start at byte 3 — inside segment
    // 0's span, not at its end.
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":3,"byte_len":9}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(record.is_err(), "an overlapping segment span must be Err");
}

#[test]
fn non_monotonic_segment_span_is_err() {
    // Segment 0 covers [0, 5); segment 1 starts at byte 6 — a gap, not the required
    // byte 5 — so the tiling is not contiguous.
    let bytes = br#"{"size":13,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":6,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "a non-monotonic (gapped) segment span must be Err"
    );
}

/// A **zero-byte** `SegmentRef` must be `Err`, even where every other rule holds.
///
/// The empty segment sits in the MIDDLE of an otherwise perfect tiling: the indices are
/// 0, 1, 2 in order with no gap, each segment still starts exactly where its predecessor
/// ends (a zero-length span advances the running offset by nothing, so contiguity is
/// preserved), and the table spans exactly the declared `size`. No other invariant
/// touches it — it is refused only by the emptiness guard
/// (`crates/core/src/metadata.rs:704-708`). A segment covering no bytes can hold no
/// chunk, so its `seg:` record is either absent or itself empty (the case the segment
/// side refuses in turn, `crates/core/src/metadata.rs:942-947`): admitting the root would
/// hand every later resolver (#649) a segment whose record it cannot produce, on a map
/// that otherwise looks complete — and a partially resolvable map under-reports the bytes
/// its object owns.
#[test]
fn a_zero_byte_segment_is_err() {
    // Segments 0..5, 5..5 (empty), 5..12 — contiguous, correctly indexed, spanning
    // exactly `size` (12).
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":3,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":0},{"index":2,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "a segment covering no bytes must be Err — it can hold no chunk, so its `seg:` \
         record could never exist"
    );
    // The control: the same three-segment table with a one-byte middle segment decodes,
    // so the refusal above is attributable to the zero-length span alone.
    let non_empty = br#"{"size":13,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":3,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":1},{"index":2,"byte_offset":6,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(non_empty);
    assert!(
        record.is_ok(),
        "the same table with a one-byte middle segment must decode, or the case above is \
         rejected for some reason other than its zero-byte segment; got {:?}",
        record.err()
    );
}

#[test]
fn non_hex_nonce_is_err() {
    // 31 lowercase-hex characters — one short of `SEG_NONCE_HEX_LEN` (32).
    let bytes = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcd","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "a nonce that is not exactly SEG_NONCE_HEX_LEN lowercase hex characters must be Err"
    );
}

/// Bonus coverage for the cross-field invariant criterion 2's parenthetical names
/// ("contiguous tiling spanning exactly `size`"): a segment table that tiles
/// contiguously and internally-consistently, but whose total span disagrees with
/// the inode's declared `size`, must also be `Err` — the table is the object's byte
/// index, so a disagreement there is structural corruption.
#[test]
fn segment_table_span_disagreeing_with_size_is_err() {
    // The table spans 0..12 (5 + 7), but `size` claims 99.
    let bytes = br#"{"size":99,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;
    let record: std::result::Result<InodeRecord, _> = decode(bytes);
    assert!(
        record.is_err(),
        "a segment table whose span disagrees with the declared size must be Err"
    );
}

/// The tiling arithmetic itself must not wrap. A table whose running offset leaves
/// `u64` is refused rather than summed: wrapped, the running offset comes back to a
/// value the record's own declared `size` can then *agree* with, admitting a map that
/// under-reports the bytes its object owns — an object whose remaining fragments nothing
/// references.
#[test]
fn a_segment_table_whose_tiling_leaves_u64_is_err() {
    // Segment 0 covers the whole space; segment 1 is contiguous, non-empty and correctly
    // indexed — everything but representable. `size` is the FORGED one an implementation
    // that wrapped (or saturated) would confirm, so this case fails on a wrapping build
    // instead of being caught by the size-vs-span rule for the wrong reason.
    let bytes = format!(
        r#"{{"size":{max},"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":1}},"segment_count":2,"segments":[{{"index":0,"byte_offset":0,"byte_len":{max}}},{{"index":1,"byte_offset":{max},"byte_len":1}}]}},"state":"Committed","version":1}}"#,
        max = u64::MAX,
    );
    let record: std::result::Result<InodeRecord, _> = decode(bytes.as_bytes());
    assert!(
        record.is_err(),
        "a segment table whose tiling overflows u64 must be Err, never a wrapped span"
    );
}

// ===========================================================================
// Criterion 3 — a segmented root stays inside the value ceiling.
// ===========================================================================

/// The module whose constants are under judgment, read as **text** at compile time.
///
/// The two numbers criterion 3 weighs — how many segments a root may name, and the value
/// ceiling it must fit — live in `crates/core/src/metadata.rs`, and they are symbols this
/// patch ADDS, so this file cannot `use` them without becoming a compile red on
/// `origin/main` (see the file doc comment). Reading the source text keeps the test
/// **patch-aware anyway**: it measures whatever `MAX_ROOT_SEGMENTS` and `MAX_VALUE_BYTES`
/// actually say today, so raising the ceiling past what fits fails HERE rather than in
/// production, and a hard-coded 512 can never drift away from the constant it stands for.
/// On `origin/main` the constants are absent and [`production_constant`] fails as an
/// assertion — the same red as the rest of this file.
const METADATA_SOURCE: &str = include_str!("../src/metadata.rs");

/// The value of `pub const <name>: <ty> = <literal>;` in [`METADATA_SOURCE`], with `_`
/// digit separators removed. Panics with the constant's name when it is absent, which is
/// exactly what a run against a tree without this patch reports.
fn production_constant(name: &str) -> u64 {
    let needle = format!("pub const {name}:");
    let line = METADATA_SOURCE
        .lines()
        .find(|line| line.trim_start().starts_with(&needle))
        .unwrap_or_else(|| {
            panic!(
                "`pub const {name}` is not declared in crates/core/src/metadata.rs — the \
                 segmented chunk-map shape this test binds is not present in the tree"
            )
        });
    let literal = line
        .rsplit_once('=')
        .and_then(|(_, tail)| tail.split(';').next())
        .unwrap_or_else(|| panic!("`pub const {name}` has no `= <literal>;` value: {line}"));
    literal
        .trim()
        .replace('_', "")
        .parse()
        .unwrap_or_else(|e| panic!("`pub const {name}` is not an integer literal: {literal} ({e})"))
}

/// The widest decimal rendering a `u64` field can take: `u64::MAX` is
/// `18446744073709551615` — twenty digits — and `serde_json` emits an integer as plain
/// unsigned decimal (no sign, no exponent, no separators), which every measurement below
/// re-checks byte-for-byte through [`re_encoded_len`].
const U64_MAX_DIGITS: u64 = 20;

/// The `state` [`render_root`] renders. `InodeState` has exactly two variants
/// (`crates/core/src/metadata.rs:87-92`) and [`state_slack`] measures both, so the bound
/// does not depend on this being the wider one.
const RENDERED_STATE: &str = "Committed";

/// One concrete segmented root at some segment count — the raw bytes of a stored record —
/// together with its **digit slack**: how many decimal digits short of [`U64_MAX_DIGITS`]
/// the `u64` fields a writer chooses fall, summed over all of them.
///
/// The slack is what turns one measured encoding into a bound over *every* root at that
/// segment count ([`encoded_upper_bound`]).
struct RootTable {
    /// The stored root, byte for byte.
    bytes: String,
    /// Σ, over `size`, `epoch`, `version` and every segment's `byte_offset` / `byte_len`,
    /// of ([`U64_MAX_DIGITS`] − digits actually rendered).
    digit_slack: u64,
}

/// The decimal width of `value` — exactly what the encoder spends on it.
fn digits(value: u64) -> u64 {
    value.to_string().len() as u64
}

/// Render a segmented root over `spans` (`(byte_offset, byte_len)` in index order) as the
/// raw bytes of a stored record, and total its digit slack. Field order is
/// `InodeRecord`'s declaration order, so these bytes are also what `encode` emits for the
/// decoded record (asserted in [`re_encoded_len`]).
fn render_root(spans: &[(u64, u64)]) -> RootTable {
    // Both at their widest rendering, so neither contributes slack of its own.
    let epoch = u64::MAX;
    let version = u64::MAX;
    let (last_offset, last_len) = *spans
        .last()
        .expect("a segmented map names at least one segment");
    let size = last_offset + last_len;
    let mut digit_slack = (U64_MAX_DIGITS - digits(size))
        + (U64_MAX_DIGITS - digits(epoch))
        + (U64_MAX_DIGITS - digits(version));
    let mut segments = String::new();
    for (index, (byte_offset, byte_len)) in spans.iter().enumerate() {
        if index > 0 {
            segments.push(',');
        }
        segments.push_str(&format!(
            r#"{{"index":{index},"byte_offset":{byte_offset},"byte_len":{byte_len}}}"#
        ));
        digit_slack +=
            (U64_MAX_DIGITS - digits(*byte_offset)) + (U64_MAX_DIGITS - digits(*byte_len));
    }
    let bytes = format!(
        r#"{{"size":{size},"chunk_map":{{"group":{{"nonce":"{NONCE}","epoch":{epoch}}},"segment_count":{count},"segments":[{segments}]}},"state":"{RENDERED_STATE}","version":{version}}}"#,
        count = spans.len(),
    );
    RootTable { bytes, digit_slack }
}

/// `n` equal spans filling the `u64` range as evenly as the tiling allows.
fn equal_span_table(n: u64) -> RootTable {
    let byte_len = u64::MAX / n;
    let spans: Vec<(u64, u64)> = (0..n).map(|i| (i * byte_len, byte_len)).collect();
    render_root(&spans)
}

/// The table that **beats** [`equal_span_table`] on encoded size: segment 0 swallows
/// `10^19` bytes, so every later `byte_offset` renders at the full twenty digits instead
/// of climbing there gradually, and the rest of the `u64` range is spread evenly so the
/// later `byte_len`s stay as wide as the remaining budget allows.
///
/// It is here because "equal spans" is *not* the worst case, and a capacity test that
/// measures only its author's guess at the worst case can pass while the real ceiling is
/// over budget: at 641 segments these two encode to 49 694 and 50 081 bytes — on either
/// side of a 50 000-byte budget. Neither is asserted against a budget directly, because
/// neither is provably the worst table either; [`encoded_upper_bound`] is what the budgets
/// are asserted against, and it dominates both.
fn front_loaded_table(n: u64) -> RootTable {
    let jump = 10_u64.pow(19);
    let rest = u64::MAX - jump;
    let each = rest / (n - 1);
    // The remainder rides in segment 0, so the tiling spans exactly `u64::MAX`.
    let mut spans = vec![(0, jump + rest % (n - 1))];
    let mut byte_offset = spans[0].1;
    for _ in 1..n {
        spans.push((byte_offset, each));
        byte_offset += each;
    }
    render_root(&spans)
}

/// How many bytes wider than [`RENDERED_STATE`] the widest `state` spelling encodes —
/// **measured** through the production codec on real records rather than assumed, so the
/// bound holds for a root in either commit state.
fn state_slack() -> u64 {
    let encoded_len = |state| {
        encode(&InodeRecord {
            state,
            ..Default::default()
        })
        .len() as u64
    };
    let rendered = encoded_len(InodeState::Committed);
    encoded_len(InodeState::Pending).max(rendered) - rendered
}

/// A **strict upper bound** on `encode(...).len()` over *every* decodable segmented root
/// with this table's segment count (ADR-0047 object metadata absent — leg 2 below adds
/// that reserve), not merely over the table handed in.
///
/// Why it bounds them all. A root's encoding is a fixed skeleton plus its numbers, and for
/// a given segment count the skeleton is the same in every decodable root — each of these
/// is a decode invariant, so no stored root can spell it differently: the nonce is exactly
/// 32 hex characters (`crates/core/src/metadata.rs:574`), `segment_count` equals the
/// number of segments (`:757`), and each `index` is exactly its position (`:691`); the
/// punctuation and field names are the codec's. What a writer still chooses is the
/// **width** of `size`, `epoch`, `version` and each segment's `byte_offset` / `byte_len` —
/// and a `u64` renders in at most [`U64_MAX_DIGITS`] digits. Adding the table's digit
/// slack widens every one of those to that maximum, so the result is ≥ the encoding of any
/// table at this count, including ones no legal tiling could realise.
///
/// This is what makes criterion 3 independent of anyone's guess at "the worst case": a
/// cleverer table — [`front_loaded_table`] is one — cannot exceed it. Looseness is safe in
/// the only direction that matters: a bound above the true maximum can make this test FAIL
/// a shape that would have fitted, never PASS one that would not.
fn encoded_upper_bound(table: &RootTable) -> u64 {
    re_encoded_len(&table.bytes) + table.digit_slack + state_slack()
}

/// The encoded length of `bytes` **as this codec re-emits them** — the number every
/// budget here is measured on, since what the store holds after a repair is
/// `encode(decode(stored))`, not the input.
fn re_encoded_len(bytes: &str) -> u64 {
    let record: InodeRecord = decode(bytes.as_bytes()).expect(
        "a table at the MAX_ROOT_SEGMENTS ceiling is not rejected at decode — the capacity \
         bound is enforced where the table becomes work (publication / ranged read), not here",
    );
    let re_encoded = encode(&record);
    assert_eq!(
        re_encoded.as_ref(),
        bytes.as_bytes(),
        "what is measured must be the bytes the codec actually emits"
    );
    re_encoded.len() as u64
}

#[test]
fn segmented_root_at_max_root_segments_stays_inside_the_value_ceiling() {
    // Every bound comes from the production module itself, so this test judges the shape
    // that actually ships rather than a copy of its numbers.
    let max_root_segments = production_constant("MAX_ROOT_SEGMENTS");
    let max_value_bytes = production_constant("MAX_VALUE_BYTES");
    // The budget `MAX_ROOT_SEGMENTS` is actually sized against: HALF the value ceiling
    // (`0016:1467`, `max_segref_bytes × MAX_ROOT_SEGMENTS ≤ V / 2`). Asserting only the
    // full ceiling would pass a table at 50–100 KB — legal today, and unwritable after any
    // later field addition, which makes the placement of an already-published object
    // permanently unrepairable.
    let max_root_value_bytes = production_constant("MAX_ROOT_VALUE_BYTES");
    assert!(
        max_root_value_bytes * 2 <= max_value_bytes,
        "MAX_ROOT_VALUE_BYTES ({max_root_value_bytes}) must be at most HALF the \
         {max_value_bytes}-byte value ceiling, or the 2x headroom 0016:1467 requires of \
         MAX_ROOT_SEGMENTS is not what the root is measured against"
    );
    assert!(
        max_root_segments >= 2,
        "MAX_ROOT_SEGMENTS must name at least two segments to be a segmented map; got \
         {max_root_segments}"
    );

    // ---- Leg 1: the segment table, the part the record SHAPE controls. -------------
    // Two concrete tables, each decoded and re-encoded, each measured on
    // `encode(...).len()`; then the bound that dominates them AND every other table at
    // this segment count. The budgets are asserted against the BOUND, so what ships is
    // judged against every root a writer could produce rather than against the two
    // tables this file happens to build.
    let equal = equal_span_table(max_root_segments);
    let front = front_loaded_table(max_root_segments);
    let equal_len = re_encoded_len(&equal.bytes);
    let front_len = re_encoded_len(&front.bytes);

    let bound = encoded_upper_bound(&equal);
    assert_eq!(
        bound,
        encoded_upper_bound(&front),
        "the bound must depend only on the segment count: two tables with the same count \
         differ only in the WIDTH of their numbers, so widening every number to \
         {U64_MAX_DIGITS} digits has to land on one total. A difference means the encoding \
         carries per-table bytes this bound does not account for, and it is not a bound"
    );
    assert!(
        equal_len <= bound && front_len <= bound,
        "the bound ({bound}) must dominate every measured table: equal spans encode to \
         {equal_len} bytes and the front-loaded table to {front_len}"
    );
    assert!(
        bound <= max_value_bytes,
        "EVERY root holding MAX_ROOT_SEGMENTS ({max_root_segments}) segments must stay inside \
         the {max_value_bytes}-byte value ceiling every backend inherits; the worst one is at \
         most {bound} bytes"
    );
    // The binding bound: the budget 0016:1467 sizes the segment table against. A table
    // that only just fits `MAX_VALUE_BYTES` leaves a root that cannot absorb one more
    // field — and a root that cannot be RE-written is an object whose placement can never
    // be repaired, which is the permanent failure mode this shape exists to avoid.
    assert!(
        bound <= max_root_value_bytes,
        "EVERY root holding MAX_ROOT_SEGMENTS ({max_root_segments}) segments must stay inside \
         the {max_root_value_bytes}-byte root budget (half the {max_value_bytes}-byte value \
         ceiling, 0016:1467); the worst one is at most {bound} bytes — lower MAX_ROOT_SEGMENTS \
         rather than spending the headroom"
    );

    // ---- Leg 2: the WHOLE record, including the fields the caller controls. ---------
    // A stored root is not just its segment table: `InodeRecord` also persists the
    // ADR-0047 object metadata — `etag`, `modified`, and `content_type`, which is the
    // client's request header round-tripped verbatim, so its width is the caller's and
    // not the record shape's. Measuring a root with those omitted would report on bytes
    // no production write emits. So the reserve — the half of the ceiling
    // `MAX_ROOT_VALUE_BYTES` deliberately does not spend — is filled with real metadata
    // and the COMPLETE root is measured.
    //
    // This leg is not leg 1 restated: it measures the encoded cost of those fields on a
    // real decode -> encode round trip. A codec that escaped or expanded a long
    // `content_type`, a field order that did not match, or a decoder that balked at a
    // ~90 KB value would fail here while leg 1 still passed. What it establishes: every
    // inode whose object metadata fits the reserve is inside the ceiling by construction.
    // One whose metadata exceeds the reserve is refused by the tightest backend when it
    // is published — a clean create failure, the same one an equally large flat record
    // already meets today, not a durability hazard (a root that was published fits, and
    // every repair re-encodes the same fields); bounding a caller-supplied header belongs
    // to the protocol gateway, not to the record shape.
    let reserve = max_value_bytes - max_root_value_bytes;
    // `etag` is a lowercase-hex SHA-256 (ADR-0047) — 64 characters, worst case by
    // construction; `modified` is epoch millis at full `u64` width; `content_type` takes
    // every remaining byte of the reserve. Its value is a WIDTH stand-in, not a media
    // type: the record shape stores the header verbatim and never validates it (the
    // gateway is what degrades an unusable one on the way out), so what matters to the
    // ceiling is only how many bytes a caller can put there. Field order is
    // `InodeRecord`'s declaration order, so the block appends cleanly and `encode`
    // reproduces it byte-for-byte.
    let etag = "f".repeat(64);
    let head = format!(r#","etag":"{etag}","content_type":""#);
    let tail = format!(r#"","modified":{}"#, u64::MAX);
    let pad = (reserve as usize)
        .checked_sub(head.len() + tail.len())
        .expect(
            "the reserve must hold at least the fixed ADR-0047 fields a published object carries",
        );
    let meta_block = format!("{head}{}{tail}", "t".repeat(pad));
    assert_eq!(
        meta_block.len() as u64,
        reserve,
        "the metadata block must spend the WHOLE reserve — a smaller one measures a \
         convenient case rather than the worst one"
    );
    let full_root = format!(
        "{}{meta_block}}}",
        front
            .bytes
            .strip_suffix('}')
            .expect("the root is a JSON object")
    );
    let full_len = re_encoded_len(&full_root);
    assert_eq!(
        full_len,
        front_len + reserve,
        "the measured root must be the segment table PLUS the full reserve, in encoded bytes"
    );
    assert!(
        full_len <= max_value_bytes,
        "a MAX_ROOT_SEGMENTS ({max_root_segments}) root carrying the full {reserve}-byte \
         object-metadata reserve must stay inside the {max_value_bytes}-byte value ceiling; got \
         {full_len} bytes — the table budget and the reserve must sum to at most the ceiling"
    );
    // The same statement over EVERY table at this segment count, not just the one
    // measured: the widest root plus the whole reserve still fits the ceiling.
    assert!(
        bound + reserve <= max_value_bytes,
        "the worst root at MAX_ROOT_SEGMENTS ({max_root_segments}) is at most {bound} bytes, \
         which with the {reserve}-byte object-metadata reserve leaves the \
         {max_value_bytes}-byte value ceiling"
    );
}
