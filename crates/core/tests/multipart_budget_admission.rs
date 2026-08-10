//! Issue #715 — the multipart **admission ledger**: `wyrd_core::multipart::{Budget,
//! AdmissionRecord, decode_admission_record}`, the `mpuctl` singleton's record value, its
//! budget profile and the two derivations that profile establishes (`U_ref`, `MAX_SESSIONS`,
//! proposal 0016 `:348`, `:1469-1470`). **Pure**: no store, no async, no fixture beyond
//! hand-authored JSON bytes and the production codec.
//!
//! Every witness here is **decoded, never constructed**: the bytes are written out by hand
//! and fed to the two surfaces one validation serves —
//!
//! * **S2** `decode_admission_record`, which attributes a rejection to **one** typed
//!   [`RecordError`] variant (every rule assertion below is made through it);
//! * **S1** `metadata::decode::<AdmissionRecord>`, the store-wide codec any consumer holding
//!   the type uses, where the same rules apply but the failure arrives untyped.
//!
//! `decode_both` runs both on every witness and asserts they agree, so no rule can be
//! enforced on one surface and missed on the other.
//!
//! The legs:
//! * **round trip** — every field survives decode, and re-encoding a record this codec wrote
//!   is byte-identical to its stored bytes, while a foreign spelling of the same value
//!   normalises rather than round-tripping (`mpuctl` is CAS'd whole, `0016:348`);
//! * **the derivations** — `U_ref` under **each** of its two arms and `MAX_SESSIONS` under
//!   **each** of its two terms, so neither the raw part-space nor the staged-ceiling arm can
//!   be miscomputed unnoticed (`0016:1469`, `:1470`);
//! * **G1–G8** — one isolating witness per rule, each violating only its own, so a rule that
//!   stopped being enforced shows up here and nowhere else;
//! * **P1–P3** — the other direction: a decoder that refuses a stored record on a number this
//!   deployment merely chose is as wrong as one that admits a torn record;
//! * **P-arith** — decode's verdict equals the verdict exact (unbounded) integer arithmetic
//!   gives, at the field maxima, with no panic and no wrap.
//!
//! No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use wyrd_core::metadata;
use wyrd_core::multipart::{decode_admission_record, AdmissionRecord, RecordError};

/// `SCAN_CAP/2` — the seam constant two of the record's rules are stated against
/// (`crates/traits/src/lib.rs:286`, `SCAN_CAP = 1 << 20`; `0016:1470`, `:1471`).
const SCAN_HALF: u64 = (wyrd_traits::SCAN_CAP as u64) / 2;

/// One `mpuctl` value's **stored bytes**, hand-authored: field names and order exactly as
/// `0016:348` states them and no spaces — this codec's own spelling, so an equality against
/// `metadata::encode` is a byte-identity claim and not a JSON-shape one.
fn ledger(count: u64, max_sessions: u64, profile: (u64, u32, u32, u32, u32)) -> Vec<u8> {
    let (w_ref, mpc, mpps, mip, msc) = profile;
    format!(
        "{{\"count\":{count},\"max_sessions\":{max_sessions},\"profile\":{{\"w_ref\":{w_ref},\
         \"max_part_chunks\":{mpc},\"max_parts_per_session\":{mpps},\
         \"max_inflight_parts\":{mip},\"max_staged_chunks\":{msc}}}}}"
    )
    .into_bytes()
}

/// Decode `bytes` through **both** surfaces and assert they agree — same verdict, and on
/// success the same value. Returns S2's typed result, which every rule assertion reads.
fn decode_both(bytes: &[u8]) -> Result<AdmissionRecord, RecordError> {
    let typed = decode_admission_record(bytes);
    let untyped = metadata::decode::<AdmissionRecord>(bytes);
    assert_eq!(
        typed.as_ref().ok(),
        untyped.as_ref().ok(),
        "S1 and S2 disagree on {}: S2={typed:?}, S1={untyped:?}",
        String::from_utf8_lossy(bytes)
    );
    typed
}

/// The witness both surfaces must accept, and the one several rule witnesses are a
/// single-field perturbation of: `U_ref = min((100+10)x165, 20_000 + 2x10x165) =
/// min(18_150, 23_300)`, so `max_sessions = min(72_600 / 18_150, 524_288) = 4`.
fn legal_ledger() -> Vec<u8> {
    ledger(3, 4, (72_600, 165, 100, 10, 20_000))
}

// ===========================================================================
// Round trip — every field preserved, and what byte identity does and does not claim
// ===========================================================================

#[test]
fn round_trip_preserves_every_field() {
    let record = decode_both(&legal_ledger()).expect("the legal ledger decodes");

    assert_eq!(record.count(), 3);
    assert_eq!(record.max_sessions(), 4);
    let profile = record.profile();
    assert_eq!(profile.w_ref(), 72_600);
    assert_eq!(profile.max_part_chunks(), 165);
    assert_eq!(profile.max_parts_per_session(), 100);
    assert_eq!(profile.max_inflight_parts(), 10);
    assert_eq!(profile.max_staged_chunks(), 20_000);
}

/// The **serialization-identity** contract, with its domain (`AGENTS.md`, "Recurring defect
/// classes": decode→encode is byte-identical wherever a CAS depends on it).
///
/// For bytes this codec wrote it holds exactly: no field is optional, defaulted or skipped,
/// and the shape is closed, so the re-encode is the stored bytes — what a whole-record CAS on
/// the re-encoded prior needs (`metadata.rs:1794`, the `inode:` shape).
///
/// What it is **not** is a canonicalisation check. A foreign spelling of the same value —
/// fields reordered, whitespace inserted — still decodes, to the *same* value, and re-encodes
/// in this codec's spelling. That is JSON's doing, not this record's, and it is pinned here
/// rather than left implicit: it is what makes "byte-identical" a claim about records this
/// codec wrote, and what obliges #656–#659 to write `mpuctl` through `metadata::encode` (or to
/// precondition on the raw bytes they read, `metadata.rs:2012`, as `pending:` does).
#[test]
fn re_encoding_a_record_this_codec_wrote_is_byte_identical() {
    let bytes = legal_ledger();
    let record = decode_both(&bytes).expect("the legal ledger decodes");
    assert_eq!(metadata::encode(&record).as_ref(), bytes.as_slice());

    let foreign = br#"{ "max_sessions": 4, "profile": {"max_part_chunks": 165, "w_ref": 72600,
         "max_staged_chunks": 20000, "max_inflight_parts": 10, "max_parts_per_session": 100},
         "count": 3 }"#;
    let same_value = decode_both(foreign).expect("a foreign spelling of the same value decodes");
    assert_eq!(same_value, record);
    assert_eq!(metadata::encode(&same_value).as_ref(), bytes.as_slice());
}

// ===========================================================================
// The derivations — BOTH arms of `U_ref` and BOTH terms of `MAX_SESSIONS`
//
// `U_ref = min(raw, ceiling)` (`0016:1469`) and `MAX_SESSIONS = min(quotient, SCAN_CAP/2)`
// (`0016:1470`) are each a `min` of two terms, and a witness where one term always wins
// leaves the other unmeasured: the arithmetic of the losing term could be anything at all and
// every assertion would still pass. Each leg below is a profile where the named term
// **determines** the result, so that term's own arithmetic is what the assertion binds.
// ===========================================================================

/// `U_ref` under **each** of its two arms (`0016:1469`).
///
/// * the **raw part-number space** `(MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) ×
///   MAX_PART_CHUNKS` binds on the legal ledger: `(100+10)x165 = 18_150`, against a staged
///   ceiling of `20_000 + 2x10x165 = 23_300`;
/// * the **staged ceiling** `MAX_STAGED_CHUNKS + 2 × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`
///   binds on the second: `10 + 2x3x10 = 70`, against a raw space of `(100+3)x10 = 1_030`.
///
/// Wrong admission-memory arithmetic is what this record exists to prevent (`0016:2593`,
/// X64): a `U_ref` too small derives a `max_sessions` too large, and the reconcile pass then
/// holds more staged references than `W_ref` was sized for. The ceiling operands are chosen so
/// **every** single-operator slip changes the value: `70` versus `600` (`+` → `×`),
/// `10 + (2+3)x10 = 60` (the coefficient's `×` → `+`), `10 + 2x(3+10) = 36` (the product's
/// `×` → `+`), `10 + 0 = 10` (either `×` → `/`) and `10 + 2x3 = 16` (the product's `×` → `%`).
#[test]
fn u_ref_takes_whichever_of_its_two_arms_binds() {
    let raw_bound = decode_both(&legal_ledger()).expect("the legal ledger decodes");
    assert_eq!(
        raw_bound.profile().u_ref(),
        18_150,
        "the raw arm is smaller"
    );
    assert_eq!(raw_bound.max_sessions(), 4);

    let ceiling_bound =
        decode_both(&ledger(0, 10, (700, 10, 100, 3, 10))).expect("the ceiling ledger decodes");
    assert_eq!(
        ceiling_bound.profile().u_ref(),
        70,
        "the ceiling is smaller"
    );
    // The whole point of the record: that footprint is what `max_sessions` is derived from.
    assert_eq!(ceiling_bound.max_sessions(), 10);
}

/// `MAX_SESSIONS`' **second** term, the `SCAN_CAP/2` clamp (`0016:1470`) — "a clamp the
/// implementation applies, not a range check left to the operator". `W_ref` is sized from host
/// RAM and `U_ref` from the caps, so a legal pairing (a large `W_ref` with small parts) makes
/// the quotient exceed `SCAN_CAP` and break the reaper's `scan("mpu:")`.
///
/// Here `⌊2_000_000 / 2⌋ = 1_000_000` and the clamp brings it to `524_288`, so the clamp is
/// what the stored `max_sessions` must equal — and the record naming the *unclamped* quotient
/// is refused. Without the clamp the two verdicts swap.
#[test]
fn max_sessions_is_clamped_to_scan_cap_half_when_that_term_binds() {
    let clamped = decode_both(&ledger(0, SCAN_HALF, (2_000_000, 1, 1, 1, 1)))
        .expect("the clamped ledger decodes");
    assert_eq!(clamped.profile().u_ref(), 2);
    assert_eq!(clamped.max_sessions(), SCAN_HALF);

    assert_eq!(
        decode_both(&ledger(0, 1_000_000, (2_000_000, 1, 1, 1, 1))),
        Err(RecordError::MaxSessionsNotDerived {
            stored: 1_000_000,
            derived: SCAN_HALF,
        })
    );
}

// ===========================================================================
// G1–G8 — one isolating witness per rule, asserted through S2's typed variant
// ===========================================================================

/// **G1**, the totality precondition (`0016:1466`): at `max_part_chunks = 0` the `U_ref` of
/// `0016:1469` is `0`, so `MAX_SESSIONS`' quotient has no divisor. Unlike G2–G8 this one
/// cannot be isolated — no value violates G1 while satisfying G8, because G8 is undefined
/// there — so what it pins is that the record is refused *before* the derivation runs.
#[test]
fn g1_zero_part_chunks_is_refused_before_the_derivation() {
    let bytes = ledger(0, 1, (2, 0, 1, 1, 1));
    assert_eq!(decode_both(&bytes), Err(RecordError::MaxPartChunksZero));

    // The same record with the single field made legal decodes: `U_ref = min(2, 3) = 2`,
    // `max_sessions = min(2 / 2, 524_288) = 1`.
    assert!(decode_both(&ledger(0, 1, (2, 1, 1, 1, 1))).is_ok());
}

/// **G2** (`0016:1471`): at `max_inflight_parts = 0` no slot can ever be reserved, so no part
/// can be committed. Isolating — `U_ref = min(1x1, 1 + 0) = 1` and `max_sessions =
/// min(1 / 1, 524_288) = 1`, so G1 and G3–G8 all hold on this witness.
#[test]
fn g2_zero_inflight_parts_is_refused() {
    let bytes = ledger(0, 1, (1, 1, 1, 0, 1));
    assert_eq!(decode_both(&bytes), Err(RecordError::MaxInflightPartsZero));
}

/// **G3** — the `part:` key space (`MAX_PART_NUMBER`), the only bound `0016`'s knob table
/// leaves for `MAX_PARTS_PER_SESSION`. Isolating: `U_ref = min(1_000_001, 3) = 3`,
/// `max_sessions = min(3 / 3, 524_288) = 1`.
#[test]
fn g3_parts_per_session_past_the_key_space_is_refused() {
    assert_eq!(
        decode_both(&ledger(0, 1, (3, 1, 1_000_000, 1, 1))),
        Err(RecordError::PartsPerSessionUnaddressable {
            max_parts_per_session: 1_000_000,
        })
    );

    // The largest addressable value is accepted — the bound is the key space, not a knob.
    assert!(decode_both(&ledger(0, 1, (3, 1, 999_999, 1, 1))).is_ok());
}

/// **G4** (`0016:1471` clamp 1). Isolating: `U_ref = min((10+20)x1, 100 + 2x20x1) = 30` and
/// `max_sessions = min(1000 / 30, 524_288) = 33`, so only G4 fails.
#[test]
fn g4_more_parts_in_flight_than_the_session_may_hold_is_refused() {
    assert_eq!(
        decode_both(&ledger(0, 33, (1_000, 1, 10, 20, 100))),
        Err(RecordError::InflightPartsExceedParts {
            max_inflight_parts: 20,
            max_parts_per_session: 10,
        })
    );
}

/// **G5** (`0016:1471`, `:2098`) — owned `sidx:` per session must stay inside one
/// complete-or-fail scan. Isolating at one past the ceiling: `U_ref = min(1_048_578,
/// 1_048_579) = 1_048_578`, `max_sessions = min(1, 524_288) = 1`.
#[test]
fn g5_owned_staging_range_past_scan_cap_half_is_refused() {
    assert_eq!(
        decode_both(&ledger(0, 1, (1_048_578, 1, 524_289, 524_289, 1))),
        Err(RecordError::StagingRangeUnscannable {
            owned_sidx: 524_289,
        })
    );

    // Exactly at `SCAN_CAP/2` the record is legal — the rule is `<=`, not `<`.
    assert!(decode_both(&ledger(0, 1, (2 * SCAN_HALF, 1, 524_288, 524_288, 1))).is_ok());
}

/// **G6** (`0016:1468`, the lower end of `MAX_STAGED_CHUNKS`' settled range): at least one
/// maximal part must remain stageable. Isolating: `U_ref = min(2x165, 164 + 2x165) = 330`,
/// `max_sessions = min(330 / 330, 524_288) = 1`.
#[test]
fn g6_staged_ceiling_below_one_maximal_part_is_refused() {
    assert_eq!(
        decode_both(&ledger(0, 1, (330, 165, 1, 1, 164))),
        Err(RecordError::StagedChunksBelowPart {
            max_staged_chunks: 164,
            max_part_chunks: 165,
        })
    );

    // Equality is the lower end of the range, so it is accepted.
    assert!(decode_both(&ledger(0, 1, (330, 165, 1, 1, 165))).is_ok());
}

/// **G7** (`0016:1473`, `W_ref`'s range `[U_ref, deployment RAM]`). Isolating: `U_ref =
/// min(2, 3) = 2` against `w_ref = 1`, and the stored `max_sessions = 0` is exactly what that
/// pairing derives — so the record satisfies G8 and fails only G7. `w_ref == U_ref` is
/// accepted (the P3-below witness), so the rule is `<`, not `<=`.
#[test]
fn g7_reference_budget_below_one_session_is_refused() {
    assert_eq!(
        decode_both(&ledger(0, 0, (1, 1, 1, 1, 1))),
        Err(RecordError::BudgetBelowFootprint { w_ref: 1, u_ref: 2 })
    );
}

/// **G8**, the identity this record exists for (`0016:1470`): the legal ledger with its
/// `max_sessions` alone moved off what its own profile derives.
#[test]
fn g8_stored_limit_disagreeing_with_its_own_profile_is_refused() {
    let bytes = ledger(3, 5, (72_600, 165, 100, 10, 20_000));
    assert_eq!(
        decode_both(&bytes),
        Err(RecordError::MaxSessionsNotDerived {
            stored: 5,
            derived: 4,
        })
    );

    // The untyped surface refuses it too, and carries the reason as prose — the only claim S1
    // can make, since serde's `custom` funnel has stringified the typed error by then.
    let untyped = metadata::decode::<AdmissionRecord>(&bytes).unwrap_err();
    let prose = untyped.to_string();
    assert!(prose.contains("`max_sessions` 5 is not the"), "{prose}");
}

// ===========================================================================
// P1–P3 — what decode must NOT refuse
// ===========================================================================

/// **P1** — occupancy above the cap is legitimate live state, not a torn identity: a profile
/// lowered while sessions are live leaves the ledger over its new cap until they drain, and
/// the ledger is the record every teardown path must read to decrement `count`
/// (`0016:390-402`).
#[test]
fn p1_count_above_its_own_max_sessions_still_decodes() {
    let record = decode_both(&ledger(9_000, 4, (72_600, 165, 100, 10, 20_000)))
        .expect("occupancy above the cap is not a decode error");
    assert_eq!(record.count(), 9_000);
    assert_eq!(record.max_sessions(), 4);
}

/// **P2** — `max_staged_chunks` far above any publishable ceiling still decodes. That ceiling
/// is `MAX_ROOT_SEGMENTS x MAX_SEG_CHUNKS` (`0016:1468`), a *deployment-capacity* product
/// enforced at part commit as a `400 EntityTooLarge`; `MAX_SEG_CHUNKS` has no definition on
/// this base and `MAX_ROOT_SEGMENTS` is explicitly not a decode constant
/// (`metadata.rs:302-322`).
///
/// **This leg is superseded by #508**: when `max_chunkref_bytes` and `MAX_SEG_CHUNKS` land,
/// the *format* maxima derived from them become decode bounds and this leg must be rewritten
/// by that slice.
#[test]
fn p2_staged_ceiling_above_any_publishable_product_still_decodes() {
    let record = decode_both(&ledger(0, 1, (330, 165, 1, 1, 10_000_000)))
        .expect("a large staged ceiling is not a decode error");
    assert_eq!(record.profile().max_staged_chunks(), 10_000_000);
    assert_eq!(record.profile().u_ref(), 330);
}

/// **P3** — `max_part_chunks` outside the proposal's 165–381 window still decodes, below it
/// and far above it. That window is `max_chunkref_bytes`-derived (`0016:1466`) and #508's;
/// `0016` names `UploadPart` as its enforcement site, not decode.
///
/// **This leg is superseded by #508**, for the same reason as P2.
#[test]
fn p3_part_chunks_outside_the_proposal_window_still_decodes() {
    // Below: `U_ref = min(2, 3) = 2`, `max_sessions = min(2 / 2, 524_288) = 1`.
    let below = decode_both(&ledger(0, 1, (2, 1, 1, 1, 1))).expect("a small cap is not an error");
    assert_eq!(below.profile().max_part_chunks(), 1);

    // Far above: `U_ref = min(2x500_000, 500_000 + 2x500_000) = 1_000_000`, and G5 holds
    // because `1 x 500_000 <= 524_288`.
    let above = decode_both(&ledger(0, 1, (1_000_000, 500_000, 1, 1, 500_000)))
        .expect("a large cap is not an error");
    assert_eq!(above.profile().max_part_chunks(), 500_000);
    assert_eq!(above.profile().u_ref(), 1_000_000);
}

// ===========================================================================
// P-arith — decode's verdict equals exact integer arithmetic's, at the field maxima
// ===========================================================================

/// **P-arith-accept** — `max_staged_chunks` at its field maximum with everything else
/// minimal. In exact arithmetic `U_ref = min(2, u32::MAX + 2) = 2`, so every rule holds and
/// the record MUST decode: the term that leaves the field's width is the one the `min`
/// discards. A same-width `max_staged_chunks + 2 x mip x mpc` panics here in debug and wraps
/// in release — which is why the record is authored at the maximum rather than near it.
#[test]
fn p_arith_accept_maximal_staged_chunks_decodes() {
    let record = decode_both(&ledger(0, 1, (2, 1, 1, 1, u32::MAX)))
        .expect("the min discards the overflowing term");
    assert_eq!(record.profile().max_staged_chunks(), u32::MAX);
    assert_eq!(record.profile().u_ref(), 2);
    assert_eq!(record.profile().max_sessions(), 1);
}

/// **P-arith-reject** — every numeric field at the maximum its wire type admits, except the
/// two the key space bounds (`max_parts_per_session` and `max_inflight_parts` at
/// `MAX_PART_NUMBER`, so G3 and G4 hold) — G1, G2, G3, G4 and G6 all hold, and the overflow
/// lands squarely on the arithmetic rules.
///
/// In exact arithmetic `max_inflight_parts x max_part_chunks = 999_999 x 4_294_967_295 =
/// 4_294_963_000_032_705`, astronomically past `SCAN_CAP/2 = 524_288`, so **G5 is genuinely
/// violated** and decode must say so — by name, not by panic, and not by an `Ok` reached
/// through a wrapped product. (A same-width `u32` product panics here in debug.)
#[test]
fn p_arith_reject_maximal_record_names_the_rule_exact_arithmetic_violates() {
    let bytes = ledger(
        u64::MAX,
        u64::MAX,
        (u64::MAX, u32::MAX, 999_999, 999_999, u32::MAX),
    );
    assert_eq!(
        decode_both(&bytes),
        Err(RecordError::StagingRangeUnscannable {
            owned_sidx: 4_294_963_000_032_705,
        })
    );
}

// ===========================================================================
// The wire shape is closed, and unreadable bytes are their own fault
// ===========================================================================

/// Bytes both surfaces refuse as **this record class's own** fault, so a consumer can tell
/// "this ledger is torn" from "the store is failing".
fn is_malformed_mpuctl(bytes: &[u8]) -> bool {
    matches!(
        decode_both(bytes),
        Err(RecordError::MalformedRecordValue {
            namespace: "mpuctl",
            ..
        })
    )
}

/// The closed wire shape, and bytes that are not a ledger at all.
///
/// An unknown field is a loud typed error, at the record and inside the profile alike:
/// `mpuctl` is CAS'd whole, so a decoder that dropped one would either conflict forever (a
/// re-encoded precondition) or silently rewrite the record without it (a raw-bytes
/// precondition) — both durable, neither observable (`0016:348`).
#[test]
fn an_unknown_field_or_unreadable_bytes_are_refused_rather_than_dropped() {
    for bytes in [
        // An unknown field at the record level, then one inside the profile.
        br#"{"count":0,"max_sessions":1,"epoch":7,"profile":{"w_ref":2,"max_part_chunks":1,"max_parts_per_session":1,"max_inflight_parts":1,"max_staged_chunks":1}}"#.as_slice(),
        br#"{"count":0,"max_sessions":1,"profile":{"w_ref":2,"max_part_chunks":1,"max_parts_per_session":1,"max_inflight_parts":1,"max_staged_chunks":1,"b_ops":9}}"#.as_slice(),
        // A field outside its wire type's range — a wire fault, not a rule violation.
        br#"{"count":0,"max_sessions":1,"profile":{"w_ref":2,"max_part_chunks":4294967296,"max_parts_per_session":1,"max_inflight_parts":1,"max_staged_chunks":1}}"#.as_slice(),
        // Not JSON at all, and JSON that is not this record.
        b"not json".as_slice(),
        b"{}".as_slice(),
    ] {
        assert!(is_malformed_mpuctl(bytes), "{}", String::from_utf8_lossy(bytes));
    }
}
