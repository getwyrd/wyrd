//! Issue #655 — the multipart protocol's **numbers**: the knob constants of
//! `wyrd_core::multipart` section 14, their derivations, and `knob_clamps_hold`, the one check
//! that a whole set of them is consistent (proposal 0016's knob table, `0016:1462-1479`; slice 2
//! of 7 of #636). **Pure**: no store, no runtime, no fixture beyond literals and the production
//! codec.
//!
//! * **Leg 1** — the shipped set passes every clamp.
//! * **Leg 2** — `knob_clamps_hold` is not vacuous: for every clamp, a set sitting exactly on
//!   its bound passes and a set one step past it is refused **by name**. This is the binding
//!   leg: a check that answers `Ok` unconditionally passes leg 1 and fails here.
//! * **Leg 3** — every derived value recomputed from `0016`'s formulas by this file's own
//!   arithmetic, never through the production helpers, and equal to the shipped constant: the
//!   halvings `0016` states (`V/2`, `E_tx/2`, `W_ref/2`) pinned, both arms of `U_ref` driven,
//!   and the chunk-ref and segment-put worst cases measured on the codec.
//! * **Leg 4** — every capacity fits the key space #691 gave it, in byte order at the cap.
//! * **Leg 5** — the admission backoff is bounded and grows, and its two retry budgets are
//!   separate.
//!
//! Every section-14 symbol is added by the patch this file ships with, so a tree without that
//! patch does not compile this file: its red is the criterion's **absence**, the posture the
//! brief pre-declares. No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use wyrd_core::metadata::{
    encode, seg_key, ChunkRef, EcScheme, SegmentGroup, MAX_ROOT_SEGMENTS, MAX_SEGMENT_INDEX,
    MAX_VALUE_BYTES,
};
use wyrd_core::multipart::RecordError::{
    BudgetBelowFootprint, InflightPartsExceedParts, PartsPerSessionUnaddressable,
    StagedChunksBelowPart, StagingRangeUnscannable,
};
// The whole section-14 surface is under test, beside the key grammar and the slot codec it is
// measured against, so the module is imported whole.
use wyrd_core::multipart::*;
use wyrd_core::multipart::{
    KnobClamp as C, ADMISSION_BACKOFF_BASE_MILLIS as BASE, ADMISSION_BACKOFF_CAP_MILLIS as CAP,
};
use wyrd_traits::SCAN_CAP;

/// `SCAN_CAP/2` (`crates/traits/src/lib.rs:286`; `0016:1470`, `:1471`).
const SCAN_HALF: u64 = SCAN_CAP as u64 / 2;

/// FoundationDB's 10 MB transaction ceiling, every backend's de-facto `E_tx`
/// (`crates/traits/src/lib.rs:1328-1331`).
const E_TX_BYTES: u64 = 10_000_000;

/// What `0016`'s batch inventory lists for a part commit besides its owned `sidx:` deletes
/// (`0016:659`, `:675`): four preconditions — the session's, the part record's, its own slot's,
/// the re-upload obligation's `require_absent` — and four mutations — `part:`, `psum:`, the slot
/// delete, the re-upload `retire:bytes:` put. Counted here from the proposal, not imported.
const PART_COMMIT_FIXED_OPS: u32 = 8;

/// The same count for the reaper's idle fence besides its one pin per slot index (`0016:664`,
/// `:675`): the session precondition, two obligation `require_absent`s, three puts.
const FENCE_FIXED_OPS: u32 = 6;

/// The shipped set.
const D: KnobSet = KnobSet::DEPLOYED;

/// A scheme with a longer `placement` than the shipped one, so a wider chunk ref.
const WIDE: EcScheme = EcScheme::ReedSolomon { k: 10, m: 4 };

/// A profile small enough that `U_ref = min((1 + 1) × 1, 1 + 2 × 1 × 1) = 2`, so a session
/// limit and an owned fleet can be put exactly on their bounds with small numbers.
const TINY: KnobSet = KnobSet {
    max_part_chunks: 1,
    max_parts_per_session: 1,
    max_inflight_parts: 1,
    max_staged_chunks: 1,
    ..D
};

/// One `set => verdict;` row per line: `knob_clamps_hold(&set)` must be `verdict`, and a row
/// that is not names itself.
macro_rules! verdicts {
    ($($set:expr => $verdict:expr;)+) => {
        $(assert_eq!(knob_clamps_hold(&$set), $verdict, "{}", stringify!($set));)+
    };
}

/// `base` with one edit applied — how every leg-2 set is spelled, so each row reads as the field
/// it moves.
fn with(base: KnobSet, edit: impl FnOnce(&mut KnobSet)) -> KnobSet {
    let mut set = base;
    edit(&mut set);
    set
}

/// `base` with `slots` parts in flight, re-derived.
fn inflight(base: KnobSet, slots: u32) -> KnobSet {
    derived(with(base, |k| k.max_inflight_parts = slots))
}

/// `U_ref` (`0016:1469`), from the formula's own text.
fn u_ref(k: &KnobSet) -> u64 {
    let parts = u64::from(k.max_parts_per_session);
    let inflight = u64::from(k.max_inflight_parts);
    let chunks = u64::from(k.max_part_chunks);
    ((parts + inflight) * chunks).min(u64::from(k.max_staged_chunks) + 2 * inflight * chunks)
}

/// `k` with every derived member recomputed from its inputs by this file's arithmetic:
/// `MAX_SESSIONS = min(⌊W_ref / U_ref⌋, SCAN_CAP/2)` (`0016:1470`), `MAX_OWNED_FLEET =
/// MAX_SESSIONS × MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS` (`0016:1472`), and the contention
/// budget, which is `MAX_SESSIONS`.
fn derived(k: KnobSet) -> KnobSet {
    let sessions = (k.w_ref / u_ref(&k)).min(SCAN_HALF);
    KnobSet {
        max_sessions: sessions,
        max_owned_fleet: sessions * u64::from(k.max_inflight_parts) * u64::from(k.max_part_chunks),
        max_admission_cas_attempts: u32::try_from(sessions).unwrap(),
        ..k
    }
}

/// One chunk ref with every number at its widest rendering — `u128::MAX` id, `u64::MAX` length,
/// a `u64::MAX` D-server id per fragment — encoded here on the store codec.
fn widest_chunkref_bytes(scheme: EcScheme) -> usize {
    let fragments = match scheme {
        EcScheme::None => 1,
        EcScheme::ReedSolomon { k, m } => usize::from(k) + usize::from(m),
    };
    encode(&ChunkRef {
        id: u128::MAX,
        scheme,
        len: u64::MAX,
        placement: vec![u64::MAX; fragments],
    })
    .len()
}

/// `⌊(V/2) / b_ref⌋` for `scheme`, halving `MAX_VALUE_BYTES` itself.
fn capacity(scheme: EcScheme) -> u32 {
    u32::try_from((MAX_VALUE_BYTES / 2) / widest_chunkref_bytes(scheme)).unwrap()
}

/// What one slot index costs the idle fence, measured here: the key at the widest index, plus
/// the widest value `decode_slot_record` accepts — the bytes its `require(slot == prior)` holds.
fn slot_pin_bytes() -> u64 {
    let id = UploadId::new("f".repeat(32)).unwrap();
    let key = slot_key(&id, SlotIndex::new(MAX_SLOT_INDEX).unwrap());
    // The smallest reservation stamp as wide as `u64::MAX`, below the widest lease.
    let (at, expiry) = (u64::MAX - 1, u64::MAX);
    let value = format!(
        r#"{{"part_number":{MAX_PART_NUMBER},"attempt_id":"{id}","reserved_at_millis":{at},"lease_expiry_millis":{expiry}}}"#
    );
    decode_slot_record(value.as_bytes()).expect("the widest slot value is a legal one");
    (key.len() + value.len()) as u64
}

/// What one `seg:` put can carry, measured here: the widest key the segment grammar spells —
/// the largest epoch and index — plus a value at the ceiling, the whole `V` `0016:661` charges.
fn seg_put_bytes() -> u64 {
    let group = SegmentGroup::new("f".repeat(32), u64::MAX).unwrap();
    (seg_key(&group, MAX_SEGMENT_INDEX).unwrap().len() + MAX_VALUE_BYTES) as u64
}

// ===========================================================================
// Leg 1 — the shipped set passes every clamp.
// ===========================================================================

#[test]
fn leg1_the_shipped_set_passes_every_clamp() {
    assert_eq!(knob_clamps_hold(&KnobSet::DEPLOYED), Ok(()));
}

// ===========================================================================
// Leg 2 — every clamp: a set on its bound passes, one step past it is refused by name.
// ===========================================================================

/// The binding leg. Each refused set is a passing set moved one unit past one bound — re-derived
/// where the step moves a profile number, so the clamp named is the one the step broke. A few
/// steps cannot help breaking a second clamp: a zero cap; a derived value past its bound (it is
/// then no longer its derivation either); a `W_ref` below one footprint; and one past
/// `SCAN_CAP/2`, which at any legal `B_ops` also overruns the idle fence. `knob_clamps_hold`
/// names the first it checks, and that is the one asserted.
#[test]
fn leg2_each_clamp_passes_its_bound_and_names_itself_one_step_past_it() {
    let (seg, part_ops) = (seg_put_bytes(), MAX_PART_CHUNKS + PART_COMMIT_FIXED_OPS);
    // `B_ops` at the whole five seconds — the ceiling the shipped value halves (leg 3).
    let (fence_edge, ops_top) = (MAX_BATCH_OPS - FENCE_FIXED_OPS, 2 * MAX_BATCH_OPS);
    // `B_bytes` at its floor and `B_ops` at its ceiling, so the idle fence's byte clamp binds
    // before its operation clamp; `pins` is the most slots whose pins fit, by this file's own
    // measurement of a pin, so `pins + 1` must cross the production bound.
    let floor = with(D, |k| (k.batch_bytes, k.batch_ops) = (seg, ops_top));
    let pins = u32::try_from(seg / slot_pin_bytes()).unwrap();
    let scan_inflight = u32::try_from(SCAN_CAP).unwrap() / (2 * MAX_PART_CHUNKS) + 1;
    let at_scan_half = derived(with(TINY, |k| k.w_ref = 2 * (SCAN_HALF + 1)));
    let (past, past_cas) = (SCAN_HALF + 1, u32::try_from(SCAN_HALF + 1).unwrap());
    let fleet_at_half = derived(with(TINY, |k| k.w_ref = 2_000));
    verdicts! {
        // On each bound. `D` holds four: every chunk cap at capacity, staging at the publishable
        // ceiling, `B_bytes` at `E_tx/2`, the contention budget at `MAX_SESSIONS`.
        D => Ok(());
        with(D, |k| k.batch_bytes = seg) => Ok(());
        with(D, |k| k.batch_ops = ops_top) => Ok(());
        with(D, |k| k.batch_ops = part_ops) => Ok(());
        inflight(D, fence_edge) => Ok(());
        inflight(floor, pins) => Ok(());
        with(D, |k| k.w_ref = MAX_SESSIONS * U_REF) => Ok(());
        at_scan_half => Ok(());
        fleet_at_half => Ok(());
        with(D, |k| (k.r_publish, k.max_complete_attempts, k.max_upload_id_attempts) = (1, 1, 1))
            => Ok(());

        // One step past. A scheme the erasure coder cannot run (ADR-0045's `EcScheme` row).
        with(D, |k| k.scheme = EcScheme::ReedSolomon { k: 6, m: 0 }) => Err(C::SchemeUnsupported);
        // `max_chunkref_bytes × chunks > V/2` for each one-value cap, each at zero, and the
        // chunk ref grown under the shipped caps by a wider scheme (`0016:1464-1466`).
        with(D, |k| k.max_map_chunks += 1) => Err(C::ValueCeiling("MAX_MAP_CHUNKS"));
        with(D, |k| k.max_map_chunks = 0) => Err(C::ValueCeiling("MAX_MAP_CHUNKS"));
        with(D, |k| k.max_seg_chunks += 1) => Err(C::ValueCeiling("MAX_SEG_CHUNKS"));
        with(D, |k| k.max_seg_chunks = 0) => Err(C::ValueCeiling("MAX_SEG_CHUNKS"));
        derived(with(D, |k| k.max_part_chunks += 1)) => Err(C::ValueCeiling("MAX_PART_CHUNKS"));
        with(D, |k| k.max_part_chunks = 0) => Err(C::ValueCeiling("MAX_PART_CHUNKS"));
        with(D, |k| k.scheme = WIDE) => Err(C::ValueCeiling("MAX_MAP_CHUNKS"));
        // `MAX_INFLIGHT_PARTS` above `MAX_PARTS_PER_SESSION` and above
        // `⌊SCAN_CAP / (2 × MAX_PART_CHUNKS)⌋` (`0016:1471`, clamps 1 and 2); `MAX_STAGED_CHUNKS`
        // below `MAX_PART_CHUNKS`; `W_ref` below one footprint (`0016:1468`, `:1473`).
        with(D, |k| k.max_parts_per_session = MAX_INFLIGHT_PARTS - 1)
            => Err(C::Profile(InflightPartsExceedParts {
                max_inflight_parts: MAX_INFLIGHT_PARTS,
                max_parts_per_session: MAX_INFLIGHT_PARTS - 1 }));
        inflight(D, scan_inflight) => Err(C::Profile(StagingRangeUnscannable {
            owned_sidx: u128::from(scan_inflight) * u128::from(MAX_PART_CHUNKS) }));
        derived(with(D, |k| k.max_staged_chunks = MAX_PART_CHUNKS - 1))
            => Err(C::Profile(StagedChunksBelowPart {
                max_staged_chunks: MAX_PART_CHUNKS - 1, max_part_chunks: MAX_PART_CHUNKS }));
        with(D, |k| k.w_ref = U_REF - 1) => Err(C::Profile(BudgetBelowFootprint {
            w_ref: U_REF - 1, u_ref: U_REF.into() }));
        // `MAX_STAGED_CHUNKS` above `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS` (`0016:1468`).
        derived(with(D, |k| k.max_staged_chunks += 1)) => Err(C::StagedAbovePublishable);
        // `B_bytes` past `E_tx/2` and below the one segment put a batch must carry; `B_ops`
        // past the five seconds (`0016:1475`, `:661`).
        with(D, |k| k.batch_bytes = E_TX_BYTES / 2 + 1) => Err(C::BatchBytesAboveHalfEnvelope);
        with(D, |k| k.batch_bytes = seg - 1) => Err(C::BatchBytesBelowSegmentPut);
        with(D, |k| k.batch_ops = ops_top + 1) => Err(C::BatchOpsAboveDeadline);
        // The part commit one operation over `B_ops`, and `MAX_PART_CHUNKS > B_ops` (`0016:1466`).
        with(D, |k| k.batch_ops = part_ops - 1) => Err(C::PartCommitOverOps);
        with(D, |k| k.batch_ops = MAX_PART_CHUNKS - 1) => Err(C::PartCommitOverOps);
        // The whole-range fence one pin over the mutation-byte budget, one operation over
        // `B_ops`, and `MAX_INFLIGHT_PARTS > B_ops` (`0016:1471`).
        inflight(floor, pins + 1) => Err(C::SlotRangeOverBytes);
        inflight(D, fence_edge + 1) => Err(C::SlotRangeOverOps);
        inflight(D, MAX_BATCH_OPS + 1) => Err(C::SlotRangeOverOps);
        // `MAX_SESSIONS × U_ref > W_ref`; `MAX_SESSIONS > SCAN_CAP/2` with its footprint exactly
        // on `W_ref`; a `MAX_SESSIONS` below its derivation (`0016:1470`).
        with(D, |k| k.max_sessions += 1) => Err(C::SessionsOverFootprint);
        with(at_scan_half, |k| (k.max_sessions, k.max_admission_cas_attempts) = (past, past_cas))
            => Err(C::SessionsOverScanHalf);
        with(D, |k| k.max_sessions -= 1) => Err(C::SessionsNotDerived);
        // `MAX_OWNED_FLEET > W_ref / 2`, and one below its derivation (`0016:1472`).
        with(fleet_at_half, |k| k.max_owned_fleet += 1) => Err(C::OwnedFleetOverHalfRef);
        with(D, |k| k.max_owned_fleet -= 1) => Err(C::OwnedFleetNotDerived);
        // A retry bound of zero, each one; a contention budget one short of the room.
        with(D, |k| k.r_publish = 0) => Err(C::RetryBoundZero("R_PUBLISH"));
        with(D, |k| k.max_complete_attempts = 0) => Err(C::RetryBoundZero("MAX_COMPLETE_ATTEMPTS"));
        with(D, |k| k.max_upload_id_attempts = 0)
            => Err(C::RetryBoundZero("MAX_UPLOAD_ID_ATTEMPTS"));
        with(D, |k| k.max_admission_cas_attempts = 0)
            => Err(C::RetryBoundZero("MAX_ADMISSION_CAS_ATTEMPTS"));
        with(D, |k| k.max_admission_cas_attempts -= 1) => Err(C::AdmissionCasBelowSessions);
    }
}

// ===========================================================================
// Leg 3 — every derived value recomputed here, never through the production helpers.
// ===========================================================================

#[test]
fn leg3_the_chunk_caps_are_half_a_value_over_the_measured_worst_chunk_ref() {
    // `b_ref` measured on the codec (`0016:1050-1053`), so a constant that drifted from the
    // encoded reality is caught — and the per-scheme companion measures the same thing.
    let measured = widest_chunkref_bytes(SIZING_SCHEME);
    assert_eq!(MAX_CHUNKREF_BYTES, measured);
    let schemes = [SIZING_SCHEME, EcScheme::None, WIDE];
    let per_scheme = schemes.map(max_chunkref_bytes_for);
    assert_eq!(per_scheme, schemes.map(widest_chunkref_bytes));
    // `V/2` — the halving `0016:1053` states — taken from `MAX_VALUE_BYTES` itself, so a rule
    // sized against a quarter of the value, or all of it, fails here. One number for all three
    // single-value caps; the staged ceiling is the publishable product; a part is its chunks.
    assert_eq!(VALUE_CHUNK_CAPACITY, capacity(SIZING_SCHEME));
    let caps = [MAX_MAP_CHUNKS, MAX_SEG_CHUNKS, MAX_PART_CHUNKS];
    assert_eq!(caps, [VALUE_CHUNK_CAPACITY; 3]);
    let publishable = MAX_ROOT_SEGMENTS as u64 * u64::from(MAX_SEG_CHUNKS);
    assert_eq!(u64::from(MAX_STAGED_CHUNKS), publishable);
    let (chunk_size, part) = (31 << 20, u64::from(MAX_PART_CHUNKS));
    assert_eq!(max_part_bytes(chunk_size), part * chunk_size);
    // `0016`'s stated range (`0016:1050-1053`): the rule at `0016`'s own two `b_ref` figures
    // lands on the range's two ends, and fails closed at a zero-byte ref…
    assert_eq!([131, 302, 0].map(value_chunk_capacity), [381, 165, 0]);
    // …and the measured worst case is no narrower than `0016`'s widest figure (which renders
    // `len` for a 1 MiB chunk), so the shipped caps sit at or below that range's conservative
    // end — never above it on an optimistic `b_ref`.
    assert!(measured >= 302, "measured b_ref {measured}");
    const { assert!(VALUE_CHUNK_CAPACITY <= 165) };
    // The segment put a batch's byte floor is (`0016:661`), measured on the key grammar.
    assert_eq!(MAX_SEGMENT_PUT_BYTES, seg_put_bytes());
}

#[test]
fn leg3_u_ref_and_the_session_limit_follow_the_formulas_under_either_arm() {
    // The shipped profile: the staged-ceiling arm of `U_ref` binds (`0016:1469`).
    let (inflight, chunks) = (u64::from(MAX_INFLIGHT_PARTS), u64::from(MAX_PART_CHUNKS));
    let ceiling = u64::from(MAX_STAGED_CHUNKS) + 2 * inflight * chunks;
    let raw = (u64::from(MAX_PARTS_PER_SESSION) + inflight) * chunks;
    assert!(ceiling < raw);
    assert_eq!((U_REF, u_ref(&D)), (ceiling, ceiling));
    assert_eq!(MAX_SESSIONS, (W_REF / U_REF).min(SCAN_HALF));
    assert_eq!(MAX_OWNED_FLEET, MAX_SESSIONS * inflight * chunks);
    assert_eq!(KnobSet::DEPLOYED, derived(KnobSet::DEPLOYED));
    // The raw arm — `0016`'s own small-part example (`0016:2847`: `MAX_PART_CHUNKS = 5`,
    // `MAX_INFLIGHT_PARTS = 16` ⇒ `U_ref = 50,080` ⇒ 79 sessions at `W_ref = 4,000,000`) — and
    // the `SCAN_CAP/2` term, when a large `W_ref` meets a small `U_ref` (`0016:1470`). The
    // production accepts each derived set, and refuses it one session short as not derived.
    let small = derived(with(D, |k| k.max_part_chunks = 5));
    assert_eq!((u_ref(&small), small.max_sessions), (50_080, 79));
    let clamped = derived(with(TINY, |k| k.w_ref = 10 * SCAN_CAP as u64));
    assert_eq!(clamped.max_sessions, SCAN_HALF);
    verdicts! {
        small => Ok(());
        with(small, |k| k.max_sessions -= 1) => Err(C::SessionsNotDerived);
        clamped => Ok(());
        with(clamped, |k| k.max_sessions -= 1) => Err(C::SessionsNotDerived);
    }
}

#[test]
fn leg3_each_budget_takes_the_half_0016_states() {
    // `E_tx/2` (`0016:1475`): half of FoundationDB's 10 MB, not a quarter and not all of it.
    assert_eq!(MAX_BATCH_BYTES, E_TX_BYTES / 2);
    // `W_ref/2` (`0016:1472`): an owned fleet of exactly half its `W_ref` passes, and one entry
    // more is refused by that bound — so the bound is neither a quarter (the first would fail)
    // nor the whole `W_ref` (the second would name another clamp). `B_ops` keeps half the five
    // seconds as margin (`0016:640-642`): twice the shipped value is the most production takes.
    let half = derived(with(TINY, |k| k.w_ref = 2_000));
    assert_eq!(half.max_owned_fleet, half.w_ref / 2);
    let twice = with(D, |k| k.batch_ops = 2 * MAX_BATCH_OPS);
    verdicts! {
        half => Ok(());
        with(half, |k| k.max_owned_fleet += 1) => Err(C::OwnedFleetOverHalfRef);
        twice => Ok(());
        with(twice, |k| k.batch_ops += 1) => Err(C::BatchOpsAboveDeadline);
    }
}

// ===========================================================================
// Leg 4 — every capacity fits the key space #691 gave it, in byte order at the cap.
// ===========================================================================

#[test]
fn leg4_every_capacity_fits_its_key_space_in_byte_order_at_the_cap() {
    let id = UploadId::new("0123456789abcdef0123456789abcdef").unwrap();
    let slot = |index| slot_key(&id, SlotIndex::new(index).unwrap());
    let part = |number| part_key(&id, PartNumber::new(number).unwrap());
    // Every slot index the in-flight cap opens, and every part number the session cap admits,
    // is one the width can spell (`SlotIndex::new`/`PartNumber::new` refuse any other), and key
    // order is numeric order through the cap — the property that makes the `slot:` key space the
    // bound (`0016:349`), since a range scan then enumerates exactly the indices below the cap.
    assert!((1..MAX_INFLIGHT_PARTS).all(|index| slot(index - 1) < slot(index)));
    assert!((2..=MAX_PARTS_PER_SESSION).all(|number| part(number - 1) < part(number)));
    // A part cap past the part-number width is refused; so is an in-flight cap past the
    // slot-index width — by the in-flight ≤ parts clamp, which always binds first, since the
    // widest legal part cap opens no index the slot grammar cannot spell.
    const { assert!(MAX_PART_NUMBER <= MAX_SLOT_INDEX + 1) };
    let (wide, slots) = (MAX_PART_NUMBER + 1, MAX_SLOT_INDEX + 2);
    verdicts! {
        with(D, |k| k.max_parts_per_session = wide)
            => Err(C::Profile(PartsPerSessionUnaddressable { max_parts_per_session: wide }));
        with(D, |k| (k.max_parts_per_session, k.max_inflight_parts) = (wide - 1, slots))
            => Err(C::Profile(InflightPartsExceedParts {
                max_inflight_parts: slots, max_parts_per_session: wide - 1 }));
    }
}

// ===========================================================================
// Leg 5 — the admission backoff is bounded and grows; its two retry budgets are separate.
// ===========================================================================

#[test]
fn leg5_the_backoff_is_bounded_jittered_and_grows_with_the_attempt() {
    // Both ends of the jitter range, every power of two, and a few irregular values.
    let jitters: Vec<u64> = (0..64)
        .map(|shift| 1u64 << shift)
        .chain([0, 3, 1_000_003, u64::MAX / 3, u64::MAX - 1, u64::MAX])
        .collect();
    for attempt in 0..12 {
        // The window the backoff states: `BASE × 2^(attempt+1)`, capped — so it doubles per
        // attempt until the cap, and both of its ends are reached exactly.
        let window = (BASE << (attempt + 1)).min(CAP);
        let delay = |jitter| admission_backoff_millis(attempt, jitter);
        assert_eq!((delay(0), delay(u64::MAX)), (BASE, window));
        let delays: BTreeSet<u64> = jitters.iter().map(|&jitter| delay(jitter)).collect();
        assert!(delays.iter().all(|d| (BASE..=window).contains(d)));
        assert!(delays.len() > 2, "attempt {attempt}: {delays:?}");
        // For every jitter the delay never shrinks as the attempt grows.
        let next = |jitter| admission_backoff_millis(attempt + 1, jitter);
        assert!(jitters.iter().all(|&jitter| next(jitter) >= delay(jitter)));
    }
    assert_eq!(admission_backoff_millis(u32::MAX, u64::MAX), CAP);
}

#[test]
fn leg5_the_two_retry_budgets_are_separate_and_contention_outlasts_the_room() {
    // One budget for both was the carried-forward `503 SlowDown` defect.
    assert_ne!(MAX_UPLOAD_ID_ATTEMPTS, MAX_ADMISSION_CAS_ATTEMPTS);
    // That defect as arithmetic: `MAX_SESSIONS` creators race an empty ledger through its one
    // serialized CAS; each round admits one, so the last admitted lost `MAX_SESSIONS - 1` times
    // and needed `MAX_SESSIONS` attempts. The contention budget covers it, and the collision
    // budget, sized for a 2^-128 event, does not: sharing it would refuse that storm on an empty
    // store.
    assert!(u64::from(MAX_ADMISSION_CAS_ATTEMPTS) >= MAX_SESSIONS);
    assert!(u64::from(MAX_UPLOAD_ID_ATTEMPTS) < MAX_SESSIONS);
}
