//! Issue #691 — slice 1/3 of #654's own re-split (itself slice 1/7 of #636, proposal 0016).
//! The multipart commit protocol's **key grammar and the validated identity types it is
//! spelled in** — `wyrd_core::multipart::{RecordError, UploadId, AttemptId, PartNumber,
//! SlotIndex, Digest}` and the key constructors/parsers for all seven keyed classes plus
//! the `retire:` token grammar. **Pure**: no store, no async, no fixture beyond literals.
//!
//! Five legs, in the order the issue states them:
//! 1. round-trip + canonical rejection, per keyed class;
//! 2. byte-lexicographic order equals numeric order, across every digit-width boundary;
//! 3. no key prefix is a prefix of another, over the full protocol key space — and no
//!    bounded per-session range selects outside its own class and session;
//! 4. the `retire:` token grammar distinguishes its two forms;
//! 5. the pinned format constants hold.
//!
//! Ahead of leg 1 sit the identity types themselves — the token grammar, the accessors
//! **by value**, the digest's hex form over every byte value, the `Deserialize` routes and
//! the typed errors every rejection surfaces as — because a key is only as canonical as the
//! components it is spelled from.
//!
//! No `#![cfg(...)]` here (the gate's vacuous-green hazard) — this file always compiles
//! and always runs.

#![forbid(unsafe_code)]

use wyrd_core::metadata;
use wyrd_core::multipart::{
    self, mpu_key, parse_mpu_key, parse_part_key, parse_psum_key, parse_retire_key,
    parse_retire_mode, parse_sidx_key, parse_slot_key, part_key, part_range, psum_key, psum_range,
    retire_key, retire_session_range, sidx_key, sidx_range, slot_key, slot_range, AttemptId,
    Digest, PartNumber, RecordError, RetireMode, RetireToken, SlotIndex, UploadId, MAX_PART_NUMBER,
    MAX_SLOT_INDEX, MPUCTL_KEY, MPU_PREFIX, PART_NUMBER_WIDTH, PART_PREFIX, PSUM_PREFIX,
    RETIRE_BYTES_PREFIX, RETIRE_RECORDS_PREFIX, SIDX_PREFIX, SLOT_INDEX_WIDTH, SLOT_PREFIX,
    TOKEN_HEX_LEN,
};

// ===========================================================================
// Literals and small helpers
// ===========================================================================

/// A [`TOKEN_HEX_LEN`]-character token built by repeating `pattern`, so a test never has
/// to hand-count hex characters (an off-by-one there would silently under-test).
fn hex_token(pattern: &str) -> String {
    let mut s = String::new();
    while s.len() < TOKEN_HEX_LEN {
        s.push_str(pattern);
    }
    s.truncate(TOKEN_HEX_LEN);
    s
}

fn upload(pattern: &str) -> UploadId {
    UploadId::new(hex_token(pattern)).expect("a well-formed token is an upload id")
}

fn attempt(pattern: &str) -> AttemptId {
    AttemptId::new(hex_token(pattern)).expect("a well-formed token is an attempt id")
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number inside the key space")
}

/// `text` with a trailing byte that is not UTF-8 — every parser's non-UTF-8 case.
fn non_utf8(text: &str) -> Vec<u8> {
    let mut key = text.as_bytes().to_vec();
    key.push(0xFF);
    key
}

/// Every short, over-wide, signed, whitespace-padded and non-decimal spelling of "the
/// number 7" (or a variant of it) at [`PART_NUMBER_WIDTH`]/[`SLOT_INDEX_WIDTH`] = 6. Bare
/// `"7"` and the padded-but-short `"007"` are named explicitly — the carried-forward
/// MUST-FIX the archived #654 v2 review found missing from its fixed-width table
/// (`multipart_records.rs:162` in that attempt).
const NUMERIC_ADVERSITIES: [&str; 13] = [
    "7", "07", "007", "0007", "00007", "0000007", "+00007", "-00007", " 00007", "00007 ", "00_007",
    "abcdef", "",
];

fn assert_rejected<T: std::fmt::Debug>(label: &str, key: &[u8], result: Result<T, RecordError>) {
    assert!(
        result.is_err(),
        "{label}: expected rejection for key {:?}, got {:?}",
        String::from_utf8_lossy(key),
        result
    );
}

// ===========================================================================
// The validated identity types
// ===========================================================================

#[test]
fn upload_and_attempt_ids_validate_the_token_grammar() {
    let token = hex_token("0123456789abcdef");
    assert!(UploadId::new(token.clone()).is_ok());
    assert!(AttemptId::new(hex_token("fedcba9876543210")).is_ok());
    assert!(
        multipart::is_token(&token),
        "the token predicate must accept what the constructors accept"
    );
    assert!(!multipart::is_token("nope"));

    for candidate in [
        String::new(),                          // empty — would name every session's range
        "aa:bb".to_string(),                    // carries the key separator
        token.to_uppercase(),                   // uppercase: a second spelling of one token
        hex_token("g"),                         // a non-hex letter
        token[..TOKEN_HEX_LEN - 1].to_string(), // short by one
        format!("{token}0"),                    // long by one
    ] {
        assert!(
            UploadId::new(candidate.clone()).is_err(),
            "upload id {candidate:?} must be rejected"
        );
        assert!(
            AttemptId::new(candidate.clone()).is_err(),
            "attempt id {candidate:?} must be rejected"
        );
    }
}

/// The accessors are load-bearing, so they are asserted **by value**, not merely exercised:
/// `UploadId::as_str` / `AttemptId::as_str` are what every key, every per-session range and
/// every `retire:` token is spelled from, so one returning some *other* string would mint a
/// key naming a different session (or, empty, every session's range) — residue nothing
/// enumerates and therefore nothing reclaims (`docs/principles.md` §5 C-1). An `is_ok()`
/// check cannot see that; an equality against the token the id was built from can.
#[test]
fn the_identity_accessors_return_the_value_they_were_built_from() {
    let upload_token = hex_token("0123456789abcdef");
    let attempt_token = hex_token("fedcba9876543210");
    let uid = UploadId::new(upload_token.clone()).unwrap();
    let att = AttemptId::new(attempt_token.clone()).unwrap();

    assert_eq!(uid.as_str(), upload_token);
    assert_eq!(uid.to_string(), upload_token);
    assert_eq!(att.as_str(), attempt_token);
    assert_eq!(att.to_string(), attempt_token);
    assert_ne!(
        att.as_str(),
        uid.as_str(),
        "two distinct tokens must not read back as one"
    );

    // The `retire:` per-part token is the one key component spelled from an attempt id, so
    // it is where a wrong `as_str` would actually land in a stored key.
    let key = retire_key(
        RetireMode::Bytes,
        &RetireToken::Session {
            upload_id: uid,
            epoch: 3,
            part: Some((part(4), att)),
        },
    );
    assert_eq!(
        String::from_utf8(key).unwrap(),
        format!("retire:bytes:s:{upload_token}:3:4:{attempt_token}")
    );

    // `get` is the other accessor a later slice will spell a key from.
    assert_eq!(part(7).get(), 7);
    assert_eq!(SlotIndex::new(7).unwrap().get(), 7);
}

/// A digest's canonical rendering, pinned as a literal — reused by the serde route below.
/// **Letters** in the high nibble (`f`,`e`,`d`,`c`,`b`,`a`) and digits in the low one.
const DIGEST_HEX_LETTER_LED: &str =
    "f0e1d2c3b4a59687f0e1d2c3b4a59687f0e1d2c3b4a59687f0e1d2c3b4a59687";

/// Exactly the bytes [`DIGEST_HEX_LETTER_LED`] renders, as a literal — so the pair pins
/// the mapping in both directions and neither side can drift to match the other.
const LETTER_LED_BYTES: [u8; 32] = [
    0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87, 0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87,
    0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87, 0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87,
];

/// Encode and decode pinned as **exact inverses over all 256 byte values**, against an
/// independent oracle (`format!("{b:02x}")`, std's own rendering), and bound to the *bytes*
/// in both directions. A digest is an **identity**: `complete_fingerprint` tells an identical
/// retry from a different assembly by it (`0016:350`), so one wrong entry in the nibble table
/// — the `0`–`9` half is the one the archived attempt's `0xAB`-only vector could not see —
/// either renders two distinct digests alike or decodes one to the wrong bytes.
#[test]
fn digest_hex_is_the_exact_inverse_over_every_byte_value() {
    for block in 0..8u32 {
        let mut raw = [0u8; 32];
        for (i, byte) in raw.iter_mut().enumerate() {
            let value = block * 32 + u32::try_from(i).expect("0..32 fits in a u32");
            *byte = u8::try_from(value).expect("0..256 fits in a u8");
        }
        let expected: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            multipart::hex_lower(&raw),
            expected,
            "hex_lower diverges from the std rendering at block {block}"
        );
        assert_eq!(Digest::from_bytes(raw).to_hex(), expected);
        assert_eq!(
            Digest::from_hex(&expected)
                .expect("the canonical rendering must parse")
                .as_bytes(),
            &raw,
            "a digest must decode to exactly the bytes it was rendered from"
        );
    }
    assert_eq!(
        Digest::from_hex(DIGEST_HEX_LETTER_LED).unwrap(),
        Digest::from_bytes(LETTER_LED_BYTES)
    );
}

/// The accepted alphabet and the accepted length, stated **totally**: over every one of the
/// 256 byte values a digest character parses **iff** it is a lowercase hex digit. `is_err()`
/// spot checks can only sample that; the equivalence pins it, and with it the refusal of
/// uppercase — `A` and `a` would otherwise be two spellings of one digest.
#[test]
fn digest_hex_rejects_every_second_spelling() {
    for byte in 0..=u8::MAX {
        let candidate = char::from(byte);
        for at in [0usize, 63] {
            let mut spoiled = DIGEST_HEX_LETTER_LED.to_string();
            spoiled.replace_range(at..=at, &candidate.to_string());
            assert_eq!(
                Digest::from_hex(&spoiled).is_ok(),
                "0123456789abcdef".contains(candidate),
                "digest character {candidate:?} ({byte:#04x}) at {at}: accepted iff lowercase hex"
            );
        }
    }
    for text in [
        DIGEST_HEX_LETTER_LED.to_uppercase(),    // uppercase
        DIGEST_HEX_LETTER_LED[..63].to_string(), // short
        format!("{DIGEST_HEX_LETTER_LED}0"),     // over-wide
        String::new(),                           // empty
    ] {
        assert!(
            Digest::from_hex(&text).is_err(),
            "digest {text:?} must be refused"
        );
    }
}

/// The decode route a stored record will arrive by. Every identity's `Deserialize` runs the
/// **same** validating constructor a call site would (ADR-0045), so a hand-edited or
/// corrupted record cannot deliver an identity the key grammar could not spell — and
/// `Serialize` stays transparent, so the stored form is the plain JSON scalar it always was
/// and the types remain a compile-time rule rather than a wire change
/// (`metadata.rs:729-732`'s reasoning for `SegmentNonce`).
#[test]
fn serde_decode_routes_through_the_validating_constructors() {
    let token = hex_token("0123456789abcdef");
    let quoted = format!("\"{token}\"");
    round_trips(&UploadId::new(token.clone()).unwrap(), &quoted);
    round_trips(&AttemptId::new(token).unwrap(), &quoted);
    round_trips(&part(7), "7");
    round_trips(&SlotIndex::new(0).unwrap(), "0");
    round_trips(
        &Digest::from_bytes(LETTER_LED_BYTES),
        &format!("\"{DIGEST_HEX_LETTER_LED}\""),
    );

    // Every refusal the constructors make, made again on the wire.
    let past_part = (u64::from(MAX_PART_NUMBER) + 1).to_string();
    let past_slot = (u64::from(MAX_SLOT_INDEX) + 1).to_string();
    let upper = format!("\"{}\"", DIGEST_HEX_LETTER_LED.to_uppercase());
    assert!(serde_json::from_str::<UploadId>("\"nothex\"").is_err());
    assert!(serde_json::from_str::<AttemptId>("\"nothex\"").is_err());
    assert!(serde_json::from_str::<PartNumber>("0").is_err(), "zero");
    assert!(serde_json::from_str::<PartNumber>(&past_part).is_err());
    assert!(serde_json::from_str::<SlotIndex>(&past_slot).is_err());
    assert!(serde_json::from_str::<Digest>(&upper).is_err());
}

/// `Serialize` is transparent — the stored form stays the plain JSON scalar it always was,
/// so the validated types are a compile-time rule and not a wire change
/// (`metadata.rs:729-732`'s reasoning for `SegmentNonce`) — and `Deserialize` comes back
/// through the same validating constructor a call site would run (ADR-0045).
fn round_trips<T>(value: &T, json: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(&serde_json::from_str::<T>(json).unwrap(), value);
}

// ===========================================================================
// Leg 1 — round-trip + canonical rejection, per keyed class
// ===========================================================================

/// The canonical spelling of every key and every bounded range, pinned **to the byte**. A
/// constructor that dropped a separator, reordered its fields or padded to another width
/// would still round-trip through its own parser; only a literal catches it — and these
/// spellings are the stored format every later slice and sibling issue builds on.
#[test]
fn the_canonical_spelling_of_every_key_is_pinned_to_the_byte() {
    let id = hex_token("0123456789abcdef");
    let uid = UploadId::new(id.clone()).unwrap();
    let att = hex_token("fedcba9876543210");
    let n42 = part(42);
    let slot42 = SlotIndex::new(42).unwrap();
    let token = session_token(&uid, Some((part(4), AttemptId::new(att.clone()).unwrap())));
    let cases: [(Vec<u8>, String); 12] = [
        (MPUCTL_KEY.to_vec(), "mpuctl".to_string()),
        (mpu_key(&uid), format!("mpu:{id}")),
        (slot_key(&uid, slot42), format!("slot:{id}:000042")),
        (slot_range(&uid), format!("slot:{id}:")),
        (part_key(&uid, n42), format!("part:{id}:000042")),
        (part_range(&uid), format!("part:{id}:")),
        (psum_key(&uid, n42), format!("psum:{id}:000042")),
        (psum_range(&uid), format!("psum:{id}:")),
        (sidx_key(&uid, n42, 7), format!("sidx:{id}:000042:7")),
        (sidx_range(&uid), format!("sidx:{id}:")),
        (
            retire_key(RetireMode::Bytes, &token),
            format!("retire:bytes:s:{id}:1:4:{att}"),
        ),
        (
            retire_session_range(RetireMode::Records, &uid),
            format!("retire:records:s:{id}:"),
        ),
    ];
    for (actual, expected) in cases {
        assert_eq!(String::from_utf8(actual).unwrap(), expected);
    }
}

#[test]
fn mpu_key_round_trips_and_rejects_noncanonical() {
    let ids = [upload("0123456789abcdef"), upload("fedcba9876543210")];
    for uid in ids.into_iter().chain([upload("0")]) {
        let key = mpu_key(&uid);
        assert_eq!(key, format!("mpu:{uid}").into_bytes());
        assert_eq!(parse_mpu_key(&key).unwrap(), uid);
    }

    let id = hex_token("0123456789abcdef");
    for key in [
        "mpu:".to_string(), // an empty upload id — would name every session
        // A single-field key (`splitn(1, ':')`) folds "trailing component" and "id
        // containing ':'" into the same shape: the whole remainder becomes one field,
        // which then fails the token grammar.
        format!("mpu:{id}:extra"), // trailing component / an id carrying ':'
        "mpu".to_string(),         // truncated: no separator
        "mp:x".to_string(),        // the wrong prefix
    ] {
        assert_rejected("mpu:", key.as_bytes(), parse_mpu_key(key.as_bytes()));
    }
    let key = non_utf8("mpu:");
    assert_rejected("mpu: non-UTF-8 bytes", &key, parse_mpu_key(&key));
}

/// Leg 1's other half: a rejection is a **typed** error that names the violation, never a
/// bare "something was wrong" — the signal an operator diagnoses a refused key from.
/// Each case is asserted twice: the **variant** (the typed part) and the exact `Display`
/// text, which carries the payload the variant holds (the namespace, the token, the number
/// refused). `is_err()` alone would hold just as well if every parser returned one anonymous
/// error carrying nothing.
#[test]
fn every_rejection_is_a_typed_error_that_names_the_violation() {
    let id = hex_token("0123456789abcdef");

    let over_wide = format!("slot:{id}:0000007");
    let past_max = u64::from(MAX_SLOT_INDEX) + 1;
    let cases: [(RecordError, RecordError, String); 6] = [
        (
            parse_slot_key(over_wide.as_bytes()).expect_err("an over-wide slot index"),
            RecordError::MalformedKey {
                namespace: "slot:",
                key: over_wide.clone(),
            },
            format!("malformed slot: key {over_wide:?}"),
        ),
        (
            parse_mpu_key(b"mpu:nothex").expect_err("a non-token upload id"),
            RecordError::TokenNotHex {
                token: "nothex".to_string(),
            },
            format!("\"nothex\" is not {TOKEN_HEX_LEN} lowercase-hex characters (a 128-bit token)"),
        ),
        (
            PartNumber::new(0).expect_err("zero is not a part"),
            RecordError::PartNumberOutOfRange { part_number: 0 },
            format!("part number 0 is outside the format bound [1, {MAX_PART_NUMBER}]"),
        ),
        (
            SlotIndex::new(MAX_SLOT_INDEX + 1).expect_err("past the slot key space"),
            RecordError::SlotIndexOutOfRange { index: past_max },
            format!("slot index {past_max} is outside the format bound [0, {MAX_SLOT_INDEX}]"),
        ),
        (
            parse_retire_mode(format!("retire:sideways:s:{id}:1").as_bytes())
                .expect_err("a third retire mode"),
            RecordError::UnknownRetireMode {
                mode: "sideways".to_string(),
            },
            "retire: key mode \"sideways\" is neither `bytes` nor `records`".to_string(),
        ),
        (
            Digest::from_hex("nope").expect_err("a non-digest"),
            RecordError::DigestNotHex {
                digest: "nope".to_string(),
            },
            "\"nope\" is not 64 lowercase-hex characters (a SHA-256 digest)".to_string(),
        ),
    ];
    for (actual, expected, display) in cases {
        // Equality against the whole variant pins the **payload** too — the namespace, the
        // token, the number refused — which `matches!` on the variant alone would not.
        assert_eq!(actual, expected);
        // Boxed as `std::error::Error`, the way a caller propagates it: the text survives.
        let boxed: Box<dyn std::error::Error> = Box::new(actual);
        assert_eq!(boxed.to_string(), display);
    }
}

/// Runs [`NUMERIC_ADVERSITIES`] against a `<prefix><id>:<body>` parser, then the
/// structural adversities (empty/colon upload id, truncated key, trailing component,
/// non-UTF-8), then asserts the canonical spelling still parses.
fn check_scoped_numeric_key<T: std::fmt::Debug + PartialEq>(
    prefix: &str,
    id: &str,
    canonical_body: &str,
    expected: T,
    parser: impl Fn(&[u8]) -> Result<T, RecordError>,
) {
    for body in NUMERIC_ADVERSITIES {
        let key = format!("{prefix}{id}:{body}");
        assert_rejected(
            &format!("{prefix} non-canonical body {body:?}"),
            key.as_bytes(),
            parser(key.as_bytes()),
        );
    }
    for key in [
        format!("{prefix}:{canonical_body}"),       // an empty upload id
        format!("{prefix}aa:bb:{canonical_body}"),  // an upload id carrying ':'
        format!("{prefix}{id}"),                    // truncated: the field is missing
        format!("{prefix}{id}:{canonical_body}:x"), // a trailing component
    ] {
        assert_rejected(prefix, key.as_bytes(), parser(key.as_bytes()));
    }
    let key = non_utf8(&format!("{prefix}{id}:"));
    assert_rejected(&format!("{prefix} non-UTF-8 bytes"), &key, parser(&key));

    let canonical = format!("{prefix}{id}:{canonical_body}");
    assert_eq!(
        parser(canonical.as_bytes()).expect("canonical spelling must parse"),
        expected,
        "{prefix}: canonical spelling did not round-trip to the expected value"
    );
}

#[test]
fn slot_key_round_trips_and_rejects_noncanonical() {
    let id = hex_token("0123456789abcdef");
    let uid = UploadId::new(id.clone()).unwrap();
    for index in [0u32, 1, 42, MAX_SLOT_INDEX] {
        let index = SlotIndex::new(index).unwrap();
        let key = slot_key(&uid, index);
        assert_eq!(parse_slot_key(&key).unwrap(), (uid.clone(), index));
        assert!(key.starts_with(&slot_range(&uid)));
        assert!(
            key.ends_with(format!("{:0width$}", index.get(), width = SLOT_INDEX_WIDTH).as_bytes())
        );
    }
    check_scoped_numeric_key(
        "slot:",
        &id,
        "000007",
        (uid, SlotIndex::new(7).unwrap()),
        parse_slot_key,
    );
}

#[test]
fn part_key_round_trips_and_rejects_noncanonical() {
    let id = hex_token("0123456789abcdef");
    let uid = UploadId::new(id.clone()).unwrap();
    for n in [1u32, 7, 10_000, MAX_PART_NUMBER] {
        let number = part(n);
        let key = part_key(&uid, number);
        assert_eq!(parse_part_key(&key).unwrap(), (uid.clone(), number));
        assert!(key.starts_with(&part_range(&uid)));
        assert!(key
            .ends_with(format!("{:0width$}", number.get(), width = PART_NUMBER_WIDTH).as_bytes()));
    }
    check_scoped_numeric_key("part:", &id, "000007", (uid, part(7)), parse_part_key);
}

#[test]
fn psum_key_round_trips_and_rejects_noncanonical() {
    let id = hex_token("0123456789abcdef");
    let uid = UploadId::new(id.clone()).unwrap();
    for n in [1u32, 7, 10_000, MAX_PART_NUMBER] {
        let number = part(n);
        let key = psum_key(&uid, number);
        assert_eq!(parse_psum_key(&key).unwrap(), (uid.clone(), number));
        assert!(key.starts_with(&psum_range(&uid)));
    }
    check_scoped_numeric_key("psum:", &id, "000007", (uid, part(7)), parse_psum_key);
}

#[test]
fn sidx_key_round_trips_and_rejects_noncanonical() {
    let id = hex_token("0123456789abcdef");
    let uid = UploadId::new(id.clone()).unwrap();
    for n in [1u32, 7, MAX_PART_NUMBER] {
        let number = part(n);
        // A chunk id is a `u128`, so the series crosses `u64::MAX` as well as the small
        // values — the key carries it un-padded and canonical.
        for chunk in [0u128, 7, u128::from(u64::MAX) + 1] {
            let key = sidx_key(&uid, number, chunk);
            assert_eq!(parse_sidx_key(&key).unwrap(), (uid.clone(), number, chunk));
            assert!(key.starts_with(&sidx_range(&uid)));
        }
    }

    // The part-number component: every numeric adversity, at the middle field.
    for body in NUMERIC_ADVERSITIES {
        let key = format!("sidx:{id}:{body}:7");
        assert_rejected(
            &format!("sidx: non-canonical part number {body:?}"),
            key.as_bytes(),
            parse_sidx_key(key.as_bytes()),
        );
    }
    // The chunk-id component is CANONICAL decimal, not fixed-width: "007" must never
    // parse as chunk 7, mirroring the fixed-width rule the padded fields enforce by width
    // instead (both forbid two spellings of one record).
    for body in ["007", "+7", "-7", " 7", "7 ", "abc", ""] {
        let key = format!("sidx:{id}:000007:{body}");
        assert_rejected(
            &format!("sidx: non-canonical chunk id {body:?}"),
            key.as_bytes(),
            parse_sidx_key(key.as_bytes()),
        );
    }
    // The canonical chunk spellings DO parse — including the `0` a leading-zero rule must
    // not sweep up with `007`.
    for chunk in [0u128, 7] {
        let key = format!("sidx:{id}:000007:{chunk}");
        assert_eq!(
            parse_sidx_key(key.as_bytes()).unwrap(),
            (uid.clone(), part(7), chunk)
        );
    }

    for key in [
        "sidx::000007:7".to_string(),      // an empty upload id
        "sidx:aa:bb:000007:7".to_string(), // an upload id carrying ':'
        format!("sidx:{id}:000007"),       // truncated: the chunk id is missing
        format!("sidx:{id}:000007:7:x"),   // a trailing component
    ] {
        assert_rejected("sidx:", key.as_bytes(), parse_sidx_key(key.as_bytes()));
    }
    let key = non_utf8(&format!("sidx:{id}:000007:"));
    assert_rejected("sidx: non-UTF-8 bytes", &key, parse_sidx_key(&key));
}

// ===========================================================================
// Leg 2 — byte-lexicographic order equals numeric order, across every digit-width
// boundary the fixed width can reach.
// ===========================================================================

const WIDTH_BOUNDARY_SERIES: [u32; 12] = [
    1, 9, 10, 99, 100, 999, 1_000, 9_999, 10_000, 99_999, 100_000, 999_999,
];

#[test]
fn fixed_width_key_byte_order_matches_numeric_order() {
    let uid = upload("0123456789abcdef");
    let slot = |n| slot_key(&uid, SlotIndex::new(n).expect("inside the slot key space"));
    for pair in WIDTH_BOUNDARY_SERIES.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        assert!(
            slot(lo) < slot(hi),
            "slot: byte order diverges from numeric order at the {lo} → {hi} width boundary"
        );
        assert!(
            part_key(&uid, part(lo)) < part_key(&uid, part(hi)),
            "part: byte order diverges from numeric order at the {lo} → {hi} width boundary"
        );
    }
}

// ===========================================================================
// Leg 3 — no key prefix is a prefix of another, over the full protocol key space; and no
// bounded per-session range selects outside its own class and session.
// ===========================================================================

#[test]
fn no_key_prefix_is_a_prefix_of_another() {
    let prefixes: [&[u8]; 15] = [
        MPU_PREFIX,
        SLOT_PREFIX,
        PART_PREFIX,
        PSUM_PREFIX,
        SIDX_PREFIX,
        RETIRE_BYTES_PREFIX,
        RETIRE_RECORDS_PREFIX,
        b"inode:",   // metadata.rs:34 (inode_key)
        b"dirent:",  // metadata.rs:39 (dirent_key)
        b"pending:", // metadata.rs:44 (pending_key)
        b"bucket:",  // metadata.rs:52 (bucket_key)
        metadata::ORPHAN_PREFIX,
        metadata::SEG_PREFIX,
        metadata::SEGGRP_PREFIX,
        b"desired:dserver:", // custodian/src/desired_state.rs:33
    ];
    for (i, a) in prefixes.iter().enumerate() {
        let a_text = String::from_utf8_lossy(a);
        for (j, b) in prefixes.iter().enumerate() {
            assert!(
                i == j || !a.starts_with(b),
                "scan({:?}) would return {a_text:?} records",
                String::from_utf8_lossy(b)
            );
        }
        // The admission singleton carries no trailing separator, so it joins the matrix as
        // a whole key rather than a prefix: no namespace scan reaches it, and it opens no
        // scan that would reach a namespace (`0016:342-344`).
        assert!(
            !MPUCTL_KEY.starts_with(a) && !a.starts_with(MPUCTL_KEY),
            "the mpuctl singleton and {a_text:?} are not disjoint"
        );
    }
}

#[test]
fn scan_mpu_does_not_reach_the_mpuctl_singleton() {
    // The named near-miss (`0016:342-344`): `mpuctl`'s 4th byte is `c`, not `:`.
    assert!(!MPUCTL_KEY.starts_with(MPU_PREFIX));
}

#[test]
fn sidx_is_not_reachable_from_a_pending_scan() {
    // The named near-miss (`0016:475-491`): the owned-staging prefix is disjoint from the
    // global `pending:` sweep, so no restore/expiry pass sees an owned entry.
    assert!(!SIDX_PREFIX.starts_with(b"pending:"));
    let key = sidx_key(&upload("0123456789abcdef"), part(1), 0);
    assert!(!key.starts_with(b"pending:"));
}

/// A session-scoped retirement token at epoch 1, with or without the per-part suffix.
fn session_token(uid: &UploadId, part: Option<(PartNumber, AttemptId)>) -> RetireToken {
    RetireToken::Session {
        upload_id: uid.clone(),
        epoch: 1,
        part,
    }
}

/// One key per keyed class and the bounded per-session range of that same class — the rows
/// and columns of the range matrix below.
fn class_key_and_range(uid: &UploadId, att: &AttemptId) -> Vec<(&'static str, Vec<u8>, Vec<u8>)> {
    use RetireMode::{Bytes, Records};
    let slot = SlotIndex::new(3).expect("inside the slot key space");
    let whole = session_token(uid, None);
    let per_part = session_token(uid, Some((part(4), att.clone())));
    vec![
        ("slot:", slot_key(uid, slot), slot_range(uid)),
        ("part:", part_key(uid, part(4)), part_range(uid)),
        ("psum:", psum_key(uid, part(4)), psum_range(uid)),
        ("sidx:", sidx_key(uid, part(4), 5), sidx_range(uid)),
        (
            "retire:bytes:",
            retire_key(Bytes, &whole),
            retire_session_range(Bytes, uid),
        ),
        (
            "retire:records:",
            retire_key(Records, &per_part),
            retire_session_range(Records, uid),
        ),
    ]
}

/// Disjoint prefixes keep one **class** out of another's scan; this is the other half — a
/// bounded **per-session range** must select exactly its own class *and* its own session.
/// Every read of these records in 0016 is such a range (`0016:349`, `:351-353`, `:374-380`),
/// and the reaper deletes what its range returns: a range that reached a neighbouring class
/// or a second session would delete live records nothing else names, and one that missed its
/// own would leave residue nothing enumerates — both faces of **C-1**.
#[test]
fn a_session_range_selects_exactly_its_own_class_and_session() {
    let mine = upload("0123456789abcdef");
    let other = upload("fedcba9876543210");
    let att = attempt("00112233445566778899aabbccddeeff");
    let generation = retire_key(
        RetireMode::Bytes,
        &RetireToken::Generation {
            inode: 12,
            version: 34,
        },
    );
    let rows = class_key_and_range(&mine, &att);

    for (range_class, _, range) in &rows {
        for (key_class, key, _) in &rows {
            assert_eq!(
                key.starts_with(range),
                key_class == range_class,
                "the {range_class} range must select the {key_class} key iff it is its own class"
            );
        }
        for (key_class, key, _) in &class_key_and_range(&other, &att) {
            assert!(
                !key.starts_with(range),
                "the {range_class} range reached another session's {key_class} key"
            );
        }
        // `0016:374-380`: a `g:` obligation belongs to the object generation, not to the
        // session that published it, so the session's terminal-delete gate must not see it —
        // a completed session may be torn down while the generation it superseded drains.
        for (what, foreign) in [
            ("the session record", mpu_key(&mine)),
            ("the admission singleton", MPUCTL_KEY.to_vec()),
            ("a generation obligation", generation.clone()),
        ] {
            assert!(
                !foreign.starts_with(range),
                "the {range_class} range reached {what}"
            );
        }
    }
}

// ===========================================================================
// Leg 4 — the `retire:` token grammar
// ===========================================================================

#[test]
fn retire_key_round_trips_both_token_forms_under_both_modes() {
    let uid = upload("0123456789abcdef");
    let other = upload("fedcba9876543210");
    let att = attempt("00112233445566778899aabbccddeeff");
    let tokens = [
        session_token(&uid, None),
        RetireToken::Session {
            upload_id: uid.clone(),
            epoch: 0,
            part: None,
        },
        session_token(&uid, Some((part(4), att))),
        RetireToken::Generation {
            inode: 12,
            version: 34,
        },
    ];
    for mode in RetireMode::ALL {
        for token in &tokens {
            let key = retire_key(mode, token);
            let (parsed_mode, parsed_token) = parse_retire_key(&key).unwrap();
            assert_eq!(parsed_mode, mode);
            assert_eq!(&parsed_token, token);
            assert_eq!(parse_retire_mode(&key).unwrap(), mode);

            // The two forms are distinguished at parse — never confused with each other.
            match token {
                RetireToken::Session { .. } => {
                    assert!(matches!(parsed_token, RetireToken::Session { .. }));
                    assert!(key.starts_with(&retire_session_range(mode, &uid)));
                    assert!(!key.starts_with(&retire_session_range(mode, &other)));
                }
                RetireToken::Generation { .. } => {
                    assert!(matches!(parsed_token, RetireToken::Generation { .. }));
                    assert!(!key.starts_with(&retire_session_range(mode, &uid)));
                }
            }
        }
    }
}

/// Every malformed spelling is refused by **both** `retire:` entry points
/// ([`assert_retire_rejected`]), so the mode a caller dispatches on and the token it acts on
/// can never disagree about whether a key decodes. The table states that as a rule over the
/// whole grammar rather than only over the rows that name the fault — a known mode with no
/// token at all (`retire:bytes:`, `retire:records:`) and one whose token bytes are not UTF-8.
#[test]
fn retire_key_rejects_a_token_of_neither_form_and_every_malformed_spelling() {
    let id = hex_token("0123456789abcdef");
    let att = hex_token("fedcba9876543210");
    for key in [
        format!("retire:unknown:s:{id}:7"),         // a third mode
        format!("retire:bytes:x:{id}:7"),           // a token of neither `s:` nor `g:` form
        format!("retire:bytes:s:{id}:+7"),          // a '+' sign on the epoch
        format!("retire:bytes:s:{id}:07"),          // a leading zero on the epoch
        format!("retire:bytes:s:{id}:abc"),         // a non-decimal epoch
        "retire:bytes:s::7".to_string(),            // an empty upload id
        "retire:bytes:s:aa:bb:7".to_string(),       // an upload id carrying ':'
        format!("retire:bytes:s:{id}"),             // truncated: the epoch is missing
        format!("retire:bytes:s:{id}:7:x"),         // a trailing component
        format!("retire:bytes:s:{id}:7:4:{att}:x"), // a trailing component, per-part form
        format!("retire:bytes:s:{id}:7:004:{att}"), // a non-canonical per-part number
        format!("retire:bytes:s:{id}:7:4:nothex"),  // a malformed attempt id
        "retire:records:g:12:abc".to_string(),      // a non-decimal generation version
        "retire:records:g:012:34".to_string(),      // a leading zero on the generation inode
        "retire:bytes:".to_string(),                // a known mode, no token at all
        "retire:records:".to_string(),              // the same, other mode
    ] {
        assert_retire_rejected(key.as_bytes());
    }
    for key in [non_utf8("retire:bytes:s:"), non_utf8("retire:bytes:")] {
        assert_retire_rejected(&key);
    }
}

/// A `retire:` key **both** entry points must refuse. `parse_retire_mode` is asserted beside
/// `parse_retire_key` on every row so the two can never drift apart: the mode is what the
/// drain dispatches on, so a mode answered for a key whose token does not decode would send
/// it orphan-marking the fragments of a key nothing can read (`0016:438-440`).
fn assert_retire_rejected(key: &[u8]) {
    assert_rejected("retire: key", key, parse_retire_key(key));
    assert_rejected("retire: mode", key, parse_retire_mode(key));
}

/// The `retire:` decode boundary reports its **two** failures apart, because they are
/// different faults for the drain that meets one (`0016:434-440`). A key *inside* the
/// namespace naming a third mode is a record whose disposal rule this build does not know —
/// orphan-mark the bytes or delete records only is precisely the choice that must never be
/// guessed, since guessing "bytes" once is silent data loss. A key that is not a `retire:`
/// key at all is a neighbour's record: calling that an unknown *mode* names a mode nothing
/// ever wrote and sends an operator hunting an obligation that does not exist.
///
/// The last four rows are a third class, and the reason `parse_retire_mode` is a whole decode
/// rather than a prefix test: a **known** mode whose token is absent, truncated or not UTF-8
/// names no obligation, so it is `MalformedKey` — never `Ok(Bytes)`, which would send the
/// drain orphan-marking the fragments of a key it cannot read.
#[test]
fn a_retire_key_reports_an_unknown_mode_apart_from_a_key_that_does_not_decode() {
    let id = hex_token("0123456789abcdef");
    let err = parse_retire_mode(format!("retire:sideways:s:{id}:1").as_bytes())
        .expect_err("a third retire mode");
    assert_eq!(
        err,
        RecordError::UnknownRetireMode {
            mode: "sideways".to_string()
        }
    );

    for key in [
        mpu_key(&upload("0")),       // a neighbouring namespace's key
        b"retire:".to_vec(),         // the namespace prefix alone
        b"retire:bytes".to_vec(),    // a mode with no token — truncated, not "unknown"
        non_utf8("retire:"),         // not UTF-8 at all
        b"retire:bytes:".to_vec(),   // a known mode, no token
        b"retire:records:".to_vec(), // the same, other mode
        non_utf8("retire:records:"), // a known mode, token bytes that are not UTF-8
        b"retire:bytes:s:".to_vec(), // a known mode, a token form with nothing under it
    ] {
        assert_eq!(
            parse_retire_mode(&key).expect_err("not a retire: key"),
            RecordError::MalformedKey {
                namespace: "retire:",
                key: String::from_utf8_lossy(&key).into_owned(),
            }
        );
        assert_rejected("retire:", &key, parse_retire_key(&key));
    }
}

// ===========================================================================
// Leg 5 — the pinned format constants hold.
// ===========================================================================

#[test]
fn pinned_format_constants_hold() {
    assert_eq!(PART_NUMBER_WIDTH, 6);
    assert_eq!(MAX_PART_NUMBER, 999_999);
    assert_eq!(SLOT_INDEX_WIDTH, 6);
    assert_eq!(MAX_SLOT_INDEX, 999_999);
    assert_eq!(TOKEN_HEX_LEN, metadata::SEG_NONCE_HEX_LEN);

    let uid = upload("0123456789abcdef");

    // Each parser's width equals its constant: the widest legal value round-trips, and
    // one past it is a typed format violation, never a silent clamp or wraparound.
    let max_part = part(MAX_PART_NUMBER);
    let part_key_bytes = part_key(&uid, max_part);
    for (key, prefix) in [
        (part_key_bytes.clone(), PART_PREFIX),
        (psum_key(&uid, max_part), PSUM_PREFIX),
    ] {
        let width = prefix.len() + uid.as_str().len() + 1 + PART_NUMBER_WIDTH;
        assert_eq!(key.len(), width);
    }
    assert_eq!(
        parse_part_key(&part_key_bytes).unwrap(),
        (uid.clone(), max_part)
    );
    assert!(PartNumber::new(MAX_PART_NUMBER + 1).is_err());

    let max_slot = SlotIndex::new(MAX_SLOT_INDEX).unwrap();
    let slot_key_bytes = slot_key(&uid, max_slot);
    assert_eq!(
        slot_key_bytes.len(),
        SLOT_PREFIX.len() + uid.as_str().len() + 1 + SLOT_INDEX_WIDTH
    );
    assert_eq!(parse_slot_key(&slot_key_bytes).unwrap(), (uid, max_slot));
    assert!(SlotIndex::new(MAX_SLOT_INDEX + 1).is_err());
}
