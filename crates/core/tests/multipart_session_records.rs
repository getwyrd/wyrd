//! Issue #716 — the multipart **in-flight lifecycle records**:
//! `wyrd_core::multipart::{SessionRecord, SessionState, PublishTarget, Completion, SlotRecord,
//! PartRecord, PartSummary}`, the `mpu:`/`slot:`/`part:`/`psum:` values (proposal 0016 §1,
//! `:333-527`, the state machine at `:528-602`). **Pure**: no store, no async, no fixture
//! beyond hand-authored JSON bytes and the production codec — mirroring
//! `multipart_budget_admission.rs` (#715) for the record values this child adds beneath it.
//!
//! Every witness is **decoded, never constructed**: these types have no writer-side
//! constructor (the first writer is the store round trip, #656–#659), exactly as
//! `Budget`/`AdmissionRecord` have none — see their doc for why. Bytes are written out by
//! hand and fed to two surfaces per validated record: the module's own attributed `decode_*`
//! (S2) and the store-wide `metadata::decode::<T>` (S1), asserted to agree.
//!
//! **Serialization identity is asserted on every accepted witness, not test by test.** The
//! `decode_both` helper below re-encodes whatever it decoded and compares it byte-for-byte
//! against the input, so `encode(decode(bytes)) == bytes` is a property of the *whole* file's
//! accepted set — the shape a whole-record CAS on the session needs (`0016:555-558`;
//! `AGENTS.md:170-172`, the repo's serialization-identity rule). The two spellings of an
//! absent `content_type` are what that property turns on, so each has its own witness:
//! omitted round-trips (and re-encodes omitted), `null` is refused.
//!
//! The legs (Falsifiability, brief #716): **eight** isolating negations — 1c, 1c-epoch,
//! 1i-ChunkRef-scheme, 1i-slot, 1j-forbidden-field, 1j-missing-required, 1k-len-mismatch,
//! 1m-unknown-field — plus **one** positive leg negated the other way (a `ChunkRef` whose
//! `placement` length does not match its scheme's fragment count still decodes). Nine
//! demonstrations total, each isolated so it violates only its own rule. The remaining tests
//! (round trips, the identity witnesses, the overflow half of 1k) are additional evidence, not
//! part of that count.
//!
//! No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use wyrd_core::metadata::{self, ChunkRef, EcScheme};
use wyrd_core::multipart::{
    decode_part_record, decode_part_summary, decode_session_record, decode_slot_record, Completion,
    PartSummary, PublishTarget, RecordError, SessionState,
};

/// The session's own target bucket inode and object name, shared by every witness below
/// unless a leg deliberately perturbs one.
const PARENT: u64 = 42;
const OBJECT: &str = "key/one";
const EPOCH: u64 = 3;
/// Complete fences so far. Deliberately **not** `0` or `1`: a witness that only ever observes
/// a field's zero value cannot tell a decoder that reads it from one that returns a default
/// (the surviving-mutant class `cargo mutants` reports as `replace … -> u32 with 0`).
const ATTEMPTS: u32 = 2;
/// A `content_type` that is present, non-empty, and not the string any default would invent.
const CONTENT_TYPE: &str = "text/plain";
/// The `Completing` segment-write cursor and the published generation's version every witness
/// carries — non-zero for the same reason [`ATTEMPTS`] is.
const SEGMENTS_WRITTEN: u32 = 2;
const VERSION: u64 = 4;

/// 64 lowercase-hex characters from a 2-character pair, so a hand-authored digest is always
/// exactly the length [`wyrd_core::multipart::Digest::from_hex`] demands — no fencepost
/// counting by hand.
fn hex64(pair: &str) -> String {
    pair.repeat(32)
}

/// 32 lowercase-hex characters from a 2-character pair — the token length
/// [`wyrd_core::multipart::TOKEN_HEX_LEN`] demands (upload id / attempt id).
fn hex32(pair: &str) -> String {
    pair.repeat(16)
}

// ===========================================================================
// Hand-authored bytes — field names and order exactly as the production types declare them,
// so a byte-identity assertion after re-encoding is a claim about this codec's own spelling.
// ===========================================================================

/// A session record with `content_type` **omitted** — the spelling the encoder writes for an
/// absent content type, and the default for every leg that is not about the field itself.
fn session(epoch: u64, state_json: &str) -> Vec<u8> {
    session_with(None, epoch, state_json)
}

/// A session record with each optional-or-perturbable field spelled out. `content_type` is
/// **omitted entirely** when `None` (never rendered `null`), which is what the production
/// `skip_serializing_if` emits.
fn session_with(content_type: Option<&str>, epoch: u64, state_json: &str) -> Vec<u8> {
    let content_type = match content_type {
        Some(value) => format!("\"content_type\":\"{value}\","),
        None => String::new(),
    };
    format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",{content_type}\
         \"created_at_millis\":1000,\"clock_source\":\"wall\",\"epoch\":{epoch},\
         \"attempts\":{ATTEMPTS},\"state\":{state_json}}}"
    )
    .into_bytes()
}

fn publish_target_json(parent: u64, name: &str, epoch: u64) -> String {
    format!("{{\"parent\":{parent},\"name\":\"{name}\",\"epoch\":{epoch}}}")
}

fn completing_json(fenced_at: u64, parent: u64, name: &str, epoch: u64) -> String {
    format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":{fenced_at},\
         \"segments_written\":{SEGMENTS_WRITTEN},\"publish_target\":{}}}",
        publish_target_json(parent, name, epoch)
    )
}

fn completion_json(inode: u64, etag: &str, completed_at: u64, fingerprint: &str) -> String {
    format!(
        "{{\"inode\":{inode},\"version\":{VERSION},\"etag\":\"{etag}\",\
         \"completed_at_millis\":{completed_at},\"complete_fingerprint\":\"{fingerprint}\"}}"
    )
}

const OPEN_JSON: &str = "{\"kind\":\"Open\"}";
const ABORTING_JSON: &str = "{\"kind\":\"Aborting\"}";

fn slot(part_number: u32, attempt_id: &str, reserved_at: u64, lease_expiry: u64) -> Vec<u8> {
    format!(
        "{{\"part_number\":{part_number},\"attempt_id\":\"{attempt_id}\",\
         \"reserved_at_millis\":{reserved_at},\"lease_expiry_millis\":{lease_expiry}}}"
    )
    .into_bytes()
}

fn chunk_json(id: u128, scheme_json: &str, len: u64, placement: &str) -> String {
    format!("{{\"id\":{id},\"scheme\":{scheme_json},\"len\":{len},\"placement\":{placement}}}")
}

fn part(chunks_json: &str, len: u64, digest: &str, committed_at: u64) -> Vec<u8> {
    format!(
        "{{\"chunks\":[{chunks_json}],\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{committed_at},\"session_epoch\":{EPOCH}}}"
    )
    .into_bytes()
}

fn part_summary_bytes(chunks: u32, len: u64, digest: &str, committed_at: u64) -> Vec<u8> {
    format!(
        "{{\"chunks\":{chunks},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{committed_at}}}"
    )
    .into_bytes()
}

// ===========================================================================
// S1/S2 agreement + serialization identity — decode through the module's attributed `decode_*`
// (S2) and the store-wide `metadata::decode::<T>` (S1), assert they agree, and assert that
// re-encoding an accepted value reproduces the bytes it was read from. Mirrors `decode_both`
// in `multipart_budget_admission.rs`, with the identity assertion added so **every** accepted
// witness in this file is also an identity witness (`0016:555-558`).
// ===========================================================================

/// Decode `bytes` through the module's attributed decoder `typed_decode` (S2) **and** the
/// store-wide [`metadata::decode`] (S1), assert the two agree, and — for a value either
/// accepts — assert that re-encoding it reproduces the bytes it was read from.
///
/// One generic over all three record types, so no record can be given a weaker check than its
/// siblings; the bound is the one `multipart_keys.rs:281` already uses for the same reason.
fn decode_both<T>(
    bytes: &[u8],
    typed_decode: fn(&[u8]) -> Result<T, RecordError>,
) -> Result<T, RecordError>
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let typed = typed_decode(bytes);
    let untyped = metadata::decode::<T>(bytes);
    assert_eq!(
        typed.as_ref().ok(),
        untyped.as_ref().ok(),
        "S1 and S2 disagree on {}: S2={typed:?}",
        String::from_utf8_lossy(bytes)
    );
    if let Ok(record) = &typed {
        assert_eq!(
            String::from_utf8_lossy(metadata::encode(record).as_ref()),
            String::from_utf8_lossy(bytes),
            "decode->encode is not byte-identical for {record:?}"
        );
    }
    typed
}

/// Assert that a decode was refused as this record class's attributed
/// [`RecordError::MalformedRecordValue`], with the decoder's own message naming `needle` — the
/// shape every serde-level rejection arrives in (legs 1j and 1m, and the `null` spelling).
fn assert_malformed<T: std::fmt::Debug>(
    result: Result<T, RecordError>,
    expected_namespace: &str,
    needle: &str,
) {
    match result {
        Err(RecordError::MalformedRecordValue { namespace, detail }) => {
            assert_eq!(namespace, expected_namespace);
            assert!(
                detail.contains(needle),
                "expected the decoder's message to name {needle:?}: {detail}"
            );
        }
        other => panic!("expected MalformedRecordValue({expected_namespace}), got {other:?}"),
    }
}

// ===========================================================================
// Round trip — every one of the seven landed types decodes, and re-encoding what this codec
// decoded is byte-identical to what it read (the CAS-support identity `0016:555-558` needs).
// ===========================================================================

#[test]
fn session_open_round_trips() {
    let bytes = session(EPOCH, OPEN_JSON);
    let record = decode_both(&bytes, decode_session_record).expect("an Open session decodes");
    assert_eq!(record.parent(), PARENT);
    assert_eq!(record.object(), OBJECT);
    assert_eq!(record.content_type(), None);
    assert_eq!(record.created_at_millis(), 1000);
    assert_eq!(record.clock_source(), "wall");
    assert_eq!(record.epoch(), EPOCH);
    assert_eq!(record.attempts(), ATTEMPTS);
    assert_eq!(record.state(), &SessionState::Open {});
}

/// A **present** `content_type` survives the round trip verbatim — the value witness the
/// omitted case cannot give: a decoder that answered `None` for every session would satisfy
/// `session_open_round_trips` and every negation below, and would still lose the client's
/// declared type on the first store round trip (#656–#659).
#[test]
fn session_with_content_type_round_trips() {
    let bytes = session_with(Some(CONTENT_TYPE), EPOCH, OPEN_JSON);
    let record =
        decode_both(&bytes, decode_session_record).expect("a session with a content type decodes");
    assert_eq!(record.content_type(), Some(CONTENT_TYPE));
    assert_eq!(record.attempts(), ATTEMPTS);
}

/// An **absent** `content_type` re-encodes **omitted**, never as `"content_type":null`
/// (`AGENTS.md:170-172`; `metadata.rs:1394-1419`, the same rule on `InodeRecord`'s optional
/// trio). This is the byte-identity a whole-record CAS on the session turns on: every
/// transition compares the record's exact current bytes (`0016:555-558`), so a decode→encode
/// that inserted a `null` field would either `Conflict` forever (`require(key, encode(prior))`,
/// `metadata.rs:1794`) or silently rewrite the stored record (`require(key, current)`,
/// `metadata.rs:2012`).
#[test]
fn session_absent_content_type_re_encodes_omitted() {
    let bytes = session_with(None, EPOCH, OPEN_JSON);
    let record =
        decode_both(&bytes, decode_session_record).expect("a session with no content type decodes");
    assert_eq!(record.content_type(), None);
    let re_encoded = metadata::encode(&record);
    assert!(
        !String::from_utf8_lossy(re_encoded.as_ref()).contains("content_type"),
        "an absent content_type must be omitted on re-encode, never emitted as null: {}",
        String::from_utf8_lossy(re_encoded.as_ref())
    );
}

#[test]
fn session_completing_round_trips() {
    let bytes = session(EPOCH, &completing_json(500, PARENT, OBJECT, EPOCH));
    let record = decode_both(&bytes, decode_session_record).expect("a Completing session decodes");
    assert_eq!(
        record.state(),
        &SessionState::Completing {
            fenced_at_millis: 500,
            segments_written: SEGMENTS_WRITTEN,
            publish_target: PublishTarget {
                parent: PARENT,
                name: OBJECT.to_string(),
                epoch: EPOCH,
            },
        }
    );
}

#[test]
fn session_aborting_round_trips() {
    let bytes = session(EPOCH, ABORTING_JSON);
    let record = decode_both(&bytes, decode_session_record).expect("an Aborting session decodes");
    assert_eq!(record.state(), &SessionState::Aborting {});
}

#[test]
fn session_completed_round_trips() {
    let etag = hex64("ab");
    let fingerprint = hex64("cd");
    let bytes = session(
        EPOCH,
        &format!(
            "{{\"kind\":\"Completed\",\"completion\":{}}}",
            completion_json(9, &etag, 6000, &fingerprint)
        ),
    );
    let record = decode_both(&bytes, decode_session_record).expect("a Completed session decodes");
    let SessionState::Completed { completion } = record.state() else {
        panic!("expected Completed, got {:?}", record.state());
    };
    assert_eq!(completion.inode, 9);
    assert_eq!(completion.version, VERSION);
    assert_eq!(completion.etag.to_hex(), etag);
    assert_eq!(completion.completed_at_millis, 6000);
    assert_eq!(completion.complete_fingerprint.to_hex(), fingerprint);
}

/// [`SessionState`] decodes on its own — the Defect field's "each validating inside its own
/// `Deserialize`" — independent of any [`SessionRecord`] wrapping it.
#[test]
fn session_state_round_trips_standalone() {
    let bytes = br#"{"kind":"Completing","fenced_at_millis":1,"segments_written":0,"publish_target":{"parent":1,"name":"n","epoch":1}}"#;
    let state: SessionState = metadata::decode(bytes).expect("a bare SessionState decodes");
    assert_eq!(metadata::encode(&state).as_ref(), bytes.as_slice());
}

/// [`PublishTarget`] decodes on its own, independent of any [`SessionState`] wrapping it.
#[test]
fn publish_target_round_trips_standalone() {
    let bytes = br#"{"parent":7,"name":"obj","epoch":2}"#;
    let target: PublishTarget = metadata::decode(bytes).expect("a bare PublishTarget decodes");
    assert_eq!(target.parent, 7);
    assert_eq!(target.name, "obj");
    assert_eq!(target.epoch, 2);
    assert_eq!(metadata::encode(&target).as_ref(), bytes.as_slice());
}

/// [`Completion`] decodes on its own, independent of any [`SessionState`] wrapping it.
#[test]
fn completion_round_trips_standalone() {
    let etag = hex64("11");
    let fingerprint = hex64("22");
    let bytes = completion_json(3, &etag, 42, &fingerprint).into_bytes();
    let completion: Completion = metadata::decode(&bytes).expect("a bare Completion decodes");
    assert_eq!(completion.inode, 3);
    assert_eq!(completion.etag.to_hex(), etag);
    assert_eq!(metadata::encode(&completion).as_ref(), bytes.as_slice());
}

#[test]
fn slot_round_trips() {
    let attempt = hex32("aa");
    let bytes = slot(7, &attempt, 100, 200);
    let record = decode_both(&bytes, decode_slot_record).expect("a legal slot decodes");
    assert_eq!(record.part_number().get(), 7);
    assert_eq!(record.attempt_id().as_str(), attempt);
    assert_eq!(record.reserved_at_millis(), 100);
    assert_eq!(record.lease_expiry_millis(), 200);
}

#[test]
fn part_round_trips() {
    let digest = hex64("33");
    let chunk = chunk_json(1, "\"None\"", 50, "[]");
    let bytes = part(&chunk, 50, &digest, 900);
    let record = decode_both(&bytes, decode_part_record).expect("a legal part decodes");
    assert_eq!(record.chunks().len(), 1);
    assert_eq!(record.len(), 50);
    assert!(!record.is_empty());
    assert_eq!(record.digest().to_hex(), digest);
    assert_eq!(record.committed_at_millis(), 900);
    assert_eq!(record.session_epoch(), EPOCH);
}

/// A part whose chunk carries **zero** logical bytes decodes, and reports itself empty. Two
/// things at once, both load-bearing:
///
/// * there is **no** "logical `len` must be non-zero" rule — ADR-0045's `ChunkRef` row asks for
///   a length *consistent with the scheme*, and the target's own encoder handles a zero-length
///   chunk without complaint (`crates/core/src/erasure.rs:79-83`, whose `shard_size` applies
///   `.max(1)`), so rejecting one here would be an invented bound;
/// * `is_empty` is observed **true** as well as false (`part_round_trips`), so a decoder that
///   answered a constant for it fails one of the two.
#[test]
fn part_with_zero_length_chunk_is_empty() {
    let digest = hex64("99");
    let chunk = chunk_json(4, "\"None\"", 0, "[]");
    let bytes = part(&chunk, 0, &digest, 900);
    let record = decode_both(&bytes, decode_part_record).expect("a zero-length part decodes");
    assert_eq!(record.chunks().len(), 1);
    assert_eq!(record.len(), 0);
    assert!(record.is_empty());
}

#[test]
fn part_summary_round_trips() {
    let digest = hex64("44");
    let bytes = part_summary_bytes(3, 150, &digest, 900);
    let summary: PartSummary =
        decode_both(&bytes, decode_part_summary).expect("a legal summary decodes");
    assert_eq!(summary.chunks, 3);
    assert_eq!(summary.len, 150);
    assert_eq!(summary.digest.to_hex(), digest);
    assert_eq!(summary.committed_at_millis, 900);
}

// ===========================================================================
// The eight isolating negations — each torn value violates only its own named rule.
// ===========================================================================

/// **1c** — `publish_target` names a different dirent than the session's own `parent`/`object`;
/// the `epoch` agrees, isolating the key identity from leg 1c-epoch. **Both halves of the pair
/// are perturbed one at a time**, so neither clause of the rule can be dropped and still pass.
#[test]
fn leg_1c_publish_target_key_mismatch_is_rejected() {
    let bytes = session(EPOCH, &completing_json(1, PARENT + 1, OBJECT, EPOCH));
    assert_eq!(
        decode_both(&bytes, decode_session_record),
        Err(RecordError::PublishTargetKeyMismatch {
            session_parent: PARENT,
            session_object: OBJECT.to_string(),
            target_parent: PARENT + 1,
            target_name: OBJECT.to_string(),
        })
    );

    let renamed = session(EPOCH, &completing_json(1, PARENT, "key/other", EPOCH));
    assert_eq!(
        decode_both(&renamed, decode_session_record),
        Err(RecordError::PublishTargetKeyMismatch {
            session_parent: PARENT,
            session_object: OBJECT.to_string(),
            target_parent: PARENT,
            target_name: "key/other".to_string(),
        })
    );
}

/// **1c-epoch** — `publish_target.epoch` disagrees with the session's own `epoch`;
/// `parent`/`name` both agree, isolating the epoch identity from leg 1c. The F18 class:
/// `publish_target`'s epoch is what makes the `Completing` fence's segment-group nonce
/// deterministic for *this* attempt (`0016:350`, `:560-563`).
#[test]
fn leg_1c_epoch_publish_target_epoch_mismatch_is_rejected() {
    let bytes = session(EPOCH, &completing_json(1, PARENT, OBJECT, EPOCH + 1));
    assert_eq!(
        decode_both(&bytes, decode_session_record),
        Err(RecordError::PublishTargetEpochMismatch {
            session_epoch: EPOCH,
            target_epoch: EPOCH + 1,
        })
    );
}

/// **1i (`PartRecord`/`ChunkRef` half)** — a chunk's `EcScheme::ReedSolomon { k: 0, m: 1 }` is
/// not one `erasure::supported` accepts (ADR-0045's invariant table). `len` is set to exactly
/// this chunk's own length so leg 1k's rule holds, isolating the scheme check.
#[test]
fn leg_1i_chunk_scheme_unsupported_is_rejected() {
    let digest = hex64("55");
    let chunk = chunk_json(7, r#"{"ReedSolomon":{"k":0,"m":1}}"#, 100, "[]");
    let bytes = part(&chunk, 100, &digest, 1);
    assert_eq!(
        decode_both(&bytes, decode_part_record),
        Err(RecordError::ChunkSchemeUnsupported {
            chunk_id: 7,
            k: 0,
            m: 1,
        })
    );
}

/// **1i (`SlotRecord` half)** — `lease_expiry_millis <= reserved_at_millis`: a slot born
/// already lapsed (`0016:349`). `part_number` and `attempt_id` are both well-formed,
/// isolating the lease check.
#[test]
fn leg_1i_slot_lease_already_lapsed_is_rejected() {
    let attempt = hex32("bb");
    let bytes = slot(1, &attempt, 1_000, 1_000);
    assert_eq!(
        decode_both(&bytes, decode_slot_record),
        Err(RecordError::SlotLeaseAlreadyLapsed {
            reserved_at_millis: 1_000,
            lease_expiry_millis: 1_000,
        })
    );

    // The same slot with a lease one millisecond past reservation decodes.
    let ok = slot(1, &attempt, 1_000, 1_001);
    assert!(decode_both(&ok, decode_slot_record).is_ok());
}

/// **1j (forbidden field)** — an `Open` session carrying a `Completing`-only
/// `fenced_at_millis` is a decode error, not a value (`0016:403-415`'s normative example).
/// Every other field is well-formed, isolating the state-shape check.
#[test]
fn leg_1j_open_session_with_forbidden_completing_field_is_rejected() {
    let bytes = session(EPOCH, "{\"kind\":\"Open\",\"fenced_at_millis\":9}");
    assert_malformed(
        decode_both(&bytes, decode_session_record),
        "mpu:",
        "fenced_at_millis",
    );
}

/// **1j (missing required field), the mirror** — a `Completing` session missing
/// `segments_written` is a decode error, never a silently-defaulted value.
#[test]
fn leg_1j_completing_session_missing_required_field_is_rejected() {
    let bytes = session(
        EPOCH,
        &format!(
            "{{\"kind\":\"Completing\",\"fenced_at_millis\":1,\"publish_target\":{}}}",
            publish_target_json(PARENT, OBJECT, EPOCH)
        ),
    );
    assert_malformed(
        decode_both(&bytes, decode_session_record),
        "mpu:",
        "segments_written",
    );
}

/// **1k** — a `PartRecord`'s own `len` does not equal the checked sum of its `chunks`' logical
/// lengths (`0016:351`). The lone chunk's scheme is `None` (always valid), isolating the
/// length-agreement check from leg 1i's scheme check.
#[test]
fn leg_1k_part_length_mismatch_is_rejected() {
    let digest = hex64("66");
    let chunk = chunk_json(1, "\"None\"", 100, "[]");
    let bytes = part(&chunk, 50, &digest, 1);
    assert_eq!(
        decode_both(&bytes, decode_part_record),
        Err(RecordError::PartLengthMismatch {
            declared: 50,
            chunks: 100,
        })
    );
}

/// **1k, the overflow half** — an absurd chunk list whose lengths overflow `u64` is a typed
/// error, never a silent wrap a same-width comparison against `len` could then confirm
/// (mirrors `SegmentLengthOverflow`, `metadata.rs:1208-1218`). Not one of the nine named
/// demonstrations, but the same guard leg 1k pins, exercised at its other failure mode.
#[test]
fn part_length_overflow_is_rejected() {
    let digest = hex64("77");
    let chunk_a = chunk_json(1, "\"None\"", u64::MAX, "[]");
    let chunk_b = chunk_json(2, "\"None\"", 1, "[]");
    let bytes = part(&format!("{chunk_a},{chunk_b}"), 0, &digest, 1);
    assert_eq!(
        decode_both(&bytes, decode_part_record),
        Err(RecordError::PartLengthOverflow { chunks: 2 })
    );
}

/// **1m** — an unknown field in a stored value is a typed decode rejection
/// (`#[serde(deny_unknown_fields)]` on every landed record type). Exercised on `SlotRecord`'s
/// own wire shape — a different record class than leg 1j's `SessionState`-nested check, so the
/// two cannot be satisfied by one shared code path.
#[test]
fn leg_1m_unknown_field_is_rejected() {
    let attempt = hex32("cc");
    let bytes = format!(
        "{{\"part_number\":1,\"attempt_id\":\"{attempt}\",\"reserved_at_millis\":1,\
         \"lease_expiry_millis\":2,\"extra_field\":true}}"
    )
    .into_bytes();
    assert_malformed(
        decode_both(&bytes, decode_slot_record),
        "slot:",
        "extra_field",
    );
}

/// **1m, one level in** — an unknown field inside a `part:` record's **nested chunk** is a
/// typed decode rejection too, not a silently dropped one.
///
/// [`ChunkRef`]'s own wire shape is open (`metadata.rs:128-140` carries no
/// `deny_unknown_fields`), so a chunk read through it would decode with `bogus` discarded and
/// re-encode without it — a stored field dropped underneath a whole-record CAS, which is
/// exactly the fault leg 1m closes for the outer record. The part record therefore reads its
/// chunks through the module's own closed `ChunkRefWire`. Everything else about this witness is
/// well-formed — `len` agrees with the chunk, the scheme is `None` — so only the unknown-field
/// rule can reject it.
#[test]
fn leg_1m_unknown_field_in_nested_chunk_is_rejected() {
    let digest = hex64("a1");
    let chunk = "{\"id\":1,\"scheme\":\"None\",\"len\":100,\"placement\":[],\"bogus\":true}";
    let bytes = part(chunk, 100, &digest, 1);
    assert_malformed(decode_both(&bytes, decode_part_record), "part:", "bogus");
}

/// **1m, two levels in** — the same closure inside the chunk's own `scheme` object. Without it
/// the nested closure would stop at the chunk and leave `{"ReedSolomon":{…}}` open, so
/// `"junk"` would decode, vanish, and re-encode gone. `k`/`m` are a supported pair
/// (`erasure::supported(2, 1)`) and `len` agrees with the record, isolating the unknown-field
/// rule from leg 1i's geometry check and leg 1k's length check.
#[test]
fn leg_1m_unknown_field_in_nested_scheme_is_rejected() {
    let digest = hex64("a2");
    let chunk = chunk_json(
        1,
        r#"{"ReedSolomon":{"k":2,"m":1,"junk":0}}"#,
        100,
        "[1,2,3]",
    );
    let bytes = part(&chunk, 100, &digest, 1);
    assert_malformed(decode_both(&bytes, decode_part_record), "part:", "junk");
}

/// **The identity mirror of leg 1m, on the nested chunk** — a chunk with `placement`
/// **omitted** is a decode rejection, not a value with an empty placement.
///
/// [`ChunkRef`] carries `#[serde(default)]` on `placement` for the `inode:` corpus written
/// before the field existed (`metadata.rs:120-124`), and nothing skips it on the way out, so an
/// omitted `placement` would decode to `[]` and re-encode as `"placement":[]` — bytes the store
/// never held. Under `require(key, encode(prior))` (`metadata.rs:1794`) that is a permanent
/// `Conflict` on every later part commit; under `require(key, current)`
/// (`metadata.rs:2012`) the CAS wins and silently rewrites the record. `part:` has no stored
/// corpus to stay compatible with (its first writer is #656–#659) and
/// [`metadata::encode`] always emits `placement`, so refusing the spelling the encoder can
/// never write makes the accepted set exactly the encoder's output — the same argument the
/// `content_type` `null` spelling turns on, one level in.
#[test]
fn part_chunk_omitted_placement_is_rejected() {
    let digest = hex64("a3");
    let chunk = "{\"id\":1,\"scheme\":\"None\",\"len\":100}";
    let bytes = part(chunk, 100, &digest, 1);
    assert_malformed(
        decode_both(&bytes, decode_part_record),
        "part:",
        "placement",
    );
}

/// **1m's mirror on the one optional field** — `"content_type":null` is a decode rejection,
/// not a second spelling of "absent". `Option`'s own `Deserialize` would map it to the same
/// `None` the omitted field yields, but the encoder omits `None`
/// (`session_absent_content_type_re_encodes_omitted`), so a `null` this decoder accepted
/// could never be written back: decode→encode would rewrite the stored bytes underneath a
/// whole-record CAS (`0016:555-558`). Not one of the nine named demonstrations — the other
/// half of the identity the omitted-field witness above pins.
#[test]
fn session_null_content_type_spelling_is_rejected() {
    let bytes = format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"content_type\":null,\
         \"created_at_millis\":1000,\"clock_source\":\"wall\",\"epoch\":{EPOCH},\
         \"attempts\":{ATTEMPTS},\"state\":{OPEN_JSON}}}"
    )
    .into_bytes();
    assert_malformed(
        decode_both(&bytes, decode_session_record),
        "mpu:",
        "content_type",
    );
}

// ===========================================================================
// The one positive leg, negated the other way (build-notes.md records the negation).
// ===========================================================================

/// **1i, the positive case** — a `ChunkRef` whose `placement` length does NOT match its
/// scheme's fragment count still decodes: placement length is the standing *contextual*
/// check, liberal on read (ADR-0045, `AGENTS.md:146-149`, `0016:416-429`). `EcScheme::ReedSolomon
/// { k: 2, m: 1 }` has `fragment_count() == 3`, but `placement` carries one entry — decode
/// must still succeed, because geometry is validated and length is not.
///
/// The decoded chunk is compared **whole**, so this is also the witness that reading chunks
/// through the module's closed wire mirror carries every field through unchanged: a
/// conversion that dropped `placement`, defaulted `len` or flattened the scheme would fail
/// here. Each field is deliberately non-default — a non-zero id, a `ReedSolomon` scheme, a
/// non-zero length, a non-empty placement naming a D-server that is not index 0.
#[test]
fn leg_1i_chunk_ref_wrong_placement_length_still_decodes() {
    let digest = hex64("88");
    let chunk = chunk_json(1, r#"{"ReedSolomon":{"k":2,"m":1}}"#, 100, "[5]");
    let bytes = part(&chunk, 100, &digest, 1);
    let record = decode_both(&bytes, decode_part_record).expect(
        "a ChunkRef whose placement length disagrees with its scheme's fragment count still \
         decodes (ADR-0045: placement length is contextual, not structural)",
    );
    assert_eq!(
        record.chunks(),
        [ChunkRef {
            id: 1,
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            len: 100,
            placement: vec![5],
        }]
    );
    assert_eq!(record.chunks()[0].fragment_count(), 3);
}
