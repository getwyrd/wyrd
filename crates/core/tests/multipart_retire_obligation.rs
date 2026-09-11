//! Issue #771 — the multipart **retirement obligation**: the `retire:bytes:<token>` /
//! `retire:records:<token>` *value* (`wyrd_core::multipart::{RetirePayload, PartScope,
//! PartNumberSet, RetireGeneration, decode_retire_obligation}`), proposal 0016 §1 (`:346-356`),
//! its token grammar (`:358-388`), its decode boundary (`:390-441`) and the writer table the
//! accepted shapes are derived from (`:659-673`, `:2187`, `:2193`, `:2417`).
//!
//! **Pure**: no store, no async, no fixture beyond hand-authored JSON bytes and the production
//! codec — mirroring `multipart_session_records.rs` (#716) for the record value this child adds
//! beside it. Every witness is **decoded, never constructed**: [`RetirePayload`] has no
//! writer-side constructor (the first writers are the store round trips, #656–#659), exactly as
//! the session and part records have none.
//!
//! **One decode surface, deliberately.** The sibling files decode each witness twice — through
//! the module's attributed `decode_*` (S2) and through the store-wide `metadata::decode::<T>`
//! (S1) — and assert the two agree. This record class has **no S1 surface at all**: most of its
//! rules are relations against the key that names it, so [`RetirePayload`] carries no
//! `Deserialize` and `metadata::decode::<RetirePayload>` does not compile. That absence is the
//! boundary, not an omission — a payload obtained without its key is exactly the value ADR-0045
//! decision 1 says must not exist. `decode_witness` below is `multipart_session_records.rs:169`'s
//! `decode_both` with the S1 leg dropped for that reason and its identity leg kept.
//!
//! **Serialization identity (R9), and exactly where it is enforced.** Every retirement
//! obligation is installed under `require_absent(retire:<mode>:<token>)` and drained under
//! `require(retire:… == prior)` (`0016:369-373`, `:667`), so a re-encode that is not the identity
//! is a record nothing can precondition on (`AGENTS.md:170-172`). **Production** is what enforces
//! it: `decode_retire_obligation` ends in `require_canonical`, which returns
//! `NoncanonicalRecordValue` unless `encode(decode(bytes)) == bytes` — so the rule is *pinned* by
//! `a_foreign_spelling_of_an_accepted_payload_is_rejected` below, the leg that fails the moment
//! that gate is dropped. `decode_witness`'s own `assert_eq!` re-states the same postcondition over
//! every accepted witness in the file; it is a cross-check, **not** independent evidence, and this
//! file does not claim otherwise: while the gate stands no *accepted* witness can fail it, because
//! production has already proved that equality before returning. What it does is catch the failure
//! for a witness production stopped rejecting — remove `require_canonical` and this assertion is
//! what reports the foreign spellings' non-identity — and it would catch a refactor that compared
//! a *normalized* form instead of the bytes. The sibling helper it is shaped after earns its
//! identity assertion through the S1 leg (`multipart_session_records.rs:169`), which this record
//! class has no surface for (see above).
//!
//! The legs (Falsifiability, brief #771): **ten** isolating negations — R1-generation-both (a
//! generation naming both reclamation sources), R2 (an obligation owing nothing; its three levels
//! — the whole payload, a present-but-empty chunk list, and a generation naming neither source —
//! are separately load-bearing), R3 (mode-in-the-key), R4 in both directions (a session-wide
//! component under a per-part token; the per-part component under a session-wide one), R5
//! (generation identity), R6 (the canonical token epoch), R7 (nested chunk geometry), R8 (the
//! part-number set's canonical spelling) and R9 (the canonical-bytes gate) — each asserted in
//! **one** test, so removing one production check fails exactly one test. The `all`-wildcard scope
//! rule (#771's first-round review) is an eleventh. R1's completeness legs run the other way: the
//! ten writer shapes each decode under their own key, and a payload type whose arms made
//! `{session, parts}` or `{parts} + {seg}` inexpressible would fail them.
//!
//! Two boundary witnesses pin what this child deliberately does **not** check: a `ChunkRef` whose
//! `placement` length disagrees with its scheme still decodes (the standing contextual check,
//! liberal on read — ADR-0045 `:45-49`, `:72`; `AGENTS.md:146-149`; `0016:416-432`), and a
//! segment group's *nonce* is never checked against its token (`0016:499-509`) — only its epoch.
//!
//! No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use wyrd_core::metadata::{self, ChunkRef, EcScheme, SegmentGroup};
use wyrd_core::multipart::{
    decode_retire_obligation, retire_key, AttemptId, PartNumber, PartNumberSet, PartScope,
    RecordError, RetireMode, RetirePayload, RetireToken, RetiredMap, UploadId, MAX_PART_NUMBER,
};

/// The session every session-scoped witness belongs to, and the fence epoch its token names.
/// The epoch is deliberately neither `0` nor `1`: a witness that only ever observes a field's
/// zero value cannot tell a decoder that reads it from one that returns a default, and R6's
/// `E ± 1` legs need room on both sides.
const EPOCH: u64 = 7;
/// The generation a `g:` token names — neither component zero, for the same reason.
const INODE: u64 = 42;
const VERSION: u64 = 4;
/// The part attempt a per-part token names.
const PART: u32 = 3;

/// 32 lowercase-hex characters from a 2-character pair — the length an upload id, an attempt id
/// and a segment-group nonce all share (`0016:493-497`).
fn hex32(pair: &str) -> String {
    pair.repeat(16)
}

/// The **suffix-free** session token — the whole-session obligations a fence, a publication or a
/// rollback installs (`0016:358-366`).
fn session_token() -> RetireToken {
    RetireToken::Session {
        upload_id: UploadId::new(hex32("a1")).expect("32 lowercase-hex characters are an id"),
        epoch: EPOCH,
        part: None,
    }
}

/// The **per-part** session token — its optional `:<part-number>:<attempt-id>` suffix exists for
/// exactly the per-part obligations (`0016:659`, `:672`, `:1620`).
fn part_token() -> RetireToken {
    RetireToken::Session {
        upload_id: UploadId::new(hex32("a1")).expect("32 lowercase-hex characters are an id"),
        epoch: EPOCH,
        part: Some((
            PartNumber::new(PART).expect("a part number in range"),
            AttemptId::new(hex32("b2")).expect("32 lowercase-hex characters are an attempt id"),
        )),
    }
}

/// The generation token — `g:<inode-id>:<version>`, minted by exactly one publication.
fn generation_token() -> RetireToken {
    RetireToken::Generation {
        inode: INODE,
        version: VERSION,
    }
}

/// The four keys every leg is spelled against, minted by the production `retire_key`.
fn bytes_session() -> Vec<u8> {
    retire_key(RetireMode::Bytes, &session_token())
}
fn bytes_part() -> Vec<u8> {
    retire_key(RetireMode::Bytes, &part_token())
}
fn bytes_generation() -> Vec<u8> {
    retire_key(RetireMode::Bytes, &generation_token())
}
fn records_session() -> Vec<u8> {
    retire_key(RetireMode::Records, &session_token())
}

/// A segment group's stored spelling, `{nonce, epoch}` (`crate::metadata::SegmentGroup`).
fn seg_json(epoch: u64) -> String {
    format!("{{\"nonce\":\"{}\",\"epoch\":{epoch}}}", hex32("c3"))
}

fn seg_group(epoch: u64) -> SegmentGroup {
    SegmentGroup::new(hex32("c3"), epoch).expect("32 lowercase-hex characters are a nonce")
}

/// A `{seg: {…}}` payload — one rolled-back attempt's dangling segment records.
fn seg_value(epoch: u64) -> Vec<u8> {
    format!("{{\"seg\":{}}}", seg_json(epoch)).into_bytes()
}

/// One chunk of an obligation's chunk list, spelled exactly as `wyrd_core::multipart`'s closed
/// `ChunkRefWire` declares it: `id`, `scheme`, `len`, `placement`, in that order, none omitted.
fn chunk_json(id: u128, scheme_json: &str, len: u64, placement: &str) -> String {
    format!("{{\"id\":{id},\"scheme\":{scheme_json},\"len\":{len},\"placement\":{placement}}}")
}

/// A supported Reed-Solomon geometry — `erasure::supported(2, 1)`.
const RS_2_1: &str = r#"{"ReedSolomon":{"k":2,"m":1}}"#;

/// The chunk every `chunks` witness carries unless the leg perturbs it. Each field is
/// deliberately non-default: a non-zero id, a real scheme, a non-zero length, a non-empty
/// placement.
fn chunk() -> String {
    chunk_json(9, RS_2_1, 100, "[5,6,7]")
}

/// A `{chunks: […]}` payload around one chunk spelling.
fn chunks_value(chunk_json: &str) -> Vec<u8> {
    format!("{{\"chunks\":[{chunk_json}]}}").into_bytes()
}

/// A `{generation: {inode, version <tail>}}` payload, `tail` being the map it names (`,"chunks":
/// […]`, `,"segments":{…}`, or nothing at all).
fn generation_value(inode: u64, version: u64, tail: &str) -> Vec<u8> {
    format!("{{\"generation\":{{\"inode\":{inode},\"version\":{version}{tail}}}}}").into_bytes()
}

// ===========================================================================
// The one decode surface + serialization identity. Every witness in this file — accepted or
// refused — goes through this helper, which is what makes R9 a property of the whole accepted
// set rather than a test of its own (`multipart_session_records.rs:169`, minus the S1 leg this
// record class has no surface for; see the header).
// ===========================================================================

/// Decode `value` under `key` the way production does, and assert of an **accepted** witness that
/// re-encoding it through the store-wide codec reproduces the bytes it was read from (R9).
fn decode_witness(
    key: &[u8],
    value: &[u8],
) -> Result<(RetireMode, RetireToken, RetirePayload), RecordError> {
    let decoded = decode_retire_obligation(key, value);
    if let Ok((_, _, payload)) = &decoded {
        assert_eq!(
            String::from_utf8_lossy(metadata::encode(payload).as_ref()),
            String::from_utf8_lossy(value),
            "decode->encode is not byte-identical for {payload:?}"
        );
    }
    decoded
}

/// The payload of a witness this file expects to be **accepted**, with `why` naming the writer
/// row it comes from.
fn accepted(key: &[u8], value: &[u8], why: &str) -> RetirePayload {
    decode_witness(key, value).expect(why).2
}

/// The chunks a decoded [`RetiredMap`] carries, or a panic naming the arm found instead.
fn flat(map: &RetiredMap) -> &[ChunkRef] {
    match map {
        RetiredMap::Flat(chunks) => chunks,
        other => panic!("expected a flat generation, got {other:?}"),
    }
}

/// Assert a witness is **refused** with exactly `expected` — the typed rejection, never a
/// message match, so a leg cannot pass on the wrong rule.
fn refused(key: &[u8], value: &[u8], expected: RecordError) {
    assert_eq!(
        decode_witness(key, value),
        Err(expected),
        "unexpected verdict for {}",
        String::from_utf8_lossy(value)
    );
}

/// Assert a witness was refused by serde itself, as this record class's attributed
/// [`RecordError::MalformedRecordValue`] naming `needle`.
fn refused_as_malformed(key: &[u8], value: &[u8], needle: &str) {
    match decode_witness(key, value) {
        Err(RecordError::MalformedRecordValue { namespace, detail }) => {
            assert_eq!(namespace, "retire:");
            assert!(
                detail.contains(needle),
                "expected the decoder's message to name {needle:?}: {detail}"
            );
        }
        other => panic!("expected MalformedRecordValue(retire:), got {other:?}"),
    }
}

/// The runs a decoded [`PartScope::Set`] carries, or a panic naming what was found instead.
fn runs(scope: Option<&PartScope>) -> Vec<(u32, u32)> {
    match scope {
        Some(PartScope::Set(set)) => set.runs().to_vec(),
        other => panic!("expected an explicit part set, got {other:?}"),
    }
}

// ===========================================================================
// R1 — shape completeness. Every obligation `0016`'s writer rows install decodes from its own
// key, as a SINGLE value under a SINGLE key. The combined shapes (`{session, parts}` and
// `{parts} + {seg}`) are the reviewed defect of the archived attempts: a payload whose components
// are mutually exclusive forces a writer to install two records where the protocol installs one —
// two keys where `require_absent` and the session's own emptiness gate expect one
// (`0016:369-380`). A generation's two sources are the one exclusive choice in the record, for the
// opposite reason: they mirror the two-arm committed map, and only one of them ever described a
// published generation (`a_generation_naming_both_sources_is_rejected`).
// ===========================================================================

/// `retire:bytes:` + a suffix-free `s:` token, `{session, all}` — the reaper's `Open` teardown
/// (`0016:2187`), and the **only** row the `all` wildcard has: it is the instruction to enumerate
/// the session's own `part:<id>:` range at drain time, which the teardown fence has just made
/// immutable.
#[test]
fn bytes_session_and_all_parts_decodes() {
    let value = br#"{"session":true,"parts":"all"}"#;
    let (_, token, payload) =
        decode_witness(&bytes_session(), value).expect("the session teardown obligation");
    assert_eq!(token, session_token());
    assert!(payload.session());
    assert_eq!(payload.parts(), Some(&PartScope::All));
    assert!(payload.chunks().is_empty());
    assert!(payload.generation().is_none());
    assert!(payload.segments().is_none());
}

/// `retire:bytes:` + a suffix-free `s:` token, `{session, parts: <set>}` — the
/// `Completing`→`Aborting` fence and the restore fence (`0016:665`, `:823`, `:2193`). **The
/// combined shape**: one value carrying both the session's staged residue and the exact part set
/// the fence froze.
#[test]
fn bytes_session_and_parts_decodes() {
    let payload = accepted(
        &bytes_session(),
        br#"{"session":true,"parts":[[1,4],[7,9]]}"#,
        "a session teardown naming its parts",
    );
    assert!(payload.session());
    assert_eq!(runs(payload.parts()), vec![(1, 4), (7, 9)]);
}

/// `retire:bytes:` + a suffix-free `s:` token, `{session}` alone — the abort/reap fence row's own
/// spelling (`0016:664`, `:1001`): the session's staged residue, no part.
#[test]
fn bytes_session_alone_decodes() {
    let payload = accepted(
        &bytes_session(),
        br#"{"session":true}"#,
        "the session's staged residue",
    );
    assert!(payload.session());
    assert!(payload.parts().is_none());
}

/// `retire:bytes:` + a suffix-free `s:` token, `{parts: <set>}` alone — the root flip's
/// **unnamed** staged parts, whose bytes are orphan-marked and whose records are then deleted by
/// this one obligation (`0016:662`, `:919-921`). No `session` component: the session itself
/// survives the publication.
#[test]
fn bytes_parts_alone_decodes() {
    let payload = accepted(
        &bytes_session(),
        br#"{"parts":[[2,2]]}"#,
        "the unnamed staged parts",
    );
    assert!(!payload.session());
    assert_eq!(runs(payload.parts()), vec![(2, 2)]);
}

/// `retire:bytes:` + a **per-part** `s:` token, `{chunks: […]}` — a losing writer's compensation,
/// a re-upload's superseded chunks, a post-staging local refusal (`0016:659`, `:672`, `:1620`).
/// Its chunks have no naming record left, so the obligation carries them.
#[test]
fn bytes_chunks_under_a_per_part_token_decodes() {
    let (_, token, payload) = decode_witness(&bytes_part(), &chunks_value(&chunk()))
        .expect("a per-part chunk obligation");
    assert_eq!(token, part_token());
    assert_eq!(payload.chunks().len(), 1);
    assert_eq!(payload.chunks()[0].id, 9);
    assert_eq!(
        payload.chunks()[0].scheme,
        EcScheme::ReedSolomon { k: 2, m: 1 }
    );
    assert_eq!(payload.chunks()[0].len, 100);
    assert_eq!(payload.chunks()[0].placement, vec![5, 6, 7]);
}

/// `retire:bytes:` + a `g:` token, `{generation: {inode, version, chunks}}` — a superseded or
/// deleted **flat** generation, retired by the chunk list its root carried (`0016:355`, `:668`).
#[test]
fn bytes_generation_with_chunks_decodes() {
    let value = generation_value(INODE, VERSION, &format!(",\"chunks\":[{}]", chunk()));
    let (_, token, payload) =
        decode_witness(&bytes_generation(), &value).expect("a superseded flat generation");
    assert_eq!(token, generation_token());
    let generation = payload.generation().expect("a generation component");
    assert_eq!(generation.inode(), INODE);
    assert_eq!(generation.version(), VERSION);
    assert_eq!(flat(generation.map()).len(), 1);
    assert_eq!(flat(generation.map())[0].id, 9);
}

/// `retire:bytes:` + a `g:` token, `{generation: {inode, version, segments}}` — a superseded or
/// deleted **segmented** generation, named by its `seg:` key range rather than by frozen
/// placements (`0016:2417-2425`).
#[test]
fn bytes_generation_with_segments_decodes() {
    let value = generation_value(INODE, VERSION, &format!(",\"segments\":{}", seg_json(2)));
    let payload = accepted(
        &bytes_generation(),
        &value,
        "a superseded segmented generation",
    );
    let generation = payload.generation().expect("a generation component");
    assert_eq!(generation.map(), &RetiredMap::Segmented(seg_group(2)));
}

/// `retire:records:` + a suffix-free `s:` token, `{parts: <set>}` — the root flip's **published**
/// parts, whose bytes the published inode now protects (`0016:662`, `:919-921`).
#[test]
fn records_parts_decodes() {
    let payload = accepted(
        &records_session(),
        br#"{"parts":[[1,4]]}"#,
        "the published parts' records",
    );
    assert_eq!(runs(payload.parts()), vec![(1, 4)]);
}

/// `retire:records:` + a suffix-free `s:` token, `{seg: {nonce, epoch}}` — one rolled-back
/// `Completing` attempt's dangling segment records, naming **exactly that epoch's** keys
/// (`0016:663`, `:665`, `:2357-2362`).
#[test]
fn records_seg_decodes() {
    let payload = accepted(
        &records_session(),
        &seg_value(EPOCH),
        "a rolled-back attempt's segments",
    );
    assert_eq!(payload.segments(), Some(&seg_group(EPOCH)));
}

/// The decode **answers the mode**, so the two opposite obligations that share the `{parts}` shape
/// are told apart by their decode results alone (`0016:434-441`). One value, two keys: under
/// `retire:bytes:` it means orphan-mark those parts' fragments and then delete their records
/// (`0016:662`, `:919-921`); under `retire:records:` it means delete the **published** parts'
/// records, whose bytes the published object now protects, and orphan-mark nothing. A drain handed
/// only `(token, payload)` would have to re-parse the key to know which — a second spelling of a
/// decision this decode has already made, and the one place it could be got backwards.
#[test]
fn the_mode_is_part_of_the_decoded_obligation() {
    let value = br#"{"parts":[[1,4]]}"#;
    let (bytes_mode, bytes_token, bytes_payload) =
        decode_witness(&bytes_session(), value).expect("the unnamed staged parts");
    let (records_mode, records_token, records_payload) =
        decode_witness(&records_session(), value).expect("the published parts' records");
    assert_eq!(bytes_mode, RetireMode::Bytes);
    assert_eq!(records_mode, RetireMode::Records);
    assert_ne!(bytes_mode, records_mode);
    // Everything else about the two obligations is identical — the mode is the whole difference,
    // and it is in the answer rather than left in the key the caller was given.
    assert_eq!(bytes_token, records_token);
    assert_eq!(bytes_payload, records_payload);
}

/// `retire:records:` + a suffix-free `s:` token, `{parts: <set>, seg: {…}}` — **both in one
/// payload** (`0016:356`, "and/or"). The second combined shape the archived attempts could not
/// express.
#[test]
fn records_parts_and_seg_decodes() {
    let value = format!("{{\"parts\":[[1,4]],\"seg\":{}}}", seg_json(EPOCH)).into_bytes();
    let payload = accepted(
        &records_session(),
        &value,
        "parts and segments in one obligation",
    );
    assert_eq!(runs(payload.parts()), vec![(1, 4)]);
    assert_eq!(payload.segments(), Some(&seg_group(EPOCH)));
}

// ===========================================================================
// R1-generation-both and R2–R9 — the rejections. One test per rule, so removing one production
// check fails exactly one test.
// ===========================================================================

/// **R1, the exclusive half** — a generation naming **both** reclamation sources is refused. The
/// obligation mirrors the committed map it retires, and that map is the two-arm
/// `metadata::ChunkMap` (`Flat | Segmented`, `metadata.rs:1014`): a segmented root carries no
/// inline chunk list, so a flat generation retires by its copied `chunks` and a segmented one by
/// its `segments` group, re-read at drain time. `0016` spells the row two ways (`:355`
/// `chunks, segments?`; `:2417` `chunks?, segments`) — **those are the two cases, not a union**
/// (settled 2026-09-11; the erratum rides the PR description, not an edit to the proposal).
///
/// Accepting the pair would leave the first drain (#656–#659) to invent a meaning for a value no
/// writer installs: orphan-mark the inline list *and* walk a segment range, for a generation only
/// one of them ever described. `RetiredMap` makes the pair unrepresentable **after** decode; this
/// leg pins the rejection an operator is shown when a stored value spells it — the typed
/// `RetireGenerationBothSources`, naming the generation, rather than the canonical-bytes gate's
/// undifferentiated "non-canonical bytes" (which is what refuses it once the typed check is
/// removed — the same division of labour `checked_chunks` makes for a present-but-empty list).
#[test]
fn a_generation_naming_both_sources_is_rejected() {
    let both = format!(",\"chunks\":[{}],\"segments\":{}", chunk(), seg_json(2));
    refused(
        &bytes_generation(),
        &generation_value(INODE, VERSION, &both),
        RecordError::RetireGenerationBothSources {
            inode: INODE,
            version: VERSION,
        },
    );
}

/// **R2** — an obligation naming nothing is refused: residue nothing drains. A drain that met one
/// would mark nothing, delete the obligation, and record the work as done.
///
/// Every spelling of "nothing" is a witness, and each names the component it was found in — the
/// whole value, an explicitly empty part set, a present-but-empty chunk list (the payload's own
/// and a generation's, which are two reads of the same rule), and a generation naming neither of
/// its two reclamation sources. A **present-but-empty** list is the spelling that survives a
/// shape rule written only over the outer value: `{"generation":{…,"chunks":[]}}` names a
/// generation, so the payload owes "something" by any outer count while the drain has nothing to
/// mark.
#[test]
fn an_obligation_owing_nothing_is_rejected() {
    let key = bytes_session();
    let owes_nothing = |component| RecordError::RetireObligationOwesNothing { component };
    let refuse = |value: &[u8], component| refused(&key, value, owes_nothing(component));
    refuse(b"{}", "payload");
    refuse(br#"{"session":false}"#, "payload");
    refuse(br#"{"chunks":[]}"#, "chunks");
    refuse(br#"{"parts":[]}"#, "parts");
    let refuse_generation = |tail: &str, component| {
        refused(
            &bytes_generation(),
            &generation_value(INODE, VERSION, tail),
            owes_nothing(component),
        );
    };
    refuse_generation("", "generation");
    refuse_generation(",\"chunks\":[]", "generation.chunks");
}

/// **R3** — mode agreement. The mode lives in the key precisely so this is a decode error and
/// never a misread boolean (`0016:434-441`). `chunks` and `generation` orphan-mark, so they are
/// `retire:bytes:` only; `session` names the staged residue it marks before deleting, so it is
/// `retire:bytes:` only too (`0016:355`, `:664`, `:2587`); the `all` wildcard has one writer row
/// and it is a `retire:bytes:` one (`0016:2187`) — under `retire:records:` it would delete every
/// part record of a live session, staged ones included (`0016:919-921`, X104); and `seg` must
/// never orphan anything, since its segments' fragments are still protected by the `part:`
/// records, so it is `retire:records:` only (`0016:2347-2350`).
#[test]
fn a_component_under_the_wrong_mode_is_rejected() {
    let wrong_mode = |key_mode, component| RecordError::RetireModeMismatch {
        key_mode,
        component,
    };
    let under_records = |key: &[u8], value: &[u8], component| {
        refused(key, value, wrong_mode(RetireMode::Records, component));
    };
    let records_session = records_session();
    under_records(
        &retire_key(RetireMode::Records, &part_token()),
        &chunks_value(&chunk()),
        "chunks",
    );
    under_records(
        &retire_key(RetireMode::Records, &generation_token()),
        &generation_value(INODE, VERSION, &format!(",\"segments\":{}", seg_json(2))),
        "generation",
    );
    under_records(&records_session, br#"{"session":true}"#, "session");
    under_records(&records_session, br#"{"parts":"all"}"#, "parts:all");
    refused(
        &bytes_session(),
        &seg_value(EPOCH),
        wrong_mode(RetireMode::Bytes, "seg"),
    );
}

/// The `all` wildcard's other half: it may not appear **without** the `session` teardown that
/// installs it (`0016:2187` is its only row). `all` is an instruction to enumerate the session's
/// `part:<id>:` range at drain time, and only the teardown fence freezes that range — without it
/// the obligation names whatever a still-live session happens to hold when the drain arrives, a
/// set no writer chose. Its mode half is the R3 leg above.
#[test]
fn the_all_parts_wildcard_outside_a_session_teardown_is_rejected() {
    refused(
        &bytes_session(),
        br#"{"parts":"all"}"#,
        RecordError::RetireAllPartsWithoutSession,
    );
}

/// **R4, first direction** — a **session-wide** component under a **per-part** token. The
/// optional `:<part-number>:<attempt-id>` suffix exists only for the per-part obligations
/// (`0016:358-366`); a whole-session obligation filed under one part's token is cleared by that
/// part's drain and is never enumerated by the session's own emptiness gate (`0016:374-380`).
/// #692's batch review recorded the broken arm accepting **every** session-scoped payload, so
/// each of the three is asserted.
#[test]
fn a_session_wide_component_under_a_per_part_token_is_rejected() {
    let suffix_mismatch = |component| RecordError::RetireTokenSuffixMismatch {
        component,
        token_names_part: true,
    };
    let key = bytes_part();
    let refuse = |value: &[u8], component| refused(&key, value, suffix_mismatch(component));
    refuse(br#"{"session":true,"parts":"all"}"#, "session");
    refuse(br#"{"parts":[[1,4]]}"#, "parts");
    refused(
        &retire_key(RetireMode::Records, &part_token()),
        &seg_value(EPOCH),
        suffix_mismatch("seg"),
    );
}

/// **R4, second direction** — the **per-part** component under a **session-wide** token. A
/// per-part obligation filed session-wide names an attempt nothing can attribute, and its token
/// is one a later whole-session fence at the same epoch would collide with.
#[test]
fn chunks_under_a_session_wide_token_is_rejected() {
    refused(
        &bytes_session(),
        &chunks_value(&chunk()),
        RecordError::RetireTokenSuffixMismatch {
            component: "chunks",
            token_names_part: false,
        },
    );
}

/// **R5** — generation identity, in all three directions: a `generation` payload whose
/// `(inode, version)` differs from its `g:` token's, a `generation` under an `s:` token, and a
/// session-scoped payload under a `g:` token. Any of them lets a drain evidence one generation's
/// fragments while clearing another's obligation (`0016:369-373`, outcome (a)).
#[test]
fn a_generation_disagreeing_with_its_token_is_rejected() {
    let chunks = format!(",\"chunks\":[{}]", chunk());
    let mismatch = |payload_inode, payload_version| RecordError::RetireGenerationIdentityMismatch {
        key_inode: INODE,
        key_version: VERSION,
        payload_inode,
        payload_version,
    };
    refused(
        &bytes_generation(),
        &generation_value(INODE + 1, VERSION, &chunks),
        mismatch(INODE + 1, VERSION),
    );
    refused(
        &bytes_generation(),
        &generation_value(INODE, VERSION + 1, &chunks),
        mismatch(INODE, VERSION + 1),
    );
    refused(
        &bytes_session(),
        &generation_value(INODE, VERSION, &chunks),
        RecordError::RetireTokenScopeMismatch {
            token: "s:",
            component: "generation",
        },
    );
    refused(
        &bytes_generation(),
        br#"{"session":true,"parts":"all"}"#,
        RecordError::RetireTokenScopeMismatch {
            token: "g:",
            component: "session",
        },
    );
}

/// **R6** — the canonical token epoch. The token's epoch is the epoch the installing fence was
/// taken against — `require(mpu == Completing@E)` — which for a `{seg}` obligation is `E` itself,
/// the epoch whose segment keys it names (`0016:663-665`, `:2357-2362`). Decode enforces
/// `token.epoch == seg.epoch` **exactly**: `E ± 1` is refused, because one obligation with
/// several legal keys is one `require_absent` cannot refuse a second installation of — the same
/// obligation installed and drained twice (`0016:369-373`).
#[test]
fn a_segment_group_epoch_other_than_the_tokens_is_rejected() {
    for epoch in [EPOCH - 1, EPOCH + 1] {
        refused(
            &records_session(),
            &seg_value(epoch),
            RecordError::RetireSegmentEpochMismatch {
                key_epoch: EPOCH,
                segment_epoch: epoch,
            },
        );
    }
    accepted(
        &records_session(),
        &seg_value(EPOCH),
        "the token's own epoch is the one canonical spelling",
    );
}

/// **The limit of R6, stated rather than over-claimed.** A segment group's nonce is deliberately
/// independent of the upload id, because segment records outlive the `mpu:` tombstone that would
/// otherwise be their only reuse guard (`0016:499-509`) — so a *foreign* group under your token
/// is **not** detectable at decode, and the epoch is the only component this check binds. A `g:`
/// token carries no epoch at all, so a generation's segment group is checked against nothing
/// here: its identity is the installing writer's to establish and the drain's to act on
/// (#656–#659).
#[test]
fn a_segment_groups_nonce_and_a_generations_epoch_are_unchecked() {
    for epoch in [0, EPOCH, u64::MAX] {
        let value = generation_value(
            INODE,
            VERSION,
            &format!(",\"segments\":{}", seg_json(epoch)),
        );
        let payload = accepted(&bytes_generation(), &value, "a generation's segment group");
        let generation = payload.generation().expect("a generation component");
        assert_eq!(generation.map(), &RetiredMap::Segmented(seg_group(epoch)));
    }
    let foreign = format!(
        "{{\"seg\":{{\"nonce\":\"{}\",\"epoch\":{EPOCH}}}}}",
        hex32("d4")
    )
    .into_bytes();
    accepted(
        &records_session(),
        &foreign,
        "a foreign group's nonce is not detectable at decode — only its epoch is bound",
    );
}

/// **R7** — nested chunk geometry. Every `ChunkRef` in `chunks` **and** in `generation.chunks` is
/// rejected unless `erasure::supported(k, m)` — the `checked_chunk_scheme` rule, the #285
/// precedent, ADR-0045's invariant table (`0045:71-72`). An obligation's chunk list is exactly
/// the untrusted stored geometry a drain fans its orphan marks out over.
#[test]
fn an_unsupported_chunk_scheme_is_rejected() {
    let bad = chunk_json(9, r#"{"ReedSolomon":{"k":0,"m":1}}"#, 100, "[5]");
    let unsupported = RecordError::ChunkSchemeUnsupported {
        chunk_id: 9,
        k: 0,
        m: 1,
    };
    refused(&bytes_part(), &chunks_value(&bad), unsupported.clone());
    refused(
        &bytes_generation(),
        &generation_value(INODE, VERSION, &format!(",\"chunks\":[{bad}]")),
        unsupported,
    );
}

/// **R8** — the part-number set's structure. `parts` is range-encoded (`0016:382-388`) and its
/// spelling is **canonical**: runs ordered, non-overlapping and non-adjacent, each endpoint in
/// `[1, MAX_PART_NUMBER]`, `lo <= hi`. Two spellings of one obligation defeat `require_absent`
/// exactly as two keys do.
#[test]
fn a_noncanonical_part_number_set_is_rejected() {
    let key = bytes_session();
    let refuse = |value: &[u8], expected| refused(&key, value, expected);
    let not_coalesced =
        |lo, previous_hi| RecordError::PartNumberRunsNotCoalesced { lo, previous_hi };
    // Adjacent: `[[1,2],[3,4]]` is not a second spelling of `[[1,4]]`.
    refuse(br#"{"parts":[[1,2],[3,4]]}"#, not_coalesced(3, 2));
    // Overlapping, and out of order.
    refuse(br#"{"parts":[[1,5],[4,8]]}"#, not_coalesced(4, 5));
    refuse(br#"{"parts":[[5,9],[1,3]]}"#, not_coalesced(1, 9));
    // Reversed: names no part at all.
    refuse(
        br#"{"parts":[[4,2]]}"#,
        RecordError::PartNumberRunReversed { lo: 4, hi: 2 },
    );
    // Out of range, at both ends of what the `part:` key grammar can spell.
    let out_of_range = |part_number| RecordError::PartNumberOutOfRange { part_number };
    refuse(br#"{"parts":[[0,3]]}"#, out_of_range(0));
    let past_the_key_space = u64::from(MAX_PART_NUMBER) + 1;
    let past = format!("{{\"parts\":[[1,{past_the_key_space}]]}}");
    refuse(past.as_bytes(), out_of_range(past_the_key_space));
    // The boundary itself is addressable and decodes.
    let whole = format!("{{\"parts\":[[1,{MAX_PART_NUMBER}]]}}");
    let payload = accepted(&key, whole.as_bytes(), "the whole key space is a set");
    assert_eq!(runs(payload.parts()), vec![(1, MAX_PART_NUMBER)]);
}

/// **R9** — the canonical-bytes gate. Every accepted witness above re-encodes byte-identically
/// (asserted file-wide by `decode_witness`); this is the other half — a foreign spelling of an
/// otherwise valid payload is refused rather than decoded into a value whose re-encode no CAS
/// could match (`0016:369-373`, `:667`).
#[test]
fn a_foreign_spelling_of_an_accepted_payload_is_rejected() {
    let noncanonical = || RecordError::NoncanonicalRecordValue {
        namespace: "retire:",
    };
    for value in [
        // Fields reordered — `parts` before `session`.
        &br#"{"parts":[[1,4]],"session":true}"#[..],
        // Whitespace inserted after one colon.
        &br#"{"session": true,"parts":"all"}"#[..],
        // An absent component spelled `false` rather than omitted (`AGENTS.md:170-172`).
        &br#"{"session":false,"parts":[[1,4]]}"#[..],
    ] {
        refused(&bytes_session(), value, noncanonical());
    }
    // A generation's **absent** source spelled `null` rather than omitted — the same rule one
    // level in. `null` decodes as absence, so the value is a well-formed flat generation whose
    // re-encode simply does not carry the field; only the byte gate can refuse it, and it is what
    // keeps `{…, "segments": null}` from becoming a second spelling of `{…}`.
    let null_segments = generation_value(
        INODE,
        VERSION,
        &format!(",\"chunks\":[{}],\"segments\":null", chunk()),
    );
    refused(&bytes_generation(), &null_segments, noncanonical());
    // An equivalent `\u` escape inside a nonce — JSON calls the strings equal, the encoder never
    // writes the escape.
    let escaped = format!(
        "{{\"seg\":{{\"nonce\":\"\\u0063{}\",\"epoch\":{EPOCH}}}}}",
        &hex32("c3")[1..]
    )
    .into_bytes();
    refused(&records_session(), &escaped, noncanonical());
}

// ===========================================================================
// The boundary this child does NOT cross, and the wire shapes it reads through.
// ===========================================================================

/// A `ChunkRef` whose `placement` length disagrees with its scheme's fragment count **decodes**:
/// the standing *contextual* check, liberal on read and strict in maintenance paths (ADR-0045
/// `:45-49` and its `ChunkRef` row `:72`, `AGENTS.md:146-149`, `0016:416-432`). Turning it into a
/// decode error would convert a quarantinable record into an error that aborts a whole reconcile
/// step.
#[test]
fn a_placement_length_mismatch_still_decodes() {
    // rs(2,1) has three fragments; the placement names one.
    let value = chunks_value(&chunk_json(9, RS_2_1, 100, "[5]"));
    let payload = accepted(
        &bytes_part(),
        &value,
        "a length-mismatched placement decodes",
    );
    assert_eq!(payload.chunks()[0].placement, vec![5]);
}

/// The chunk list is read through the module's **own closed** `ChunkRefWire`, not through
/// `metadata::ChunkRef`: an omitted `placement` is a decode error here rather than a field
/// defaulted on the way in and re-encoded as `"placement":[]` — bytes no CAS built from
/// decode→encode could match. The payload shape is closed the same way, so a field this build
/// does not know is a decode error rather than one dropped on the way in.
#[test]
fn an_unknown_or_omitted_field_is_rejected() {
    let no_placement = format!("{{\"id\":9,\"scheme\":{RS_2_1},\"len\":100}}");
    refused_as_malformed(&bytes_part(), &chunks_value(&no_placement), "placement");
    refused_as_malformed(
        &bytes_session(),
        br#"{"session":true,"sessions":true}"#,
        "sessions",
    );
}

/// `parts` is the wildcard string or the range encoding, and **nothing else** — a third spelling
/// is a typed rejection rather than a wildcard by accident. A segment group is read through
/// `metadata::SegmentGroup`'s own validating decode, so a nonce that could not key a reproducible
/// `seg:` range is refused rather than stored (`metadata.rs:741-763`).
#[test]
fn a_component_spelled_outside_its_grammar_is_rejected() {
    let scope = "range-encoded part-number set";
    refused_as_malformed(&bytes_session(), br#"{"parts":"everything"}"#, scope);
    refused_as_malformed(&bytes_session(), br#"{"parts":4}"#, scope);
    refused_as_malformed(
        &records_session(),
        br#"{"seg":{"nonce":"nothex","epoch":7}}"#,
        "nonce",
    );
}

/// The **key** half is decoded first and fails closed: a `retire:` key naming a third mode is
/// [`RecordError::UnknownRetireMode`] and a key that is not a `retire:` key at all is
/// [`RecordError::MalformedKey`], whatever the value says (`0016:434-441`; `parse_retire_key`).
#[test]
fn a_key_that_names_no_obligation_is_rejected() {
    let session = br#"{"session":true}"#;
    refused(
        b"retire:sideways:s:x:1",
        session,
        RecordError::UnknownRetireMode {
            mode: "sideways".to_string(),
        },
    );
    refused(
        b"retire:bytes:",
        session,
        RecordError::MalformedKey {
            namespace: "retire:",
            key: "retire:bytes:".to_string(),
        },
    );
}

// ===========================================================================
// The part-number set's minting side — the writer-facing constructor that makes a second
// spelling unrepresentable at the source, not merely refused at decode.
// ===========================================================================

/// `PartNumberSet::from_numbers` sorts, deduplicates and coalesces, so it can only mint the
/// canonical encoding its own decode accepts — and the set it mints round-trips through the
/// stored spelling.
///
/// It can also only mint a set its own decode **accepts at all**: an iterator naming no part
/// answers `None` rather than a value whose stored spelling `[]` decode then refuses. The writer
/// rows that call it compute a possibly-empty set — the staged parts a Complete did not name
/// (`0016:662`, `:919-921`) — so a total constructor would let a #656–#659 writer install an
/// obligation no drain can decode, under a key the session's terminal-delete emptiness gate
/// (`0016:673`) then never sees cleared. The emptiness decision belongs in front of the writer,
/// which is what `Option` puts it there.
#[test]
fn part_number_set_minting_is_canonical_and_never_empty() {
    let numbers = [5u32, 1, 2, 9, 3, 5]
        .into_iter()
        .map(|n| PartNumber::new(n).expect("a part number in range"));
    let set = PartNumberSet::from_numbers(numbers).expect("six part numbers are a set");
    assert_eq!(set.runs(), [(1, 3), (5, 5), (9, 9)]);
    assert_eq!(
        PartNumberSet::from_runs(set.runs().to_vec()).expect("a minted set is canonical"),
        set
    );
    // What a total constructor would have minted here is exactly what decode refuses under
    // `{"parts":[]}` (`an_obligation_owing_nothing_is_rejected`) — the two halves of one rule.
    assert_eq!(PartNumberSet::from_numbers([]), None);
}
