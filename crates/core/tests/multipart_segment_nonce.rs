//! Issue #840 (809.2) — every multipart **session record** carries its segment-group nonce,
//! in every state, as `"segment_nonce"`: exactly 32 lowercase hex characters, immediately
//! after `"clock_source"` and before `"epoch"`. `publish_target` is unchanged.
//!
//! The nonce is independent of the upload id, so nothing else on the record can derive it,
//! yet proposal 0016 needs it from Create (`0016:508-518`, `:656`) through every `Completing`
//! attempt (`0016:665`, X57 at `:880`) and rollback (`0016:538-552`) to the terminal delete
//! (`0016:673`), which runs from `Open` and `Aborting` as well as `Completed`.
//!
//! Codec-only: hand-authored bytes through the production `decode_session_record` and
//! `metadata::encode`. The file names only symbols that exist before this change, so on the
//! base it compiles and fails by assertion; the accessors this change adds are witnessed in
//! `multipart_session_records.rs`. Each leg runs once per state as its own `#[test]`:
//!
//! * **(a)** a record carrying the nonce decodes and re-encodes to the input bytes exactly;
//! * **(b)** the same record without the nonce is refused;
//! * **(c)** a nonce that is not 32 lowercase hex characters is refused;
//! * **(d)** the record names its nonce once — a second copy (repeated, or inside
//!   `publish_target`) is refused, and so is the field in any other position.
//!
//! (b)–(d) each begin with (a)'s positive arm, so a decoder that refused the field outright
//! cannot pass them.

#![forbid(unsafe_code)]

use wyrd_core::metadata::{self, SEG_NONCE_HEX_LEN};
use wyrd_core::multipart::{decode_session_record, RecordError, SessionRecord};

const NONCE: &str = "0123456789abcdef0123456789abcdef";

/// The shared wire spelling, verbatim — what #841, #842, #843 and #810's fixtures build on.
const OPEN_EXAMPLE: &str = r#"{"parent":1,"object":"n","created_at_millis":1000,"clock_source":"wall","segment_nonce":"0123456789abcdef0123456789abcdef","epoch":3,"attempts":1,"state":{"kind":"Open"}}"#;
const COMPLETING_EXAMPLE: &str = r#"{"parent":1,"object":"n","created_at_millis":1000,"clock_source":"wall","segment_nonce":"0123456789abcdef0123456789abcdef","epoch":3,"attempts":1,"state":{"kind":"Completing","fenced_at_millis":1,"segments_written":2,"publish_target":{"parent":1,"name":"n","epoch":3}}}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    Completing,
    Aborting,
    Completed,
}

impl State {
    fn json(self) -> String {
        match self {
            Self::Open => r#"{"kind":"Open"}"#.to_owned(),
            Self::Completing => completing(r#"{"parent":1,"name":"n","epoch":3}"#),
            Self::Aborting => r#"{"kind":"Aborting"}"#.to_owned(),
            Self::Completed => format!(
                "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":9,\"version\":4,\
                 \"etag\":\"{}-3\",\"completed_at_millis\":6000,\
                 \"complete_fingerprint\":\"{}\"}}}}",
                "ab".repeat(32),
                "cd".repeat(32)
            ),
        }
    }
}

/// A `Completing` state whose `publish_target` is `target` (its JSON).
fn completing(target: &str) -> String {
    format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":1,\"segments_written\":2,\
         \"publish_target\":{target}}}"
    )
}

/// The record's fields in canonical order, values already JSON, so a leg can drop, replace,
/// repeat or move exactly one field.
fn fields(state: State) -> Vec<(&'static str, String)> {
    vec![
        ("parent", "1".to_owned()),
        ("object", "\"n\"".to_owned()),
        ("created_at_millis", "1000".to_owned()),
        ("clock_source", "\"wall\"".to_owned()),
        ("segment_nonce", format!("\"{NONCE}\"")),
        ("epoch", "3".to_owned()),
        ("attempts", "1".to_owned()),
        ("state", state.json()),
    ]
}

/// Where `segment_nonce` sits in [`fields`]: after `clock_source`, before `epoch`.
const AT: usize = 4;

fn render(fields: &[(&str, String)]) -> String {
    let body: Vec<String> = fields.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect();
    format!("{{{}}}", body.join(","))
}

/// Assert `result` is an `mpu:` [`RecordError::MalformedRecordValue`] whose message names
/// every one of `needles`.
fn assert_malformed(result: Result<SessionRecord, RecordError>, needles: &[&str], what: &str) {
    match result {
        Err(RecordError::MalformedRecordValue { namespace, detail }) => {
            assert_eq!(namespace, "mpu:", "{what}");
            for needle in needles {
                assert!(
                    detail.contains(needle),
                    "{what}: {needle:?} not in {detail}"
                );
            }
        }
        other => panic!("{what}: expected MalformedRecordValue(mpu:), got {other:?}"),
    }
}

/// **(a)** — the canonical record decodes, re-encodes to its input bytes exactly, and the
/// store-wide decode agrees. Also the positive arm (b)–(d) open with.
fn leg_a(state: State) -> SessionRecord {
    let bytes = render(&fields(state));
    let record = decode_session_record(bytes.as_bytes())
        .unwrap_or_else(|fault| panic!("{state:?} carrying its nonce decodes: {fault}"));
    assert_eq!(
        String::from_utf8_lossy(metadata::encode(&record).as_ref()),
        bytes,
        "{state:?}: decode->encode is not byte-identical"
    );
    assert_eq!(
        metadata::decode::<SessionRecord>(bytes.as_bytes()).ok(),
        Some(record.clone()),
        "{state:?}: the store-wide decode disagrees"
    );
    record
}

/// **(b)** — without `segment_nonce` the record is refused, never a session that cannot name
/// the group Create reserved.
fn leg_b(state: State) {
    leg_a(state);
    let mut without = fields(state);
    without.remove(AT);
    let bytes = render(&without);
    assert_malformed(
        decode_session_record(bytes.as_bytes()),
        &["missing field", "segment_nonce"],
        &format!("{state:?} without a nonce"),
    );
    assert!(metadata::decode::<SessionRecord>(bytes.as_bytes()).is_err());
}

/// **(c)** — refused by the nonce rule itself: uppercase (a second spelling of the same 128
/// bits), one character short, and full length but carrying the `seg:` grammar's `:`.
fn leg_c(state: State) {
    leg_a(state);
    let rule = format!("not {SEG_NONCE_HEX_LEN} lowercase hex");
    for bad in [
        "0123456789ABCDEF0123456789abcdef",
        "0123456789abcdef0123456789abcde",
        "0123456789abcdef:123456789abcdef",
    ] {
        let mut malformed = fields(state);
        malformed[AT].1 = format!("\"{bad}\"");
        let bytes = render(&malformed);
        assert_malformed(
            decode_session_record(bytes.as_bytes()),
            &["segment_nonce", &rule],
            &format!("{state:?} with nonce {bad:?}"),
        );
        assert!(metadata::decode::<SessionRecord>(bytes.as_bytes()).is_err());
    }
}

/// **(d)** — one nonce, in one place. Any other position is the canonical-bytes refusal (the
/// store-wide decode, which cannot see bytes, accepts the same value, so nothing else is
/// wrong with it); the key repeated is refused; and for `Completing`, a copy inside
/// `publish_target` is refused both beside the record's own and instead of it.
fn leg_d(state: State) {
    let canonical = leg_a(state);
    let base = fields(state);
    for position in (0..base.len()).filter(|&position| position != AT) {
        let mut moved = base.clone();
        let nonce = moved.remove(AT);
        moved.insert(position, nonce);
        let bytes = render(&moved);
        assert_eq!(
            decode_session_record(bytes.as_bytes()),
            Err(RecordError::NoncanonicalRecordValue { namespace: "mpu:" }),
            "{bytes}"
        );
        assert_eq!(
            metadata::decode::<SessionRecord>(bytes.as_bytes())
                .ok()
                .as_ref(),
            Some(&canonical),
            "{bytes}"
        );
    }

    let mut repeated = base.clone();
    repeated.insert(AT + 1, base[AT].clone());
    assert_malformed(
        decode_session_record(render(&repeated).as_bytes()),
        &["duplicate field", "segment_nonce"],
        &format!("{state:?} naming segment_nonce twice"),
    );

    if state == State::Completing {
        let mut beside = base;
        let state_at = beside.len() - 1;
        beside[state_at].1 = completing(&format!(
            "{{\"parent\":1,\"name\":\"n\",\"epoch\":3,\"segment_nonce\":\"{NONCE}\"}}"
        ));
        assert_malformed(
            decode_session_record(render(&beside).as_bytes()),
            &["unknown field", "segment_nonce"],
            "a second copy inside publish_target",
        );
        let mut instead = beside;
        instead.remove(AT);
        assert_malformed(
            decode_session_record(render(&instead).as_bytes()),
            &["segment_nonce"],
            "the nonce inside publish_target instead of on the record",
        );
    }
}

/// One `#[test]` per state for each leg, so a red run names every state a leg fails in.
macro_rules! per_state {
    ($($module:ident => $leg:ident;)+) => {$(
        mod $module {
            use super::State;

            #[test]
            fn open() {
                super::$leg(State::Open);
            }

            #[test]
            fn completing() {
                super::$leg(State::Completing);
            }

            #[test]
            fn aborting() {
                super::$leg(State::Aborting);
            }

            #[test]
            fn completed() {
                super::$leg(State::Completed);
            }
        }
    )+};
}

per_state! {
    leg_a_nonce_decodes_and_re_encodes_byte_identically => leg_a;
    leg_b_record_without_nonce_is_refused => leg_b;
    leg_c_nonce_not_32_lowercase_hex_is_refused => leg_c;
    leg_d_nonce_named_once_in_one_position => leg_d;
}

/// The fixture renders exactly the shared wire spelling, and both examples round-trip verbatim.
#[test]
fn fixture_is_the_shared_wire_spelling() {
    for (state, example) in [
        (State::Open, OPEN_EXAMPLE),
        (State::Completing, COMPLETING_EXAMPLE),
    ] {
        assert_eq!(render(&fields(state)), example);
        let record = decode_session_record(example.as_bytes())
            .unwrap_or_else(|fault| panic!("the {state:?} example decodes: {fault}"));
        assert_eq!(metadata::encode(&record).as_ref(), example.as_bytes());
    }
}
