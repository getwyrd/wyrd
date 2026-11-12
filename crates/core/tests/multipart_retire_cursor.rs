//! Issue #1061 — the retirement drain's cursor inside `RetirePayload`: its codec, decoded only
//! through `decode_retire_obligation` against the key and value it is given, and written only
//! through `RetirePayload::advanced`, which checks it the same way.
//!
//! * **(K, unchanged bytes)** An obligation without a cursor decodes to none and re-encodes
//!   byte-identical, for every row the decoder accepts, and the installers this file can reach
//!   (the restore and abort teardowns) write no cursor: no installer's bytes change.
//! * **(K, round trip)** A cursor in each component decodes to exactly itself and re-encodes
//!   byte-identical; `advanced` writes those same bytes.
//! * **(K, refusals)** Decode refuses, each with its typed error, every cursor it can judge from
//!   the key and value: a component the payload does not carry; an entry off the work the payload
//!   lists (a part number past the set's last member or between its runs, an index past a
//!   `{chunks}` list or a flat `{generation}` list); an entry past the key grammar's bound
//!   (`MAX_SEGMENT_INDEX` for a `{seg}` group); an item under `retire:records:`; a non-canonical
//!   spelling. `advanced` refuses the same cursors with the same errors. A `{seg}` cursor past the
//!   group's last record is **not** judged here — the payload names no extent; the drain judges it
//!   by reading the range (`crates/custodian/tests/multipart_retirement_drain.rs`, leg K).
//! * **(K, room)** `RETIRE_CURSOR_MAX_BYTES` is what the widest cursor decode accepts adds to a
//!   value, byte for byte, and `RETIRE_VALUE_CEILING` is `MAX_VALUE_BYTES` less it.
//! * The part set's member lookup the drain pages with (`PartNumberSet::members_from`).

#![forbid(unsafe_code)]

use wyrd_core::metadata::{self as md, MAX_SEGMENT_INDEX, MAX_VALUE_BYTES};
use wyrd_core::multipart::{
    decode_retire_obligation, decode_session_record, retire_key, AttemptId, PartNumber,
    PartNumberSet, RecordError, RetireComponent, RetireCursor, RetireMode, RetireToken, UploadId,
    MAX_PART_NUMBER, RETIRE_CURSOR_MAX_BYTES, RETIRE_VALUE_CEILING,
};

const EPOCH: u64 = 3;
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const CHUNK: &str = r#"{"id":7,"scheme":"None","len":5,"placement":[1]}"#;
const CHUNK_2: &str = r#"{"id":8,"scheme":"None","len":5,"placement":[2]}"#;

fn upload() -> UploadId {
    UploadId::new("ab".repeat(16)).unwrap()
}

fn session_key(mode: RetireMode) -> Vec<u8> {
    let token = RetireToken::Session {
        upload_id: upload(),
        epoch: EPOCH,
        part: None,
    };
    retire_key(mode, &token)
}

fn part_key() -> Vec<u8> {
    let token = RetireToken::Session {
        upload_id: upload(),
        epoch: EPOCH,
        part: Some((
            PartNumber::new(4).unwrap(),
            AttemptId::new("cd".repeat(16)).unwrap(),
        )),
    };
    retire_key(RetireMode::Bytes, &token)
}

fn generation_key() -> Vec<u8> {
    retire_key(
        RetireMode::Bytes,
        &RetireToken::Generation {
            inode: 70,
            version: 2,
        },
    )
}

fn seg() -> String {
    format!(r#"{{"nonce":"{NONCE}","epoch":{EPOCH}}}"#)
}

/// Every row the decoder accepts (the table on `RetirePayload`), as today's installers spell it:
/// `(mode, key, value)`.
fn rows() -> Vec<(RetireMode, Vec<u8>, String)> {
    let records = session_key(RetireMode::Records);
    let bytes = session_key(RetireMode::Bytes);
    vec![
        (
            RetireMode::Bytes,
            bytes.clone(),
            r#"{"session":true}"#.to_owned(),
        ),
        (
            RetireMode::Bytes,
            bytes.clone(),
            r#"{"session":true,"parts":"all"}"#.to_owned(),
        ),
        (
            RetireMode::Bytes,
            bytes.clone(),
            r#"{"session":true,"parts":[[1,4],[7,7]]}"#.to_owned(),
        ),
        (
            RetireMode::Bytes,
            bytes,
            r#"{"parts":[[2,2],[6,9]]}"#.to_owned(),
        ),
        (
            RetireMode::Bytes,
            part_key(),
            format!(r#"{{"chunks":[{CHUNK},{CHUNK_2}]}}"#),
        ),
        (
            RetireMode::Bytes,
            generation_key(),
            format!(r#"{{"generation":{{"inode":70,"version":2,"chunks":[{CHUNK}]}}}}"#),
        ),
        (
            RetireMode::Bytes,
            generation_key(),
            format!(
                r#"{{"generation":{{"inode":70,"version":2,"segments":{}}}}}"#,
                seg()
            ),
        ),
        (
            RetireMode::Records,
            records.clone(),
            r#"{"parts":[[1,4],[7,7]]}"#.to_owned(),
        ),
        (
            RetireMode::Records,
            records.clone(),
            format!(r#"{{"seg":{}}}"#, seg()),
        ),
        (
            RetireMode::Records,
            records,
            format!(r#"{{"parts":[[1,4]],"seg":{}}}"#, seg()),
        ),
    ]
}

/// `value` with `cursor` spelled in as its last field.
fn with_cursor(value: &str, cursor: &str) -> String {
    let open = value.strip_suffix('}').expect("an object");
    format!(r#"{open},"cursor":{cursor}}}"#)
}

/// **(K, unchanged bytes)** Every row, spelled as today: it decodes with no cursor and re-encodes
/// to exactly the bytes it was read from. The teardowns a session record mints — the abort and
/// restore fences' `{session, all}`, a `Completing` fence's `{session, all}` and `{seg}` — write
/// no cursor either.
#[test]
fn k_an_obligation_without_a_cursor_decodes_to_none_and_encodes_as_before() {
    for (mode, key, value) in rows() {
        let (found, _, payload) = decode_retire_obligation(&key, value.as_bytes()).expect(&value);
        assert_eq!(found, mode);
        assert_eq!(payload.cursor(), None, "{value}");
        assert_eq!(md::encode(&payload), value.as_bytes(), "{value}");
    }

    let open = format!(
        r#"{{"parent":42,"object":"o","created_at_millis":100,"clock_source":"wall","segment_nonce":"{NONCE}","epoch":{EPOCH},"attempts":1,"state":{{"kind":"Open"}}}}"#
    );
    let open = decode_session_record(open.as_bytes()).unwrap();
    let teardown = open.open_teardown(&upload()).expect("a teardown");
    let installed = md::encode(teardown.obligation().payload());
    assert_eq!(installed.as_ref(), br#"{"session":true,"parts":"all"}"#);

    let completing = format!(
        r#"{{"parent":42,"object":"o","created_at_millis":100,"clock_source":"wall","segment_nonce":"{NONCE}","epoch":{EPOCH},"attempts":1,"state":{{"kind":"Completing","fenced_at_millis":900,"segments_written":2,"publish_target":{{"parent":42,"name":"o","epoch":{EPOCH}}}}}}}"#
    );
    let completing = decode_session_record(completing.as_bytes()).unwrap();
    let teardown = completing
        .completing_teardown(&upload())
        .expect("a teardown");
    let bytes = md::encode(teardown.bytes().payload());
    assert_eq!(bytes.as_ref(), br#"{"session":true,"parts":"all"}"#);
    let records = md::encode(teardown.records().payload());
    assert_eq!(
        records.as_ref(),
        format!(r#"{{"seg":{}}}"#, seg()).as_bytes()
    );
}

/// **(K, round trip)** A cursor in each component of each row decodes to exactly itself, the rest
/// of the payload unchanged, and re-encodes to the bytes read; `advanced` from the cursor-less
/// payload writes those same bytes. An item is spelled only when it is not zero.
#[test]
fn k_a_cursor_round_trips_exactly() {
    use RetireComponent::{Chunks, Generation, Parts, Seg, Session};
    let rows = rows();
    let cases: [(usize, RetireCursor, &str); 12] = [
        (
            0,
            RetireCursor::new(Session, 1, 0),
            r#"{"component":"session","entry":1}"#,
        ),
        (
            1,
            RetireCursor::new(Parts, MAX_PART_NUMBER, 5),
            r#"{"component":"parts","entry":999999,"item":5}"#,
        ),
        (
            1,
            RetireCursor::new(Session, 17, 0),
            r#"{"component":"session","entry":17}"#,
        ),
        (
            2,
            RetireCursor::new(Parts, 7, 3),
            r#"{"component":"parts","entry":7,"item":3}"#,
        ),
        (
            3,
            RetireCursor::new(Parts, 9, 0),
            r#"{"component":"parts","entry":9}"#,
        ),
        (
            4,
            RetireCursor::new(Chunks, 1, 8),
            r#"{"component":"chunks","entry":1,"item":8}"#,
        ),
        (
            5,
            RetireCursor::new(Generation, 0, 2),
            r#"{"component":"generation","entry":0,"item":2}"#,
        ),
        (
            6,
            RetireCursor::new(Generation, MAX_SEGMENT_INDEX, u32::MAX),
            r#"{"component":"generation","entry":999999,"item":4294967295}"#,
        ),
        (
            7,
            RetireCursor::new(Parts, 3, 0),
            r#"{"component":"parts","entry":3}"#,
        ),
        (
            8,
            RetireCursor::new(Seg, MAX_SEGMENT_INDEX, 0),
            r#"{"component":"seg","entry":999999}"#,
        ),
        (
            9,
            RetireCursor::new(Parts, 4, 0),
            r#"{"component":"parts","entry":4}"#,
        ),
        (
            9,
            RetireCursor::new(Seg, 0, 0),
            r#"{"component":"seg","entry":0}"#,
        ),
    ];
    for (row, cursor, spelled) in cases {
        let (mode, key, value) = &rows[row];
        let stored = with_cursor(value, spelled);
        let (_, _, payload) = decode_retire_obligation(key, stored.as_bytes()).expect(&stored);
        assert_eq!(payload.cursor(), Some(&cursor), "{stored}");
        assert_eq!(md::encode(&payload), stored.as_bytes(), "{stored}");
        let (_, _, base) = decode_retire_obligation(key, value.as_bytes()).unwrap();
        let advanced = base.advanced(*mode, cursor).expect(&stored);
        assert_eq!(
            md::encode(&advanced),
            stored.as_bytes(),
            "advanced: {stored}"
        );
        assert_eq!(advanced, payload);
    }
}

/// **(K, refusals)** Every cursor decode can judge from the key and value it is given is refused
/// with its own typed error, and `advanced` refuses the same cursor with the same error.
#[test]
fn k_decode_refuses_every_cursor_it_can_judge() {
    use RecordError::{
        RetireCursorComponentAbsent as Absent, RetireCursorItemInRecordsMode as Item,
        RetireCursorOutsideWork as Outside, RetireCursorUnaddressable as Unaddressable,
    };
    use RetireComponent::{Chunks, Generation, Parts, Seg, Session};
    let rows = rows();
    let cases: Vec<(usize, RetireCursor, RecordError)> = vec![
        // A component the payload does not carry.
        (7, RetireCursor::new(Seg, 0, 0), Absent { component: "seg" }),
        (
            8,
            RetireCursor::new(Parts, 1, 0),
            Absent { component: "parts" },
        ),
        (
            3,
            RetireCursor::new(Session, 1, 0),
            Absent {
                component: "session",
            },
        ),
        (
            4,
            RetireCursor::new(Generation, 0, 0),
            Absent {
                component: "generation",
            },
        ),
        (
            5,
            RetireCursor::new(Chunks, 0, 0),
            Absent {
                component: "chunks",
            },
        ),
        // Off the work the payload lists: past the set's last member, between its runs, below
        // its first; past a `{chunks}` list; past a flat `{generation}` list.
        (
            7,
            RetireCursor::new(Parts, 8, 0),
            Outside {
                component: "parts",
                entry: 8,
            },
        ),
        (
            7,
            RetireCursor::new(Parts, 5, 0),
            Outside {
                component: "parts",
                entry: 5,
            },
        ),
        (
            3,
            RetireCursor::new(Parts, 1, 0),
            Outside {
                component: "parts",
                entry: 1,
            },
        ),
        (
            4,
            RetireCursor::new(Chunks, 2, 0),
            Outside {
                component: "chunks",
                entry: 2,
            },
        ),
        (
            5,
            RetireCursor::new(Generation, 1, 0),
            Outside {
                component: "generation",
                entry: 1,
            },
        ),
        // Past the key grammar's bound, for work named only by a store range.
        (
            8,
            RetireCursor::new(Seg, MAX_SEGMENT_INDEX + 1, 0),
            Unaddressable {
                component: "seg",
                entry: MAX_SEGMENT_INDEX + 1,
                max: MAX_SEGMENT_INDEX,
            },
        ),
        (
            6,
            RetireCursor::new(Generation, MAX_SEGMENT_INDEX + 1, 0),
            Unaddressable {
                component: "generation",
                entry: MAX_SEGMENT_INDEX + 1,
                max: MAX_SEGMENT_INDEX,
            },
        ),
        (
            1,
            RetireCursor::new(Parts, 0, 0),
            Unaddressable {
                component: "parts",
                entry: 0,
                max: MAX_PART_NUMBER,
            },
        ),
        (
            0,
            RetireCursor::new(Session, MAX_PART_NUMBER + 1, 0),
            Unaddressable {
                component: "session",
                entry: MAX_PART_NUMBER + 1,
                max: MAX_PART_NUMBER,
            },
        ),
        // An item inside a records-mode entry, which has none.
        (
            8,
            RetireCursor::new(Seg, 3, 1),
            Item {
                component: "seg",
                item: 1,
            },
        ),
        (
            7,
            RetireCursor::new(Parts, 2, 4),
            Item {
                component: "parts",
                item: 4,
            },
        ),
    ];
    for (row, cursor, expected) in cases {
        let (mode, key, value) = &rows[row];
        let mut spelled = format!(
            r#"{{"component":"{}","entry":{}"#,
            cursor.component().name(),
            cursor.entry()
        );
        if cursor.item() != 0 {
            spelled.push_str(&format!(r#","item":{}"#, cursor.item()));
        }
        spelled.push('}');
        let stored = with_cursor(value, &spelled);
        let refused = decode_retire_obligation(key, stored.as_bytes()).expect_err(&stored);
        assert_eq!(refused, expected, "{stored}");
        let (_, _, base) = decode_retire_obligation(key, value.as_bytes()).unwrap();
        assert_eq!(
            base.advanced(*mode, cursor).unwrap_err(),
            expected,
            "advanced: {stored}"
        );
    }
}

/// **(K, spelling)** A cursor spelled any way but the codec's own is refused: an explicit zero
/// item (the codec omits it), an unknown field, an unknown component, an entry past `u32`.
#[test]
fn k_decode_refuses_a_cursor_spelled_any_other_way() {
    let rows = rows();
    let (_, key, value) = &rows[8];
    let noncanonical = with_cursor(value, r#"{"component":"seg","entry":3,"item":0}"#);
    assert_eq!(
        decode_retire_obligation(key, noncanonical.as_bytes()).unwrap_err(),
        RecordError::NoncanonicalRecordValue {
            namespace: "retire:"
        }
    );
    for cursor in [
        r#"{"component":"seg","entry":3,"page":1}"#,
        r#"{"component":"segments","entry":3}"#,
        r#"{"component":"seg","entry":4294967296}"#,
        r#"{"component":"seg"}"#,
    ] {
        let stored = with_cursor(value, cursor);
        let refused = decode_retire_obligation(key, stored.as_bytes()).expect_err(&stored);
        assert!(
            matches!(
                refused,
                RecordError::MalformedRecordValue {
                    namespace: "retire:",
                    ..
                }
            ),
            "{stored}: {refused:?}"
        );
    }
}

/// **(K, room)** The widest cursor decode accepts — the longest component name, the widest entry
/// any component's bound admits, the widest item — adds exactly `RETIRE_CURSOR_MAX_BYTES` to a
/// value, and the narrower records-mode cursors add less. The installer ceiling is
/// `MAX_VALUE_BYTES` less that room, 99,931 bytes.
#[test]
fn k_the_reserved_room_is_the_widest_cursor_decode_accepts() {
    let rows = rows();
    let (mode, key, value) = &rows[6];
    let (_, _, base) = decode_retire_obligation(key, value.as_bytes()).unwrap();
    let widest = RetireCursor::new(RetireComponent::Generation, MAX_SEGMENT_INDEX, u32::MAX);
    let advanced = md::encode(&base.advanced(*mode, widest).unwrap());
    assert_eq!(advanced.len() - value.len(), RETIRE_CURSOR_MAX_BYTES);
    assert_eq!(RETIRE_CURSOR_MAX_BYTES, 69);
    assert_eq!(
        RETIRE_VALUE_CEILING,
        MAX_VALUE_BYTES - RETIRE_CURSOR_MAX_BYTES
    );
    assert_eq!(RETIRE_VALUE_CEILING, 99_931);

    for (row, cursor) in [
        (7, RetireCursor::new(RetireComponent::Parts, 7, 0)),
        (
            8,
            RetireCursor::new(RetireComponent::Seg, MAX_SEGMENT_INDEX, 0),
        ),
    ] {
        let (mode, key, value) = &rows[row];
        let (_, _, base) = decode_retire_obligation(key, value.as_bytes()).unwrap();
        let advanced = md::encode(&base.advanced(*mode, cursor).unwrap());
        assert!(advanced.len() - value.len() <= RETIRE_CURSOR_MAX_BYTES);
    }
}

/// The drain's member lookup: the members from the first one at or after a position, by the runs,
/// whatever run the position falls in, between, before or after; `contains` agrees.
#[test]
fn members_from_starts_at_the_first_member_at_or_after_its_position() {
    let set = PartNumberSet::from_runs(vec![(1, 3), (7, 7), (10, 12)]).unwrap();
    let from = |n: u32| set.members_from(n).collect::<Vec<u32>>();
    assert_eq!(from(0), [1, 2, 3, 7, 10, 11, 12]);
    assert_eq!(from(2), [2, 3, 7, 10, 11, 12]);
    assert_eq!(from(4), [7, 10, 11, 12]);
    assert_eq!(from(7), [7, 10, 11, 12]);
    assert_eq!(from(8), [10, 11, 12]);
    assert_eq!(from(12), [12]);
    assert_eq!(from(13), Vec::<u32>::new());
    let members = [1, 2, 3, 7, 10, 11, 12];
    for n in 0..=14 {
        assert_eq!(set.contains(n), members.contains(&n), "{n}");
    }

    // A sparse set over the whole key space: every lookup lands on its member, wherever it is.
    let sparse = PartNumberSet::from_numbers(
        (1..=MAX_PART_NUMBER)
            .step_by(2)
            .map(|n| PartNumber::new(n).unwrap()),
    )
    .unwrap();
    assert_eq!(sparse.runs().len(), 500_000);
    for n in [1, 2, 499_999, 500_000, 999_997, 999_999] {
        let first = sparse.members_from(n).next();
        assert_eq!(first, Some(n | 1), "{n}");
    }
    assert_eq!(sparse.members_from(MAX_PART_NUMBER + 1).next(), None);
}
