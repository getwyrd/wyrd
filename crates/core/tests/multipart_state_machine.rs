//! Issue #693 (#654 split 3/3) — the vocabulary every later multipart slice answers in:
//! decision 3's **verb × state answer table** as pure, total functions
//! (`wyrd_core::multipart::{answer, Verb, *Answer, *Outcome, Refusal, InvalidPart,
//! Backpressure, Publication}`), the object's identity `multipart_etag`, and the request
//! identity a `Completed` tombstone answers a retry on, `complete_fingerprint` (proposal 0016
//! decision 3, `:894-1037`; the composition ADR-0047 deferred, `0016:3064-3070`,
//! ADR-0047:73-89, `:112`).
//!
//! **Pure**: no store, no async — literals, hand-authored record bytes, and the production
//! functions under test. Both digests are checked against **independent oracles** built here
//! straight from `sha2` over bytes this file assembles itself; beyond `Digest`'s raw-byte
//! accessors, no oracle touches production code, and none goes through a production
//! composition or hex rendering.
//!
//! The five legs (Success criterion, brief #693):
//!
//! * **leg 1** — every cell of the 5 × 5 product (`{UploadPart, CompleteMultipartUpload,
//!   AbortMultipartUpload, ListParts, ListMultipartUploads} × {Open, Completing, Aborting,
//!   Completed, absent}`) is answered by its typed outcome. One helper enumerates the whole
//!   product and fails on any cell the transcribed table has no answer for, so a later verb or
//!   state cannot be added silently. The conditional cell gets both branches, and its match
//!   branch is asserted down to the **whole** recorded ETag, `-N` suffix included — from a
//!   tombstone decoded out of its stored bytes, as a retry meets it;
//! * **leg 2** — `multipart_etag` equals the oracle, with its discriminating cases, and refuses
//!   a non-ascending or duplicate list rather than sorting it;
//! * **leg 3** — `complete_fingerprint` tells an identical retry from a different assembly, and
//!   is pinned to its oracle;
//! * **leg 4** — `MultipartEtag::parse` validates the grammar and the count's range, accepting
//!   exactly `[1, MAX_PART_NUMBER]` and attributing every refusal to its rule;
//! * **leg 5** — every public outcome enum is matched without a wildcard arm, so a
//!   `#[non_exhaustive]` marker (today or added later) fails this file to compile.
//!
//! No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use std::collections::HashSet;

use sha2::{Digest as _, Sha256};
use wyrd_core::metadata;
use wyrd_core::multipart::{
    abort_answer, answer, complete_answer, complete_fingerprint, decode_session_record,
    list_parts_answer, list_uploads_answer, multipart_etag, upload_part_answer, AbortAnswer,
    AbortOutcome, Answer, Backpressure, CompleteAnswer, CompleteOutcome, Completion, CreateOutcome,
    Digest, InvalidPart, ListPartsAnswer, ListUploadsAnswer, MultipartEtag, PartNumber,
    Publication, PublishTarget, RecordError, Refusal, ReserveOutcome, SessionState,
    UploadPartAnswer, UploadPartOutcome, Verb, MAX_PART_NUMBER,
};

// ===========================================================================
// Oracles and fixtures — built from `sha2` and std only, never a production helper
// ===========================================================================

/// SHA-256 over the concatenation of `pieces`, straight from `sha2`.
fn sha256(pieces: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for piece in pieces {
        hasher.update(piece);
    }
    hasher.finalize().into()
}

/// Lowercase hex through std's own `{:02x}` — deliberately not `multipart::hex_lower`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A part's content digest: the SHA-256 of `label`, so every fixture digest is distinct and
/// none is the all-zero value a copy-paste slip would produce unnoticed.
fn digest(label: &str) -> Digest {
    Digest::from_bytes(sha256(&[label.as_bytes()]))
}

fn part(number: u32) -> PartNumber {
    PartNumber::new(number).expect("test part numbers are in range")
}

/// A named-part list in the production argument shape.
fn named(list: &[(u32, Digest)]) -> Vec<(PartNumber, Digest)> {
    list.iter()
        .map(|&(number, digest)| (part(number), digest))
        .collect()
}

/// Leg 2's oracle, the composition written out: `lowercase_hex(SHA-256(d_1 ‖ … ‖ d_N)) + "-"
/// + N` over the **raw** digest bytes, in the order given, `N` the number of entries.
fn oracle_etag(list: &[(u32, Digest)]) -> String {
    let raw: Vec<&[u8]> = list
        .iter()
        .map(|(_, digest)| &digest.as_bytes()[..])
        .collect();
    format!("{}-{}", hex(&sha256(&raw)), list.len())
}

/// Leg 3's oracle: SHA-256 over `be32(n_i) ‖ d_i` for each pair, in the order given.
fn oracle_fingerprint(list: &[(u32, Digest)]) -> Digest {
    let mut preimage = Vec::new();
    for (number, digest) in list {
        preimage.extend_from_slice(&number.to_be_bytes());
        preimage.extend_from_slice(digest.as_bytes());
    }
    Digest::from_bytes(sha256(&[&preimage]))
}

/// The winning Complete every `Completed` fixture recorded: parts 1, 2 and 4. Its `N` (3) is
/// neither 1 nor its highest part number (4), so an answer whose suffix is dropped, defaulted
/// or taken from the wrong quantity is visible.
fn winning_parts() -> Vec<(u32, Digest)> {
    vec![
        (1, digest("part 1")),
        (2, digest("part 2")),
        (4, digest("part 4")),
    ]
}

/// The ETag that winning Complete answered — the oracle's spelling, never `multipart_etag`'s.
fn recorded_etag_text() -> String {
    oracle_etag(&winning_parts())
}

fn recorded_etag() -> MultipartEtag {
    MultipartEtag::parse(&recorded_etag_text()).expect("the oracle spells a canonical ETag")
}

fn recorded_fingerprint() -> Digest {
    oracle_fingerprint(&winning_parts())
}

const INODE: u64 = 9;
const VERSION: u64 = 4;
const COMPLETED_AT: u64 = 6000;

/// What the tombstone recorded, and therefore what an identical retry must be answered with.
fn recorded_publication() -> Publication {
    Publication {
        inode: INODE,
        version: VERSION,
        etag: recorded_etag(),
        completed_at_millis: COMPLETED_AT,
    }
}

// ===========================================================================
// Leg 1 — every decision-3 cell has a typed answer (`0016:969-978`)
// ===========================================================================

/// The columns of decision 3's table: the four [`SessionState`]s and "record absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Open,
    Completing,
    Aborting,
    Completed,
    Absent,
}

/// How many columns the table has. Every row below is an array of exactly this many answers,
/// so a column added here without an answer in every row fails to compile.
const STATES: usize = 5;

const COLUMNS: [Column; STATES] = [
    Column::Open,
    Column::Completing,
    Column::Aborting,
    Column::Completed,
    Column::Absent,
];

impl Column {
    /// The column a state sits in. Exhaustive with **no wildcard arm**: a new `SessionState`
    /// variant fails this file to compile until it is given a column — and, through [`STATES`],
    /// an answer in every row of the table.
    fn of(state: Option<&SessionState>) -> Self {
        match state {
            Some(SessionState::Open {}) => Self::Open,
            Some(SessionState::Completing { .. }) => Self::Completing,
            Some(SessionState::Aborting {}) => Self::Aborting,
            Some(SessionState::Completed { .. }) => Self::Completed,
            None => Self::Absent,
        }
    }

    /// A state in this column. Only the `Completed` fields are ever read by the table (the
    /// conditional cell's), and they are the winning Complete's.
    fn state(self) -> Option<SessionState> {
        match self {
            Self::Open => Some(SessionState::Open {}),
            Self::Completing => Some(SessionState::Completing {
                fenced_at_millis: 1,
                segments_written: 0,
                publish_target: PublishTarget {
                    parent: 7,
                    name: "object".to_string(),
                    epoch: 1,
                },
            }),
            Self::Aborting => Some(SessionState::Aborting {}),
            Self::Completed => Some(SessionState::Completed {
                completion: Completion {
                    inode: INODE,
                    version: VERSION,
                    etag: recorded_etag(),
                    completed_at_millis: COMPLETED_AT,
                    complete_fingerprint: recorded_fingerprint(),
                },
            }),
            Self::Absent => None,
        }
    }
}

fn upload(answer: UploadPartAnswer) -> Answer {
    Answer::UploadPart(answer)
}

fn complete(answer: CompleteAnswer) -> Answer {
    Answer::Complete(answer)
}

fn abort(answer: AbortAnswer) -> Answer {
    Answer::Abort(answer)
}

fn list_parts(answer: ListPartsAnswer) -> Answer {
    Answer::ListParts(answer)
}

fn list_uploads(answer: ListUploadsAnswer) -> Answer {
    Answer::ListUploads(answer)
}

/// Decision 3's table (`0016:969-978`) in the proposal's own layout: one row per verb, its
/// cells in [`COLUMNS`] order — `Open`, `Completing`, `Aborting`, `Completed`, absent — and no
/// wildcard. The `CompleteMultipartUpload` × `Completed` cell is its **match** branch: the
/// request is the winning Complete, retried.
fn decision_3_table() -> Vec<(Verb, [Answer; STATES])> {
    let no_such_upload = || Refusal::NoSuchUpload;
    let operation_aborted = || Refusal::OperationAborted;
    vec![
        (
            Verb::UploadPart,
            [
                upload(UploadPartAnswer::Accepted),
                upload(UploadPartAnswer::Refused(no_such_upload())),
                upload(UploadPartAnswer::Refused(no_such_upload())),
                upload(UploadPartAnswer::Refused(no_such_upload())),
                upload(UploadPartAnswer::Refused(no_such_upload())),
            ],
        ),
        (
            Verb::CompleteMultipartUpload,
            [
                complete(CompleteAnswer::Fences),
                complete(CompleteAnswer::Refused(operation_aborted())),
                complete(CompleteAnswer::Refused(no_such_upload())),
                complete(CompleteAnswer::AlreadyCompleted(recorded_publication())),
                complete(CompleteAnswer::Refused(no_such_upload())),
            ],
        ),
        (
            Verb::AbortMultipartUpload,
            [
                abort(AbortAnswer::Fences),
                abort(AbortAnswer::Refused(operation_aborted())),
                abort(AbortAnswer::AlreadyAborting),
                abort(AbortAnswer::Refused(no_such_upload())),
                abort(AbortAnswer::Refused(no_such_upload())),
            ],
        ),
        (
            Verb::ListParts,
            [
                list_parts(ListPartsAnswer::OpenSet),
                list_parts(ListPartsAnswer::FrozenSet),
                list_parts(ListPartsAnswer::Refused(no_such_upload())),
                list_parts(ListPartsAnswer::Refused(no_such_upload())),
                list_parts(ListPartsAnswer::Refused(no_such_upload())),
            ],
        ),
        (
            Verb::ListMultipartUploads,
            [
                list_uploads(ListUploadsAnswer::Listed),
                list_uploads(ListUploadsAnswer::Listed),
                list_uploads(ListUploadsAnswer::NotListed),
                list_uploads(ListUploadsAnswer::NotListed),
                list_uploads(ListUploadsAnswer::NotListed),
            ],
        ),
    ]
}

/// Leg 1's helper. Enumerates the whole `Verb::ALL × COLUMNS` product and asserts production's
/// [`answer`] against the table in every cell. A verb with no row fails as **unanswered** (one
/// with two rows as ambiguous), and every row holds exactly [`STATES`] cells by type, so
/// neither a verb nor a state can be added without an answer.
fn assert_every_cell_answered(table: &[(Verb, [Answer; STATES])], fingerprint: Option<&Digest>) {
    let verbs: HashSet<Verb> = Verb::ALL.into_iter().collect();
    assert_eq!(
        verbs.len(),
        5,
        "Verb::ALL must name each of the five verbs once"
    );

    let mut cells = 0usize;
    for verb in Verb::ALL {
        let rows: Vec<&[Answer; STATES]> = table
            .iter()
            .filter(|(row_verb, _)| *row_verb == verb)
            .map(|(_, row)| row)
            .collect();
        assert_eq!(
            rows.len(),
            1,
            "{verb:?} has {} rows in decision 3's table; none means it is unanswered",
            rows.len()
        );
        for (column, expected) in COLUMNS.into_iter().zip(rows[0]) {
            let state = column.state();
            assert_eq!(Column::of(state.as_ref()), column, "fixture for {column:?}");
            assert_eq!(
                answer(verb, state.as_ref(), fingerprint),
                *expected,
                "{verb:?} × {column:?}"
            );
            cells += 1;
        }
    }
    assert_eq!(cells, 25, "the product is exactly 5 verbs × 5 states");
    assert_eq!(
        table.len(),
        Verb::ALL.len(),
        "a row for a verb outside Verb::ALL"
    );
}

/// All 25 cells, for an identical retry — the request fingerprint is the winning Complete's.
#[test]
fn every_decision_3_cell_is_answered_for_an_identical_retry() {
    let retry = complete_fingerprint(&named(&winning_parts()))
        .expect("the winning list is ascending, so it fingerprints");
    assert_every_cell_answered(&decision_3_table(), Some(&retry));
}

/// The conditional cell's other branch (`0016:898-908`), for both inputs a non-identical
/// Complete can bring: a different assembly's fingerprint, and none at all (a named-part list
/// `complete_fingerprint` refused). The tombstone answers `NoSuchUpload` — never the recorded
/// publication — and every other cell is unchanged, because the fingerprint is read by that
/// one cell only.
#[test]
fn a_non_identical_complete_is_refused_by_the_tombstone() {
    let completed = COLUMNS
        .iter()
        .position(|c| *c == Column::Completed)
        .unwrap();
    let mut table = decision_3_table();
    for (verb, row) in &mut table {
        if *verb == Verb::CompleteMultipartUpload {
            row[completed] = complete(CompleteAnswer::Refused(Refusal::NoSuchUpload));
        }
    }
    let subset = complete_fingerprint(&named(&winning_parts()[..2])).unwrap();
    assert_ne!(subset, recorded_fingerprint());
    assert_every_cell_answered(&table, Some(&subset));
    assert_every_cell_answered(&table, None);
}

/// Each per-verb function is independently callable (the shape #508 and #656–#659 need) and
/// agrees with the dispatcher on every state.
#[test]
fn per_verb_answers_agree_with_the_dispatcher() {
    let fingerprint = recorded_fingerprint();
    for column in COLUMNS {
        let state = column.state();
        let state = state.as_ref();
        assert_eq!(
            upload(upload_part_answer(state)),
            answer(Verb::UploadPart, state, None)
        );
        assert_eq!(
            complete(complete_answer(state, Some(&fingerprint))),
            answer(Verb::CompleteMultipartUpload, state, Some(&fingerprint))
        );
        assert_eq!(
            abort(abort_answer(state)),
            answer(Verb::AbortMultipartUpload, state, None)
        );
        assert_eq!(
            list_parts(list_parts_answer(state)),
            answer(Verb::ListParts, state, None)
        );
        assert_eq!(
            list_uploads(list_uploads_answer(state)),
            answer(Verb::ListMultipartUploads, state, None)
        );
    }
}

/// A `Completed` session record exactly as the store holds it — field names and order as the
/// production types declare them, so the canonical-bytes gate of `decode_session_record`
/// accepts it only if the codec re-spells it identically.
fn completed_session_bytes(etag: &str, fingerprint: &Digest) -> Vec<u8> {
    format!(
        "{{\"parent\":42,\"object\":\"key/one\",\"created_at_millis\":1000,\
         \"clock_source\":\"wall\",\"epoch\":3,\"attempts\":2,\"state\":{{\"kind\":\"Completed\",\
         \"completion\":{{\"inode\":{INODE},\"version\":{VERSION},\"etag\":\"{etag}\",\
         \"completed_at_millis\":{COMPLETED_AT},\"complete_fingerprint\":\"{}\"}}}}}}",
        hex(fingerprint.as_bytes())
    )
    .into_bytes()
}

/// The retry path end to end, from **stored bytes**: the tombstone decodes, an identical retry
/// — fingerprinted by production from its own named parts — is answered with the recorded
/// publication, and its ETag is the recorded one **verbatim, `-3` included**. By then the part
/// records the ETag was composed from may already be retired (`0016:964-968`), so the record is
/// all there is: an answer carrying only the digest half would tell the client that the object
/// its first Complete created has a different ETag.
///
/// Every different assembly under the same consumed upload id is refused.
#[test]
fn a_stored_tombstone_answers_an_identical_retry_with_the_whole_recorded_etag() {
    let bytes = completed_session_bytes(&recorded_etag_text(), &recorded_fingerprint());
    let record = decode_session_record(&bytes).expect("a Completed session record decodes");
    assert_eq!(metadata::encode(&record).as_ref(), bytes.as_slice());

    let retry = complete_fingerprint(&named(&winning_parts())).unwrap();
    let CompleteAnswer::AlreadyCompleted(publication) =
        complete_answer(Some(record.state()), Some(&retry))
    else {
        panic!("an identical retry must be answered from the tombstone");
    };
    assert_eq!(publication.etag.to_string(), recorded_etag_text());
    assert!(publication.etag.to_string().ends_with("-3"));
    assert_eq!(publication.etag.parts(), 3);
    assert_eq!(publication, recorded_publication());

    let winning = winning_parts();
    let (d1, d2, d4) = (winning[0].1, winning[1].1, winning[2].1);
    let different_assemblies = [
        vec![(1, d1), (2, d2)],
        vec![(1, d1), (2, d2), (3, d4)],
        vec![(1, d1), (2, d2), (4, digest("another part 4"))],
        vec![(1, d1), (2, d2), (4, d4), (5, digest("part 5"))],
    ];
    for list in different_assemblies {
        let fingerprint = complete_fingerprint(&named(&list)).unwrap();
        assert_eq!(
            complete_answer(Some(record.state()), Some(&fingerprint)),
            CompleteAnswer::Refused(Refusal::NoSuchUpload),
            "{list:?} is a different assembly"
        );
    }
}

/// A tombstone that recorded a bare digest — no `-N` — is refused at decode: it could only
/// answer a retry with an ETag the original Complete never returned.
#[test]
fn a_tombstone_recording_a_bare_digest_is_refused_at_decode() {
    let bare = hex(recorded_etag().composed().as_bytes());
    let bytes = completed_session_bytes(&bare, &recorded_fingerprint());
    match decode_session_record(&bytes) {
        Err(RecordError::MalformedRecordValue { namespace, detail }) => {
            assert_eq!(namespace, "mpu:");
            assert!(detail.contains(&bare), "{detail}");
        }
        other => panic!("a bare-digest ETag must not decode, got {other:?}"),
    }
}

// ===========================================================================
// Leg 2 — `multipart_etag` is the settled composition, against the oracle
// ===========================================================================

#[test]
fn multipart_etag_of_one_part_is_the_oracle() {
    let list = [(1, digest("solo"))];
    let etag = multipart_etag(&named(&list)).unwrap();
    assert_eq!(etag.to_string(), oracle_etag(&list));
    assert_eq!(etag.parts(), 1);
    assert!(etag.to_string().ends_with("-1"));
}

#[test]
fn multipart_etag_of_the_winning_list_is_the_recorded_etag() {
    let etag = multipart_etag(&named(&winning_parts())).unwrap();
    assert_eq!(etag.to_string(), recorded_etag_text());
    assert_eq!(etag, recorded_etag());
}

#[test]
fn multipart_etag_of_a_strict_subset_differs_from_the_full_set() {
    let full = winning_parts();
    let subset = &full[..2];
    let etag_full = multipart_etag(&named(&full)).unwrap();
    let etag_subset = multipart_etag(&named(subset)).unwrap();
    assert_ne!(etag_full.composed(), etag_subset.composed());
    assert_eq!(etag_subset.to_string(), oracle_etag(subset));
}

/// The digests are concatenated as **raw bytes**; concatenating their hex text is a different
/// composition with a different result.
#[test]
fn multipart_etag_is_over_raw_digest_bytes_not_hex_text() {
    let list = winning_parts();
    let etag = multipart_etag(&named(&list)).unwrap();
    let hex_texts: Vec<String> = list.iter().map(|(_, d)| hex(d.as_bytes())).collect();
    let pieces: Vec<&[u8]> = hex_texts.iter().map(String::as_bytes).collect();
    let over_hex_text = format!("{}-{}", hex(&sha256(&pieces)), list.len());
    assert_ne!(etag.to_string(), over_hex_text);
    assert_eq!(etag.to_string(), oracle_etag(&list));
}

/// `N` is how many parts were named — not the highest part number, not a constant.
#[test]
fn multipart_etag_suffix_is_the_named_count() {
    let list = [
        (5, digest("five")),
        (9, digest("nine")),
        (MAX_PART_NUMBER, digest("last")),
    ];
    let etag = multipart_etag(&named(&list)).unwrap();
    assert_eq!(etag.parts(), 3);
    assert!(etag.to_string().ends_with("-3"));
    assert_eq!(etag.to_string(), oracle_etag(&list));
}

/// A list out of order, naming a part twice, or naming none is a **typed error** — never an
/// ETag over the sorted or de-duplicated list it contains. The reordered case is one a sort
/// would silently "fix" into the winning assembly.
#[test]
fn multipart_etag_refuses_a_non_ascending_duplicate_or_empty_list() {
    let winning = winning_parts();
    let refused = [
        (
            vec![winning[0], winning[2], winning[1]],
            RecordError::PartsOutOfOrder {
                part_number: 2,
                previous: 4,
            },
        ),
        (
            vec![winning[0], winning[1], (2, digest("part 2 again"))],
            RecordError::DuplicatePart { part_number: 2 },
        ),
        (vec![], RecordError::NoPartsNamed),
    ];
    for (list, expected) in refused {
        assert_eq!(multipart_etag(&named(&list)), Err(expected), "{list:?}");
    }
}

// ===========================================================================
// Leg 3 — `complete_fingerprint` tells an identical retry from a different assembly
// ===========================================================================

#[test]
fn complete_fingerprint_is_the_oracle() {
    let list = winning_parts();
    assert_eq!(
        complete_fingerprint(&named(&list)).unwrap(),
        oracle_fingerprint(&list)
    );
}

#[test]
fn complete_fingerprint_agrees_on_an_identical_retry() {
    let first = complete_fingerprint(&named(&winning_parts())).unwrap();
    let retry = complete_fingerprint(&named(&winning_parts())).unwrap();
    assert_eq!(first, retry);
}

#[test]
fn complete_fingerprint_disagrees_on_one_changed_digest() {
    let mut changed = winning_parts();
    changed[1].1 = digest("part 2, re-uploaded");
    assert_ne!(
        complete_fingerprint(&named(&changed)).unwrap(),
        recorded_fingerprint()
    );
}

/// The same digests in the same order under **different part numbers** (`4` renumbered `3`):
/// the ETag, which hashes the digests alone, cannot tell these apart — the fingerprint must.
#[test]
fn complete_fingerprint_disagrees_on_the_same_digests_under_different_numbers() {
    let winning = winning_parts();
    let renumbered = [winning[0], winning[1], (3, winning[2].1)];
    assert_eq!(
        multipart_etag(&named(&renumbered)).unwrap(),
        multipart_etag(&named(&winning)).unwrap()
    );
    assert_ne!(
        complete_fingerprint(&named(&renumbered)).unwrap(),
        complete_fingerprint(&named(&winning)).unwrap()
    );
}

#[test]
fn complete_fingerprint_disagrees_on_a_strict_subset() {
    let winning = winning_parts();
    assert_ne!(
        complete_fingerprint(&named(&winning[..2])).unwrap(),
        complete_fingerprint(&named(&winning)).unwrap()
    );
}

/// Canonical order **is** the request order and must be ascending: the fingerprint refuses a
/// non-ascending, duplicate or empty list — never fingerprints a sorted copy — with the same
/// typed error `multipart_etag` gives it.
#[test]
fn complete_fingerprint_refuses_what_multipart_etag_refuses() {
    let winning = winning_parts();
    let refused = [
        (
            vec![winning[1], winning[0], winning[2]],
            RecordError::PartsOutOfOrder {
                part_number: 1,
                previous: 2,
            },
        ),
        (
            vec![winning[0], winning[1], winning[1]],
            RecordError::DuplicatePart { part_number: 2 },
        ),
        (vec![], RecordError::NoPartsNamed),
    ];
    for (list, expected) in refused {
        assert_eq!(
            complete_fingerprint(&named(&list)),
            Err(expected.clone()),
            "{list:?}"
        );
        assert_eq!(multipart_etag(&named(&list)), Err(expected), "{list:?}");
    }
}

// ===========================================================================
// Leg 4 — `MultipartEtag` decode validates the grammar and the count's range
// ===========================================================================

fn hex64() -> String {
    hex(digest("any digest").as_bytes())
}

/// Every count in `[1, MAX_PART_NUMBER]` — both ends included — parses, re-renders as the
/// same text, and round-trips through serde as that text.
#[test]
fn multipart_etag_parse_accepts_the_whole_count_range() {
    for count in [1, 2, 10_000, MAX_PART_NUMBER] {
        let text = format!("{}-{count}", hex64());
        let etag = MultipartEtag::parse(&text).expect("a count in range parses");
        assert_eq!(etag.parts(), count);
        assert_eq!(etag.composed(), digest("any digest"));
        assert_eq!(etag.to_string(), text);

        let json = format!("\"{text}\"");
        assert_eq!(metadata::encode(&etag).as_ref(), json.as_bytes());
        assert_eq!(
            metadata::decode::<MultipartEtag>(json.as_bytes()).ok(),
            Some(etag)
        );
    }
}

/// A canonical count outside `[1, MAX_PART_NUMBER]` is out of range, reported as the number
/// it is — including one past `u32::MAX` and one past any integer width, which a parse into
/// a fixed-width integer would otherwise report as malformed.
#[test]
fn multipart_etag_parse_refuses_a_count_outside_the_range() {
    let past_u32 = (u64::from(u32::MAX) + 1).to_string();
    let past_every_width = "9".repeat(40);
    let max_plus_one = (MAX_PART_NUMBER + 1).to_string();
    let counts: [&str; 5] = [
        "0",
        &max_plus_one,
        "4294967295",
        &past_u32,
        &past_every_width,
    ];
    for count in counts {
        let text = format!("{}-{count}", hex64());
        assert_eq!(
            MultipartEtag::parse(&text),
            Err(RecordError::EtagPartCountOutOfRange {
                count: count.to_string()
            }),
            "count {count}"
        );
        let json = format!("\"{text}\"");
        assert!(metadata::decode::<MultipartEtag>(json.as_bytes()).is_err());
    }
}

/// A count that is not canonical decimal, or no `-` at all, is malformed — never read as a
/// number by a lenient parse (`+3`, `03`) and never truncated at a second `-`.
#[test]
fn multipart_etag_parse_refuses_a_malformed_suffix() {
    let hex64 = hex64();
    let mut texts: Vec<String> = ["", "+3", "03", "3x", " 3", "3 ", "-3", "3-3", "\u{0663}"]
        .iter()
        .map(|suffix| format!("{hex64}-{suffix}"))
        .collect();
    texts.push(hex64.clone());
    for text in texts {
        assert_eq!(
            MultipartEtag::parse(&text),
            Err(RecordError::MultipartEtagMalformed { etag: text.clone() }),
            "{text:?}"
        );
    }
}

/// A hex half that is not 64 lowercase-hex characters is not a digest.
#[test]
fn multipart_etag_parse_refuses_a_malformed_digest() {
    let hex64 = hex64();
    let not_digests = [
        format!("A{}", &hex64[1..]),
        hex64[..63].to_string(),
        format!("{hex64}0"),
        format!("g{}", &hex64[1..]),
        String::new(),
    ];
    for hex_half in not_digests {
        assert_eq!(
            MultipartEtag::parse(&format!("{hex_half}-3")),
            Err(RecordError::DigestNotHex {
                digest: hex_half.clone()
            }),
            "{hex_half:?}"
        );
    }
}

// ===========================================================================
// Leg 5 — the outcome enums are exhaustive (no `#[non_exhaustive]`)
// ===========================================================================
//
// Each function below matches every declared variant with no `_` arm. Were any of these
// types `#[non_exhaustive]` — today, or after a later edit — this file, compiled as an
// external crate against `wyrd_core`, would fail with "non-exhaustive patterns" rather than
// compile and pass: exactly the leg-5 contract.

fn exhaust_invalid_part(value: &InvalidPart) -> &'static str {
    match value {
        InvalidPart::Absent => "absent",
        InvalidPart::DigestMismatch => "digest-mismatch",
        InvalidPart::OutOfOrder => "out-of-order",
    }
}

fn exhaust_backpressure(value: &Backpressure) -> &'static str {
    match value {
        Backpressure::SessionCap { .. } => "session-cap",
        Backpressure::InflightParts { .. } => "inflight-parts",
        Backpressure::AdmissionContention { .. } => "admission-contention",
    }
}

fn exhaust_refusal(value: &Refusal) -> &'static str {
    match value {
        Refusal::NoSuchUpload => "no-such-upload",
        Refusal::NoSuchBucket => "no-such-bucket",
        Refusal::OperationAborted => "operation-aborted",
        Refusal::InvalidPart { .. } => "invalid-part",
        Refusal::EntityTooLarge { .. } => "entity-too-large",
        Refusal::SlowDown { .. } => "slow-down",
        Refusal::ProfileSkew { .. } => "profile-skew",
        Refusal::CompleteAttemptsExhausted => "complete-attempts-exhausted",
    }
}

fn exhaust_create_outcome(value: &CreateOutcome) -> &'static str {
    match value {
        CreateOutcome::Created { .. } => "created",
        CreateOutcome::Refused(_) => "refused",
    }
}

fn exhaust_reserve_outcome(value: &ReserveOutcome) -> &'static str {
    match value {
        ReserveOutcome::Reserved { .. } => "reserved",
        ReserveOutcome::Refused(_) => "refused",
    }
}

fn exhaust_upload_part_outcome(value: &UploadPartOutcome) -> &'static str {
    match value {
        UploadPartOutcome::Committed { .. } => "committed",
        UploadPartOutcome::Refused(_) => "refused",
    }
}

fn exhaust_complete_outcome(value: &CompleteOutcome) -> &'static str {
    match value {
        CompleteOutcome::Published(_) => "published",
        CompleteOutcome::AlreadyCompleted(_) => "already-completed",
        CompleteOutcome::Refused(_) => "refused",
    }
}

fn exhaust_abort_outcome(value: &AbortOutcome) -> &'static str {
    match value {
        AbortOutcome::Fenced => "fenced",
        AbortOutcome::AlreadyAborting => "already-aborting",
        AbortOutcome::Refused(_) => "refused",
    }
}

fn exhaust_verb(value: Verb) -> &'static str {
    match value {
        Verb::UploadPart => "upload-part",
        Verb::CompleteMultipartUpload => "complete",
        Verb::AbortMultipartUpload => "abort",
        Verb::ListParts => "list-parts",
        Verb::ListMultipartUploads => "list-uploads",
    }
}

fn exhaust_upload_part_answer(value: &UploadPartAnswer) -> &'static str {
    match value {
        UploadPartAnswer::Accepted => "accepted",
        UploadPartAnswer::Refused(_) => "refused",
    }
}

fn exhaust_complete_answer(value: &CompleteAnswer) -> &'static str {
    match value {
        CompleteAnswer::Fences => "fences",
        CompleteAnswer::AlreadyCompleted(_) => "already-completed",
        CompleteAnswer::Refused(_) => "refused",
    }
}

fn exhaust_abort_answer(value: &AbortAnswer) -> &'static str {
    match value {
        AbortAnswer::Fences => "fences",
        AbortAnswer::AlreadyAborting => "already-aborting",
        AbortAnswer::Refused(_) => "refused",
    }
}

fn exhaust_list_parts_answer(value: &ListPartsAnswer) -> &'static str {
    match value {
        ListPartsAnswer::OpenSet => "open-set",
        ListPartsAnswer::FrozenSet => "frozen-set",
        ListPartsAnswer::Refused(_) => "refused",
    }
}

fn exhaust_list_uploads_answer(value: &ListUploadsAnswer) -> &'static str {
    match value {
        ListUploadsAnswer::Listed => "listed",
        ListUploadsAnswer::NotListed => "not-listed",
    }
}

fn exhaust_answer(value: &Answer) -> &'static str {
    match value {
        Answer::UploadPart(_) => "upload-part",
        Answer::Complete(_) => "complete",
        Answer::Abort(_) => "abort",
        Answer::ListParts(_) => "list-parts",
        Answer::ListUploads(_) => "list-uploads",
    }
}

#[test]
fn outcome_enums_are_exhaustive() {
    let names = [
        exhaust_invalid_part(&InvalidPart::OutOfOrder),
        exhaust_backpressure(&Backpressure::AdmissionContention { attempts: 3 }),
        exhaust_refusal(&Refusal::NoSuchUpload),
        exhaust_create_outcome(&CreateOutcome::Refused(Refusal::NoSuchBucket)),
        exhaust_reserve_outcome(&ReserveOutcome::Refused(Refusal::CompleteAttemptsExhausted)),
        exhaust_upload_part_outcome(&UploadPartOutcome::Committed {
            part_number: part(1),
            digest: digest("committed"),
        }),
        exhaust_complete_outcome(&CompleteOutcome::AlreadyCompleted(recorded_publication())),
        exhaust_abort_outcome(&AbortOutcome::Fenced),
        exhaust_verb(Verb::ListMultipartUploads),
        exhaust_upload_part_answer(&UploadPartAnswer::Accepted),
        exhaust_complete_answer(&CompleteAnswer::Fences),
        exhaust_abort_answer(&AbortAnswer::AlreadyAborting),
        exhaust_list_parts_answer(&ListPartsAnswer::FrozenSet),
        exhaust_list_uploads_answer(&ListUploadsAnswer::NotListed),
        exhaust_answer(&Answer::ListUploads(ListUploadsAnswer::Listed)),
    ];
    let expected = [
        "out-of-order",
        "admission-contention",
        "no-such-upload",
        "refused",
        "refused",
        "committed",
        "already-completed",
        "fenced",
        "list-uploads",
        "accepted",
        "fences",
        "already-aborting",
        "frozen-set",
        "not-listed",
        "list-uploads",
    ];
    assert_eq!(names, expected);
}
