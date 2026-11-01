//! The capability matrix (proposal 0017 §3, §7; #937, first slice): today's profile, the one
//! ladder every exercise of every row is judged by, the `unsupported` and
//! `supported, not checked` rows judged in full, the facts every row shares, version keying,
//! and the run verdict. Pure: every leg calls the production `wyrd_validate::matrix` functions
//! with a fixed input. No network, no second process.
//!
//! A `required` row's own checks are the second slice's
//! (`crates/validate/tests/capability_matrix_required.rs`). Here a `required` row is asserted
//! only where that slice cannot change the answer: a 5xx is `protocol`, no response head is no
//! verdict, an exercise of another row or one that carries no check of its own
//! ([`Exercise::Other`]) is unsettled.

#![forbid(unsafe_code)]

use wyrd_validate::matrix::{
    classify, is_strong_etag, parse_version, select, select_reported, verdict, Answer, Arrived,
    Body, Bound, DeclaredCode, DeleteFailure, DeleteResult, Exercise, Expected, FailureClass,
    Listed, NoProfile, NotCovered, Operation, Outcome, PreId, Profile, Release, ReportedVersion,
    Response, Row, RowResult, Unproved, VersionError, VersionRange, PRE_0_1, PROFILES,
};
use wyrd_validate::ErrorCode;
use FailureClass::{Integrity, Oracle, Protocol, UnexpectedSupport};
use Operation::*;

// --- Fixtures -------------------------------------------------------------------------------

const OBJECT: &[u8] = b"0123456789abcdef";
const ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";
/// A strong entity-tag the object does not have.
const OTHER: &str = "\"0123456789abcdef0123456789abcdef\"";
const NIMPL: DeclaredCode = DeclaredCode::Code("NotImplemented");
const ANY: VersionRange = range(None, None);

const fn range(from: Option<Bound>, below: Option<Bound>) -> VersionRange {
    VersionRange { from, below }
}

fn resp(status: u16) -> Response {
    Response::new(status)
}

fn ans(response: Response) -> Answer {
    Answer::Response(response)
}

fn just(status: u16) -> Answer {
    ans(resp(status))
}

fn body(status: u16, body: Body) -> Answer {
    ans(resp(status).with_body(body))
}

fn none() -> Answer {
    Answer::NoAnswer {
        why: "timed out before a response head".to_string(),
    }
}

fn code(c: &str) -> ErrorCode {
    ErrorCode::Code(c.to_string())
}

/// An error response whose `<Error>` document the client read, naming `c`.
fn refusal(status: u16, c: &str) -> Answer {
    ans(resp(status).with_error_code(code(c)))
}

fn refusal_with(status: u16, c: &str, b: Body) -> Answer {
    ans(resp(status).with_error_code(code(c)).with_body(b))
}

fn row(op: Operation) -> &'static Row {
    PRE_0_1.row(op).expect("pre-0.1 declares the row")
}

fn class_of(result: &RowResult) -> Option<FailureClass> {
    match result {
        RowResult::Fatal { class, .. } => Some(*class),
        _ => None,
    }
}

fn fatal(class: FailureClass, detail: &str) -> RowResult {
    RowResult::Fatal {
        class,
        detail: detail.to_string(),
    }
}

fn is_unsettled(result: &RowResult) -> bool {
    matches!(result, RowResult::Unsettled { .. })
}

fn bytes(b: &[u8]) -> Body {
    Body::Bytes(b.to_vec())
}

fn cut(arrived: Arrived) -> Body {
    Body::Cut {
        arrived,
        why: "connection reset after the head".to_string(),
    }
}

fn cutb(b: &[u8]) -> Body {
    cut(Arrived::Bytes(b.to_vec()))
}

fn cutn(n: u64) -> Body {
    cut(Arrived::Count(n))
}

fn bad() -> Body {
    Body::Unreadable {
        why: "not an XML document".to_string(),
    }
}

fn other(operation: Operation, answer: Answer) -> Exercise {
    Exercise::Other { operation, answer }
}

/// `HeadBucket` answered its declared 400, the client reading no body as its code, with `b`.
fn hb400(b: Body) -> Exercise {
    let r = resp(400).with_error_code(ErrorCode::NoBody).with_body(b);
    other(HeadBucket, ans(r))
}

fn listed(key: &str, size: u64) -> Listed {
    Listed {
        key: key.to_string(),
        size,
    }
}

fn strings(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|k| k.to_string()).collect()
}

/// One exercise of every typed form, each with every field it records filled in and its own
/// request answered `answer`: what #939 maps a run onto, whole.
fn typed(answer: Answer) -> Vec<Exercise> {
    let follow_up = |key: &str| (key.to_string(), just(404));
    let written = vec![listed("p/a", 16), listed("p/b", 3)];
    vec![
        Exercise::PutObject {
            answer: answer.clone(),
            request_body_ended: true,
        },
        Exercise::GetObject {
            object: OBJECT.to_vec(),
            answer: answer.clone(),
        },
        Exercise::GetObjectRanged {
            object: OBJECT.to_vec(),
            first: 2,
            last: 5,
            answer: answer.clone(),
        },
        Exercise::GetObjectIfNoneMatchHit {
            sent: ETAG.to_string(),
            etag: ETAG.to_string(),
            object: OBJECT.to_vec(),
            answer: answer.clone(),
        },
        Exercise::GetObjectIfNoneMatchMiss {
            sent: OTHER.to_string(),
            etag: ETAG.to_string(),
            object: OBJECT.to_vec(),
            answer: answer.clone(),
        },
        Exercise::HeadObject {
            size: 16,
            etag: ETAG.to_string(),
            answer: answer.clone(),
        },
        Exercise::DeleteObject {
            key: "p/a".to_string(),
            answer: answer.clone(),
            follow_ups: vec![follow_up("p/a")],
        },
        Exercise::DeleteObjects {
            keys: strings(&["p/a", "p/b"]),
            answer: answer.clone(),
            follow_ups: vec![follow_up("p/a"), follow_up("p/b")],
        },
        Exercise::ListObjectsV2 {
            expected: written.clone(),
            answer: answer.clone(),
        },
        Exercise::ListObjects {
            expected: written,
            answer,
        },
    ]
}

/// Each `unsupported` row of `profile` answered exactly as declared.
fn declared_refusals(profile: &Profile) -> Vec<Exercise> {
    let mut run = Vec::new();
    for r in profile.rows() {
        if let Expected::Unsupported { status, code, .. } = r.expected {
            let response = match code {
                DeclaredCode::Code(c) => resp(status).with_error_code(ErrorCode::Code(c.into())),
                DeclaredCode::NoBody => resp(status).with_error_code(ErrorCode::NoBody),
            };
            run.push(other(r.operation, ans(response)));
        }
    }
    run
}

/// Against `pre-0.1`, `exercise` alone is fatal with `class`, and nothing else is.
fn fails(exercise: Exercise, class: FailureClass) {
    let op = exercise.operation();
    let v = verdict(&PRE_0_1, &[exercise]);
    assert_eq!(v.outcome, Outcome::Fail, "{v:?}");
    let fatal: Vec<_> = v.fatal.iter().map(|f| (f.operation, f.class)).collect();
    assert_eq!(fatal, [(op, class)], "{v:?}");
    assert!(!v.proved.contains(&op), "{v:?}");
}

/// Against `pre-0.1`, `exercise` leaves its row unproved for `why`, with no fatal result.
fn unproved_as(exercise: Exercise, why: fn(&Unproved) -> bool) {
    let op = exercise.operation();
    let v = verdict(&PRE_0_1, &[exercise]);
    assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
    assert!(v.fatal.is_empty(), "{v:?}");
    let reason = v.unproved.iter().find(|(o, _)| *o == op);
    assert!(reason.is_some_and(|(_, u)| why(u)), "{op:?}: {v:?}");
    assert!(!v.proved.contains(&op), "{v:?}");
}

fn unproved(exercise: Exercise) {
    unproved_as(exercise, |u| matches!(u, Unproved::Unsettled { .. }));
}

fn no_answer(exercise: Exercise) {
    unproved_as(exercise, |u| matches!(u, Unproved::NoAnswer { .. }));
}

/// Against `pre-0.1`, `exercise` alone passes and proves its row.
fn proves(exercise: Exercise) {
    let op = exercise.operation();
    assert_eq!(classify(row(op), &exercise), RowResult::Pass, "{op:?}");
    let v = verdict(&PRE_0_1, &[exercise]);
    assert!(v.fatal.is_empty(), "{v:?}");
    assert_eq!(v.proved, [op], "{v:?}");
}

// Test-local rows and profiles.

const CHECKED_ROWS: &[Row] = &[
    Row::unsupported(CopyObject, 766, 501, NIMPL, "test"),
    Row::unsupported(HeadBucket, 511, 400, DeclaredCode::NoBody, "test"),
    Row::not_checked(UploadPart, 750, "test"),
];
/// `unsupported` rows and one `supported, not checked` row: a profile a run can PASS against
/// before any `required` row has its checks.
const CHECKED: Profile = Profile::new("checked", ANY, CHECKED_ROWS);

const WITH_REQUIRED_ROWS: &[Row] = &[
    Row::unsupported(CopyObject, 766, 501, NIMPL, "test"),
    Row::unsupported(HeadBucket, 511, 400, DeclaredCode::NoBody, "test"),
    Row::required(PutObject, "test"),
    Row::not_checked(UploadPart, 750, "test"),
];
const WITH_REQUIRED: Profile = Profile::new("with-required", ANY, WITH_REQUIRED_ROWS);

fn not_checked_row() -> &'static Row {
    CHECKED.row(UploadPart).expect("declared")
}

// --- (A) Today's profile --------------------------------------------------------------------

#[test]
fn pre_0_1_declares_exactly_todays_rows() {
    let unsupported = |issue, status, code| Expected::Unsupported {
        issue,
        status,
        code,
    };
    let (r, n508) = (Expected::Required, unsupported(508, 501, NIMPL));
    let invalid = unsupported(511, 400, DeclaredCode::Code("InvalidRequest"));
    let mut want: Vec<(Operation, Expected)> = vec![
        (PutObject, r),
        (GetObject, r),
        (GetObjectRanged, r),
        (GetObjectIfNoneMatchHit, r),
        (GetObjectIfNoneMatchMiss, r),
        (HeadObject, r),
        (DeleteObject, r),
        (DeleteObjects, r),
        (ListObjectsV2, r),
        (ListObjects, r),
        (CopyObject, unsupported(766, 501, NIMPL)),
        (CreateMultipartUpload, n508),
        (UploadPart, n508),
        (CompleteMultipartUpload, n508),
        (AbortMultipartUpload, n508),
        (ListParts, n508),
        (ListMultipartUploads, n508),
        (GetBucketLocation, unsupported(511, 501, NIMPL)),
        (CreateBucket, invalid),
        (DeleteBucket, invalid),
        (ListBuckets, invalid),
        (HeadBucket, unsupported(511, 400, DeclaredCode::NoBody)),
    ];
    // Exactly these rows, in any order: none `supported, not checked` today.
    let rows = PRE_0_1.rows();
    let mut got: Vec<_> = rows.iter().map(|r| (r.operation, r.expected)).collect();
    got.sort_by_key(|(op, _)| *op);
    want.sort_by_key(|(op, _)| *op);
    assert_eq!(got, want, "pre-0.1 declares exactly today's rows");
    assert_eq!(PRE_0_1.name(), "pre-0.1");
    // Each row cites the gateway line that gives today's answer.
    for r in rows {
        assert!(
            r.cites.starts_with("crates/gateway-s3/src/lib.rs:"),
            "{r:?}"
        );
    }
    let refusals = [
        (CopyObject, ":1865-1871"),
        (CreateMultipartUpload, ":1842-1849"),
        (UploadPart, ":1842-1849"),
        (CompleteMultipartUpload, ":1842-1849"),
        (AbortMultipartUpload, ":1842-1849"),
        (ListParts, ":1842-1849"),
        (ListMultipartUploads, ":1797-1804"),
        (GetBucketLocation, ":1797-1804"),
        (CreateBucket, ":1809-1815"),
        (DeleteBucket, ":1809-1815"),
        (HeadBucket, ":1809-1815"),
        (ListBuckets, ":1819-1826"),
        (DeleteObject, ":2026-2028"),
        (DeleteObjects, ":1766-1788"),
    ];
    for (op, lines) in refusals {
        assert!(row(op).cites.contains(lines), "{op:?}: {}", row(op).cites);
    }
}

fn selected(table: &[Profile], v: &str) -> Result<&'static str, NoProfile> {
    select_reported(table, v).map(Profile::name)
}

#[test]
fn pre_0_1_covers_every_version_below_0_1_0() {
    // Untagged builds, a release candidate, and a build past one.
    let below = "0.0.0 0.0.0+git.abc1234 0.0.0+git.abc1234.dirty 0.0.9 0.0.99-alpha \
                 0.1.0-rc.1 0.1.0-rc.1+git.3.abc1234 0.1.0-rc.1+git.3.abc1234.dirty";
    for v in below.split_whitespace() {
        assert_eq!(selected(PROFILES, v), Ok("pre-0.1"), "{v}");
    }
    for v in "0.1.0 0.1.0+git.dirty 0.1.0+git.3.abc1234 0.2.0 1.0.0".split(' ') {
        let no_match = matches!(select_reported(PROFILES, v), Err(NoProfile::NoMatch(_)));
        assert!(no_match, "{v} is not covered by pre-0.1");
    }
}

#[test]
#[should_panic(expected = "declares an operation twice")]
fn a_profile_cannot_declare_an_operation_twice() {
    const TWICE: &[Row] = &[
        Row::required(PutObject, "test"),
        Row::unsupported(PutObject, 1, 501, NIMPL, "test"),
    ];
    let _ = Profile::new("twice", ANY, TWICE);
}

// --- (B) The ladder -------------------------------------------------------------------------

#[test]
fn an_exercise_of_another_row_is_unsettled_on_every_row_state() {
    // Step 1 comes before every fact: even a 5xx or a 2xx shows nothing about another row.
    for r in [
        row(PutObject),
        row(CopyObject),
        row(HeadBucket),
        not_checked_row(),
    ] {
        for answer in [just(200), just(503), refusal(501, "NotImplemented"), none()] {
            let ex = other(ListBuckets, answer);
            assert!(is_unsettled(&classify(r, &ex)), "{r:?}: {ex:?}");
        }
    }
    assert_eq!(
        classify(row(CopyObject), &other(UploadPart, just(200))),
        RowResult::Unsettled {
            why: "an exercise of UploadPart cannot settle the CopyObject row".to_string()
        }
    );
}

#[test]
fn no_response_head_is_no_verdict_on_every_row_state() {
    let no_head = |r: &Row| classify(r, &other(r.operation, none()));
    for r in [
        row(PutObject),
        row(CopyObject),
        row(HeadBucket),
        not_checked_row(),
    ] {
        let result = no_head(r);
        assert!(
            matches!(result, RowResult::NoAnswer { .. }),
            "{r:?}: {result:?}"
        );
    }
    // The follow-ups of a request that got no head are not judged, whatever they say.
    for answer in [just(200), just(503)] {
        let ex = Exercise::DeleteObject {
            key: "p/a".to_string(),
            answer: none(),
            follow_ups: vec![("p/a".to_string(), answer)],
        };
        let result = classify(row(DeleteObject), &ex);
        assert!(matches!(result, RowResult::NoAnswer { .. }), "{result:?}");
        no_answer(ex);
    }
    no_answer(other(CopyObject, none()));
    no_answer(other(PutObject, none()));
}

#[test]
fn the_first_failing_check_in_message_order_names_the_class() {
    let copy = row(CopyObject);
    let hb = row(HeadBucket);
    // The status comes before the body: a 2xx is support whatever the body was.
    assert_eq!(
        classify(copy, &other(CopyObject, body(200, bad()))),
        fatal(
            UnexpectedSupport,
            "answered 200; declared 501 NotImplemented"
        )
    );
    assert_eq!(
        classify(hb, &other(HeadBucket, body(200, bytes(b"xy")))),
        fatal(UnexpectedSupport, "answered 200; declared 400 no body")
    );
    // A wrong status is named, not the wrong body after it.
    assert_eq!(
        classify(
            copy,
            &other(CopyObject, refusal_with(400, "InvalidRequest", bad()))
        ),
        fatal(Protocol, "answered 400; declared 501 NotImplemented")
    );
    // A 5xx the row does not declare: the shared fact is the first check of the status.
    assert_eq!(
        classify(hb, &other(HeadBucket, body(503, bytes(b"x")))),
        fatal(
            Protocol,
            "answered 503, a 5xx the row does not declare; the row is unsupported(#511), \
             refused 400 no body"
        )
    );
    // Within the body, the shape before the code.
    assert_eq!(
        classify(copy, &other(CopyObject, refusal_with(501, "Nope", bad()))),
        fatal(
            Protocol,
            "501 error body is not an <Error> document: not an XML document"
        )
    );
    assert_eq!(
        classify(hb, &hb400(bytes(b"xy"))),
        fatal(Protocol, "answered 400 with a body of 2 bytes")
    );
}

#[test]
fn nothing_that_leaves_an_exercise_unproved_stops_a_later_check() {
    let copy = row(CopyObject);
    // A cut after the head never hides the status that arrived before it.
    for (answer, class) in [
        (body(200, cutn(3)), UnexpectedSupport),
        (body(200, cutb(b"<Error>")), UnexpectedSupport),
        (body(400, cutn(7)), Protocol),
        (body(503, cutn(0)), Protocol),
    ] {
        let ex = other(CopyObject, answer);
        assert_eq!(class_of(&classify(copy, &ex)), Some(class), "{ex:?}");
        fails(ex, class);
    }
    // Nor a code that was read before it: the code check still runs after the cut.
    let wrong_then_cut = refusal_with(501, "InvalidRequest", cutn(40));
    assert_eq!(
        classify(copy, &other(CopyObject, wrong_then_cut.clone())),
        fatal(
            Protocol,
            "answered 501 with code InvalidRequest; declared 501 NotImplemented"
        )
    );
    fails(other(CopyObject, wrong_then_cut), Protocol);
    // Nor, on a row declaring no body, a byte that arrived or a code that was read.
    fails(hb400(cutn(1)), Protocol);
    fails(hb400(cutb(b"x")), Protocol);
    let code_then_cut = resp(400).with_error_code(code("InvalidRequest"));
    fails(
        other(HeadBucket, ans(code_then_cut.with_body(cutn(0)))),
        Protocol,
    );
    // With nothing wrong before the cut, it is unsettled.
    unproved(other(
        CopyObject,
        refusal_with(501, "NotImplemented", cutn(40)),
    ));
    unproved(other(CopyObject, body(501, cutn(7))));
    unproved(hb400(cutn(0)));
}

// --- (C) The classes ------------------------------------------------------------------------

#[test]
fn the_fatal_classes_print_as_0017_names_them() {
    let printed: Vec<String> = [Integrity, Oracle, UnexpectedSupport, Protocol]
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        printed,
        ["integrity", "oracle", "unexpected-support", "protocol"]
    );
}

#[test]
fn a_5xx_is_protocol_on_every_row_unless_the_row_declares_that_status() {
    // A `required` row, every typed exercise and one that carries no check of its own.
    for status in [500, 501, 502, 503, 599] {
        for ex in typed(just(status)) {
            fails(ex, Protocol);
        }
        fails(other(PutObject, just(status)), Protocol);
    }
    // An `unsupported` row: a 5xx it does not declare, with any code or none.
    let copy = row(CopyObject);
    for answer in [
        refusal(500, "InternalError"),
        refusal(503, "SlowDown"),
        just(502),
    ] {
        fails(other(CopyObject, answer), Protocol);
    }
    assert_eq!(
        classify(copy, &other(CopyObject, refusal(500, "InternalError"))),
        fatal(
            Protocol,
            "answered 500, a 5xx the row does not declare; the row is unsupported(#766), \
             refused 501 NotImplemented"
        )
    );
    fails(other(HeadBucket, just(500)), Protocol);
    // The 5xx it declares is its answer.
    proves(other(CopyObject, refusal(501, "NotImplemented")));
    const SLOW: Row = Row::unsupported(
        GetBucketLocation,
        1,
        503,
        DeclaredCode::Code("SlowDown"),
        "test",
    );
    let slow = other(GetBucketLocation, refusal(503, "SlowDown"));
    assert_eq!(classify(&SLOW, &slow), RowResult::Pass);
    // A `supported, not checked` row declares no status at all.
    let unchecked = not_checked_row();
    for status in [500, 501, 503] {
        let result = classify(
            unchecked,
            &other(UploadPart, refusal(status, "NotImplemented")),
        );
        assert_eq!(class_of(&result), Some(Protocol), "{status}");
    }
    assert_eq!(
        classify(unchecked, &other(UploadPart, just(501))),
        fatal(
            Protocol,
            "answered 501, a 5xx the row does not declare; the row is supported, not checked \
             (#750)"
        )
    );
}

#[test]
fn a_required_rows_exercise_with_a_head_and_no_check_of_its_own_is_unsettled() {
    // Not a 5xx: unproved, never passed, whatever came back.
    for status in [200, 204, 206, 304, 400, 404] {
        let ex = other(PutObject, just(status));
        assert!(is_unsettled(&classify(row(PutObject), &ex)), "{status}");
        unproved(ex);
    }
    unproved(other(DeleteObjects, body(200, cutn(9))));
}

// --- (D) Unsupported rows -------------------------------------------------------------------

#[test]
fn unsupported_row_passes_only_on_the_declared_status_and_code() {
    let copy = row(CopyObject);
    let ex = |answer| other(CopyObject, answer);
    proves(ex(refusal(501, "NotImplemented")));
    proves(ex(refusal_with(501, "NotImplemented", bytes(b"<Error/>"))));
    for wrong in [
        refusal(501, "InvalidRequest"),
        refusal(501, "notimplemented"),
        refusal(400, "NotImplemented"),
        refusal(404, "NoSuchKey"),
        just(501),
        ans(resp(501).with_error_code(ErrorCode::NoBody)),
        ans(resp(501).with_error_code(ErrorCode::MissingInXml)),
    ] {
        assert_eq!(
            class_of(&classify(copy, &ex(wrong.clone()))),
            Some(Protocol),
            "{wrong:?}"
        );
        fails(ex(wrong), Protocol);
    }
    assert_eq!(
        classify(copy, &ex(refusal(501, "InvalidRequest"))),
        fatal(
            Protocol,
            "answered 501 with code InvalidRequest; declared 501 NotImplemented"
        )
    );
    assert_eq!(
        classify(copy, &ex(just(501))),
        fatal(
            Protocol,
            "answered 501 with no error code; declared 501 NotImplemented"
        )
    );
    // Every shipped refusal passes only on its own code.
    for r in PRE_0_1.rows() {
        if let Expected::Unsupported {
            status,
            code: DeclaredCode::Code(c),
            ..
        } = r.expected
        {
            proves(other(r.operation, refusal(status, c)));
            fails(
                other(r.operation, refusal(status, "AccessDenied")),
                Protocol,
            );
        }
    }
}

#[test]
fn head_bucket_passes_on_a_400_with_no_body_only() {
    let hb = row(HeadBucket);
    let ex = |answer| other(HeadBucket, answer);
    proves(hb400(Body::Empty));
    proves(hb400(bytes(b"")));
    proves(ex(just(400)));
    assert_eq!(
        classify(hb, &ex(refusal(400, "InvalidRequest"))),
        fatal(
            Protocol,
            "answered 400 with code InvalidRequest; declared 400 no body"
        )
    );
    fails(
        ex(ans(resp(400).with_error_code(ErrorCode::MissingInXml))),
        Protocol,
    );
    fails(
        ex(ans(resp(404).with_error_code(ErrorCode::NoBody))),
        Protocol,
    );
    // The declared 400 with a body, whole or cut off after a byte, in both arrival forms.
    for b in [
        bytes(b"x"),
        cutb(b"x"),
        cutn(1),
        bad(),
        Body::Listing(vec![]),
    ] {
        assert_eq!(class_of(&classify(hb, &hb400(b.clone()))), Some(Protocol));
        fails(hb400(b), Protocol);
    }
    // A cut after no byte settles nothing, whatever code the client read or did not.
    for b in [cutb(b""), cutn(0)] {
        assert!(is_unsettled(&classify(hb, &hb400(b.clone()))), "{b:?}");
        unproved(hb400(b.clone()));
        unproved(ex(body(400, b)));
    }
}

#[test]
fn any_2xx_to_an_unsupported_row_is_unexpected_support() {
    for op in [CopyObject, UploadPart, ListBuckets, HeadBucket] {
        // A 2xx head, whatever the body did after: nothing, cut, unreadable, a clean <Error>.
        for answer in [
            just(200),
            just(204),
            just(206),
            just(299),
            body(200, cutn(0)),
            body(200, cutb(b"<xml")),
            body(200, bad()),
            body(200, bytes(OBJECT)),
            refusal(200, "NotImplemented"),
            refusal_with(200, "NotImplemented", cutn(5)),
        ] {
            let result = classify(row(op), &other(op, answer.clone()));
            assert_eq!(
                class_of(&result),
                Some(UnexpectedSupport),
                "{op:?}: {answer:?}"
            );
        }
    }
    fails(other(CompleteMultipartUpload, just(200)), UnexpectedSupport);
    fails(other(CopyObject, body(200, cutn(3))), UnexpectedSupport);
    // A typed exercise of a row a profile declares `unsupported` is judged the same way.
    const PUT_UNSUPPORTED: Row = Row::unsupported(PutObject, 1, 501, NIMPL, "test");
    let put = Exercise::PutObject {
        answer: ans(resp(200).with_etag(ETAG)),
        request_body_ended: true,
    };
    assert_eq!(
        class_of(&classify(&PUT_UNSUPPORTED, &put)),
        Some(UnexpectedSupport)
    );
}

#[test]
fn unsupported_row_with_its_error_body_cut_off_is_unsettled() {
    // Cut off before its code was read, after some or no bytes, on the declared status.
    for n in [7, 0] {
        let result = classify(row(CopyObject), &other(CopyObject, body(501, cutn(n))));
        assert_eq!(
            result,
            RowResult::Unsettled {
                why: "501 error body cut off before its code could be read: connection reset \
                      after the head"
                    .to_string()
            }
        );
        unproved(other(CopyObject, body(501, cutn(n))));
        unproved(other(CreateBucket, body(400, cutb(b"<Err"))));
    }
}

#[test]
fn unsupported_row_with_an_unreadable_error_body_is_protocol() {
    // Arrived whole and not an <Error> document: an answer, not a cut. So is another document.
    assert_eq!(
        classify(row(CopyObject), &other(CopyObject, body(501, bad()))),
        fatal(
            Protocol,
            "501 error body is not an <Error> document: not an XML document"
        )
    );
    fails(other(CopyObject, body(501, bad())), Protocol);
    let listing = Body::Listing(vec![listed("p/a", 1)]);
    fails(
        other(CopyObject, refusal_with(501, "NotImplemented", listing)),
        Protocol,
    );
    let deleted = Body::DeleteResult(DeleteResult::default());
    fails(
        other(CopyObject, refusal_with(501, "NotImplemented", deleted)),
        Protocol,
    );
}

#[test]
fn an_unsupported_row_reads_no_header() {
    // Not read: any header, nor `Content-Length` on an error response.
    let odd = |status| {
        resp(status)
            .with_etag("not a tag")
            .with_content_range("items */0")
            .with_content_length(999)
    };
    proves(other(
        CopyObject,
        ans(odd(501).with_error_code(code("NotImplemented"))),
    ));
    proves(other(
        HeadBucket,
        ans(odd(400).with_error_code(ErrorCode::NoBody)),
    ));
}

#[test]
fn a_not_checked_row_is_never_a_pass() {
    let unchecked = not_checked_row();
    // Anything but a 5xx is "not checked", whatever it was.
    for answer in [
        just(200),
        refusal(400, "InvalidRequest"),
        body(200, cutn(4)),
        body(200, bad()),
        just(404),
    ] {
        let result = classify(unchecked, &other(UploadPart, answer.clone()));
        assert_eq!(result, RowResult::NotChecked { issue: 750 }, "{answer:?}");
    }
    let no_head = classify(unchecked, &other(UploadPart, none()));
    assert!(matches!(no_head, RowResult::NoAnswer { .. }), "{no_head:?}");
}

// --- (E) Version keying ---------------------------------------------------------------------

fn release(major: u64, minor: u64, patch: u64, pre: Vec<PreId>) -> ReportedVersion {
    ReportedVersion::Release(Release {
        major,
        minor,
        patch,
        pre,
    })
}

fn rel(major: u64, minor: u64, patch: u64) -> Result<ReportedVersion, VersionError> {
    Ok(release(major, minor, patch, vec![]))
}

#[test]
fn parse_reads_releases_and_ignores_build_metadata() {
    assert_eq!(parse_version("0.1.0"), rel(0, 1, 0));
    assert_eq!(parse_version("0.1.0+git.dirty"), rel(0, 1, 0));
    assert_eq!(parse_version("0.1.0+git.3.abc1234.dirty"), rel(0, 1, 0));
    assert_eq!(parse_version("0.0.0+git.abc1234"), rel(0, 0, 0));
    let rc1 = || vec![PreId::Alpha("rc".to_string()), PreId::Numeric(1)];
    assert_eq!(
        parse_version("1.2.3-rc.1+build.5"),
        Ok(release(1, 2, 3, rc1()))
    );
    assert_eq!(
        parse_version("0.1.0-rc.1+git.3.abc1234"),
        Ok(release(0, 1, 0, rc1()))
    );
    assert_eq!(parse_version("10.20.30"), rel(10, 20, 30));
    assert_eq!(
        parse_version("18446744073709551615.0.0"),
        rel(u64::MAX, 0, 0)
    );
}

#[test]
fn unknown_is_never_read_as_release_0_0_0() {
    for v in [
        "unknown",
        "0.0.0+git.unknown",
        "0.0.0+git.unknown.dirty",
        "0.1.0+git.unknown",
    ] {
        assert_eq!(parse_version(v), Ok(ReportedVersion::Unknown), "{v}");
        assert_eq!(select_reported(PROFILES, v), Err(NoProfile::Unknown), "{v}");
    }
    // A sha that merely contains `unknown` is a real build.
    assert_eq!(parse_version("0.0.0+git.unknownx"), rel(0, 0, 0));
}

#[test]
fn parse_refuses_malformed_versions() {
    // Space-separated, then the three that are empty or carry whitespace.
    let malformed = "wyrd/0.1.0 v0.1.0 0.1 0.1.0.0 0..0 .0.0 0.0. 01.1.0 0.01.0 +1.0.0 0.-1.0 \
                     18446744073709551616.0.0 0.1.0-rc.18446744073709551616 0.1.0- 0.1.0-rc..1 \
                     0.1.0-01 0.0.0-r_c 0.1.0+ 0.1.0+git..x 0.1.0+git_x Unknown";
    for v in malformed.split(' ').chain(["", " 0.1.0", "0.1.0 "]) {
        assert!(parse_version(v).is_err(), "{v:?} must not parse");
        let unparsable = matches!(select_reported(PROFILES, v), Err(NoProfile::Unparsable(_)));
        assert!(unparsable, "{v:?}");
    }
}

const OLD_ROWS: &[Row] = &[Row::unsupported(CopyObject, 766, 501, NIMPL, "test")];
const NEW_ROWS: &[Row] = &[Row::required(CopyObject, "test")];
const OLD: Profile = Profile::new("old", range(None, Some(Bound::new(1, 0, 0))), OLD_ROWS);
const NEW: Profile = Profile::new(
    "new",
    range(Some(Bound::new(1, 0, 0)), Some(Bound::new(2, 0, 0))),
    NEW_ROWS,
);
const TABLE: &[Profile] = &[OLD, NEW];

#[test]
fn each_version_selects_its_own_profile_and_the_rows_flip_with_it() {
    let old = select_reported(TABLE, "0.9.3+git.2.abc").expect("0.9.3 selects");
    let new = select_reported(TABLE, "1.4.0").expect("1.4.0 selects");
    assert_eq!(old.name(), "old");
    assert_eq!(new.name(), "new");
    assert_eq!(selected(TABLE, "1.0.0-rc.1"), Ok("old"));
    assert_eq!(selected(TABLE, "1.0.0"), Ok("new"));
    // The same answer, the declared refusal: a pass where the row is `unsupported`, and a 5xx
    // the row does not declare where it is `required`.
    let same = other(CopyObject, refusal(501, "NotImplemented"));
    let copy_row = |p: &Profile| p.row(CopyObject).expect("declared");
    assert_eq!(classify(copy_row(old), &same), RowResult::Pass);
    assert_eq!(class_of(&classify(copy_row(new), &same)), Some(Protocol));
    let run = [same];
    let (passed, failed) = (verdict(old, &run), verdict(new, &run));
    assert_eq!(passed.outcome, Outcome::Pass, "{passed:?}");
    assert_eq!(passed.proved, [CopyObject]);
    assert_eq!(failed.outcome, Outcome::Fail, "{failed:?}");
    assert_eq!(
        (failed.fatal[0].operation, failed.fatal[0].class),
        (CopyObject, Protocol)
    );
}

#[test]
fn a_version_no_profile_covers_selects_none_and_says_why() {
    let no_match = matches!(selected(TABLE, "2.0.0"), Err(NoProfile::NoMatch(_)));
    let unparsable = matches!(selected(TABLE, "two"), Err(NoProfile::Unparsable(_)));
    assert!(no_match, "2.0.0");
    assert!(unparsable, "two");
    assert_eq!(selected(TABLE, "unknown"), Err(NoProfile::Unknown));
    assert_eq!(
        selected(TABLE, "0.0.0+git.unknown"),
        Err(NoProfile::Unknown)
    );
    assert_eq!(
        select(TABLE, &ReportedVersion::Unknown),
        Err(NoProfile::Unknown)
    );
    let why = |v: &str| select_reported(TABLE, v).unwrap_err().to_string();
    assert_eq!(why("unknown"), "the server's version is unknown");
    assert_eq!(
        why("2.1.0-rc.1.x+git.4.abc"),
        "no profile covers release 2.1.0-rc.1.x"
    );
    assert_eq!(
        why("0.1"),
        "cannot read version \"0.1\": expected MAJOR.MINOR.PATCH"
    );
    // Two profiles covering one release: neither is picked.
    const OVERLAP: Profile =
        Profile::new("overlap", range(Some(Bound::new(0, 5, 0)), None), NEW_ROWS);
    const BOTH: &[Profile] = &[OLD, OVERLAP];
    let why = select_reported(BOTH, "0.6.0").unwrap_err();
    assert!(matches!(why, NoProfile::Ambiguous(_)), "{why:?}");
    assert_eq!(
        why.to_string(),
        "more than one profile covers release 0.6.0"
    );
}

// --- The observation type -------------------------------------------------------------------

#[test]
fn the_observation_type_records_every_answer_and_ask() {
    // Every typed form names its own operation and gives back its own request's answer.
    let head = ans(resp(200)
        .with_etag(ETAG)
        .with_content_range("bytes 2-5/16")
        .with_content_length(4)
        .with_body(bytes(&OBJECT[2..=5])));
    let ops: Vec<Operation> = typed(head.clone())
        .iter()
        .map(|e| {
            assert_eq!(e.answer(), &head, "{e:?}");
            e.operation()
        })
        .collect();
    assert_eq!(
        ops,
        [
            PutObject,
            GetObject,
            GetObjectRanged,
            GetObjectIfNoneMatchHit,
            GetObjectIfNoneMatchMiss,
            HeadObject,
            DeleteObject,
            DeleteObjects,
            ListObjectsV2,
            ListObjects,
        ]
    );
    // Every body form, on every row state: none of them passes a `required` or a
    // `supported, not checked` row.
    let failure = DeleteFailure {
        key: "p/b".to_string(),
        code: "InternalError".to_string(),
    };
    let documents = [
        Body::Empty,
        bytes(OBJECT),
        Body::Listing(vec![listed("p/a", 16)]),
        Body::DeleteResult(DeleteResult {
            deleted: strings(&["p/a"]),
            errors: vec![failure],
        }),
        bad(),
        cutb(&OBJECT[..3]),
        cutn(3),
    ];
    for b in documents {
        let r = ans(resp(200).with_etag(ETAG).with_body(b));
        assert_ne!(
            classify(not_checked_row(), &other(UploadPart, r.clone())),
            RowResult::Pass
        );
        assert!(is_unsettled(&classify(
            row(PutObject),
            &other(PutObject, r)
        )));
    }
}

#[test]
fn strong_entity_tag_grammar() {
    for ok in ["\"abc\"", "\"\"", "\"a-b_c.1!#~\"", ETAG, OTHER] {
        assert!(is_strong_etag(ok), "{ok}");
    }
    // `*`, unquoted, weak, empty, unbalanced or inner quotes, a space outside or inside.
    let bad = r#"*;abc;W/"abc";;";"a"b";"abc;abc"; "abc";"a b""#;
    for bad in bad.split(';') {
        assert!(!is_strong_etag(bad), "{bad:?}");
    }
}

// --- (H) The run verdict --------------------------------------------------------------------

#[test]
fn a_run_proving_every_checked_row_passes_and_lists_the_not_checked_row() {
    let run = declared_refusals(&CHECKED);
    let v = verdict(&CHECKED, &run);
    assert_eq!(v.outcome, Outcome::Pass, "{v:?}");
    assert_eq!(v.proved, [CopyObject, HeadBucket]);
    assert!(v.fatal.is_empty() && v.unproved.is_empty(), "{v:?}");
    let not_covered = [NotCovered {
        operation: UploadPart,
        issue: 750,
    }];
    assert_eq!(v.not_covered, not_covered);
    // Answering the not-checked row, or failing to get an answer to it, changes nothing: it is
    // neither proved nor needed, and stays not covered.
    for answer in [just(200), refusal(400, "InvalidRequest"), none()] {
        let mut with = run.clone();
        with.push(other(UploadPart, answer.clone()));
        let v = verdict(&CHECKED, &with);
        assert_eq!(v.outcome, Outcome::Pass, "{answer:?}: {v:?}");
        assert!(!v.proved.contains(&UploadPart), "{v:?}");
        assert_eq!(v.not_covered, not_covered);
    }
    // But a fatal answer to it is fatal.
    let mut with = run;
    with.push(other(UploadPart, just(503)));
    let v = verdict(&CHECKED, &with);
    assert_eq!(v.outcome, Outcome::Fail, "{v:?}");
    assert_eq!(
        (v.fatal[0].operation, v.fatal[0].class),
        (UploadPart, Protocol)
    );
}

#[test]
fn any_fatal_fails_the_run_naming_every_fatal_row() {
    let mut run = declared_refusals(&PRE_0_1);
    run.retain(|e| !matches!(e.operation(), CopyObject | HeadBucket));
    run.push(other(CopyObject, body(200, cutn(3))));
    run.push(other(HeadBucket, hb400_answer(bytes(b"x"))));
    // A required row's 5xx, and an unproved row, which does not soften a fatal one.
    run.push(other(PutObject, just(503)));
    run.push(other(ListObjects, none()));
    let v = verdict(&PRE_0_1, &run);
    assert_eq!(v.outcome, Outcome::Fail, "{v:?}");
    let fatal: Vec<_> = v.fatal.iter().map(|f| (f.operation, f.class)).collect();
    assert_eq!(
        fatal,
        [
            (CopyObject, UnexpectedSupport),
            (HeadBucket, Protocol),
            (PutObject, Protocol)
        ]
    );
    for op in [CopyObject, HeadBucket, PutObject, ListObjects] {
        assert!(!v.proved.contains(&op), "{op:?}: {v:?}");
    }
    let listed = v.unproved.iter().find(|(op, _)| *op == ListObjects);
    assert!(
        matches!(listed, Some((_, Unproved::NoAnswer { .. }))),
        "{v:?}"
    );
}

fn hb400_answer(b: Body) -> Answer {
    ans(resp(400).with_error_code(ErrorCode::NoBody).with_body(b))
}

#[test]
fn an_unproved_required_row_keeps_the_run_inconclusive() {
    let refusals = declared_refusals(&WITH_REQUIRED);
    // Every other row proved; the `required` row exercised with a head and no check of its
    // own, with no head, or not at all.
    for (put, why) in [
        (
            Some(other(PutObject, ans(resp(200).with_etag(ETAG)))),
            (|u| matches!(u, Unproved::Unsettled { .. })) as fn(&Unproved) -> bool,
        ),
        (Some(other(PutObject, none())), |u| {
            matches!(u, Unproved::NoAnswer { .. })
        }),
        (
            Some(Exercise::PutObject {
                answer: none(),
                request_body_ended: true,
            }),
            |u| matches!(u, Unproved::NoAnswer { .. }),
        ),
        (None, |u| matches!(u, Unproved::NotExercised)),
    ] {
        let mut run = refusals.clone();
        run.extend(put);
        let v = verdict(&WITH_REQUIRED, &run);
        assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
        assert!(v.fatal.is_empty(), "{v:?}");
        assert_eq!(v.proved, [CopyObject, HeadBucket], "{v:?}");
        assert_eq!(v.unproved.len(), 1, "{v:?}");
        assert_eq!(v.unproved[0].0, PutObject, "{v:?}");
        assert!(why(&v.unproved[0].1), "{v:?}");
    }
}

#[test]
fn an_unexercised_undeclared_or_unsettled_row_is_inconclusive() {
    let run = declared_refusals(&CHECKED);
    // An `unsupported` row not exercised.
    let mut missing = run.clone();
    missing.retain(|e| e.operation() != HeadBucket);
    let v = verdict(&CHECKED, &missing);
    assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
    assert_eq!(v.unproved, [(HeadBucket, Unproved::NotExercised)]);
    // An exercise of an operation the profile has no row for.
    let mut undeclared = run.clone();
    undeclared.push(other(ListBuckets, refusal(400, "InvalidRequest")));
    let v = verdict(&CHECKED, &undeclared);
    assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
    assert_eq!(v.unproved, [(ListBuckets, Unproved::Undeclared)]);
    // An `unsupported` row whose error body was cut off: unsettled, not proved.
    let mut cut_off = run;
    cut_off.retain(|e| e.operation() != CopyObject);
    cut_off.push(other(CopyObject, body(501, cutn(12))));
    let v = verdict(&CHECKED, &cut_off);
    assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
    assert!(
        matches!(v.unproved[..], [(CopyObject, Unproved::Unsettled { .. })]),
        "{v:?}"
    );
    // An empty run proves nothing: every row that must be proved is named.
    let v = verdict(&PRE_0_1, &[]);
    assert_eq!(v.outcome, Outcome::Inconclusive);
    let must_prove = PRE_0_1
        .rows()
        .iter()
        .filter(|r| !matches!(r.expected, Expected::SupportedNotChecked { .. }))
        .count();
    assert_eq!(v.unproved.len(), must_prove, "{v:?}");
    assert!(v.unproved.iter().all(|(_, u)| *u == Unproved::NotExercised));
    assert!(v.proved.is_empty());
}

#[test]
fn a_row_is_proved_only_when_every_exercise_of_it_passed() {
    let run = declared_refusals(&CHECKED);
    for (again, outcome) in [
        (other(CopyObject, none()), Outcome::Inconclusive),
        (other(CopyObject, body(501, cutn(3))), Outcome::Inconclusive),
        (other(CopyObject, just(200)), Outcome::Fail),
    ] {
        let mut with = run.clone();
        with.push(again.clone());
        let v = verdict(&CHECKED, &with);
        assert_eq!(v.outcome, outcome, "{again:?}: {v:?}");
        assert_eq!(v.proved, [HeadBucket], "{v:?}");
    }
}

#[test]
fn a_run_that_proves_no_row_is_inconclusive() {
    // Nothing to prove: no fatal, no unproved row, and still not a PASS (0017's vacuity gate).
    const NOTHING: &[Row] = &[Row::not_checked(UploadPart, 750, "test")];
    let exercised = vec![other(UploadPart, just(200))];
    for (profile, run) in [
        (Profile::new("nothing", ANY, NOTHING), exercised),
        (Profile::new("empty", ANY, &[]), vec![]),
    ] {
        let v = verdict(&profile, &run);
        assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
        assert!(v.proved.is_empty() && v.fatal.is_empty() && v.unproved.is_empty());
    }
    // Every shipped profile has a row to prove.
    for p in PROFILES {
        let checked = |r: &Row| !matches!(r.expected, Expected::SupportedNotChecked { .. });
        assert!(p.rows().iter().any(checked), "{}", p.name());
    }
}
