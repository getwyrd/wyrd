//! The capability matrix's `required` rows (proposal 0017 §3, §7; #937, second slice): the
//! closed table of what each row reads, the failure classes as one rule over every row, the asks
//! that cannot test their row, the 404 fact, and the full `pre-0.1` run. Pure: every leg calls
//! the production `wyrd_validate::matrix` functions with a fixed input. No network, no second
//! process.
//!
//! Fixtures are chosen per class and per precedence pair: the pair puts the fact that only leaves
//! the exercise unproved first in message order and the fatal one later, and asserts the fatal
//! class.

#![forbid(unsafe_code)]

use wyrd_validate::matrix::{
    classify, verdict, Answer, Arrived, Body, DeclaredCode, DeleteFailure, DeleteResult, Exercise,
    Expected, FailureClass, Listed, Operation, Outcome, Response, Row, RowResult, Unproved,
    PRE_0_1,
};
use wyrd_validate::ErrorCode;
use FailureClass::{Integrity, Oracle, Protocol, UnexpectedSupport};
use Operation::*;

// --- Fixtures -------------------------------------------------------------------------------

const OBJECT: &[u8] = b"0123456789abcdef";
const SIZE: u64 = 16;
const ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";
/// A strong entity-tag the object does not have.
const OTHER: &str = "\"0123456789abcdef0123456789abcdef\"";
/// What a `GetObjectRanged` asks by default: `bytes=2-5`.
const SPAN: &[u8] = b"2345";
/// Every shape of a tag that cannot test either conditional row.
const NOT_STRONG: [&str; 4] = ["*", "abc", "W/\"abc\"", ""];

fn resp(status: u16) -> Response {
    Response::new(status)
}

fn ans(response: Response) -> Answer {
    Answer::Response(response)
}

fn just(status: u16) -> Answer {
    ans(resp(status))
}

fn body(status: u16, b: Body) -> Answer {
    ans(resp(status).with_body(b))
}

fn none() -> Answer {
    Answer::NoAnswer {
        why: "timed out before a response head".to_string(),
    }
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

fn row(op: Operation) -> &'static Row {
    PRE_0_1.row(op).expect("pre-0.1 declares the row")
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

fn with_differing_byte(b: &[u8]) -> Vec<u8> {
    let mut b = b.to_vec();
    b[1] ^= 1;
    b
}

// Exercises: the row's ask, as recorded, and the answer under test.

fn put_with(answer: Answer, request_body_ended: bool) -> Exercise {
    Exercise::PutObject {
        answer,
        request_body_ended,
    }
}

fn put(answer: Answer) -> Exercise {
    put_with(answer, true)
}

fn put_200() -> Response {
    resp(200).with_etag(ETAG)
}

fn get(answer: Answer) -> Exercise {
    Exercise::GetObject {
        object: OBJECT.to_vec(),
        answer,
    }
}

fn ranged_at(first: u64, last: u64, answer: Answer) -> Exercise {
    Exercise::GetObjectRanged {
        object: OBJECT.to_vec(),
        first,
        last,
        answer,
    }
}

fn ranged(answer: Answer) -> Exercise {
    ranged_at(2, 5, answer)
}

fn r206() -> Response {
    resp(206).with_content_range("bytes 2-5/16")
}

fn hit_sending(sent: &str, answer: Answer) -> Exercise {
    Exercise::GetObjectIfNoneMatchHit {
        sent: sent.to_string(),
        etag: ETAG.to_string(),
        object: OBJECT.to_vec(),
        answer,
    }
}

fn hit(answer: Answer) -> Exercise {
    hit_sending(ETAG, answer)
}

fn miss_sending(sent: &str, answer: Answer) -> Exercise {
    Exercise::GetObjectIfNoneMatchMiss {
        sent: sent.to_string(),
        etag: ETAG.to_string(),
        object: OBJECT.to_vec(),
        answer,
    }
}

fn miss(answer: Answer) -> Exercise {
    miss_sending(OTHER, answer)
}

fn head(answer: Answer) -> Exercise {
    Exercise::HeadObject {
        size: SIZE,
        etag: ETAG.to_string(),
        answer,
    }
}

fn head_200() -> Response {
    resp(200).with_content_length(SIZE).with_etag(ETAG)
}

fn probe(key: &str, answer: Answer) -> (String, Answer) {
    (key.to_string(), answer)
}

fn del(answer: Answer, follow_ups: Vec<(String, Answer)>) -> Exercise {
    Exercise::DeleteObject {
        key: "a".to_string(),
        answer,
        follow_ups,
    }
}

/// The delete of `a` answered its defined 204, and these follow-ups.
fn del_204(follow_ups: Vec<(String, Answer)>) -> Exercise {
    del(just(204), follow_ups)
}

fn bulk(keys: &[&str], answer: Answer, follow_ups: Vec<(String, Answer)>) -> Exercise {
    Exercise::DeleteObjects {
        keys: strings(keys),
        answer,
        follow_ups,
    }
}

fn result(deleted: &[&str], errors: Vec<DeleteFailure>) -> Answer {
    body(
        200,
        Body::DeleteResult(DeleteResult {
            deleted: strings(deleted),
            errors,
        }),
    )
}

fn failure(key: &str) -> DeleteFailure {
    DeleteFailure {
        key: key.to_string(),
        code: "InternalError".to_string(),
    }
}

/// The bulk delete of `a` and `b`, each proved gone, with `answer`.
fn bulk_ab(answer: Answer) -> Exercise {
    bulk(
        &["a", "b"],
        answer,
        vec![probe("a", just(404)), probe("b", just(404))],
    )
}

/// The bulk delete of `a` alone, with a defined `<DeleteResult>`, and these follow-ups.
fn bulk_a(follow_ups: Vec<(String, Answer)>) -> Exercise {
    bulk(&["a"], result(&["a"], vec![]), follow_ups)
}

fn written() -> Vec<Listed> {
    vec![listed("p/a", 16), listed("p/b", 3)]
}

fn listing_of(keys: Vec<Listed>) -> Answer {
    body(200, Body::Listing(keys))
}

fn list_v2(answer: Answer) -> Exercise {
    Exercise::ListObjectsV2 {
        expected: written(),
        answer,
    }
}

fn list_v1(answer: Answer) -> Exercise {
    Exercise::ListObjects {
        expected: written(),
        answer,
    }
}

// Assertions: each goes through both production entry points.

/// `exercise` alone is fatal with `class`, in `classify` and in the run verdict.
fn fails(exercise: Exercise, class: FailureClass) {
    let op = exercise.operation();
    match classify(row(op), &exercise) {
        RowResult::Fatal { class: got, .. } => assert_eq!(got, class, "{exercise:?}"),
        got => panic!("{exercise:?}: {got:?}"),
    }
    let v = verdict(&PRE_0_1, &[exercise]);
    assert_eq!(v.outcome, Outcome::Fail, "{v:?}");
    let fatal: Vec<_> = v.fatal.iter().map(|f| (f.operation, f.class)).collect();
    assert_eq!(fatal, [(op, class)], "{v:?}");
    assert!(!v.proved.contains(&op), "{v:?}");
}

/// `exercise` alone is unsettled: nothing fatal, its row reported unproved.
fn unsettled(exercise: Exercise) {
    let op = exercise.operation();
    let result = classify(row(op), &exercise);
    assert!(
        matches!(result, RowResult::Unsettled { .. }),
        "{exercise:?}: {result:?}"
    );
    let v = verdict(&PRE_0_1, &[exercise]);
    assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
    assert!(v.fatal.is_empty(), "{v:?}");
    let reason = v.unproved.iter().find(|(o, _)| *o == op);
    assert!(
        matches!(reason, Some((_, Unproved::Unsettled { .. }))),
        "{op:?}: {v:?}"
    );
    assert!(!v.proved.contains(&op), "{v:?}");
}

/// `exercise` alone passes and proves its row.
fn passes(exercise: Exercise) {
    let op = exercise.operation();
    assert_eq!(
        classify(row(op), &exercise),
        RowResult::Pass,
        "{exercise:?}"
    );
    let v = verdict(&PRE_0_1, &[exercise]);
    assert!(v.fatal.is_empty(), "{v:?}");
    assert_eq!(v.proved, [op], "{v:?}");
}

/// The exact fatal result, where the wording shows which check spoke first.
fn fatal_with(exercise: Exercise, class: FailureClass, detail: &str) {
    assert_eq!(
        classify(row(exercise.operation()), &exercise),
        RowResult::Fatal {
            class,
            detail: detail.to_string()
        },
        "{exercise:?}"
    );
}

// --- (H) The full run: the first leg, red on child-1's module -------------------------------

/// Every `required` row exercised with the defined answer, in profile order.
fn defined_required() -> Vec<Exercise> {
    vec![
        put(ans(put_200().with_content_length(0))),
        get(ans(resp(200)
            .with_content_length(SIZE)
            .with_etag(ETAG)
            .with_body(bytes(OBJECT)))),
        ranged(ans(r206().with_content_length(4).with_body(bytes(SPAN)))),
        hit(just(304)),
        miss(body(200, bytes(OBJECT))),
        head(ans(head_200())),
        del_204(vec![probe("a", just(404))]),
        bulk(
            &["a", "b"],
            result(&["a", "b"], vec![]),
            vec![probe("a", just(404)), probe("b", just(404))],
        ),
        list_v2(listing_of(written())),
        list_v1(listing_of(written())),
    ]
}

/// Every `unsupported` row with its declared refusal.
fn declared_refusals() -> Vec<Exercise> {
    PRE_0_1
        .rows()
        .iter()
        .filter_map(|r| match r.expected {
            Expected::Unsupported { status, code, .. } => {
                let code = match code {
                    DeclaredCode::Code(c) => ErrorCode::Code(c.to_string()),
                    DeclaredCode::NoBody => ErrorCode::NoBody,
                };
                Some(Exercise::Other {
                    operation: r.operation,
                    answer: ans(resp(status).with_error_code(code)),
                })
            }
            _ => None,
        })
        .collect()
}

fn full_run() -> Vec<Exercise> {
    let mut run = defined_required();
    run.extend(declared_refusals());
    run
}

fn replacing(replacement: &Exercise) -> Vec<Exercise> {
    let op = replacement.operation();
    let mut run = full_run();
    let at = run
        .iter()
        .position(|e| e.operation() == op)
        .expect("the full run exercises the row");
    run[at] = replacement.clone();
    run
}

#[test]
fn a_full_defined_run_against_pre_0_1_passes() {
    let run = full_run();
    for exercise in &run {
        let result = classify(row(exercise.operation()), exercise);
        assert_eq!(result, RowResult::Pass, "{exercise:?}");
    }
    let v = verdict(&PRE_0_1, &run);
    assert_eq!(v.outcome, Outcome::Pass, "{v:?}");
    assert!(v.fatal.is_empty() && v.unproved.is_empty(), "{v:?}");
    let every_row: Vec<Operation> = PRE_0_1.rows().iter().map(|r| r.operation).collect();
    assert_eq!(v.proved, every_row, "{v:?}");
}

#[test]
fn replacing_any_exercise_with_a_fatal_one_fails_the_run_naming_row_and_class() {
    let fatal = [
        (put(just(404)), Protocol),
        (get(just(404)), Oracle),
        (get(body(200, bytes(b"nope"))), Integrity),
        (ranged(just(404)), Oracle),
        (ranged(body(206, bytes(SPAN))), Protocol),
        (hit(just(200)), Protocol),
        (miss(just(304)), Protocol),
        (head(ans(resp(200).with_etag(ETAG))), Oracle),
        (del_204(vec![probe("a", just(200))]), Oracle),
        (bulk_ab(result(&["a"], vec![])), Protocol),
        (list_v2(listing_of(vec![])), Oracle),
        (list_v1(listing_of(vec![])), Oracle),
        (
            Exercise::Other {
                operation: CopyObject,
                answer: just(200),
            },
            UnexpectedSupport,
        ),
    ];
    for (exercise, class) in fatal {
        let op = exercise.operation();
        let v = verdict(&PRE_0_1, &replacing(&exercise));
        assert_eq!(v.outcome, Outcome::Fail, "{exercise:?}: {v:?}");
        let named: Vec<_> = v.fatal.iter().map(|f| (f.operation, f.class)).collect();
        assert_eq!(named, [(op, class)], "{exercise:?}");
        assert!(!v.proved.contains(&op), "{v:?}");
    }
    // Every `unsupported` row: a success is support, whatever else the run proved.
    for refusal in declared_refusals() {
        let operation = refusal.operation();
        let v = verdict(
            &PRE_0_1,
            &replacing(&Exercise::Other {
                operation,
                answer: just(200),
            }),
        );
        assert_eq!(v.outcome, Outcome::Fail, "{operation:?}: {v:?}");
        let named: Vec<_> = v.fatal.iter().map(|f| (f.operation, f.class)).collect();
        assert_eq!(named, [(operation, UnexpectedSupport)], "{v:?}");
    }
}

#[test]
fn replacing_any_exercise_with_an_unsettled_or_unanswered_one_is_inconclusive_naming_the_row() {
    let unsettled = [
        put(ans(put_200().with_body(cutn(0)))),
        get(body(200, cutb(&OBJECT[..4]))),
        ranged(ans(r206().with_body(cutn(1)))),
        hit(body(304, cutn(0))),
        miss(body(200, cutb(b""))),
        head(ans(head_200().with_body(cutn(0)))),
        del_204(vec![]),
        bulk(&["a"], result(&["a"], vec![]), vec![]),
        list_v2(body(200, cutn(9))),
        list_v1(body(200, cutn(9))),
        Exercise::Other {
            operation: CopyObject,
            answer: body(501, cutn(3)),
        },
    ];
    for exercise in unsettled {
        let op = exercise.operation();
        let v = verdict(&PRE_0_1, &replacing(&exercise));
        assert_eq!(v.outcome, Outcome::Inconclusive, "{exercise:?}: {v:?}");
        assert!(v.fatal.is_empty(), "{v:?}");
        assert!(
            matches!(v.unproved[..], [(o, Unproved::Unsettled { .. })] if o == op),
            "{exercise:?}: {v:?}"
        );
        assert!(!v.proved.contains(&op), "{v:?}");
    }
    let unanswered = [
        put(none()),
        get(none()),
        ranged(none()),
        hit(none()),
        miss(none()),
        head(none()),
        del(none(), vec![probe("a", just(200))]),
        bulk(&["a"], none(), vec![probe("a", just(200))]),
        list_v2(none()),
        list_v1(none()),
        Exercise::Other {
            operation: CopyObject,
            answer: none(),
        },
    ];
    for exercise in unanswered {
        let op = exercise.operation();
        let v = verdict(&PRE_0_1, &replacing(&exercise));
        assert_eq!(v.outcome, Outcome::Inconclusive, "{exercise:?}: {v:?}");
        assert!(v.fatal.is_empty(), "{v:?}");
        assert!(
            matches!(v.unproved[..], [(o, Unproved::NoAnswer { .. })] if o == op),
            "{exercise:?}: {v:?}"
        );
    }
}

#[test]
fn a_row_with_one_passing_and_one_unsettled_exercise_is_not_proved() {
    for again in [
        get(body(200, cutn(3))),
        ranged_at(5, 2, just(200)),
        del_204(vec![]),
    ] {
        let op = again.operation();
        let mut run = full_run();
        run.push(again);
        let v = verdict(&PRE_0_1, &run);
        assert_eq!(v.outcome, Outcome::Inconclusive, "{v:?}");
        assert!(!v.proved.contains(&op), "{v:?}");
        assert!(
            matches!(v.unproved[..], [(o, Unproved::Unsettled { .. })] if o == op),
            "{v:?}"
        );
    }
}

// --- (B) The ladder: each precedence pair, the unproving fact first -------------------------

#[test]
fn a_cut_never_hides_a_fatal_fact() {
    // A wrong declared length, then a correct prefix cut off.
    let wrong_length = resp(200).with_content_length(15);
    fails(
        get(ans(wrong_length.with_body(cutb(&OBJECT[..4])))),
        Integrity,
    );
    // A wrong header before a cut after no byte, on an answer with no body.
    fails(
        put(ans(resp(200).with_etag("W/\"x\"").with_body(cutn(0)))),
        Protocol,
    );
    fails(
        put(ans(put_200().with_content_length(7).with_body(cutn(0)))),
        Protocol,
    );
    fails(
        head(ans(resp(200).with_etag(ETAG).with_body(cutn(0)))),
        Oracle,
    );
    fails(ranged(body(206, cutb(&SPAN[..1]))), Protocol);
    fails(
        ranged(ans(r206()
            .with_content_length(9)
            .with_body(cutb(&SPAN[..1])))),
        Integrity,
    );
    // The status of a request cut off after no byte.
    fails(get(body(404, cutn(0))), Oracle);
    fails(hit(body(200, cutn(0))), Protocol);
    fails(list_v2(body(500, cutn(0))), Protocol);
    // A readable key after a delete whose own answer was cut off.
    fails(del(body(204, cutn(0)), vec![probe("a", just(200))]), Oracle);
    fails(
        bulk(&["a"], body(200, cutn(40)), vec![probe("a", just(200))]),
        Oracle,
    );
    fails(
        bulk(&["a"], body(200, cutn(40)), vec![probe("a", just(403))]),
        Protocol,
    );
}

#[test]
fn an_unfit_ask_never_hides_a_fatal_fact() {
    // Asked before any answer, and still the 404, the 5xx, the byte, the document.
    fails(ranged_at(2, 16, just(404)), Oracle);
    fails(ranged_at(5, 2, just(503)), Protocol);
    fails(hit_sending(OTHER, body(304, bytes(b"x"))), Protocol);
    fails(miss_sending(ETAG, body(200, bytes(b"nope"))), Integrity);
    fails(
        bulk(&["a", "a"], result(&["a", "a"], vec![failure("a")]), vec![]),
        Protocol,
    );
    fails(
        bulk(
            &["a", "a"],
            result(&["a"], vec![]),
            vec![probe("a", just(200))],
        ),
        Oracle,
    );
}

#[test]
fn a_lost_follow_up_or_an_unprobed_key_never_hides_a_fatal_fact() {
    // A lost probe, in message order before a readable key.
    fails(
        bulk(
            &["a", "b"],
            result(&["a", "b"], vec![]),
            vec![probe("a", none()), probe("b", just(200))],
        ),
        Oracle,
    );
    fails(
        del_204(vec![probe("b", just(200)), probe("a", just(200))]),
        Oracle,
    );
    // A cut-off probe, before one with a 5xx.
    fails(
        del_204(vec![probe("a", body(404, cutn(0))), probe("a", just(503))]),
        Protocol,
    );
    // A key with no probe at all, and an <Error> row in the document.
    fails(
        bulk(
            &["a", "b"],
            result(&["a"], vec![failure("b")]),
            vec![probe("a", just(404))],
        ),
        Protocol,
    );
}

#[test]
fn an_empty_listing_ask_never_hides_a_listed_key() {
    let ask = |answer| Exercise::ListObjects {
        expected: vec![],
        answer,
    };
    fails(ask(listing_of(vec![listed("p/ghost", 1)])), Oracle);
    fails(ask(body(200, bad())), Protocol);
    fails(ask(just(403)), Protocol);
}

#[test]
fn the_first_failing_check_in_message_order_names_the_class() {
    // The request side before the status.
    fatal_with(
        put_with(ans(put_200().with_content_length(9)), false),
        Protocol,
        "answered 200 before the request body had ended",
    );
    // The status before the headers and the body: a 404, whatever else was wrong.
    fatal_with(
        get(ans(resp(404).with_content_length(1).with_body(bytes(b"x")))),
        Oracle,
        "answered 404 to a read of a key the run wrote",
    );
    fatal_with(
        get(ans(resp(206).with_content_length(1).with_body(bytes(b"x")))),
        Protocol,
        "answered 206; the row defines 200",
    );
    // Headers in the table's order, across classes: Content-Range before Content-Length.
    fatal_with(
        ranged(ans(resp(206)
            .with_content_range("bytes 0-3/16")
            .with_content_length(9)
            .with_body(bytes(b"nope")))),
        Protocol,
        "Content-Range \"bytes 0-3/16\"; expected \"bytes 2-5/16\"",
    );
    // The header before the body, across classes.
    fatal_with(
        ranged(ans(resp(206)
            .with_content_range("bytes 0-3/16")
            .with_body(bytes(b"nope")))),
        Protocol,
        "Content-Range \"bytes 0-3/16\"; expected \"bytes 2-5/16\"",
    );
    fatal_with(
        get(ans(resp(200)
            .with_content_length(1)
            .with_body(bytes(b"nope")))),
        Integrity,
        "Content-Length 1; expected 16 bytes",
    );
    fatal_with(
        head(ans(resp(200)
            .with_content_length(1)
            .with_etag(OTHER)
            .with_body(bytes(b"x")))),
        Oracle,
        "Content-Length Some(1); the object holds 16 bytes",
    );
    // ETag before Content-Length on a PUT, and both before the body.
    fatal_with(
        put(ans(resp(200).with_etag("abc").with_content_length(9))),
        Protocol,
        "ETag \"abc\" is not a strong entity-tag",
    );
    // A delete's header and its body, before its follow-ups, across classes.
    fatal_with(
        del(
            ans(resp(204).with_content_length(1)),
            vec![probe("a", just(200))],
        ),
        Protocol,
        "answered 204 declaring Content-Length 1; it carries no body",
    );
    fatal_with(
        del(body(204, bytes(b"x")), vec![probe("a", just(200))]),
        Protocol,
        "answered 204 with a body of 1 bytes",
    );
    fatal_with(
        bulk(&["a"], result(&["z"], vec![]), vec![probe("a", just(200))]),
        Protocol,
        "<Deleted> names [\"z\"]; the delete requested [\"a\"]",
    );
    // Follow-ups in their order.
    fatal_with(
        del_204(vec![probe("a", just(503)), probe("a", just(200))]),
        Protocol,
        "the follow-up HEAD of \"a\" answered 503; the row defines 404",
    );
    fatal_with(
        del_204(vec![probe("a", just(200)), probe("a", just(503))]),
        Oracle,
        "\"a\" answered 200 to a HEAD after its delete",
    );
    // Within one follow-up, the status before the body.
    fatal_with(
        del_204(vec![probe("a", body(200, bytes(b"x")))]),
        Oracle,
        "\"a\" answered 200 to a HEAD after its delete",
    );
}

#[test]
fn no_response_head_is_no_verdict_and_its_follow_ups_are_not_judged() {
    for follow_up in [just(200), just(503), body(404, bytes(b"x"))] {
        for ex in [
            del(none(), vec![probe("a", follow_up.clone())]),
            bulk(&["a"], none(), vec![probe("a", follow_up.clone())]),
        ] {
            let result = classify(row(ex.operation()), &ex);
            assert!(matches!(result, RowResult::NoAnswer { .. }), "{ex:?}");
        }
    }
    // Before an ask that cannot test the row, too: no head is step 2.
    for ex in [
        ranged_at(5, 2, none()),
        hit_sending("*", none()),
        bulk(&[], none(), vec![]),
    ] {
        let result = classify(row(ex.operation()), &ex);
        assert!(matches!(result, RowResult::NoAnswer { .. }), "{ex:?}");
    }
}

// --- (C) Oracle -----------------------------------------------------------------------------

#[test]
fn a_404_to_a_typed_read_of_a_written_key_is_oracle_whatever_was_asked() {
    let not_found = || {
        vec![
            just(404),
            ans(resp(404).with_error_code(ErrorCode::Code("NoSuchKey".to_string()))),
            body(404, bad()),
            body(404, cutn(3)),
        ]
    };
    for answer in not_found() {
        for exercise in [
            get(answer.clone()),
            ranged(answer.clone()),
            hit(answer.clone()),
            miss(answer.clone()),
            head(answer.clone()),
            // Asks that cannot test their row.
            ranged_at(5, 2, answer.clone()),
            ranged_at(2, 16, answer.clone()),
            hit_sending(OTHER, answer.clone()),
            hit_sending("*", answer.clone()),
            miss_sending(ETAG, answer.clone()),
            miss_sending("W/\"abc\"", answer.clone()),
        ] {
            fails(exercise, Oracle);
        }
    }
    // A 404 anywhere else is not a typed read: a write, a delete, a listing.
    fails(put(just(404)), Protocol);
    fails(del(just(404), vec![probe("a", just(404))]), Protocol);
    fails(bulk_ab(just(404)), Protocol);
    fails(list_v2(just(404)), Protocol);
    fails(list_v1(just(404)), Protocol);
    // Nor is a delete's follow-up: there a 404 is the defined answer.
    passes(del_204(vec![probe("a", just(404))]));
}

#[test]
fn a_head_that_misreports_the_object_is_oracle() {
    let h = |len, etag| head(ans(resp(200).with_content_length(len).with_etag(etag)));
    fails(h(SIZE - 1, ETAG), Oracle);
    fails(h(SIZE + 1, ETAG), Oracle);
    fails(h(0, ETAG), Oracle);
    fails(h(SIZE, OTHER), Oracle);
    // A strong tag and its weak form are different tags.
    fails(h(SIZE, "W/\"d41d8cd98f00b204e9800998ecf8427e\""), Oracle);
    // Absent counts as different.
    fails(head(ans(resp(200).with_etag(ETAG))), Oracle);
    fails(head(ans(resp(200).with_content_length(SIZE))), Oracle);
    fails(head(just(200)), Oracle);
}

#[test]
fn a_listing_that_is_not_exactly_what_was_written_is_oracle() {
    let mut missing = written();
    missing.pop();
    let mut extra = written();
    extra.push(listed("p/c", 1));
    let mut wrong_size = written();
    wrong_size[1].size = 4;
    let mut twice = written();
    twice.push(listed("p/a", 16));
    for keys in [vec![], missing, extra, wrong_size, twice] {
        fails(list_v2(listing_of(keys.clone())), Oracle);
        fails(list_v1(listing_of(keys)), Oracle);
    }
    // Order does not matter; every page counts together.
    let mut reversed = written();
    reversed.reverse();
    passes(list_v2(listing_of(reversed.clone())));
    passes(list_v1(listing_of(reversed)));
}

#[test]
fn the_written_keys_may_be_recorded_in_any_order() {
    // A run records keys in write order, so `expected` need not be sorted. `written()` is sorted,
    // so this is the case that shows `expected` itself is put in order.
    let mut unsorted = written();
    unsorted.reverse();
    for answer_keys in [written(), unsorted.clone()] {
        passes(Exercise::ListObjectsV2 {
            expected: unsorted.clone(),
            answer: listing_of(answer_keys.clone()),
        });
        passes(Exercise::ListObjects {
            expected: unsorted.clone(),
            answer: listing_of(answer_keys),
        });
    }
    // Still exact: a missing key is `oracle` against an unsorted record too.
    fails(
        Exercise::ListObjectsV2 {
            expected: unsorted.clone(),
            answer: listing_of(vec![listed("p/a", 16)]),
        },
        Oracle,
    );
}

// --- (C) The follow-up key pair: the same three on both delete rows -------------------------

#[test]
fn a_follow_up_is_judged_against_the_key_it_asked_about() {
    for row_with in [del_204 as fn(Vec<(String, Answer)>) -> Exercise, bulk_a] {
        // Of `a`, requested: 200 is `oracle`; any 2xx is.
        fails(row_with(vec![probe("a", just(200))]), Oracle);
        fails(row_with(vec![probe("a", just(204))]), Oracle);
        // Of `b`, not requested: 200 proves nothing, and neither does anything else.
        unsettled(row_with(vec![probe("b", just(200))]));
        unsettled(row_with(vec![probe("b", just(404))]));
        unsettled(row_with(vec![probe("b", just(403))]));
        unsettled(row_with(vec![probe("b", none())]));
        unsettled(row_with(vec![probe("b", body(404, cutn(0)))]));
        // Of `b`, the checks that do not depend on the key still run.
        fails(row_with(vec![probe("b", just(500))]), Protocol);
        fails(row_with(vec![probe("b", just(503))]), Protocol);
        fails(row_with(vec![probe("b", just(599))]), Protocol);
        fails(row_with(vec![probe("b", body(200, bytes(b"x")))]), Protocol);
        fails(row_with(vec![probe("b", body(404, cutb(b"x")))]), Protocol);
        fails(row_with(vec![probe("b", body(404, bad()))]), Protocol);
        // Proved gone, and then a follow-up of another key: never a pass, never a failure.
        unsettled(row_with(vec![probe("a", just(404)), probe("b", just(200))]));
        fails(
            row_with(vec![probe("a", just(404)), probe("b", just(500))]),
            Protocol,
        );
        // Of `a`, any other status is not the defined one.
        fails(row_with(vec![probe("a", just(403))]), Protocol);
        fails(row_with(vec![probe("a", just(304))]), Protocol);
        fails(row_with(vec![probe("a", just(500))]), Protocol);
        fails(row_with(vec![probe("a", just(599))]), Protocol);
        // Of `a`, the defined answer with a body.
        fails(row_with(vec![probe("a", body(404, bytes(b"x")))]), Protocol);
        fails(row_with(vec![probe("a", body(404, cutn(1)))]), Protocol);
        fails(row_with(vec![probe("a", body(404, bad()))]), Protocol);
        // Of `a`, left unproved.
        unsettled(row_with(vec![]));
        unsettled(row_with(vec![probe("a", none())]));
        unsettled(row_with(vec![probe("a", body(404, cutn(0)))]));
        // Of `a`, proved.
        passes(row_with(vec![probe("a", just(404))]));
        passes(row_with(vec![probe("a", body(404, bytes(b"")))]));
        passes(row_with(vec![probe("a", just(404)), probe("a", just(404))]));
    }
}

#[test]
fn a_clean_404_never_erases_a_lost_or_cut_off_follow_up() {
    for row_with in [del_204 as fn(Vec<(String, Answer)>) -> Exercise, bulk_a] {
        for incomplete in [
            vec![probe("a", none()), probe("a", just(404))],
            vec![probe("a", just(404)), probe("a", none())],
            vec![probe("a", body(404, cutn(0))), probe("a", just(404))],
            vec![probe("a", just(404)), probe("a", body(404, cutb(b"")))],
        ] {
            let exercise = row_with(incomplete);
            let op = exercise.operation();
            unsettled(exercise.clone());
            // And in a full run: the row is not proved, so the run cannot pass.
            let v = verdict(&PRE_0_1, &replacing(&exercise));
            assert_eq!(v.outcome, Outcome::Inconclusive, "{exercise:?}: {v:?}");
            assert!(
                matches!(v.unproved[..], [(o, Unproved::Unsettled { .. })] if o == op),
                "{exercise:?}: {v:?}"
            );
            assert!(!v.proved.contains(&op), "{exercise:?}: {v:?}");
        }
        // Two clean answers are still a pass: the rule is about the incomplete one.
        passes(row_with(vec![probe("a", just(404)), probe("a", just(404))]));
    }
}

#[test]
fn a_second_follow_up_never_undoes_a_fatal_one() {
    for row_with in [del_204 as fn(Vec<(String, Answer)>) -> Exercise, bulk_a] {
        fails(
            row_with(vec![probe("a", just(404)), probe("a", just(200))]),
            Oracle,
        );
        fails(
            row_with(vec![probe("a", just(200)), probe("a", just(404))]),
            Oracle,
        );
        fails(
            row_with(vec![probe("a", just(503)), probe("a", just(404))]),
            Protocol,
        );
        // Nor does a lost one hide it, on either side.
        fails(
            row_with(vec![probe("a", none()), probe("a", just(200))]),
            Oracle,
        );
        fails(
            row_with(vec![probe("a", body(404, cutn(0))), probe("a", just(503))]),
            Protocol,
        );
        // A cut after the follow-up's head does not hide what the head said.
        fails(row_with(vec![probe("a", body(200, cutn(0)))]), Oracle);
        fails(row_with(vec![probe("a", body(503, cutn(0)))]), Protocol);
        fails(row_with(vec![probe("a", body(403, cutn(0)))]), Protocol);
    }
}

#[test]
fn every_requested_key_needs_its_own_proof() {
    let abc = ["a", "b", "c"];
    let doc = || result(&abc, vec![]);
    let heads = |p: &[(&str, u16)]| -> Vec<(String, Answer)> {
        p.iter().map(|(k, s)| probe(k, just(*s))).collect()
    };
    passes(bulk(
        &abc,
        doc(),
        heads(&[("c", 404), ("a", 404), ("b", 404)]),
    ));
    // The last key is the readable one: a probe paired with the wrong key would miss it.
    fails(
        bulk(&abc, doc(), heads(&[("a", 404), ("b", 404), ("c", 200)])),
        Oracle,
    );
    // Two probes of `a` prove nothing of `c`.
    unsettled(bulk(
        &abc,
        doc(),
        heads(&[("a", 404), ("a", 404), ("b", 404)]),
    ));
    unsettled(bulk(&abc, doc(), vec![]));
}

// --- (C) Integrity --------------------------------------------------------------------------

/// Every byte rule on one row that carries object bytes: `of` is what it must carry; `exercise`
/// builds its defined answer with a declared length, if any, and a body.
fn holds_to_the_bytes(exercise: fn(Option<u64>, Body) -> Exercise, of: &[u8]) {
    let undeclared = |b: Body| exercise(None, b);
    let declaring = |n: u64, b: Body| exercise(Some(n), b);
    let size = of.len() as u64;
    let long = [of, b"!"].concat();
    let wrong = with_differing_byte(of);
    // A declared length: absent or the expected count passes.
    passes(undeclared(bytes(of)));
    passes(declaring(size, bytes(of)));
    fails(declaring(size - 1, bytes(of)), Integrity);
    fails(declaring(size + 1, bytes(of)), Integrity);
    fails(declaring(0, bytes(of)), Integrity);
    // Bytes that arrived whole and differ: one off, short, long, none.
    fails(undeclared(bytes(&wrong)), Integrity);
    fails(undeclared(bytes(&of[..of.len() - 1])), Integrity);
    fails(undeclared(bytes(&long)), Integrity);
    fails(undeclared(Body::Empty), Integrity);
    fails(undeclared(bytes(b"")), Integrity);
    // Bytes before a cut: not a prefix, or more than expected.
    fails(undeclared(cutb(&wrong)), Integrity);
    fails(undeclared(cutb(&long)), Integrity);
    fails(undeclared(cutn(size + 1)), Integrity);
    // A cut after a correct prefix, after no byte, or at the very end leaves it unproved.
    unsettled(undeclared(cutb(&of[..1])));
    unsettled(undeclared(cutb(of)));
    unsettled(undeclared(cutb(b"")));
    unsettled(undeclared(cutn(0)));
    unsettled(undeclared(cutn(size)));
    // A declared length that is right does not settle a cut; one that is wrong settles it.
    unsettled(declaring(size, cutb(&of[..1])));
    fails(declaring(size + 1, cutb(&of[..1])), Integrity);
    fails(declaring(size + 1, cutn(1)), Integrity);
    // Not bytes at all: the wrong shape, not a byte mismatch.
    fails(undeclared(bad()), Protocol);
    fails(undeclared(Body::Listing(written())), Protocol);
    fails(
        undeclared(Body::DeleteResult(DeleteResult::default())),
        Protocol,
    );
}

/// `base` with the declared length, if any, and the body.
fn framed(base: Response, length: Option<u64>, b: Body) -> Answer {
    let framed = match length {
        Some(n) => base.with_content_length(n),
        None => base,
    };
    ans(framed.with_body(b))
}

#[test]
fn the_report_of_wrong_bytes_says_where_they_differ() {
    let differ = |got: usize, before: &str, at: usize| {
        format!("{got} bytes{before} differ from the expected 16 at offset {at}")
    };
    let wrong = with_differing_byte(OBJECT);
    let long = [OBJECT, b"!"].concat();
    fatal_with(get(body(200, bytes(&wrong))), Integrity, &differ(16, "", 1));
    fatal_with(
        get(body(200, bytes(&OBJECT[..15]))),
        Integrity,
        &differ(15, "", 15),
    );
    fatal_with(get(body(200, bytes(&long))), Integrity, &differ(17, "", 16));
    fatal_with(get(just(200)), Integrity, &differ(0, "", 0));
    fatal_with(
        get(body(200, cutb(&wrong[..4]))),
        Integrity,
        &differ(4, " before the cut", 1),
    );
}

#[test]
fn the_bytes_of_a_whole_get_are_the_objects() {
    holds_to_the_bytes(|n, b| get(framed(resp(200), n, b)), OBJECT);
}

#[test]
fn the_bytes_of_a_ranged_get_are_the_spans() {
    holds_to_the_bytes(|n, b| ranged(framed(r206(), n, b)), SPAN);
}

#[test]
fn the_bytes_of_a_conditional_200_are_the_objects() {
    holds_to_the_bytes(|n, b| miss(framed(resp(200), n, b)), OBJECT);
}

#[test]
fn an_empty_object_is_read_as_no_bytes() {
    let empty = |answer| Exercise::GetObject {
        object: vec![],
        answer,
    };
    passes(empty(just(200)));
    passes(empty(body(200, bytes(b""))));
    passes(empty(ans(resp(200).with_content_length(0))));
    unsettled(empty(body(200, cutn(0))));
    fails(empty(body(200, bytes(b"x"))), Integrity);
    fails(empty(body(200, cutn(1))), Integrity);
    fails(empty(ans(resp(200).with_content_length(1))), Integrity);
    fails(empty(listing_of(written())), Protocol);
}

// --- (C) Protocol ---------------------------------------------------------------------------

#[test]
fn any_status_other_than_the_rows_defined_one_is_protocol() {
    type AnsweredRow = (fn(Answer) -> Exercise, &'static [u16]);
    let rows: [AnsweredRow; 8] = [
        (put, &[201, 204, 304, 400, 403, 404, 412]),
        (get, &[201, 206, 304, 400, 403, 416]),
        (ranged, &[200, 304, 400, 403, 416]),
        (hit, &[200, 204, 206, 400, 403, 412]),
        (miss, &[204, 206, 304, 400, 403, 412]),
        (head, &[204, 304, 400, 403]),
        (list_v2, &[201, 204, 400, 403, 404]),
        (list_v1, &[201, 204, 400, 403, 404]),
    ];
    for (exercise, statuses) in rows {
        for status in statuses {
            fails(exercise(just(*status)), Protocol);
        }
    }
    for status in [200, 201, 400, 403, 404] {
        fails(del(just(status), vec![probe("a", just(404))]), Protocol);
    }
    for status in [201, 204, 400, 403, 404] {
        fails(bulk_ab(just(status)), Protocol);
    }
    // A 5xx, on every row; it is an answer, and not the defined one.
    for status in [500, 501, 503, 599] {
        for exercise in [
            put(just(status)),
            get(just(status)),
            ranged(just(status)),
            hit(just(status)),
            miss(just(status)),
            head(just(status)),
            del(just(status), vec![probe("a", just(404))]),
            bulk_ab(just(status)),
            list_v2(just(status)),
            list_v1(just(status)),
            // Asks that cannot test their row, and an exercise with no check of its own.
            ranged_at(5, 2, just(status)),
            ranged_at(2, 16, just(status)),
            hit_sending(OTHER, just(status)),
            miss_sending(ETAG, just(status)),
            bulk(&[], just(status), vec![]),
            Exercise::Other {
                operation: PutObject,
                answer: just(status),
            },
        ] {
            fails(exercise, Protocol);
        }
    }
}

/// Each row's defined answer with only its status changed, so no other check can give the same
/// class: this leg is red if any row's own status check goes missing.
#[test]
fn a_defined_answer_under_only_a_wrong_status_is_protocol_on_every_row() {
    let wrong = |defined: u16, status: u16| format!("answered {status}; the row defines {defined}");
    let delete_result = || {
        Body::DeleteResult(DeleteResult {
            deleted: strings(&["a", "b"]),
            errors: vec![],
        })
    };
    for status in [201, 202, 299, 302, 400, 403] {
        fatal_with(
            put(ans(Response {
                status,
                ..put_200().with_content_length(0)
            })),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            get(ans(resp(status)
                .with_content_length(SIZE)
                .with_body(bytes(OBJECT)))),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            ranged(ans(resp(status)
                .with_content_range("bytes 2-5/16")
                .with_content_length(4)
                .with_body(bytes(SPAN)))),
            Protocol,
            &wrong(206, status),
        );
        fatal_with(hit(just(status)), Protocol, &wrong(304, status));
        fatal_with(
            miss(ans(resp(status)
                .with_content_length(SIZE)
                .with_body(bytes(OBJECT)))),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            head(ans(Response {
                status,
                ..head_200()
            })),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            del(just(status), vec![probe("a", just(404))]),
            Protocol,
            &wrong(204, status),
        );
        fatal_with(
            bulk(
                &["a", "b"],
                body(status, delete_result()),
                vec![probe("a", just(404)), probe("b", just(404))],
            ),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            list_v2(body(status, Body::Listing(written()))),
            Protocol,
            &wrong(200, status),
        );
        fatal_with(
            list_v1(body(status, Body::Listing(written()))),
            Protocol,
            &wrong(200, status),
        );
    }
    // The statuses that are some other row's defined one, each on a row it is wrong for.
    fatal_with(
        ranged(ans(resp(200)
            .with_content_range("bytes 2-5/16")
            .with_body(bytes(SPAN)))),
        Protocol,
        &wrong(206, 200),
    );
    fatal_with(
        bulk(
            &["a", "b"],
            body(204, delete_result()),
            vec![probe("a", just(404)), probe("b", just(404))],
        ),
        Protocol,
        &wrong(200, 204),
    );
    fatal_with(
        del(just(200), vec![probe("a", just(404))]),
        Protocol,
        &wrong(204, 200),
    );
}

#[test]
fn a_put_is_a_receipt_only_with_a_strong_etag_and_after_the_request_body_ended() {
    passes(put(ans(put_200())));
    passes(put(ans(put_200().with_content_length(0))));
    // ETag absent, or not strong (RFC 9110 §8.8.3).
    fails(put(just(200)), Protocol);
    for etag in NOT_STRONG {
        fails(put(ans(resp(200).with_etag(etag))), Protocol);
    }
    fails(put(ans(resp(200).with_etag("\"a\"b\""))), Protocol);
    // A success before the request body ended is not a receipt.
    fails(put_with(ans(put_200()), false), Protocol);
    fails(put_with(ans(put_200().with_body(cutn(0))), false), Protocol);
    // A refusal may rightly come early: there the status alone speaks.
    fatal_with(
        put_with(just(403), false),
        Protocol,
        "answered 403; the row defines 200",
    );
    // The declared length (the length-framed case is the matrix's).
    fails(put(ans(put_200().with_content_length(1))), Protocol);
    fails(
        put(ans(put_200().with_content_length(1).with_body(cutn(0)))),
        Protocol,
    );
}

#[test]
fn a_204_declaring_a_body_is_protocol_and_declaring_none_is_not() {
    let proved = || vec![probe("a", just(404))];
    passes(del(ans(resp(204).with_content_length(0)), proved()));
    fails(
        del(ans(resp(204).with_content_length(1)), proved()),
        Protocol,
    );
    fails(
        del(
            ans(resp(204).with_content_length(1).with_body(cutn(0))),
            proved(),
        ),
        Protocol,
    );
}

#[test]
fn a_byte_on_an_answer_that_carries_no_body_is_protocol() {
    let no_body_answers: [fn(Body) -> Exercise; 6] = [
        |b| put(ans(put_200().with_body(b))),
        |b| hit(body(304, b)),
        |b| head(ans(head_200().with_body(b))),
        |b| del(body(204, b), vec![probe("a", just(404))]),
        |b| del_204(vec![probe("a", body(404, b))]),
        |b| bulk_a(vec![probe("a", body(404, b))]),
    ];
    for answer in no_body_answers {
        for b in [
            bytes(b"x"),
            cutb(b"x"),
            cutn(1),
            bad(),
            Body::Listing(written()),
            Body::DeleteResult(DeleteResult::default()),
        ] {
            fails(answer(b), Protocol);
        }
        // No byte at all is no body, however it was reported.
        for b in [Body::Empty, bytes(b"")] {
            passes(answer(b));
        }
        // A cut after no byte: the end of the answer was never seen.
        unsettled(answer(cutb(b"")));
        unsettled(answer(cutn(0)));
    }
}

#[test]
fn a_whole_body_that_is_not_the_document_its_status_calls_for_is_protocol() {
    let not_a_listing = [
        just(200),
        body(200, bytes(b"<xml/>")),
        body(200, bad()),
        body(200, Body::DeleteResult(DeleteResult::default())),
    ];
    for answer in not_a_listing {
        fails(list_v2(answer.clone()), Protocol);
        fails(list_v1(answer), Protocol);
    }
    for answer in [
        just(200),
        body(200, bytes(b"<xml/>")),
        body(200, bad()),
        listing_of(written()),
    ] {
        fails(bulk_ab(answer), Protocol);
    }
    fatal_with(
        list_v2(body(200, bad())),
        Protocol,
        "the body is not a listing: not an XML document",
    );
}

#[test]
fn a_delete_result_with_an_error_row_or_not_naming_each_key_once_is_protocol() {
    let ab = ["a", "b"];
    passes(bulk_ab(result(&ab, vec![])));
    passes(bulk_ab(result(&["b", "a"], vec![])));
    // An <Error> row, on a requested key or another, whatever <Deleted> says.
    fails(bulk_ab(result(&ab, vec![failure("b")])), Protocol);
    fails(bulk_ab(result(&["a"], vec![failure("b")])), Protocol);
    fails(bulk_ab(result(&ab, vec![failure("z")])), Protocol);
    // <Deleted> omits a key, repeats one, or names another.
    for deleted in [
        ["a"].as_slice(),
        &[],
        &["a", "a", "b"],
        &["a", "a"],
        &["a", "b", "z"],
        &["a", "z"],
    ] {
        fails(bulk_ab(result(deleted, vec![])), Protocol);
    }
}

#[test]
fn the_requested_keys_may_be_recorded_in_any_order() {
    let gone = || vec![probe("a", just(404)), probe("b", just(404))];
    for deleted in [["a", "b"], ["b", "a"]] {
        passes(bulk(&["b", "a"], result(&deleted, vec![]), gone()));
    }
    // Still exact against an unsorted ask.
    fails(bulk(&["b", "a"], result(&["a"], vec![]), gone()), Protocol);
    // A key named twice is found wherever it sits in the ask, so the exact match does not run.
    for deleted in [["a", "b"].as_slice(), &["a", "b", "a"], &["z"]] {
        unsettled(bulk(&["a", "b", "a"], result(deleted, vec![]), gone()));
    }
}

// --- (C) Unsettled --------------------------------------------------------------------------

#[test]
fn a_document_cut_before_it_could_be_read_is_unsettled() {
    for b in [cutn(0), cutn(40), cutb(b"<ListBucketResult>")] {
        unsettled(list_v2(body(200, b.clone())));
        unsettled(list_v1(body(200, b.clone())));
        unsettled(bulk_ab(body(200, b)));
    }
}

#[test]
fn an_exercise_with_no_check_of_its_own_is_unsettled_on_a_required_row() {
    // Even a 404 to a read: a generic exercise records nothing that makes it a typed read.
    for status in [200, 204, 206, 304, 400, 404] {
        for operation in [PutObject, GetObject, HeadObject, ListObjectsV2] {
            unsettled(Exercise::Other {
                operation,
                answer: just(status),
            });
        }
    }
}

// --- (F) What a row reads, and what it does not ---------------------------------------------

#[test]
fn content_range_must_be_exactly_the_span_asked_for() {
    let with = |cr: &str| ranged(ans(resp(206).with_content_range(cr).with_body(bytes(SPAN))));
    passes(with("bytes 2-5/16"));
    // The range unit is case-insensitive (RFC 9110 §14.1); digits may carry leading zeros (§14.4).
    passes(with("BYTES 2-5/16"));
    passes(with("Bytes 02-05/016"));
    let wrong = [
        // Another span or size.
        "bytes 2-6/16",
        "bytes 1-5/16",
        "bytes 2-5/17",
        "bytes 0-3/16",
        // Not the grammar: unit, signs, separators, an unknown size or range, whitespace.
        "items 2-5/16",
        "2-5/16",
        "bytes +2-5/16",
        "bytes 2-+5/16",
        "bytes 2-5/+16",
        "bytes -2-5/16",
        "bytes 2-5",
        "bytes 2-5/",
        "bytes 2-5/*",
        "bytes */16",
        "bytes -5/16",
        "bytes 2-/16",
        "bytes  2-5/16",
        "bytes 2 - 5/16",
        "bytes 2-5/16 ",
        " bytes 2-5/16",
        "bytes=2-5/16",
        "bytes 0x2-5/16",
        // Digits that do not fit.
        "bytes 2-5/18446744073709551616",
        "",
    ];
    for cr in wrong {
        fails(with(cr), Protocol);
    }
    // Absent.
    fails(ranged(body(206, bytes(SPAN))), Protocol);
}

#[test]
fn a_ranged_get_passes_at_the_edges_of_the_object() {
    for (first, last) in [(0u64, 0u64), (3, 3), (15, 15), (0, 15)] {
        let want = &OBJECT[first as usize..=last as usize];
        passes(ranged_at(
            first,
            last,
            ans(resp(206)
                .with_content_range(&format!("bytes {first}-{last}/16"))
                .with_content_length(want.len() as u64)
                .with_body(bytes(want))),
        ));
    }
}

#[test]
fn a_row_reads_nothing_the_table_does_not_list() {
    // A GET's ETag: another tag than the object's, on a whole, ranged or non-matching answer.
    passes(get(ans(resp(200)
        .with_etag(OTHER)
        .with_body(bytes(OBJECT)))));
    passes(ranged(ans(r206()
        .with_etag("weak?")
        .with_body(bytes(SPAN)))));
    passes(miss(ans(resp(200)
        .with_etag(OTHER)
        .with_body(bytes(OBJECT)))));
    // A Content-Range on an answer that is not a range.
    passes(get(ans(resp(200)
        .with_content_range("nonsense")
        .with_body(bytes(OBJECT)))));
    // A 304's Content-Length (RFC 9110 §8.6) and ETag.
    passes(hit(ans(resp(304).with_content_length(SIZE))));
    passes(hit(ans(resp(304).with_etag(OTHER))));
    // Content-Length on a HEAD 404, a listing, a <DeleteResult>; a 204's ETag.
    passes(del_204(vec![probe(
        "a",
        ans(resp(404).with_content_length(99).with_etag(OTHER)),
    )]));
    passes(list_v2(ans(resp(200)
        .with_content_length(99)
        .with_body(Body::Listing(written())))));
    passes(bulk_ab(ans(resp(200)
        .with_content_length(99)
        .with_etag("x")
        .with_body(Body::DeleteResult(DeleteResult {
            deleted: strings(&["a", "b"]),
            errors: vec![],
        })))));
    passes(del(
        ans(resp(204).with_etag("x")),
        vec![probe("a", just(404))],
    ));
    // The error code of an answer that is the defined one.
    let coded = |r: Response| r.with_error_code(ErrorCode::Code("Nope".to_string()));
    passes(get(ans(coded(resp(200)).with_body(bytes(OBJECT)))));
    passes(del_204(vec![probe("a", ans(coded(resp(404))))]));
}

// --- (G) Asks that cannot test their row ----------------------------------------------------

#[test]
fn a_conditional_get_with_an_unfit_tag_never_passes_and_is_held_to_what_it_got() {
    // The sent tag equals the object's only on a Hit; differs only on a Miss.
    let unfit_hit: Vec<&str> = NOT_STRONG.iter().copied().chain([OTHER]).collect();
    let unfit_miss: Vec<&str> = NOT_STRONG.iter().copied().chain([ETAG]).collect();
    type Form<'a> = (&'a [&'a str], fn(&str, Answer) -> Exercise);
    let forms: [Form; 2] = [(&unfit_hit, hit_sending), (&unfit_miss, miss_sending)];
    for (sent_values, sending) in forms {
        for sent in sent_values {
            // 200 with the object, and 304 with nothing: both acceptable, neither a pass.
            unsettled(sending(sent, body(200, bytes(OBJECT))));
            unsettled(sending(
                sent,
                ans(resp(200).with_content_length(SIZE).with_body(bytes(OBJECT))),
            ));
            unsettled(sending(sent, just(304)));
            unsettled(sending(sent, body(200, cutb(&OBJECT[..3]))));
            unsettled(sending(sent, body(304, cutn(0))));
            // A 200 is held to the object-bytes checks.
            fails(sending(sent, body(200, bytes(b"nope"))), Integrity);
            fails(sending(sent, body(200, cutb(b"nope"))), Integrity);
            fails(
                sending(
                    sent,
                    ans(resp(200).with_content_length(1).with_body(bytes(OBJECT))),
                ),
                Integrity,
            );
            fails(sending(sent, body(200, bad())), Protocol);
            // A 304 is held to the no-body check.
            fails(sending(sent, body(304, bytes(b"x"))), Protocol);
            fails(sending(sent, body(304, cutn(1))), Protocol);
            // Any other status is judged by the classes.
            fails(sending(sent, just(404)), Oracle);
            fails(sending(sent, just(500)), Protocol);
            fails(sending(sent, just(400)), Protocol);
            fails(sending(sent, just(204)), Protocol);
            // Only 200 and 304 are acceptable: the object's bytes under a 206 do not make it one.
            fails(sending(sent, just(206)), Protocol);
            fails(sending(sent, body(206, bytes(OBJECT))), Protocol);
            fails(sending(sent, body(201, bytes(OBJECT))), Protocol);
        }
    }
    // Fit, each form has one defined status, and the other is wrong.
    passes(hit(just(304)));
    fails(hit(body(200, bytes(OBJECT))), Protocol);
    passes(miss(body(200, bytes(OBJECT))));
    fails(miss(just(304)), Protocol);
}

#[test]
fn a_matching_tag_that_is_not_strong_cannot_test_the_matching_row() {
    // The sent value equals the object's tag, and it is not a strong tag: "a strong tag equal to
    // the object's" is two conditions, and this is the fixture where only the first one fails.
    for tag in NOT_STRONG {
        let ask = |answer| Exercise::GetObjectIfNoneMatchHit {
            sent: tag.to_string(),
            etag: tag.to_string(),
            object: OBJECT.to_vec(),
            answer,
        };
        unsettled(ask(just(304)));
        unsettled(ask(body(200, bytes(OBJECT))));
        fails(ask(body(304, bytes(b"x"))), Protocol);
        fails(ask(body(200, bytes(b"nope"))), Integrity);
        fails(ask(just(404)), Oracle);
    }
}

#[test]
fn a_range_outside_its_object_reads_only_the_two_facts() {
    // Whatever else came back settles nothing: the ask says nothing of what it should be.
    for (first, last) in [
        (5, 4),
        (2, 16),
        (16, 16),
        (u64::MAX, u64::MAX),
        (0, u64::MAX),
    ] {
        let ask = |answer| ranged_at(first, last, answer);
        for answer in [
            just(200),
            just(206),
            just(304),
            just(400),
            just(416),
            body(206, bytes(b"nope")),
            body(416, bad()),
            body(206, cutn(99)),
            ans(resp(206)
                .with_content_range("nonsense")
                .with_content_length(1)),
        ] {
            unsettled(ask(answer));
        }
        fails(ask(just(404)), Oracle);
        fails(ask(body(404, cutn(1))), Oracle);
        fails(ask(just(500)), Protocol);
        fails(ask(body(503, cutb(b"x"))), Protocol);
    }
}

#[test]
fn a_delete_of_no_keys_reads_only_the_two_facts_of_its_own_answer() {
    for answer in [
        just(200),
        just(400),
        just(404),
        result(&[], vec![]),
        result(&["z"], vec![failure("z")]),
        body(200, bad()),
        body(200, cutn(9)),
    ] {
        unsettled(bulk(&[], answer, vec![]));
    }
    fails(bulk(&[], just(500), vec![]), Protocol);
    // Every follow-up is of a key it did not request: only the key-independent checks run.
    unsettled(bulk(&[], result(&[], vec![]), vec![probe("a", just(200))]));
    unsettled(bulk(&[], result(&[], vec![]), vec![probe("a", just(404))]));
    fails(
        bulk(&[], result(&[], vec![]), vec![probe("a", just(503))]),
        Protocol,
    );
    fails(
        bulk(
            &[],
            result(&[], vec![]),
            vec![probe("a", body(404, bytes(b"x")))],
        ),
        Protocol,
    );
}

#[test]
fn a_delete_naming_a_key_twice_runs_every_check_but_the_exact_match() {
    let twice = ["a", "a"];
    let gone = || vec![probe("a", just(404))];
    // <Deleted> cannot match it either way: no shape of it is wrong, none is a pass.
    for deleted in [["a"].as_slice(), &["a", "a"], &[], &["z"]] {
        unsettled(bulk(&twice, result(deleted, vec![]), gone()));
    }
    // Every other check runs.
    fails(
        bulk(&twice, result(&["a"], vec![failure("a")]), gone()),
        Protocol,
    );
    fails(bulk(&twice, just(400), gone()), Protocol);
    fails(bulk(&twice, just(500), gone()), Protocol);
    fails(bulk(&twice, body(200, bad()), gone()), Protocol);
    fails(
        bulk(&twice, result(&["a"], vec![]), vec![probe("a", just(200))]),
        Oracle,
    );
    fails(
        bulk(&twice, result(&["a"], vec![]), vec![probe("a", just(503))]),
        Protocol,
    );
    unsettled(bulk(&twice, body(200, cutn(4)), gone()));
    unsettled(bulk(&twice, result(&["a"], vec![]), vec![]));
}

#[test]
fn a_listing_of_a_prefix_with_nothing_written_never_passes() {
    let ask = |answer| Exercise::ListObjectsV2 {
        expected: vec![],
        answer,
    };
    unsettled(ask(listing_of(vec![])));
    unsettled(ask(body(200, cutn(4))));
    fails(ask(listing_of(vec![listed("p/ghost", 1)])), Oracle);
    fails(ask(just(200)), Protocol);
    fails(ask(body(200, bad())), Protocol);
    fails(ask(just(404)), Protocol);
    fails(ask(just(500)), Protocol);
}
