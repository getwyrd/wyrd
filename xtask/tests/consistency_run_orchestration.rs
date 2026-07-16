//! Unit tests for the checked-consistency-run **orchestration** (`xtask::consistency_run` —
//! the Check-time flippable coverage, #408, slice 5 of #329; ADR-0041).
//!
//! **Why these tests exist.** The live checked run stands up the ≥3-process
//! `deploy/fdb-multi-replica` cluster, drives the #406 workload under a #407 nemesis leg, and
//! shells `java -jar elle-cli.jar` — none of which the unprivileged `cargo xtask ci` gate may
//! touch (ADR-0041's own MUST: no JVM in `cargo xtask ci`). "Deferred" means the *live green
//! run* is off-Check (`WYRD_TIER1=1`); it does NOT mean the pipeline is unbuilt. This file
//! exercises, at Check, exactly what the brief's Success criterion names: the run-orchestration
//! plan, the summary-based inconclusive gate (Design §4 — "non-vacuity is a gate, not a note"),
//! the pinned elle-cli invocation building (Design §3 — `rw-register` / `set`), the three-valued
//! verdict-token parser against **committed real-elle-cli-captured checker outputs** (golden
//! files, `xtask/tests/fixtures/consistency-run/`), the golden EDN vocabulary those same fixtures
//! pin (the txn / set shapes verified at Plan), the report renderer (the five fields the issue
//! names), the runner's opted-in-but-missing-environment error paths (never a silent skip), and
//! the fidelity delta (T2) — that the **#407 typed materialization evidence** is carried through
//! the run summary and into the report, never collapsed to a bare boolean.
//!
//! Everything imported here is default-compiled: no `wyrd-server`, no FDB, no Docker, no Java,
//! no elle-cli in the build graph (the test-graph constraint, Verification posture) — the module
//! is `pub` and wired from `xtask/src/lib.rs`, mirroring the `metadata_faults` /
//! `nemesis_orchestration` precedent. These tests run inside `cargo xtask ci`'s `cargo test
//! --workspace` and go **RED** when the orchestration, the vacuity gate, the invocation builder,
//! the parser, the fixtures, the evidence fidelity, or the report renderer are regressed/removed.

use xtask::consistency_run::{
    consistency_run_scenario_args, edn_history_has_expected_vocabulary, elle_invocation,
    elle_version_extraction, evaluate_summary, parse_checker_output, parse_elle_version,
    parse_run_summary, preflight, render_report, run_plan, selected_leg, self_check_matches,
    wyrd_check_violations, CheckOutcome, CheckerIdentity, DeletePoolChecks, DeletePoolSummary,
    Environment, InconclusiveReason, MemberId, NemesisEvidence, OutcomeCounts, ReportInputs,
    RunSummary, SelfCheckExpectation, CONSISTENCY_RUN_SCENARIO_TEST, ELLE_VERSION_JAR_ENTRY,
    MODEL_DIRECTORY_SET, MODEL_REGISTER,
};
use xtask::nemesis::NemesisLegKind;

/// A fully-present opted-in environment; individual pieces are knocked out per test.
fn full_environment() -> Environment {
    Environment {
        docker: true,
        java: true,
        jar_present: true,
        unzip: true,
    }
}

// ─── Golden fixtures — REAL elle-cli-accepted samples (Design §3/§5) ──────────────────

const REGISTER_KNOWN_GOOD_EDN: &str =
    include_str!("fixtures/consistency-run/register-history-known-good.edn");
const REGISTER_KNOWN_BAD_EDN: &str =
    include_str!("fixtures/consistency-run/register-history-known-bad.edn");
const DIRECTORY_KNOWN_GOOD_EDN: &str =
    include_str!("fixtures/consistency-run/directory-history-known-good.edn");
const DIRECTORY_KNOWN_BAD_EDN: &str =
    include_str!("fixtures/consistency-run/directory-history-known-bad.edn");
const CHECKER_OUTPUT_PASS: &str = include_str!("fixtures/consistency-run/checker-output-pass.txt");
const CHECKER_OUTPUT_FAIL: &str = include_str!("fixtures/consistency-run/checker-output-fail.txt");
const CHECKER_OUTPUT_UNKNOWN: &str =
    include_str!("fixtures/consistency-run/checker-output-unknown.txt");
/// The degrade path's real sample: a `set` history whose composed final read is `:info` because
/// the post-heal sweep could not resolve every member — exactly what
/// `DirectoryHistory::to_elle_edn` emits for an indeterminate sweep. Committed WITH the real
/// checker's captured answer so the degrade's safety is an artifact, not a comment.
const DIRECTORY_INDETERMINATE_FINAL_READ_EDN: &str =
    include_str!("fixtures/consistency-run/directory-history-indeterminate-final-read.edn");
const CHECKER_OUTPUT_INDETERMINATE_FINAL_READ: &str =
    include_str!("fixtures/consistency-run/checker-output-indeterminate-final-read.txt");

// ─── (1) The run-orchestration plan ────────────────────────────────────────────────────

#[test]
fn run_plan_carries_bring_up_through_report_in_order() {
    let plan = run_plan();
    let want = [
        "bring-up",
        "workload",
        "nemesis-window",
        "heal",
        "export",
        "check",
        "report",
    ];
    assert_eq!(
        plan, want,
        "a stage dropped or reordered here is a pipeline step the runner silently stopped \
         performing (or now performs out of order): {plan:?}"
    );
}

#[test]
fn scenario_argv_targets_wyrd_server_with_the_fdb_feature_and_ignored_flag() {
    let args = consistency_run_scenario_args(CONSISTENCY_RUN_SCENARIO_TEST);
    assert_eq!(
        args[0], "test",
        "must be a `cargo test` invocation: {args:?}"
    );
    let flat = args.join(" ");
    assert!(
        flat.contains("wyrd-server"),
        "the live scenario lives in crates/server/tests/ (Design §1): {flat}"
    );
    assert!(
        flat.contains("--features fdb"),
        "the scenario drives the real FdbMetadataStore, so the argv MUST enable the fdb \
         feature (off by default): {flat}"
    );
    assert!(
        flat.contains(CONSISTENCY_RUN_SCENARIO_TEST) && flat.contains("--ignored"),
        "must run the #[ignore]d {CONSISTENCY_RUN_SCENARIO_TEST} scenario: {flat}"
    );
}

// ─── (2) Nemesis-leg selection: partition first, skew/pause selectable ────────────────

#[test]
fn the_default_leg_is_partition_never_a_silent_no_fault() {
    assert_eq!(
        selected_leg(None).expect("default"),
        NemesisLegKind::NetworkPartition,
        "Design §2 pins partition as the default leg"
    );
    assert_eq!(
        selected_leg(Some("partition")).expect("explicit partition"),
        NemesisLegKind::NetworkPartition
    );
}

#[test]
fn skew_and_pause_are_selectable_by_name() {
    assert_eq!(
        selected_leg(Some("clock-skew")).expect("clock-skew"),
        NemesisLegKind::ClockSkew
    );
    assert_eq!(
        selected_leg(Some("process-pause")).expect("process-pause"),
        NemesisLegKind::ProcessPause
    );
}

#[test]
fn an_unrecognized_leg_name_is_a_hard_error_not_a_silent_fallback() {
    let err = selected_leg(Some("typo'd-leg")).unwrap_err();
    assert!(
        err.contains("typo'd-leg"),
        "the error must name the bad value so a typo is diagnosable: {err}"
    );
}

// ─── (3) The summary-based inconclusive gate (Design §4) ──────────────────────────────

fn attesting_evidence() -> NemesisEvidence {
    NemesisEvidence {
        kind: NemesisLegKind::NetworkPartition.as_str().to_string(),
        target: "fdb0 (wyrd-consistency-run-fdb0-1 @ 172.30.58.11:4500)".into(),
        materialized: true,
        diagnosis: "peers_saw_target before=true during=false (must flip true→false), \
                    target_running_during=true (must be true — a crash is not a partition)"
            .into(),
    }
}

fn all_checks_held() -> DeletePoolChecks {
    DeletePoolChecks {
        session_read_your_writes: true,
        session_monotonic_reads: true,
        reads_monotone_per_key: true,
    }
}

fn attesting_summary() -> RunSummary {
    RunSummary {
        workload: "register overwrite PUT/GET (Elle-fed) + register delete pool (Wyrd) + \
                   directory create/read (set)"
            .into(),
        nemesis: attesting_evidence(),
        genuinely_concurrent: true,
        register_ops: 320,
        directory_ops: 80,
        register_outcomes: OutcomeCounts {
            invoked: 160,
            ok: 150,
            fail: 4,
            info: 6,
        },
        directory_outcomes: OutcomeCounts {
            invoked: 40,
            ok: 38,
            fail: 0,
            info: 2,
        },
        delete_pool: DeletePoolSummary {
            ops: 240,
            outcomes: OutcomeCounts {
                invoked: 240,
                ok: 230,
                fail: 2,
                info: 8,
            },
            checks: all_checks_held(),
        },
        member_id_map: vec![
            MemberId {
                member: "dir/member-1".into(),
                id: 1,
            },
            MemberId {
                member: "dir/member-61".into(),
                id: 61,
            },
        ],
        composed_final_read: vec![1, 61],
        composed_final_read_determinate: true,
        composed_final_read_unresolved: Vec::new(),
    }
}

#[test]
fn a_summary_attesting_both_witnesses_evaluates_conclusive() {
    assert!(evaluate_summary(&attesting_summary()).is_ok());
}

#[test]
fn a_summary_missing_the_inv2_witness_is_inconclusive() {
    let mut summary = attesting_summary();
    summary.genuinely_concurrent = false;
    assert_eq!(
        evaluate_summary(&summary),
        Err(InconclusiveReason::NotGenuinelyConcurrent),
        "a vacuous history (no genuine concurrency) must NEVER reach a verdict — the #250 \
         failure mode this issue exists to bury"
    );
}

#[test]
fn a_summary_whose_typed_evidence_did_not_materialize_is_inconclusive() {
    // T2: the gate reads the leg's OWN typed evidence (`nemesis.materialized`), not a bare
    // boolean the scenario asserted from `drive_leg`'s contract. An evidence record whose
    // oracle says the fault did not bite must block the verdict.
    let mut summary = attesting_summary();
    summary.nemesis.materialized = false;
    assert_eq!(
        evaluate_summary(&summary),
        Err(InconclusiveReason::NemesisNotMaterialized),
        "a nemesis whose typed evidence says it never bit must NEVER reach a verdict — a note \
         is not a gate (#442)"
    );
}

#[test]
fn a_summary_whose_composed_final_read_is_indeterminate_is_inconclusive_and_names_the_members() {
    // The post-heal sweep could not resolve every member, so the scenario emitted the composed
    // full-set read as `:info` and the `set` model has no definite final read to judge. The
    // alternative — dropping the unresolved members from a definite `:ok` read — would state they
    // are ABSENT, which in the set model is a lost element: a fabricated `false` on a correct run.
    //
    // The real checker refuses this history on its own (verified against elle-cli 0.1.9: an `:info`
    // final read ⇒ `:unknown` ⇒ the pinned parser's INCONCLUSIVE), so the gate cannot be what makes
    // the run safe. What it adds is the diagnosis: WHICH members were unresolved, rather than a
    // bare `:unknown` for an operator to reverse-engineer.
    let mut summary = attesting_summary();
    summary.composed_final_read_determinate = false;
    summary.composed_final_read_unresolved = vec![61];
    assert_eq!(
        evaluate_summary(&summary),
        Err(InconclusiveReason::FinalReadIndeterminate),
        "an indeterminate composed final read must block the verdict, never be rounded to the \
         partial set that was observed"
    );
    let message = InconclusiveReason::FinalReadIndeterminate.message();
    assert!(
        message.contains("composed full-set read") && message.contains(":info"),
        "the inconclusive reason must say what was indeterminate: {message}"
    );
}

#[test]
fn the_run_summary_is_parsed_from_json_carrying_the_typed_evidence_and_outcome_counts() {
    // The seam contract: the live scenario writes this JSON (with the nested `nemesis` typed
    // evidence object and the per-pool outcome counts), and the runner deserializes it. A summary
    // that only carried a boolean would fail to round-trip the leg's diagnosis — this pins the
    // seam T2 and the Design §4 richer-evidence schema require.
    let summary = parse_run_summary(&scenario_shaped_summary_json()).expect("valid summary parses");
    assert!(evaluate_summary(&summary).is_ok());
    assert_eq!(summary.nemesis.kind, "partition");
    assert_eq!(summary.register_outcomes.ok, 5);
    assert!(
        summary.nemesis.diagnosis.contains("during=false"),
        "the summary must round-trip the leg's OWN diagnosis, not collapse it to a boolean: {:?}",
        summary.nemesis
    );

    let malformed = parse_run_summary("{ not json");
    assert!(
        malformed.is_err(),
        "malformed JSON must be a hard parse error, never a default-filled (accidentally \
         conclusive) summary"
    );

    // A summary missing the nested evidence entirely must be a hard parse error — it can NEVER
    // silently default to a materialized fault.
    let no_evidence =
        r#"{"workload":"w","genuinely_concurrent":true,"register_ops":1,"directory_ops":1}"#;
    assert!(
        parse_run_summary(no_evidence).is_err(),
        "a summary lacking the #407 typed evidence must not parse to an accidentally-conclusive \
         default"
    );
}

/// A run summary in exactly the shape the live scenario emits
/// (`crates/server/tests/consistency_run_fdb.rs`) — every field it writes, none this side invents.
/// The seam is only pinned if the JSON here is the JSON the scenario actually produces.
fn scenario_shaped_summary_json() -> String {
    r#"{
        "workload": "w",
        "nemesis": {
            "kind": "partition",
            "target": "fdb0 (wyrd-consistency-run-fdb0-1 @ 172.30.58.11:4500)",
            "materialized": true,
            "diagnosis": "peers_saw_target before=true during=false; target_running_during=true"
        },
        "genuinely_concurrent": true,
        "register_ops": 10,
        "directory_ops": 2,
        "register_outcomes": {"invoked": 5, "ok": 5, "fail": 0, "info": 0},
        "directory_outcomes": {"invoked": 1, "ok": 1, "fail": 0, "info": 0},
        "delete_pool": {
            "ops": 8,
            "outcomes": {"invoked": 8, "ok": 6, "fail": 0, "info": 2},
            "checks": {
                "session_read_your_writes": true,
                "session_monotonic_reads": true,
                "reads_monotone_per_key": true
            }
        },
        "member_id_map": [
            {"member": "dir/member-1", "id": 1},
            {"member": "dir/member-61", "id": 61}
        ],
        "composed_final_read": [1, 61],
        "composed_final_read_determinate": true,
        "composed_final_read_unresolved": []
    }"#
    .to_string()
}

#[test]
fn the_member_id_map_crosses_the_seam_rather_than_being_dropped_on_the_floor() {
    // The scenario has always emitted `member_id_map`; the runner used to parse a summary struct
    // that had no such field, so serde discarded it silently and the report could never show it.
    // This pins the map through the seam it must survive.
    let summary = parse_run_summary(&scenario_shaped_summary_json()).expect("parses");
    assert_eq!(
        summary.member_id_map,
        vec![
            MemberId {
                member: "dir/member-1".into(),
                id: 1
            },
            MemberId {
                member: "dir/member-61".into(),
                id: 61
            },
        ],
        "the name↔id map the scenario emitted must arrive intact on the runner side"
    );
    assert_eq!(
        summary.composed_final_read,
        vec![1, 61],
        "the composed post-heal final read must cross the seam too"
    );
}

#[test]
fn a_field_the_scenario_emits_but_the_seam_does_not_name_is_a_hard_error() {
    // Why `deny_unknown_fields` is load-bearing: serde's default is to ignore unknown fields, which
    // is silent data loss in the one direction that matters — the scenario emits something the
    // report then never shows, and nothing fails. That is exactly how `member_id_map` was dropped
    // for three iterations. The seam must break loudly when the two sides drift apart.
    let with_unknown = scenario_shaped_summary_json().replace(
        r#""composed_final_read": [1, 61]"#,
        r#""composed_final_read": [1, 61], "some_new_evidence": {"a": 1}"#,
    );
    let err = parse_run_summary(&with_unknown).unwrap_err();
    assert!(
        err.contains("some_new_evidence"),
        "an unknown summary field must name itself in a hard parse error, never be discarded: \
         {err}"
    );
}

#[test]
fn an_unknown_field_nested_inside_the_seam_is_a_hard_error_too() {
    // The test above pins only the TOP level, which is exactly as far as serde's
    // `deny_unknown_fields` reaches: the attribute is per-struct and is NOT inherited by nested
    // types. So a `RunSummary` that denies unknown fields still silently swallowed a field added
    // inside `nemesis` or any `outcomes` object — the same class of loss `member_id_map` suffered,
    // one level down, with the claim "the seam fails loudly" written over it. Every nested seam
    // object must deny too, and each is pinned here rather than trusted.
    let cases = [
        // `nemesis` (NemesisEvidence) — where a leg's new typed evidence would land.
        (
            r#""diagnosis": "peers_saw_target before=true during=false; target_running_during=true""#,
            r#""diagnosis": "d", "confirmed_at": "2026-07-16T00:00:00Z""#,
            "confirmed_at",
        ),
        // `register_outcomes` (OutcomeCounts) — where a new op-kind count would land.
        (
            r#""register_outcomes": {"invoked": 5, "ok": 5, "fail": 0, "info": 0}"#,
            r#""register_outcomes": {"invoked": 5, "ok": 5, "fail": 0, "info": 0, "timeout": 3}"#,
            "timeout",
        ),
        // `delete_pool.outcomes` — the same struct nested three deep.
        (
            r#""outcomes": {"invoked": 8, "ok": 6, "fail": 0, "info": 2}"#,
            r#""outcomes": {"invoked": 8, "ok": 6, "fail": 0, "info": 2, "skipped": 1}"#,
            "skipped",
        ),
    ];

    for (from, to, field) in cases {
        let json = scenario_shaped_summary_json().replace(from, to);
        assert_ne!(
            json,
            scenario_shaped_summary_json(),
            "the nested-field test must actually modify the summary JSON (the `{field}` case's \
             anchor no longer matches the scenario-shaped fixture)"
        );
        let err = parse_run_summary(&json).unwrap_err();
        assert!(
            err.contains(field),
            "an unknown field nested inside the seam must name itself in a hard parse error, \
             never be silently dropped (`{field}`): {err}"
        );
    }
}

// ─── (3b) The Wyrd-checked delete pool's verdict (Design §2) ──────────────────────────

#[test]
fn a_delete_pool_whose_checks_held_reports_no_violation() {
    assert!(
        wyrd_check_violations(&attesting_summary()).is_empty(),
        "a clean delete pool must report no violation"
    );
}

#[test]
fn each_violated_wyrd_check_is_named_so_the_run_can_fail_on_it() {
    // The delete pool exists because no Elle model can represent a delete — so the #406 checks are
    // its only judge. A pool that is driven and then not acted on would be decorative, and a report
    // listing delete traffic nobody judged overstates exactly what this issue exists to stop.
    for (mutate, expected) in [
        (
            (|c: &mut DeletePoolChecks| c.session_read_your_writes = false)
                as fn(&mut DeletePoolChecks),
            "session_read_your_writes",
        ),
        (
            |c: &mut DeletePoolChecks| c.session_monotonic_reads = false,
            "session_monotonic_reads",
        ),
        (
            |c: &mut DeletePoolChecks| c.reads_monotone_per_key = false,
            "reads_monotone_per_key",
        ),
    ] {
        let mut summary = attesting_summary();
        mutate(&mut summary.delete_pool.checks);
        assert_eq!(
            wyrd_check_violations(&summary),
            vec![expected],
            "a violated `{expected}` must be named as a run violation"
        );
    }

    // Every check violated at once — all are named, not just the first.
    let mut summary = attesting_summary();
    summary.delete_pool.checks = DeletePoolChecks {
        session_read_your_writes: false,
        session_monotonic_reads: false,
        reads_monotone_per_key: false,
    };
    assert_eq!(wyrd_check_violations(&summary).len(), 3);
}

#[test]
fn the_delete_pools_violation_is_independent_of_the_vacuity_gate() {
    // A violated Wyrd check is a run FAILURE, not an inconclusive: the run learned something
    // definite and bad. The vacuity gate must stay orthogonal to it.
    let mut summary = attesting_summary();
    summary.delete_pool.checks.reads_monotone_per_key = false;
    assert!(
        evaluate_summary(&summary).is_ok(),
        "a Wyrd-check violation is a FAILURE, not a reason to call the run inconclusive"
    );
    assert!(!wyrd_check_violations(&summary).is_empty());
}

// ─── (3c) The checker's identity — version, not just a hash (Design §3/§6) ────────────

#[test]
fn the_version_is_read_from_the_jars_own_metadata_entry() {
    let argv = elle_version_extraction("/opt/elle-cli-0.1.9-standalone.jar");
    assert_eq!(
        argv,
        vec![
            "-p".to_string(),
            "/opt/elle-cli-0.1.9-standalone.jar".to_string(),
            ELLE_VERSION_JAR_ENTRY.to_string(),
        ],
        "the version must be read out of the jar actually invoked — not from its filename, which \
         a rename would make lie"
    );
}

#[test]
fn the_real_jars_pom_properties_parses_to_its_version_and_revision() {
    // Captured verbatim from the real elle-cli 0.1.9 standalone jar
    // (`unzip -p <jar> META-INF/maven/elle-cli/elle-cli/pom.properties`) — the same bytes the
    // runner parses off the jar the witnessed run invokes.
    let properties = "#Leiningen\n\
         #Wed Aug 27 18:29:41 UTC 2025\n\
         groupId=elle-cli\n\
         artifactId=elle-cli\n\
         version=0.1.9\n\
         revision=6d4afc4c5f794e8cb038bb33de465f66cb21f3a4\n";
    let (version, revision) = parse_elle_version(properties).expect("the real metadata parses");
    assert_eq!(version, "0.1.9");
    assert_eq!(
        revision.as_deref(),
        Some("6d4afc4c5f794e8cb038bb33de465f66cb21f3a4")
    );
}

#[test]
fn metadata_without_a_version_is_a_hard_error_never_an_unknown_placeholder() {
    // A report naming an unidentifiable checker is not a credibility artifact — so this fails the
    // run rather than rendering "version: unknown".
    let err = parse_elle_version("groupId=elle-cli\nartifactId=elle-cli\n").unwrap_err();
    assert!(err.contains("version"), "{err}");

    // A commented-out version is not a version.
    assert!(parse_elle_version("#version=0.1.9\n").is_err());
    // An empty value is not a version.
    assert!(parse_elle_version("version=\n").is_err());
}

#[test]
fn a_jar_without_a_recorded_revision_still_identifies_by_version_and_hash() {
    let (version, revision) = parse_elle_version("version=0.2.0\n").expect("parses");
    assert_eq!(version, "0.2.0");
    assert_eq!(revision, None);
    let described = CheckerIdentity {
        version,
        revision,
        jar_sha256: "abc123".into(),
    }
    .describe();
    assert!(described.contains("elle-cli 0.2.0") && described.contains("jar sha256=abc123"));
    assert!(
        !described.contains("revision"),
        "an absent revision must not render an empty `revision ,` artifact: {described}"
    );
}

// ─── (4) The pinned elle-cli invocation (Design §3) ────────────────────────────────────

#[test]
fn the_register_invocation_uses_the_rw_register_model() {
    let argv = elle_invocation("elle-cli.jar", MODEL_REGISTER, "register-history.edn");
    assert_eq!(
        argv,
        vec![
            "-jar".to_string(),
            "elle-cli.jar".to_string(),
            "--model".to_string(),
            "rw-register".to_string(),
            "register-history.edn".to_string(),
        ]
    );
}

#[test]
fn the_directory_invocation_uses_the_set_model_not_set_full() {
    // Design §3: the directory model is `set` (not v2's falsified `set-full`).
    assert_eq!(MODEL_DIRECTORY_SET, "set");
    let argv = elle_invocation("elle-cli.jar", MODEL_DIRECTORY_SET, "directory-history.edn");
    assert!(argv.contains(&"set".to_string()));
    assert!(!argv.contains(&"set-full".to_string()));
    assert!(argv.contains(&"directory-history.edn".to_string()));
}

// ─── (5) The three-valued verdict parser against committed real checker outputs ───────

#[test]
fn a_captured_pass_output_parses_to_pass() {
    let outcome = parse_checker_output(CHECKER_OUTPUT_PASS, true);
    assert_eq!(outcome, CheckOutcome::Pass);
}

#[test]
fn a_captured_fail_output_parses_to_a_genuine_violation() {
    // The captured fixture line is `<file>\tfalse`; the parser must read the trailing token as a
    // genuine violation (Design §3: false is ALWAYS a run FAILURE), keyed on the token.
    let outcome = parse_checker_output(CHECKER_OUTPUT_FAIL, false);
    assert!(!outcome.is_pass());
    assert!(
        outcome.is_violation(),
        "a `false` token must be a genuine Violation, not merely inconclusive: {outcome:?}"
    );
}

#[test]
fn a_captured_unknown_output_parses_to_inconclusive_even_with_exit_zero() {
    // The captured `:unknown` fixture exits 0 (verified at Plan). The parser MUST NOT read exit 0
    // as a pass — it keys on the token, so `:unknown` is inconclusive, never a silent pass.
    let outcome = parse_checker_output(CHECKER_OUTPUT_UNKNOWN, true);
    assert!(
        !outcome.is_pass(),
        "an :unknown checker verdict must NEVER read as a pass, even at exit 0"
    );
    assert!(
        matches!(outcome, CheckOutcome::Inconclusive(_)),
        "an :unknown verdict is inconclusive, never a violation and never a pass: {outcome:?}"
    );
}

#[test]
fn the_degraded_composed_read_is_checker_verified_to_be_inconclusive_not_a_vacuous_pass() {
    // The load-bearing premise of the whole degrade design (Design §2, the composed final read):
    // when the sweep cannot resolve a member, the scenario emits the composed read as `:info`
    // rather than dropping the member from a definite `:ok` set (which the `set` model reads as a
    // lost element ⇒ a fabricated `false`). That is only safe if `:info` does not instead read as a
    // VACUOUS PASS — a set history nobody can refute.
    //
    // The committed fixture is that exact history and the committed output is what the REAL
    // elle-cli 0.1.9 answered for it: `:unknown`, at exit 0. So the degrade lands in INCONCLUSIVE
    // by the checker's own judgment, and the "exit 0 lies" trap is what makes the token-keyed
    // parser necessary here rather than merely tidy.
    assert!(
        DIRECTORY_INDETERMINATE_FINAL_READ_EDN.contains(":type :info, :f :read, :value nil"),
        "the fixture must BE the degraded shape the serializer emits: \
         {DIRECTORY_INDETERMINATE_FINAL_READ_EDN}"
    );
    assert!(
        !DIRECTORY_INDETERMINATE_FINAL_READ_EDN.contains(":type :ok, :f :read"),
        "the degraded shape must carry no definite read at all"
    );
    assert!(
        CHECKER_OUTPUT_INDETERMINATE_FINAL_READ.contains(":unknown"),
        "the captured real-checker answer for the degraded shape must be `:unknown`: \
         {CHECKER_OUTPUT_INDETERMINATE_FINAL_READ}"
    );

    // exit 0 — the trap. Parse it as the runner does (`output.status.success()` = true).
    let outcome = parse_checker_output(CHECKER_OUTPUT_INDETERMINATE_FINAL_READ, true);
    assert!(
        !outcome.is_pass(),
        "an indeterminate composed read must never yield a PASS: a directory verdict claimed \
         over a set nobody could read is precisely the overstatement this run refuses"
    );
    assert!(
        !outcome.is_violation(),
        "…and it is not a violation either — the run learned nothing, it did not learn something \
         bad: {outcome:?}"
    );
    assert!(matches!(outcome, CheckOutcome::Inconclusive(_)));
}

#[test]
fn a_true_token_contradicted_by_a_nonzero_exit_is_inconclusive_never_a_pass() {
    let outcome = parse_checker_output("history.edn \t true", false);
    assert!(
        !outcome.is_pass(),
        "a `true` token with a non-zero exit disagrees with itself — inconclusive, never a pass"
    );
}

// ─── (6) The fixtures self-check comparison (Design §5, the off-Check half's pure core) ──

#[test]
fn the_self_check_confirms_known_good_parses_pass_and_known_bad_parses_violation() {
    use SelfCheckExpectation::{Inconclusive, Pass, Violation};

    let good_outcome = parse_checker_output(CHECKER_OUTPUT_PASS, true);
    let bad_outcome = parse_checker_output(CHECKER_OUTPUT_FAIL, false);
    assert!(
        self_check_matches(Pass, &good_outcome),
        "the known-good fixture's checker output must parse as a pass"
    );
    assert!(
        self_check_matches(Violation, &bad_outcome),
        "the known-bad fixture's checker output must parse as a genuine violation"
    );
    // The cross products must NOT match — otherwise the self-check could never catch a parser
    // regression that flips pass/fail.
    assert!(!self_check_matches(Violation, &good_outcome));
    assert!(!self_check_matches(Pass, &bad_outcome));
    // An :unknown must satisfy NEITHER definite polarity — it is not a caught violation and not a
    // pass.
    let unknown = parse_checker_output(CHECKER_OUTPUT_UNKNOWN, true);
    assert!(!self_check_matches(Pass, &unknown));
    assert!(
        !self_check_matches(Violation, &unknown),
        "an :unknown must not count as the known-bad fixture's caught violation — the self-check \
         demands a genuine `false`, not merely `not a pass`"
    );

    // The degrade path's polarity (Design §2/§5): the `:info`-composed-read fixture must come back
    // WITHOUT a verdict. A checker build that blessed it as a pass would make every degraded run
    // silently green, so the self-check refuses that build before any verdict is trusted.
    let degraded = parse_checker_output(CHECKER_OUTPUT_INDETERMINATE_FINAL_READ, true);
    assert!(
        self_check_matches(Inconclusive, &degraded),
        "the degraded composed-read fixture must self-check as inconclusive: {degraded:?}"
    );
    assert!(
        !self_check_matches(Pass, &degraded),
        "…and must never satisfy the pass polarity — that is the vacuous-pass regression this \
         fixture exists to catch"
    );
    // A definite verdict must NOT satisfy the inconclusive polarity either, or the new expectation
    // would be satisfied by anything and catch nothing.
    assert!(!self_check_matches(Inconclusive, &good_outcome));
    assert!(!self_check_matches(Inconclusive, &bad_outcome));
}

// ─── (7) The golden EDN vocabulary (Design §3/§5) ──────────────────────────────────────

#[test]
fn the_known_good_and_known_bad_register_histories_carry_the_txn_vocabulary() {
    assert!(
        edn_history_has_expected_vocabulary(REGISTER_KNOWN_GOOD_EDN),
        "known-good register history must carry the rw-register txn vocabulary \
         (:process/:type/:f/:txn/:value/:time + a [[:w/[[:r micro-op): {REGISTER_KNOWN_GOOD_EDN}"
    );
    assert!(
        edn_history_has_expected_vocabulary(REGISTER_KNOWN_BAD_EDN),
        "known-bad register history must carry the same vocabulary — it is a bad HISTORY, not a \
         malformed one: {REGISTER_KNOWN_BAD_EDN}"
    );
    // The rejected #406 scalar `:value` shape (a bare integer, no micro-op) must fail the pin.
    let scalar = REGISTER_KNOWN_GOOD_EDN
        .replace("[[:w \"k\" 1]]", "1")
        .replace("[[:r \"k\" 1]]", "1")
        .replace("[[:r \"k\" nil]]", "nil")
        .replace("[[:w \"k\" 2]]", "2")
        .replace("[[:r \"k\" 2]]", "2");
    assert!(
        !edn_history_has_expected_vocabulary(&scalar),
        "a scalar-:value history (the #406 shape the real checker rejected) must fail the pin"
    );
}

#[test]
fn the_directory_fixtures_use_the_set_vocabulary_with_integer_elements_only() {
    for edn in [DIRECTORY_KNOWN_GOOD_EDN, DIRECTORY_KNOWN_BAD_EDN] {
        assert!(
            edn.contains(":f :add") && edn.contains(":f :read"),
            "the set fixtures must use :add/:read: {edn}"
        );
        assert!(
            !edn.contains(":remove") && !edn.contains(":contains"),
            "the set fixtures must never carry the checker-rejected :remove/:contains: {edn}"
        );
        assert!(
            edn.contains("#{"),
            "the composed final read must be an integer-set literal #{{...}}: {edn}"
        );
    }
}

#[test]
fn a_missing_field_fails_the_vocabulary_pin() {
    let missing_time = REGISTER_KNOWN_GOOD_EDN.replace(":time", ":nope");
    assert!(
        !edn_history_has_expected_vocabulary(&missing_time),
        "the vocabulary pin must actually be load-bearing, not a tautology"
    );
}

// ─── (8) The opted-in-but-missing-environment error paths (never a silent skip) ───────

#[test]
fn not_opted_in_is_always_ok_deferred_not_an_error() {
    assert!(preflight(
        false,
        Environment {
            docker: false,
            java: false,
            jar_present: false,
            unzip: false,
        }
    )
    .is_ok());
}

#[test]
fn opted_in_with_a_full_environment_is_ok() {
    assert!(preflight(true, full_environment()).is_ok());
}

#[test]
fn opted_in_but_missing_any_piece_is_a_hard_error_naming_it() {
    let err = preflight(
        true,
        Environment {
            docker: false,
            ..full_environment()
        },
    )
    .unwrap_err();
    assert!(err.contains("docker"), "must name the missing piece: {err}");

    let err = preflight(
        true,
        Environment {
            java: false,
            ..full_environment()
        },
    )
    .unwrap_err();
    assert!(err.contains("java"), "must name the missing piece: {err}");

    let err = preflight(
        true,
        Environment {
            jar_present: false,
            ..full_environment()
        },
    )
    .unwrap_err();
    assert!(
        err.contains("elle-cli") || err.contains("WYRD_ELLE_CLI_JAR"),
        "must name the missing piece: {err}"
    );

    // The report must name the checker that produced the verdict (Design §6), and the version
    // comes out of the jar — so the tool that reads it is part of the opted-in environment, not
    // an optional nicety discovered after a full nemesis window has already run.
    let err = preflight(
        true,
        Environment {
            unzip: false,
            ..full_environment()
        },
    )
    .unwrap_err();
    assert!(err.contains("unzip"), "must name the missing piece: {err}");
}

#[test]
fn opted_in_with_nothing_present_names_every_missing_piece() {
    let err = preflight(
        true,
        Environment {
            docker: false,
            java: false,
            jar_present: false,
            unzip: false,
        },
    )
    .unwrap_err();
    assert!(
        err.contains("docker")
            && err.contains("java")
            && err.contains("elle-cli")
            && err.contains("unzip")
    );
}

// ─── (9) The report renderer — the five fields, with typed evidence fidelity (T2) ─────

fn sample_report_inputs() -> ReportInputs {
    ReportInputs {
        workload: "register overwrite PUT/GET (Elle-fed) + directory create/read (set)".into(),
        nemesis: ReportInputs::nemesis_field(&attesting_evidence()),
        history_size: "register: 320 ops; directory: 80 ops".into(),
        model: "rw-register (register), set (directory)".into(),
        checker: CheckerIdentity {
            version: "0.1.9".into(),
            revision: Some("6d4afc4c5f794e8cb038bb33de465f66cb21f3a4".into()),
            jar_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        }
        .describe(),
        member_id_map: ReportInputs::member_id_map_field(&attesting_summary().member_id_map),
        verdict: "PASS".into(),
    }
}

#[test]
fn the_report_renders_all_five_named_fields() {
    let report = render_report(&sample_report_inputs());
    for needle in [
        "register overwrite",
        "partition",
        "320 ops",
        "rw-register",
        "PASS",
    ] {
        assert!(
            report.contains(needle),
            "report must render `{needle}`: {report}"
        );
    }
    assert!(
        report.contains("set (directory)"),
        "the report must name the `set` model (not set-full): {report}"
    );
}

#[test]
fn the_report_names_the_checker_version_not_only_the_jar_hash() {
    // Design §6: the report is the credibility artifact — an outsider must be able to obtain the
    // SAME checker and re-run the verdict. A bare sha256 identifies bytes nobody can look up, so
    // the version string is load-bearing, not decoration.
    let report = render_report(&sample_report_inputs());
    assert!(
        report.contains("elle-cli 0.1.9"),
        "the report must name the checker AND its version: {report}"
    );
    assert!(
        report.contains("jar sha256=e3b0c442"),
        "the report must still pin the exact jar bytes: {report}"
    );
    assert!(
        report.contains("revision 6d4afc4c"),
        "when the jar records the upstream revision it must be rendered — it names the checker's \
         source, the strongest identification available: {report}"
    );
}

#[test]
fn the_report_resolves_the_set_models_integer_elements_via_the_member_id_map() {
    // The `set` checker takes integer elements only, so the wire object names never enter the EDN
    // (Design §2). Without the map crossing the seam into the report, the committed history's
    // elements resolve to nothing and the artifact cannot be tied back to what the run created.
    let report = render_report(&sample_report_inputs());
    assert!(
        report.contains("Member-id map"),
        "the report must carry the member-id map field: {report}"
    );
    assert!(
        report.contains("61 -> `dir/member-61`"),
        "each integer element must resolve to the object the run created: {report}"
    );
}

#[test]
fn the_member_id_map_field_renders_every_pair_and_says_so_when_empty() {
    let field = ReportInputs::member_id_map_field(&attesting_summary().member_id_map);
    assert!(field.contains("2 members"), "{field}");
    assert!(field.contains("1 -> `dir/member-1`"), "{field}");

    let empty = ReportInputs::member_id_map_field(&[]);
    assert!(
        empty.contains("empty"),
        "an empty map is a fact about the run, rendered rather than hidden: {empty}"
    );
}

#[test]
fn the_report_nemesis_field_attests_how_the_fault_bit_not_a_bare_boolean() {
    // T2: the report must carry the #407 typed materialization evidence — which fault, on which
    // target, and the leg's own diagnosis of how it provably bit — not a collapsed
    // `materialized: true`. A regression that dropped the diagnosis/target back to a boolean flips
    // this red.
    let field = ReportInputs::nemesis_field(&attesting_evidence());
    assert!(
        field.contains("partition"),
        "the fault class must be named: {field}"
    );
    assert!(
        field.contains("fdb0") && field.contains("172.30.58.11"),
        "the target the fault hit must be named (service + address), not just the fault class: \
         {field}"
    );
    assert!(
        field.contains("during=false") && field.contains("target_running_during=true"),
        "the leg's OWN diagnosis (the sampled observations proving the fault bit) must be \
         rendered verbatim — this is the fidelity the bare boolean lacked: {field}"
    );
}
